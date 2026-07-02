package loom_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/IbrahimMohammedi/loom"
	"github.com/IbrahimMohammedi/loom/store/memory"
)

type fakeLLM struct {
	failModels map[string]bool
}

func (f *fakeLLM) Complete(ctx context.Context, req loom.LLMRequest) (loom.LLMResponse, error) {
	inTok := len(req.Prompt) / 4
	if f.failModels[req.Model] {
		// A failed hop still bills input tokens.
		return loom.LLMResponse{Model: req.Model, InputTokens: inTok}, errors.New("simulated 529")
	}
	return loom.LLMResponse{Model: req.Model, Text: "drafted reply", InputTokens: inTok, OutputTokens: 10}, nil
}

var chain = []loom.ModelSpec{
	{Model: "big", MaxInputTokens: 1000, MaxOutputTokens: 500, InPrice: 3000, OutPrice: 15000},
	{Model: "mini", MaxInputTokens: 1000, MaxOutputTokens: 500, InPrice: 250, OutPrice: 1250},
}

type approvalIn struct {
	Approved bool   `json:"approved"`
	Note     string `json:"note,omitempty"`
}

func triageDef(t *testing.T) *loom.Definition {
	t.Helper()
	wf := loom.NewWorkflow("triage")
	wf.Step("classify", loom.Typed(func(ctx context.Context, sctx *loom.StepContext, in string) (string, error) {
		return "billing", nil
	}))
	wf.LLMStep("draft", chain, loom.Typed(func(ctx context.Context, sctx *loom.StepContext, in string) (string, error) {
		resp, err := sctx.LLM().Complete(ctx, "draft a reply about "+in)
		if err != nil {
			return "", err
		}
		return resp.Text, nil
	}))
	wf.Approval("gate")
	wf.Step("send", loom.Typed(func(ctx context.Context, sctx *loom.StepContext, in approvalIn) (string, error) {
		var draft string
		if err := sctx.Output("draft", &draft); err != nil {
			return "", err
		}
		return "sent: " + draft, nil
	})).Reads("draft")
	def, err := wf.Build()
	if err != nil {
		t.Fatal(err)
	}
	return def
}

func newTestEngine(t *testing.T, store loom.StateStore, def *loom.Definition, llm loom.LLMClient) *loom.Engine {
	t.Helper()
	eng, err := loom.NewEngine(loom.Config{Store: store, LLM: llm, ExecutorID: "test", Tick: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	eng.Register(def)
	return eng
}

func TestEngineEndToEndWithApproval(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	def := triageDef(t)
	eng := newTestEngine(t, store, def, &fakeLLM{})

	if err := eng.StartInstance(ctx, "triage", "i1", "ticket text", loom.Dollars(1)); err != nil {
		t.Fatal(err)
	}
	if err := eng.RunInstance("i1"); err != nil {
		t.Fatal(err)
	}
	meta, _ := store.Meta(ctx, "i1")
	if meta.Status != loom.StatusSuspended {
		t.Fatalf("expected suspension at gate, got %s", meta.Status)
	}

	res, err := eng.Resume(ctx, "i1", loom.ApprovalDecision{Approved: true})
	if err != nil || res.AlreadyResumed {
		t.Fatalf("resume: %v %+v", err, res)
	}
	// Second delivery: first-wins, returns the recorded decision.
	res2, err := eng.Resume(ctx, "i1", loom.ApprovalDecision{Approved: false, Note: "racer"})
	if err != nil {
		t.Fatal(err)
	}
	if !res2.AlreadyResumed {
		t.Fatal("second resume must report AlreadyResumed")
	}

	if err := eng.RunInstance("i1"); err != nil {
		t.Fatal(err)
	}
	meta, _ = store.Meta(ctx, "i1")
	if meta.Status != loom.StatusCompleted {
		t.Fatalf("expected completed, got %s", meta.Status)
	}
	// Budget: one reservation settled at actual.
	events, _ := store.Load(ctx, "i1")
	settled := 0
	for _, ev := range events {
		if ev.Type == loom.EvBudgetSettled {
			settled++
		}
	}
	if settled != 1 {
		t.Fatalf("expected 1 settlement, got %d", settled)
	}
}

func TestEngineRejectionHalts(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	def := triageDef(t)
	eng := newTestEngine(t, store, def, &fakeLLM{})

	_ = eng.StartInstance(ctx, "triage", "i2", "t", loom.Dollars(1))
	_ = eng.RunInstance("i2")
	if _, err := eng.Resume(ctx, "i2", loom.ApprovalDecision{Approved: false}); err != nil {
		t.Fatal(err)
	}
	_ = eng.RunInstance("i2")
	meta, _ := store.Meta(ctx, "i2")
	if meta.Status != loom.StatusRejected {
		t.Fatalf("expected rejected, got %s", meta.Status)
	}
	// send must never have started: structurally impossible, not defensively checked.
	events, _ := store.Load(ctx, "i2")
	for _, ev := range events {
		if ev.Step == "send" {
			t.Fatalf("send executed after rejection: %+v", ev)
		}
	}
}

func TestEngineBudgetExhaustion(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	def := triageDef(t)
	eng := newTestEngine(t, store, def, &fakeLLM{})

	// Chain reservation is sum of per-hop maxes: big (1000*3000 + 500*15000)
	// + mini (1000*250 + 500*1250) = 10.5e6 + 0.875e6 nano$ = ~$0.0114.
	_ = eng.StartInstance(ctx, "triage", "i3", "t", loom.Cost(1000)) // far below reservation
	_ = eng.RunInstance("i3")
	meta, _ := store.Meta(ctx, "i3")
	if meta.Status != loom.StatusBudgetExhausted {
		t.Fatalf("expected budget_exhausted, got %s", meta.Status)
	}
}

func TestEngineFallbackChainBillsFailedHop(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	def := triageDef(t)
	eng := newTestEngine(t, store, def, &fakeLLM{failModels: map[string]bool{"big": true}})

	_ = eng.StartInstance(ctx, "triage", "i4", "t", loom.Dollars(1))
	_ = eng.RunInstance("i4")
	events, _ := store.Load(ctx, "i4")
	var settled loom.Cost
	for _, ev := range events {
		if ev.Type == loom.EvBudgetSettled {
			var p struct {
				Amount loom.Cost `json:"amount_nanousd"`
			}
			unmarshal(t, ev.Payload, &p)
			settled = p.Amount
		}
	}
	// Prompt "draft a reply about billing" = 26 chars -> 6 input tokens.
	// big fails: 6*3000 = 18000. mini succeeds: 6*250 + 10*1250 = 14000.
	want := loom.Cost(18000 + 14000)
	if settled != want {
		t.Fatalf("failed hop must be billed: want %d got %d", want, settled)
	}
}

func TestEngineOrphanedReservationSettledAtMax(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	def := triageDef(t)
	eng := newTestEngine(t, store, def, &fakeLLM{})

	_ = eng.StartInstance(ctx, "triage", "i5", "t", loom.Dollars(1))
	// Manufacture the crash window: classify done, draft started+reserved,
	// no settle, no completion — as a killed process would leave it.
	epoch, err := store.ClaimInstance(ctx, "i5", "dead-process")
	if err != nil {
		t.Fatal(err)
	}
	mk := func(typ loom.EventType, step string, attempt int, payload any) {
		ev := loom.Event{Type: typ, Step: step, Attempt: attempt, Time: time.Now().UTC()}
		if payload != nil {
			ev.Payload = marshal(t, payload)
		}
		if err := store.Append(ctx, "i5", epoch, ev, nil); err != nil {
			t.Fatal(err)
		}
	}
	mk(loom.EvStepStarted, "classify", 1, nil)
	mk(loom.EvStepCompleted, "classify", 1, loom.StepCompletedPayload{Output: marshal(t, "billing")})
	mk(loom.EvStepStarted, "draft", 1, nil)
	reserve := loom.Cost(1000*3000+500*15000) + loom.Cost(1000*250+500*1250)
	mk(loom.EvBudgetReserved, "draft", 1, loom.BudgetReservedPayload{Amount: reserve})

	// Fresh process resumes: same code path, no recovery mode.
	if err := eng.RunInstance("i5"); err != nil {
		t.Fatal(err)
	}
	events, _ := store.Load(ctx, "i5")
	classifyStarts, orphanSettles := 0, 0
	for _, ev := range events {
		if ev.Type == loom.EvStepStarted && ev.Step == "classify" {
			classifyStarts++
		}
		if ev.Type == loom.EvBudgetSettled {
			var p loom.BudgetSettledPayload
			unmarshal(t, ev.Payload, &p)
			if p.Reason == "crash-orphaned" {
				orphanSettles++
				if p.Amount != reserve {
					t.Fatalf("orphan must settle at reserved max: want %d got %d", reserve, p.Amount)
				}
			}
		}
	}
	if classifyStarts != 1 {
		t.Fatalf("completed step re-executed: %d starts", classifyStarts)
	}
	if orphanSettles != 1 {
		t.Fatalf("expected 1 crash-orphaned settlement, got %d", orphanSettles)
	}
	meta, _ := store.Meta(ctx, "i5")
	if meta.Status != loom.StatusSuspended { // resumes and reaches the gate
		t.Fatalf("got %s", meta.Status)
	}
}

func TestEngineStartAcquisitionLoop(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	wf := loom.NewWorkflow("simple")
	wf.Step("only", loom.Typed(func(ctx context.Context, sctx *loom.StepContext, in string) (string, error) {
		return in + "!", nil
	}))
	def, _ := wf.Build()
	eng := newTestEngine(t, store, def, nil)

	_ = eng.StartInstance(ctx, "simple", "s1", "hey", 0)
	eng.Start()
	deadline := time.Now().Add(3 * time.Second)
	for {
		meta, _ := store.Meta(ctx, "s1")
		if meta.Status == loom.StatusCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("acquisition loop never completed instance, status=%s", meta.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	sctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := eng.Shutdown(sctx); err != nil {
		t.Fatal(err)
	}
}

func TestStaleEpochFencing(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	def := triageDef(t)
	eng := newTestEngine(t, store, def, &fakeLLM{})
	_ = eng.StartInstance(ctx, "triage", "z1", "t", loom.Dollars(1))

	oldEpoch, _ := store.ClaimInstance(ctx, "z1", "zombie")
	_, _ = store.ClaimInstance(ctx, "z1", "usurper")
	err := store.Append(ctx, "z1", oldEpoch, loom.Event{Type: loom.EvStepStarted, Step: "classify", Attempt: 1, Time: time.Now()}, nil)
	if !errors.Is(err, loom.ErrStaleEpoch) {
		t.Fatalf("want ErrStaleEpoch, got %v", err)
	}
}
