package replay_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/IbrahimMohammedi/loom"
	"github.com/IbrahimMohammedi/loom/replay"
	"github.com/IbrahimMohammedi/loom/store/memory"
)

// mockLLM's output depends only on its configured text: injectable, so
// replay is deterministic (principle 5, scoped honestly).
type mockLLM struct{ text string }

func (m *mockLLM) Complete(ctx context.Context, req loom.LLMRequest) (loom.LLMResponse, error) {
	return loom.LLMResponse{Model: req.Model, Text: m.text, InputTokens: len(req.Prompt) / 4, OutputTokens: len(m.text) / 4}, nil
}

var chain = []loom.ModelSpec{{Model: "m", MaxInputTokens: 100, MaxOutputTokens: 100, InPrice: 100, OutPrice: 100}}

type draft struct {
	Body string `json:"body"`
}

func buildDef(t *testing.T, classifierOut string) *loom.Definition {
	t.Helper()
	wf := loom.NewWorkflow("wf")
	wf.Step("classify", loom.Typed(func(ctx context.Context, sctx *loom.StepContext, in string) (string, error) {
		return classifierOut, nil
	}))
	wf.LLMStep("draft", chain, loom.Typed(func(ctx context.Context, sctx *loom.StepContext, in string) (draft, error) {
		resp, err := sctx.LLM().Complete(ctx, "about "+in)
		if err != nil {
			return draft{}, err
		}
		return draft{Body: resp.Text}, nil
	}))
	wf.Approval("gate")
	wf.Step("send", loom.Typed(func(ctx context.Context, sctx *loom.StepContext, in loom.ApprovalDecision) (string, error) {
		return "sent", nil
	}))
	def, err := wf.Build()
	if err != nil {
		t.Fatal(err)
	}
	return def
}

// record produces a completed instance log by actually running the engine.
func record(t *testing.T, def *loom.Definition) []loom.Event {
	t.Helper()
	ctx := context.Background()
	store := memory.New()
	eng, err := loom.NewEngine(loom.Config{Store: store, LLM: &mockLLM{text: "hello"}, ExecutorID: "rec", Tick: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	eng.Register(def)
	if err := eng.StartInstance(ctx, "wf", "r1", "ticket", loom.Dollars(1)); err != nil {
		t.Fatal(err)
	}
	if err := eng.RunInstance("r1"); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Resume(ctx, "r1", loom.ApprovalDecision{Approved: true}); err != nil {
		t.Fatal(err)
	}
	if err := eng.RunInstance("r1"); err != nil {
		t.Fatal(err)
	}
	events, err := store.Load(ctx, "r1")
	if err != nil {
		t.Fatal(err)
	}
	return events
}

func TestTruncateAndRerunNoDivergence(t *testing.T) {
	def := buildDef(t, "billing")
	log := record(t, def)

	h := replay.New(log, replay.WithLLM(&mockLLM{text: "hello"}), replay.TruncateAt("draft", 1))
	res, err := h.Run(def)
	if err != nil {
		t.Fatal(err)
	}
	if res.Diverged {
		t.Fatalf("same code, same mocks must not diverge: %+v", res)
	}
	if res.FinalStatus != loom.StatusCompleted {
		t.Fatalf("recorded approval must replay from the log; got status %s", res.FinalStatus)
	}
}

func TestDivergenceDetectedAtFirstChangedStep(t *testing.T) {
	log := record(t, buildDef(t, "billing"))
	// Step logic regressed: classify now returns a different category.
	changed := buildDef(t, "technical")

	h := replay.New(log, replay.WithLLM(&mockLLM{text: "hello"}), replay.TruncateAt("classify", 1))
	res, err := h.Run(changed)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Diverged || res.DivergedAt != "classify" {
		t.Fatalf("want divergence at classify, got %+v", res)
	}
	for _, sc := range res.Steps {
		if sc.Step != "classify" && !sc.Match && !sc.Advisory {
			t.Fatalf("post-divergence mismatches must be advisory: %+v", sc)
		}
	}
}

func TestLLMStepUsesStructuralComparison(t *testing.T) {
	log := record(t, buildDef(t, "billing"))
	def := buildDef(t, "billing")

	// Different LLM text, same output shape: not a divergence for an LLM
	// step — replay regression-tests everything around the model.
	h := replay.New(log, replay.WithLLM(&mockLLM{text: "completely different wording"}), replay.TruncateAt("draft", 1))
	res, err := h.Run(def)
	if err != nil {
		t.Fatal(err)
	}
	if res.Diverged {
		t.Fatalf("structural comparator must accept different LLM content: %+v", res)
	}
}

func TestInjectFailureExercisesRetry(t *testing.T) {
	def := buildDef(t, "billing")
	log := record(t, def)

	h := replay.New(log, replay.WithLLM(&mockLLM{text: "hello"}),
		replay.TruncateAt("draft", 1),
		replay.InjectFailure("draft", errors.New("injected 529")))
	res, err := h.Run(def)
	if err != nil {
		t.Fatal(err)
	}
	// Default retry Max=1: one recorded failure exhausts the policy.
	if res.FinalStatus != loom.StatusFailed {
		t.Fatalf("injected failure with Max=1 must terminate failed, got %s", res.FinalStatus)
	}
}

func TestUnrecordedSuspensionStopsCleanly(t *testing.T) {
	def := buildDef(t, "billing")
	// Record only up to the suspension: no resume decision exists.
	ctx := context.Background()
	store := memory.New()
	eng, _ := loom.NewEngine(loom.Config{Store: store, LLM: &mockLLM{text: "hello"}, ExecutorID: "rec", Tick: time.Hour})
	eng.Register(def)
	_ = eng.StartInstance(ctx, "wf", "r2", "ticket", loom.Dollars(1))
	_ = eng.RunInstance("r2")
	log, _ := store.Load(ctx, "r2")

	h := replay.New(log, replay.WithLLM(&mockLLM{text: "hello"}), replay.TruncateAt("draft", 1))
	res, err := h.Run(def)
	if err != nil {
		t.Fatal(err)
	}
	if res.Diverged {
		t.Fatal("stopping for unrecorded input is not a divergence")
	}
	if res.StoppedAt != "gate" || res.StopReason != "awaiting-unrecorded-input" {
		t.Fatalf("got %+v", res)
	}

	// The what-if tool: script the decision reality never supplied.
	h2 := replay.New(log, replay.WithLLM(&mockLLM{text: "hello"}),
		replay.WithResumeDecision("gate", loom.ApprovalDecision{Approved: false}))
	res2, err := h2.Run(def)
	if err != nil {
		t.Fatal(err)
	}
	if res2.FinalStatus != loom.StatusRejected {
		t.Fatalf("scripted rejection must halt the workflow, got %s", res2.FinalStatus)
	}
}
