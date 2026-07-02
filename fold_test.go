package loom

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func testDef(t *testing.T) *Definition {
	t.Helper()
	noop := func(ctx context.Context, sctx *StepContext, in json.RawMessage) (StepOutcome, error) {
		return StepOutcome{Output: json.RawMessage(`"ok"`)}, nil
	}
	wf := NewWorkflow("wf")
	wf.Step("a", noop)
	wf.Step("b", noop).Retry(Retry{Max: 3, Backoff: Exponential(30 * time.Second)})
	wf.Step("c", noop)
	def, err := wf.Build()
	if err != nil {
		t.Fatal(err)
	}
	return def
}

func createdEvent(def *Definition, budget Cost) Event {
	ev := newEvent(EvInstanceCreated, "", 0, InstanceCreatedPayload{
		Workflow: def.name, Input: json.RawMessage(`"in"`),
		StepNames: def.stepNames, SuffixHashes: def.suffixHashes, Budget: budget,
	})
	ev.Seq = 1
	return ev
}

func TestFoldPrescribesFirstStep(t *testing.T) {
	def := testDef(t)
	_, p, err := Fold(def, []Event{createdEvent(def, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if p.Kind != PrescribeRunStep || p.Step != "a" || p.Attempt != 1 {
		t.Fatalf("got %+v", p)
	}
	if string(p.Input) != `"in"` {
		t.Fatalf("first step input should be workflow input, got %s", p.Input)
	}
}

func TestFoldSkipsCompletedAndFeedsPrevOutput(t *testing.T) {
	def := testDef(t)
	events := []Event{
		createdEvent(def, 0),
		newEvent(EvStepStarted, "a", 1, nil),
		newEvent(EvStepCompleted, "a", 1, StepCompletedPayload{Output: json.RawMessage(`"a-out"`)}),
	}
	_, p, _ := Fold(def, events)
	if p.Step != "b" || string(p.Input) != `"a-out"` {
		t.Fatalf("got %+v", p)
	}
}

func TestFoldHaltIsSoleSourceOfTerminality(t *testing.T) {
	def := testDef(t)
	events := []Event{
		createdEvent(def, 0),
		newEvent(EvStepStarted, "a", 1, nil),
		newEvent(EvStepCompleted, "a", 1, StepCompletedPayload{Halt: true, Status: "rejected"}),
	}
	_, p, _ := Fold(def, events)
	if p.Kind != PrescribeDone || p.Status != "rejected" {
		t.Fatalf("halted step must terminate the fold, got %+v", p)
	}
}

func TestFoldRetryNotBeforeFromLoggedTimestampsOnly(t *testing.T) {
	def := testDef(t)
	failAt := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fail := newEvent(EvStepFailed, "b", 1, StepFailedPayload{Error: "boom"})
	fail.Time = failAt
	events := []Event{
		createdEvent(def, 0),
		newEvent(EvStepStarted, "a", 1, nil),
		newEvent(EvStepCompleted, "a", 1, StepCompletedPayload{}),
		newEvent(EvStepStarted, "b", 1, nil),
		fail,
	}
	_, p, _ := Fold(def, events)
	if p.Kind != PrescribeRunStep || p.Step != "b" || p.Attempt != 2 {
		t.Fatalf("got %+v", p)
	}
	if !p.NotBefore.Equal(failAt.Add(30 * time.Second)) {
		t.Fatalf("notBefore must be failedAt+backoff, got %v", p.NotBefore)
	}
	// Same log, same prescription, at any replay time: fold is clock-free.
	_, p2, _ := Fold(def, events)
	if !p2.NotBefore.Equal(p.NotBefore) {
		t.Fatal("fold is not deterministic")
	}
}

func TestFoldRetriesExhaustedIsTerminalFailed(t *testing.T) {
	def := testDef(t)
	events := []Event{
		createdEvent(def, 0),
		newEvent(EvStepStarted, "a", 1, nil),
		newEvent(EvStepCompleted, "a", 1, StepCompletedPayload{}),
	}
	for i := 1; i <= 3; i++ {
		events = append(events,
			newEvent(EvStepStarted, "b", i, nil),
			newEvent(EvStepFailed, "b", i, StepFailedPayload{Error: "boom"}))
	}
	_, p, _ := Fold(def, events)
	if p.Kind != PrescribeDone || p.Status != StatusFailed {
		t.Fatalf("got %+v", p)
	}
}

func TestFoldCrashedAttemptDoesNotCountAgainstRetries(t *testing.T) {
	def := testDef(t)
	events := []Event{
		createdEvent(def, 0),
		newEvent(EvStepStarted, "a", 1, nil), // crashed: no outcome
	}
	_, p, _ := Fold(def, events)
	if p.Kind != PrescribeRunStep || p.Step != "a" || p.Attempt != 2 {
		t.Fatalf("crashed attempt should re-run as attempt 2, got %+v", p)
	}
	if !p.NotBefore.IsZero() {
		t.Fatal("crashed attempt must not incur backoff")
	}
}

func TestFoldDetectsOrphanedReservations(t *testing.T) {
	def := testDef(t)
	events := []Event{
		createdEvent(def, Dollars(1)),
		newEvent(EvStepStarted, "a", 1, nil),
		newEvent(EvBudgetReserved, "a", 1, BudgetReservedPayload{Amount: Dollars(0.40)}),
		// crash: no settle, no completion
	}
	st, p, _ := Fold(def, events)
	if len(p.Orphans) != 1 || p.Orphans[0].Amount != Dollars(0.40) {
		t.Fatalf("got orphans %+v", p.Orphans)
	}
	if st.Committed != Dollars(0.40) {
		t.Fatalf("open reservation must count at reserved amount, got %s", st.Committed)
	}
}

func TestFoldBudgetSettleReplacesReservation(t *testing.T) {
	def := testDef(t)
	events := []Event{
		createdEvent(def, Dollars(1)),
		newEvent(EvStepStarted, "a", 1, nil),
		newEvent(EvBudgetReserved, "a", 1, BudgetReservedPayload{Amount: Dollars(0.40)}),
		newEvent(EvBudgetSettled, "a", 1, BudgetSettledPayload{Amount: Dollars(0.05), Reason: "actual"}),
		newEvent(EvStepCompleted, "a", 1, StepCompletedPayload{}),
	}
	st, _, _ := Fold(def, events)
	if st.Committed != Dollars(0.05) {
		t.Fatalf("committed should be settled actual, got %s", st.Committed)
	}
}

func TestFoldIncompatibleWhenNextStepRenamed(t *testing.T) {
	def := testDef(t)
	// Instance created under a definition whose next step "b2" no longer exists.
	created := InstanceCreatedPayload{
		Workflow:  "wf",
		StepNames: []string{"a", "b2", "c"},
		SuffixHashes: []string{"x", "y", "z"},
	}
	ev := newEvent(EvInstanceCreated, "", 0, created)
	events := []Event{
		ev,
		newEvent(EvStepStarted, "a", 1, nil),
		newEvent(EvStepCompleted, "a", 1, StepCompletedPayload{}),
	}
	_, p, _ := Fold(def, events)
	if p.Kind != PrescribeIncompatible || p.Step != "b2" {
		t.Fatalf("renamed next step must strand, got %+v", p)
	}
}

func TestFoldIncompatibleWhenSuffixChanged(t *testing.T) {
	def := testDef(t)
	created := InstanceCreatedPayload{
		Workflow:     "wf",
		StepNames:    def.stepNames,
		SuffixHashes: []string{"stale", "stale", "stale"},
	}
	events := []Event{newEvent(EvInstanceCreated, "", 0, created)}
	_, p, _ := Fold(def, events)
	if p.Kind != PrescribeIncompatible {
		t.Fatalf("got %+v", p)
	}
}

func TestFoldNameAnchorSurvivesPrefixInsertion(t *testing.T) {
	// New definition inserts "sanitize" before "b"; instance is past "a".
	noop := func(ctx context.Context, sctx *StepContext, in json.RawMessage) (StepOutcome, error) {
		return StepOutcome{}, nil
	}
	oldWf := NewWorkflow("wf")
	oldWf.Step("a", noop)
	oldWf.Step("b", noop)
	oldWf.Step("c", noop)
	oldDef, _ := oldWf.Build()

	newWf := NewWorkflow("wf")
	newWf.Step("a", noop)
	newWf.Step("sanitize", noop)
	newWf.Step("b", noop)
	newWf.Step("c", noop)
	newDef, _ := newWf.Build()

	events := []Event{
		createdEvent(oldDef, 0),
		newEvent(EvStepStarted, "a", 1, nil),
		newEvent(EvStepCompleted, "a", 1, StepCompletedPayload{Output: json.RawMessage(`1`)}),
	}
	_, p, _ := Fold(newDef, events)
	if p.Kind != PrescribeRunStep || p.Step != "b" {
		t.Fatalf("prefix insertion must not strand; sanitize never runs for old instances; got %+v", p)
	}
}

func TestFoldSuspendedAwaitsResume(t *testing.T) {
	noop := func(ctx context.Context, sctx *StepContext, in json.RawMessage) (StepOutcome, error) {
		return StepOutcome{}, nil
	}
	wf := NewWorkflow("wf")
	wf.Step("a", noop)
	wf.Approval("gate")
	wf.Step("send", noop)
	def, _ := wf.Build()

	events := []Event{
		createdEvent(def, 0),
		newEvent(EvStepStarted, "a", 1, nil),
		newEvent(EvStepCompleted, "a", 1, StepCompletedPayload{}),
		newEvent(EvStepSuspended, "gate", 1, nil),
	}
	_, p, _ := Fold(def, events)
	if p.Kind != PrescribeAwaitResume || p.Step != "gate" {
		t.Fatalf("got %+v", p)
	}
	events = append(events, newEvent(EvStepResumed, "gate", 1, StepResumedPayload{Decision: json.RawMessage(`{"approved":true}`)}))
	_, p, _ = Fold(def, events)
	if p.Kind != PrescribeFinalizeResume {
		t.Fatalf("got %+v", p)
	}
}

func TestFoldAllStepsCompletedIsTerminalCompleted(t *testing.T) {
	def := testDef(t)
	events := []Event{createdEvent(def, 0)}
	for _, name := range def.stepNames {
		events = append(events,
			newEvent(EvStepStarted, name, 1, nil),
			newEvent(EvStepCompleted, name, 1, StepCompletedPayload{}))
	}
	_, p, _ := Fold(def, events)
	if p.Kind != PrescribeDone || p.Status != StatusCompleted {
		t.Fatalf("got %+v", p)
	}
}

func TestWorkflowValidation(t *testing.T) {
	noop := func(ctx context.Context, sctx *StepContext, in json.RawMessage) (StepOutcome, error) {
		return StepOutcome{}, nil
	}
	wf := NewWorkflow("bad")
	wf.Step("a", noop)
	wf.Step("a", noop)                    // duplicate name (ADR-4)
	wf.Step("b", noop).Reads("nonexist")  // undeclared earlier step (ADR-13)
	if _, err := wf.Build(); err == nil {
		t.Fatal("Build must fail on duplicate names and bad Reads")
	}
}
