package loom_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/IbrahimMohammedi/loom"
	"github.com/IbrahimMohammedi/loom/store/memory"
)

// tornStore simulates a crash between a log append and a follow-up
// SetStatus: it drops the first terminal SetStatus, leaving an instance
// whose log is complete but whose status cache still says running.
type tornStore struct {
	loom.StateStore
	mu      sync.Mutex
	dropped bool
}

func (s *tornStore) SetStatus(ctx context.Context, id, status string, nb *time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.dropped && loom.IsTerminalStatus(status) {
		s.dropped = true
		return nil // the process "died" before this write landed
	}
	return s.StateStore.SetStatus(ctx, id, status, nb)
}

// TestTornStatusWriteStillAcquiredAndCompleted: kill between log-append and
// SetStatus, restart, assert the instance is still acquired and completed.
// This passes by construction: SetStatus-only transitions all move an
// instance FROM a claimable status, so the torn state is claimable and the
// acquisition loop re-drives it to the terminal status.
func TestTornStatusWriteStillAcquiredAndCompleted(t *testing.T) {
	ctx := context.Background()
	inner := memory.New()
	store := &tornStore{StateStore: inner}

	wf := loom.NewWorkflow("simple")
	wf.Step("only", loom.Typed(func(ctx context.Context, sctx *loom.StepContext, in string) (string, error) {
		return in + "!", nil
	}))
	def, err := wf.Build()
	if err != nil {
		t.Fatal(err)
	}

	eng1, _ := loom.NewEngine(loom.Config{Store: store, ExecutorID: "crashy", Tick: time.Hour})
	eng1.Register(def)
	_ = eng1.StartInstance(ctx, "simple", "torn1", "hi", 0)
	if err := eng1.RunInstance("torn1"); err != nil {
		t.Fatal(err)
	}

	// The torn state: log is fully completed, cache disagrees.
	meta, _ := inner.Meta(ctx, "torn1")
	if meta.Status == loom.StatusCompleted {
		t.Fatal("test setup broken: terminal SetStatus was not dropped")
	}

	// "Restart": a fresh engine over the same (untorn) store, acquisition
	// loop only — no direct RunInstance.
	eng2, _ := loom.NewEngine(loom.Config{Store: inner, ExecutorID: "fresh", Tick: 10 * time.Millisecond})
	eng2.Register(def)
	eng2.Start()
	defer func() {
		sctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_ = eng2.Shutdown(sctx)
	}()

	deadline := time.Now().Add(3 * time.Second)
	for {
		meta, _ := inner.Meta(ctx, "torn1")
		if meta.Status == loom.StatusCompleted {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("torn instance never re-acquired; status=%s", meta.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// racingStore deterministically reproduces the worst interleaving of the
// suspend/resume race: at the moment the driver parks an instance as
// suspended, a webhook Resume has already landed (appending StepResumed and
// setting status running); the driver's SetStatus(suspended) then overwrites
// it. Without the driver's park-then-refold, the cache would say suspended
// (unclaimable) while the log holds a resume — wedged forever.
type racingStore struct {
	loom.StateStore
	mu    sync.Mutex
	raced bool
}

func (s *racingStore) SetStatus(ctx context.Context, id, status string, nb *time.Time) error {
	s.mu.Lock()
	race := status == loom.StatusSuspended && !s.raced
	if race {
		s.raced = true
	}
	s.mu.Unlock()
	if race {
		// The webhook lands first...
		if _, err := loom.Resume(ctx, s.StateStore, id, loom.ApprovalDecision{Approved: true}); err != nil {
			return err
		}
	}
	// ...then the driver's park overwrites its status write.
	return s.StateStore.SetStatus(ctx, id, status, nb)
}

func TestResumeRacingParkDoesNotWedge(t *testing.T) {
	ctx := context.Background()
	inner := memory.New()
	store := &racingStore{StateStore: inner}

	wf := loom.NewWorkflow("gated")
	wf.Step("prep", loom.Typed(func(ctx context.Context, sctx *loom.StepContext, in string) (string, error) {
		return in, nil
	}))
	wf.Approval("gate")
	wf.Step("send", loom.Typed(func(ctx context.Context, sctx *loom.StepContext, d loom.ApprovalDecision) (string, error) {
		return "sent", nil
	}))
	def, err := wf.Build()
	if err != nil {
		t.Fatal(err)
	}

	eng, _ := loom.NewEngine(loom.Config{Store: store, ExecutorID: "racer", Tick: time.Hour})
	eng.Register(def)
	_ = eng.StartInstance(ctx, "gated", "race1", "hi", 0)
	if err := eng.RunInstance("race1"); err != nil {
		t.Fatal(err)
	}
	if !store.raced {
		t.Fatal("test setup broken: the race was never injected")
	}
	// The re-fold after parking must see the raced-in resume and drive the
	// workflow to completion in the same run — no wedge, no external help.
	meta, _ := inner.Meta(ctx, "race1")
	if meta.Status != loom.StatusCompleted {
		t.Fatalf("raced instance wedged with status %s (log holds a resume the cache hides)", meta.Status)
	}
}
