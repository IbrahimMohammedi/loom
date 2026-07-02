package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/IbrahimMohammedi/loom"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "loom.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func created(id string) loom.Event {
	return loom.Event{Seq: 1, Type: loom.EvInstanceCreated, Time: time.Now().UTC()}
}

func TestCreateInstanceIsConstrainedAppend(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	if err := s.CreateInstance(ctx, "i1", "wf", created("i1")); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateInstance(ctx, "i1", "wf", created("i1")); !errors.Is(err, loom.ErrInstanceExists) {
		t.Fatalf("duplicate submission must be rejected, got %v", err)
	}
}

func TestEpochFencing(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	_ = s.CreateInstance(ctx, "i1", "wf", created("i1"))

	zombieEpoch, err := s.ClaimInstance(ctx, "i1", "zombie")
	if err != nil {
		t.Fatal(err)
	}
	usurperEpoch, _ := s.ClaimInstance(ctx, "i1", "usurper")
	if usurperEpoch != zombieEpoch+1 {
		t.Fatalf("claim must bump epoch: %d -> %d", zombieEpoch, usurperEpoch)
	}
	ev := loom.Event{Type: loom.EvStepStarted, Step: "a", Attempt: 1, Time: time.Now()}
	if err := s.Append(ctx, "i1", zombieEpoch, ev, nil); !errors.Is(err, loom.ErrStaleEpoch) {
		t.Fatalf("stale append must be rejected, got %v", err)
	}
	if err := s.Append(ctx, "i1", usurperEpoch, ev, nil); err != nil {
		t.Fatalf("current-epoch append must succeed: %v", err)
	}
}

func TestResumeFirstWins(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	_ = s.CreateInstance(ctx, "i1", "wf", created("i1"))

	ev := loom.Event{Type: loom.EvStepResumed, Step: "gate", Attempt: 1,
		Payload: []byte(`{"decision":{"approved":true}}`), Time: time.Now()}
	prior, err := s.AppendResumed(ctx, "i1", ev, &loom.MetaUpdate{Status: loom.StatusRunning})
	if err != nil || prior != nil {
		t.Fatalf("first resume: %v %v", err, prior)
	}
	ev2 := ev
	ev2.Payload = []byte(`{"decision":{"approved":false}}`)
	prior, err = s.AppendResumed(ctx, "i1", ev2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if prior == nil || string(prior.Payload) != `{"decision":{"approved":true}}` {
		t.Fatalf("second resume must return the first decision, got %v", prior)
	}
	events, _ := s.Load(ctx, "i1")
	resumes := 0
	for _, e := range events {
		if e.Type == loom.EvStepResumed {
			resumes++
		}
	}
	if resumes != 1 {
		t.Fatalf("exactly one resume event must exist, got %d", resumes)
	}
}

func TestRunnableAndStatus(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	_ = s.CreateInstance(ctx, "i1", "wf", created("i1"))
	_ = s.CreateInstance(ctx, "i2", "wf", created("i2"))

	future := time.Now().Add(time.Hour)
	if err := s.SetStatus(ctx, "i2", loom.StatusWaitingRetry, &future); err != nil {
		t.Fatal(err)
	}
	ids, err := s.Runnable(ctx, time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "i1" {
		t.Fatalf("only i1 should be runnable, got %v", ids)
	}
	ids, _ = s.Runnable(ctx, future.Add(time.Minute), 10)
	if len(ids) != 2 {
		t.Fatalf("elapsed notBefore must be runnable, got %v", ids)
	}
}

func TestPruneTerminalOnly(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	_ = s.CreateInstance(ctx, "done", "wf", created("done"))
	_ = s.CreateInstance(ctx, "waiting", "wf", created("waiting"))
	_ = s.SetStatus(ctx, "done", loom.StatusCompleted, nil)
	_ = s.SetStatus(ctx, "waiting", loom.StatusSuspended, nil)

	n, err := s.Prune(ctx, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("only the terminal instance is prunable, got %d", n)
	}
	if _, err := s.Meta(ctx, "waiting"); err != nil {
		t.Fatal("suspended instance must survive prune")
	}
	if _, err := s.Meta(ctx, "done"); !errors.Is(err, loom.ErrNotFound) {
		t.Fatal("terminal instance must be gone")
	}
}
