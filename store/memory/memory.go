// Package memory is an in-memory StateStore for tests and the replay
// harness (ADR-11): no external services, same append semantics as sqlite.
package memory

import (
	"context"
	"sync"
	"time"

	"github.com/IbrahimMohammedi/loom"
)

type instance struct {
	meta   loom.InstanceMeta
	events []loom.Event
}

type Store struct {
	mu   sync.Mutex
	inst map[string]*instance
}

var _ loom.StateStore = (*Store)(nil)

func New() *Store { return &Store{inst: map[string]*instance{}} }

func (s *Store) CreateInstance(_ context.Context, id, workflow string, ev loom.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.inst[id]; ok {
		return loom.ErrInstanceExists
	}
	now := time.Now().UTC()
	ev.Seq = 1
	s.inst[id] = &instance{
		meta:   loom.InstanceMeta{ID: id, Workflow: workflow, Status: loom.StatusPending, CreatedAt: now, UpdatedAt: now},
		events: []loom.Event{ev},
	}
	return nil
}

// Seed installs a pre-existing log verbatim — the replay harness's entry
// point for teleporting a recorded incident into a local store.
func (s *Store) Seed(id, workflow, status string, events []loom.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	evs := make([]loom.Event, len(events))
	copy(evs, events)
	for i := range evs {
		evs[i].Seq = int64(i + 1)
	}
	s.inst[id] = &instance{
		meta:   loom.InstanceMeta{ID: id, Workflow: workflow, Status: status, CreatedAt: now, UpdatedAt: now},
		events: evs,
	}
}

func (s *Store) Append(_ context.Context, id string, epoch int64, ev loom.Event, meta *loom.MetaUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, ok := s.inst[id]
	if !ok {
		return loom.ErrNotFound
	}
	if in.meta.Epoch != epoch {
		return loom.ErrStaleEpoch
	}
	ev.Seq = int64(len(in.events) + 1)
	in.events = append(in.events, ev)
	s.applyMeta(in, meta)
	return nil
}

func (s *Store) AppendResumed(_ context.Context, id string, ev loom.Event, meta *loom.MetaUpdate) (*loom.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, ok := s.inst[id]
	if !ok {
		return nil, loom.ErrNotFound
	}
	for i := range in.events {
		p := in.events[i]
		if p.Type == loom.EvStepResumed && p.Step == ev.Step && p.Attempt == ev.Attempt {
			prior := p
			return &prior, nil
		}
	}
	ev.Seq = int64(len(in.events) + 1)
	in.events = append(in.events, ev)
	s.applyMeta(in, meta)
	return nil, nil
}

func (s *Store) applyMeta(in *instance, meta *loom.MetaUpdate) {
	in.meta.UpdatedAt = time.Now().UTC()
	if meta == nil {
		return
	}
	if meta.Status != "" {
		in.meta.Status = meta.Status
	}
	if meta.NotBefore != nil {
		in.meta.NotBefore = *meta.NotBefore
	} else {
		in.meta.NotBefore = time.Time{}
	}
}

func (s *Store) ClaimInstance(_ context.Context, id, executor string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, ok := s.inst[id]
	if !ok {
		return 0, loom.ErrNotFound
	}
	in.meta.Epoch++
	in.meta.ClaimedBy = executor
	in.meta.UpdatedAt = time.Now().UTC()
	return in.meta.Epoch, nil
}

func (s *Store) Load(_ context.Context, id string) ([]loom.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, ok := s.inst[id]
	if !ok {
		return nil, loom.ErrNotFound
	}
	out := make([]loom.Event, len(in.events))
	copy(out, in.events)
	return out, nil
}

func (s *Store) Meta(_ context.Context, id string) (loom.InstanceMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, ok := s.inst[id]
	if !ok {
		return loom.InstanceMeta{}, loom.ErrNotFound
	}
	return in.meta, nil
}

func (s *Store) List(_ context.Context) ([]loom.InstanceMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]loom.InstanceMeta, 0, len(s.inst))
	for _, in := range s.inst {
		out = append(out, in.meta)
	}
	return out, nil
}

func (s *Store) Runnable(_ context.Context, now time.Time, limit int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for id, in := range s.inst {
		if len(out) >= limit {
			break
		}
		switch in.meta.Status {
		case loom.StatusPending, loom.StatusRunning:
			out = append(out, id)
		case loom.StatusWaitingRetry:
			if !in.meta.NotBefore.After(now) {
				out = append(out, id)
			}
		}
	}
	return out, nil
}

func (s *Store) SetStatus(_ context.Context, id, status string, notBefore *time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, ok := s.inst[id]
	if !ok {
		return loom.ErrNotFound
	}
	in.meta.Status = status
	if notBefore != nil {
		in.meta.NotBefore = *notBefore
	} else {
		in.meta.NotBefore = time.Time{}
	}
	in.meta.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *Store) Prune(_ context.Context, olderThan time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, in := range s.inst {
		if loom.IsTerminalStatus(in.meta.Status) && in.meta.UpdatedAt.Before(olderThan) {
			delete(s.inst, id)
			n++
		}
	}
	return n, nil
}
