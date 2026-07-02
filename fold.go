package loom

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

type PrescriptionKind int

const (
	PrescribeDone PrescriptionKind = iota
	PrescribeRunStep
	PrescribeAwaitResume
	PrescribeFinalizeResume
	PrescribeIncompatible
)

type OrphanReservation struct {
	Step    string
	Attempt int
	Amount  Cost
}

// Prescription is the fold's output: the single next action the instance's
// state prescribes (ADR-1). NotBefore is computed from logged timestamps
// and declared policy only — the fold never reads the wall clock (ADR-9);
// the executor compares NotBefore to its own clock.
type Prescription struct {
	Kind      PrescriptionKind
	Step      string
	Attempt   int
	NotBefore time.Time
	Status    string          // terminal status, for Done
	Decision  json.RawMessage // recorded external input, for FinalizeResume
	Orphans   []OrphanReservation
	Input     json.RawMessage // previous step's output (workflow input for step 1)
	Reason    string          // human-readable, for Incompatible
}

type stepState struct {
	maxStarted   int
	completed    *StepCompletedPayload
	failures     int
	permanent    *StepFailedPayload
	lastFailTime time.Time
	suspendedAt  int // attempt; 0 = never suspended
	resumed      *StepResumedPayload
	openRes      map[int]Cost // attempt -> open (unsettled) reservation
}

// FoldState is the computed state of an instance log.
type FoldState struct {
	Created   InstanceCreatedPayload
	steps     map[string]*stepState
	Committed Cost // open reservations at reserved amount + settled actuals
	outputs   map[string]json.RawMessage
}

// Outputs returns completed step outputs by step name.
func (st *FoldState) Outputs() map[string]json.RawMessage { return st.outputs }

func foldEvents(events []Event) (*FoldState, error) {
	if len(events) == 0 || events[0].Type != EvInstanceCreated {
		return nil, fmt.Errorf("loom: event log must begin with %s", EvInstanceCreated)
	}
	st := &FoldState{steps: map[string]*stepState{}, outputs: map[string]json.RawMessage{}}
	if err := json.Unmarshal(orNull(events[0].Payload), &st.Created); err != nil {
		return nil, fmt.Errorf("loom: decode instance_created: %w", err)
	}
	get := func(name string) *stepState {
		s, ok := st.steps[name]
		if !ok {
			s = &stepState{openRes: map[int]Cost{}}
			st.steps[name] = s
		}
		return s
	}
	for _, ev := range events[1:] {
		s := get(ev.Step)
		switch ev.Type {
		case EvStepStarted:
			if ev.Attempt > s.maxStarted {
				s.maxStarted = ev.Attempt
			}
		case EvStepCompleted:
			var p StepCompletedPayload
			if err := json.Unmarshal(orNull(ev.Payload), &p); err != nil {
				return nil, err
			}
			s.completed = &p
			st.outputs[ev.Step] = p.Output
		case EvStepFailed:
			var p StepFailedPayload
			if err := json.Unmarshal(orNull(ev.Payload), &p); err != nil {
				return nil, err
			}
			if p.Permanent {
				pc := p
				s.permanent = &pc
			} else {
				s.failures++
				s.lastFailTime = ev.Time
			}
		case EvStepSuspended:
			s.suspendedAt = ev.Attempt
			if ev.Attempt > s.maxStarted {
				s.maxStarted = ev.Attempt
			}
		case EvStepResumed:
			var p StepResumedPayload
			if err := json.Unmarshal(orNull(ev.Payload), &p); err != nil {
				return nil, err
			}
			s.resumed = &p
		case EvBudgetReserved:
			var p BudgetReservedPayload
			if err := json.Unmarshal(orNull(ev.Payload), &p); err != nil {
				return nil, err
			}
			s.openRes[ev.Attempt] = p.Amount
			st.Committed += p.Amount
		case EvBudgetSettled:
			var p BudgetSettledPayload
			if err := json.Unmarshal(orNull(ev.Payload), &p); err != nil {
				return nil, err
			}
			if res, ok := s.openRes[ev.Attempt]; ok {
				st.Committed += p.Amount - res
				delete(s.openRes, ev.Attempt)
			} else {
				st.Committed += p.Amount
			}
		}
	}
	return st, nil
}

// Fold computes instance state and the single next action it prescribes
// (ADR-1). Recovery and normal execution are the same fold. Progress walks
// the instance's creation-time step order; the next unexecuted step is
// anchored into the current definition by name, and the remaining suffix
// must hash-match, or the instance is incompatible (ADR-5).
func Fold(def *Definition, events []Event) (*FoldState, Prescription, error) {
	st, err := foldEvents(events)
	if err != nil {
		return nil, Prescription{}, err
	}
	prevOutput := st.Created.Input
	for i, name := range st.Created.StepNames {
		s := st.steps[name]
		if s != nil && s.completed != nil {
			if s.completed.Halt {
				status := s.completed.Status
				if status == "" {
					status = "halted"
				}
				return st, Prescription{Kind: PrescribeDone, Status: status}, nil
			}
			prevOutput = s.completed.Output
			continue
		}
		// name is the next unexecuted step.
		if s != nil && s.permanent != nil {
			status := StatusFailed
			if s.permanent.Reason == StatusBudgetExhausted {
				status = StatusBudgetExhausted
			}
			return st, Prescription{Kind: PrescribeDone, Status: status}, nil
		}
		defIdx, ok := def.index[name]
		if !ok {
			return st, Prescription{Kind: PrescribeIncompatible, Step: name,
				Reason: fmt.Sprintf("next step %q does not exist in the registered definition (renamed or deleted)", name)}, nil
		}
		if i >= len(st.Created.SuffixHashes) || def.suffixHashes[defIdx] != st.Created.SuffixHashes[i] {
			return st, Prescription{Kind: PrescribeIncompatible, Step: name,
				Reason: fmt.Sprintf("definition suffix changed for not-yet-run steps from %q onward", name)}, nil
		}
		sd := def.steps[defIdx]
		if s != nil && s.failures >= sd.retry.Max {
			return st, Prescription{Kind: PrescribeDone, Status: StatusFailed}, nil
		}
		if s != nil && s.suspendedAt > 0 {
			if s.resumed == nil {
				return st, Prescription{Kind: PrescribeAwaitResume, Step: name, Attempt: s.suspendedAt}, nil
			}
			return st, Prescription{Kind: PrescribeFinalizeResume, Step: name, Attempt: s.suspendedAt, Decision: s.resumed.Decision}, nil
		}
		p := Prescription{Kind: PrescribeRunStep, Step: name, Attempt: 1, Input: prevOutput}
		if s != nil {
			p.Attempt = s.maxStarted + 1
			if s.failures > 0 && sd.retry.Backoff != nil {
				p.NotBefore = s.lastFailTime.Add(sd.retry.Backoff(s.failures))
			}
			for att, amt := range s.openRes {
				p.Orphans = append(p.Orphans, OrphanReservation{Step: name, Attempt: att, Amount: amt})
			}
			sort.Slice(p.Orphans, func(a, b int) bool { return p.Orphans[a].Attempt < p.Orphans[b].Attempt })
		}
		return st, p, nil
	}
	return st, Prescription{Kind: PrescribeDone, Status: StatusCompleted}, nil
}
