package loom

import (
	"context"
	"encoding/json"
	"fmt"
)

// ApprovalDecision is the default decision shape for Approval steps.
type ApprovalDecision struct {
	Approved bool   `json:"approved"`
	Note     string `json:"note,omitempty"`
}

// StatusRejected is the terminal status the default finalize uses for a
// non-approved decision (part of the ADR-3 terminal set by construction).
const StatusRejected = "rejected"

func defaultFinalize(decision json.RawMessage) (StepOutcome, error) {
	var d ApprovalDecision
	if err := json.Unmarshal(orNull(decision), &d); err != nil {
		return StepOutcome{}, Permanent(fmt.Errorf("loom: approval decision: %w", err))
	}
	if !d.Approved {
		return StepOutcome{Output: decision, Halt: true, Status: StatusRejected}, nil
	}
	return StepOutcome{Output: decision}, nil
}

// ResumeResult reports what Resume recorded — or, first-wins, what someone
// else already recorded (ADR-10).
type ResumeResult struct {
	AlreadyResumed bool
	Decision       json.RawMessage
}

// Resume records an external decision for a suspended instance. It is a
// constrained append (ADR-7): no claim, no epoch. Duplicate deliveries —
// webhook retries, racing approvers, a timeout sweep racing a human — are
// resolved first-wins by the store's uniqueness constraint.
func Resume(ctx context.Context, store StateStore, id string, decision any) (ResumeResult, error) {
	raw, err := json.Marshal(decision)
	if err != nil {
		return ResumeResult{}, err
	}
	events, err := store.Load(ctx, id)
	if err != nil {
		return ResumeResult{}, err
	}
	st, err := foldEvents(events)
	if err != nil {
		return ResumeResult{}, err
	}
	step, attempt := "", 0
	for name, s := range st.steps {
		if s.suspendedAt > 0 && s.completed == nil {
			step, attempt = name, s.suspendedAt
		}
	}
	if step == "" {
		return ResumeResult{}, fmt.Errorf("loom: instance %s is not awaiting resume", id)
	}
	ev := newEvent(EvStepResumed, step, attempt, StepResumedPayload{Decision: raw})
	prior, err := store.AppendResumed(ctx, id, ev, &MetaUpdate{Status: StatusRunning})
	if err != nil {
		return ResumeResult{}, err
	}
	if prior != nil {
		var p StepResumedPayload
		if err := json.Unmarshal(orNull(prior.Payload), &p); err != nil {
			return ResumeResult{}, err
		}
		return ResumeResult{AlreadyResumed: true, Decision: p.Decision}, nil
	}
	return ResumeResult{Decision: raw}, nil
}
