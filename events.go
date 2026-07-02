package loom

import (
	"encoding/json"
	"fmt"
	"time"
)

// Cost is money in nano-dollars (1e-9 USD). Per-token prices for cheap
// models are fractional micro-dollars, so the unit must be finer than the
// smallest price anyone quotes: $0.25 per 1M tokens == Cost(250) per token.
type Cost int64

// Dollars converts a dollar amount to Cost.
func Dollars(f float64) Cost { return Cost(f * 1e9) }

func (c Cost) Dollars() float64 { return float64(c) / 1e9 }
func (c Cost) String() string   { return fmt.Sprintf("$%.6f", c.Dollars()) }

type EventType string

const (
	EvInstanceCreated EventType = "instance_created"
	EvStepStarted     EventType = "step_started"
	EvStepCompleted   EventType = "step_completed"
	EvStepFailed      EventType = "step_failed"
	EvStepSuspended   EventType = "step_suspended"
	EvStepResumed     EventType = "step_resumed"
	EvBudgetReserved  EventType = "budget_reserved"
	EvBudgetSettled   EventType = "budget_settled"
)

// Event is one entry in an instance's append-only log (ADR-1). Terminality,
// budget state, and progress are all fold-computed from events; there is no
// separate authoritative state.
type Event struct {
	Seq     int64           `json:"seq"`
	Type    EventType       `json:"type"`
	Step    string          `json:"step,omitempty"`
	Attempt int             `json:"attempt,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Time    time.Time       `json:"time"`
}

// InstanceCreatedPayload freezes the creation-time shape of the workflow:
// step names for anchoring and per-suffix hashes for compatibility (ADR-5).
type InstanceCreatedPayload struct {
	Workflow     string          `json:"workflow"`
	Input        json.RawMessage `json:"input,omitempty"`
	StepNames    []string        `json:"step_names"`
	SuffixHashes []string        `json:"suffix_hashes"`
	Budget       Cost            `json:"budget_nanousd,omitempty"`
}

// StepCompletedPayload with Halt set is the sole source of terminality for
// halted workflows (ADR-3): there is no WorkflowTerminated event.
type StepCompletedPayload struct {
	Output json.RawMessage `json:"output,omitempty"`
	Halt   bool            `json:"halt,omitempty"`
	Status string          `json:"status,omitempty"`
}

type StepFailedPayload struct {
	Error     string `json:"error"`
	Permanent bool   `json:"permanent,omitempty"`
	// Reason distinguishes engine-manufactured permanent failures; the only
	// MVP value is "budget_exhausted".
	Reason string `json:"reason,omitempty"`
}

type StepResumedPayload struct {
	Decision json.RawMessage `json:"decision"`
}

type BudgetReservedPayload struct {
	Amount Cost   `json:"amount_nanousd"`
	Reason string `json:"reason,omitempty"`
}

type BudgetSettledPayload struct {
	Amount Cost   `json:"amount_nanousd"`
	Reason string `json:"reason"` // "actual" or "crash-orphaned"
}

func mustJSON(v any) json.RawMessage {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("loom: marshal event payload: %v", err))
	}
	return b
}

func newEvent(t EventType, step string, attempt int, payload any) Event {
	return Event{Type: t, Step: step, Attempt: attempt, Payload: mustJSON(payload), Time: time.Now().UTC()}
}
