package loom

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// StepOutcome is the result envelope every step returns. Halt is the
// engine's single piece of control flow (ADR-3): a halting step is a
// completed step, and StepCompleted{Halt} is the sole source of
// terminality.
type StepOutcome struct {
	Output json.RawMessage
	Halt   bool
	Status string
}

// StepFunc is the raw step shape — the escape hatch. The documented default
// is Typed (ADR-13).
type StepFunc func(ctx context.Context, sctx *StepContext, input json.RawMessage) (StepOutcome, error)

// StepContext is Loom's surface inside a step: identity, prior outputs, the
// LLM chain caller. It does not wrap context.Context; cancellation flows
// through the standard first parameter (ADR-13).
type StepContext struct {
	instanceID    string
	step          *stepDef
	attempt       int
	workflowInput json.RawMessage
	outputs       map[string]json.RawMessage
	llm           *ChainCaller
}

func (s *StepContext) InstanceID() string { return s.instanceID }
func (s *StepContext) StepName() string   { return s.step.name }
func (s *StepContext) Attempt() int       { return s.attempt }

// IdempotencyKey derives a stable key from (instanceID, stepName). It is
// deliberately attempt-independent: the key exists so that a re-executed
// attempt deduplicates against the side effect of a crashed one, which an
// attempt-scoped key would defeat. The attempt number is available
// separately via Attempt() for callers that want per-attempt keys.
func (s *StepContext) IdempotencyKey() string {
	return IdempotencyKey(s.instanceID, s.step.name)
}

// IdempotencyKey is the standalone form for use outside a step.
func IdempotencyKey(instanceID, stepName string) string {
	h := sha256.Sum256([]byte("loom\x00" + instanceID + "\x00" + stepName))
	return hex.EncodeToString(h[:16])
}

// Input unmarshals the workflow's creation-time input.
func (s *StepContext) Input(v any) error {
	return json.Unmarshal(orNull(s.workflowInput), v)
}

// Output unmarshals a completed earlier step's output. The read must have
// been declared with .Reads(name) at definition time (ADR-13).
func (s *StepContext) Output(name string, v any) error {
	if !s.step.reads[name] {
		return fmt.Errorf("loom: step %q read output of %q without declaring it: add .Reads(%q) at definition time", s.step.name, name, name)
	}
	raw, ok := s.outputs[name]
	if !ok {
		return fmt.Errorf("loom: no completed output for step %q", name)
	}
	return json.Unmarshal(orNull(raw), v)
}

// LLM returns the fallback-chain caller for LLM steps. The engine wraps the
// call in reserve-then-settle accounting (ADR-6).
func (s *StepContext) LLM() *ChainCaller {
	if s.llm == nil {
		panic(fmt.Sprintf("loom: step %q is not an LLM step; register it with wf.LLMStep to use sctx.LLM()", s.step.name))
	}
	return s.llm
}

type haltError struct{ status string }

func (h *haltError) Error() string { return "loom: workflow halted: " + h.status }

// Halt returns an error a Typed step uses to terminate the workflow with a
// status. The step still completes (its output is persisted); the workflow
// does not continue (ADR-3).
func Halt(status string) error { return &haltError{status: status} }

// Typed wraps a typed step function so step bodies never touch
// json.RawMessage (ADR-13). In is the previous step's output (the
// workflow's input for the first step).
func Typed[In, Out any](fn func(context.Context, *StepContext, In) (Out, error)) StepFunc {
	return func(ctx context.Context, sctx *StepContext, input json.RawMessage) (StepOutcome, error) {
		var in In
		if err := json.Unmarshal(orNull(input), &in); err != nil {
			return StepOutcome{}, Permanent(fmt.Errorf("loom: unmarshal input for step %q: %w", sctx.StepName(), err))
		}
		out, err := fn(ctx, sctx, in)
		var h *haltError
		if errors.As(err, &h) {
			raw, merr := json.Marshal(out)
			if merr != nil {
				return StepOutcome{}, Permanent(merr)
			}
			return StepOutcome{Output: raw, Halt: true, Status: h.status}, nil
		}
		if err != nil {
			return StepOutcome{}, err
		}
		raw, merr := json.Marshal(out)
		if merr != nil {
			return StepOutcome{}, Permanent(merr)
		}
		return StepOutcome{Output: raw}, nil
	}
}

func orNull(r json.RawMessage) json.RawMessage {
	if len(r) == 0 {
		return json.RawMessage("null")
	}
	return r
}

// Retry is per-step declarative policy (ADR-9). Max is the total number of
// recorded failures tolerated before the workflow terminates as failed.
// Crashed attempts (started, never concluded) do not count against Max;
// they are re-executed under at-least-once semantics.
type Retry struct {
	Max     int
	Backoff func(failures int) time.Duration
}

var defaultRetry = Retry{Max: 1, Backoff: noBackoff}

func noBackoff(int) time.Duration { return 0 }

// Exponential doubles the base per prior failure: base, 2*base, 4*base...
func Exponential(base time.Duration) func(int) time.Duration {
	return func(failures int) time.Duration {
		d := base
		for i := 1; i < failures; i++ {
			d *= 2
		}
		return d
	}
}

type permanentError struct{ err error }

func (p *permanentError) Error() string { return p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// Permanent wraps an error to signal the failure is not retryable (ADR-9).
func Permanent(err error) error { return &permanentError{err: err} }

func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}
