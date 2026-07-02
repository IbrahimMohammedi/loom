// Package replay is Loom's re-execution replay harness (ADR-11).
//
// It re-executes a recorded instance log against current code, with two
// rules: side effects come from mocks (build the Definition with mock
// dependencies; the LLM client is supplied via WithLLM), and recorded
// external inputs — approval decisions — replay from the log, never from
// mocks and never by blocking on a human.
//
// The determinism claim, scoped honestly: the fold is deterministic; step
// re-execution is deterministic only if your steps take side effects
// through injectable dependencies.
package replay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/IbrahimMohammedi/loom"
	"github.com/IbrahimMohammedi/loom/store/memory"
)

// Comparator reports whether a replayed output matches a recorded one.
type Comparator func(recorded, replayed json.RawMessage) bool

// ExactBytes is the default comparator (after JSON compaction).
func ExactBytes(recorded, replayed json.RawMessage) bool {
	return bytes.Equal(compact(recorded), compact(replayed))
}

// Structural compares JSON shape — types and object keys — not content.
// It is the default for LLM steps: recorded LLM output never byte-matches
// a re-execution, and what replay regression-tests is everything around
// the LLM call, not the model.
func Structural(recorded, replayed json.RawMessage) bool {
	var a, b any
	if json.Unmarshal(orNull(recorded), &a) != nil || json.Unmarshal(orNull(replayed), &b) != nil {
		return false
	}
	return sameShape(a, b)
}

func sameShape(a, b any) bool {
	switch av := a.(type) {
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			w, ok := bv[k]
			if !ok || !sameShape(v, w) {
				return false
			}
		}
		return true
	case []any:
		_, ok := b.([]any)
		return ok
	case string:
		_, ok := b.(string)
		return ok
	case float64:
		_, ok := b.(float64)
		return ok
	case bool:
		_, ok := b.(bool)
		return ok
	case nil:
		return b == nil
	}
	return false
}

// StepComparison is one step's recorded-vs-replayed verdict. After the
// first divergence at step N, step N+1's replayed input differs from its
// recorded input, so downstream comparisons compare trajectories, not
// code: they are marked Advisory and are not findings.
type StepComparison struct {
	Step     string
	Match    bool
	Advisory bool
	Recorded json.RawMessage
	Replayed json.RawMessage
}

// Result is the harness's report. DivergedAt is authoritative; StoppedAt
// (reason awaiting-unrecorded-input) is distinct from divergence — the code
// under test didn't fail; reality never supplied the input.
type Result struct {
	Diverged   bool
	DivergedAt string
	StoppedAt  string // step name; "" unless stopped awaiting unrecorded input
	StopReason string
	Steps      []StepComparison
	// FinalStatus is the replayed run's terminal (or resting) status.
	FinalStatus string
	// RecordedStatus is where the recorded log's fold rests under the
	// same definition.
	RecordedStatus string
	Events         []loom.Event // the replayed log
}

type Harness struct {
	recorded    []loom.Event
	llm         loom.LLMClient
	truncStep   string
	truncAtt    int
	failStep    map[string]error
	failFired   map[string]bool
	decisions   map[string]json.RawMessage
	comparators map[string]Comparator
}

type Option func(*Harness)

// WithLLM supplies the mocked LLM client for LLM steps. Non-LLM side
// effects are mocked by building the Definition with mock dependencies.
func WithLLM(c loom.LLMClient) Option { return func(h *Harness) { h.llm = c } }

// TruncateAt cuts the log immediately before the given attempt of the
// given step and re-executes forward — a simulated crash at that point.
// It tests resume-from-checkpoint (the log surface).
func TruncateAt(step string, attempt int) Option {
	return func(h *Harness) { h.truncStep, h.truncAtt = step, attempt }
}

// InjectFailure makes the named step fail with err on the first attempt
// the replay executes — the dependency surface: it tests the step's error
// handling and the engine's retry/halt behavior. Wrap with loom.Permanent
// to inject a non-retryable failure.
func InjectFailure(step string, err error) Option {
	return func(h *Harness) { h.failStep[step] = err }
}

// WithResumeDecision scripts a hypothetical external input for a
// suspension the recorded log never resumed — the what-if tool. Recorded
// decisions always take precedence.
func WithResumeDecision(step string, decision any) Option {
	return func(h *Harness) { h.decisions[step] = mustMarshal(decision) }
}

// WithComparator overrides the comparator for one step.
func WithComparator(step string, c Comparator) Option {
	return func(h *Harness) { h.comparators[step] = c }
}

func New(recorded []loom.Event, opts ...Option) *Harness {
	h := &Harness{
		recorded:    recorded,
		failStep:    map[string]error{},
		failFired:   map[string]bool{},
		decisions:   map[string]json.RawMessage{},
		comparators: map[string]Comparator{},
	}
	for _, o := range opts {
		o(h)
	}
	return h
}

// Run re-executes the (possibly truncated) log against def and compares
// the replayed trajectory to the recorded one.
func (h *Harness) Run(def *loom.Definition) (*Result, error) {
	if len(h.recorded) == 0 || h.recorded[0].Type != loom.EvInstanceCreated {
		return nil, fmt.Errorf("replay: recorded log must begin with instance_created")
	}
	log := h.truncate()
	store := memory.New()
	store.Seed("replay", def.Name(), loom.StatusRunning, log)

	recordedResumes := map[string]json.RawMessage{}
	for _, ev := range h.recorded {
		if ev.Type == loom.EvStepResumed {
			var p loom.StepResumedPayload
			if err := json.Unmarshal(orNull(ev.Payload), &p); err != nil {
				return nil, err
			}
			recordedResumes[ev.Step] = p.Decision
		}
	}

	hooks := &loom.DriveHooks{
		BeforeStep: func(step string, attempt int) error {
			if err, ok := h.failStep[step]; ok && !h.failFired[step] {
				h.failFired[step] = true
				return err
			}
			return nil
		},
		ResumeDecision: func(step string) (json.RawMessage, bool) {
			if d, ok := recordedResumes[step]; ok {
				return d, true
			}
			if d, ok := h.decisions[step]; ok {
				return d, true
			}
			return nil, false
		},
		// Replay never waits out a backoff: recorded and injected failures
		// would otherwise park the run in waiting_retry. The fold's
		// notBefore math is exercised by its own unit tests.
		Now: func() time.Time { return time.Now().Add(24 * 365 * time.Hour) },
	}

	err := loom.Drive(context.Background(), loom.DriveOptions{
		Store:       store,
		Definitions: map[string]*loom.Definition{def.Name(): def},
		LLM:         h.llm,
		ExecutorID:  "replay-harness",
		Logger:      slog.New(slog.DiscardHandler),
		Hooks:       hooks,
	}, "replay")
	if err != nil {
		return nil, fmt.Errorf("replay: drive: %w", err)
	}

	replayed, err := store.Load(context.Background(), "replay")
	if err != nil {
		return nil, err
	}
	meta, err := store.Meta(context.Background(), "replay")
	if err != nil {
		return nil, err
	}
	res := &Result{FinalStatus: meta.Status, Events: replayed}
	if meta.Status == loom.StatusSuspended {
		if p := lastSuspended(replayed); p != "" {
			res.StoppedAt = p
			res.StopReason = "awaiting-unrecorded-input"
		}
	}
	if _, rp, ferr := loom.Fold(def, h.recorded); ferr == nil && rp.Kind == loom.PrescribeDone {
		res.RecordedStatus = rp.Status
	}
	h.compare(def, res, replayed)
	return res, nil
}

func (h *Harness) compare(def *loom.Definition, res *Result, replayed []loom.Event) {
	rec := outputsOf(h.recorded)
	rep := outputsOf(replayed)
	diverged := false
	for _, info := range def.Steps() {
		r, rok := rec[info.Name]
		p, pok := rep[info.Name]
		if !rok || !pok {
			continue
		}
		cmp := h.comparators[info.Name]
		if cmp == nil {
			if info.Kind == "llm" {
				cmp = Structural
			} else {
				cmp = ExactBytes
			}
		}
		match := cmp(r, p)
		sc := StepComparison{Step: info.Name, Match: match, Advisory: diverged, Recorded: r, Replayed: p}
		res.Steps = append(res.Steps, sc)
		if !match && !diverged {
			diverged = true
			res.Diverged = true
			res.DivergedAt = info.Name
		}
	}
}

// truncate keeps events strictly before the first event of the target
// (step, attempt); the InstanceCreated event always survives.
func (h *Harness) truncate() []loom.Event {
	if h.truncStep == "" {
		return h.recorded
	}
	for i, ev := range h.recorded {
		if i == 0 {
			continue
		}
		if ev.Step == h.truncStep && ev.Attempt >= h.truncAtt {
			return h.recorded[:i]
		}
	}
	return h.recorded
}

func outputsOf(events []loom.Event) map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	for _, ev := range events {
		if ev.Type == loom.EvStepCompleted {
			var p loom.StepCompletedPayload
			if json.Unmarshal(orNull(ev.Payload), &p) == nil {
				out[ev.Step] = p.Output
			}
		}
	}
	return out
}

func lastSuspended(events []loom.Event) string {
	step := ""
	resumed := map[string]bool{}
	for _, ev := range events {
		switch ev.Type {
		case loom.EvStepSuspended:
			step = ev.Step
		case loom.EvStepResumed:
			resumed[ev.Step] = true
		}
	}
	if step != "" && !resumed[step] {
		return step
	}
	return ""
}

func compact(r json.RawMessage) []byte {
	var buf bytes.Buffer
	if err := json.Compact(&buf, orNull(r)); err != nil {
		return r
	}
	return buf.Bytes()
}

func orNull(r json.RawMessage) json.RawMessage {
	if len(r) == 0 {
		return json.RawMessage("null")
	}
	return r
}

func mustMarshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
