package loom

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

type stepKind string

const (
	kindFunc     stepKind = "step"
	kindLLM      stepKind = "llm"
	kindApproval stepKind = "approval"
)

type stepDef struct {
	name     string
	kind     stepKind
	fn       StepFunc
	reads    map[string]bool
	retry    Retry
	chain    []ModelSpec
	finalize func(decision json.RawMessage) (StepOutcome, error)
}

// Workflow is the definition builder. Validation errors accumulate and are
// returned by Build, so a misdeclared workflow fails at startup (ADR-13).
type Workflow struct {
	name  string
	steps []*stepDef
	errs  []error
}

func NewWorkflow(name string) *Workflow { return &Workflow{name: name} }

type StepBuilder struct {
	w *Workflow
	s *stepDef
}

func (w *Workflow) add(s *stepDef) *StepBuilder {
	if s.name == "" {
		w.errs = append(w.errs, fmt.Errorf("loom: workflow %q: step name must not be empty", w.name))
	}
	for _, ex := range w.steps {
		if ex.name == s.name {
			// Uniqueness is load-bearing: (stepName, attempt) identity,
			// resume idempotency, and suffix anchoring all assume it (ADR-4).
			w.errs = append(w.errs, fmt.Errorf("loom: workflow %q: duplicate step name %q: step names must be unique within a definition", w.name, s.name))
		}
	}
	w.steps = append(w.steps, s)
	return &StepBuilder{w: w, s: s}
}

func (w *Workflow) Step(name string, fn StepFunc) *StepBuilder {
	return w.add(&stepDef{name: name, kind: kindFunc, fn: fn, retry: defaultRetry, reads: map[string]bool{}})
}

// LLMStep registers a step whose sctx.LLM() calls run against a fallback
// chain, guarded by the budget ledger (ADR-6). The reservation is the sum
// of per-hop maxes: failed hops are not free.
func (w *Workflow) LLMStep(name string, chain []ModelSpec, fn StepFunc) *StepBuilder {
	if len(chain) == 0 {
		w.errs = append(w.errs, fmt.Errorf("loom: workflow %q: llm step %q: model chain must not be empty", w.name, name))
	}
	return w.add(&stepDef{name: name, kind: kindLLM, fn: fn, chain: chain, retry: defaultRetry, reads: map[string]bool{}})
}

// Approval registers a human-gate step (ADR-10). Executing it suspends the
// instance; Resume records the decision; the default finalize halts with
// status "rejected" when the decision is not approved.
func (w *Workflow) Approval(name string) *StepBuilder {
	return w.add(&stepDef{name: name, kind: kindApproval, retry: defaultRetry, reads: map[string]bool{}, finalize: defaultFinalize})
}

// Reads declares which earlier steps' outputs this step may access via
// sctx.Output (ADR-13). Undeclared reads fail at runtime; declared reads of
// nonexistent earlier steps fail here, at definition time.
func (b *StepBuilder) Reads(names ...string) *StepBuilder {
	for _, n := range names {
		found := false
		for _, s := range b.w.steps {
			if s == b.s {
				break
			}
			if s.name == n {
				found = true
				break
			}
		}
		if !found {
			b.w.errs = append(b.w.errs, fmt.Errorf("loom: step %q declares Reads(%q), but no earlier step has that name", b.s.name, n))
		}
		b.s.reads[n] = true
	}
	return b
}

func (b *StepBuilder) Retry(r Retry) *StepBuilder {
	if r.Max < 1 {
		r.Max = 1
	}
	if r.Backoff == nil {
		r.Backoff = noBackoff
	}
	b.s.retry = r
	return b
}

// Definition is a validated, immutable workflow with precomputed suffix
// hashes (ADR-5).
type Definition struct {
	name         string
	steps        []*stepDef
	index        map[string]int
	stepNames    []string
	suffixHashes []string
}

func (w *Workflow) Build() (*Definition, error) {
	if len(w.steps) == 0 {
		w.errs = append(w.errs, fmt.Errorf("loom: workflow %q has no steps", w.name))
	}
	if len(w.errs) > 0 {
		return nil, errors.Join(w.errs...)
	}
	d := &Definition{name: w.name, steps: w.steps, index: map[string]int{}}
	for i, s := range w.steps {
		d.index[s.name] = i
		d.stepNames = append(d.stepNames, s.name)
	}
	d.suffixHashes = suffixHashes(w.steps)
	return d, nil
}

// suffixHashes[i] covers step names and types from i to the end — a rolling
// hash, one reverse pass (ADR-5). Names and types only: step bodies are
// invisible by design.
func suffixHashes(steps []*stepDef) []string {
	out := make([]string, len(steps))
	prev := ""
	for i := len(steps) - 1; i >= 0; i-- {
		h := sha256.Sum256([]byte(steps[i].name + "\x00" + string(steps[i].kind) + "\x00" + prev))
		prev = hex.EncodeToString(h[:])
		out[i] = prev
	}
	return out
}

func (d *Definition) Name() string { return d.name }

// StepInfo is the read-only view consumed by the replay harness and the
// inspector.
type StepInfo struct {
	Name  string
	Kind  string
	Reads []string
}

func (d *Definition) Steps() []StepInfo {
	out := make([]StepInfo, 0, len(d.steps))
	for _, s := range d.steps {
		info := StepInfo{Name: s.name, Kind: string(s.kind)}
		for r := range s.reads {
			info.Reads = append(info.Reads, r)
		}
		out = append(out, info)
	}
	return out
}
