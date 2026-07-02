package loom

import (
	"context"
	"errors"
	"time"
)

var (
	ErrStaleEpoch     = errors.New("loom: append rejected: stale epoch")
	ErrInstanceExists = errors.New("loom: instance already exists")
	ErrNotFound       = errors.New("loom: instance not found")
)

// Status cache values. The cache is derived from the fold and rebuildable;
// the event log is the only truth. Terminal statuses are the ADR-3 set:
// StatusCompleted, StatusFailed, StatusBudgetExhausted, plus any
// halt-with-status value (e.g. "rejected").
const (
	StatusPending         = "pending"
	StatusRunning         = "running"
	StatusWaitingRetry    = "waiting_retry"
	StatusSuspended       = "suspended"
	StatusIncompatible    = "incompatible"
	StatusCompleted       = "completed"
	StatusFailed          = "failed"
	StatusBudgetExhausted = "budget_exhausted"
)

// IsTerminalStatus reports whether a status is in the ADR-3 terminal set.
// Halt-with-status values are user-defined, so membership is defined by
// exclusion from the (closed) non-terminal set.
func IsTerminalStatus(s string) bool {
	switch s {
	case StatusPending, StatusRunning, StatusWaitingRetry, StatusSuspended, StatusIncompatible, "":
		return false
	}
	return true
}

type InstanceMeta struct {
	ID        string
	Workflow  string
	Epoch     int64
	ClaimedBy string
	Status    string
	NotBefore time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}

// MetaUpdate piggybacks a status-cache update on an append so the cache and
// the event land together. The cache is an index for Runnable and the
// inspector, never an input to the fold.
type MetaUpdate struct {
	Status    string
	NotBefore *time.Time
}

// StateStore is the durability contract. Every concurrency invariant in
// Loom is enforced at the append (ADR-7), because the append is the only
// serialization point that exists in all deployment modes.
type StateStore interface {
	// CreateInstance is a constrained append (ADR-7): unique on instance ID.
	// The event must be the InstanceCreated event with Seq 1. Returns
	// ErrInstanceExists on duplicate submission.
	CreateInstance(ctx context.Context, id, workflow string, ev Event) error

	// Append is a fenced append (ADR-7): rejected with ErrStaleEpoch unless
	// epoch equals the instance's current epoch. Seq is assigned by the
	// store. meta, if non-nil, updates the status cache in the same
	// transaction.
	Append(ctx context.Context, id string, epoch int64, ev Event, meta *MetaUpdate) error

	// AppendResumed is a constrained append (ADR-7): unique on
	// (instance, step, attempt, type=step_resumed). If the event already
	// exists, nothing is appended, meta is not applied, and the prior event
	// is returned (first-wins).
	AppendResumed(ctx context.Context, id string, ev Event, meta *MetaUpdate) (prior *Event, err error)

	// ClaimInstance grants ownership unconditionally and increments the
	// instance epoch (ADR-8). Appends bearing a stale epoch are rejected
	// (safety). The store does NOT arbitrate between live claimants; callers
	// must ensure one claimant per instance (liveness). In embedded mode the
	// process is the arbiter; in distributed mode the dispatcher is.
	ClaimInstance(ctx context.Context, id, executor string) (int64, error)

	// Load returns the full event log in Seq order.
	Load(ctx context.Context, id string) ([]Event, error)

	Meta(ctx context.Context, id string) (InstanceMeta, error)
	List(ctx context.Context) ([]InstanceMeta, error)

	// Runnable returns instance IDs whose cached status indicates
	// prescribable work: pending, running, or waiting_retry with NotBefore
	// elapsed. The log is the queue (ADR-16); this is its index.
	Runnable(ctx context.Context, now time.Time, limit int) ([]string, error)

	// SetStatus updates only the derived status cache — no event, no epoch.
	// Used for fold-computed terminal statuses, suspension, backoff waits,
	// and incompatibility (which is a relationship between the log and the
	// currently registered code, not a property of the log, so it can never
	// be an event).
	SetStatus(ctx context.Context, id, status string, notBefore *time.Time) error

	// Prune deletes terminal instances (ADR-17) last updated before
	// olderThan and returns how many were deleted. It appends nothing.
	Prune(ctx context.Context, olderThan time.Time) (int, error)
}
