// Package sqlite is the embedded-mode StateStore (ADR-3 of the principles,
// ADR-14): pure-Go SQLite via modernc.org/sqlite — no CGO, so
// CGO_ENABLED=0 cross-compiles and scratch containers keep working.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/IbrahimMohammedi/loom"
	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS instances (
  id         TEXT PRIMARY KEY,
  workflow   TEXT NOT NULL,
  epoch      INTEGER NOT NULL DEFAULT 0,
  claimed_by TEXT NOT NULL DEFAULT '',
  status     TEXT NOT NULL DEFAULT 'pending',
  not_before INTEGER,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
  instance_id TEXT NOT NULL,
  seq         INTEGER NOT NULL,
  type        TEXT NOT NULL,
  step        TEXT NOT NULL DEFAULT '',
  attempt     INTEGER NOT NULL DEFAULT 0,
  payload     BLOB,
  ts          INTEGER NOT NULL,
  PRIMARY KEY (instance_id, seq)
);
-- Resume idempotency lives in the store, not executor logic (ADR-7).
CREATE UNIQUE INDEX IF NOT EXISTS resume_once
  ON events(instance_id, step, attempt) WHERE type = 'step_resumed';
`

type Store struct {
	db *sql.DB
}

var _ loom.StateStore = (*Store)(nil)

// Open opens (creating if needed) a Loom state database at path.
func Open(path string) (*Store, error) {
	// _txlock=immediate: write transactions take the write lock up front,
	// avoiding upgrade deadlocks between processes (the zombie test runs
	// two processes against one file on purpose).
	dsn := fmt.Sprintf("file:%s?_txlock=immediate&_pragma=busy_timeout(5000)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) CreateInstance(ctx context.Context, id, workflow string, ev loom.Event) error {
	now := time.Now().UTC().UnixNano()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO instances (id, workflow, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		id, workflow, loom.StatusPending, now, now); err != nil {
		if isUniqueViolation(err) {
			return loom.ErrInstanceExists
		}
		return err
	}
	if err := insertEvent(ctx, tx, id, 1, ev); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Append(ctx context.Context, id string, epoch int64, ev loom.Event, meta *loom.MetaUpdate) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var cur int64
	if err := tx.QueryRowContext(ctx, `SELECT epoch FROM instances WHERE id = ?`, id).Scan(&cur); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return loom.ErrNotFound
		}
		return err
	}
	if cur != epoch {
		return loom.ErrStaleEpoch
	}
	seq, err := nextSeq(ctx, tx, id)
	if err != nil {
		return err
	}
	if err := insertEvent(ctx, tx, id, seq, ev); err != nil {
		return err
	}
	if err := applyMeta(ctx, tx, id, meta); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) AppendResumed(ctx context.Context, id string, ev loom.Event, meta *loom.MetaUpdate) (*loom.Event, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM instances WHERE id = ?`, id).Scan(&exists); err != nil {
		return nil, err
	}
	if exists == 0 {
		return nil, loom.ErrNotFound
	}
	seq, err := nextSeq(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if err := insertEvent(ctx, tx, id, seq, ev); err != nil {
		if isUniqueViolation(err) {
			// First-wins: return the recorded decision, append nothing.
			tx.Rollback()
			prior, perr := s.priorResume(ctx, id, ev.Step, ev.Attempt)
			if perr != nil {
				return nil, perr
			}
			return prior, nil
		}
		return nil, err
	}
	if err := applyMeta(ctx, tx, id, meta); err != nil {
		return nil, err
	}
	return nil, tx.Commit()
}

func (s *Store) priorResume(ctx context.Context, id, step string, attempt int) (*loom.Event, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT seq, type, step, attempt, payload, ts FROM events
		 WHERE instance_id = ? AND type = ? AND step = ? AND attempt = ?`,
		id, string(loom.EvStepResumed), step, attempt)
	ev, err := scanEvent(row)
	if err != nil {
		return nil, err
	}
	return &ev, nil
}

func (s *Store) ClaimInstance(ctx context.Context, id, executor string) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx,
		`UPDATE instances SET epoch = epoch + 1, claimed_by = ?, updated_at = ? WHERE id = ?`,
		executor, time.Now().UTC().UnixNano(), id)
	if err != nil {
		return 0, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return 0, loom.ErrNotFound
	}
	var epoch int64
	if err := tx.QueryRowContext(ctx, `SELECT epoch FROM instances WHERE id = ?`, id).Scan(&epoch); err != nil {
		return 0, err
	}
	return epoch, tx.Commit()
}

func (s *Store) Load(ctx context.Context, id string) ([]loom.Event, error) {
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM instances WHERE id = ?`, id).Scan(&exists); err != nil {
		return nil, err
	}
	if exists == 0 {
		return nil, loom.ErrNotFound
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT seq, type, step, attempt, payload, ts FROM events WHERE instance_id = ? ORDER BY seq`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []loom.Event
	for rows.Next() {
		ev, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

func (s *Store) Meta(ctx context.Context, id string) (loom.InstanceMeta, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, workflow, epoch, claimed_by, status, not_before, created_at, updated_at
		 FROM instances WHERE id = ?`, id)
	m, err := scanMeta(row)
	if errors.Is(err, sql.ErrNoRows) {
		return loom.InstanceMeta{}, loom.ErrNotFound
	}
	return m, err
}

func (s *Store) List(ctx context.Context) ([]loom.InstanceMeta, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, workflow, epoch, claimed_by, status, not_before, created_at, updated_at
		 FROM instances ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []loom.InstanceMeta
	for rows.Next() {
		m, err := scanMeta(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) Runnable(ctx context.Context, now time.Time, limit int) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id FROM instances
		 WHERE status IN (?, ?, ?) AND (not_before IS NULL OR not_before <= ?)
		 ORDER BY updated_at LIMIT ?`,
		loom.StatusPending, loom.StatusRunning, loom.StatusWaitingRetry, now.UTC().UnixNano(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *Store) SetStatus(ctx context.Context, id, status string, notBefore *time.Time) error {
	var nb any
	if notBefore != nil {
		nb = notBefore.UTC().UnixNano()
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE instances SET status = ?, not_before = ?, updated_at = ? WHERE id = ?`,
		status, nb, time.Now().UTC().UnixNano(), id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return loom.ErrNotFound
	}
	return nil
}

func (s *Store) Prune(ctx context.Context, olderThan time.Time) (int, error) {
	metas, err := s.List(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, m := range metas {
		if !loom.IsTerminalStatus(m.Status) || !m.UpdatedAt.Before(olderThan) {
			continue
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return n, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM events WHERE instance_id = ?`, m.ID); err != nil {
			tx.Rollback()
			return n, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM instances WHERE id = ?`, m.ID); err != nil {
			tx.Rollback()
			return n, err
		}
		if err := tx.Commit(); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func nextSeq(ctx context.Context, tx *sql.Tx, id string) (int64, error) {
	var seq int64
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE instance_id = ?`, id).Scan(&seq)
	return seq, err
}

func insertEvent(ctx context.Context, tx *sql.Tx, id string, seq int64, ev loom.Event) error {
	ts := ev.Time
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	_, err := tx.ExecContext(ctx,
		`INSERT INTO events (instance_id, seq, type, step, attempt, payload, ts) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id, seq, string(ev.Type), ev.Step, ev.Attempt, []byte(ev.Payload), ts.UnixNano())
	return err
}

func applyMeta(ctx context.Context, tx *sql.Tx, id string, meta *loom.MetaUpdate) error {
	now := time.Now().UTC().UnixNano()
	if meta == nil {
		_, err := tx.ExecContext(ctx, `UPDATE instances SET updated_at = ? WHERE id = ?`, now, id)
		return err
	}
	var nb any
	if meta.NotBefore != nil {
		nb = meta.NotBefore.UTC().UnixNano()
	}
	if meta.Status != "" {
		_, err := tx.ExecContext(ctx,
			`UPDATE instances SET status = ?, not_before = ?, updated_at = ? WHERE id = ?`,
			meta.Status, nb, now, id)
		return err
	}
	_, err := tx.ExecContext(ctx,
		`UPDATE instances SET not_before = ?, updated_at = ? WHERE id = ?`, nb, now, id)
	return err
}

type scannable interface{ Scan(dest ...any) error }

func scanEvent(row scannable) (loom.Event, error) {
	var ev loom.Event
	var typ string
	var payload []byte
	var ts int64
	if err := row.Scan(&ev.Seq, &typ, &ev.Step, &ev.Attempt, &payload, &ts); err != nil {
		return ev, err
	}
	ev.Type = loom.EventType(typ)
	ev.Payload = payload
	ev.Time = time.Unix(0, ts).UTC()
	return ev, nil
}

func scanMeta(row scannable) (loom.InstanceMeta, error) {
	var m loom.InstanceMeta
	var nb sql.NullInt64
	var created, updated int64
	if err := row.Scan(&m.ID, &m.Workflow, &m.Epoch, &m.ClaimedBy, &m.Status, &nb, &created, &updated); err != nil {
		return m, err
	}
	if nb.Valid {
		m.NotBefore = time.Unix(0, nb.Int64).UTC()
	}
	m.CreatedAt = time.Unix(0, created).UTC()
	m.UpdatedAt = time.Unix(0, updated).UTC()
	return m, nil
}

func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(strings.ToUpper(err.Error()), "UNIQUE")
}
