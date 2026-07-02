// Command triage is Loom's flagship demo: a support-ticket pipeline
// (classify -> fetch-context -> LLM draft with budget guard -> approval
// gate -> send) against a SQLite file, with a --crash-at flag that
// hard-exits mid-step so the recovery transcript can demonstrate itself.
//
// Usage:
//
//	triage -db state.db run [-id t1] [-budget 1.0] [-crash-at draft] [-linger 5s] [-fail-large]
//	triage -db state.db approve -id t1
//	triage -db state.db reject  -id t1
//	triage -db state.db timeline -id t1
//
// Exit codes: 0 ok (terminal, suspended, or waiting), 1 error,
// 3 stale epoch (this process's claim was usurped — the fencing working).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/IbrahimMohammedi/loom"
	"github.com/IbrahimMohammedi/loom/store/sqlite"
)

func main() {
	fs := flag.NewFlagSet("triage", flag.ExitOnError)
	db := fs.String("db", "triage.db", "path to the SQLite state file")
	id := fs.String("id", "t1", "instance id")
	budget := fs.Float64("budget", 1.0, "workflow budget in dollars")
	crashAt := fs.String("crash-at", "", "hard-exit inside this step (after its LLM call, before its append)")
	linger := fs.Duration("linger", 0, "hold the mock LLM call open this long (widens the crash window for tests)")
	failLarge := fs.Bool("fail-large", false, "simulate the primary model failing with a 529 (falls back to quill-mini)")

	args := os.Args[1:]
	cmd := "run"
	// Accept the command anywhere: flags then command, or command then flags.
	var rest []string
	for _, a := range args {
		switch a {
		case "run", "approve", "reject", "timeline":
			cmd = a
		default:
			rest = append(rest, a)
		}
	}
	if err := fs.Parse(rest); err != nil {
		os.Exit(1)
	}

	if err := realMain(cmd, *db, *id, *budget, *crashAt, *linger, *failLarge); err != nil {
		if errors.Is(err, loom.ErrStaleEpoch) {
			fmt.Fprintf(os.Stderr, "stale epoch: another process claimed instance %s; this process's appends were rejected (fencing working as designed)\n", *id)
			os.Exit(3)
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func realMain(cmd, db, id string, budget float64, crashAt string, linger time.Duration, failLarge bool) error {
	ctx := context.Background()
	store, err := sqlite.Open(db)
	if err != nil {
		return err
	}
	defer store.Close()

	mailer := NewMockMailer()
	def, err := BuildTriage(MockKB{}, mailer, crashAt)
	if err != nil {
		return err
	}

	var llm loom.LLMClient = &MockLLM{Linger: linger, FailLarge: failLarge}
	if live, ok := newLiveClient(); ok {
		fmt.Println("LOOM_DEMO_LIVE=1: using a real model through the same LLMClient seam the mock implements")
		llm = live
	}

	eng, err := loom.NewEngine(loom.Config{Store: store, LLM: llm, ExecutorID: fmt.Sprintf("triage-pid%d", os.Getpid())})
	if err != nil {
		return err
	}
	eng.Register(def)

	switch cmd {
	case "approve", "reject":
		res, err := eng.Resume(ctx, id, loom.ApprovalDecision{Approved: cmd == "approve", Note: "via CLI"})
		if err != nil {
			return err
		}
		if res.AlreadyResumed {
			fmt.Printf("already decided: %s (first decision wins)\n", res.Decision)
			return nil
		}
		fmt.Printf("recorded decision: %s\n", res.Decision)
		fmt.Println("run the workflow again to continue:  triage -db", db, "-id", id, "run")
		return nil

	case "timeline":
		return printTimeline(ctx, store, def, id)

	case "run":
		ticket := Ticket{From: "customer@example.com", Body: "I was double-charged on my last invoice, please refund the duplicate."}
		err := eng.StartInstance(ctx, "triage", id, ticket, loom.Dollars(budget))
		switch {
		case errors.Is(err, loom.ErrInstanceExists):
			fmt.Printf("instance %s exists: resuming from its log (recovery is the same code path as normal execution)\n", id)
		case err != nil:
			return err
		default:
			fmt.Printf("instance %s created (budget %s)\n", id, loom.Dollars(budget))
		}

		before := completedSteps(ctx, store, id)
		if err := eng.RunInstance(id); err != nil {
			return err
		}
		return printRun(ctx, store, def, id, before)
	}
	return fmt.Errorf("unknown command %q", cmd)
}

func completedSteps(ctx context.Context, store loom.StateStore, id string) map[string]bool {
	done := map[string]bool{}
	events, err := store.Load(ctx, id)
	if err != nil {
		return done
	}
	for _, ev := range events {
		if ev.Type == loom.EvStepCompleted {
			done[ev.Step] = true
		}
	}
	return done
}

func printRun(ctx context.Context, store loom.StateStore, def *loom.Definition, id string, before map[string]bool) error {
	events, err := store.Load(ctx, id)
	if err != nil {
		return err
	}
	meta, err := store.Meta(ctx, id)
	if err != nil {
		return err
	}
	done := map[string]bool{}
	var spent loom.Cost
	fmt.Println()
	for _, ev := range events {
		switch ev.Type {
		case loom.EvStepCompleted:
			if !done[ev.Step] {
				done[ev.Step] = true
				if before[ev.Step] {
					fmt.Printf("  ✓ %-14s recovered from log (not re-executed)\n", ev.Step)
				} else {
					fmt.Printf("  ✓ %-14s completed\n", ev.Step)
				}
			}
		case loom.EvBudgetSettled:
			var p loom.BudgetSettledPayload
			if json.Unmarshal(ev.Payload, &p) == nil {
				spent += p.Amount
				if p.Reason == "crash-orphaned" {
					fmt.Printf("  $ %-14s orphaned reservation settled at reserved max: %s (crash-orphaned)\n", ev.Step, p.Amount)
				} else {
					fmt.Printf("  $ %-14s settled at actual: %s\n", ev.Step, p.Amount)
				}
			}
		case loom.EvStepSuspended:
			fmt.Printf("  ⏸ %-14s suspended, awaiting approval\n", ev.Step)
		}
	}
	fmt.Printf("\n  status: %s   recorded spend: %s\n", meta.Status, spent)
	if meta.Status == loom.StatusSuspended {
		fmt.Printf("  next:   triage -id %s approve   (or reject)\n", id)
	}
	return nil
}

func printTimeline(ctx context.Context, store loom.StateStore, def *loom.Definition, id string) error {
	events, err := store.Load(ctx, id)
	if err != nil {
		return err
	}
	for _, ev := range events {
		fmt.Printf("%4d  %-18s %-14s attempt=%d  %s\n", ev.Seq, ev.Type, ev.Step, ev.Attempt, ev.Time.Format(time.RFC3339))
	}
	meta, err := store.Meta(ctx, id)
	if err != nil {
		return err
	}
	fmt.Printf("status=%s epoch=%d claimed_by=%s\n", meta.Status, meta.Epoch, meta.ClaimedBy)
	return nil
}
