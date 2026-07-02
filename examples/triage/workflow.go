package main

import (
	"context"
	"fmt"
	"os"

	"github.com/IbrahimMohammedi/loom"
)

type Ticket struct {
	From string `json:"from"`
	Body string `json:"body"`
}

type Classification struct {
	Category string `json:"category"`
}

type TicketContext struct {
	Category string `json:"category"`
	KB       string `json:"kb"`
}

type Draft struct {
	Body string `json:"body"`
}

// BuildTriage assembles the flagship pipeline:
// classify -> fetch-context -> draft (LLM, budget-guarded) -> gate -> send.
// crashAt hard-exits the process inside the named step, after its LLM call
// returned and before its result appends — deliberately the worst window.
func BuildTriage(kb MockKB, mailer *MockMailer, crashAt string) (*loom.Definition, error) {
	wf := loom.NewWorkflow("triage")

	wf.Step("classify", loom.Typed(func(ctx context.Context, sctx *loom.StepContext, t Ticket) (Classification, error) {
		cat := "general"
		switch {
		case contains(t.Body, "refund", "charge", "invoice"):
			cat = "billing"
		case contains(t.Body, "crash", "error", "bug"):
			cat = "technical"
		}
		return Classification{Category: cat}, nil
	}))

	wf.Step("fetch-context", loom.Typed(func(ctx context.Context, sctx *loom.StepContext, c Classification) (TicketContext, error) {
		return TicketContext{Category: c.Category, KB: kb.Lookup(c.Category)}, nil
	}))

	wf.LLMStep("draft", Chain, loom.Typed(func(ctx context.Context, sctx *loom.StepContext, tc TicketContext) (Draft, error) {
		prompt := fmt.Sprintf("Draft a support reply.\ncategory: %s\ncontext: %s", tc.Category, tc.KB)
		resp, err := sctx.LLM().Complete(ctx, prompt)
		if err != nil {
			return Draft{}, err
		}
		if crashAt == "draft" {
			// The manufactured crash: tokens are spent, money is gone, and
			// this process dies before the result persists. Recovery must
			// settle the orphaned reservation at reserved max (ADR-6).
			fmt.Println("CRASH: exiting hard inside draft, after the LLM call, before the append")
			os.Exit(1)
		}
		return Draft{Body: resp.Text}, nil
	}))

	wf.Approval("gate")

	wf.Step("send", loom.Typed(func(ctx context.Context, sctx *loom.StepContext, d loom.ApprovalDecision) (string, error) {
		var draft Draft
		if err := sctx.Output("draft", &draft); err != nil {
			return "", err
		}
		var ticket Ticket
		if err := sctx.Input(&ticket); err != nil {
			return "", err
		}
		if err := mailer.Send(sctx.IdempotencyKey(), ticket.From, draft.Body); err != nil {
			return "", err
		}
		return "sent", nil
	})).Reads("draft")

	return wf.Build()
}

func contains(s string, words ...string) bool {
	for _, w := range words {
		if len(s) >= len(w) && indexFold(s, w) {
			return true
		}
	}
	return false
}

func indexFold(s, sub string) bool {
	lower := func(b byte) byte {
		if 'A' <= b && b <= 'Z' {
			return b + 32
		}
		return b
	}
outer:
	for i := 0; i+len(sub) <= len(s); i++ {
		for j := 0; j < len(sub); j++ {
			if lower(s[i+j]) != lower(sub[j]) {
				continue outer
			}
		}
		return true
	}
	return false
}
