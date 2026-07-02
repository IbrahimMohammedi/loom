package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/IbrahimMohammedi/loom"
)

// liveClient proves the LLMClient seam is real, not a demo-only fiction:
// set LOOM_DEMO_LIVE=1 and ANTHROPIC_API_KEY to run the same pipeline
// against a real model through the same interface the mock implements.
// Deliberately minimal (net/http only): the library ships no provider SDK.
type liveClient struct {
	key   string
	model string
}

func newLiveClient() (loom.LLMClient, bool) {
	if os.Getenv("LOOM_DEMO_LIVE") != "1" {
		return nil, false
	}
	key := os.Getenv("ANTHROPIC_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "LOOM_DEMO_LIVE=1 but ANTHROPIC_API_KEY is unset; falling back to mock")
		return nil, false
	}
	return &liveClient{key: key, model: "claude-haiku-4-5-20251001"}, true
}

func (c *liveClient) Complete(ctx context.Context, req loom.LLMRequest) (loom.LLMResponse, error) {
	body, err := json.Marshal(map[string]any{
		"model":      c.model,
		"max_tokens": req.MaxOutputTokens,
		"messages":   []map[string]string{{"role": "user", "content": req.Prompt}},
	})
	if err != nil {
		return loom.LLMResponse{}, err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.anthropic.com/v1/messages", bytes.NewReader(body))
	if err != nil {
		return loom.LLMResponse{}, err
	}
	hreq.Header.Set("content-type", "application/json")
	hreq.Header.Set("x-api-key", c.key)
	hreq.Header.Set("anthropic-version", "2023-06-01")
	resp, err := http.DefaultClient.Do(hreq)
	if err != nil {
		return loom.LLMResponse{}, err
	}
	defer resp.Body.Close()
	var out struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
		Usage struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return loom.LLMResponse{}, err
	}
	if out.Error != nil || resp.StatusCode != http.StatusOK {
		msg := resp.Status
		if out.Error != nil {
			msg = out.Error.Message
		}
		return loom.LLMResponse{Model: req.Model, InputTokens: out.Usage.InputTokens}, fmt.Errorf("anthropic api: %s", msg)
	}
	text := ""
	if len(out.Content) > 0 {
		text = out.Content[0].Text
	}
	return loom.LLMResponse{
		Model: req.Model, Text: text,
		InputTokens: out.Usage.InputTokens, OutputTokens: out.Usage.OutputTokens,
	}, nil
}
