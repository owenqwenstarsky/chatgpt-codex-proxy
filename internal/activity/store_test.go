package activity

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStoreLifecyclePublishesAndPersistsRedactedRecord(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	snapshot, events, cancel := store.Subscribe()
	defer cancel()
	if len(snapshot.Requests) != 0 {
		t.Fatalf("initial requests = %d, want 0", len(snapshot.Requests))
	}

	store.Start("req_1", "/v1/responses")
	store.Update("req_1", func(record *Record) {
		record.Model = "gpt-6-astra"
		record.AccountID = "acct_1"
		record.AccountLabel = "primary"
		record.Phase = PhaseStreaming
	})
	now = now.Add(1250 * time.Millisecond)
	if err := store.Finish("req_1", "/v1/responses", 502, OutcomeFailed, "upstream_error", "Bearer top-secret-token"); err != nil {
		t.Fatal(err)
	}

	for range 3 {
		select {
		case event := <-events:
			if event.Type != "upsert" {
				t.Fatalf("event type = %q", event.Type)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for activity event")
		}
	}

	got := store.Snapshot()
	if len(got.Requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(got.Requests))
	}
	record := got.Requests[0]
	if record.Outcome != OutcomeFailed || record.Phase != PhaseComplete || record.Model != "gpt-6-astra" {
		t.Fatalf("unexpected record: %#v", record)
	}
	if record.DurationMS == nil || *record.DurationMS != 1250 {
		t.Fatalf("duration = %v, want 1250", record.DurationMS)
	}
	if record.ErrorMessage != "upstream request failed" {
		t.Fatalf("error message = %q", record.ErrorMessage)
	}

	payload, err := os.ReadFile(filepath.Join(dir, "request-2026-09-19.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), "top-secret-token") {
		t.Fatal("persisted activity exposed an error secret")
	}
}

func TestStoreLogsFiltersPaginatesAndSweeps(t *testing.T) {
	t.Parallel()
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	finish := func(id, model, account string, outcome Outcome) {
		store.Start(id, "/v1/chat/completions")
		store.Update(id, func(record *Record) {
			record.Model = model
			record.AccountID = account
		})
		now = now.Add(time.Second)
		status := 200
		code := ""
		if outcome == OutcomeFailed {
			status = 500
			code = "upstream_error"
		}
		if err := store.Finish(id, "/v1/chat/completions", status, outcome, code, "secret detail"); err != nil {
			t.Fatal(err)
		}
	}
	finish("req_1", "gpt-6-astra", "acct_a", OutcomeSucceeded)
	finish("req_2", "gpt-5.6-terra", "acct_b", OutcomeFailed)
	finish("req_3", "gpt-6-astra", "acct_a", OutcomeSucceeded)

	days, err := store.Dates()
	if err != nil {
		t.Fatal(err)
	}
	if len(days) != 1 || days[0].Total != 3 || days[0].Failed != 1 {
		t.Fatalf("days = %#v", days)
	}

	page, err := store.Logs(LogQuery{Date: "2026-09-19", Limit: 1, Model: "astra", Account: "acct_a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 1 || page.Records[0].ID != "req_3" || page.NextCursor == "" {
		t.Fatalf("first page = %#v", page)
	}
	next, err := store.Logs(LogQuery{Date: "2026-09-19", Limit: 1, Model: "astra", Account: "acct_a", Cursor: page.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Records) != 1 || next.Records[0].ID != "req_1" || next.NextCursor != "" {
		t.Fatalf("second page = %#v", next)
	}

	_, events, cancel := store.Subscribe()
	defer cancel()
	now = now.Add(61 * time.Second)
	if got := store.Snapshot(); len(got.Requests) != 0 {
		t.Fatalf("requests after expiry = %d", len(got.Requests))
	}
	removed := 0
	for removed < 3 {
		select {
		case event := <-events:
			if event.Type == "remove" {
				removed++
			}
		case <-time.After(time.Second):
			t.Fatalf("received %d remove events, want 3", removed)
		}
	}
}

func TestStorePrunesLogsOlderThanThirtyDays(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	oldPath := filepath.Join(dir, "request-2026-08-20.jsonl")
	keepPath := filepath.Join(dir, "request-2026-08-21.jsonl")
	if err := os.WriteFile(oldPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keepPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Sweep(time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC))
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("old log still exists: %v", err)
	}
	if _, err := os.Stat(keepPath); err != nil {
		t.Fatalf("retained log missing: %v", err)
	}
}
