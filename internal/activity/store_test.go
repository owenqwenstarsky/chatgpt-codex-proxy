package activity

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type testClock struct{ now time.Time }

func (c *testClock) Now() time.Time      { return c.now }
func (c *testClock) Add(d time.Duration) { c.now = c.now.Add(d) }

func TestStoreLifecyclePersistenceAndExpiry(t *testing.T) {
	dir := t.TempDir()
	clock := &testClock{now: time.Date(2026, 9, 19, 20, 0, 0, 123456000, time.UTC)}
	store := NewStore(dir, Options{Now: clock.Now, TerminalRetention: time.Minute, SubscriberBuffer: 64})
	snapshot, sub := store.Subscribe()
	defer sub.Close()
	if len(snapshot.Requests) != 0 {
		t.Fatalf("initial requests = %d, want 0", len(snapshot.Requests))
	}

	started, ok := store.Start("req_1", "/v1/responses?secret=no")
	if !ok || started.Phase != PhaseRouting || started.Outcome != OutcomeActive {
		t.Fatalf("Start() = %#v, %v", started, ok)
	}
	store.SetModel("req_1", "  gpt-5.6-terra  ")
	store.SetAccount("req_1", " acct_01 ", " primary ")
	store.SetPhase("req_1", PhaseStreaming)
	clock.Add(1250 * time.Millisecond)
	status := 200
	finished, first, err := store.Finish("req_1", FinishInput{Outcome: OutcomeFailed, Status: &status, ErrorCode: "raw-secret-token-value"})
	if err != nil || !first {
		t.Fatalf("Finish() = first %v, err %v", first, err)
	}
	if finished.Phase != PhaseComplete || finished.DurationMS == nil || *finished.DurationMS != 1250 {
		t.Fatalf("finished = %#v", finished)
	}
	if finished.ErrorCode != "api_error" || finished.ErrorMessage != "request failed" {
		t.Fatalf("safe error = %q %q", finished.ErrorCode, finished.ErrorMessage)
	}
	if _, duplicate, err := store.Finish("req_1", FinishInput{Outcome: OutcomeSucceeded}); err != nil || duplicate {
		t.Fatalf("duplicate Finish() = %v, %v", duplicate, err)
	}

	events := make([]Event, 0, 5)
	for len(events) < 5 {
		select {
		case event := <-sub.Events:
			events = append(events, event)
		case <-time.After(time.Second):
			t.Fatalf("timed out after %d events", len(events))
		}
	}
	for _, event := range events {
		if event.Type != "upsert" || event.Record == nil || event.Record.ID != "req_1" {
			t.Fatalf("event = %#v", event)
		}
	}

	path := filepath.Join(dir, "request-2026-09-19.jsonl")
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(payload), "\n") != 1 || strings.Contains(string(payload), "raw-secret") || strings.Contains(string(payload), "?secret") {
		t.Fatalf("unsafe or duplicate log: %s", payload)
	}

	clock.Add(time.Minute)
	store.Sweep()
	select {
	case event := <-sub.Events:
		if event.Type != "remove" || event.ID != "req_1" {
			t.Fatalf("remove = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("missing remove event")
	}
	if got := store.Snapshot().Requests; len(got) != 0 {
		t.Fatalf("snapshot requests = %d, want 0", len(got))
	}
}

func TestStoreSnapshotOrderingAndSlowSubscriber(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)}
	store := NewStore(t.TempDir(), Options{Now: clock.Now, SubscriberBuffer: 1})
	_, sub := store.Subscribe()
	defer sub.Close()
	store.Start("older", "/v1/responses")
	clock.Add(time.Second)
	store.Start("newer", "/v1/responses")
	if _, ok := <-sub.Events; !ok {
		// A full subscriber is intentionally closed; either the queued event or
		// the close may be observed first.
	}
	if _, ok := <-sub.Events; ok {
		t.Fatal("slow subscriber remained registered")
	}
	snapshot := store.Snapshot()
	if len(snapshot.Requests) != 2 || snapshot.Requests[0].ID != "newer" {
		t.Fatalf("snapshot order = %#v", snapshot.Requests)
	}
}

func TestStoreLogsFiltersPaginationMalformedAndRetention(t *testing.T) {
	dir := t.TempDir()
	clock := &testClock{now: time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)}
	store := NewStore(dir, Options{Now: clock.Now})
	finish := func(id, model, account string, outcome Outcome, code string) {
		t.Helper()
		store.Start(id, "/v1/responses")
		store.SetModel(id, model)
		store.SetAccount(id, account, "Primary")
		clock.Add(time.Second)
		if _, _, err := store.Finish(id, FinishInput{Outcome: outcome, ErrorCode: code}); err != nil {
			t.Fatal(err)
		}
	}
	finish("req_one", "gpt-5.6-terra", "acct_A", OutcomeSucceeded, "")
	finish("req_two", "gpt-5.6-sol", "acct_B", OutcomeFailed, "upstream_error")
	finish("req_three", "gpt-5.6-terra", "acct_A", OutcomeTimedOut, "request_timeout")

	path := filepath.Join(dir, "request-2026-09-19.jsonl")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.WriteString("{truncated\n")
	_ = file.Close()

	dates, err := store.ListDates()
	if err != nil || len(dates.Days) != 1 || dates.Days[0].Total != 3 || dates.Days[0].Failed != 2 {
		t.Fatalf("ListDates() = %#v, %v", dates, err)
	}
	page, err := store.QueryLogs(LogQuery{Date: "2026-09-19", Limit: 1, Model: "TERRA", Account: "primary", Q: "REQ_"})
	if err != nil || len(page.Records) != 1 || page.Records[0].ID != "req_three" || page.NextCursor == nil {
		t.Fatalf("first page = %#v, %v", page, err)
	}
	next, err := store.QueryLogs(LogQuery{Date: "2026-09-19", Limit: 1, Model: "terra", Account: "ACCT_A", Q: "req_", Cursor: *page.NextCursor})
	if err != nil || len(next.Records) != 1 || next.Records[0].ID != "req_one" || next.NextCursor != nil {
		t.Fatalf("next page = %#v, %v", next, err)
	}
	missing, err := store.QueryLogs(LogQuery{Date: "2026-09-18", Limit: 50})
	if err != nil || len(missing.Records) != 0 {
		t.Fatalf("missing = %#v, %v", missing, err)
	}

	old := filepath.Join(dir, "request-2026-08-20.jsonl")
	keep := filepath.Join(dir, "request-2026-08-21.jsonl")
	unrelated := filepath.Join(dir, "accounts.json")
	for _, name := range []string{old, keep, unrelated} {
		if err := os.WriteFile(name, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.PruneLogs(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("old log was not pruned: %v", err)
	}
	for _, name := range []string{keep, unrelated} {
		if _, err := os.Stat(name); err != nil {
			t.Fatalf("unexpected prune of %s: %v", name, err)
		}
	}
}

func TestReadRecordsSupportsLargeMalformedLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "request-2026-09-19.jsonl")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := bufio.NewWriter(file)
	_, _ = writer.WriteString(strings.Repeat("x", 4*1024*1024) + "\n")
	record := Record{ID: "valid", StartedAt: time.Now().UTC(), Route: "/v1/responses", Phase: PhaseComplete, Outcome: OutcomeSucceeded}
	payload, _ := json.Marshal(record)
	_, _ = writer.Write(append(payload, '\n'))
	_ = writer.Flush()
	_ = file.Close()
	records, err := readRecords(path)
	if err != nil || len(records) != 1 || records[0].ID != "valid" {
		t.Fatalf("readRecords() = %#v, %v", records, err)
	}
}

func TestConcurrentFinishesWriteWholeLines(t *testing.T) {
	dir := t.TempDir()
	clock := &testClock{now: time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)}
	store := NewStore(dir, Options{Now: clock.Now})
	var wait sync.WaitGroup
	for index := 0; index < 32; index++ {
		id := "req_" + strings.Repeat("x", index+1)
		store.Start(id, "/v1/responses")
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, _, err := store.Finish(id, FinishInput{Outcome: OutcomeSucceeded}); err != nil {
				t.Errorf("Finish(%q): %v", id, err)
			}
		}()
	}
	wait.Wait()
	records, err := readRecords(filepath.Join(dir, "request-2026-09-19.jsonl"))
	if err != nil || len(records) != 32 {
		t.Fatalf("records = %d, err=%v", len(records), err)
	}
}
