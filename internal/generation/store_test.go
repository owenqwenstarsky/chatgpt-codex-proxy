package generation

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStorePersistsSanitizedAttemptAndQueries(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir)
	r, ok := s.Start(StartInput{ParentRequestID: "req_1", Endpoint: "responses", Transport: "http", AccountID: "acct_1", Model: "gpt-test", Attempt: 1, Payload: map[string]any{
		"model": "gpt-test", "input": []any{map[string]any{"role": "user", "content": "hello"}}, "headers": map[string]any{"Authorization": "secret"},
	}})
	if !ok || r.ID == "" {
		t.Fatal("Start did not create attempt")
	}
	status := 200
	if _, finished, err := s.Finish(r.ID, FinishInput{Outcome: OutcomeSucceeded, Status: &status, ResponseID: "resp_1"}); err != nil || !finished {
		t.Fatalf("Finish = (%v, %v)", finished, err)
	}
	day := r.StartedAt.UTC().Format("2006-01-02")
	got, err := s.Query(Query{Date: day, ParentRequestID: "req_1", Model: "gpt-test"})
	if err != nil || len(got.Records) != 1 {
		t.Fatalf("Query = %#v, %v", got, err)
	}
	var payload map[string]any
	b, _ := json.Marshal(got.Records[0].Payload)
	_ = json.Unmarshal(b, &payload)
	if payload["input"] == nil || payload["headers"] != "<redacted>" {
		t.Fatalf("payload sanitization = %#v", payload)
	}
	if _, err := os.Stat(filepath.Join(dir, "generation-"+day+".jsonl")); err != nil {
		t.Fatal(err)
	}
}

func TestStorePaginationAndPrune(t *testing.T) {
	now := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	s := NewStore(t.TempDir())
	s.now = func() time.Time { return now }
	for i := 0; i < 3; i++ {
		r, _ := s.Start(StartInput{ParentRequestID: "req", Endpoint: "responses", Transport: "http", Attempt: i + 1})
		_, _, _ = s.Finish(r.ID, FinishInput{Outcome: OutcomeSucceeded})
	}
	day := now.Format("2006-01-02")
	page, err := s.Query(Query{Date: day, Limit: 2})
	if err != nil || len(page.Records) != 2 || page.NextCursor == nil {
		t.Fatalf("first page = %#v, %v", page, err)
	}
	page2, err := s.Query(Query{Date: day, Limit: 2, Cursor: *page.NextCursor})
	if err != nil || len(page2.Records) != 1 {
		t.Fatalf("second page = %#v, %v", page2, err)
	}
	old := filepath.Join(s.dataDir, "generation-2026-01-01.jsonl")
	if err := os.WriteFile(old, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.Prune(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("old log still exists: %v", err)
	}
}
