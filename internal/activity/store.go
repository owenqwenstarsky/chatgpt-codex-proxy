package activity

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	terminalTTL  = time.Minute
	retainedDays = 30
	logPrefix    = "request-"
	logSuffix    = ".jsonl"
)

type Phase string

const (
	PhaseRouting    Phase = "routing"
	PhaseUpstream   Phase = "upstream"
	PhaseStreaming  Phase = "streaming"
	PhaseFinalizing Phase = "finalizing"
	PhaseComplete   Phase = "complete"
)

type Outcome string

const (
	OutcomeActive    Outcome = "active"
	OutcomeSucceeded Outcome = "succeeded"
	OutcomeFailed    Outcome = "failed"
	OutcomeCancelled Outcome = "cancelled"
	OutcomeTimedOut  Outcome = "timed_out"
)

type Record struct {
	ID           string  `json:"id"`
	StartedAt    string  `json:"startedAt"`
	EndedAt      *string `json:"endedAt,omitempty"`
	Route        string  `json:"route"`
	Model        string  `json:"model,omitempty"`
	AccountID    string  `json:"accountId,omitempty"`
	AccountLabel string  `json:"accountLabel,omitempty"`
	Phase        Phase   `json:"phase"`
	Outcome      Outcome `json:"outcome"`
	Status       int     `json:"status,omitempty"`
	DurationMS   *int64  `json:"durationMs,omitempty"`
	ErrorCode    string  `json:"errorCode,omitempty"`
	ErrorMessage string  `json:"errorMessage,omitempty"`
}

type Snapshot struct {
	Requests  []Record `json:"requests"`
	EmittedAt string   `json:"emittedAt"`
}

type Event struct {
	Type      string    `json:"type"`
	Record    *Record   `json:"record,omitempty"`
	ID        string    `json:"id,omitempty"`
	EmittedAt string    `json:"emittedAt,omitempty"`
	Snapshot  *Snapshot `json:"snapshot,omitempty"`
}

type LogDay struct {
	Date   string `json:"date"`
	Total  int    `json:"total"`
	Failed int    `json:"failed"`
}

type LogQuery struct {
	Date    string
	Limit   int
	Cursor  string
	Query   string
	Outcome Outcome
	Account string
	Model   string
}

type LogPage struct {
	Date       string   `json:"date"`
	Records    []Record `json:"records"`
	NextCursor string   `json:"nextCursor,omitempty"`
	FetchedAt  string   `json:"fetchedAt"`
}

type Store struct {
	mu          sync.Mutex
	logMu       sync.Mutex
	dataDir     string
	records     map[string]Record
	subscribers map[uint64]chan Event
	nextSubID   uint64
	now         func() time.Time
}

func NewStore(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create activity data dir: %w", err)
	}
	store := &Store{
		dataDir:     dataDir,
		records:     make(map[string]Record),
		subscribers: make(map[uint64]chan Event),
		now:         func() time.Time { return time.Now().UTC() },
	}
	if err := store.pruneLogFiles(store.now()); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Store) Start(id, route string) {
	if s == nil || strings.TrimSpace(id) == "" {
		return
	}
	now := s.now().UTC()
	record := Record{
		ID:        strings.TrimSpace(id),
		StartedAt: now.Format(time.RFC3339Nano),
		Route:     strings.TrimSpace(route),
		Phase:     PhaseRouting,
		Outcome:   OutcomeActive,
	}
	s.mu.Lock()
	s.records[record.ID] = record
	s.publishLocked(upsertEvent(record, now))
	s.mu.Unlock()
}

func (s *Store) Update(id string, update func(*Record)) {
	if s == nil || update == nil {
		return
	}
	s.mu.Lock()
	record, ok := s.records[id]
	if !ok || record.Outcome != OutcomeActive {
		s.mu.Unlock()
		return
	}
	update(&record)
	s.records[id] = record
	s.publishLocked(upsertEvent(record, s.now()))
	s.mu.Unlock()
}

func (s *Store) Finish(id, route string, status int, outcome Outcome, errorCode, errorMessage string) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	record, ok := s.records[id]
	if !ok || record.Outcome != OutcomeActive {
		s.mu.Unlock()
		return nil
	}
	now := s.now().UTC()
	endedAt := now.Format(time.RFC3339Nano)
	startedAt, _ := time.Parse(time.RFC3339Nano, record.StartedAt)
	duration := max(now.Sub(startedAt).Milliseconds(), 0)
	if strings.TrimSpace(route) != "" {
		record.Route = strings.TrimSpace(route)
	}
	record.EndedAt = &endedAt
	record.DurationMS = &duration
	record.Phase = PhaseComplete
	record.Outcome = outcome
	record.Status = status
	record.ErrorCode = strings.TrimSpace(errorCode)
	record.ErrorMessage = safeErrorMessage(record.ErrorCode, errorMessage)
	s.records[id] = record
	s.publishLocked(upsertEvent(record, now))
	s.mu.Unlock()
	return s.appendRecord(record)
}

func (s *Store) Snapshot() Snapshot {
	if s == nil {
		return Snapshot{Requests: []Record{}, EmittedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.pruneExpiredLocked(now)
	return s.snapshotLocked(now)
}

func (s *Store) Subscribe() (Snapshot, <-chan Event, func()) {
	s.mu.Lock()
	now := s.now()
	s.pruneExpiredLocked(now)
	s.nextSubID++
	id := s.nextSubID
	ch := make(chan Event, 64)
	s.subscribers[id] = ch
	snapshot := s.snapshotLocked(now)
	s.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			s.mu.Lock()
			if existing, ok := s.subscribers[id]; ok {
				delete(s.subscribers, id)
				close(existing)
			}
			s.mu.Unlock()
		})
	}
	return snapshot, ch, cancel
}

func (s *Store) Sweep(now time.Time) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.pruneExpiredLocked(now)
	_ = s.pruneLogFiles(now)
	s.mu.Unlock()
}

func (s *Store) pruneExpiredLocked(now time.Time) {
	for id, record := range s.records {
		if record.EndedAt == nil {
			continue
		}
		endedAt, err := time.Parse(time.RFC3339Nano, *record.EndedAt)
		if err != nil || now.Sub(endedAt) < terminalTTL {
			continue
		}
		delete(s.records, id)
		s.publishLocked(Event{Type: "remove", ID: id, EmittedAt: now.UTC().Format(time.RFC3339Nano)})
	}
}

func (s *Store) Dates() ([]LogDay, error) {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		return nil, err
	}
	days := make([]LogDay, 0)
	for _, entry := range entries {
		date, ok := logDate(entry.Name())
		if entry.IsDir() || !ok {
			continue
		}
		records, err := readRecords(filepath.Join(s.dataDir, entry.Name()))
		if err != nil {
			return nil, err
		}
		day := LogDay{Date: date, Total: len(records)}
		for _, record := range records {
			if record.Outcome == OutcomeFailed || record.Outcome == OutcomeTimedOut {
				day.Failed++
			}
		}
		days = append(days, day)
	}
	slices.SortFunc(days, func(a, b LogDay) int { return strings.Compare(b.Date, a.Date) })
	return days, nil
}

func (s *Store) Logs(query LogQuery) (LogPage, error) {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	records, err := readRecords(s.logPath(query.Date))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return LogPage{}, err
	}
	slices.Reverse(records)
	filtered := records[:0]
	for _, record := range records {
		if matches(record, query) {
			filtered = append(filtered, record)
		}
	}
	offset, err := decodeCursor(query.Cursor)
	if err != nil || offset > len(filtered) {
		return LogPage{}, errors.New("invalid cursor")
	}
	limit := query.Limit
	if limit <= 0 {
		limit = 50
	}
	end := min(offset+limit, len(filtered))
	page := LogPage{
		Date:      query.Date,
		Records:   append([]Record(nil), filtered[offset:end]...),
		FetchedAt: s.now().UTC().Format(time.RFC3339Nano),
	}
	if end < len(filtered) {
		page.NextCursor = encodeCursor(end)
	}
	return page, nil
}

func (s *Store) snapshotLocked(now time.Time) Snapshot {
	records := make([]Record, 0, len(s.records))
	for _, record := range s.records {
		records = append(records, record)
	}
	slices.SortFunc(records, func(a, b Record) int { return strings.Compare(b.StartedAt, a.StartedAt) })
	return Snapshot{Requests: records, EmittedAt: now.UTC().Format(time.RFC3339Nano)}
}

func (s *Store) publishLocked(event Event) {
	for id, ch := range s.subscribers {
		select {
		case ch <- event:
		default:
			delete(s.subscribers, id)
			close(ch)
		}
	}
}

func (s *Store) appendRecord(record Record) error {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	startedAt, err := time.Parse(time.RFC3339Nano, record.StartedAt)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(s.logPath(startedAt.UTC().Format(time.DateOnly)), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(append(payload, '\n'))
	return err
}

func (s *Store) pruneLogFiles(now time.Time) error {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	return s.pruneLogFilesLocked(now)
}

func (s *Store) pruneLogFilesLocked(now time.Time) error {
	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		return err
	}
	cutoff := now.UTC().AddDate(0, 0, -(retainedDays - 1)).Format(time.DateOnly)
	for _, entry := range entries {
		date, ok := logDate(entry.Name())
		if !ok || date >= cutoff {
			continue
		}
		if err := os.Remove(filepath.Join(s.dataDir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (s *Store) logPath(date string) string {
	return filepath.Join(s.dataDir, logPrefix+date+logSuffix)
}

func logDate(name string) (string, bool) {
	if !strings.HasPrefix(name, logPrefix) || !strings.HasSuffix(name, logSuffix) {
		return "", false
	}
	date := strings.TrimSuffix(strings.TrimPrefix(name, logPrefix), logSuffix)
	parsed, err := time.Parse(time.DateOnly, date)
	return date, err == nil && parsed.Format(time.DateOnly) == date
}

func readRecords(path string) ([]Record, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	records := make([]Record, 0)
	scanner := bufio.NewScanner(file)
	buffer := make([]byte, 64*1024)
	scanner.Buffer(buffer, 4*1024*1024)
	for scanner.Scan() {
		var record Record
		if json.Unmarshal(scanner.Bytes(), &record) == nil {
			records = append(records, record)
		}
	}
	return records, scanner.Err()
}

func matches(record Record, query LogQuery) bool {
	if query.Outcome != "" && record.Outcome != query.Outcome {
		return false
	}
	if !containsFold(record.AccountID+" "+record.AccountLabel, query.Account) || !containsFold(record.Model, query.Model) {
		return false
	}
	metadata := strings.Join([]string{
		record.ID, record.Route, record.Model, record.AccountID, record.AccountLabel,
		record.ErrorCode, record.ErrorMessage, string(record.Outcome),
	}, " ")
	return containsFold(metadata, query.Query)
}

func containsFold(value, query string) bool {
	query = strings.TrimSpace(query)
	return query == "" || strings.Contains(strings.ToLower(value), strings.ToLower(query))
}

func encodeCursor(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

func decodeCursor(cursor string) (int, error) {
	if strings.TrimSpace(cursor) == "" {
		return 0, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, err
	}
	offset, err := strconv.Atoi(string(payload))
	if err != nil || offset < 0 {
		return 0, errors.New("invalid cursor")
	}
	return offset, nil
}

func safeErrorMessage(code, fallback string) string {
	switch strings.TrimSpace(code) {
	case "client_canceled":
		return "client canceled request"
	case "request_timeout":
		return "request timed out"
	case "rate_limited":
		return "all eligible accounts are rate limited"
	case "quota_exhausted":
		return "upstream account quota exhausted"
	case "no_available_accounts":
		return "no available accounts"
	case "model_not_found":
		return "model not found"
	case "invalid_api_key", "authentication_error":
		return "authentication failed"
	case "invalid_request_error", "invalid_json":
		return "invalid request"
	case "upstream_error", "stream_error", "api_error":
		return "upstream request failed"
	}
	if strings.TrimSpace(code) != "" || strings.TrimSpace(fallback) != "" {
		return "request failed"
	}
	return ""
}

func upsertEvent(record Record, now time.Time) Event {
	copy := record
	return Event{Type: "upsert", Record: &copy, EmittedAt: now.UTC().Format(time.RFC3339Nano)}
}
