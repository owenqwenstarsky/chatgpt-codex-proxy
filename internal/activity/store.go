package activity

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
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
	ID           string     `json:"id"`
	StartedAt    time.Time  `json:"startedAt"`
	EndedAt      *time.Time `json:"endedAt,omitempty"`
	Route        string     `json:"route"`
	Model        string     `json:"model,omitempty"`
	AccountID    string     `json:"accountId,omitempty"`
	AccountLabel string     `json:"accountLabel,omitempty"`
	Phase        Phase      `json:"phase"`
	Outcome      Outcome    `json:"outcome"`
	Status       *int       `json:"status,omitempty"`
	DurationMS   *int64     `json:"durationMs,omitempty"`
	ErrorCode    string     `json:"errorCode,omitempty"`
	ErrorMessage string     `json:"errorMessage,omitempty"`
}

type Snapshot struct {
	Requests  []Record  `json:"requests"`
	EmittedAt time.Time `json:"emittedAt"`
}

type Event struct {
	Type      string     `json:"type"`
	Snapshot  *Snapshot  `json:"snapshot,omitempty"`
	Record    *Record    `json:"record,omitempty"`
	ID        string     `json:"id,omitempty"`
	EmittedAt *time.Time `json:"emittedAt,omitempty"`
}

type DaySummary struct {
	Date   string `json:"date"`
	Total  int    `json:"total"`
	Failed int    `json:"failed"`
}

type DatesResponse struct {
	Days      []DaySummary `json:"days"`
	FetchedAt time.Time    `json:"fetchedAt"`
}

type LogResponse struct {
	Date       string    `json:"date"`
	Records    []Record  `json:"records"`
	NextCursor *string   `json:"nextCursor,omitempty"`
	FetchedAt  time.Time `json:"fetchedAt"`
}

type LogQuery struct {
	Date    string
	Limit   int
	Cursor  string
	Q       string
	Outcome Outcome
	Account string
	Model   string
}

type FinishInput struct {
	Outcome   Outcome
	Status    *int
	ErrorCode string
}

type Subscription struct {
	Events <-chan Event
	cancel func()
}

func (s Subscription) Close() {
	if s.cancel != nil {
		s.cancel()
	}
}

type Options struct {
	Now               func() time.Time
	TerminalRetention time.Duration
	SubscriberBuffer  int
}

type subscriber struct {
	ch chan Event
}

type Store struct {
	mu                sync.Mutex
	records           map[string]Record
	subscribers       map[uint64]*subscriber
	nextSubscriberID  uint64
	lastEmittedAt     time.Time
	now               func() time.Time
	terminalRetention time.Duration
	subscriberBuffer  int
	dataDir           string
	writeMu           sync.Mutex
}

var requestLogPattern = regexp.MustCompile(`^request-(\d{4}-\d{2}-\d{2})\.jsonl$`)

func NewStore(dataDir string, options Options) *Store {
	now := options.Now
	if now == nil {
		now = time.Now
	}
	retention := options.TerminalRetention
	if retention <= 0 {
		retention = 60 * time.Second
	}
	buffer := options.SubscriberBuffer
	if buffer <= 0 {
		buffer = 64
	}
	return &Store{
		records:           make(map[string]Record),
		subscribers:       make(map[uint64]*subscriber),
		now:               now,
		terminalRetention: retention,
		subscriberBuffer:  buffer,
		dataDir:           dataDir,
	}
}

func (s *Store) Start(id, route string) (Record, bool) {
	id = strings.TrimSpace(id)
	if id == "" {
		return Record{}, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.records[id]; exists {
		return Record{}, false
	}
	record := Record{
		ID:        id,
		StartedAt: s.nowUTC(),
		Route:     sanitizeRoute(route),
		Phase:     PhaseRouting,
		Outcome:   OutcomeActive,
	}
	s.records[id] = record
	s.publishLocked(upsertEvent(record, s.nextEmittedAtLocked()))
	return cloneRecord(record), true
}

func (s *Store) Update(id string, mutate func(*Record)) bool {
	if mutate == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[id]
	if !ok || record.Outcome != OutcomeActive || record.Phase == PhaseComplete {
		return false
	}
	before := record
	mutate(&record)
	sanitizeRecordUpdate(&record, before)
	if recordsEqual(before, record) {
		return false
	}
	s.records[id] = record
	s.publishLocked(upsertEvent(record, s.nextEmittedAtLocked()))
	return true
}

func (s *Store) Finish(id string, input FinishInput) (Record, bool, error) {
	s.mu.Lock()
	record, ok := s.records[id]
	if !ok || record.Outcome != OutcomeActive || record.Phase == PhaseComplete {
		s.mu.Unlock()
		return Record{}, false, nil
	}
	endedAt := s.nowUTC()
	duration := endedAt.Sub(record.StartedAt).Milliseconds()
	if duration < 0 {
		duration = 0
	}
	record.EndedAt = &endedAt
	record.DurationMS = &duration
	record.Phase = PhaseComplete
	record.Outcome = terminalOutcome(input.Outcome)
	if input.Status != nil {
		status := *input.Status
		record.Status = &status
	}
	record.ErrorCode, record.ErrorMessage = safeFailure(record.Outcome, input.ErrorCode, record.Status)
	s.records[id] = record
	s.publishLocked(upsertEvent(record, s.nextEmittedAtLocked()))
	s.mu.Unlock()

	err := s.appendRecord(record)
	return cloneRecord(record), true, err
}

func (s *Store) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneExpiredLocked()
	return s.snapshotLocked()
}

func (s *Store) Subscribe() (Snapshot, Subscription) {
	s.mu.Lock()
	s.pruneExpiredLocked()
	s.nextSubscriberID++
	id := s.nextSubscriberID
	sub := &subscriber{ch: make(chan Event, s.subscriberBuffer)}
	s.subscribers[id] = sub
	snapshot := s.snapshotLocked()
	s.mu.Unlock()

	var once sync.Once
	return snapshot, Subscription{
		Events: sub.ch,
		cancel: func() {
			once.Do(func() { s.unsubscribe(id) })
		},
	}
}

func (s *Store) Sweep() {
	s.mu.Lock()
	s.pruneExpiredLocked()
	s.mu.Unlock()
}

func (s *Store) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, sub := range s.subscribers {
		delete(s.subscribers, id)
		close(sub.ch)
	}
}

func (s *Store) SetModel(id, model string) bool {
	return s.Update(id, func(record *Record) { record.Model = model })
}

func (s *Store) SetAccount(id, accountID, accountLabel string) bool {
	return s.Update(id, func(record *Record) {
		record.AccountID = accountID
		record.AccountLabel = accountLabel
		record.Phase = PhaseUpstream
	})
}

func (s *Store) SetPhase(id string, phase Phase) bool {
	return s.Update(id, func(record *Record) { record.Phase = phase })
}

func (s *Store) SetRoute(id, route string) bool {
	return s.Update(id, func(record *Record) { record.Route = route })
}

func (s *Store) ListDates() (DatesResponse, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now := s.nowUTC()
	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		return DatesResponse{}, err
	}
	cutoff := utcDay(now).AddDate(0, 0, -29)
	today := utcDay(now)
	days := make([]DaySummary, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		date, day, ok := parseLogFilename(entry.Name())
		if !ok || day.Before(cutoff) || day.After(today) {
			continue
		}
		records, err := readRecords(filepath.Join(s.dataDir, entry.Name()))
		if err != nil {
			return DatesResponse{}, err
		}
		summary := DaySummary{Date: date, Total: len(records)}
		for _, record := range records {
			if record.Outcome == OutcomeFailed || record.Outcome == OutcomeTimedOut {
				summary.Failed++
			}
		}
		days = append(days, summary)
	}
	sort.Slice(days, func(i, j int) bool { return days[i].Date > days[j].Date })
	return DatesResponse{Days: days, FetchedAt: now}, nil
}

func (s *Store) QueryLogs(query LogQuery) (LogResponse, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	records, err := readRecords(filepath.Join(s.dataDir, "request-"+query.Date+".jsonl"))
	if err != nil {
		return LogResponse{}, err
	}
	filtered := make([]Record, 0, len(records))
	for idx := len(records) - 1; idx >= 0; idx-- {
		if matches(records[idx], query) {
			filtered = append(filtered, records[idx])
		}
	}
	offset, err := decodeCursor(query.Cursor)
	if err != nil || offset < 0 || offset > len(filtered) {
		return LogResponse{}, ErrInvalidCursor
	}
	limit := query.Limit
	if limit <= 0 {
		limit = 50
	}
	end := offset + limit
	if end > len(filtered) {
		end = len(filtered)
	}
	page := make([]Record, end-offset)
	copy(page, filtered[offset:end])
	response := LogResponse{Date: query.Date, Records: page, FetchedAt: s.nowUTC()}
	if end < len(filtered) {
		cursor := encodeCursor(end)
		response.NextCursor = &cursor
	}
	return response, nil
}

func (s *Store) PruneLogs() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		return err
	}
	cutoff := utcDay(s.nowUTC()).AddDate(0, 0, -29)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		_, day, ok := parseLogFilename(entry.Name())
		if !ok || !day.Before(cutoff) {
			continue
		}
		if err := os.Remove(filepath.Join(s.dataDir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

var ErrInvalidCursor = errors.New("invalid cursor")

func EncodeCursor(offset int) string { return encodeCursor(offset) }

func DecodeCursor(cursor string) (int, error) { return decodeCursor(cursor) }

func (s *Store) nowUTC() time.Time { return s.now().UTC() }

func (s *Store) nextEmittedAtLocked() time.Time {
	now := s.nowUTC()
	if !s.lastEmittedAt.IsZero() && !now.After(s.lastEmittedAt) {
		now = s.lastEmittedAt.Add(time.Nanosecond)
	}
	s.lastEmittedAt = now
	return now
}

func (s *Store) snapshotLocked() Snapshot {
	records := make([]Record, 0, len(s.records))
	for _, record := range s.records {
		records = append(records, cloneRecord(record))
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].StartedAt.Equal(records[j].StartedAt) {
			return records[i].ID > records[j].ID
		}
		return records[i].StartedAt.After(records[j].StartedAt)
	})
	return Snapshot{Requests: records, EmittedAt: s.nextEmittedAtLocked()}
}

func (s *Store) pruneExpiredLocked() {
	now := s.nowUTC()
	for id, record := range s.records {
		if record.EndedAt == nil || now.Sub(*record.EndedAt) < s.terminalRetention {
			continue
		}
		delete(s.records, id)
		emittedAt := s.nextEmittedAtLocked()
		s.publishLocked(Event{Type: "remove", ID: id, EmittedAt: &emittedAt})
	}
}

func (s *Store) publishLocked(event Event) {
	for id, sub := range s.subscribers {
		select {
		case sub.ch <- event:
		default:
			delete(s.subscribers, id)
			close(sub.ch)
		}
	}
}

func (s *Store) unsubscribe(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sub, ok := s.subscribers[id]; ok {
		delete(s.subscribers, id)
		close(sub.ch)
	}
}

func (s *Store) appendRecord(record Record) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	path := filepath.Join(s.dataDir, "request-"+record.StartedAt.UTC().Format(time.DateOnly)+".jsonl")
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	written, writeErr := file.Write(payload)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if written != len(payload) {
		return io.ErrShortWrite
	}
	return closeErr
}

func upsertEvent(record Record, emittedAt time.Time) Event {
	copy := cloneRecord(record)
	return Event{Type: "upsert", Record: &copy, EmittedAt: &emittedAt}
}

func sanitizeRecordUpdate(record *Record, before Record) {
	record.ID = before.ID
	record.StartedAt = before.StartedAt
	record.EndedAt = before.EndedAt
	record.Status = before.Status
	record.DurationMS = before.DurationMS
	record.Outcome = before.Outcome
	record.ErrorCode = before.ErrorCode
	record.ErrorMessage = before.ErrorMessage
	record.Route = sanitizeRoute(record.Route)
	record.Model = sanitizeMetadata(record.Model)
	record.AccountID = sanitizeMetadata(record.AccountID)
	record.AccountLabel = sanitizeMetadata(record.AccountLabel)
	if phaseRank(record.Phase) < phaseRank(before.Phase) || record.Phase == PhaseComplete || phaseRank(record.Phase) == 0 {
		record.Phase = before.Phase
	}
}

func sanitizeMetadata(value string) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if len(runes) > 200 {
		value = string(runes[:200])
	}
	return strings.ToValidUTF8(value, "")
}

func sanitizeRoute(value string) string {
	value = strings.TrimSpace(value)
	if index := strings.IndexAny(value, "?#"); index >= 0 {
		value = value[:index]
	}
	if !strings.HasPrefix(value, "/") {
		return ""
	}
	return sanitizeMetadata(value)
}

func phaseRank(phase Phase) int {
	switch phase {
	case PhaseRouting:
		return 1
	case PhaseUpstream:
		return 2
	case PhaseStreaming:
		return 3
	case PhaseFinalizing:
		return 4
	case PhaseComplete:
		return 5
	default:
		return 0
	}
}

func terminalOutcome(outcome Outcome) Outcome {
	switch outcome {
	case OutcomeSucceeded, OutcomeFailed, OutcomeCancelled, OutcomeTimedOut:
		return outcome
	default:
		return OutcomeFailed
	}
}

func safeFailure(outcome Outcome, rawCode string, status *int) (string, string) {
	if outcome == OutcomeSucceeded {
		return "", ""
	}
	if outcome == OutcomeCancelled {
		return "client_canceled", "client canceled request"
	}
	if outcome == OutcomeTimedOut {
		return "request_timeout", "request timed out"
	}
	code := strings.ToLower(strings.TrimSpace(rawCode))
	switch code {
	case "client_canceled":
		return code, "client canceled request"
	case "request_timeout":
		return code, "request timed out"
	case "rate_limited":
		return code, "all eligible accounts are rate limited"
	case "quota_exhausted":
		return code, "upstream account quota exhausted"
	case "no_available_accounts", "continuation_account_unavailable":
		return "no_available_accounts", "no available accounts"
	case "model_not_found":
		return code, "model not found"
	case "invalid_api_key", "authentication_error":
		return code, "authentication failed"
	case "invalid_json":
		return code, "invalid request"
	case "invalid_request_error", "unsupported_content_part":
		return "invalid_request_error", "invalid request"
	case "stream_error":
		return code, "upstream request failed"
	case "upstream_error", "upstream_unauthorized", "image_generation_failed":
		return "upstream_error", "upstream request failed"
	case "api_error":
		return code, "upstream request failed"
	}
	if status != nil {
		switch *status {
		case 401, 403:
			return "authentication_error", "authentication failed"
		case 408, 504:
			return "request_timeout", "request timed out"
		case 429:
			return "rate_limited", "all eligible accounts are rate limited"
		}
		if *status >= 400 && *status < 500 {
			return "invalid_request_error", "invalid request"
		}
	}
	return "api_error", "request failed"
}

func cloneRecord(record Record) Record {
	if record.EndedAt != nil {
		endedAt := *record.EndedAt
		record.EndedAt = &endedAt
	}
	if record.Status != nil {
		status := *record.Status
		record.Status = &status
	}
	if record.DurationMS != nil {
		duration := *record.DurationMS
		record.DurationMS = &duration
	}
	return record
}

func recordsEqual(left, right Record) bool {
	leftJSON, _ := json.Marshal(left)
	rightJSON, _ := json.Marshal(right)
	return string(leftJSON) == string(rightJSON)
}

func readRecords(path string) ([]Record, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return []Record{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)
	records := make([]Record, 0)
	for scanner.Scan() {
		var record Record
		if json.Unmarshal(scanner.Bytes(), &record) != nil || !validRecord(record) {
			continue
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func validRecord(record Record) bool {
	if strings.TrimSpace(record.ID) == "" || record.StartedAt.IsZero() || strings.TrimSpace(record.Route) == "" {
		return false
	}
	if phaseRank(record.Phase) == 0 {
		return false
	}
	switch record.Outcome {
	case OutcomeActive, OutcomeSucceeded, OutcomeFailed, OutcomeCancelled, OutcomeTimedOut:
		return true
	default:
		return false
	}
}

func matches(record Record, query LogQuery) bool {
	if query.Outcome != "" && record.Outcome != query.Outcome {
		return false
	}
	if !containsFold(record.AccountID+" "+record.AccountLabel, query.Account) {
		return false
	}
	if !containsFold(record.Model, query.Model) {
		return false
	}
	search := strings.Join([]string{record.ID, record.Route, record.Model, record.AccountID, record.AccountLabel, record.ErrorCode, record.ErrorMessage, string(record.Outcome)}, " ")
	return containsFold(search, query.Q)
}

func containsFold(value, query string) bool {
	query = strings.ToLower(strings.TrimSpace(query))
	return query == "" || strings.Contains(strings.ToLower(value), query)
}

func encodeCursor(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

func decodeCursor(cursor string) (int, error) {
	cursor = strings.TrimSpace(cursor)
	if cursor == "" {
		return 0, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil || len(payload) == 0 || len(payload) > 20 || !utf8.Valid(payload) {
		return 0, ErrInvalidCursor
	}
	offset, err := strconv.Atoi(string(payload))
	if err != nil || offset < 0 {
		return 0, ErrInvalidCursor
	}
	return offset, nil
}

func parseLogFilename(name string) (string, time.Time, bool) {
	match := requestLogPattern.FindStringSubmatch(name)
	if len(match) != 2 {
		return "", time.Time{}, false
	}
	day, err := time.Parse(time.DateOnly, match[1])
	if err != nil || day.Format(time.DateOnly) != match[1] {
		return "", time.Time{}, false
	}
	return match[1], day.UTC(), true
}

func utcDay(value time.Time) time.Time {
	year, month, day := value.UTC().Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func ValidateDate(value string) bool {
	parsed, err := time.Parse(time.DateOnly, value)
	return err == nil && parsed.Format(time.DateOnly) == value
}

func ValidateOutcome(value string) bool {
	switch Outcome(value) {
	case "", OutcomeActive, OutcomeSucceeded, OutcomeFailed, OutcomeCancelled, OutcomeTimedOut:
		return true
	default:
		return false
	}
}

func ParseLimit(value string) (int, error) {
	if strings.TrimSpace(value) == "" {
		return 50, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit < 1 || limit > 100 {
		return 0, fmt.Errorf("invalid limit")
	}
	return limit, nil
}
