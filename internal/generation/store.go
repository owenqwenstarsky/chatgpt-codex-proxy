package generation

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
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
	ID                string         `json:"id"`
	ParentRequestID   string         `json:"parentRequestId"`
	StartedAt         time.Time      `json:"startedAt"`
	EndedAt           *time.Time     `json:"endedAt,omitempty"`
	DurationMS        *int64         `json:"durationMs,omitempty"`
	Endpoint          string         `json:"endpoint"`
	Transport         string         `json:"transport"`
	Attempt           int            `json:"attempt"`
	AccountID         string         `json:"accountId,omitempty"`
	AccountLabel      string         `json:"accountLabel,omitempty"`
	UpstreamAccountID string         `json:"upstreamAccountId,omitempty"`
	Model             string         `json:"model,omitempty"`
	Outcome           Outcome        `json:"outcome"`
	Status            *int           `json:"status,omitempty"`
	ErrorCode         string         `json:"errorCode,omitempty"`
	ErrorMessage      string         `json:"errorMessage,omitempty"`
	ResponseID        string         `json:"responseId,omitempty"`
	ResponseModel     string         `json:"responseModel,omitempty"`
	Usage             map[string]any `json:"usage,omitempty"`
	TerminalEvent     string         `json:"terminalEvent,omitempty"`
	Payload           any            `json:"payload,omitempty"`
}

type StartInput struct {
	ParentRequestID, Endpoint, Transport, AccountID, AccountLabel, UpstreamAccountID, Model string
	Attempt                                                                                 int
	Payload                                                                                 any
}
type FinishInput struct {
	Outcome                                                           Outcome
	Status                                                            *int
	ErrorCode, ErrorMessage, ResponseID, ResponseModel, TerminalEvent string
	Usage                                                             map[string]any
}
type Query struct {
	Date, Cursor, ParentRequestID, Account, Model, Endpoint, Transport string
	Outcome                                                            Outcome
	Limit                                                              int
}
type Response struct {
	Date       string    `json:"date"`
	Records    []Record  `json:"records"`
	NextCursor *string   `json:"nextCursor,omitempty"`
	FetchedAt  time.Time `json:"fetchedAt"`
}
type DateSummary struct {
	Date   string `json:"date"`
	Total  int    `json:"total"`
	Failed int    `json:"failed"`
}
type DatesResponse struct {
	Days      []DateSummary `json:"days"`
	FetchedAt time.Time     `json:"fetchedAt"`
}

var ErrInvalidCursor = errors.New("invalid cursor")

type Store struct {
	mu      sync.Mutex
	records map[string]Record
	dataDir string
	writeMu sync.Mutex
	now     func() time.Time
}

func NewStore(dataDir string) *Store {
	return &Store{records: map[string]Record{}, dataDir: dataDir, now: time.Now}
}
func (s *Store) Start(in StartInput) (Record, bool) {
	if strings.TrimSpace(in.ParentRequestID) == "" {
		return Record{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id := "gen_" + s.now().UTC().Format("20060102T150405.000000000") + "_" + randomSuffix()
	r := Record{ID: id, ParentRequestID: strings.TrimSpace(in.ParentRequestID), StartedAt: s.now().UTC(), Endpoint: in.Endpoint, Transport: in.Transport, Attempt: in.Attempt, AccountID: in.AccountID, AccountLabel: in.AccountLabel, UpstreamAccountID: in.UpstreamAccountID, Model: in.Model, Outcome: OutcomeActive, Payload: Sanitize(in.Payload)}
	s.records[id] = r
	return r, true
}
func (s *Store) Finish(id string, in FinishInput) (Record, bool, error) {
	s.mu.Lock()
	r, ok := s.records[id]
	if !ok || r.Outcome != OutcomeActive {
		s.mu.Unlock()
		return Record{}, false, nil
	}
	now := s.now().UTC()
	d := now.Sub(r.StartedAt).Milliseconds()
	if d < 0 {
		d = 0
	}
	r.EndedAt = &now
	r.DurationMS = &d
	r.Outcome = terminal(in.Outcome)
	r.Status = cloneInt(in.Status)
	r.ErrorCode = safe(in.ErrorCode)
	r.ErrorMessage = truncate(in.ErrorMessage)
	r.ResponseID = truncate(in.ResponseID)
	r.ResponseModel = truncate(in.ResponseModel)
	r.TerminalEvent = truncate(in.TerminalEvent)
	r.Usage = in.Usage
	s.records[id] = r
	s.mu.Unlock()
	return r, true, s.append(r)
}
func (s *Store) Get(id string) (Record, bool) {
	s.mu.Lock()
	r, ok := s.records[id]
	s.mu.Unlock()
	if ok {
		return r, true
	}
	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		return Record{}, false
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "generation-") {
			continue
		}
		records, err := read(filepath.Join(s.dataDir, entry.Name()))
		if err != nil {
			continue
		}
		for _, record := range records {
			if record.ID == id {
				return record, true
			}
		}
	}
	return Record{}, false
}
func (s *Store) Query(q Query) (Response, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	rs, err := read(filepath.Join(s.dataDir, "generation-"+q.Date+".jsonl"))
	if err != nil {
		return Response{}, err
	}
	f := rs[:0]
	for i := len(rs) - 1; i >= 0; i-- {
		if match(rs[i], q) {
			f = append(f, rs[i])
		}
	}
	off := 0
	if q.Cursor != "" {
		var e error
		off, e = decode(q.Cursor)
		if e != nil {
			return Response{}, ErrInvalidCursor
		}
	}
	if off < 0 || off > len(f) {
		return Response{}, ErrInvalidCursor
	}
	lim := q.Limit
	if lim <= 0 {
		lim = 50
	}
	end := off + lim
	if end > len(f) {
		end = len(f)
	}
	out := Response{Date: q.Date, Records: append([]Record(nil), f[off:end]...), FetchedAt: s.now().UTC()}
	if end < len(f) {
		c := encode(end)
		out.NextCursor = &c
	}
	return out, nil
}
func (s *Store) Dates() (DatesResponse, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	es, e := os.ReadDir(s.dataDir)
	if e != nil {
		return DatesResponse{}, e
	}
	var out []DateSummary
	for _, x := range es {
		if x.IsDir() || !strings.HasPrefix(x.Name(), "generation-") || !strings.HasSuffix(x.Name(), ".jsonl") {
			continue
		}
		date := strings.TrimSuffix(strings.TrimPrefix(x.Name(), "generation-"), ".jsonl")
		rs, e := read(filepath.Join(s.dataDir, x.Name()))
		if e != nil {
			return DatesResponse{}, e
		}
		d := DateSummary{Date: date, Total: len(rs)}
		for _, r := range rs {
			if r.Outcome == OutcomeFailed || r.Outcome == OutcomeTimedOut {
				d.Failed++
			}
		}
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Date > out[j].Date })
	return DatesResponse{Days: out, FetchedAt: s.now().UTC()}, nil
}
func (s *Store) Prune() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	es, e := os.ReadDir(s.dataDir)
	if e != nil {
		return e
	}
	cut := s.now().UTC().AddDate(0, 0, -29).Format("2006-01-02")
	for _, x := range es {
		if strings.HasPrefix(x.Name(), "generation-") && strings.HasSuffix(x.Name(), ".jsonl") {
			d := strings.TrimSuffix(strings.TrimPrefix(x.Name(), "generation-"), ".jsonl")
			if d < cut {
				if err := os.Remove(filepath.Join(s.dataDir, x.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
		}
	}
	return nil
}
func (s *Store) append(r Record) error {
	b, e := json.Marshal(r)
	if e != nil {
		return e
	}
	b = append(b, '\n')
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	f, e := os.OpenFile(filepath.Join(s.dataDir, "generation-"+r.StartedAt.UTC().Format("2006-01-02")+".jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	_, e = f.Write(b)
	ce := f.Close()
	if e != nil {
		return e
	}
	return ce
}
func read(path string) ([]Record, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	var out []Record
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 16<<20)
	for sc.Scan() {
		var r Record
		if json.Unmarshal(sc.Bytes(), &r) == nil {
			out = append(out, r)
		}
	}
	return out, sc.Err()
}
func match(r Record, q Query) bool {
	return (q.ParentRequestID == "" || r.ParentRequestID == q.ParentRequestID) && (q.Account == "" || r.AccountID == q.Account || r.UpstreamAccountID == q.Account) && (q.Model == "" || r.Model == q.Model) && (q.Endpoint == "" || r.Endpoint == q.Endpoint) && (q.Transport == "" || r.Transport == q.Transport) && (q.Outcome == "" || r.Outcome == q.Outcome)
}
func terminal(o Outcome) Outcome {
	switch o {
	case OutcomeSucceeded, OutcomeFailed, OutcomeCancelled, OutcomeTimedOut:
		return o
	}
	return OutcomeFailed
}
func safe(v string) string {
	v = strings.TrimSpace(v)
	if len(v) > 200 {
		return v[:200]
	}
	return v
}
func truncate(v string) string {
	v = strings.TrimSpace(v)
	if len(v) > 2048 {
		return v[:2048] + "…"
	}
	return v
}
func cloneInt(v *int) *int {
	if v == nil {
		return nil
	}
	x := *v
	return &x
}
func encode(v int) string          { return fmt.Sprintf("%d", v) }
func decode(v string) (int, error) { var n int; _, e := fmt.Sscanf(v, "%d", &n); return n, e }
func randomSuffix() string         { return fmt.Sprintf("%x", time.Now().UnixNano()) }

func Sanitize(v any) any {
	b, e := json.Marshal(v)
	if e != nil {
		return map[string]any{"_omitted": "unserializable"}
	}
	var x any
	if json.Unmarshal(b, &x) != nil {
		return map[string]any{"_omitted": "non-json"}
	}
	sanitize(x)
	return x
}
func sanitize(v any) {
	switch x := v.(type) {
	case map[string]any:
		for k, val := range x {
			lk := strings.ToLower(strings.TrimSpace(k))
			if lk == "authorization" || lk == "cookie" || lk == "set-cookie" || lk == "headers" || lk == "proxy-authorization" || lk == "access_token" || lk == "refresh_token" || lk == "token" || lk == "api_key" || lk == "x-api-key" {
				x[k] = "<redacted>"
			} else {
				sanitize(val)
			}
		}
	case []any:
		for _, val := range x {
			sanitize(val)
		}
	}
}
