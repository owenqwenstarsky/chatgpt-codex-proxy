package server

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"chatgpt-codex-proxy/internal/accountmanager"
	"chatgpt-codex-proxy/internal/accounts"
	"chatgpt-codex-proxy/internal/config"
)

func TestNoValidationForwardsOpaqueRequestAndResponse(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	cfg := config.Config{ProxyAPIKey: "proxy-secret"}
	accountsSvc := newServerAccounts(t, &accounts.Record{
		ID:        "acct-1",
		AccountID: "upstream-account",
		Status:    accounts.StatusActive,
		Token: accounts.OAuthToken{
			AccessToken: "upstream-token",
			ExpiresAt:   time.Now().Add(time.Hour),
		},
		Cookies: map[string]string{"session": "upstream-cookie"},
	})
	app := &App{
		cfg:        cfg,
		engine:     gin.New(),
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		accounts:   accountsSvc,
		accountMgr: accountmanager.NewAccountManager(cfg, accountsSvc, nil, nil, nil),
	}

	wantBody := []byte("not-json\x00still-opaque")
	app.noValidationOpen = func(_ context.Context, record accounts.Record, method, target string, headers http.Header, payload []byte) (*http.Response, error) {
		if record.ID != "acct-1" {
			t.Fatalf("account = %q, want acct-1", record.ID)
		}
		if method != http.MethodPatch {
			t.Fatalf("method = %q, want PATCH", method)
		}
		if target != "/codex/responses?mode=raw%2Ftest" {
			t.Fatalf("target = %q", target)
		}
		if !bytes.Equal(payload, wantBody) {
			t.Fatalf("payload = %q, want %q", payload, wantBody)
		}
		if headers.Get("Authorization") != "Bearer client-placeholder" {
			t.Fatalf("handler should receive original headers")
		}
		return &http.Response{
			StatusCode: http.StatusTeapot,
			Header: http.Header{
				"Content-Type": []string{"application/octet-stream"},
				"X-Upstream":   []string{"kept"},
				"Set-Cookie":   []string{"secret=must-not-leak"},
				"Connection":   []string{"X-Hop"},
				"X-Hop":        []string{"removed"},
			},
			Body: io.NopCloser(bytes.NewReader([]byte("upstream-body"))),
		}, nil
	}
	app.routes()

	req := httptest.NewRequest(http.MethodPatch, "/noval/v1/responses?mode=raw%2Ftest", bytes.NewReader(wantBody))
	req.Header.Set("X-API-Key", "proxy-secret")
	req.Header.Set("Authorization", "Bearer client-placeholder")
	recorder := httptest.NewRecorder()
	app.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusTeapot)
	}
	if recorder.Body.String() != "upstream-body" {
		t.Fatalf("body = %q", recorder.Body.String())
	}
	if recorder.Header().Get("X-Upstream") != "kept" {
		t.Fatal("upstream response header was not relayed")
	}
	if recorder.Header().Get("Set-Cookie") != "" || recorder.Header().Get("X-Hop") != "" {
		t.Fatal("unsafe response headers were relayed")
	}
}

func TestNoValidationTarget(t *testing.T) {
	t.Parallel()

	target, err := noValidationTarget("/models", "client_version=1%2E2")
	if err != nil || target != "/codex/models?client_version=1%2E2" {
		t.Fatalf("target = %q, err = %v", target, err)
	}
	if _, err := noValidationTarget("/../usage", ""); err == nil {
		t.Fatal("path traversal should be rejected")
	}
}

func TestNoValidationWebSocketForwardsOpaqueMessages(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	received := make(chan []byte, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RequestURI() != "/backend-api/codex/responses?mode=raw%2Ftest" {
			t.Errorf("upstream target = %q", r.URL.RequestURI())
		}
		if r.Header.Get("Authorization") != "Bearer upstream-token" {
			t.Errorf("upstream authorization = %q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("ChatGPT-Account-Id") != "upstream-account" {
			t.Errorf("upstream account = %q", r.Header.Get("ChatGPT-Account-Id"))
		}
		if r.Header.Get("Cookie") != "session=upstream-cookie" {
			t.Errorf("upstream cookie = %q", r.Header.Get("Cookie"))
		}
		if r.Header.Get("X-API-Key") != "" {
			t.Errorf("proxy API key leaked upstream")
		}

		conn, err := upgrader.Upgrade(w, r, http.Header{
			"Sec-WebSocket-Protocol": []string{"opaque.v1"},
			"X-Upstream":             []string{"kept"},
			"Set-Cookie":             []string{"secret=must-not-leak"},
		})
		if err != nil {
			t.Errorf("upgrade upstream: %v", err)
			return
		}
		defer conn.Close()
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			t.Errorf("read upstream message: %v", err)
			return
		}
		if messageType != websocket.BinaryMessage {
			t.Errorf("upstream message type = %d, want binary", messageType)
		}
		received <- append([]byte(nil), payload...)
		if err := conn.WriteMessage(websocket.TextMessage, []byte("opaque-reply\x00")); err != nil {
			t.Errorf("write upstream message: %v", err)
		}
	}))
	defer upstream.Close()

	cfg := config.Config{ProxyAPIKey: "proxy-secret", CodexBaseURL: upstream.URL + "/backend-api"}
	accountsSvc := newServerAccounts(t, &accounts.Record{
		ID:        "acct-1",
		AccountID: "upstream-account",
		Status:    accounts.StatusActive,
		Token: accounts.OAuthToken{
			AccessToken: "upstream-token",
			ExpiresAt:   time.Now().Add(time.Hour),
		},
		Cookies: map[string]string{"session": "upstream-cookie"},
	})
	app := &App{
		cfg:        cfg,
		engine:     gin.New(),
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		accounts:   accountsSvc,
		accountMgr: accountmanager.NewAccountManager(cfg, accountsSvc, nil, nil, nil),
	}
	app.routes()
	proxy := httptest.NewServer(app.Handler())
	defer proxy.Close()

	headers := http.Header{
		"X-API-Key":              []string{"proxy-secret"},
		"Authorization":          []string{"Bearer client-placeholder"},
		"Sec-WebSocket-Protocol": []string{"opaque.v1"},
	}
	endpoint := "ws" + strings.TrimPrefix(proxy.URL, "http") + "/noval/v1/responses?mode=raw%2Ftest"
	conn, response, err := websocket.DefaultDialer.Dial(endpoint, headers)
	if err != nil {
		t.Fatalf("connect noval websocket: %v", err)
	}
	defer conn.Close()
	if conn.Subprotocol() != "opaque.v1" {
		t.Fatalf("subprotocol = %q", conn.Subprotocol())
	}
	if response.Header.Get("X-Upstream") != "kept" {
		t.Fatal("upstream handshake header was not relayed")
	}
	if response.Header.Get("Set-Cookie") != "" {
		t.Fatal("upstream cookie leaked to client")
	}

	want := []byte("not-json\x00still-opaque")
	if err := conn.WriteMessage(websocket.BinaryMessage, want); err != nil {
		t.Fatalf("write client message: %v", err)
	}
	messageType, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read client message: %v", err)
	}
	if messageType != websocket.TextMessage || !bytes.Equal(payload, []byte("opaque-reply\x00")) {
		t.Fatalf("relayed response type = %d, payload = %q", messageType, payload)
	}
	if got := <-received; !bytes.Equal(got, want) {
		t.Fatalf("upstream payload = %q, want %q", got, want)
	}
}

func TestNoValidationWebSocketEndpoint(t *testing.T) {
	t.Parallel()

	got, err := noValidationWebSocketEndpoint("https://chatgpt.example/backend-api/", "/codex/responses?mode=raw")
	if err != nil || got != "wss://chatgpt.example/backend-api/codex/responses?mode=raw" {
		t.Fatalf("endpoint = %q, err = %v", got, err)
	}
}

func TestNoValidationWebSocketRelaysUpstreamHandshakeRejection(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Upstream", "rejected")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("opaque rejection"))
	}))
	defer upstream.Close()

	cfg := config.Config{ProxyAPIKey: "proxy-secret", CodexBaseURL: upstream.URL}
	accountsSvc := newServerAccounts(t, &accounts.Record{
		ID:     "acct-1",
		Status: accounts.StatusActive,
		Token: accounts.OAuthToken{
			AccessToken: "upstream-token",
			ExpiresAt:   time.Now().Add(time.Hour),
		},
	})
	app := &App{
		cfg:        cfg,
		engine:     gin.New(),
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		accounts:   accountsSvc,
		accountMgr: accountmanager.NewAccountManager(cfg, accountsSvc, nil, nil, nil),
	}
	app.routes()
	proxy := httptest.NewServer(app.Handler())
	defer proxy.Close()

	endpoint := "ws" + strings.TrimPrefix(proxy.URL, "http") + "/noval/v1/responses"
	_, response, err := websocket.DefaultDialer.Dial(endpoint, http.Header{"X-API-Key": []string{"proxy-secret"}})
	if err == nil {
		t.Fatal("websocket handshake unexpectedly succeeded")
	}
	if response == nil {
		t.Fatal("missing relayed handshake response")
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(response.Body)
	if readErr != nil {
		t.Fatalf("read rejection: %v", readErr)
	}
	if response.StatusCode != http.StatusTeapot || string(body) != "opaque rejection" {
		t.Fatalf("status = %d, body = %q", response.StatusCode, body)
	}
	if response.Header.Get("X-Upstream") != "rejected" {
		t.Fatal("upstream rejection header was not relayed")
	}
}
