package server

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

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
