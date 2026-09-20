package codex

import (
	"net/http"
	"testing"

	"chatgpt-codex-proxy/internal/accounts"
)

func TestPassthroughHeadersPreserveRequestAndReplaceCredentials(t *testing.T) {
	t.Parallel()

	headers := PassthroughHeaders(accounts.Record{
		AccountID: "real-account",
		Token:     accounts.OAuthToken{AccessToken: "real-token"},
		Cookies:   map[string]string{"session": "real-cookie"},
	}, http.Header{
		"Authorization":      []string{"Bearer proxy-key"},
		"X-Api-Key":          []string{"proxy-key"},
		"Chatgpt-Account-Id": []string{"fake-account"},
		"Cookie":             []string{"fake-cookie=1"},
		"Content-Type":       []string{"application/custom+json"},
		"X-Codex-Turn-State": []string{"opaque-state"},
		"Connection":         []string{"X-Remove"},
		"X-Remove":           []string{"hop-value"},
	})

	if got := headers.Get("Authorization"); got != "Bearer real-token" {
		t.Fatalf("Authorization = %q", got)
	}
	if got := headers.Get("ChatGPT-Account-Id"); got != "real-account" {
		t.Fatalf("ChatGPT-Account-Id = %q", got)
	}
	if got := headers.Get("Cookie"); got != "session=real-cookie" {
		t.Fatalf("Cookie = %q", got)
	}
	if headers.Get("X-API-Key") != "" || headers.Get("X-Remove") != "" || headers.Get("Connection") != "" {
		t.Fatal("proxy credentials or hop-by-hop headers were retained")
	}
	if headers.Get("Content-Type") != "application/custom+json" || headers.Get("X-Codex-Turn-State") != "opaque-state" {
		t.Fatal("ordinary client headers were not preserved")
	}
}
