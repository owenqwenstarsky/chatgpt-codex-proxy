package codex

import (
	"bytes"
	"context"
	"net/http"
	"strings"

	httpcloak "github.com/sardanioss/httpcloak/client"

	"chatgpt-codex-proxy/internal/accounts"
)

// OpenPassthrough sends an opaque request to an endpoint under the upstream
// /codex namespace. Upstream HTTP errors are returned as ordinary responses so
// callers can relay their status, headers, and body without translating them.
func (c *HTTPClient) OpenPassthrough(ctx context.Context, record accounts.Record, method, target string, incoming http.Header, payload []byte) (*http.Response, error) {
	headers := PassthroughHeaders(record, incoming)
	resp, err := c.sessionFor(record.ID).DoStream(ctx, &httpcloak.Request{
		Method:  method,
		URL:     JoinURL(c.cfg.CodexBaseURL, target),
		Headers: headers,
		Body:    bytes.NewReader(payload),
	})
	if err != nil {
		return nil, err
	}

	return &http.Response{
		StatusCode: resp.StatusCode,
		Header:     CanonicalHeader(resp.Headers),
		Body:       resp,
	}, nil
}

// PassthroughHeaders preserves caller headers unless they are connection-local
// or contain credentials belonging to this proxy. Account credentials always
// replace their client-provided counterparts.
func PassthroughHeaders(record accounts.Record, incoming http.Header) http.Header {
	headers := incoming.Clone()
	removeHopByHopHeaders(headers)
	for _, key := range []string{
		"Authorization",
		"ChatGPT-Account-Id",
		"Content-Length",
		"Cookie",
		"Host",
		"Proxy-Authorization",
		"X-API-Key",
	} {
		headers.Del(key)
	}

	defaults := BuildHeaders(record.Token.AccessToken, HeaderOptions{
		AccountID: record.AccountID,
		Cookies:   record.Cookies,
		RequestID: NewRequestID(),
	})
	for key, values := range defaults {
		if _, exists := headers[key]; !exists {
			headers[key] = append([]string(nil), values...)
		}
	}

	// These values must never be supplied by the proxy's caller.
	headers.Set("Authorization", "Bearer "+record.Token.AccessToken)
	if record.AccountID != "" {
		headers.Set("ChatGPT-Account-Id", record.AccountID)
	}
	if cookie := defaults.Get("Cookie"); cookie != "" {
		headers.Set("Cookie", cookie)
	}
	return headers
}

func removeHopByHopHeaders(headers http.Header) {
	for _, value := range headers.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			if token = strings.TrimSpace(token); token != "" {
				headers.Del(token)
			}
		}
	}
	for _, key := range []string{
		"Connection",
		"Keep-Alive",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"TE",
		"Trailer",
		"Transfer-Encoding",
		"Upgrade",
	} {
		headers.Del(key)
	}
}
