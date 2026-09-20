package server

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"chatgpt-codex-proxy/internal/codex"
	"chatgpt-codex-proxy/internal/middleware"
)

func (a *App) handleNoValidation(c *gin.Context) {
	target, err := noValidationTarget(c.Param("path"), c.Request.URL.RawQuery)
	if err != nil {
		a.writeOpenAIError(c, http.StatusBadRequest, "invalid_request_error", err.Error(), "invalid_request_error")
		return
	}
	payload, err := io.ReadAll(c.Request.Body)
	if err != nil {
		a.writeOpenAIError(c, http.StatusBadRequest, "invalid_request_error", err.Error(), "invalid_request_error")
		return
	}

	account, err := a.accountMgr.AcquireReady(c.Request.Context(), "")
	if err != nil {
		a.handleOpenStreamError(c, "noval", "", "", err)
		return
	}
	a.setRequestAccount(c, account)

	open := a.noValidationOpen
	if open == nil {
		open = a.httpClient.OpenPassthrough
	}
	response, err := open(c.Request.Context(), account, c.Request.Method, target, c.Request.Header, payload)
	if err != nil {
		a.handleOpenStreamError(c, "noval", account.ID, account.ID, err)
		return
	}
	defer response.Body.Close()

	copyNoValidationResponseHeaders(c.Writer.Header(), response.Header)
	a.observeQuotaSnapshot(account.ID, codex.ParseQuotaFromHeaders(response.Header))
	c.Status(response.StatusCode)

	buffer := make([]byte, 32*1024)
	for {
		n, readErr := response.Body.Read(buffer)
		if n > 0 {
			if _, writeErr := c.Writer.Write(buffer[:n]); writeErr != nil {
				return
			}
			c.Writer.Flush()
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				middleware.SetRequestOutcome(c, "stream_error")
			}
			break
		}
	}
	middleware.MarkActivityFinalizing(c)
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		a.accounts.NoteSuccess(account.ID)
	} else {
		// Preserve the upstream response while still keeping account health and
		// cooldown state consistent with the translated endpoints.
		a.classifyUpstreamError(account.ID, codex.NewUpstreamError("codex passthrough", response.StatusCode, "", response.Header))
	}
}

func noValidationTarget(path, rawQuery string) (string, error) {
	if path != "" && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	for _, segment := range strings.Split(path, "/") {
		if segment == "." || segment == ".." {
			return "", errors.New("path traversal is not allowed")
		}
	}
	target := "/codex" + path
	if rawQuery != "" {
		target += "?" + rawQuery
	}
	return target, nil
}

func copyNoValidationResponseHeaders(dst, src http.Header) {
	clean := src.Clone()
	codexRemoveHopByHopHeaders(clean)
	clean.Del("Set-Cookie")
	switch strings.ToLower(strings.TrimSpace(clean.Get("Content-Encoding"))) {
	case "gzip", "br", "zstd":
		// httpcloak transparently decodes these response bodies.
		clean.Del("Content-Encoding")
		clean.Del("Content-Length")
	}
	for key, values := range clean {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

// Kept server-local so the transport's header sanitizer does not need to be
// exported solely for response relaying.
func codexRemoveHopByHopHeaders(headers http.Header) {
	for _, value := range headers.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			if token = strings.TrimSpace(token); token != "" {
				headers.Del(token)
			}
		}
	}
	for _, key := range []string{"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "TE", "Trailer", "Transfer-Encoding", "Upgrade"} {
		headers.Del(key)
	}
}
