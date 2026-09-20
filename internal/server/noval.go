package server

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"chatgpt-codex-proxy/internal/accounts"
	"chatgpt-codex-proxy/internal/codex"
	"chatgpt-codex-proxy/internal/middleware"
)

func (a *App) handleNoValidation(c *gin.Context) {
	target, err := noValidationTarget(c.Param("path"), c.Request.URL.RawQuery)
	if err != nil {
		a.writeOpenAIError(c, http.StatusBadRequest, "invalid_request_error", err.Error(), "invalid_request_error")
		return
	}
	if websocket.IsWebSocketUpgrade(c.Request) {
		lease, err := a.accountMgr.AcquireReadyLease(c.Request.Context(), "")
		if err != nil {
			a.handleOpenStreamError(c, "noval", "", "", err)
			return
		}
		account := lease.Account
		a.setRequestAccount(c, account)
		a.handleNoValidationWebSocket(c, account, target, lease.Release)
		return
	}

	payload, err := io.ReadAll(c.Request.Body)
	if err != nil {
		a.writeOpenAIError(c, http.StatusBadRequest, "invalid_request_error", err.Error(), "invalid_request_error")
		return
	}
	lease, err := a.accountMgr.AcquireReadyLease(c.Request.Context(), "")
	if err != nil {
		a.handleOpenStreamError(c, "noval", "", "", err)
		return
	}
	account := lease.Account
	a.setRequestAccount(c, account)

	open := a.noValidationOpen
	if open == nil {
		open = a.httpClient.OpenPassthrough
	}
	response, err := open(c.Request.Context(), account, c.Request.Method, target, c.Request.Header, payload)
	if err != nil {
		lease.Release()
		a.handleOpenStreamError(c, "noval", account.ID, account.ID, err)
		return
	}
	response.Body = &releaseReadCloser{ReadCloser: response.Body, release: lease.Release}
	a.relayNoValidationResponse(c, account.ID, response)
}

func (a *App) relayNoValidationResponse(c *gin.Context, accountID string, response *http.Response) {
	defer response.Body.Close()

	copyNoValidationResponseHeaders(c.Writer.Header(), response.Header)
	a.observeQuotaSnapshot(accountID, codex.ParseQuotaFromHeaders(response.Header))
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
		a.accounts.NoteSuccess(accountID)
	} else {
		// Preserve the upstream response while still keeping account health and
		// cooldown state consistent with the translated endpoints.
		a.classifyUpstreamError(accountID, codex.NewUpstreamError("codex passthrough", response.StatusCode, "", response.Header))
	}
}

func (a *App) handleNoValidationWebSocket(c *gin.Context, account accounts.Record, target string, release func()) {
	endpoint, err := noValidationWebSocketEndpoint(a.cfg.CodexBaseURL, target)
	if err != nil {
		release()
		a.handleOpenStreamError(c, "noval", account.ID, account.ID, err)
		return
	}

	headers := codex.PassthroughHeaders(account, c.Request.Header)
	for _, key := range []string{"Sec-WebSocket-Key", "Sec-WebSocket-Version", "Sec-WebSocket-Extensions"} {
		headers.Del(key)
	}
	upstream, response, err := websocket.DefaultDialer.DialContext(c.Request.Context(), endpoint, headers)
	if err != nil {
		if response != nil {
			response.Body = &releaseReadCloser{ReadCloser: response.Body, release: release}
			a.relayNoValidationResponse(c, account.ID, response)
			return
		}
		release()
		a.handleOpenStreamError(c, "noval", account.ID, account.ID, err)
		return
	}
	defer upstream.Close()

	responseHeaders := make(http.Header)
	copyNoValidationResponseHeaders(responseHeaders, response.Header)
	responseHeaders.Del("Sec-WebSocket-Accept")
	responseHeaders.Del("Sec-WebSocket-Extensions")
	downstream, err := responsesWebSocketUpgrader.Upgrade(c.Writer, c.Request, responseHeaders)
	if err != nil {
		release()
		return
	}
	defer downstream.Close()

	a.observeQuotaSnapshot(account.ID, codex.ParseQuotaFromHeaders(response.Header))
	a.accounts.NoteSuccess(account.ID)
	errCh := make(chan error, 2)
	go relayNoValidationWebSocket(upstream, downstream, errCh)
	go relayNoValidationWebSocket(downstream, upstream, errCh)

	select {
	case <-c.Request.Context().Done():
	case <-errCh:
	}
	// Closing both sides unblocks the other relay goroutine immediately.
	_ = downstream.Close()
	_ = upstream.Close()
	release()
	middleware.MarkActivityFinalizing(c)
}

func relayNoValidationWebSocket(dst, src *websocket.Conn, result chan<- error) {
	for {
		messageType, payload, err := src.ReadMessage()
		if err != nil {
			if closeErr, ok := err.(*websocket.CloseError); ok {
				_ = dst.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(closeErr.Code, closeErr.Text), time.Now().Add(time.Second))
			}
			result <- err
			return
		}
		if err := dst.WriteMessage(messageType, payload); err != nil {
			result <- err
			return
		}
	}
}

func noValidationWebSocketEndpoint(baseURL, target string) (string, error) {
	endpoint, err := url.Parse(codex.JoinURL(baseURL, target))
	if err != nil {
		return "", err
	}
	switch endpoint.Scheme {
	case "http":
		endpoint.Scheme = "ws"
	case "https":
		endpoint.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", errors.New("unsupported upstream WebSocket URL scheme")
	}
	return endpoint.String(), nil
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
