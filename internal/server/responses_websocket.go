package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"chatgpt-codex-proxy/internal/accountmanager"
	"chatgpt-codex-proxy/internal/accounts"
	"chatgpt-codex-proxy/internal/codex"
	"chatgpt-codex-proxy/internal/jsonutil"
	"chatgpt-codex-proxy/internal/middleware"
	"chatgpt-codex-proxy/internal/models"
	"chatgpt-codex-proxy/internal/openai"
	"chatgpt-codex-proxy/internal/turn"
)

var responsesWebSocketUpgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

const maxWebSocketMessageBytes = 64 << 20

type responsesWebSocketStream interface {
	eventStream
	SendJSON(any) error
}

type responsesWebSocketConnector func(context.Context, string, http.Header, any) (responsesWebSocketStream, error)

type responsesWebSocketSession struct {
	stream         responsesWebSocketStream
	account        accounts.Record
	lastResponseID string
}

func (a *App) connectResponsesWebSocket(ctx context.Context, endpoint string, headers http.Header, body any) (responsesWebSocketStream, error) {
	if a.wsConnector != nil {
		return a.wsConnector(ctx, endpoint, headers, body)
	}
	return codex.ConnectWS(ctx, endpoint, headers, body)
}

func (a *App) handleResponsesWebSocket(c *gin.Context) {
	conn, err := responsesWebSocketUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.SetReadLimit(maxWebSocketMessageBytes)

	session := responsesWebSocketSession{}
	defer func() {
		if session.stream != nil {
			_ = session.stream.Close()
		}
	}()

	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
			if !writeResponsesWebSocketError(conn, http.StatusBadRequest, "invalid_websocket_event", "Responses WebSocket events must contain JSON", "invalid_request_error", "") {
				return
			}
			continue
		}

		a.logIncomingPayload(c, "responses_websocket", message)
		normalized, err := normalizeResponsesWebSocketMessage(message, a.modelCatalog())
		if err != nil {
			status, code, message, param := responsesWebSocketRequestError(err)
			if !writeResponsesWebSocketError(conn, status, code, message, "invalid_request_error", param) {
				return
			}
			continue
		}

		if !a.handleResponsesWebSocketTurn(c, conn, normalized, &session) {
			return
		}
	}
}

type responsesWebSocketEnvelope struct {
	Type       string          `json:"type"`
	Background bool            `json:"background"`
	Generate   *bool           `json:"generate"`
	Input      json.RawMessage `json:"input"`
}

var errResponsesWebSocketBackground = errors.New("background responses are not supported over WebSocket")

func normalizeResponsesWebSocketMessage(body []byte, catalog *models.Catalog) (turn.NormalizedRequest, error) {
	var envelope responsesWebSocketEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return turn.NormalizedRequest{}, err
	}
	eventType := strings.TrimSpace(envelope.Type)
	if eventType != "response.create" && eventType != "response.append" {
		return turn.NormalizedRequest{}, fmt.Errorf("unsupported websocket event type %q", envelope.Type)
	}
	if envelope.Background {
		return turn.NormalizedRequest{}, errResponsesWebSocketBackground
	}

	// The regular Responses normalizer intentionally ignores transport-only
	// fields. WebSocket streaming is implicit, while generate belongs only to
	// the upstream WebSocket payload.
	normalized, err := normalizeResponsesBody(body, catalog)
	if err != nil {
		return turn.NormalizedRequest{}, err
	}
	normalized.Stream = true
	normalized.Generate = envelope.Generate
	normalized.WebSocketAppend = eventType == "response.append"
	if normalized.WebSocketAppend {
		input := bytes.TrimSpace(envelope.Input)
		if len(input) == 0 || input[0] != '[' {
			return turn.NormalizedRequest{}, errors.New("response.append requires array field: input")
		}
	}
	return normalized, nil
}

func (a *App) handleResponsesWebSocketTurn(c *gin.Context, conn *websocket.Conn, normalized turn.NormalizedRequest, session *responsesWebSocketSession) bool {
	if normalized.WebSocketAppend && session.lastResponseID == "" {
		return writeResponsesWebSocketError(conn, http.StatusBadRequest, "invalid_request_error", "response.append received before response.create", "invalid_request_error", "")
	}
	implicitSocketResume := false
	if normalized.PreviousResponseID == "" && session.lastResponseID != "" && (normalized.WebSocketAppend || !hasPriorAssistantOrToolHistory(normalized.Input)) {
		normalized.PreviousResponseID = session.lastResponseID
		implicitSocketResume = true
	}
	resolution, err := a.resolveSession(normalized)
	if err != nil {
		code := "invalid_request_error"
		param := ""
		if errors.Is(err, errInvalidPreviousResponseID) {
			code = "previous_response_not_found"
			param = "previous_response_id"
		}
		return writeResponsesWebSocketError(conn, http.StatusBadRequest, code, err.Error(), "invalid_request_error", param)
	}
	// An append (and the implicit follow-up form used by the public socket) is
	// stateful at the upstream.  It must remain on the account which owns the
	// persistent connection, even when another account has spare capacity.
	if session.stream != nil && resolution.PreferredAccountID == "" && (normalized.WebSocketAppend || implicitSocketResume || resolution.ImplicitResume) {
		resolution.PreferredAccountID = session.account.ID
		resolution.ImplicitResume = true
	}
	if key := strings.TrimSpace(resolution.ConversationKey); key != "" {
		c.Set(stickyThreadConversationKey, key)
	}

	body := resolution.Request.ToCodexWSCreatePayload()
	accumulator := turn.NewAccumulator(resolution.Request)
	var tupleTextBuffer strings.Builder
	var started time.Time
	attempted := make(map[string]struct{})
	attempts := 0
	var account accounts.Record
	var turnLease *accountmanager.Lease
	var firstEvent *codex.StreamEvent

	// Do not expose an event until the upstream has proved that this attempt is
	// usable.  Before that boundary the request can safely be sent again.
	for firstEvent == nil {
		turnLease, err = a.acquireWebSocketTurnLease(c.Request.Context(), conn, &resolution, attempted)
		if turnLease != nil {
			account = turnLease.Account
		}
		attempts++
		if err == nil {
			a.setRequestAccount(c, account)
			if session.stream == nil || session.account.ID != account.ID {
				if session.stream != nil {
					_ = session.stream.Close()
				}
				headers := codex.BuildHeaders(account.Token.AccessToken, codex.HeaderOptions{AccountID: account.AccountID, Cookies: account.Cookies, TurnState: resolution.TurnState, RequestID: codex.NewRequestID(), IncludeBeta: true})
				a.logUpstreamPayload(c, "responses_websocket", "websocket", account.ID, body)
				session.stream, err = a.connectResponsesWebSocket(c.Request.Context(), websocketEndpoint(a.cfg.CodexBaseURL), headers, body)
				if err == nil {
					session.account = account
					a.observeQuotaSnapshot(account.ID, codex.ParseQuotaFromHeaders(session.stream.Headers()))
				}
			} else {
				a.logUpstreamPayload(c, "responses_websocket", "websocket", account.ID, body)
				err = session.stream.SendJSON(body)
			}
			if err == nil {
				var upstreamErr bool
				firstEvent, upstreamErr, err = a.nextStreamEvent(c.Request.Context(), account, accumulator, session.stream)
				_ = upstreamErr // classification below is intentionally identical for open and event failures.
			}
		}
		if err == nil {
			break
		}
		if turnLease != nil {
			turnLease.Release()
			turnLease = nil
		}
		if err == io.EOF {
			err = errIncompleteResponse
		}
		err = normalizeRequestContextError(c.Request.Context(), err)
		if a.recordRequestCancellation(c, account.ID, accumulator.ResponseID, err) {
			return false
		}
		if account.ID != "" {
			a.classifyUpstreamError(account.ID, err)
			a.noteStickyThreadQuotaFailure(resolution.ConversationKey, account.ID, err)
			a.logUpstreamStreamFailure(c, "responses_websocket", account.ID, accumulator.ResponseID, err)
		}
		if session.stream != nil {
			_ = session.stream.Close()
			session.stream = nil
			session.account = accounts.Record{}
		}

		if account.ID != "" && shouldFailoverRequest(err) {
			attempted[account.ID] = struct{}{}
			if resolution.ExplicitPrevious || resolution.ImplicitResume {
				if isRateLimitCapacityFailure(err) {
					if retry, recoveryErr := a.waitForCapacityRecovery(c.Request.Context(), "responses_websocket", rateLimitRecoveryStart(&started), attempts, a.recoveryAllowForResolution(&resolution)); recoveryErr != nil {
						return a.writeResponsesWebSocketRecoveryError(conn, recoveryErr)
					} else if retry {
						clear(attempted)
						continue
					}
				}
				status, code, message := a.responsesWebSocketOpenError(c, account.ID, err)
				return writeResponsesWebSocketError(conn, status, code, message, "api_error", "")
			}
			continue
		}
		if retry, recoveryErr := a.waitForCapacityRecovery(c.Request.Context(), "responses_websocket", rateLimitRecoveryStart(&started), attempts, a.recoveryAllowForResolution(&resolution)); recoveryErr != nil {
			return a.writeResponsesWebSocketRecoveryError(conn, recoveryErr)
		} else if retry {
			clear(attempted)
			continue
		}
		status, code, message := a.responsesWebSocketOpenError(c, account.ID, err)
		return writeResponsesWebSocketError(conn, status, code, message, "api_error", "")
	}
	defer turnLease.Release()

	for event := firstEvent; ; {
		for _, outgoing := range a.responsesStreamEvents(c, accumulator, resolution.Request, &tupleTextBuffer, event) {
			payload := turn.ResponseEventJSON(outgoing.Type, accumulator.ResponseID, outgoing.Payload)
			if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return false
			}
		}
		if event.IsTerminalResponse() {
			break
		}
		for {
			event, upstreamErr, err := a.nextStreamEvent(c.Request.Context(), account, accumulator, session.stream)
			if err != nil {
				if err == io.EOF {
					err = errIncompleteResponse
				}
				if a.recordRequestCancellation(c, account.ID, accumulator.ResponseID, err) {
					return false
				}
				// A failed turn cannot safely share its upstream connection with a
				// later client turn, regardless of whether the failure was encoded
				// as an upstream event or a transport error.
				_ = upstreamErr
				_ = session.stream.Close()
				session.stream = nil
				session.account = accounts.Record{}
				status, code, message := a.classifyUpstreamError(account.ID, err)
				a.noteStickyThreadQuotaFailure(resolution.ConversationKey, account.ID, err)
				a.logUpstreamStreamFailure(c, "responses_websocket", account.ID, accumulator.ResponseID, err)
				middleware.SetRequestOutcome(c, "upstream_error")
				middleware.SetRequestError(c, code, message)
				middleware.SetRequestResponseID(c, accumulator.ResponseID)
				return writeResponsesWebSocketError(conn, status, code, message, "api_error", "")
			}

			for _, outgoing := range a.responsesStreamEvents(c, accumulator, resolution.Request, &tupleTextBuffer, event) {
				payload := turn.ResponseEventJSON(outgoing.Type, accumulator.ResponseID, outgoing.Payload)
				if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
					return false
				}
			}
			if event.IsTerminalResponse() {
				break
			}
		}
		break
	}

	a.finalizeSuccessfulStream(account.ID, accumulator, session.stream)
	session.lastResponseID = accumulator.ResponseID
	return true
}

func (a *App) acquireWebSocketTurnLease(ctx context.Context, conn *websocket.Conn, resolution *sessionResolution, attempted map[string]struct{}) (*accountmanager.Lease, error) {
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		lease *accountmanager.Lease
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		lease, err := a.acquireLeaseForResolution(waitCtx, resolution, attempted)
		resultCh <- result{lease: lease, err: err}
	}()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case value := <-resultCh:
			return value.lease, value.err
		case <-ctx.Done():
			cancel()
			value := <-resultCh
			if value.lease != nil {
				value.lease.Release()
			}
			return nil, ctx.Err()
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second)); err != nil {
				cancel()
				value := <-resultCh
				if value.lease != nil {
					value.lease.Release()
				}
				return nil, err
			}
		}
	}
}

func (a *App) writeResponsesWebSocketRecoveryError(conn *websocket.Conn, err error) bool {
	delay, ok := rateLimitRecoveryRetryAfter(err)
	if !ok {
		status, code, message := a.responsesWebSocketOpenError(nil, "", err)
		return writeResponsesWebSocketError(conn, status, code, message, "api_error", "")
	}
	payload := map[string]any{
		"type": "error", "status": http.StatusTooManyRequests, "retry_after": delay,
		"error": middleware.OpenAIErrorBody{Message: "all eligible accounts are rate limited; retry later", Type: "api_error", Code: "rate_limited"},
	}
	return conn.WriteJSON(payload) == nil
}

func (a *App) responsesStreamEvents(c *gin.Context, accumulator *turn.Accumulator, normalized turn.NormalizedRequest, tupleTextBuffer *strings.Builder, event *codex.StreamEvent) []turn.ResponseStreamEvent {
	if normalized.TupleSchema != nil {
		switch event.Type {
		case "response.output_text.delta":
			tupleTextBuffer.WriteString(jsonutil.StringValue(event.Raw["delta"]))
			return nil
		case "response.output_text.done":
			if text := jsonutil.StringValue(event.Raw["text"]); text != "" {
				tupleTextBuffer.Reset()
				tupleTextBuffer.WriteString(text)
			}
			return nil
		}
	}

	if toolEvents, handled := accumulator.ResponsesStreamEventsForEvent(event); handled {
		return toolEvents
	}

	events := make([]turn.ResponseStreamEvent, 0, 3)
	if event.IsTerminalResponse() {
		if normalized.TupleSchema != nil && strings.TrimSpace(tupleTextBuffer.String()) != "" {
			reconverted := tupleTextBuffer.String()
			if patched, err := openai.ReconvertJSONText(reconverted, normalized.TupleSchema); err != nil {
				a.logTupleReconversionWarning(c, "responses", accumulator.ResponseID, err)
			} else {
				reconverted = patched
			}
			events = append(events, turn.ResponseStreamEvent{
				Type:    "response.output_text.delta",
				Payload: map[string]any{"delta": reconverted},
			})
		}
		events = append(events, accumulator.PendingResponseToolCallCompletionEvents()...)
	}

	payload := responseStreamPayload(event, accumulator)
	if normalized.TupleSchema != nil && event.IsTerminalResponse() {
		if err := openai.PatchResponsesObjectForTuple(jsonutil.MapValue(payload, "response"), normalized.TupleSchema); err != nil {
			a.logTupleReconversionWarning(c, "responses", accumulator.ResponseID, err)
		}
	}
	return append(events, turn.ResponseStreamEvent{Type: event.Type, Payload: payload})
}

func responsesWebSocketRequestError(err error) (int, string, string, string) {
	var modelErr *openai.ModelNotFoundError
	if errors.As(err, &modelErr) {
		return http.StatusNotFound, "model_not_found", "Model '" + strings.TrimSpace(modelErr.Model) + "' not found", "model"
	}
	var contentErr *openai.UnsupportedContentPartError
	if errors.As(err, &contentErr) {
		return http.StatusBadRequest, "unsupported_content_part", contentErr.Error(), "input"
	}
	if errors.Is(err, errResponsesWebSocketBackground) {
		return http.StatusBadRequest, "unsupported_value", err.Error(), "background"
	}
	return http.StatusBadRequest, "invalid_request_error", err.Error(), ""
}

func (a *App) responsesWebSocketOpenError(c *gin.Context, accountID string, err error) (int, string, string) {
	if errors.Is(err, accounts.ErrThreadQuotaExhausted) {
		return http.StatusPaymentRequired, "quota_exhausted", "upstream account quota exhausted"
	}
	if errors.Is(err, errContinuationAccountUnavailable) {
		return http.StatusServiceUnavailable, "continuation_account_unavailable", "continuation account unavailable"
	}
	if strings.Contains(strings.ToLower(err.Error()), "no active accounts") {
		return http.StatusServiceUnavailable, "no_available_accounts", "no available accounts"
	}
	status, code, message := a.classifyUpstreamError(accountID, err)
	a.logUpstreamRequestFailure(c, "responses_websocket", accountID, status, code, err)
	return status, code, message
}

func writeResponsesWebSocketError(conn *websocket.Conn, status int, code, message, errorType, param string) bool {
	payload := map[string]any{
		"type":   "error",
		"status": status,
		"error": middleware.OpenAIErrorBody{
			Message: message,
			Type:    errorType,
			Code:    code,
			Param:   param,
		},
	}
	return conn.WriteJSON(payload) == nil
}
