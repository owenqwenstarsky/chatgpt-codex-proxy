package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/gin-gonic/gin"

	"chatgpt-codex-proxy/internal/middleware"
	"chatgpt-codex-proxy/internal/turn"
)

func (a *App) logUpstreamRequestFailure(c *gin.Context, endpoint, accountID string, status int, code string, err error) {
	if a == nil || a.logger == nil || err == nil {
		return
	}

	attrs := contextLogAttrs(c, endpoint)
	attrs = append(attrs,
		"status", status,
		"error_code", code,
		"error", strings.TrimSpace(err.Error()),
	)
	attrs = appendStringAttr(attrs, "account_id", accountID)

	a.logger.Error("upstream request failed", attrs...)
}

func (a *App) logUpstreamStreamFailure(c *gin.Context, endpoint, accountID, responseID string, err error) {
	if a == nil || a.logger == nil || err == nil {
		return
	}

	attrs := contextLogAttrs(c, endpoint)
	attrs = append(attrs, "error", strings.TrimSpace(err.Error()))
	attrs = appendStringAttr(attrs, "account_id", accountID)
	attrs = appendStringAttr(attrs, "response_id", responseID)

	a.logger.Error("upstream stream failed", attrs...)
}

func (a *App) logTupleReconversionWarning(c *gin.Context, endpoint, responseID string, err error) {
	if a == nil || a.logger == nil || err == nil {
		return
	}

	attrs := contextLogAttrs(c, endpoint)
	attrs = append(attrs, "error", strings.TrimSpace(err.Error()))
	attrs = appendStringAttr(attrs, "response_id", responseID)
	a.logger.Warn("tuple reconversion failed", attrs...)
}

func (a *App) logIncomingPayload(c *gin.Context, endpoint string, payload []byte) {
	if a == nil || a.logger == nil || !a.cfg.DebugLogPayloads {
		return
	}
	formatted := formatPayloadForLog(payload)
	if formatted == "" {
		return
	}

	attrs := contextLogAttrs(c, endpoint)
	attrs = append(attrs,
		"direction", "incoming",
		"payload", formatted,
	)
	a.logger.Info("payload debug", attrs...)
}

func (a *App) logUpstreamPayload(c *gin.Context, endpoint, transport, accountID string, payload any) {
	if a == nil || a.logger == nil || !a.cfg.DebugLogPayloads {
		return
	}
	formatted := formatPayloadForLog(payload)
	if formatted == "" {
		return
	}

	attrs := contextLogAttrs(c, endpoint)
	attrs = append(attrs,
		"direction", "upstream",
		"transport", transport,
		"payload", formatted,
	)
	attrs = appendStringAttr(attrs, "account_id", accountID)
	a.logger.Info("payload debug", attrs...)
}

func (a *App) logCustomToolTrace(c *gin.Context, endpoint, phase string, eventType string, state *turn.ToolCallState) {
	if a == nil || a.logger == nil || !a.cfg.DebugLogPayloads || state == nil {
		return
	}

	attrs := contextLogAttrs(c, endpoint)
	attrs = append(attrs,
		"phase", phase,
		"event_type", eventType,
		"call_id", state.CallID,
		"item_id", state.ItemID,
		"name", state.Name,
		"status", state.Status,
		"input_len", len(state.Input),
		"completed", strings.EqualFold(strings.TrimSpace(state.Status), "completed"),
	)
	a.logger.Info("custom tool debug", attrs...)
}

func contextLogAttrs(c *gin.Context, endpoint string) []any {
	return []any{
		"request_id", middleware.GetRequestID(c),
		"path", c.Request.URL.Path,
		"endpoint", endpoint,
	}
}

func appendStringAttr(attrs []any, key, value string) []any {
	if value == "" {
		return attrs
	}
	return append(attrs, key, value)
}

func normalizeRequestContextError(ctx context.Context, err error) error {
	if ctx == nil || err == nil || ctx.Err() == nil || errors.Is(err, ctx.Err()) {
		return err
	}
	return fmt.Errorf("%w: transport error: %v", ctx.Err(), err)
}

func (a *App) recordRequestCancellation(c *gin.Context, accountID, responseID string, err error) bool {
	if err == nil {
		return false
	}
	if c != nil && c.Request != nil {
		err = normalizeRequestContextError(c.Request.Context(), err)
	}

	outcome, code := "", ""
	switch {
	case errors.Is(err, context.Canceled):
		outcome, code = "client_canceled", "client_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		outcome, code = "request_timeout", "request_timeout"
	default:
		return false
	}

	middleware.SetRequestOutcome(c, outcome)
	middleware.SetRequestError(c, code, err.Error())
	middleware.SetRequestResponseID(c, responseID)
	if c != nil && strings.TrimSpace(accountID) != "" {
		c.Set(middleware.RequestAccountIDKey, strings.TrimSpace(accountID))
	}
	return true
}

func formatPayloadForLog(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case []byte:
		return normalizePayloadString(typed)
	case string:
		return normalizePayloadString([]byte(typed))
	default:
		payload, err := json.Marshal(typed)
		if err != nil {
			return fmt.Sprintf("<payload marshal error: %v>", err)
		}
		return normalizePayloadString(payload)
	}
}

func normalizePayloadString(payload []byte) string {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return ""
	}
	if len(trimmed) > 1<<20 {
		return fmt.Sprintf("<payload omitted: %d bytes>", len(trimmed))
	}
	var value any
	if json.Unmarshal(trimmed, &value) != nil {
		return fmt.Sprintf("<non-JSON payload omitted: %d bytes>", len(trimmed))
	}
	switch value.(type) {
	case map[string]any, []any:
		redactPayloadValue(value)
	default:
		// Scalar payloads can contain arbitrary user content (for example, a
		// JSON string). Never emit those values verbatim in debug logs.
		value = "<redacted>"
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("<payload summary error: %v>", err)
	}
	if len(encoded) > 16<<10 {
		const suffix = "...<truncated>"
		end := 16<<10 - len(suffix)
		for end > 0 && !utf8.Valid(encoded[:end]) {
			end--
		}
		return string(encoded[:end]) + suffix
	}
	return string(encoded)
}

func redactPayloadValue(value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			switch strings.ToLower(strings.TrimSpace(key)) {
			case "model", "stream", "store", "service_tier", "type", "background", "generate", "n", "size", "quality", "output_format", "response_format", "moderation", "input_fidelity", "partial_images":
				redactPayloadValue(child)
			default:
				typed[key] = "<redacted>"
			}
		}
	case []any:
		for index := range typed {
			typed[index] = "<redacted>"
		}
	}
}
