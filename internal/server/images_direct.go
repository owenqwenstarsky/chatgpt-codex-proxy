package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"chatgpt-codex-proxy/internal/accounts"
	"chatgpt-codex-proxy/internal/codex"
	"chatgpt-codex-proxy/internal/generation"
	"chatgpt-codex-proxy/internal/jsonutil"
	"chatgpt-codex-proxy/internal/middleware"
	"chatgpt-codex-proxy/internal/turn"
)

func prepareDirectImagePayload(body []byte, model string, stream bool) ([]byte, error) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	payload["model"] = mustRawJSON(model)
	if stream {
		payload["stream"] = json.RawMessage("true")
	} else {
		delete(payload, "stream")
	}
	return json.Marshal(payload)
}

func (a *App) handleDirectImageResponse(c *gin.Context, endpoint, path string, payload []byte, stream bool) bool {
	account, response, err := a.openDirectImageWithFailover(c.Request.Context(), c, endpoint, path, payload, stream)
	if err != nil {
		if directImageEndpointUnavailable(err) {
			return false
		}
		a.setRequestAccount(c, account)
		a.handleOpenStreamError(c, endpoint, account.ID, account.ID, err)
		return true
	}
	defer response.Body.Close()

	a.setRequestAccount(c, account)
	a.observeQuotaSnapshot(account.ID, codex.ParseQuotaFromHeaders(response.Header))
	contentType := strings.TrimSpace(response.Header.Get("Content-Type"))
	mediaType, _, _ := mime.ParseMediaType(contentType)
	if stream && mediaType == "application/json" {
		var body map[string]any
		err := json.NewDecoder(response.Body).Decode(&body)
		results := jsonutil.SliceOfMaps(body["data"])
		if err == nil && len(results) == 0 {
			err = fmt.Errorf("upstream image response contained no images")
		}
		if err != nil {
			a.handleOpenStreamError(c, endpoint, account.ID, account.ID, err)
			return true
		}
		prefix := "image_generation"
		if endpoint == "images_edits" {
			prefix = "image_edit"
		}
		delete(body, "data")
		if created, ok := body["created"]; ok {
			body["created_at"] = created
			delete(body, "created")
		}
		prepareStreamResponse(c)
		middleware.MarkActivityFinalizing(c)
		for _, result := range results {
			payload := jsonutil.CloneMap(body)
			maps.Copy(payload, result)
			payload["type"] = prefix + ".completed"
			writeSSE(c.Writer, prefix+".completed", turn.MustJSON(payload))
			c.Writer.Flush()
		}
	} else if stream {
		prepareStreamResponse(c)
		if contentType != "" {
			c.Header("Content-Type", contentType)
		}
		buffer := make([]byte, 32*1024)
		for {
			n, readErr := response.Body.Read(buffer)
			if n > 0 {
				_, _ = c.Writer.Write(buffer[:n])
				c.Writer.Flush()
			}
			if readErr != nil {
				if readErr != io.EOF {
					a.respondStreamError(c, endpoint, account.ID, "", "error", readErr, false)
					return true
				}
				break
			}
		}
		middleware.MarkActivityFinalizing(c)
	} else {
		body, readErr := io.ReadAll(response.Body)
		if readErr != nil {
			a.handleOpenStreamError(c, endpoint, account.ID, account.ID, readErr)
			return true
		}
		if contentType == "" {
			contentType = "application/json"
		}
		middleware.MarkActivityFinalizing(c)
		c.Data(http.StatusOK, contentType, body)
	}
	a.accounts.NoteSuccess(account.ID)
	return true
}

func (a *App) openDirectImageWithFailover(ctx context.Context, c *gin.Context, endpoint, path string, payload []byte, stream bool) (accounts.Record, *http.Response, error) {
	attempted := make(map[string]struct{})
	var lastAccount accounts.Record
	var lastErr error
	var started time.Time
	attemptCount := 0
	for {
		attemptCount++
		lease, err := a.accountMgr.AcquireMatchingLease(ctx, "", func(record accounts.Record) bool {
			_, alreadyAttempted := attempted[record.ID]
			return !alreadyAttempted
		})
		account := accounts.Record{}
		if lease != nil {
			account = lease.Account
		}
		if err != nil {
			if retry, recoveryErr := a.waitForCapacityRecovery(ctx, endpoint, rateLimitRecoveryStart(&started), attemptCount, nil); recoveryErr != nil {
				return lastAccount, nil, recoveryErr
			} else if retry {
				clear(attempted)
				continue
			}
			if lastErr != nil {
				return lastAccount, nil, lastErr
			}
			return account, nil, err
		}
		a.setRequestAccount(c, account)
		if err == nil {
			a.setRequestAccount(c, account)
			a.logUpstreamPayload(c, endpoint, "http", account.ID, json.RawMessage(payload))
			attemptID := a.startGeneration(c, endpoint, "http", account, "", attemptCount, json.RawMessage(payload))
			open := a.directImageOpen
			if open == nil {
				open = a.httpClient.OpenImage
			}
			var response *http.Response
			response, err = open(ctx, account, path, payload, stream)
			if err == nil {
				response.Body = &releaseReadCloser{ReadCloser: response.Body, release: lease.Release}
				return account, response, nil
			}
			a.finishGeneration(attemptID, generation.OutcomeFailed, 0, err, "")
		}
		lease.Release()
		err = normalizeRequestContextError(ctx, err)
		if directImageEndpointUnavailable(err) || !shouldFailoverRequest(err) {
			return account, nil, err
		}
		attempted[account.ID] = struct{}{}
		lastAccount = account
		lastErr = err
		a.classifyUpstreamError(account.ID, err)
	}
}

func directImageEndpointUnavailable(err error) bool {
	var upstreamErr *codex.UpstreamError
	if !errors.As(err, &upstreamErr) {
		return false
	}
	switch upstreamErr.StatusCode {
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return true
	default:
		return false
	}
}
