package server

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"chatgpt-codex-proxy/internal/accounts"
	"chatgpt-codex-proxy/internal/codex"
	"chatgpt-codex-proxy/internal/jsonutil"
	"chatgpt-codex-proxy/internal/middleware"
	"chatgpt-codex-proxy/internal/models"
	"chatgpt-codex-proxy/internal/openai"
	"chatgpt-codex-proxy/internal/turn"
)

func (a *App) handleResponsesCompact(c *gin.Context) {
	body, err := readRequestBody(c.Request)
	if err != nil {
		a.respondOpenAIInvalidRequest(c, err)
		return
	}
	a.logIncomingPayload(c, "responses_compact", body)

	normalized, err := normalizeResponsesCompactBody(body, a.modelCatalog())
	if err != nil {
		a.respondOpenAINormalizeError(c, err)
		return
	}
	middleware.SetActivityModel(c, normalized.Model)

	normalized, preferredAccountID, err := a.resolveCompactRequest(normalized)
	if err != nil {
		if a.writeRequestError(c, err) {
			return
		}
		a.respondOpenAINormalizeError(c, err)
		return
	}
	middleware.SetActivityModel(c, normalized.Model)

	account, upstream, quota, err := a.callCompactWithRecovery(c, c.Request.Context(), preferredAccountID, &normalized)
	if err != nil {
		a.setRequestAccount(c, account)
		if a.writeRateLimitRecoveryOpenAIError(c, err) {
			return
		}
		if a.recordRequestCancellation(c, account.ID, "", err) {
			return
		}
		status, code, message := a.classifyUpstreamError(account.ID, err)
		a.logUpstreamRequestFailure(c, "responses_compact", account.ID, status, code, err)
		middleware.SetRequestOutcome(c, "upstream_error")
		a.writeOpenAIError(c, status, code, message, "api_error")
		return
	}
	a.setRequestAccount(c, account)

	a.observeQuotaSnapshot(account.ID, quota)
	a.accounts.NoteSuccess(account.ID)

	response := compactResponseObject(upstream)
	if err := openai.PatchResponsesObjectForTuple(response, normalized.TupleSchema); err != nil {
		a.logTupleReconversionWarning(c, "responses_compact", jsonutil.StringValue(response["id"]), err)
	}
	middleware.MarkActivityFinalizing(c)
	c.JSON(http.StatusOK, response)
}

func (a *App) callCompactWithRecovery(c *gin.Context, ctx context.Context, preferredAccountID string, normalized *turn.NormalizedCompactRequest) (accounts.Record, codex.CompactResponse, *accounts.QuotaSnapshot, error) {
	attempted := make(map[string]struct{})
	var lastAccount accounts.Record
	var lastErr error
	started := time.Now().UTC()
	attemptCount := 0
	for {
		attemptCount++
		account, err := a.acquireAccountForCompactExcluding(ctx, preferredAccountID, normalized, attempted)
		a.setRequestAccount(c, account)
		if err != nil {
			allow := func(record accounts.Record) bool {
				if _, tried := attempted[record.ID]; tried {
					return false
				}
				return strings.TrimSpace(normalized.Model) == "" || a.modelCatalog().SupportsRecord(record, normalized.Model)
			}
			if retry, recoveryErr := a.waitForCapacityRecovery(ctx, "responses_compact", started, attemptCount, allow); recoveryErr != nil {
				return lastAccount, codex.CompactResponse{}, nil, recoveryErr
			} else if retry {
				clear(attempted)
				continue
			}
			if lastErr != nil {
				return lastAccount, codex.CompactResponse{}, nil, lastErr
			}
			return account, codex.CompactResponse{}, nil, err
		}
		middleware.SetActivityModel(c, normalized.Model)
		payload := normalized.CompactRequest
		a.logUpstreamPayload(c, "responses_compact", "http", account.ID, payload)
		caller := a.compactCaller
		if caller == nil {
			caller = a.httpClient.CompactResponse
		}
		upstream, quota, err := caller(ctx, account, payload)
		err = normalizeRequestContextError(ctx, err)
		if err == nil {
			return account, upstream, quota, nil
		}
		if !isRateLimitCapacityFailure(err) {
			return account, codex.CompactResponse{}, nil, err
		}
		a.classifyUpstreamError(account.ID, err)
		attempted[account.ID] = struct{}{}
		lastAccount, lastErr = account, err
	}
}

func normalizeResponsesCompactBody(body []byte, catalog *models.Catalog) (turn.NormalizedCompactRequest, error) {
	var req openai.ResponsesCompactRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return turn.NormalizedCompactRequest{}, err
	}
	return openai.Compact(req, catalog)
}

func (a *App) resolveCompactRequest(normalized turn.NormalizedCompactRequest) (turn.NormalizedCompactRequest, string, error) {
	if strings.TrimSpace(normalized.PreviousResponseID) == "" {
		return normalized, "", nil
	}

	record, ok := a.continuations.Get(normalized.PreviousResponseID)
	if !ok {
		return turn.NormalizedCompactRequest{}, "", errInvalidPreviousResponseID
	}
	if strings.TrimSpace(normalized.Model) == "" {
		normalized.Model = record.Model
	}
	normalized.ToolNameAliases = turn.MergeToolNameAliases(normalized.ToolNameAliases, record.ToolNameAliases)

	history := cloneContinuationInputItems(record.InputHistory)
	if len(history) > 0 {
		normalized.Input = slices.Concat(history, normalized.Input)
	}

	return normalized, strings.TrimSpace(record.AccountID), nil
}

func (a *App) acquireAccountForCompact(ctx context.Context, preferredAccountID string, normalized *turn.NormalizedCompactRequest) (accounts.Record, error) {
	if !normalized.ModelExplicit && strings.TrimSpace(normalized.Model) == "" {
		account, err := a.accountMgr.AcquireReady(ctx, preferredAccountID)
		if err != nil {
			return accounts.Record{}, err
		}
		modelID := a.modelCatalog().ResolveDefaultForRecord(account, a.cfg.DefaultModel)
		if strings.TrimSpace(modelID) == "" {
			return accounts.Record{}, errContinuationAccountUnavailable
		}
		normalized.Model = modelID
		return account, nil
	}
	return a.accountMgr.AcquireReadyForModel(ctx, preferredAccountID, normalized.Model)
}

func (a *App) acquireAccountForCompactExcluding(ctx context.Context, preferredAccountID string, normalized *turn.NormalizedCompactRequest, attempted map[string]struct{}) (accounts.Record, error) {
	allow := func(record accounts.Record) bool {
		if _, tried := attempted[record.ID]; tried {
			return false
		}
		return strings.TrimSpace(normalized.Model) == "" || a.modelCatalog().SupportsRecord(record, normalized.Model)
	}
	record, err := a.accounts.AcquireMatching(preferredAccountID, allow)
	if err != nil {
		return accounts.Record{}, err
	}
	ready, err := a.accountMgr.EnsureReady(ctx, record.ID)
	if err != nil {
		return record, err
	}
	if !normalized.ModelExplicit && strings.TrimSpace(normalized.Model) == "" {
		modelID := a.modelCatalog().ResolveDefaultForRecord(ready, a.cfg.DefaultModel)
		if strings.TrimSpace(modelID) == "" {
			return accounts.Record{}, errContinuationAccountUnavailable
		}
		normalized.Model = modelID
	}
	return ready, nil
}

func compactResponseObject(upstream codex.CompactResponse) map[string]any {
	response := map[string]any{
		"id":         upstream.ID,
		"object":     "response.compaction",
		"created_at": upstream.CreatedAt,
	}
	response["output"] = jsonutil.CloneValue(upstream.Output)
	if len(upstream.Usage) > 0 {
		response["usage"] = upstream.Usage
	}
	return response
}
