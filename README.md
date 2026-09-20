<div align="center">

# chatgpt-codex-proxy

*Talk to ChatGPT Codex accounts using any OpenAI or Anthropic client.*

[![Go](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go&logoColor=white)](https://go.dev)
[![Docker](https://img.shields.io/badge/Deploy-Compose-2496ED?style=flat&logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![API](https://img.shields.io/badge/API-OpenAI%20%2B%20Anthropic-412991?style=flat&logo=openai&logoColor=white)]()
[![License](https://img.shields.io/badge/License-MIT-yellow?style=flat)](LICENSE)

</div>

---

```bash
cp .env.example .env          # set PROXY_API_KEY
docker compose up -d --build  # or: go run ./cmd/api
```

Exposes OpenAI and Anthropic endpoints, translates each request into the private
`chatgpt.com/backend-api/codex/*` format, and routes it across your logged-in
Codex accounts.

The upstream API is private and undocumented, so it can change without notice.
Built for local and small-scale use.

## Features

- **Two API surfaces, one backend** — OpenAI Chat Completions, Responses, and Images plus Anthropic Messages.
- **Streaming everywhere** — SSE, a persistent WebSocket for Responses, or plain JSON.
- **Multi-account rotation** — least-used, round-robin, sticky, or sticky-thread, with cooldowns and quota awareness.
- **Device login** — add an account by opening a URL. No cookie scraping, no pasted tokens.
- **Tools and structured output** — custom tools, legacy `functions`, `json_schema`, `json_object`.
- **Activity and Logbook telemetry** — live request phases over authenticated SSE plus 30 days of redacted daily history.

## Quick Start

Needs Docker or Go `1.26.8` or later, and a long random `PROXY_API_KEY`.

```bash
export PROXY_URL=http://localhost:8080
export PROXY_API_KEY=change-me-to-a-long-random-string
```

Add an account — start a device login, open the returned `auth_url`, then poll
until `status` is `ready`:

```bash
curl -sS -X POST "${PROXY_URL}/admin/accounts/device-login/start" \
  -H "Authorization: Bearer ${PROXY_API_KEY}"

curl -sS "${PROXY_URL}/admin/accounts/device-login/<login_id>" \
  -H "Authorization: Bearer ${PROXY_API_KEY}"
```

Then:

```bash
curl -sS "${PROXY_URL}/v1/chat/completions" \
  -H "Authorization: Bearer ${PROXY_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-6-astra","messages":[{"role":"user","content":"hello"}]}'
```

## Clients

Point any OpenAI client at `http://localhost:8080/v1` using `PROXY_API_KEY` as
the API key. For Claude Code or another Anthropic client:

```bash
export ANTHROPIC_BASE_URL=http://localhost:8080
export ANTHROPIC_API_KEY="${PROXY_API_KEY}"
export ANTHROPIC_MODEL=gpt-6-astra
```

Use a model ID from `GET /v1/models`; the default is `gpt-6-astra`.

Every route except `GET /health/live` needs the key, as either
`Authorization: Bearer <key>` or `X-API-Key: <key>`.

## API

```
POST /v1/completions              POST /v1/messages
POST /v1/chat/completions         POST /v1/messages/count_tokens
POST /v1/responses                GET  /v1/models
GET  /v1/responses   (WebSocket)  GET  /v1/models/:model_id
POST /v1/responses/compact        GET  /health
POST /v1/images/generations       GET  /health/live
POST /v1/images/edits
ANY  /noval/v1/*
```

Text, image, and file inputs, reasoning, hosted web search, streaming and
non-streaming. The model catalog comes from upstream for each account at runtime.

Gotchas:

- `GET /v1/responses` is a WebSocket, not SSE. First message must be `response.create`; later turns use `response.create` or `response.append`. No `[DONE]` marker.
- `/v1/chat/completions` also accepts a Responses-shaped body when `messages` is omitted.
- Images default to `gpt-image-2`. If the native endpoint returns `404`, `405`, or `501`, it falls back to the Responses image tool — one image, data URL.
- Anthropic requests need `Anthropic-Version: 2023-06-01`. Sampling controls, stop sequences, `max_tokens` truncation, and thinking budgets are accepted but advisory; Codex has no equivalent.
- Audio input is rejected. Request bodies must be identity or zstd.

Exact rules: [docs/TRANSLATION.md](docs/TRANSLATION.md),
[docs/ANTHROPIC.md](docs/ANTHROPIC.md).

### No-validation passthrough

`/noval/v1/*` is an opaque HTTP passthrough for clients such as Codex and Pi
that already produce the private Codex request shape. The suffix maps directly
under the upstream `/codex` namespace: for example,
`POST /noval/v1/responses` calls `POST /codex/responses`. The proxy does not
parse or rewrite the body, and relays the upstream status and body as-is. It
still requires `PROXY_API_KEY`, selects a ready account, replaces client auth
with that account's upstream credentials, and removes hop-by-hop headers and
cookies that would expose either side's session.

## Accounts

```
GET    /admin/accounts
POST   /admin/accounts/device-login/start
GET    /admin/accounts/device-login/:login_id
DELETE /admin/accounts/:account_id
PATCH  /admin/accounts/:account_id
GET    /admin/accounts/:account_id/usage
POST   /admin/accounts/:account_id/refresh
GET    /admin/rotation
PUT    /admin/rotation
```

Rotation is `least_used`, `round_robin`, `sticky`, or `sticky-thread`. An account is skipped when
its status is `disabled`, `expired`, or `banned`, a cooldown is active, its
token is missing, or its quota is spent. `code_review_rate_limit` is tracked but
does not affect routing.

`sticky-thread` keeps each keyed conversation on its own account for the configured
sticky-thread TTL (30 minutes by default). If that account hits a quota limit on
a normal request, the proxy releases the affinity immediately, fails over to
another eligible account, and rebinds only after that replacement succeeds. The
affinity is local and does not guarantee that OpenAI's upstream prompt cache
still exists.

When an upstream 402 or 429 exhausts all usable accounts, normal requests first
try every eligible account, then wait for the earliest known cooldown or quota
reset for up to `RATE_LIMIT_MAX_WAIT` (2 minutes by default). If capacity still
has not recovered, the proxy returns a retryable 429 with `Retry-After`.
Explicit `previous_response_id` continuations remain pinned to their original
account and wait for that account instead of migrating.

A failed OAuth refresh only expires an account on `invalid_grant`. Anything else
keeps it active behind a 60-second cooldown.

Details: [docs/MULTI_ACCOUNT_ROTATION_STRATEGY.md](docs/MULTI_ACCOUNT_ROTATION_STRATEGY.md).

## Activity and Logbook

The authenticated admin API exposes live inference-request metadata and durable
daily history:

```
GET /admin/requests/activity
GET /admin/requests/activity/stream
GET /admin/requests/logs/dates
GET /admin/requests/logs?date=YYYY-MM-DD
```

The stream starts with a named `snapshot` event, then sends named `upsert` and
`remove` events plus a comment heartbeat every 15 seconds. Completed requests
remain in the live Activity window for approximately 60 seconds.

Logbook supports `limit`, `cursor`, `q`, `outcome`, `account`, and `model`
filters. Daily files are written as `${DATA_DIR}/request-YYYY-MM-DD.jsonl` using
the request's UTC start date and are retained for 30 UTC days.

Activity data is deliberately metadata-only: request and response bodies,
prompts, tool data, images, files, headers, tokens, cookies, client addresses,
user agents, query strings, upstream error bodies, and stack traces are never
stored or streamed. Records contain only request identity, route, model,
selected account metadata, lifecycle phase/outcome, timing, status, and safe
allowlisted failure classifications.

## Deployment

`compose.yaml` persists state in the `chatgpt-codex-proxy-data` volume and runs
with basic hardening. `Dockerfile` builds the API server alone.

```bash
docker compose up -d --build
docker compose logs -f
```

Config is environment-only: `PROXY_API_KEY` (required), `PORT` (`8080`),
`DATA_DIR` (`data`, or `/app/data` in Docker), `DEBUG_LOG_PAYLOADS` (`false`),
and `STICKY_THREAD_TTL` (`30m`). `RATE_LIMIT_MAX_WAIT` is a positive duration
and defaults to `2m`.

`${DATA_DIR}` holds `accounts.json` — accounts, OAuth tokens, labels, status,
quota, cooldowns — `models-cache.json`, and redacted `request-YYYY-MM-DD.jsonl`
Logbook files. Continuation state, live Activity state, and in-flight device
logins are memory-only and do not survive a restart.

## How It Works

1. Gin accepts an OpenAI- or Anthropic-shaped request.
2. Adapters normalize it into one internal turn model.
3. The proxy picks a ready account, translates, and calls Codex over SSE or WebSocket.
4. Upstream events convert back to whichever protocol the client asked for.

<img width="4599" height="2073" alt="chatgpt-codex-proxy architecture flowchart" src="https://github.com/user-attachments/assets/05cd8446-dd4b-43bc-a3fc-eb370ad917e6" />

```
POST https://chatgpt.com/backend-api/codex/responses
GET  https://chatgpt.com/backend-api/codex/usage
GET  https://chatgpt.com/backend-api/codex/models
WSS  https://chatgpt.com/backend-api/codex/responses
     https://auth.openai.com/api/accounts/deviceauth/*
     https://auth.openai.com/oauth/token
```

## Layout

```
chatgpt-codex-proxy/
├── cmd/api/                  # server entrypoint
├── internal/
│   ├── server/               # Gin routing and handlers
│   ├── openai/ anthropic/    # public protocol adapters
│   ├── turn/                 # internal turn model and accumulation
│   ├── codex/ codexauth/     # private upstream client and OAuth
│   ├── accounts/             # account store
│   ├── accountmanager/       # rotation, cooldowns, quota routing
│   ├── devicelogin/          # device-auth onboarding
│   ├── conversation/         # continuation state and affinity
│   ├── models/               # runtime model catalog
│   ├── middleware/           # authentication and request logging
│   └── config/               # environment configuration
├── test/integration/         # live compatibility suite
└── docs/                     # upstream and translation references
```

## Development

```bash
go test ./...
```

Live tests, against a proxy you already have running:

```bash
OPENAI_API_KEY=change-me-to-a-long-random-string \
OPENAI_MODEL=gpt-6-astra \
OPENAI_BASE_URL="${PROXY_URL}/v1" \
go test -tags=live ./test/integration -v -count=1
```

## Docs

- [docs/TRANSLATION.md](docs/TRANSLATION.md) — OpenAI-to-Codex translation and compatibility rules
- [docs/ANTHROPIC.md](docs/ANTHROPIC.md) — Anthropic mapping, streaming, token counting, limits
- [docs/MULTI_ACCOUNT_ROTATION_STRATEGY.md](docs/MULTI_ACCOUNT_ROTATION_STRATEGY.md) — account selection and quota routing
- [docs/CODEX_API_DOCS.md](docs/CODEX_API_DOCS.md) — private upstream behavior, inferred from this codebase

## Limitations

- Upstream is private and may change without notice.
- Device auth is the only onboarding flow.
- Continuation state is in memory and expires with its TTL.
- Deliberately small; it does not chase every edge of the public OpenAI platform.
