# AGENTS.md — Antigravity Engineering & Coding Guidelines

## 1. Project Identity & Tech Stack
You are building a production **Meta Messenger AI Agent** in **Go (1.23+)** designed for Bangladeshi businesses.
- **Language:** Go 1.23+ (standard `net/http` routing with method patterns, `log/slog`, `context.Context`).
- **Database:** PostgreSQL 16+ with `pgvector` and `pg_trgm`, accessed via `[github.com/jackc/pgx/v5](https://github.com/jackc/pgx/v5)` and `[github.com/pgvector/pgvector-go](https://github.com/pgvector/pgvector-go)`.
- **Cache / Debouncer / State:** Redis 7+ via `[github.com/redis/go-redis/v9](https://github.com/redis/go-redis/v9)` using atomic Lua scripts.
- **LLM Integration:** Official Go SDK (`google.golang.org/genai` or `[github.com/openai/openai-go](https://github.com/openai/openai-go)`) supporting multimodal inputs (text, image URLs, audio) and structured function/tool calling.
- **External API:** Meta Graph API `v26.0` (`[https://graph.facebook.com/v26.0/me/messages](https://graph.facebook.com/v26.0/me/messages)`).

## 2. Directory Layout
Maintain strict package boundaries:
- `cmd/server/main.go`: Dependency injection, HTTP server setup, graceful shutdown (`SIGINT`/`SIGTERM`).
- `internal/config/`: Typed environment configuration with startup validation (fail fast on missing secrets).
- `internal/webhook/`: Meta webhook verification, HMAC-SHA256 validation, payload parsing.
- `internal/debounce/`: Redis deduplication, atomic Lua message burst aggregation, graceful flush.
- `internal/messenger/`: Meta Graph API outbound client (`typing_on`, `RESPONSE` messages, UTF-8 rune splitter).
- `internal/agent/`: System prompt builder, Banglish query normalizer, LLM loop, and deterministic Go tools.
- `internal/store/`: PostgreSQL (`pgxpool`) repositories and Redis session/pause state helpers.
- `migrations/`: Sequential SQL migrations (`001_init.sql`, etc.).

## 3. Non-Negotiable Engineering Rules
1. **Never Block the Webhook Handler:** `POST /webhook` must verify HMAC, push to Redis, and return `200 OK` immediately. Never perform LLM calls, DB vector queries, or outbound Messenger API calls synchronously inside the HTTP handler.
2. **Deterministic Commerce Logic:** Never allow the LLM to calculate prices, inventory counts, or delivery fees in prose. All pricing, stock checks, and order creation must execute inside Go tool functions backed by PostgreSQL transactions.
3. **Bangla UTF-8 Rune Safety:** Bangla characters use multi-byte UTF-8 sequences and combining marks. Always slice strings by `[]rune`—never by byte index (`s[:1900]`).
4. **Constant-Time Secret Comparisons:** Always use `crypto/subtle.ConstantTimeCompare` for `hub.verify_token` and `hmac.Equal` for `X-Hub-Signature-256`.
5. **Context & Error Wrapping:** Pass `context.Context` as the first parameter to every I/O function. Wrap errors with `fmt.Errorf("context: %w", err)`. Never ignore errors silently except best-effort UI hints like `typing_on`.
6. **No Global Mutable State:** Inject `*pgxpool.Pool`, `*redis.Client`, `*slog.Logger`, and config structs explicitly via constructors.
7. **24-Hour Messaging Policy:** Always include `"messaging_type": "RESPONSE"` on standard replies within the 24-hour customer service window.

## 4. Redis Key Conventions
- `msg:dedup:{mid}` -> `"1"` (TTL: 24h) — prevents duplicate webhook processing.
- `debounce:seq:{sender_id}` -> monotonic integer (TTL: 2m) — tracks latest burst message.
- `debounce:msgs:{sender_id}` -> Redis List of JSON `InboundMessage` (TTL: 2m).
- `bot:paused:{sender_id}` -> `"1"` (TTL: 24h) — disables AI when escalated to a human agent.
- `chat:history:{sender_id}` -> Redis List of last 16 conversation turns (TTL: 12h) for fast context assembly.

## 5. Testing & Verification Protocol
Before marking any task complete:
1. Run `go vet ./...` and `go test -race ./...`.
2. Write table-driven unit tests covering:
   - Valid and tampered `X-Hub-Signature-256` headers.
   - Multi-message Banglish bursts in the Redis debouncer (verifying only the final timer flushes).
   - Bangladeshi phone number validation (`017...`, `+88018...`, invalid numbers).
   - UTF-8 Bangla rune splitting across multi-paragraph replies.
3. Verify Docker Compose services (`postgres`, `redis`) start cleanly and migrations apply without errors.
