# ShohojAI (`messenger-agent`)

Production **Meta Messenger AI Agent** in **Go (1.23+)** designed for Bangladeshi e-commerce businesses. The agent understands customer messages written in **Bangla (বাংলা)**, **English**, and **Banglish (Romanized Bengali)**, aggregates rapid multi-message customer bursts in Redis, executes deterministic commerce operations (hybrid product search, live stock checks, Bangladeshi district delivery quotes, and transactional Cash-on-Delivery order creation) in PostgreSQL, and replies in natural Bangladeshi colloquial business Bangla (**চলিত ভাষা**, **আপনি** register).

---

## Architecture & Request Flow

```mermaid
sequenceDiagram
    participant U as Customer (Messenger)
    participant W as Webhook Handler (Go)
    participant R as Redis 7 (Lua Debouncer)
    participant O as AI Orchestrator (Go)
    participant L as Gemini Multimodal LLM
    participant DB as PostgreSQL 16 (pgvector + pg_trgm)
    participant M as Meta Graph API v26.0

    U->>W: POST /webhook ("vai", "ei black hoodie ta ache?", "delivery koto?")
    W->>W: Verify X-Hub-Signature-256 (hmac.Equal)
    W->>R: SETNX msg:dedup:{mid} 1 EX 86400
    W->>R: Atomic Lua pushScript (debounce:seq & debounce:msgs)
    W-->>U: 200 OK (< 50ms)
    Note over R,O: 3-second quiet window expires (or batch reaches 8)
    R->>O: Atomic Lua flushScript drains burst (msg_count: 3)
    O->>M: POST /me/messages (sender_action: typing_on)
    O->>L: Generate(SystemPrompt + History + Burst)
    L-->>O: ToolCalls: check_stock("HD-BLK-001", "XXL"), calculate_delivery_fee("Dhaka")
    O->>DB: SELECT stock_qty, price_bdt FROM products
    O-->>L: Tool JSON results (Price: 1250 BDT, Delivery: 70 BDT)
    L-->>O: Final Bangla reply (চলিত ভাষা)
    O->>M: POST /me/messages (messaging_type: RESPONSE, 1900-rune UTF-8 chunks)
```

---

## Directory Structure

```text
.
├── .agent/workflows/test-webhook.md  # Webhook & Redis debouncer verification workflow
├── .env.example                      # Environment variable template
├── AGENTS.md / GEMINI.md             # Engineering & coding guidelines
├── Dockerfile                        # Multi-stage Go 1.23 Alpine build
├── PLAN.md                           # Phased execution plan
├── PRD.md                            # Product Requirements Document
├── cmd/server/                       # HTTP server entrypoint, admin routes, E2E tests
├── docker-compose.yml                # PostgreSQL 16 (pgvector) & Redis 7 services
├── internal/
│   ├── agent/                        # Bangla system prompt, Banglish normalizer, tools, orchestrator
│   ├── config/                       # Typed environment configuration & fail-fast validation
│   ├── debounce/                     # Redis deduplication & atomic Lua burst aggregation
│   ├── messenger/                    # Meta Graph API v26.0 client, UTF-8 Bangla rune splitter, media
│   ├── store/                        # PostgreSQL (pgxpool) & Redis repositories
│   └── webhook/                      # Meta webhook verification & HMAC-SHA256 handler
└── migrations/
    ├── 001_init.sql                  # Schema: products, customers, chat_messages, orders
    └── 002_seed_products.sql         # Sample Bangladeshi e-commerce catalog seed
```

---

## Quick Start

### 1. Start PostgreSQL (`pgvector`) & Redis
```bash
docker compose up -d
```

### 2. Configure Environment Variables
Copy `.env.example` to `.env` and fill in your credentials:
```bash
cp .env.example .env
```

Required variables:
- `META_APP_SECRET` — Meta App Secret used to verify `X-Hub-Signature-256`
- `META_VERIFY_TOKEN` — Webhook verification token for `GET /webhook`
- `META_PAGE_ACCESS_TOKEN` — Page Access Token for outbound Graph API `v26.0` messages
- `DATABASE_URL` — PostgreSQL connection string (`postgres://postgres:postgres@localhost:5432/messenger_agent?sslmode=disable`)
- `REDIS_ADDR` — Redis address (`localhost:6379`)
- `LLM_API_KEY` — Gemini API key

### 3. Run the Server
```bash
go run ./cmd/server
```

---

## HTTP Endpoints

| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/healthz` | Healthcheck endpoint returning `{"status":"ok"}` |
| `GET` | `/webhook` | Meta subscription verification (`hub.mode=subscribe`, constant-time `hub.verify_token`) |
| `POST` | `/webhook` | Meta webhook ingestion (`X-Hub-Signature-256` verification, 1 MB cap, Redis debouncer) |
| `POST` | `/admin/threads/{sender_id}/pause` | Pause AI replies for `{sender_id}` (`X-Admin-API-Key` or `Authorization: Bearer <ADMIN_API_KEY>`) |
| `POST` | `/admin/threads/{sender_id}/resume` | Resume AI replies for `{sender_id}` (`X-Admin-API-Key` or `Authorization: Bearer <ADMIN_API_KEY>`) |

---

## Testing & Verification

Run static analysis and the full race-enabled test suite:
```bash
go vet ./...
go test -race -v ./...
```
