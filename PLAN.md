# Execution Plan: Go Meta Messenger AI Agent

## Phase 1: Workspace Initialization & Infrastructure
- [x] Initialize Go module (`go mod init messenger-agent`) and install dependencies (`go-redis/v9`, `pgx/v5`, `pgvector-go`, LLM SDK).
- [x] Create `docker-compose.yml` with `pgvector/pgvector:pg16` and `redis:7-alpine` with healthchecks.
- [x] Create `.env.example` and `internal/config/config.go` with strict validation for `META_APP_SECRET`, `META_VERIFY_TOKEN`, `META_PAGE_ACCESS_TOKEN`, `DATABASE_URL`, `REDIS_ADDR`, and `LLM_API_KEY`.
- [x] Create `migrations/001_init.sql` with `products`, `customers`, `chat_messages`, and `orders` tables plus HNSW and GIN trigram indexes.

## Phase 2: Data & State Layer (`internal/store/`)
- [x] Implement `internal/store/postgres.go` with `pgxpool` initialization, `UpsertCustomer`, `SaveMessage`, and `GetRecentHistory`.
- [x] Implement `SearchProductsHybrid` combining `pgvector` cosine similarity and `pg_trgm` text similarity.
- [x] Implement transactional order creation (`CreateOrderTx`) that validates stock, locks product rows (`FOR UPDATE`), decrements `stock_qty`, and inserts into `orders`.
- [x] Implement `internal/store/redis.go` for `SetBotPaused`, `IsBotPaused`, and sliding-window chat history caching.

## Phase 3: Webhook & Atomic Redis Debouncer (`internal/webhook/`, `internal/debounce/`)
- [x] Implement `internal/webhook/types.go` and `internal/webhook/handler.go` (`Verify` and `Receive`).
- [x] Implement `internal/debounce/debouncer.go` with `ClaimMessageID`, `Push`, atomic Lua `flushScript`, and graceful `Shutdown`.
- [x] Add table-driven unit tests in `internal/webhook/handler_test.go` and `internal/debounce/debouncer_test.go`.

## Phase 4: Messenger Client & Media Ingestion (`internal/messenger/`)
- [x] Implement `internal/messenger/client.go` targeting Meta Graph API `v26.0` (`SendTyping`, `SendText` with 1,900-rune UTF-8 chunking, and exponential backoff on HTTP 429/5xx).
- [x] Implement `internal/messenger/media.go` to fetch image attachments and transcribe `audio` voice notes.

## Phase 5: AI Orchestrator & Bangladeshi Business Tools (`internal/agent/`)
- [ ] Implement `internal/agent/prompt.go` containing the Bangladeshi business Bangla (*চলিত ভাষা*, *আপনি* register) system prompt and Banglish few-shot examples.
- [ ] Implement `internal/agent/tools.go` wiring `search_products`, `check_stock`, `calculate_delivery_fee`, `create_order`, and `escalate_to_human`.
- [ ] Implement `internal/agent/orchestrator.go` handling the multi-turn tool-calling loop, query embeddings, and fallback Bangla error replies.

## Phase 6: Server Wiring, Admin Controls & End-to-End Verification
- [ ] Wire all components in `cmd/server/main.go` with graceful HTTP + debouncer shutdown.
- [ ] Add protected admin endpoints (`POST /admin/threads/{sender_id}/pause` and `/resume`) secured by `ADMIN_API_KEY`.
- [ ] Seed sample Bangladeshi e-commerce products and run `/test-webhook` to verify end-to-end Banglish burst -> Bangla order flow.
