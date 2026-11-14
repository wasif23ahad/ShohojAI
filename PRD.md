# Product Requirements Document (PRD): Go Meta Messenger AI Agent

## 1. Overview
An autonomous, low-latency Facebook Messenger customer support and sales AI agent built in **Go**. The agent understands customer messages written in **Bangla (বাংলা)**, **English**, and **Banglish (Romanized Bengali)**, and responds primarily in **natural, polite Bangladeshi business Bangla (চলিত ভাষা)**.

## 2. Core Objectives
- **Zero Dropped Webhooks:** Acknowledge Meta Graph API webhooks within `< 500ms` (hard limit `< 5s`) with HMAC-SHA256 signature verification.
- **Burst Aggregation:** Debounce rapid, multi-message customer bursts (e.g., `"vai"`, `"ei product ta ache?"`, `"price koto?"`) over a 3-second Redis window before invoking the LLM.
- **Banglish-to-Bangla Fluency:** Accurately interpret code-mixed Banglish, product screenshots, and Bangla voice notes, replying in natural Bangladeshi colloquial Bangla (*আপনি* register).
- **Deterministic Commerce Operations:** Execute stock checks, Dhaka vs. Outside Dhaka delivery calculations, and order creation strictly via typed Go tool calls—never via LLM text generation.
- **Seamless Human-in-the-Loop (HITL):** Automatically pause bot replies and notify human operators on complaints, refund requests, or explicit requests to speak with a human.

## 3. Linguistic & Prompt Engineering Specification
- **Input Languages:** Bangla script, English, and phonetic Banglish (including spelling variations like *koto / dam koto*, *ache / ase*, *dhakar vitore / inside dhaka*).
- **Output Language:** Natural **Bangladeshi business Bangla (চলিত ভাষা)** using polite pronouns (**আপনি / আপনার**, never *তুমি* or *তুই*).
- **Natural Loanwords:** Retain standard e-commerce terms in Bangla script or English where natural (*ডেলিভারি চার্জ*, *ক্যাশ অন ডেলিভারি*, *সাইজ*, *অর্ডার কনফার্ম*, *ইনবক্স*).
- **Few-Shot Calibration:**
  - User (Banglish): `vai ei black hoodie ta XXL size hobe? dhakar vitore delivery charge koto?`
  - Agent (Bangla): `জি স্যার/ম্যাডাম, আমাদের এই ব্ল্যাক হুডিটির XXL সাইজ বর্তমানে স্টকে আছে। ঢাকার ভেতরে ডেলিভারি চার্জ ৭০ টাকা (ক্যাশ অন ডেলিভারি)। আপনি কি অর্ডারটি কনফার্ম করতে চান?`
  - User (English + Banglish): `order korbo kivabe? advance lagbe naki?`
  - Agent (Bangla): `অর্ডার কনফার্ম করার জন্য অনুগ্রহ করে আপনার নাম, পূর্ণ ঠিকানা এবং মোবাইল নম্বরটি দিন। কোনো অগ্রিম পেমেন্ট লাগবে না—পণ্য হাতে পেয়ে ক্যাশ অন ডেলিভারিতে পেমেন্ট করতে পারবেন।`

## 4. Functional Requirements

### 4.1 Meta Webhook & Debouncing
- **Verification (`GET /webhook`):** Validate `hub.mode == "subscribe"` and constant-time compare `hub.verify_token`, returning `hub.challenge`.
- **Ingestion (`POST /webhook`):** Enforce 1 MB max body, validate `X-Hub-Signature-256`, deduplicate by `message.mid` (`SETNX msg:dedup:{mid} 1 EX 86400`), skip echoes and paused threads (`bot:paused:{sender_id}`), and aggregate bursts over a `3s` Redis Lua window (max batch size `8`).

### 4.2 Deterministic LLM Tools
1. `search_products(query string, category string) -> []Product` (hybrid `pgvector` cosine distance + `pg_trgm` text similarity).
2. `check_stock(product_sku string, variant string) -> StockStatus` (live inventory & BDT price).
3. `calculate_delivery_fee(district string) -> DeliveryQuote` (Inside Dhaka = `70 BDT` 1–2 days; Suburbs Gazipur/Narayanganj/Savar/Keraniganj = `100 BDT` 2–3 days; Outside Dhaka = `130 BDT` 2–4 days).
4. `create_order(sender_id, customer_name, phone, address, district, items) -> OrderConfirmation` (validates `^(\+88)?01[3-9]\d{8}$`, decrements stock in PostgreSQL transaction).
5. `escalate_to_human(sender_id string, reason string) -> EscalationResult` (sets `bot:paused:{sender_id}` in Redis and notifies support).

### 4.3 Multimodal & Non-Functional Requirements
- Pass image attachment URLs directly to the multimodal LLM; transcribe `audio` voice notes before inference.
- Split outbound messages exceeding 1,900 Unicode runes (`[]rune`) to respect Meta's 2,000-character limit safely.
- Structured JSON logging via `log/slog`.

## 5. Database Schema (`migrations/001_init.sql`)
```sql
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE products (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sku TEXT UNIQUE NOT NULL,
    name_en TEXT NOT NULL,
    name_bn TEXT NOT NULL,
    description TEXT NOT NULL,
    price_bdt NUMERIC(10, 2) NOT NULL,
    stock_qty INT NOT NULL DEFAULT 0,
    variants JSONB NOT NULL DEFAULT '{}',
    embedding vector(1536),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_products_embedding ON products USING hnsw (embedding vector_cosine_ops);
CREATE INDEX idx_products_name_trgm ON products USING gin (name_en gin_trgm_ops);

CREATE TABLE customers (
    sender_id TEXT PRIMARY KEY,
    full_name TEXT,
    phone TEXT,
    default_address TEXT,
    district TEXT,
    bot_paused BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE chat_messages (
    id BIGSERIAL PRIMARY KEY,
    sender_id TEXT NOT NULL REFERENCES customers(sender_id),
    mid TEXT UNIQUE,
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'tool', 'system')),
    content TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_chat_messages_sender_created ON chat_messages (sender_id, created_at DESC);

CREATE TABLE orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_number TEXT UNIQUE NOT NULL,
    sender_id TEXT NOT NULL REFERENCES customers(sender_id),
    customer_name TEXT NOT NULL,
    phone TEXT NOT NULL,
    address TEXT NOT NULL,
    district TEXT NOT NULL,
    items JSONB NOT NULL,
    subtotal_bdt NUMERIC(10, 2) NOT NULL,
    delivery_fee_bdt NUMERIC(10, 2) NOT NULL,
    total_bdt NUMERIC(10, 2) NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```
