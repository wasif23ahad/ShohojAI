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
