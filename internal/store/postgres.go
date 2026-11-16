package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgvector/pgvector-go"
)

var (
	// ErrProductNotFound is returned when a product SKU does not exist.
	ErrProductNotFound = errors.New("store: product not found")
	// ErrInsufficientStock is returned when requested quantity exceeds available stock.
	ErrInsufficientStock = errors.New("store: insufficient stock")
	// ErrInvalidPhone is returned when a Bangladeshi phone number fails validation.
	ErrInvalidPhone = errors.New("store: invalid Bangladeshi phone number")
	// ErrEmptyOrderItems is returned when attempting to create an order with no items.
	ErrEmptyOrderItems = errors.New("store: order must contain at least one item")
)

// bdPhoneRegex matches valid Bangladeshi mobile numbers: optional +88 followed by 01[3-9] and 8 digits.
var bdPhoneRegex = regexp.MustCompile(`^(\+88)?01[3-9]\d{8}$`)

// ValidateBDPhone validates and normalizes a Bangladeshi mobile number.
func ValidateBDPhone(raw string) (string, error) {
	cleaned := strings.TrimSpace(raw)
	cleaned = strings.NewReplacer(" ", "", "-", "", "(", "", ")", "").Replace(cleaned)
	if !bdPhoneRegex.MatchString(cleaned) {
		return "", fmt.Errorf("%w: %q (expected 01XXXXXXXXX or +8801XXXXXXXXX)", ErrInvalidPhone, raw)
	}
	return cleaned, nil
}

// Product represents a catalog item from the products table.
type Product struct {
	ID          string          `json:"id"`
	SKU         string          `json:"sku"`
	NameEN      string          `json:"name_en"`
	NameBN      string          `json:"name_bn"`
	Description string          `json:"description"`
	PriceBDT    float64         `json:"price_bdt"`
	StockQty    int             `json:"stock_qty"`
	Variants    json.RawMessage `json:"variants"`
	Score       float64         `json:"score,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// StockStatus represents deterministic inventory and price information for a SKU and optional variant.
type StockStatus struct {
	SKU           string          `json:"sku"`
	NameEN        string          `json:"name_en"`
	NameBN        string          `json:"name_bn"`
	Variant       string          `json:"variant,omitempty"`
	PriceBDT      float64         `json:"price_bdt"`
	AvailableQty  int             `json:"available_qty"`
	InStock       bool            `json:"in_stock"`
	AllVariants   json.RawMessage `json:"all_variants,omitempty"`
}

// DeliveryQuote represents a deterministic delivery fee calculation for a Bangladeshi district.
type DeliveryQuote struct {
	District        string  `json:"district"`
	Zone            string  `json:"zone"`
	DeliveryFeeBDT  float64 `json:"delivery_fee_bdt"`
	EstimatedDays   string  `json:"estimated_days"`
	EstimatedDaysBN string  `json:"estimated_days_bn"`
	CashOnDelivery  bool    `json:"cash_on_delivery"`
}

// CalculateDeliveryFee computes the deterministic BDT delivery charge and ETA for a district.
// Rules per PRD 4.2:
// - Inside Dhaka: 70 BDT (1-2 days)
// - Dhaka Suburbs (Gazipur, Narayanganj, Savar, Keraniganj, Tongi, Ashulia): 100 BDT (2-3 days)
// - Outside Dhaka: 130 BDT (2-4 days)
func CalculateDeliveryFee(district string) DeliveryQuote {
	norm := strings.ToLower(strings.TrimSpace(district))

	suburbKeywords := []string{
		"gazipur", "গাজীপুর", "gajipur",
		"narayanganj", "নারায়ণগঞ্জ", "নারায়ণগঞ্জ", "narayongonj",
		"savar", "সাভার", "shavar",
		"keraniganj", "কেরানীগঞ্জ", "keranigonj",
		"tongi", "টঙ্গী",
		"ashulia", "আশুলিয়া", "আশুলিয়া",
	}
	for _, kw := range suburbKeywords {
		if strings.Contains(norm, kw) {
			return DeliveryQuote{
				District:        strings.TrimSpace(district),
				Zone:            "Dhaka Suburbs",
				DeliveryFeeBDT:  100,
				EstimatedDays:   "2-3 days",
				EstimatedDaysBN: "২-৩ কার্যদিবস",
				CashOnDelivery:  true,
			}
		}
	}

	outsideMarkers := []string{
		"outside dhaka", "dhakar baire", "ঢাকার বাইরে",
	}
	for _, m := range outsideMarkers {
		if strings.Contains(norm, m) {
			return DeliveryQuote{
				District:        strings.TrimSpace(district),
				Zone:            "Outside Dhaka",
				DeliveryFeeBDT:  130,
				EstimatedDays:   "2-4 days",
				EstimatedDaysBN: "২-৪ কার্যদিবস",
				CashOnDelivery:  true,
			}
		}
	}

	insideDhakaKeywords := []string{
		"dhaka", "ঢাকা", "mirpur", "মিরপুর", "uttara", "উত্তরা",
		"dhanmondi", "ধানমন্ডি", "gulshan", "গুলশান", "banani", "বনানী",
		"mohammadpur", "মোহাম্মদপুর", "badda", "বাড্ডা", "motijheel", "মতিঝিল",
		"khilgaon", "খিলগাঁও", "malibagh", "মালিবাগ", "rampura", "রামপুরা",
		"bashundhara", "বসুন্ধরা", "tejgaon", "তেজগাঁও", "jatrabari", "যাত্রাবাড়ী",
	}
	for _, kw := range insideDhakaKeywords {
		if strings.Contains(norm, kw) {
			return DeliveryQuote{
				District:        strings.TrimSpace(district),
				Zone:            "Inside Dhaka",
				DeliveryFeeBDT:  70,
				EstimatedDays:   "1-2 days",
				EstimatedDaysBN: "১-২ কার্যদিবস",
				CashOnDelivery:  true,
			}
		}
	}

	return DeliveryQuote{
		District:        strings.TrimSpace(district),
		Zone:            "Outside Dhaka",
		DeliveryFeeBDT:  130,
		EstimatedDays:   "2-4 days",
		EstimatedDaysBN: "২-৪ কার্যদিবস",
		CashOnDelivery:  true,
	}
}

// Customer represents a Messenger user in the customers table.
type Customer struct {
	SenderID       string    `json:"sender_id"`
	FullName       string    `json:"full_name,omitempty"`
	Phone          string    `json:"phone,omitempty"`
	DefaultAddress string    `json:"default_address,omitempty"`
	District       string    `json:"district,omitempty"`
	BotPaused      bool      `json:"bot_paused"`
	CreatedAt      time.Time `json:"created_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
}

// ChatMessage represents a persisted conversation turn in chat_messages and Redis cache.
type ChatMessage struct {
	ID        int64           `json:"id,omitempty"`
	SenderID  string          `json:"sender_id"`
	MID       string          `json:"mid,omitempty"`
	Role      string          `json:"role"`
	Content   string          `json:"content"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

// OrderItemInput represents a line item requested for order creation.
type OrderItemInput struct {
	SKU      string `json:"sku"`
	Variant  string `json:"variant,omitempty"`
	Quantity int    `json:"quantity"`
}

// OrderLineItem represents a priced and verified order line item persisted in orders.items.
type OrderLineItem struct {
	SKU          string  `json:"sku"`
	NameEN       string  `json:"name_en"`
	NameBN       string  `json:"name_bn"`
	Variant      string  `json:"variant,omitempty"`
	Quantity     int     `json:"quantity"`
	UnitPriceBDT float64 `json:"unit_price_bdt"`
	LineTotalBDT float64 `json:"line_total_bdt"`
}

// CreateOrderRequest holds the parameters required to create a customer order transactionally.
type CreateOrderRequest struct {
	SenderID     string           `json:"sender_id"`
	CustomerName string           `json:"customer_name"`
	Phone        string           `json:"phone"`
	Address      string           `json:"address"`
	District     string           `json:"district"`
	Items        []OrderItemInput `json:"items"`
}

// OrderConfirmation is returned after a successful transactional order creation.
type OrderConfirmation struct {
	OrderID         string          `json:"order_id"`
	OrderNumber     string          `json:"order_number"`
	SenderID        string          `json:"sender_id"`
	CustomerName    string          `json:"customer_name"`
	Phone           string          `json:"phone"`
	Address         string          `json:"address"`
	District        string          `json:"district"`
	Zone            string          `json:"zone"`
	Items           []OrderLineItem `json:"items"`
	SubtotalBDT     float64         `json:"subtotal_bdt"`
	DeliveryFeeBDT  float64         `json:"delivery_fee_bdt"`
	TotalBDT        float64         `json:"total_bdt"`
	EstimatedDaysBN string          `json:"estimated_days_bn"`
	Status          string          `json:"status"`
	CreatedAt       time.Time       `json:"created_at"`
}

// PostgresStore wraps *pgxpool.Pool to execute customer, message, product, and order queries.
type PostgresStore struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewPostgresPool initializes and pings a PostgreSQL connection pool.
func NewPostgresPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("store: parse database url: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: create pgxpool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping postgres: %w", err)
	}
	return pool, nil
}

// NewPostgresStore creates a new PostgresStore with explicit dependency injection.
func NewPostgresStore(pool *pgxpool.Pool, logger *slog.Logger) *PostgresStore {
	if logger == nil {
		logger = slog.Default()
	}
	return &PostgresStore{
		pool:   pool,
		logger: logger,
	}
}

// Pool returns the underlying *pgxpool.Pool.
func (s *PostgresStore) Pool() *pgxpool.Pool {
	return s.pool
}

// sqlExecer is satisfied by both *pgxpool.Pool and pgx.Tx for shared query helpers.
type sqlExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

const upsertCustomerQuery = `
INSERT INTO customers (sender_id, full_name, phone, default_address, district, bot_paused, updated_at)
VALUES ($1, NULLIF($2, ''), NULLIF($3, ''), NULLIF($4, ''), NULLIF($5, ''), $6, NOW())
ON CONFLICT (sender_id) DO UPDATE SET
    full_name       = COALESCE(NULLIF(EXCLUDED.full_name, ''), customers.full_name),
    phone           = COALESCE(NULLIF(EXCLUDED.phone, ''), customers.phone),
    default_address = COALESCE(NULLIF(EXCLUDED.default_address, ''), customers.default_address),
    district        = COALESCE(NULLIF(EXCLUDED.district, ''), customers.district),
    updated_at      = NOW()
`

func execUpsertCustomer(ctx context.Context, exec sqlExecer, c Customer) error {
	if strings.TrimSpace(c.SenderID) == "" {
		return errors.New("store: upsert customer: sender_id is required")
	}
	if _, err := exec.Exec(ctx, upsertCustomerQuery,
		strings.TrimSpace(c.SenderID),
		strings.TrimSpace(c.FullName),
		strings.TrimSpace(c.Phone),
		strings.TrimSpace(c.DefaultAddress),
		strings.TrimSpace(c.District),
		c.BotPaused,
	); err != nil {
		return fmt.Errorf("store: upsert customer %s: %w", c.SenderID, err)
	}
	return nil
}

// SeedSampleProducts inserts the default Bangladeshi e-commerce product catalog if not already present.
func (s *PostgresStore) SeedSampleProducts(ctx context.Context) error {
	const seedSQL = `
INSERT INTO products (sku, name_en, name_bn, description, price_bdt, stock_qty, variants)
VALUES
    ('HD-BLK-001', 'Premium Black Fleece Hoodie', 'প্রিমিয়াম ব্ল্যাক ফ্লিস হুডি', 'Heavyweight 320 GSM winter cotton fleece hoodie in jet black.', 1250.00, 25, '{"sizes":{"M":5,"L":8,"XL":6,"XXL":6},"category":"hoodie","color":"black"}'::jsonb),
    ('PNJ-WHT-002', 'Embroidered Cotton Panjabi', 'এমব্রয়ডারি সুতি পাঞ্জাবি', '100% breathable combed cotton white panjabi with collar embroidery.', 1850.00, 20, '{"sizes":{"40":5,"42":8,"44":7},"category":"panjabi","color":"white"}'::jsonb),
    ('SAR-JMD-003', 'Dhakai Jamdani Saree', 'ঢাকাই জামদানি শাড়ি', 'Authentic handloom cotton-silk Dhakai Jamdani saree.', 4500.00, 10, '{"category":"saree","color":"red-gold"}'::jsonb),
    ('TSH-NVY-004', 'Heavyweight Drop-Shoulder T-Shirt', 'ড্রপ-শোল্ডার কটন টি-শার্ট', '220 GSM organic ring-spun cotton oversized drop-shoulder t-shirt.', 650.00, 40, '{"sizes":{"M":10,"L":15,"XL":10,"XXL":5},"category":"t-shirt","color":"navy"}'::jsonb),
    ('CHN-KHK-005', 'Stretchable Gabardine Chinos', 'স্ট্রেচেবল গ্যাবার্ডিন চিনোস প্যান্ট', 'Slim-fit cotton spandex stretchable gabardine trouser in khaki.', 1150.00, 30, '{"sizes":{"30":6,"32":10,"34":8,"36":6},"category":"pant","color":"khaki"}'::jsonb)
ON CONFLICT (sku) DO NOTHING
`
	if _, err := s.pool.Exec(ctx, seedSQL); err != nil {
		return fmt.Errorf("store: seed sample products: %w", err)
	}
	return nil
}

// UpsertCustomer inserts a customer record or updates non-empty profile fields on conflict.
func (s *PostgresStore) UpsertCustomer(ctx context.Context, c Customer) error {
	return execUpsertCustomer(ctx, s.pool, c)
}

// SetCustomerBotPaused updates the bot_paused flag in PostgreSQL for a customer thread.
func (s *PostgresStore) SetCustomerBotPaused(ctx context.Context, senderID string, paused bool) error {
	if strings.TrimSpace(senderID) == "" {
		return errors.New("store: set customer bot paused: sender_id is required")
	}
	const q = `
INSERT INTO customers (sender_id, bot_paused, updated_at)
VALUES ($1, $2, NOW())
ON CONFLICT (sender_id) DO UPDATE SET
    bot_paused = EXCLUDED.bot_paused,
    updated_at = NOW()
`
	if _, err := s.pool.Exec(ctx, q, senderID, paused); err != nil {
		return fmt.Errorf("store: set customer bot paused %s: %w", senderID, err)
	}
	return nil
}

// SaveMessage persists a chat turn in chat_messages (ensuring the customer row exists first).
func (s *PostgresStore) SaveMessage(ctx context.Context, msg ChatMessage) (int64, error) {
	if strings.TrimSpace(msg.SenderID) == "" {
		return 0, errors.New("store: save message: sender_id is required")
	}
	switch msg.Role {
	case "user", "assistant", "tool", "system":
	default:
		return 0, fmt.Errorf("store: save message: invalid role %q", msg.Role)
	}

	if err := s.UpsertCustomer(ctx, Customer{SenderID: msg.SenderID}); err != nil {
		return 0, err
	}

	meta := msg.Metadata
	if len(meta) == 0 {
		meta = json.RawMessage(`{}`)
	}

	var midArg any
	if strings.TrimSpace(msg.MID) != "" {
		midArg = strings.TrimSpace(msg.MID)
	}

	const q = `
INSERT INTO chat_messages (sender_id, mid, role, content, metadata)
VALUES ($1, $2, $3, $4, $5::jsonb)
ON CONFLICT (mid) DO UPDATE SET content = EXCLUDED.content
RETURNING id
`
	var id int64
	if err := s.pool.QueryRow(ctx, q, msg.SenderID, midArg, msg.Role, msg.Content, string(meta)).Scan(&id); err != nil {
		return 0, fmt.Errorf("store: save message for %s: %w", msg.SenderID, err)
	}
	return id, nil
}

// GetRecentHistory retrieves the most recent limit messages for senderID in chronological order (oldest first).
func (s *PostgresStore) GetRecentHistory(ctx context.Context, senderID string, limit int) ([]ChatMessage, error) {
	if strings.TrimSpace(senderID) == "" {
		return nil, errors.New("store: get recent history: sender_id is required")
	}
	if limit <= 0 {
		limit = 16
	}

	const q = `
SELECT id, sender_id, COALESCE(mid, ''), role, content, metadata, created_at
FROM chat_messages
WHERE sender_id = $1
ORDER BY created_at DESC, id DESC
LIMIT $2
`
	rows, err := s.pool.Query(ctx, q, senderID, limit)
	if err != nil {
		return nil, fmt.Errorf("store: query recent history for %s: %w", senderID, err)
	}
	defer rows.Close()

	var desc []ChatMessage
	for rows.Next() {
		var m ChatMessage
		if err := rows.Scan(&m.ID, &m.SenderID, &m.MID, &m.Role, &m.Content, &m.Metadata, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: scan chat message: %w", err)
		}
		desc = append(desc, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate recent history: %w", err)
	}

	// Reverse into chronological order (oldest -> newest) for LLM prompt context.
	for i, j := 0, len(desc)-1; i < j; i, j = i+1, j-1 {
		desc[i], desc[j] = desc[j], desc[i]
	}
	return desc, nil
}

// SearchProductsHybrid searches products combining pgvector cosine similarity and pg_trgm text similarity.
// When queryEmbedding is empty, it falls back cleanly to pg_trgm + ILIKE keyword ranking.
func (s *PostgresStore) SearchProductsHybrid(
	ctx context.Context,
	query string,
	category string,
	queryEmbedding []float32,
	limit int,
) ([]Product, error) {
	query = strings.TrimSpace(query)
	category = strings.TrimSpace(category)
	if limit <= 0 {
		limit = 5
	}

	var (
		rows pgx.Rows
		err  error
	)

	if len(queryEmbedding) > 0 {
		vec := pgvector.NewVector(queryEmbedding)
		const hybridSQL = `
SELECT
    id,
    sku,
    name_en,
    name_bn,
    description,
    price_bdt,
    stock_qty,
    variants,
    (
        COALESCE(0.65 * (1 - (embedding <=> $1::vector)), 0) +
        0.35 * GREATEST(
            similarity(name_en, $2),
            word_similarity($2, name_en || ' ' || name_bn || ' ' || description),
            CASE WHEN name_en ILIKE '%' || $2 || '%' OR name_bn ILIKE '%' || $2 || '%' OR description ILIKE '%' || $2 || '%' THEN 0.6 ELSE 0 END
        )
    ) AS hybrid_score,
    created_at,
    updated_at
FROM products
WHERE ($3 = '' OR COALESCE(variants->>'category', '') ILIKE '%' || $3 || '%' OR description ILIKE '%' || $3 || '%')
ORDER BY hybrid_score DESC, stock_qty DESC
LIMIT $4
`
		rows, err = s.pool.Query(ctx, hybridSQL, vec, query, category, limit)
	} else {
		const trgmSQL = `
SELECT
    id,
    sku,
    name_en,
    name_bn,
    description,
    price_bdt,
    stock_qty,
    variants,
    GREATEST(
        similarity(name_en, $1),
        word_similarity($1, name_en || ' ' || name_bn || ' ' || description),
        CASE WHEN name_en ILIKE '%' || $1 || '%' OR name_bn ILIKE '%' || $1 || '%' OR description ILIKE '%' || $1 || '%' OR sku ILIKE '%' || $1 || '%' THEN 0.75 ELSE 0 END
    ) AS hybrid_score,
    created_at,
    updated_at
FROM products
WHERE ($2 = '' OR COALESCE(variants->>'category', '') ILIKE '%' || $2 || '%' OR description ILIKE '%' || $2 || '%')
  AND (
      $1 = ''
      OR similarity(name_en, $1) > 0.1
      OR word_similarity($1, name_en || ' ' || name_bn || ' ' || description) > 0.15
      OR name_en ILIKE '%' || $1 || '%'
      OR name_bn ILIKE '%' || $1 || '%'
      OR description ILIKE '%' || $1 || '%'
      OR sku ILIKE '%' || $1 || '%'
  )
ORDER BY hybrid_score DESC, stock_qty DESC
LIMIT $3
`
		rows, err = s.pool.Query(ctx, trgmSQL, query, category, limit)
	}

	if err != nil {
		return nil, fmt.Errorf("store: search products hybrid: %w", err)
	}
	defer rows.Close()

	var products []Product
	for rows.Next() {
		var p Product
		if err := rows.Scan(
			&p.ID,
			&p.SKU,
			&p.NameEN,
			&p.NameBN,
			&p.Description,
			&p.PriceBDT,
			&p.StockQty,
			&p.Variants,
			&p.Score,
			&p.CreatedAt,
			&p.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("store: scan product: %w", err)
		}
		products = append(products, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate products: %w", err)
	}
	return products, nil
}

// CheckStock retrieves live stock and price for a product SKU and optional variant.
func (s *PostgresStore) CheckStock(ctx context.Context, sku string, variant string) (*StockStatus, error) {
	sku = strings.TrimSpace(sku)
	variant = strings.TrimSpace(variant)
	if sku == "" {
		return nil, errors.New("store: check stock: sku is required")
	}

	const q = `
SELECT sku, name_en, name_bn, price_bdt, stock_qty, variants
FROM products
WHERE UPPER(sku) = UPPER($1)
`
	var (
		dbSKU    string
		nameEN   string
		nameBN   string
		priceBDT float64
		stockQty int
		variants json.RawMessage
	)
	err := s.pool.QueryRow(ctx, q, sku).Scan(&dbSKU, &nameEN, &nameBN, &priceBDT, &stockQty, &variants)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", ErrProductNotFound, sku)
		}
		return nil, fmt.Errorf("store: check stock %s: %w", sku, err)
	}

	availQty := resolveVariantQty(stockQty, variants, variant)
	return &StockStatus{
		SKU:          dbSKU,
		NameEN:       nameEN,
		NameBN:       nameBN,
		Variant:      variant,
		PriceBDT:     priceBDT,
		AvailableQty: availQty,
		InStock:      availQty > 0,
		AllVariants:  variants,
	}, nil
}

// CreateOrderTx validates inputs, locks product rows (FOR UPDATE), verifies and decrements inventory,
// calculates deterministic totals + delivery fee, and inserts the order inside a single PostgreSQL transaction.
func (s *PostgresStore) CreateOrderTx(ctx context.Context, req CreateOrderRequest) (*OrderConfirmation, error) {
	req.SenderID = strings.TrimSpace(req.SenderID)
	req.CustomerName = strings.TrimSpace(req.CustomerName)
	req.Address = strings.TrimSpace(req.Address)
	req.District = strings.TrimSpace(req.District)

	if req.SenderID == "" {
		return nil, errors.New("store: create order: sender_id is required")
	}
	if req.CustomerName == "" {
		return nil, errors.New("store: create order: customer_name is required")
	}
	if req.Address == "" {
		return nil, errors.New("store: create order: address is required")
	}
	if req.District == "" {
		return nil, errors.New("store: create order: district is required")
	}
	if len(req.Items) == 0 {
		return nil, ErrEmptyOrderItems
	}

	normalizedPhone, err := ValidateBDPhone(req.Phone)
	if err != nil {
		return nil, err
	}

	for _, item := range req.Items {
		if strings.TrimSpace(item.SKU) == "" {
			return nil, errors.New("store: create order: item sku is required")
		}
		if item.Quantity <= 0 {
			return nil, fmt.Errorf("store: create order: item %s quantity must be positive, got %d", item.SKU, item.Quantity)
		}
	}

	// Sort items by SKU to acquire row locks in a consistent order and prevent deadlocks.
	sortedItems := make([]OrderItemInput, len(req.Items))
	copy(sortedItems, req.Items)
	sort.Slice(sortedItems, func(i, j int) bool {
		return strings.ToUpper(sortedItems[i].SKU) < strings.ToUpper(sortedItems[j].SKU)
	})

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("store: begin order tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// Upsert customer inside the transaction using shared helper.
	if err := execUpsertCustomer(ctx, tx, Customer{
		SenderID:       req.SenderID,
		FullName:       req.CustomerName,
		Phone:          normalizedPhone,
		DefaultAddress: req.Address,
		District:       req.District,
	}); err != nil {
		return nil, fmt.Errorf("store: upsert customer in order tx: %w", err)
	}

	const lockProductSQL = `
SELECT id, sku, name_en, name_bn, price_bdt, stock_qty, variants
FROM products
WHERE UPPER(sku) = UPPER($1)
FOR UPDATE
`
	const decrementStockSQL = `
UPDATE products
SET stock_qty = stock_qty - $1,
    updated_at = NOW()
WHERE id = $2
`

	lineItems := make([]OrderLineItem, 0, len(sortedItems))
	var subtotalBDT float64

	for _, item := range sortedItems {
		var (
			prodID   string
			dbSKU    string
			nameEN   string
			nameBN   string
			priceBDT float64
			stockQty int
			variants json.RawMessage
		)
		err := tx.QueryRow(ctx, lockProductSQL, strings.TrimSpace(item.SKU)).Scan(
			&prodID, &dbSKU, &nameEN, &nameBN, &priceBDT, &stockQty, &variants,
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, fmt.Errorf("%w: %s", ErrProductNotFound, item.SKU)
			}
			return nil, fmt.Errorf("store: lock product %s: %w", item.SKU, err)
		}

		availQty := resolveVariantQty(stockQty, variants, item.Variant)
		if availQty < item.Quantity || stockQty < item.Quantity {
			return nil, fmt.Errorf("%w: sku=%s variant=%q requested=%d available=%d",
				ErrInsufficientStock, dbSKU, item.Variant, item.Quantity, availQty)
		}

		if _, err := tx.Exec(ctx, decrementStockSQL, item.Quantity, prodID); err != nil {
			return nil, fmt.Errorf("store: decrement stock for %s: %w", dbSKU, err)
		}

		lineTotal := roundBDT(priceBDT * float64(item.Quantity))
		subtotalBDT = roundBDT(subtotalBDT + lineTotal)

		lineItems = append(lineItems, OrderLineItem{
			SKU:          dbSKU,
			NameEN:       nameEN,
			NameBN:       nameBN,
			Variant:      strings.TrimSpace(item.Variant),
			Quantity:     item.Quantity,
			UnitPriceBDT: priceBDT,
			LineTotalBDT: lineTotal,
		})
	}

	quote := CalculateDeliveryFee(req.District)
	deliveryFeeBDT := roundBDT(quote.DeliveryFeeBDT)
	totalBDT := roundBDT(subtotalBDT + deliveryFeeBDT)

	itemsJSON, err := json.Marshal(lineItems)
	if err != nil {
		return nil, fmt.Errorf("store: marshal order items: %w", err)
	}

	orderNumber := generateOrderNumber()

	const insertOrderSQL = `
INSERT INTO orders (
    order_number,
    sender_id,
    customer_name,
    phone,
    address,
    district,
    items,
    subtotal_bdt,
    delivery_fee_bdt,
    total_bdt,
    status
) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, 'pending')
RETURNING id, status, created_at
`
	var (
		orderID   string
		status    string
		createdAt time.Time
	)
	if err := tx.QueryRow(ctx, insertOrderSQL,
		orderNumber,
		req.SenderID,
		req.CustomerName,
		normalizedPhone,
		req.Address,
		req.District,
		string(itemsJSON),
		subtotalBDT,
		deliveryFeeBDT,
		totalBDT,
	).Scan(&orderID, &status, &createdAt); err != nil {
		return nil, fmt.Errorf("store: insert order: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("store: commit order tx: %w", err)
	}

	return &OrderConfirmation{
		OrderID:         orderID,
		OrderNumber:     orderNumber,
		SenderID:        req.SenderID,
		CustomerName:    req.CustomerName,
		Phone:           normalizedPhone,
		Address:         req.Address,
		District:        req.District,
		Zone:            quote.Zone,
		Items:           lineItems,
		SubtotalBDT:     subtotalBDT,
		DeliveryFeeBDT:  deliveryFeeBDT,
		TotalBDT:        totalBDT,
		EstimatedDaysBN: quote.EstimatedDaysBN,
		Status:          status,
		CreatedAt:       createdAt,
	}, nil
}

// resolveVariantQty checks whether a specific variant (e.g. size "XXL") has per-variant stock in JSONB,
// or falls back to overall stockQty.
func resolveVariantQty(stockQty int, rawVariants json.RawMessage, variant string) int {
	variant = strings.TrimSpace(variant)
	if variant == "" || len(rawVariants) == 0 {
		return stockQty
	}

	var parsed map[string]any
	if err := json.Unmarshal(rawVariants, &parsed); err != nil || len(parsed) == 0 {
		return stockQty
	}

	// Check nested "sizes" map if present: {"sizes": {"M": 5, "XXL": 3}}
	if sizesRaw, ok := parsed["sizes"]; ok {
		if sizesMap, ok := sizesRaw.(map[string]any); ok {
			if qty, found := lookupCaseInsensitiveInt(sizesMap, variant); found {
				if qty < stockQty {
					return qty
				}
				return stockQty
			}
			// Variant requested does not exist in sizes map
			return 0
		}
		if sizesSlice, ok := sizesRaw.([]any); ok {
			for _, s := range sizesSlice {
				if strings.EqualFold(fmt.Sprint(s), variant) {
					return stockQty
				}
			}
			return 0
		}
	}

	// Check direct top-level key if numeric: {"M": 5, "XXL": 3}
	if qty, found := lookupCaseInsensitiveInt(parsed, variant); found {
		if qty < stockQty {
			return qty
		}
		return stockQty
	}

	return stockQty
}

func lookupCaseInsensitiveInt(m map[string]any, key string) (int, bool) {
	for k, v := range m {
		if strings.EqualFold(strings.TrimSpace(k), key) {
			switch n := v.(type) {
			case float64:
				return int(n), true
			case int:
				return n, true
			}
		}
	}
	return 0, false
}

func roundBDT(val float64) float64 {
	return math.Round(val*100) / 100
}

func generateOrderNumber() string {
	var b [3]byte
	_, _ = rand.Read(b[:])
	suffix := strings.ToUpper(hex.EncodeToString(b[:]))
	return fmt.Sprintf("ORD-%s-%s", time.Now().UTC().Format("20060102"), suffix)
}
