package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func TestValidateBDPhone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     string
		wantPhone string
		wantErr   bool
	}{
		{
			name:      "valid Grameenphone 11-digit",
			input:     "01712345678",
			wantPhone: "01712345678",
			wantErr:   false,
		},
		{
			name:      "valid Robi with +88 prefix",
			input:     "+8801812345678",
			wantPhone: "+8801812345678",
			wantErr:   false,
		},
		{
			name:      "valid Banglalink with spaces and hyphens",
			input:     " +88 01912-345678 ",
			wantPhone: "+8801912345678",
			wantErr:   false,
		},
		{
			name:      "valid Teletalk 015",
			input:     "01512345678",
			wantPhone: "01512345678",
			wantErr:   false,
		},
		{
			name:      "valid Airtel 016",
			input:     "01612345678",
			wantPhone: "01612345678",
			wantErr:   false,
		},
		{
			name:    "invalid operator prefix 011",
			input:   "01112345678",
			wantErr: true,
		},
		{
			name:    "invalid operator prefix 012",
			input:   "01212345678",
			wantErr: true,
		},
		{
			name:    "too short 10 digits",
			input:   "0171234567",
			wantErr: true,
		},
		{
			name:    "too long 12 digits",
			input:   "017123456789",
			wantErr: true,
		},
		{
			name:    "non-numeric characters",
			input:   "0171234567a",
			wantErr: true,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ValidateBDPhone(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ValidateBDPhone(%q) succeeded with %q, expected error", tc.input, got)
				}
				if !errors.Is(err, ErrInvalidPhone) {
					t.Errorf("ValidateBDPhone(%q) error = %v, expected ErrInvalidPhone", tc.input, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ValidateBDPhone(%q) unexpected error: %v", tc.input, err)
			}
			if got != tc.wantPhone {
				t.Errorf("ValidateBDPhone(%q) = %q, want %q", tc.input, got, tc.wantPhone)
			}
		})
	}
}

func TestCalculateDeliveryFee(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		district string
		wantZone string
		wantFee  float64
	}{
		{
			name:     "Inside Dhaka English",
			district: "Dhaka",
			wantZone: "Inside Dhaka",
			wantFee:  70,
		},
		{
			name:     "Inside Dhaka Bangla",
			district: "ঢাকা (মিরপুর ১০)",
			wantZone: "Inside Dhaka",
			wantFee:  70,
		},
		{
			name:     "Dhaka Suburb Gazipur",
			district: "Gazipur",
			wantZone: "Dhaka Suburbs",
			wantFee:  100,
		},
		{
			name:     "Dhaka Suburb Savar in Bangla",
			district: "সাভার, ঢাকা",
			wantZone: "Dhaka Suburbs",
			wantFee:  100,
		},
		{
			name:     "Dhaka Suburb Narayanganj",
			district: "Narayanganj",
			wantZone: "Dhaka Suburbs",
			wantFee:  100,
		},
		{
			name:     "Dhaka Suburb Keraniganj",
			district: "Keraniganj",
			wantZone: "Dhaka Suburbs",
			wantFee:  100,
		},
		{
			name:     "Outside Dhaka Chattogram",
			district: "Chattogram",
			wantZone: "Outside Dhaka",
			wantFee:  130,
		},
		{
			name:     "Outside Dhaka Sylhet Bangla",
			district: "সিলেট",
			wantZone: "Outside Dhaka",
			wantFee:  130,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			quote := CalculateDeliveryFee(tc.district)
			if quote.Zone != tc.wantZone {
				t.Errorf("CalculateDeliveryFee(%q).Zone = %q, want %q", tc.district, quote.Zone, tc.wantZone)
			}
			if quote.DeliveryFeeBDT != tc.wantFee {
				t.Errorf("CalculateDeliveryFee(%q).DeliveryFeeBDT = %v, want %v", tc.district, quote.DeliveryFeeBDT, tc.wantFee)
			}
			if !quote.CashOnDelivery {
				t.Errorf("CalculateDeliveryFee(%q).CashOnDelivery = false, want true", tc.district)
			}
		})
	}
}

func TestRedisStore_PauseAndSlidingHistory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client := redis.NewInMemoryClient()
	rStore := NewRedisStore(client, nil)
	senderID := "USER_BD_1001"

	paused, err := rStore.IsBotPaused(ctx, senderID)
	if err != nil {
		t.Fatalf("IsBotPaused initial error: %v", err)
	}
	if paused {
		t.Fatalf("expected initial paused = false, got true")
	}

	if err := rStore.SetBotPaused(ctx, senderID, true); err != nil {
		t.Fatalf("SetBotPaused(true) error: %v", err)
	}
	paused, err = rStore.IsBotPaused(ctx, senderID)
	if err != nil {
		t.Fatalf("IsBotPaused after pause error: %v", err)
	}
	if !paused {
		t.Fatalf("expected paused = true after SetBotPaused(true)")
	}

	if err := rStore.SetBotPaused(ctx, senderID, false); err != nil {
		t.Fatalf("SetBotPaused(false) error: %v", err)
	}
	paused, err = rStore.IsBotPaused(ctx, senderID)
	if err != nil {
		t.Fatalf("IsBotPaused after resume error: %v", err)
	}
	if paused {
		t.Fatalf("expected paused = false after SetBotPaused(false)")
	}

	// Push 20 turns and verify sliding window retains exactly the last 16 turns.
	for i := 1; i <= 20; i++ {
		msg := ChatMessage{
			SenderID:  senderID,
			Role:      "user",
			Content:   fmt.Sprintf("turn-%d", i),
			CreatedAt: time.Now().UTC(),
		}
		if err := rStore.AppendChatHistory(ctx, senderID, msg); err != nil {
			t.Fatalf("AppendChatHistory turn %d error: %v", i, err)
		}
	}

	history, err := rStore.GetCachedChatHistory(ctx, senderID)
	if err != nil {
		t.Fatalf("GetCachedChatHistory error: %v", err)
	}
	if len(history) != MaxCachedHistoryTurns {
		t.Fatalf("len(history) = %d, want %d", len(history), MaxCachedHistoryTurns)
	}
	if history[0].Content != "turn-5" {
		t.Errorf("history[0].Content = %q, want %q", history[0].Content, "turn-5")
	}
	if history[len(history)-1].Content != "turn-20" {
		t.Errorf("history[15].Content = %q, want %q", history[len(history)-1].Content, "turn-20")
	}
}

// mockPgHandler simulates PostgreSQL tables in memory to test PostgresStore methods and transactions.
type mockPgHandler struct {
	mu        sync.Mutex
	customers map[string]Customer
	messages  []ChatMessage
	products  map[string]*Product
	orders    []OrderConfirmation
}

func newMockPgHandler() *mockPgHandler {
	return &mockPgHandler{
		customers: make(map[string]Customer),
		products: map[string]*Product{
			"HD-BLK-001": {
				ID:          "uuid-prod-1",
				SKU:         "HD-BLK-001",
				NameEN:      "Premium Black Fleece Hoodie",
				NameBN:      "প্রিমিয়াম ব্ল্যাক ফ্লিস হুডি",
				Description: "Heavyweight winter cotton fleece hoodie in black",
				PriceBDT:    1250,
				StockQty:    5,
				Variants:    json.RawMessage(`{"sizes":{"M":2,"L":2,"XXL":3},"category":"hoodie"}`),
				CreatedAt:   time.Now().UTC(),
				UpdatedAt:   time.Now().UTC(),
			},
			"PNJ-WHT-002": {
				ID:          "uuid-prod-2",
				SKU:         "PNJ-WHT-002",
				NameEN:      "Embroidered Cotton Panjabi",
				NameBN:      "এমব্রয়ডারি কটন পাঞ্জাবি",
				Description: "Festive white cotton panjabi",
				PriceBDT:    1800,
				StockQty:    2,
				Variants:    json.RawMessage(`{"sizes":{"40":1,"42":1},"category":"panjabi"}`),
				CreatedAt:   time.Now().UTC(),
				UpdatedAt:   time.Now().UTC(),
			},
		},
	}
}

func (m *mockPgHandler) Ping(_ context.Context) error { return nil }
func (m *mockPgHandler) Close()                       {}

func (m *mockPgHandler) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.execLocked(sql, args...)
}

func (m *mockPgHandler) execLocked(sql string, args ...any) (pgconn.CommandTag, error) {
	norm := strings.ToUpper(sql)
	switch {
	case strings.Contains(norm, "INSERT INTO CUSTOMERS"):
		senderID := fmt.Sprint(args[0])
		existing := m.customers[senderID]
		existing.SenderID = senderID
		if len(args) >= 6 {
			if s := fmt.Sprint(args[1]); s != "" {
				existing.FullName = s
			}
			if s := fmt.Sprint(args[2]); s != "" {
				existing.Phone = s
			}
			if s := fmt.Sprint(args[3]); s != "" {
				existing.DefaultAddress = s
			}
			if s := fmt.Sprint(args[4]); s != "" {
				existing.District = s
			}
			if b, ok := args[5].(bool); ok {
				existing.BotPaused = b
			}
		} else if len(args) == 5 {
			existing.FullName = fmt.Sprint(args[1])
			existing.Phone = fmt.Sprint(args[2])
			existing.DefaultAddress = fmt.Sprint(args[3])
			existing.District = fmt.Sprint(args[4])
		} else if len(args) == 2 {
			if b, ok := args[1].(bool); ok {
				existing.BotPaused = b
			}
		}
		m.customers[senderID] = existing
		return pgconn.NewCommandTag("INSERT 0 1", 1), nil

	case strings.Contains(norm, "UPDATE PRODUCTS"):
		qty, _ := args[0].(int)
		prodID := fmt.Sprint(args[1])
		for _, p := range m.products {
			if p.ID == prodID {
				p.StockQty -= qty
				return pgconn.NewCommandTag("UPDATE 1", 1), nil
			}
		}
		return pgconn.NewCommandTag("UPDATE 0", 0), nil
	}
	return pgconn.NewCommandTag("OK", 1), nil
}

func (m *mockPgHandler) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	norm := strings.ToUpper(sql)

	if strings.Contains(norm, "FROM CHAT_MESSAGES") {
		senderID := fmt.Sprint(args[0])
		limit, _ := args[1].(int)
		var rows [][]any
		for i := len(m.messages) - 1; i >= 0 && len(rows) < limit; i-- {
			msg := m.messages[i]
			if msg.SenderID == senderID {
				rows = append(rows, []any{
					msg.ID, msg.SenderID, msg.MID, msg.Role, msg.Content, msg.Metadata, msg.CreatedAt,
				})
			}
		}
		return pgxpool.NewSliceRows(rows), nil
	}

	if strings.Contains(norm, "FROM PRODUCTS") {
		var rows [][]any
		for _, p := range m.products {
			rows = append(rows, []any{
				p.ID, p.SKU, p.NameEN, p.NameBN, p.Description, p.PriceBDT, p.StockQty, p.Variants, 0.92, p.CreatedAt, p.UpdatedAt,
			})
		}
		return pgxpool.NewSliceRows(rows), nil
	}

	return pgxpool.NewSliceRows(nil), nil
}

func (m *mockPgHandler) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.queryRowLocked(sql, args...)
}

func (m *mockPgHandler) queryRowLocked(sql string, args ...any) pgx.Row {
	norm := strings.ToUpper(sql)

	if strings.Contains(norm, "INSERT INTO CHAT_MESSAGES") {
		id := int64(len(m.messages) + 1)
		mid := ""
		if args[1] != nil {
			mid = fmt.Sprint(args[1])
		}
		m.messages = append(m.messages, ChatMessage{
			ID:        id,
			SenderID:  fmt.Sprint(args[0]),
			MID:       mid,
			Role:      fmt.Sprint(args[2]),
			Content:   fmt.Sprint(args[3]),
			Metadata:  json.RawMessage(fmt.Sprint(args[4])),
			CreatedAt: time.Now().UTC(),
		})
		return pgxpool.NewSliceRow(id)
	}

	if strings.Contains(norm, "FROM PRODUCTS") && strings.Contains(norm, "FOR UPDATE") {
		sku := strings.ToUpper(strings.TrimSpace(fmt.Sprint(args[0])))
		p, ok := m.products[sku]
		if !ok {
			return pgxpool.NewErrorRow(pgx.ErrNoRows)
		}
		return pgxpool.NewSliceRow(p.ID, p.SKU, p.NameEN, p.NameBN, p.PriceBDT, p.StockQty, p.Variants)
	}

	if strings.Contains(norm, "FROM PRODUCTS") {
		sku := strings.ToUpper(strings.TrimSpace(fmt.Sprint(args[0])))
		p, ok := m.products[sku]
		if !ok {
			return pgxpool.NewErrorRow(pgx.ErrNoRows)
		}
		return pgxpool.NewSliceRow(p.SKU, p.NameEN, p.NameBN, p.PriceBDT, p.StockQty, p.Variants)
	}

	if strings.Contains(norm, "INSERT INTO ORDERS") {
		return pgxpool.NewSliceRow("uuid-order-101", "pending", time.Now().UTC())
	}

	return pgxpool.NewErrorRow(pgx.ErrNoRows)
}

func (m *mockPgHandler) Begin(ctx context.Context) (pgx.Tx, error) {
	return m.BeginTx(ctx, pgx.TxOptions{})
}

func (m *mockPgHandler) BeginTx(_ context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
	m.mu.Lock()
	snapshot := make(map[string]int, len(m.products))
	for k, p := range m.products {
		snapshot[k] = p.StockQty
	}
	m.mu.Unlock()
	return &mockTx{h: m, stockSnapshot: snapshot}, nil
}

type mockTx struct {
	h             *mockPgHandler
	stockSnapshot map[string]int
	committed     bool
	rolledBack    bool
}

func (tx *mockTx) Begin(_ context.Context) (pgx.Tx, error) { return tx, nil }

func (tx *mockTx) Commit(_ context.Context) error {
	tx.committed = true
	return nil
}

func (tx *mockTx) Rollback(_ context.Context) error {
	if tx.committed || tx.rolledBack {
		return nil
	}
	tx.rolledBack = true
	tx.h.mu.Lock()
	defer tx.h.mu.Unlock()
	for k, qty := range tx.stockSnapshot {
		if p, ok := tx.h.products[k]; ok {
			p.StockQty = qty
		}
	}
	return nil
}

func (tx *mockTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return tx.h.Exec(ctx, sql, arguments...)
}

func (tx *mockTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return tx.h.Query(ctx, sql, args...)
}

func (tx *mockTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return tx.h.QueryRow(ctx, sql, args...)
}

func TestPostgresStore_OrderAndStockFlow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mockDB := newMockPgHandler()
	pool := pgxpool.NewWithHandler(mockDB)
	defer pool.Close()

	pgStore := NewPostgresStore(pool, nil)

	// 1. Save & fetch chat messages in chronological order
	if _, err := pgStore.SaveMessage(ctx, ChatMessage{
		SenderID: "USER_BD_01",
		MID:      "mid.1",
		Role:     "user",
		Content:  "vai ei black hoodie ta XXL size hobe?",
	}); err != nil {
		t.Fatalf("SaveMessage 1 failed: %v", err)
	}
	if _, err := pgStore.SaveMessage(ctx, ChatMessage{
		SenderID: "USER_BD_01",
		MID:      "mid.2",
		Role:     "assistant",
		Content:  "জি স্যার, XXL সাইজ স্টকে আছে।",
	}); err != nil {
		t.Fatalf("SaveMessage 2 failed: %v", err)
	}

	history, err := pgStore.GetRecentHistory(ctx, "USER_BD_01", 10)
	if err != nil {
		t.Fatalf("GetRecentHistory failed: %v", err)
	}
	if len(history) != 2 || history[0].MID != "mid.1" || history[1].MID != "mid.2" {
		t.Fatalf("unexpected history ordering: %+v", history)
	}

	// 2. CheckStock for existing variant XXL
	stock, err := pgStore.CheckStock(ctx, "HD-BLK-001", "XXL")
	if err != nil {
		t.Fatalf("CheckStock failed: %v", err)
	}
	if !stock.InStock || stock.AvailableQty != 3 || stock.PriceBDT != 1250 {
		t.Fatalf("unexpected StockStatus: %+v", stock)
	}

	// 3. Hybrid product search
	products, err := pgStore.SearchProductsHybrid(ctx, "black hoodie", "hoodie", []float32{0.1, 0.2, 0.3}, 5)
	if err != nil {
		t.Fatalf("SearchProductsHybrid failed: %v", err)
	}
	if len(products) == 0 {
		t.Fatalf("expected products from SearchProductsHybrid, got 0")
	}

	// 4. Successful CreateOrderTx inside Dhaka
	orderResp, err := pgStore.CreateOrderTx(ctx, CreateOrderRequest{
		SenderID:     "USER_BD_01",
		CustomerName: "রহিম উদ্দিন",
		Phone:        "01712-345678",
		Address:      "বাড়ি ১২, রোড ৫, ধানমন্ডি",
		District:     "Dhaka",
		Items: []OrderItemInput{
			{SKU: "HD-BLK-001", Variant: "XXL", Quantity: 2},
		},
	})
	if err != nil {
		t.Fatalf("CreateOrderTx failed: %v", err)
	}
	if orderResp.SubtotalBDT != 2500 || orderResp.DeliveryFeeBDT != 70 || orderResp.TotalBDT != 2570 {
		t.Errorf("unexpected totals: subtotal=%v delivery=%v total=%v",
			orderResp.SubtotalBDT, orderResp.DeliveryFeeBDT, orderResp.TotalBDT)
	}

	// Verify stock was decremented from 5 to 3
	stockAfter, err := pgStore.CheckStock(ctx, "HD-BLK-001", "")
	if err != nil {
		t.Fatalf("CheckStock after order failed: %v", err)
	}
	if stockAfter.AvailableQty != 3 {
		t.Errorf("expected remaining stock 3, got %d", stockAfter.AvailableQty)
	}

	// 5. Insufficient stock must fail and roll back transaction
	_, err = pgStore.CreateOrderTx(ctx, CreateOrderRequest{
		SenderID:     "USER_BD_01",
		CustomerName: "রহিম উদ্দিন",
		Phone:        "01712345678",
		Address:      "ধানমন্ডি",
		District:     "Dhaka",
		Items: []OrderItemInput{
			{SKU: "HD-BLK-001", Variant: "XXL", Quantity: 10},
		},
	})
	if !errors.Is(err, ErrInsufficientStock) {
		t.Fatalf("expected ErrInsufficientStock, got %v", err)
	}
}
