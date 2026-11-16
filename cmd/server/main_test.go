package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"messenger-agent/internal/agent"
	"messenger-agent/internal/debounce"
	"messenger-agent/internal/store"
	"messenger-agent/internal/webhook"
)

type e2eMessengerRecorder struct {
	mu      sync.Mutex
	replies []string
	doneCh  chan string
}

func newE2EMessengerRecorder() *e2eMessengerRecorder {
	return &e2eMessengerRecorder{
		doneCh: make(chan string, 4),
	}
}

func (m *e2eMessengerRecorder) SendTyping(_ context.Context, _ string, _ bool) error {
	return nil
}

func (m *e2eMessengerRecorder) SendText(_ context.Context, _ string, text string) error {
	m.mu.Lock()
	m.replies = append(m.replies, text)
	m.mu.Unlock()
	m.doneCh <- text
	return nil
}

type e2eLLM struct {
	mu   sync.Mutex
	step int
}

func (l *e2eLLM) Generate(_ context.Context, req agent.GenerateRequest) (*agent.GenerateResponse, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.step++

	lastMsg := req.Messages[len(req.Messages)-1]

	// Step 1: Customer burst asking about black hoodie XXL and Inside Dhaka delivery charge.
	if lastMsg.Role == "user" && strings.Contains(lastMsg.Content, "black hoodie") {
		return &agent.GenerateResponse{
			ToolCalls: []agent.ToolCall{
				{
					ID:        "call_stock_1",
					Name:      agent.ToolCheckStock,
					Arguments: json.RawMessage(`{"product_sku":"HD-BLK-001","variant":"XXL"}`),
				},
				{
					ID:        "call_deliv_1",
					Name:      agent.ToolCalculateDeliveryFee,
					Arguments: json.RawMessage(`{"district":"Dhaka"}`),
				},
			},
		}, nil
	}

	// Step 2: Synthesize Bangla reply after receiving check_stock and calculate_delivery_fee tool outputs.
	if lastMsg.Role == "tool" && lastMsg.ToolName == agent.ToolCalculateDeliveryFee {
		return &agent.GenerateResponse{
			Text: "জি স্যার/ম্যাডাম, আমাদের এই ব্ল্যাক হুডিটির XXL সাইজ বর্তমানে স্টকে আছে (মূল্য ১২৫০ টাকা)। ঢাকার ভেতরে ডেলিভারি চার্জ ৭০ টাকা (ক্যাশ অন ডেলিভারি)। আপনি কি অর্ডারটি কনফার্ম করতে চান?",
		}, nil
	}

	// Step 3: Customer sends name, phone, address to confirm the order.
	if lastMsg.Role == "user" && strings.Contains(lastMsg.Content, "01712345678") {
		return &agent.GenerateResponse{
			ToolCalls: []agent.ToolCall{
				{
					ID:   "call_order_1",
					Name: agent.ToolCreateOrder,
					Arguments: json.RawMessage(`{
						"customer_name": "রহিম উদ্দিন",
						"phone": "01712345678",
						"address": "বাড়ি ১২, রোড ৫, ধানমন্ডি",
						"district": "Dhaka",
						"items": [{"sku": "HD-BLK-001", "variant": "XXL", "quantity": 1}]
					}`),
				},
			},
		}, nil
	}

	// Step 4: Synthesize Bangla order confirmation after create_order succeeds.
	if lastMsg.Role == "tool" && lastMsg.ToolName == agent.ToolCreateOrder {
		return &agent.GenerateResponse{
			Text: "ধন্যবাদ স্যার! আপনার অর্ডারটি সফলভাবে কনফার্ম করা হয়েছে। মোট বিল ১৩২০ টাকা (ডেলিভারি চার্জ ৭০ টাকা সহ, ক্যাশ অন ডেলিভারি)।",
		}, nil
	}

	return &agent.GenerateResponse{Text: "জি স্যার, বলুন কিভাবে সাহায্য করতে পারি?"}, nil
}

func (l *e2eLLM) EmbedQuery(_ context.Context, _ string) ([]float32, error) {
	return []float32{0.1, 0.2, 0.3}, nil
}

func (l *e2eLLM) TranscribeAudio(_ context.Context, _ []byte, _ string) (string, error) {
	return "", nil
}

type e2ePgHandler struct {
	mu       sync.Mutex
	stockQty int
	paused   map[string]bool
}

func newE2EPgHandler() *e2ePgHandler {
	return &e2ePgHandler{
		stockQty: 6,
		paused:   make(map[string]bool),
	}
}

func (h *e2ePgHandler) Ping(_ context.Context) error { return nil }
func (h *e2ePgHandler) Close()                       {}

func (h *e2ePgHandler) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	upper := strings.ToUpper(sql)
	if strings.Contains(upper, "UPDATE PRODUCTS") {
		qty, _ := args[0].(int)
		h.stockQty -= qty
	}
	if strings.Contains(upper, "INSERT INTO CUSTOMERS") && len(args) == 2 {
		if paused, ok := args[1].(bool); ok {
			h.paused[fmt.Sprint(args[0])] = paused
		}
	}
	return pgconn.NewCommandTag("OK", 1), nil
}

func (h *e2ePgHandler) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	return pgxpool.NewSliceRows(nil), nil
}

func (h *e2ePgHandler) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	h.mu.Lock()
	defer h.mu.Unlock()
	upper := strings.ToUpper(sql)
	if strings.Contains(upper, "INSERT INTO CHAT_MESSAGES") {
		return pgxpool.NewSliceRow(int64(1))
	}
	if strings.Contains(upper, "FROM PRODUCTS") && strings.Contains(upper, "FOR UPDATE") {
		return pgxpool.NewSliceRow(
			"uuid-hd-1",
			"HD-BLK-001",
			"Premium Black Fleece Hoodie",
			"প্রিমিয়াম ব্ল্যাক ফ্লিস হুডি",
			1250.0,
			h.stockQty,
			json.RawMessage(`{"sizes":{"XXL":6}}`),
		)
	}
	if strings.Contains(upper, "FROM PRODUCTS") {
		return pgxpool.NewSliceRow(
			"HD-BLK-001",
			"Premium Black Fleece Hoodie",
			"প্রিমিয়াম ব্ল্যাক ফ্লিস হুডি",
			1250.0,
			h.stockQty,
			json.RawMessage(`{"sizes":{"XXL":6}}`),
		)
	}
	if strings.Contains(upper, "INSERT INTO ORDERS") {
		return pgxpool.NewSliceRow("uuid-order-999", "pending", time.Now().UTC())
	}
	return pgxpool.NewErrorRow(pgx.ErrNoRows)
}

func (h *e2ePgHandler) Begin(ctx context.Context) (pgx.Tx, error) {
	return h.BeginTx(ctx, pgx.TxOptions{})
}

func (h *e2ePgHandler) BeginTx(_ context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
	return &e2eTx{h: h}, nil
}

type e2eTx struct {
	h *e2ePgHandler
}

func (t *e2eTx) Begin(_ context.Context) (pgx.Tx, error)  { return t, nil }
func (t *e2eTx) Commit(_ context.Context) error           { return nil }
func (t *e2eTx) Rollback(_ context.Context) error         { return nil }
func (t *e2eTx) Exec(ctx context.Context, s string, a ...any) (pgconn.CommandTag, error) {
	return t.h.Exec(ctx, s, a...)
}
func (t *e2eTx) Query(ctx context.Context, s string, a ...any) (pgx.Rows, error) {
	return t.h.Query(ctx, s, a...)
}
func (t *e2eTx) QueryRow(ctx context.Context, s string, a ...any) pgx.Row {
	return t.h.QueryRow(ctx, s, a...)
}

func signWebhookBody(secret string, body []byte) string {
	return webhook.ComputeSignature(body, []byte(secret))
}

func TestEndToEnd_BanglishBurstToBanglaOrderAndAdminControls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const (
		verifyToken = "verify-token-e2e"
		appSecret   = "app-secret-e2e"
		adminKey    = "admin-key-e2e"
	)

	pgHandler := newE2EPgHandler()
	pgPool := pgxpool.NewWithHandler(pgHandler)
	defer pgPool.Close()
	pgStore := store.NewPostgresStore(pgPool, nil)

	redisClient := redis.NewInMemoryClient()
	redisStore := store.NewRedisStore(redisClient, nil)

	msgRecorder := newE2EMessengerRecorder()
	llm := &e2eLLM{}

	orch := agent.NewOrchestrator(pgStore, redisStore, msgRecorder, nil, llm, nil)
	debouncer := debounce.NewDebouncer(redisClient, 100*time.Millisecond, 8, orch.HandleBurst, nil)
	defer func() { _ = debouncer.Shutdown(ctx) }()

	whHandler := webhook.NewHandler(verifyToken, appSecret, debouncer, redisStore, nil)
	mux := NewServerMux(whHandler, pgStore, redisStore, adminKey, nil)

	// 1. Verify GET /webhook challenge
	challengeReq := httptest.NewRequest(
		http.MethodGet,
		"/webhook?hub.mode=subscribe&hub.verify_token="+verifyToken+"&hub.challenge=123456789",
		nil,
	)
	challengeRec := httptest.NewRecorder()
	mux.ServeHTTP(challengeRec, challengeReq)
	if challengeRec.Code != http.StatusOK || challengeRec.Body.String() != "123456789" {
		t.Fatalf("GET /webhook failed: code=%d body=%q", challengeRec.Code, challengeRec.Body.String())
	}

	// 2. Simulate rapid 3-message Banglish burst from .agent/workflows/test-webhook.md
	burstMsgs := []struct {
		mid  string
		text string
	}{
		{"mid.test.1", "vai"},
		{"mid.test.2", "ei black hoodie ta ache?"},
		{"mid.test.3", "dhakar vitore delivery charge koto?"},
	}

	for _, bm := range burstMsgs {
		payload := fmt.Sprintf(`{
			"object":"page",
			"entry":[{
				"id":"PAGE_123",
				"time":1700000000000,
				"messaging":[{
					"sender":{"id":"USER_TEST_001"},
					"recipient":{"id":"PAGE_123"},
					"timestamp":1700000000001,
					"message":{"mid":%q,"text":%q}
				}]
			}]
		}`, bm.mid, bm.text)
		body := []byte(payload)
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
		req.Header.Set(webhook.SignatureHeader, signWebhookBody(appSecret, body))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("POST /webhook (%s) returned %d", bm.mid, rec.Code)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Wait for single debounced turn reply
	select {
	case reply1 := <-msgRecorder.doneCh:
		if !strings.Contains(reply1, "৭০ টাকা") || !strings.Contains(reply1, "XXL") {
			t.Errorf("unexpected Bangla reply 1: %q", reply1)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for first Bangla reply")
	}

	// 3. Send order confirmation details and verify stock decrements from 6 to 5
	orderBody := []byte(`{
		"object":"page",
		"entry":[{
			"id":"PAGE_123",
			"time":1700000001000,
			"messaging":[{
				"sender":{"id":"USER_TEST_001"},
				"recipient":{"id":"PAGE_123"},
				"timestamp":1700000001001,
				"message":{"mid":"mid.test.4","text":"নাম রহিম উদ্দিন, ফোন 01712345678, ধানমন্ডি ঢাকা, ১টা XXL ব্ল্যাক হুডি অর্ডার কনফার্ম করেন"}
			}]
		}]
	}`)
	reqOrder := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(orderBody))
	reqOrder.Header.Set(webhook.SignatureHeader, signWebhookBody(appSecret, orderBody))
	recOrder := httptest.NewRecorder()
	mux.ServeHTTP(recOrder, reqOrder)
	if recOrder.Code != http.StatusOK {
		t.Fatalf("POST /webhook order returned %d", recOrder.Code)
	}

	select {
	case reply2 := <-msgRecorder.doneCh:
		if !strings.Contains(reply2, "১৩২০ টাকা") {
			t.Errorf("unexpected Bangla order confirmation reply: %q", reply2)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for Bangla order confirmation reply")
	}

	pgHandler.mu.Lock()
	remainingStock := pgHandler.stockQty
	pgHandler.mu.Unlock()
	if remainingStock != 5 {
		t.Errorf("expected product stock to decrement from 6 to 5, got %d", remainingStock)
	}

	// 4. Test protected Admin pause & resume endpoints
	unauthReq := httptest.NewRequest(http.MethodPost, "/admin/threads/USER_TEST_001/pause", nil)
	unauthRec := httptest.NewRecorder()
	mux.ServeHTTP(unauthRec, unauthReq)
	if unauthRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing ADMIN_API_KEY, got %d", unauthRec.Code)
	}

	pauseReq := httptest.NewRequest(http.MethodPost, "/admin/threads/USER_TEST_001/pause", nil)
	pauseReq.Header.Set("Authorization", "Bearer "+adminKey)
	pauseRec := httptest.NewRecorder()
	mux.ServeHTTP(pauseRec, pauseReq)
	if pauseRec.Code != http.StatusOK {
		t.Fatalf("expected 200 on admin pause, got %d", pauseRec.Code)
	}

	isPaused, _ := redisStore.IsBotPaused(ctx, "USER_TEST_001")
	if !isPaused {
		t.Errorf("expected thread to be paused in Redis after POST /admin/threads/USER_TEST_001/pause")
	}

	resumeReq := httptest.NewRequest(http.MethodPost, "/admin/threads/USER_TEST_001/resume", nil)
	resumeReq.Header.Set("X-Admin-API-Key", adminKey)
	resumeRec := httptest.NewRecorder()
	mux.ServeHTTP(resumeRec, resumeReq)
	if resumeRec.Code != http.StatusOK {
		t.Fatalf("expected 200 on admin resume, got %d", resumeRec.Code)
	}

	isPausedAfter, _ := redisStore.IsBotPaused(ctx, "USER_TEST_001")
	if isPausedAfter {
		t.Errorf("expected thread to be resumed in Redis after POST /admin/threads/USER_TEST_001/resume")
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestWorkflow_VerifyWebhookAndRedisDebouncer executes the verification criteria defined in
// .agent/workflows/test-webhook.md:
// 1. GET /webhook challenge verification returns exact challenge string "123456789".
// 2. All 3 rapid Banglish POST /webhook requests return 200 OK in < 50ms.
// 3. Only ONE "processing debounced turn" JSON log entry fires with msg_count: 3.
// 4. The agent responds in natural Bangladeshi Bangla (চলিত ভাষা).
func TestWorkflow_VerifyWebhookAndRedisDebouncer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const (
		verifyToken = "workflow-verify-token"
		appSecret   = "workflow-app-secret"
	)

	var logBuf syncBuffer
	jsonLogger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	pgHandler := newE2EPgHandler()
	pgPool := pgxpool.NewWithHandler(pgHandler)
	defer pgPool.Close()
	pgStore := store.NewPostgresStore(pgPool, jsonLogger)

	redisClient := redis.NewInMemoryClient()
	redisStore := store.NewRedisStore(redisClient, jsonLogger)

	msgRecorder := newE2EMessengerRecorder()
	llm := &e2eLLM{}

	orch := agent.NewOrchestrator(pgStore, redisStore, msgRecorder, nil, llm, jsonLogger)
	debouncer := debounce.NewDebouncer(redisClient, 150*time.Millisecond, 8, orch.HandleBurst, jsonLogger)
	defer func() { _ = debouncer.Shutdown(ctx) }()

	whHandler := webhook.NewHandler(verifyToken, appSecret, debouncer, redisStore, jsonLogger)
	mux := NewServerMux(whHandler, pgStore, redisStore, "admin-key", jsonLogger)

	// Step 2 of workflow: Verify challenge endpoint returns exact challenge string
	challengeReq := httptest.NewRequest(
		http.MethodGet,
		"/webhook?hub.mode=subscribe&hub.verify_token="+verifyToken+"&hub.challenge=123456789",
		nil,
	)
	challengeRec := httptest.NewRecorder()
	mux.ServeHTTP(challengeRec, challengeReq)
	if challengeRec.Code != http.StatusOK || challengeRec.Body.String() != "123456789" {
		t.Fatalf("expected 200 OK with challenge 123456789, got status=%d body=%q",
			challengeRec.Code, challengeRec.Body.String())
	}

	// Step 4 of workflow: Simulate rapid 3-message Banglish burst
	msgs := []struct {
		mid  string
		text string
	}{
		{"mid.test.1", "vai"},
		{"mid.test.2", "ei black hoodie ta ache?"},
		{"mid.test.3", "dhakar vitore delivery charge koto?"},
	}

	for _, m := range msgs {
		body := []byte(fmt.Sprintf(`{
			"object": "page",
			"entry": [{
				"id": "PAGE_123",
				"time": 1700000000000,
				"messaging": [{
					"sender": {"id": "USER_TEST_001"},
					"recipient": {"id": "PAGE_123"},
					"timestamp": 1700000000001,
					"message": {"mid": %q, "text": %q}
				}]
			}]
		}`, m.mid, m.text))

		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(webhook.SignatureHeader, signWebhookBody(appSecret, body))
		rec := httptest.NewRecorder()

		start := time.Now()
		mux.ServeHTTP(rec, req)
		elapsed := time.Since(start)

		if rec.Code != http.StatusOK {
			t.Fatalf("%s returned status %d, want 200 OK", m.mid, rec.Code)
		}
		if elapsed >= 50*time.Millisecond {
			t.Errorf("%s took %v, expected < 50ms", m.mid, elapsed)
		}
		time.Sleep(30 * time.Millisecond)
	}

	// Step 5 of workflow: Confirm natural Bangladeshi Bangla reply and single debounced log entry
	var banglaReply string
	select {
	case banglaReply = <-msgRecorder.doneCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for debounced Bangla reply")
	}

	// Wait briefly to ensure no extra timer flushes occur
	time.Sleep(200 * time.Millisecond)

	logLines := strings.Split(strings.TrimSpace(logBuf.String()), "\n")
	debouncedTurnCount := 0
	for _, line := range logLines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("invalid JSON log line %q: %v", line, err)
		}
		if entry["msg"] == "processing debounced turn" {
			debouncedTurnCount++
			if count, ok := entry["msg_count"].(float64); !ok || int(count) != 3 {
				t.Errorf("expected msg_count=3 in JSON log, got %v", entry["msg_count"])
			}
		}
	}

	if debouncedTurnCount != 1 {
		t.Fatalf("expected exactly 1 'processing debounced turn' log entry, got %d\nLogs:\n%s",
			debouncedTurnCount, logBuf.String())
	}

	if !strings.Contains(banglaReply, "আপনি") || !strings.Contains(banglaReply, "৭০ টাকা") {
		t.Errorf("expected natural Bangladeshi Bangla reply in আপনি register, got %q", banglaReply)
	}
}

