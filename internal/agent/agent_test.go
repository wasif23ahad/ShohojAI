package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"messenger-agent/internal/debounce"
	"messenger-agent/internal/store"
)

func TestNormalizeBanglishQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		input        string
		wantContains []string
		wantExcluded []string
	}{
		{
			name:         "Banglish hoodie query expands to English and Bangla keywords",
			input:        "vai ei kalo hudi ta XXL ache? dam koto?",
			wantContains: []string{"black", "কালো", "hoodie", "হুডি", "XXL"},
			wantExcluded: []string{"vai", "ache", "koto", "dam"},
		},
		{
			name:         "Banglish panjabi query expands to panjabi পাঞ্জাবি",
			input:        "bhai shada punjabi 42 size hobe?",
			wantContains: []string{"white", "সাদা", "panjabi", "পাঞ্জাবি", "42"},
			wantExcluded: []string{"bhai", "hobe"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := NormalizeBanglishQuery(tc.input)
			for _, w := range tc.wantContains {
				if !strings.Contains(got, w) {
					t.Errorf("NormalizeBanglishQuery(%q) = %q, missing %q", tc.input, got, w)
				}
			}
			for _, ex := range tc.wantExcluded {
				for _, tok := range strings.Fields(got) {
					if strings.EqualFold(tok, ex) {
						t.Errorf("NormalizeBanglishQuery(%q) = %q, should have stripped stopword %q", tc.input, got, ex)
					}
				}
			}
		})
	}
}

type fakeStore struct {
	mu        sync.Mutex
	paused    map[string]bool
	history   map[string][]store.ChatMessage
	stockQty  int
	orders    []store.OrderConfirmation
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		paused:   make(map[string]bool),
		history:  make(map[string][]store.ChatMessage),
		stockQty: 5,
	}
}

func (f *fakeStore) SearchProductsHybrid(_ context.Context, _ string, _ string, _ []float32, _ int) ([]store.Product, error) {
	return []store.Product{
		{
			ID:          "prod-1",
			SKU:         "HD-BLK-001",
			NameEN:      "Premium Black Fleece Hoodie",
			NameBN:      "প্রিমিয়াম ব্ল্যাক ফ্লিস হুডি",
			PriceBDT:    1250,
			StockQty:    f.stockQty,
			Variants:    json.RawMessage(`{"sizes":{"XXL":3}}`),
		},
	}, nil
}

func (f *fakeStore) CheckStock(_ context.Context, sku string, variant string) (*store.StockStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !strings.EqualFold(sku, "HD-BLK-001") {
		return nil, store.ErrProductNotFound
	}
	return &store.StockStatus{
		SKU:          "HD-BLK-001",
		NameEN:       "Premium Black Fleece Hoodie",
		NameBN:       "প্রিমিয়াম ব্ল্যাক ফ্লিস হুডি",
		Variant:      variant,
		PriceBDT:     1250,
		AvailableQty: f.stockQty,
		InStock:      f.stockQty > 0,
	}, nil
}

func (f *fakeStore) CreateOrderTx(_ context.Context, req store.CreateOrderRequest) (*store.OrderConfirmation, error) {
	phone, err := store.ValidateBDPhone(req.Phone)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stockQty < req.Items[0].Quantity {
		return nil, store.ErrInsufficientStock
	}
	f.stockQty -= req.Items[0].Quantity
	quote := store.CalculateDeliveryFee(req.District)
	subtotal := 1250.0 * float64(req.Items[0].Quantity)
	conf := store.OrderConfirmation{
		OrderID:        "ord-1001",
		OrderNumber:    "ORD-20261114-ABC123",
		SenderID:       req.SenderID,
		CustomerName:   req.CustomerName,
		Phone:          phone,
		Address:        req.Address,
		District:       req.District,
		Zone:           quote.Zone,
		SubtotalBDT:    subtotal,
		DeliveryFeeBDT: quote.DeliveryFeeBDT,
		TotalBDT:       subtotal + quote.DeliveryFeeBDT,
		Status:         "pending",
		CreatedAt:      time.Now().UTC(),
	}
	f.orders = append(f.orders, conf)
	return &conf, nil
}

func (f *fakeStore) SetCustomerBotPaused(_ context.Context, senderID string, paused bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.paused[senderID] = paused
	return nil
}

func (f *fakeStore) SaveMessage(_ context.Context, msg store.ChatMessage) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.history[msg.SenderID] = append(f.history[msg.SenderID], msg)
	return int64(len(f.history[msg.SenderID])), nil
}

func (f *fakeStore) GetRecentHistory(_ context.Context, senderID string, _ int) ([]store.ChatMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.history[senderID], nil
}

func (f *fakeStore) SetBotPaused(_ context.Context, senderID string, paused bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.paused[senderID] = paused
	return nil
}

func (f *fakeStore) IsBotPaused(_ context.Context, senderID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.paused[senderID], nil
}

func (f *fakeStore) GetCachedChatHistory(_ context.Context, senderID string) ([]store.ChatMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.history[senderID], nil
}

func (f *fakeStore) AppendChatHistory(_ context.Context, _ string, _ ...store.ChatMessage) error {
	return nil
}

type fakeMessenger struct {
	mu      sync.Mutex
	typings []bool
	replies []string
}

func (m *fakeMessenger) SendTyping(_ context.Context, _ string, on bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.typings = append(m.typings, on)
	return nil
}

func (m *fakeMessenger) SendText(_ context.Context, _ string, text string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replies = append(m.replies, text)
	return nil
}

type scriptedLLM struct {
	mu        sync.Mutex
	responses []*GenerateResponse
	err       error
	idx       int
}

func (l *scriptedLLM) Generate(_ context.Context, _ GenerateRequest) (*GenerateResponse, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return nil, l.err
	}
	if l.idx >= len(l.responses) {
		return &GenerateResponse{Text: "জি স্যার, ধন্যবাদ।"}, nil
	}
	r := l.responses[l.idx]
	l.idx++
	return r, nil
}

func (l *scriptedLLM) EmbedQuery(_ context.Context, _ string) ([]float32, error) {
	return []float32{0.1, 0.2, 0.3}, nil
}

func (l *scriptedLLM) TranscribeAudio(_ context.Context, _ []byte, _ string) (string, error) {
	return "", nil
}

func TestOrchestrator_HandleBurst_MultiToolLoopAndBanglaReply(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	st := newFakeStore()
	msgClient := &fakeMessenger{}
	llm := &scriptedLLM{
		responses: []*GenerateResponse{
			{
				ToolCalls: []ToolCall{
					{
						ID:        "call_1",
						Name:      ToolCheckStock,
						Arguments: json.RawMessage(`{"product_sku":"HD-BLK-001","variant":"XXL"}`),
					},
					{
						ID:        "call_2",
						Name:      ToolCalculateDeliveryFee,
						Arguments: json.RawMessage(`{"district":"Dhaka"}`),
					},
				},
			},
			{
				Text: "জি স্যার/ম্যাডাম, আমাদের এই ব্ল্যাক হুডিটির XXL সাইজ বর্তমানে স্টকে আছে (মূল্য ১২৫০ টাকা)। ঢাকার ভেতরে ডেলিভারি চার্জ ৭০ টাকা (ক্যাশ অন ডেলিভারি)। আপনি কি অর্ডারটি কনফার্ম করতে চান?",
			},
		},
	}

	orch := NewOrchestrator(st, st, msgClient, nil, llm, nil)

	orch.HandleBurst(ctx, "USER_TEST_001", []debounce.InboundMessage{
		{MID: "mid.1", SenderID: "USER_TEST_001", Text: "vai"},
		{MID: "mid.2", SenderID: "USER_TEST_001", Text: "ei black hoodie ta ache?"},
		{MID: "mid.3", SenderID: "USER_TEST_001", Text: "dhakar vitore delivery charge koto?"},
	})

	msgClient.mu.Lock()
	defer msgClient.mu.Unlock()
	if len(msgClient.typings) != 1 || !msgClient.typings[0] {
		t.Errorf("expected typing_on to be sent once, got %v", msgClient.typings)
	}
	if len(msgClient.replies) != 1 {
		t.Fatalf("expected 1 outbound reply, got %d", len(msgClient.replies))
	}
	if !strings.Contains(msgClient.replies[0], "৭০ টাকা") {
		t.Errorf("expected Bangla reply mentioning ৭০ টাকা, got %q", msgClient.replies[0])
	}
}

func TestOrchestrator_HandleBurst_EscalationPausesThread(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	st := newFakeStore()
	msgClient := &fakeMessenger{}
	llm := &scriptedLLM{
		responses: []*GenerateResponse{
			{
				ToolCalls: []ToolCall{
					{
						ID:        "call_esc",
						Name:      ToolEscalateToHuman,
						Arguments: json.RawMessage(`{"reason":"customer requested human agent"}`),
					},
				},
			},
			{
				Text: EscalationConfirmationBangla,
			},
		},
	}

	orch := NewOrchestrator(st, st, msgClient, nil, llm, nil)
	orch.HandleBurst(ctx, "USER_ESC_01", []debounce.InboundMessage{
		{MID: "mid.e1", SenderID: "USER_ESC_01", Text: "ami manush er sathe kotha bolte chai"},
	})

	paused, _ := st.IsBotPaused(ctx, "USER_ESC_01")
	if !paused {
		t.Fatalf("expected USER_ESC_01 thread to be paused after escalate_to_human")
	}
}

func TestOrchestrator_HandleBurst_FallbackOnLLMError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	st := newFakeStore()
	msgClient := &fakeMessenger{}
	llm := &scriptedLLM{err: errors.New("upstream quota exceeded")}

	orch := NewOrchestrator(st, st, msgClient, nil, llm, nil)
	orch.HandleBurst(ctx, "USER_ERR_01", []debounce.InboundMessage{
		{MID: "mid.err1", SenderID: "USER_ERR_01", Text: "vai price koto?"},
	})

	msgClient.mu.Lock()
	defer msgClient.mu.Unlock()
	if len(msgClient.replies) != 1 || msgClient.replies[0] != FallbackBanglaReply {
		t.Fatalf("expected FallbackBanglaReply, got %v", msgClient.replies)
	}
}
