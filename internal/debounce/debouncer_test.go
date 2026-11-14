package debounce

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestDebouncer_ClaimMessageID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client := redis.NewInMemoryClient()
	d := NewDebouncer(client, 100*time.Millisecond, 8, nil, nil)
	defer func() { _ = d.Shutdown(ctx) }()

	claimed1, err := d.ClaimMessageID(ctx, "mid.1001")
	if err != nil {
		t.Fatalf("first ClaimMessageID error: %v", err)
	}
	if !claimed1 {
		t.Fatalf("expected first ClaimMessageID to return true")
	}

	claimed2, err := d.ClaimMessageID(ctx, "mid.1001")
	if err != nil {
		t.Fatalf("second ClaimMessageID error: %v", err)
	}
	if claimed2 {
		t.Fatalf("expected duplicate ClaimMessageID to return false")
	}
}

func TestDebouncer_BurstAggregation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		window          time.Duration
		maxBatch        int
		messages        []InboundMessage
		interMsgDelay   time.Duration
		wantFlushCount  int
		wantFirstBatch  []string
	}{
		{
			name:          "3-message Banglish burst flushes once after final timer",
			window:        120 * time.Millisecond,
			maxBatch:      8,
			interMsgDelay: 35 * time.Millisecond,
			messages: []InboundMessage{
				{MID: "mid.1", SenderID: "USER_001", Text: "vai"},
				{MID: "mid.2", SenderID: "USER_001", Text: "ei black hoodie ta ache?"},
				{MID: "mid.3", SenderID: "USER_001", Text: "dhakar vitore delivery charge koto?"},
			},
			wantFlushCount: 1,
			wantFirstBatch: []string{
				"vai",
				"ei black hoodie ta ache?",
				"dhakar vitore delivery charge koto?",
			},
		},
		{
			name:          "burst reaching maxBatch flushes immediately",
			window:        500 * time.Millisecond,
			maxBatch:      3,
			interMsgDelay: 5 * time.Millisecond,
			messages: []InboundMessage{
				{MID: "mid.a", SenderID: "USER_002", Text: "hello"},
				{MID: "mid.b", SenderID: "USER_002", Text: "panjabi ache?"},
				{MID: "mid.c", SenderID: "USER_002", Text: "size 42 lagbe"},
			},
			wantFlushCount: 1,
			wantFirstBatch: []string{
				"hello",
				"panjabi ache?",
				"size 42 lagbe",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			client := redis.NewInMemoryClient()

			var (
				mu      sync.Mutex
				flushes [][]InboundMessage
				flushed = make(chan struct{}, 4)
			)

			d := NewDebouncer(client, tc.window, tc.maxBatch, func(_ context.Context, _ string, msgs []InboundMessage) {
				mu.Lock()
				cp := make([]InboundMessage, len(msgs))
				copy(cp, msgs)
				flushes = append(flushes, cp)
				mu.Unlock()
				flushed <- struct{}{}
			}, nil)
			defer func() { _ = d.Shutdown(ctx) }()

			for i, m := range tc.messages {
				if err := d.Push(ctx, m); err != nil {
					t.Fatalf("Push(%d) error: %v", i, err)
				}
				if i < len(tc.messages)-1 && tc.interMsgDelay > 0 {
					time.Sleep(tc.interMsgDelay)
				}
			}

			select {
			case <-flushed:
			case <-time.After(2 * time.Second):
				t.Fatalf("timed out waiting for debounce flush")
			}

			// Wait slightly longer than window to verify no duplicate timer flushes occur.
			time.Sleep(tc.window + 40*time.Millisecond)

			mu.Lock()
			defer mu.Unlock()
			if len(flushes) != tc.wantFlushCount {
				t.Fatalf("flush count = %d, want %d", len(flushes), tc.wantFlushCount)
			}
			gotBatch := flushes[0]
			if len(gotBatch) != len(tc.wantFirstBatch) {
				t.Fatalf("len(batch) = %d, want %d", len(gotBatch), len(tc.wantFirstBatch))
			}
			for i, wantText := range tc.wantFirstBatch {
				if gotBatch[i].Text != wantText {
					t.Errorf("batch[%d].Text = %q, want %q", i, gotBatch[i].Text, wantText)
				}
			}
		})
	}
}

func TestDebouncer_GracefulShutdownFlushesPending(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := redis.NewInMemoryClient()

	var (
		mu      sync.Mutex
		flushed []InboundMessage
	)

	// Long 10s window so only Shutdown triggers the flush.
	d := NewDebouncer(client, 10*time.Second, 8, func(_ context.Context, _ string, msgs []InboundMessage) {
		mu.Lock()
		flushed = append(flushed, msgs...)
		mu.Unlock()
	}, nil)

	if err := d.Push(ctx, InboundMessage{MID: "mid.s1", SenderID: "USER_SHUTDOWN", Text: "vai order korbo"}); err != nil {
		t.Fatalf("Push error: %v", err)
	}

	if err := d.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(flushed) != 1 || flushed[0].Text != "vai order korbo" {
		t.Fatalf("expected pending message to flush on Shutdown, got %+v", flushed)
	}
}
