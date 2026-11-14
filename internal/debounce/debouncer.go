package debounce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// DedupKeyPrefix is the Redis key prefix for webhook message deduplication (TTL: 24h).
	DedupKeyPrefix = "msg:dedup:"
	// SeqKeyPrefix is the Redis key prefix for monotonic per-sender burst sequence numbers (TTL: 2m).
	SeqKeyPrefix = "debounce:seq:"
	// MsgsKeyPrefix is the Redis key prefix for per-sender buffered JSON InboundMessages (TTL: 2m).
	MsgsKeyPrefix = "debounce:msgs:"

	// DedupTTL is the 24-hour TTL for msg:dedup:{mid}.
	DedupTTL = 24 * time.Hour
	// DebounceKeyTTLSeconds is the 2-minute TTL (120s) for debounce:seq:{sender_id} and debounce:msgs:{sender_id}.
	DebounceKeyTTLSeconds = 120
)

// pushScript atomically increments the sender's sequence counter, appends the JSON message to the list,
// refreshes the 2-minute TTL on both keys, and returns {seq, list_len}.
var pushScript = redis.NewScript(`
local seq = redis.call("INCR", KEYS[1])
redis.call("EXPIRE", KEYS[1], tonumber(ARGV[2]))
local count = redis.call("RPUSH", KEYS[2], ARGV[1])
redis.call("EXPIRE", KEYS[2], tonumber(ARGV[2]))
return {seq, count}
`)

// flushScript atomically checks whether the current sequence number matches ARGV[1] (or ARGV[1] == "0" for forced flush),
// fetches all buffered messages, and deletes both debounce keys so no other worker can double-process the burst.
var flushScript = redis.NewScript(`
local current = redis.call("GET", KEYS[1])
if ARGV[1] ~= "0" then
    if not current or tostring(current) ~= tostring(ARGV[1]) then
        return {}
    end
end
local msgs = redis.call("LRANGE", KEYS[2], 0, -1)
redis.call("DEL", KEYS[1], KEYS[2])
return msgs
`)

// InboundMessage represents a normalized customer message buffered inside a debounce burst window.
type InboundMessage struct {
	MID         string   `json:"mid"`
	SenderID    string   `json:"sender_id"`
	RecipientID string   `json:"recipient_id,omitempty"`
	Text        string   `json:"text,omitempty"`
	ImageURLs   []string `json:"image_urls,omitempty"`
	AudioURLs   []string `json:"audio_urls,omitempty"`
	Timestamp   int64    `json:"timestamp"`
}

// FlushFunc is the callback invoked asynchronously when a sender's message burst flushes.
type FlushFunc func(ctx context.Context, senderID string, msgs []InboundMessage)

type pendingTimer struct {
	seq   int64
	timer *time.Timer
}

// Debouncer aggregates rapid customer message bursts in Redis using atomic Lua scripts.
type Debouncer struct {
	client   *redis.Client
	window   time.Duration
	maxBatch int
	onFlush  FlushFunc
	logger   *slog.Logger

	mu      sync.Mutex
	timers  map[string]*pendingTimer
	closed  bool
	wg      sync.WaitGroup
	baseCtx context.Context
	cancel  context.CancelFunc
}

// NewDebouncer creates a new Redis-backed Debouncer.
func NewDebouncer(
	client *redis.Client,
	window time.Duration,
	maxBatch int,
	onFlush FlushFunc,
	logger *slog.Logger,
) *Debouncer {
	if window <= 0 {
		window = 3 * time.Second
	}
	if maxBatch <= 0 {
		maxBatch = 8
	}
	if logger == nil {
		logger = slog.Default()
	}
	baseCtx, cancel := context.WithCancel(context.Background())
	return &Debouncer{
		client:   client,
		window:   window,
		maxBatch: maxBatch,
		onFlush:  onFlush,
		logger:   logger,
		timers:   make(map[string]*pendingTimer),
		baseCtx:  baseCtx,
		cancel:   cancel,
	}
}

// DedupKey returns "msg:dedup:{mid}".
func DedupKey(mid string) string {
	return DedupKeyPrefix + strings.TrimSpace(mid)
}

// SeqKey returns "debounce:seq:{sender_id}".
func SeqKey(senderID string) string {
	return SeqKeyPrefix + strings.TrimSpace(senderID)
}

// MsgsKey returns "debounce:msgs:{sender_id}".
func MsgsKey(senderID string) string {
	return MsgsKeyPrefix + strings.TrimSpace(senderID)
}

// ClaimMessageID atomically sets "msg:dedup:{mid}" -> "1" (TTL: 24h) using SETNX.
// Returns true if this is the first time seeing mid, or false if it is a duplicate.
func (d *Debouncer) ClaimMessageID(ctx context.Context, mid string) (bool, error) {
	mid = strings.TrimSpace(mid)
	if mid == "" {
		return false, errors.New("debounce: claim message id: mid is required")
	}
	key := DedupKey(mid)
	claimed, err := d.client.SetNX(ctx, key, "1", DedupTTL).Result()
	if err != nil {
		return false, fmt.Errorf("debounce: setnx %s: %w", key, err)
	}
	return claimed, nil
}

// Push appends an InboundMessage to the sender's Redis burst buffer and resets the debounce timer.
// If the buffered message count reaches maxBatch, the burst is flushed immediately.
func (d *Debouncer) Push(ctx context.Context, msg InboundMessage) error {
	senderID := strings.TrimSpace(msg.SenderID)
	if senderID == "" {
		return errors.New("debounce: push: sender_id is required")
	}
	msg.SenderID = senderID

	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("debounce: marshal inbound message: %w", err)
	}

	keys := []string{SeqKey(senderID), MsgsKey(senderID)}
	vals, err := pushScript.Run(ctx, d.client, keys, string(payload), DebounceKeyTTLSeconds).Slice()
	if err != nil {
		return fmt.Errorf("debounce: run push lua script for %s: %w", senderID, err)
	}
	if len(vals) < 2 {
		return fmt.Errorf("debounce: unexpected push script return length %d", len(vals))
	}

	seq, err := toInt64(vals[0])
	if err != nil {
		return fmt.Errorf("debounce: parse seq: %w", err)
	}
	count, err := toInt64(vals[1])
	if err != nil {
		return fmt.Errorf("debounce: parse count: %w", err)
	}

	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return errors.New("debounce: debouncer is shut down")
	}

	if existing, ok := d.timers[senderID]; ok {
		existing.timer.Stop()
		delete(d.timers, senderID)
	}

	// If burst reached maxBatch (e.g. 8 messages), flush immediately without waiting for timer.
	if int(count) >= d.maxBatch {
		d.wg.Add(1)
		d.mu.Unlock()
		go func() {
			defer d.wg.Done()
			d.executeFlush(d.baseCtx, senderID, seq, false)
		}()
		return nil
	}

	targetSeq := seq
	timer := time.AfterFunc(d.window, func() {
		d.mu.Lock()
		if d.closed {
			d.mu.Unlock()
			return
		}
		if cur, ok := d.timers[senderID]; ok && cur.seq == targetSeq {
			delete(d.timers, senderID)
		}
		d.wg.Add(1)
		d.mu.Unlock()

		defer d.wg.Done()
		d.executeFlush(d.baseCtx, senderID, targetSeq, false)
	})

	d.timers[senderID] = &pendingTimer{
		seq:   targetSeq,
		timer: timer,
	}
	d.mu.Unlock()
	return nil
}

func (d *Debouncer) executeFlush(ctx context.Context, senderID string, expectedSeq int64, force bool) {
	seqArg := strconv.FormatInt(expectedSeq, 10)
	if force {
		seqArg = "0"
	}

	keys := []string{SeqKey(senderID), MsgsKey(senderID)}
	rawList, err := flushScript.Run(ctx, d.client, keys, seqArg).StringSlice()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return
		}
		d.logger.Error("failed to execute debounce flush script",
			"sender_id", senderID,
			"seq", expectedSeq,
			"err", err,
		)
		return
	}
	if len(rawList) == 0 {
		return
	}

	msgs := make([]InboundMessage, 0, len(rawList))
	for _, raw := range rawList {
		var m InboundMessage
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			d.logger.Warn("skipping malformed debounced message",
				"sender_id", senderID,
				"err", err,
			)
			continue
		}
		msgs = append(msgs, m)
	}
	if len(msgs) == 0 {
		return
	}

	d.logger.Info("processing debounced turn",
		"sender_id", senderID,
		"msg_count", len(msgs),
		"seq", expectedSeq,
	)

	if d.onFlush != nil {
		d.onFlush(ctx, senderID, msgs)
	}
}

// Shutdown stops all pending timers, flushes any buffered messages in Redis immediately,
// and waits for all in-flight flush handlers to finish or ctx to expire.
func (d *Debouncer) Shutdown(ctx context.Context) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true

	pendingSenders := make([]string, 0, len(d.timers))
	for senderID, pt := range d.timers {
		pt.timer.Stop()
		pendingSenders = append(pendingSenders, senderID)
		delete(d.timers, senderID)
	}
	d.mu.Unlock()

	for _, senderID := range pendingSenders {
		d.wg.Add(1)
		go func(sid string) {
			defer d.wg.Done()
			d.executeFlush(ctx, sid, 0, true)
		}(senderID)
	}

	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		d.cancel()
		return nil
	case <-ctx.Done():
		d.cancel()
		return fmt.Errorf("debounce: shutdown timed out: %w", ctx.Err())
	}
}

func toInt64(v any) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	case string:
		return strconv.ParseInt(val, 10, 64)
	default:
		return 0, fmt.Errorf("unexpected numeric type %T", v)
	}
}
