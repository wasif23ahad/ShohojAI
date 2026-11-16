package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"messenger-agent/internal/config"
)

const (
	// BotPausedKeyPrefix is the Redis key prefix for human-handoff thread pause flags (TTL: 24h).
	BotPausedKeyPrefix = "bot:paused:"
	// ChatHistoryKeyPrefix is the Redis key prefix for cached sliding-window conversation turns (TTL: 12h).
	ChatHistoryKeyPrefix = "chat:history:"

	// BotPausedTTL is the 24-hour TTL for bot:paused:{sender_id}.
	BotPausedTTL = 24 * time.Hour
	// ChatHistoryTTL is the 12-hour TTL for chat:history:{sender_id}.
	ChatHistoryTTL = 12 * time.Hour
	// MaxCachedHistoryTurns is the sliding window size (16 turns) for chat:history:{sender_id}.
	MaxCachedHistoryTurns = 16
)

// BotPausedKey returns "bot:paused:{sender_id}".
func BotPausedKey(senderID string) string {
	return BotPausedKeyPrefix + strings.TrimSpace(senderID)
}

// ChatHistoryKey returns "chat:history:{sender_id}".
func ChatHistoryKey(senderID string) string {
	return ChatHistoryKeyPrefix + strings.TrimSpace(senderID)
}

// NewRedisClient creates and pings a *redis.Client from Config.
func NewRedisClient(ctx context.Context, cfg *config.Config) (*redis.Client, error) {
	if cfg == nil {
		return nil, errors.New("store: config is nil")
	}
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("store: ping redis %s: %w", cfg.RedisAddr, err)
	}
	return client, nil
}

// RedisStore manages thread pause flags and sliding-window conversation history in Redis.
type RedisStore struct {
	client *redis.Client
	logger *slog.Logger
}

// NewRedisStore constructs a RedisStore with explicit dependency injection.
func NewRedisStore(client *redis.Client, logger *slog.Logger) *RedisStore {
	if logger == nil {
		logger = slog.Default()
	}
	return &RedisStore{
		client: client,
		logger: logger,
	}
}

// Client returns the underlying *redis.Client.
func (r *RedisStore) Client() *redis.Client {
	return r.client
}

// SetBotPaused sets or clears "bot:paused:{sender_id}" -> "1" (TTL: 24h).
func (r *RedisStore) SetBotPaused(ctx context.Context, senderID string, paused bool) error {
	senderID = strings.TrimSpace(senderID)
	if senderID == "" {
		return errors.New("store: set bot paused: sender_id is required")
	}

	key := BotPausedKey(senderID)
	if paused {
		if err := r.client.Set(ctx, key, "1", BotPausedTTL).Err(); err != nil {
			return fmt.Errorf("store: set %s: %w", key, err)
		}
		return nil
	}

	if err := r.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("store: del %s: %w", key, err)
	}
	return nil
}

// IsBotPaused checks whether "bot:paused:{sender_id}" is active in Redis.
func (r *RedisStore) IsBotPaused(ctx context.Context, senderID string) (bool, error) {
	senderID = strings.TrimSpace(senderID)
	if senderID == "" {
		return false, errors.New("store: is bot paused: sender_id is required")
	}

	key := BotPausedKey(senderID)
	val, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, fmt.Errorf("store: get %s: %w", key, err)
	}
	return val == "1", nil
}

// AppendChatHistory appends one or more conversation turns to "chat:history:{sender_id}",
// trims the list to the last 16 turns, and refreshes the 12-hour TTL.
func (r *RedisStore) AppendChatHistory(ctx context.Context, senderID string, msgs ...ChatMessage) error {
	senderID = strings.TrimSpace(senderID)
	if senderID == "" {
		return errors.New("store: append chat history: sender_id is required")
	}
	if len(msgs) == 0 {
		return nil
	}

	payloads := make([]any, 0, len(msgs))
	now := time.Now().UTC()
	for _, m := range msgs {
		if m.SenderID == "" {
			m.SenderID = senderID
		}
		if m.CreatedAt.IsZero() {
			m.CreatedAt = now
		}
		if len(m.Metadata) == 0 {
			m.Metadata = json.RawMessage(`{}`)
		}
		b, err := json.Marshal(m)
		if err != nil {
			return fmt.Errorf("store: marshal chat history turn: %w", err)
		}
		payloads = append(payloads, string(b))
	}

	key := ChatHistoryKey(senderID)
	pipe := r.client.TxPipeline()
	pipe.RPush(ctx, key, payloads...)
	pipe.LTrim(ctx, key, -MaxCachedHistoryTurns, -1)
	pipe.Expire(ctx, key, ChatHistoryTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("store: append chat history %s: %w", key, err)
	}
	return nil
}

// GetCachedChatHistory reads the cached conversation turns (up to 16) from "chat:history:{sender_id}"
// in chronological order (oldest first).
func (r *RedisStore) GetCachedChatHistory(ctx context.Context, senderID string) ([]ChatMessage, error) {
	senderID = strings.TrimSpace(senderID)
	if senderID == "" {
		return nil, errors.New("store: get cached chat history: sender_id is required")
	}

	key := ChatHistoryKey(senderID)
	rawItems, err := r.client.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("store: lrange %s: %w", key, err)
	}
	if len(rawItems) == 0 {
		return nil, nil
	}

	turns := make([]ChatMessage, 0, len(rawItems))
	for _, raw := range rawItems {
		var m ChatMessage
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			r.logger.Warn("skipping malformed cached chat message", "key", key, "err", err)
			continue
		}
		turns = append(turns, m)
	}
	return turns, nil
}

// ClearChatHistory removes the cached conversation history for senderID.
func (r *RedisStore) ClearChatHistory(ctx context.Context, senderID string) error {
	senderID = strings.TrimSpace(senderID)
	if senderID == "" {
		return errors.New("store: clear chat history: sender_id is required")
	}
	key := ChatHistoryKey(senderID)
	if err := r.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("store: del %s: %w", key, err)
	}
	return nil
}

// RedisPauseSetter updates the bot:paused:{sender_id} flag in Redis.
type RedisPauseSetter interface {
	SetBotPaused(ctx context.Context, senderID string, paused bool) error
}

// PostgresPauseSetter updates the customers.bot_paused column in PostgreSQL.
type PostgresPauseSetter interface {
	SetCustomerBotPaused(ctx context.Context, senderID string, paused bool) error
}

// SyncBotPauseState updates the thread pause state in Redis (primary fast check)
// and synchronizes the flag to PostgreSQL (logging a warning if Postgres fails).
func SyncBotPauseState(
	ctx context.Context,
	redisStore RedisPauseSetter,
	pgStore PostgresPauseSetter,
	senderID string,
	paused bool,
	logger *slog.Logger,
) error {
	if redisStore != nil {
		if err := redisStore.SetBotPaused(ctx, senderID, paused); err != nil {
			return fmt.Errorf("store: set redis bot paused for %s: %w", senderID, err)
		}
	}
	if pgStore != nil {
		if err := pgStore.SetCustomerBotPaused(ctx, senderID, paused); err != nil && logger != nil {
			logger.Warn("failed synchronizing bot_paused flag in postgres",
				"sender_id", senderID,
				"paused", paused,
				"err", err,
			)
		}
	}
	return nil
}

