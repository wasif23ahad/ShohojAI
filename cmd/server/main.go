package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"messenger-agent/internal/agent"
	"messenger-agent/internal/config"
	"messenger-agent/internal/debounce"
	"messenger-agent/internal/messenger"
	"messenger-agent/internal/store"
	"messenger-agent/internal/webhook"
)

func main() {
	baseLogger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(baseLogger)

	cfg, err := config.Load()
	if err != nil {
		baseLogger.Error("configuration validation failed", "err", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pgPool, err := store.NewPostgresPool(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to initialize PostgreSQL pool", "err", err)
		os.Exit(1)
	}
	defer pgPool.Close()

	pgStore := store.NewPostgresStore(pgPool, logger)
	if err := pgStore.SeedSampleProducts(ctx); err != nil {
		logger.Warn("sample product seed skipped or failed", "err", err)
	}

	redisClient, err := store.NewRedisClient(ctx, cfg)
	if err != nil {
		logger.Error("failed to initialize Redis client", "err", err)
		os.Exit(1)
	}
	defer func() { _ = redisClient.Close() }()

	redisStore := store.NewRedisStore(redisClient, logger)

	msgClient := messenger.NewClient(cfg.MetaGraphAPIBaseURL, cfg.MetaPageAccessToken, logger)
	llmClient := agent.NewGeminiClient(cfg.LLMAPIKey, cfg.LLMModel, cfg.LLMEmbeddingModel, nil, logger)
	mediaProc := messenger.NewMediaProcessor(nil, llmClient, logger)

	orchestrator := agent.NewOrchestrator(pgStore, redisStore, msgClient, mediaProc, llmClient, logger)

	debouncer := debounce.NewDebouncer(
		redisClient,
		cfg.DebounceWindow,
		cfg.DebounceMaxBatch,
		orchestrator.HandleBurst,
		logger,
	)

	webhookHandler := webhook.NewHandler(
		cfg.MetaVerifyToken,
		cfg.MetaAppSecret,
		debouncer,
		redisStore,
		logger,
	)

	mux := NewServerMux(webhookHandler, pgStore, redisStore, cfg.AdminAPIKey, logger)

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("starting Meta Messenger AI Agent server",
			"port", cfg.Port,
			"debounce_window", cfg.DebounceWindow.String(),
			"debounce_max_batch", cfg.DebounceMaxBatch,
			"llm_model", cfg.LLMModel,
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining HTTP server and Redis debouncer")
	case err := <-errCh:
		if err != nil {
			logger.Error("HTTP server exited with error", "err", err)
			os.Exit(1)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server graceful shutdown error", "err", err)
	}
	if err := debouncer.Shutdown(shutdownCtx); err != nil {
		logger.Error("debouncer graceful shutdown error", "err", err)
	}

	logger.Info("server shutdown complete")
}

// ThreadAdminStore abstracts the pause/resume state operations needed by admin endpoints.
type ThreadAdminStore interface {
	SetCustomerBotPaused(ctx context.Context, senderID string, paused bool) error
}

// RedisAdminStore abstracts the Redis pause flag operations needed by admin endpoints.
type RedisAdminStore interface {
	SetBotPaused(ctx context.Context, senderID string, paused bool) error
}

// NewServerMux wires all webhook, healthcheck, and protected admin HTTP routes using Go 1.23+ method patterns.
func NewServerMux(
	webhookHandler *webhook.Handler,
	pgAdmin ThreadAdminStore,
	redisAdmin RedisAdminStore,
	adminAPIKey string,
	logger *slog.Logger,
) *http.ServeMux {
	if logger == nil {
		logger = slog.Default()
	}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "ok",
			"service": "messenger-agent",
		})
	})

	if webhookHandler != nil {
		mux.HandleFunc("GET /webhook", webhookHandler.Verify)
		mux.HandleFunc("POST /webhook", webhookHandler.Receive)
	}

	mux.HandleFunc("POST /admin/threads/{sender_id}/pause", handleAdminThreadToggle(pgAdmin, redisAdmin, adminAPIKey, true, logger))
	mux.HandleFunc("POST /admin/threads/{sender_id}/resume", handleAdminThreadToggle(pgAdmin, redisAdmin, adminAPIKey, false, logger))

	return mux
}

func handleAdminThreadToggle(
	pgAdmin ThreadAdminStore,
	redisAdmin RedisAdminStore,
	adminAPIKey string,
	pause bool,
	logger *slog.Logger,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !authorizeAdminRequest(r, adminAPIKey) {
			writeJSON(w, http.StatusUnauthorized, map[string]any{
				"error": "unauthorized: invalid or missing ADMIN_API_KEY",
			})
			return
		}

		senderID := strings.TrimSpace(r.PathValue("sender_id"))
		if senderID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"error": "sender_id path parameter is required",
			})
			return
		}

		ctx := r.Context()
		if redisAdmin != nil {
			if err := redisAdmin.SetBotPaused(ctx, senderID, pause); err != nil {
				logger.Error("admin failed updating redis bot pause state",
					"sender_id", senderID,
					"paused", pause,
					"err", err,
				)
				writeJSON(w, http.StatusInternalServerError, map[string]any{
					"error": "failed updating redis pause state",
				})
				return
			}
		}

		if pgAdmin != nil {
			if err := pgAdmin.SetCustomerBotPaused(ctx, senderID, pause); err != nil {
				logger.Warn("admin failed updating postgres bot_paused flag",
					"sender_id", senderID,
					"paused", pause,
					"err", err,
				)
			}
		}

		status := "resumed"
		if pause {
			status = "paused"
		}
		logger.Info("admin updated thread bot state",
			"sender_id", senderID,
			"bot_paused", pause,
			"status", status,
		)

		writeJSON(w, http.StatusOK, map[string]any{
			"sender_id":  senderID,
			"bot_paused": pause,
			"status":     status,
		})
	}
}

func authorizeAdminRequest(r *http.Request, expectedKey string) bool {
	expectedKey = strings.TrimSpace(expectedKey)
	if expectedKey == "" {
		return false
	}

	provided := strings.TrimSpace(r.Header.Get("X-Admin-API-Key"))
	if provided == "" {
		authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
		if strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
			provided = strings.TrimSpace(authHeader[7:])
		}
	}
	if provided == "" {
		return false
	}

	return subtle.ConstantTimeCompare([]byte(provided), []byte(expectedKey)) == 1
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
