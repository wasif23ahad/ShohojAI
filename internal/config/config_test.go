package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func validEnvMap() map[string]string {
	return map[string]string{
		"META_APP_SECRET":        "secret-123",
		"META_VERIFY_TOKEN":      "verify-123",
		"META_PAGE_ACCESS_TOKEN": "page-token-123",
		"DATABASE_URL":           "postgres://postgres:postgres@localhost:5432/messenger_agent?sslmode=disable",
		"REDIS_ADDR":             "localhost:6379",
		"LLM_API_KEY":            "llm-key-123",
	}
}

func mapLookup(env map[string]string) EnvLookupFunc {
	return func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}
}

func TestLoadFromLookup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		mutateEnv   func(env map[string]string)
		wantErr     bool
		errContains []string
		assertCfg   func(t *testing.T, cfg *Config)
	}{
		{
			name:      "valid minimal configuration applies defaults",
			mutateEnv: func(_ map[string]string) {},
			wantErr:   false,
			assertCfg: func(t *testing.T, cfg *Config) {
				t.Helper()
				if cfg.Port != DefaultPort {
					t.Errorf("Port = %q, want %q", cfg.Port, DefaultPort)
				}
				if cfg.LogLevel != slog.LevelInfo {
					t.Errorf("LogLevel = %v, want %v", cfg.LogLevel, slog.LevelInfo)
				}
				if cfg.MetaGraphAPIBaseURL != DefaultMetaGraphAPIBaseURL {
					t.Errorf("MetaGraphAPIBaseURL = %q, want %q", cfg.MetaGraphAPIBaseURL, DefaultMetaGraphAPIBaseURL)
				}
				if cfg.DebounceWindow != DefaultDebounceWindow {
					t.Errorf("DebounceWindow = %v, want %v", cfg.DebounceWindow, DefaultDebounceWindow)
				}
				if cfg.DebounceMaxBatch != DefaultDebounceMaxBatch {
					t.Errorf("DebounceMaxBatch = %d, want %d", cfg.DebounceMaxBatch, DefaultDebounceMaxBatch)
				}
				if cfg.LLMModel != DefaultLLMModel {
					t.Errorf("LLMModel = %q, want %q", cfg.LLMModel, DefaultLLMModel)
				}
				if cfg.LLMEmbeddingModel != DefaultLLMEmbeddingModel {
					t.Errorf("LLMEmbeddingModel = %q, want %q", cfg.LLMEmbeddingModel, DefaultLLMEmbeddingModel)
				}
			},
		},
		{
			name: "custom optional overrides",
			mutateEnv: func(env map[string]string) {
				env["PORT"] = "9090"
				env["LOG_LEVEL"] = "DEBUG"
				env["ADMIN_API_KEY"] = "admin-secret"
				env["META_GRAPH_API_BASE_URL"] = "https://graph.facebook.com/v26.0/"
				env["REDIS_PASSWORD"] = "redis-pass"
				env["REDIS_DB"] = "2"
				env["DEBOUNCE_WINDOW"] = "4500ms"
				env["DEBOUNCE_MAX_BATCH"] = "12"
				env["LLM_MODEL"] = "gemini-2.5-pro"
				env["LLM_EMBEDDING_MODEL"] = "text-embedding-004"
			},
			wantErr: false,
			assertCfg: func(t *testing.T, cfg *Config) {
				t.Helper()
				if cfg.Port != "9090" {
					t.Errorf("Port = %q, want 9090", cfg.Port)
				}
				if cfg.LogLevel != slog.LevelDebug {
					t.Errorf("LogLevel = %v, want DEBUG", cfg.LogLevel)
				}
				if cfg.AdminAPIKey != "admin-secret" {
					t.Errorf("AdminAPIKey = %q, want admin-secret", cfg.AdminAPIKey)
				}
				if cfg.MetaGraphAPIBaseURL != "https://graph.facebook.com/v26.0" {
					t.Errorf("MetaGraphAPIBaseURL = %q, expected trailing slash trimmed", cfg.MetaGraphAPIBaseURL)
				}
				if cfg.RedisPassword != "redis-pass" {
					t.Errorf("RedisPassword = %q, want redis-pass", cfg.RedisPassword)
				}
				if cfg.RedisDB != 2 {
					t.Errorf("RedisDB = %d, want 2", cfg.RedisDB)
				}
				if cfg.DebounceWindow != 4500*time.Millisecond {
					t.Errorf("DebounceWindow = %v, want 4.5s", cfg.DebounceWindow)
				}
				if cfg.DebounceMaxBatch != 12 {
					t.Errorf("DebounceMaxBatch = %d, want 12", cfg.DebounceMaxBatch)
				}
			},
		},
		{
			name: "missing all required variables fails fast with all listed",
			mutateEnv: func(env map[string]string) {
				for k := range env {
					delete(env, k)
				}
			},
			wantErr: true,
			errContains: []string{
				"META_APP_SECRET",
				"META_VERIFY_TOKEN",
				"META_PAGE_ACCESS_TOKEN",
				"DATABASE_URL",
				"REDIS_ADDR",
				"LLM_API_KEY",
			},
		},
		{
			name: "missing META_APP_SECRET only",
			mutateEnv: func(env map[string]string) {
				env["META_APP_SECRET"] = "   "
			},
			wantErr:     true,
			errContains: []string{"META_APP_SECRET"},
		},
		{
			name: "invalid DATABASE_URL scheme",
			mutateEnv: func(env map[string]string) {
				env["DATABASE_URL"] = "mysql://localhost:3306/db"
			},
			wantErr:     true,
			errContains: []string{"DATABASE_URL", "scheme must be postgres or postgresql"},
		},
		{
			name: "invalid PORT",
			mutateEnv: func(env map[string]string) {
				env["PORT"] = "70000"
			},
			wantErr:     true,
			errContains: []string{"PORT"},
		},
		{
			name: "invalid DEBOUNCE_WINDOW and DEBOUNCE_MAX_BATCH",
			mutateEnv: func(env map[string]string) {
				env["DEBOUNCE_WINDOW"] = "-1s"
				env["DEBOUNCE_MAX_BATCH"] = "0"
			},
			wantErr:     true,
			errContains: []string{"DEBOUNCE_WINDOW", "DEBOUNCE_MAX_BATCH"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := validEnvMap()
			tc.mutateEnv(env)

			cfg, err := LoadFromLookup(mapLookup(env))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("LoadFromLookup() succeeded, expected error containing %v", tc.errContains)
				}
				for _, substr := range tc.errContains {
					if !strings.Contains(err.Error(), substr) {
						t.Errorf("error %q does not contain expected substring %q", err.Error(), substr)
					}
				}
				return
			}

			if err != nil {
				t.Fatalf("LoadFromLookup() unexpected error: %v", err)
			}
			if tc.assertCfg != nil {
				tc.assertCfg(t, cfg)
			}
		})
	}
}
