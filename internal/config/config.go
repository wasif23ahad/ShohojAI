// Package config provides typed environment configuration with strict fail-fast startup validation.
package config

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultPort                = "8080"
	DefaultMetaGraphAPIBaseURL = "https://graph.facebook.com/v26.0"
	DefaultDebounceWindow      = 3 * time.Second
	DefaultDebounceMaxBatch    = 8
	DefaultLLMModel            = "gemini-2.5-flash"
	DefaultLLMEmbeddingModel   = "text-embedding-004"
)

// EnvLookupFunc abstracts environment variable lookup for deterministic testing.
type EnvLookupFunc func(key string) (string, bool)

// Config holds all runtime configuration and credentials for the Messenger AI Agent.
type Config struct {
	Port                string
	LogLevel            slog.Level
	AdminAPIKey         string
	MetaAppSecret       string
	MetaVerifyToken     string
	MetaPageAccessToken string
	MetaGraphAPIBaseURL string
	DatabaseURL         string
	RedisAddr           string
	RedisPassword       string
	RedisDB             int
	DebounceWindow      time.Duration
	DebounceMaxBatch    int
	LLMAPIKey           string
	LLMModel            string
	LLMEmbeddingModel   string
}

// Load reads configuration from ".env" (if present) and the process environment,
// with process environment variables taking precedence over ".env".
func Load() (*Config, error) {
	dotEnvMap := parseDotEnvFile(".env")
	lookup := func(key string) (string, bool) {
		if val, ok := os.LookupEnv(key); ok {
			return val, true
		}
		if val, ok := dotEnvMap[key]; ok {
			return val, true
		}
		return "", false
	}
	return LoadFromLookup(lookup)
}

func parseDotEnvFile(path string) map[string]string {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer func() { _ = f.Close() }()

	res := make(map[string]string)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		idx := strings.IndexByte(line, '=')
		if idx <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:idx])
		val := strings.TrimSpace(line[idx+1:])
		if len(val) >= 2 && ((val[0] == '"' && val[len(val)-1] == '"') || (val[0] == '\'' && val[len(val)-1] == '\'')) {
			val = val[1 : len(val)-1]
		}
		res[key] = val
	}
	return res
}

// LoadFromLookup reads configuration using the provided lookup function and validates it.
func LoadFromLookup(lookup EnvLookupFunc) (*Config, error) {
	if lookup == nil {
		return nil, errors.New("config: lookup function must not be nil")
	}

	cfg := &Config{
		Port:                getStringOrDefault(lookup, "PORT", DefaultPort),
		LogLevel:            slog.LevelInfo,
		AdminAPIKey:         getTrimmed(lookup, "ADMIN_API_KEY"),
		MetaAppSecret:       getTrimmed(lookup, "META_APP_SECRET"),
		MetaVerifyToken:     getTrimmed(lookup, "META_VERIFY_TOKEN"),
		MetaPageAccessToken: getTrimmed(lookup, "META_PAGE_ACCESS_TOKEN"),
		MetaGraphAPIBaseURL: strings.TrimRight(getStringOrDefault(lookup, "META_GRAPH_API_BASE_URL", DefaultMetaGraphAPIBaseURL), "/"),
		DatabaseURL:         getTrimmed(lookup, "DATABASE_URL"),
		RedisAddr:           getTrimmed(lookup, "REDIS_ADDR"),
		RedisPassword:       getRaw(lookup, "REDIS_PASSWORD"),
		RedisDB:             0,
		DebounceWindow:      DefaultDebounceWindow,
		DebounceMaxBatch:    DefaultDebounceMaxBatch,
		LLMAPIKey:           getTrimmed(lookup, "LLM_API_KEY"),
		LLMModel:            getStringOrDefault(lookup, "LLM_MODEL", DefaultLLMModel),
		LLMEmbeddingModel:   getStringOrDefault(lookup, "LLM_EMBEDDING_MODEL", DefaultLLMEmbeddingModel),
	}

	var parseErrs []error

	if rawLevel := getTrimmed(lookup, "LOG_LEVEL"); rawLevel != "" {
		var lvl slog.Level
		if err := lvl.UnmarshalText([]byte(rawLevel)); err != nil {
			parseErrs = append(parseErrs, fmt.Errorf("invalid LOG_LEVEL %q: %w", rawLevel, err))
		} else {
			cfg.LogLevel = lvl
		}
	}

	if rawRedisDB := getTrimmed(lookup, "REDIS_DB"); rawRedisDB != "" {
		db, err := strconv.Atoi(rawRedisDB)
		if err != nil || db < 0 {
			parseErrs = append(parseErrs, fmt.Errorf("invalid REDIS_DB %q: must be a non-negative integer", rawRedisDB))
		} else {
			cfg.RedisDB = db
		}
	}

	if rawWindow := getTrimmed(lookup, "DEBOUNCE_WINDOW"); rawWindow != "" {
		window, err := time.ParseDuration(rawWindow)
		if err != nil || window <= 0 {
			parseErrs = append(parseErrs, fmt.Errorf("invalid DEBOUNCE_WINDOW %q: must be a positive duration", rawWindow))
		} else {
			cfg.DebounceWindow = window
		}
	}

	if rawBatch := getTrimmed(lookup, "DEBOUNCE_MAX_BATCH"); rawBatch != "" {
		batch, err := strconv.Atoi(rawBatch)
		if err != nil || batch <= 0 {
			parseErrs = append(parseErrs, fmt.Errorf("invalid DEBOUNCE_MAX_BATCH %q: must be a positive integer", rawBatch))
		} else {
			cfg.DebounceMaxBatch = batch
		}
	}

	if err := cfg.Validate(); err != nil {
		parseErrs = append(parseErrs, err)
	}

	if len(parseErrs) > 0 {
		return nil, fmt.Errorf("config validation failed: %w", errors.Join(parseErrs...))
	}

	return cfg, nil
}

// Validate checks that all required secrets and endpoints are present and well-formed.
func (c *Config) Validate() error {
	if c == nil {
		return errors.New("config is nil")
	}

	var errs []error

	if strings.TrimSpace(c.MetaAppSecret) == "" {
		errs = append(errs, errors.New("missing required environment variable META_APP_SECRET"))
	}
	if strings.TrimSpace(c.MetaVerifyToken) == "" {
		errs = append(errs, errors.New("missing required environment variable META_VERIFY_TOKEN"))
	}
	if strings.TrimSpace(c.MetaPageAccessToken) == "" {
		errs = append(errs, errors.New("missing required environment variable META_PAGE_ACCESS_TOKEN"))
	}
	if strings.TrimSpace(c.DatabaseURL) == "" {
		errs = append(errs, errors.New("missing required environment variable DATABASE_URL"))
	} else if err := validatePostgresURL(c.DatabaseURL); err != nil {
		errs = append(errs, fmt.Errorf("invalid DATABASE_URL: %w", err))
	}
	if strings.TrimSpace(c.RedisAddr) == "" {
		errs = append(errs, errors.New("missing required environment variable REDIS_ADDR"))
	}
	if strings.TrimSpace(c.LLMAPIKey) == "" {
		errs = append(errs, errors.New("missing required environment variable LLM_API_KEY"))
	}

	portNum, err := strconv.Atoi(strings.TrimSpace(c.Port))
	if err != nil || portNum < 1 || portNum > 65535 {
		errs = append(errs, fmt.Errorf("invalid PORT %q: must be an integer between 1 and 65535", c.Port))
	}

	if c.DebounceWindow <= 0 {
		errs = append(errs, errors.New("DebounceWindow must be greater than 0"))
	}
	if c.DebounceMaxBatch <= 0 {
		errs = append(errs, errors.New("DebounceMaxBatch must be greater than 0"))
	}
	if c.RedisDB < 0 {
		errs = append(errs, errors.New("RedisDB must be non-negative"))
	}

	return errors.Join(errs...)
}

func validatePostgresURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return fmt.Errorf("scheme must be postgres or postgresql, got %q", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("host is required")
	}
	return nil
}

func getRaw(lookup EnvLookupFunc, key string) string {
	val, ok := lookup(key)
	if !ok {
		return ""
	}
	return val
}

func getTrimmed(lookup EnvLookupFunc, key string) string {
	return strings.TrimSpace(getRaw(lookup, key))
}

func getStringOrDefault(lookup EnvLookupFunc, key, defaultVal string) string {
	if val := getTrimmed(lookup, key); val != "" {
		return val
	}
	return defaultVal
}
