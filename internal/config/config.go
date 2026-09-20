package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	ListenAddr                  string
	DataDir                     string
	ProxyAPIKey                 string
	DebugLogPayloads            bool
	DefaultModel                string
	CodexBaseURL                string
	AuthIssuer                  string
	OAuthClientID               string
	LoginTimeout                time.Duration
	ContinuationTTL             time.Duration
	StickyThreadTTL             time.Duration
	RateLimitMaxWait            time.Duration
	MaxActiveRequestsPerAccount int
	RequestTimeout              time.Duration
	RefreshSkew                 time.Duration
}

func Load() (Config, error) {
	if err := godotenv.Load(".env"); err != nil && !os.IsNotExist(err) {
		return Config{}, fmt.Errorf("load .env: %w", err)
	}

	dataDir := strings.TrimSpace(os.Getenv("DATA_DIR"))
	if dataDir == "" {
		dataDir = "data"
	}
	if !filepath.IsAbs(dataDir) {
		cwd, err := os.Getwd()
		if err != nil {
			return Config{}, fmt.Errorf("resolve cwd: %w", err)
		}
		dataDir = filepath.Join(cwd, dataDir)
	}

	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "8080"
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber <= 0 || portNumber > 65535 {
		return Config{}, fmt.Errorf("PORT must be a valid TCP port")
	}
	debugLogPayloads := false
	if raw := strings.TrimSpace(os.Getenv("DEBUG_LOG_PAYLOADS")); raw != "" {
		var err error
		debugLogPayloads, err = strconv.ParseBool(raw)
		if err != nil {
			return Config{}, fmt.Errorf("DEBUG_LOG_PAYLOADS must be a boolean")
		}
	}

	stickyThreadTTL := 30 * time.Minute
	if raw := strings.TrimSpace(os.Getenv("STICKY_THREAD_TTL")); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			return Config{}, fmt.Errorf("STICKY_THREAD_TTL must be a positive duration")
		}
		stickyThreadTTL = parsed
	}

	rateLimitMaxWait := 2 * time.Minute
	if raw := strings.TrimSpace(os.Getenv("RATE_LIMIT_MAX_WAIT")); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			return Config{}, fmt.Errorf("RATE_LIMIT_MAX_WAIT must be a positive duration")
		}
		rateLimitMaxWait = parsed
	}

	maxActiveRequestsPerAccount := 2
	if raw := strings.TrimSpace(os.Getenv("MAX_ACTIVE_REQUESTS_PER_ACCOUNT")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return Config{}, fmt.Errorf("MAX_ACTIVE_REQUESTS_PER_ACCOUNT must be a positive integer")
		}
		maxActiveRequestsPerAccount = parsed
	}

	cfg := Config{
		ListenAddr:                  ":" + strconv.Itoa(portNumber),
		DataDir:                     dataDir,
		ProxyAPIKey:                 strings.TrimSpace(os.Getenv("PROXY_API_KEY")),
		DebugLogPayloads:            debugLogPayloads,
		DefaultModel:                "gpt-6-astra",
		CodexBaseURL:                "https://chatgpt.com/backend-api",
		AuthIssuer:                  "https://auth.openai.com",
		OAuthClientID:               "app_EMoamEEZ73f0CkXaXp7hrann",
		LoginTimeout:                15 * time.Minute,
		ContinuationTTL:             time.Hour,
		StickyThreadTTL:             stickyThreadTTL,
		RateLimitMaxWait:            rateLimitMaxWait,
		MaxActiveRequestsPerAccount: maxActiveRequestsPerAccount,
		RequestTimeout:              30 * time.Minute,
		RefreshSkew:                 time.Minute,
	}

	if cfg.ProxyAPIKey == "" {
		return Config{}, fmt.Errorf("PROXY_API_KEY must be set")
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return Config{}, fmt.Errorf("create data dir: %w", err)
	}

	return cfg, nil
}
