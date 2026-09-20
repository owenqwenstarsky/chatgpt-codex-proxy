package config

import (
	"path/filepath"
	"testing"
	"time"
)

func TestLoadRequiresProxyAPIKey(t *testing.T) {
	t.Chdir(t.TempDir())

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want missing PROXY_API_KEY error")
	}
}

func TestLoadBuildsListenAddrAndDataDir(t *testing.T) {
	t.Setenv("PROXY_API_KEY", "test-key")

	tests := []struct {
		name       string
		env        map[string]string
		wantListen string
		wantData   string
	}{
		{
			name:       "defaults",
			wantListen: ":8080",
			wantData:   "data",
		},
		{
			name: "data dir override",
			env: map[string]string{
				"DATA_DIR": "custom-data",
			},
			wantListen: ":8080",
			wantData:   "custom-data",
		},
		{
			name: "port override",
			env: map[string]string{
				"PORT": "9090",
			},
			wantListen: ":9090",
			wantData:   "data",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cwd := t.TempDir()
			t.Chdir(cwd)
			for key, value := range tc.env {
				t.Setenv(key, value)
			}

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.ListenAddr != tc.wantListen {
				t.Fatalf("Load() listen addr = %q, want %q", cfg.ListenAddr, tc.wantListen)
			}
			if cfg.DefaultModel != "gpt-6-astra" {
				t.Fatalf("Load() default model = %q, want gpt-6-astra", cfg.DefaultModel)
			}
			if cfg.StickyThreadTTL != 30*time.Minute {
				t.Fatalf("Load() sticky thread TTL = %s, want 30m", cfg.StickyThreadTTL)
			}
			if cfg.RateLimitMaxWait != 2*time.Minute {
				t.Fatalf("Load() rate limit max wait = %s, want 2m", cfg.RateLimitMaxWait)
			}
			if cfg.MaxActiveRequestsPerAccount != 2 {
				t.Fatalf("Load() max active requests = %d, want 2", cfg.MaxActiveRequestsPerAccount)
			}
			wantDataDir := filepath.Join(cwd, tc.wantData)
			if cfg.DataDir != wantDataDir {
				t.Fatalf("Load() data dir = %q, want %q", cfg.DataDir, wantDataDir)
			}
		})
	}
}

func TestLoadParsesAndValidatesMaxActiveRequestsPerAccount(t *testing.T) {
	t.Setenv("PROXY_API_KEY", "test-key")
	t.Setenv("MAX_ACTIVE_REQUESTS_PER_ACCOUNT", "4")
	t.Chdir(t.TempDir())
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxActiveRequestsPerAccount != 4 {
		t.Fatalf("limit = %d, want 4", cfg.MaxActiveRequestsPerAccount)
	}
	for _, value := range []string{"0", "-1", "nope"} {
		t.Setenv("MAX_ACTIVE_REQUESTS_PER_ACCOUNT", value)
		if _, err := Load(); err == nil {
			t.Fatalf("value %q accepted", value)
		}
	}
}

func TestLoadParsesRateLimitMaxWait(t *testing.T) {
	t.Setenv("PROXY_API_KEY", "test-key")
	t.Setenv("RATE_LIMIT_MAX_WAIT", "45s")
	t.Chdir(t.TempDir())

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.RateLimitMaxWait != 45*time.Second {
		t.Fatalf("RateLimitMaxWait = %s, want 45s", cfg.RateLimitMaxWait)
	}
}

func TestLoadRejectsInvalidRateLimitMaxWait(t *testing.T) {
	t.Setenv("PROXY_API_KEY", "test-key")
	t.Setenv("RATE_LIMIT_MAX_WAIT", "0s")
	t.Chdir(t.TempDir())

	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want invalid RATE_LIMIT_MAX_WAIT error")
	}
}

func TestLoadParsesStickyThreadTTL(t *testing.T) {
	t.Setenv("PROXY_API_KEY", "test-key")
	t.Setenv("STICKY_THREAD_TTL", "45m")
	t.Chdir(t.TempDir())

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.StickyThreadTTL != 45*time.Minute {
		t.Fatalf("StickyThreadTTL = %s, want 45m", cfg.StickyThreadTTL)
	}
}

func TestLoadRejectsInvalidStickyThreadTTL(t *testing.T) {
	t.Setenv("PROXY_API_KEY", "test-key")
	t.Setenv("STICKY_THREAD_TTL", "0s")
	t.Chdir(t.TempDir())

	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want invalid STICKY_THREAD_TTL error")
	}
}

func TestLoadRejectsInvalidPort(t *testing.T) {
	t.Setenv("PROXY_API_KEY", "test-key")
	t.Setenv("PORT", "not-a-port")
	t.Chdir(t.TempDir())

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want invalid PORT error")
	}
}

func TestLoadParsesDebugLogPayloads(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		wantDebug bool
		wantErr   bool
	}{
		{name: "true", value: "true", wantDebug: true},
		{name: "invalid", value: "definitely-not-bool", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("PROXY_API_KEY", "test-key")
			t.Setenv("DEBUG_LOG_PAYLOADS", tc.value)
			t.Chdir(t.TempDir())

			cfg, err := Load()
			if tc.wantErr {
				if err == nil {
					t.Fatal("Load() error = nil, want invalid DEBUG_LOG_PAYLOADS error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.DebugLogPayloads != tc.wantDebug {
				t.Fatalf("Load() debug log payloads = %v, want %v", cfg.DebugLogPayloads, tc.wantDebug)
			}
		})
	}
}
