package jsonstore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"chatgpt-codex-proxy/internal/accounts"
)

type JSONAccountsStore struct {
	path string
}

func NewJSONAccountsStore(dataDir string) *JSONAccountsStore {
	return &JSONAccountsStore{
		path: filepath.Join(dataDir, "accounts.json"),
	}
}

func (s *JSONAccountsStore) Load() (accounts.State, error) {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return accounts.State{}, nil
		}
		return accounts.State{}, fmt.Errorf("read accounts store: %w", err)
	}

	var state accounts.State
	if err := json.Unmarshal(raw, &state); err != nil {
		return accounts.State{}, fmt.Errorf("decode accounts store: %w", err)
	}
	return state, nil
}

func (s *JSONAccountsStore) Save(state accounts.State) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create store dir: %w", err)
	}

	payload, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode accounts store: %w", err)
	}
	payload = append(payload, '\n')

	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".accounts-*.tmp")
	if err != nil {
		return fmt.Errorf("create tmp accounts store: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("chmod tmp accounts store: %w", err)
	}
	if _, err := tmp.Write(payload); err != nil {
		tmp.Close()
		return fmt.Errorf("write tmp accounts store: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync tmp accounts store: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close tmp accounts store: %w", err)
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		return fmt.Errorf("rename accounts store: %w", err)
	}
	// Directory syncing is best-effort because some otherwise supported volume
	// drivers reject fsync on directories. The file itself was synced above.
	if dir, err := os.Open(filepath.Dir(s.path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}
