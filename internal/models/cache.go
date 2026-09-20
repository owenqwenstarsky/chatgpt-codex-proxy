package models

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const cacheFilename = "models-cache.json"

type CacheSnapshot struct {
	Models  []Entry             `json:"models"`
	Support map[string][]string `json:"support"`
}

func LoadCache(dataDir string) (CacheSnapshot, error) {
	path := filepath.Join(dataDir, cacheFilename)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return CacheSnapshot{}, nil
		}
		return CacheSnapshot{}, fmt.Errorf("read models cache: %w", err)
	}

	var snapshot CacheSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return CacheSnapshot{}, fmt.Errorf("decode models cache: %w", err)
	}
	return snapshot, nil
}

func SaveCache(dataDir string, snapshot CacheSnapshot) error {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("create models cache dir: %w", err)
	}

	payload, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("encode models cache: %w", err)
	}
	payload = append(payload, '\n')

	path := filepath.Join(dataDir, cacheFilename)
	tmp, err := os.CreateTemp(dataDir, ".models-cache-*.tmp")
	if err != nil {
		return fmt.Errorf("create tmp models cache: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("chmod tmp models cache: %w", err)
	}
	if _, err := tmp.Write(payload); err != nil {
		tmp.Close()
		return fmt.Errorf("write tmp models cache: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync tmp models cache: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close tmp models cache: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename models cache: %w", err)
	}
	// Directory syncing is best-effort because some otherwise supported volume
	// drivers reject fsync on directories. The file itself was synced above.
	if dir, err := os.Open(dataDir); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}
