// Package testutil provides shared helpers for tests, including config and project root resolution.
package testutil

import (
	"os"
	"path/filepath"
)

const defaultConfigRel = "config/pgtest-sandbox.yaml"

// ProjectRoot returns the project root directory (directory containing go.mod).
// Returns empty string if not found (e.g. when run outside the repo).
func ProjectRoot() string {
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

// ConfigPath returns the path to the pgtest config file to use.
// 1. If PGTEST_CONFIG is set: use it (absolute as-is; relative is joined with project root, or cwd if no root).
// 2. Else: projectRoot/config/pgtest-sandbox.yaml, or cwd/config/pgtest-sandbox.yaml if no project root.
// All callers that need a root/config path should use this so behavior is unified.
func ConfigPath() string {
	if envPath := os.Getenv("PGTEST_CONFIG"); envPath != "" {
		if filepath.IsAbs(envPath) {
			return envPath
		}
		if root := ProjectRoot(); root != "" {
			return filepath.Join(root, envPath)
		}
		workDir, _ := os.Getwd()
		return filepath.Join(workDir, envPath)
	}
	if root := ProjectRoot(); root != "" {
		return filepath.Join(root, defaultConfigRel)
	}
	workDir, _ := os.Getwd()
	return filepath.Join(workDir, defaultConfigRel)
}
