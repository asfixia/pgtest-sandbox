package config

import (
	"sync"
)

type GlobalConfig struct {
	instance   *Config
	configPath string
	mu         sync.RWMutex
}

var (
	global *GlobalConfig
	once   sync.Once
)

func Init() {
	once.Do(func() {
		global = &GlobalConfig{
			instance:   nil,
			configPath: "",
		}
	})
}

func SetOnce(config *Config, cfgPath string) {
	global.mu.Lock()
	defer global.mu.Unlock()
	if global.instance != nil {
		panic("AppConfig already initialized")
	}
	global.instance = config
	global.configPath = cfgPath
}

// GetCfgIfSet returns the current config and true if config was initialized, or (nil, false).
func GetCfgIfSet() (*Config, bool) {
	global.mu.RLock()
	defer global.mu.RUnlock()
	if global.instance == nil {
		return nil, false
	}
	cloned := *global.instance
	return &cloned, true
}

func GetCfg() *Config {
	cfg, ok := GetCfgIfSet()
	if !ok {
		panic("AppConfig not initialized")
	}
	return cfg
}

// GetConfigPath returns the path to the config file in use, or empty if not set.
func GetConfigPath() string {
	global.mu.RLock()
	defer global.mu.RUnlock()
	return global.configPath
}

// SetConfigPath updates the path of the config file used by UpdateAndSave / API.
// Used when the GUI explicitly chooses or infers a config path.
func SetConfigPath(path string) {
	global.mu.Lock()
	defer global.mu.Unlock()
	global.configPath = path
}

// SetConfig replaces the in-memory config (used after saving to file).
func SetConfig(c *Config) {
	global.mu.Lock()
	defer global.mu.Unlock()
	global.instance = c
}
