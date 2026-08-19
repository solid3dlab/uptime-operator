package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config is loaded from environment variables.
type Config struct {
	KumaURL           string
	KumaUsername      string
	KumaPassword      string
	ResyncInterval    time.Duration
	StaticMonitorsPath string
	ManagedTag        string
	ManagedTagColor   string
}

// FromEnv reads operator configuration from the process environment.
func FromEnv() (Config, error) {
	cfg := Config{
		KumaURL:            os.Getenv("KUMA_URL"),
		KumaUsername:       os.Getenv("KUMA_USERNAME"),
		KumaPassword:       os.Getenv("KUMA_PASSWORD"),
		StaticMonitorsPath: envOr("STATIC_MONITORS_PATH", "/config/monitors.yaml"),
		ManagedTag:         envOr("MANAGED_TAG", "managed-by-uptime-operator"),
		ManagedTagColor:    envOr("MANAGED_TAG_COLOR", "#2563eb"),
		ResyncInterval:     5 * time.Minute,
	}

	if cfg.KumaURL == "" || cfg.KumaUsername == "" || cfg.KumaPassword == "" {
		return Config{}, fmt.Errorf("KUMA_URL, KUMA_USERNAME, and KUMA_PASSWORD are required")
	}

	if v := os.Getenv("RESYNC_INTERVAL"); v != "" {
		sec, err := strconv.Atoi(v)
		if err != nil || sec < 10 {
			return Config{}, fmt.Errorf("invalid RESYNC_INTERVAL %q (seconds, min 10)", v)
		}
		cfg.ResyncInterval = time.Duration(sec) * time.Second
	}

	return cfg, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
