package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	// DeleteImmediate removes a managed monitor as soon as its Ingress is gone.
	DeleteImmediate = "immediate"
	// DeleteDeferred keeps probing until DefaultDeleteGrace (or the Ingress
	// annotation) elapses, so an accidental Helm uninstall still pages.
	DeleteDeferred = "deferred"
	// DeleteRetain never removes the monitor. The operator keeps probing the
	// last URL until a human deletes it in Kuma or the policy is changed.
	DeleteRetain = "retain"
)

// ValidDeletePolicy reports whether v is an accepted delete-policy value.
func ValidDeletePolicy(v string) bool {
	switch v {
	case DeleteImmediate, DeleteDeferred, DeleteRetain:
		return true
	default:
		return false
	}
}

// Config is loaded from environment variables.
type Config struct {
	KumaURL             string
	KumaUsername        string
	KumaPassword        string
	ResyncInterval      time.Duration
	StaticMonitorsPath  string
	ManagedTag          string
	ManagedTagColor     string
	DefaultDeletePolicy string
	DefaultDeleteGrace  time.Duration
}

// FromEnv reads operator configuration from the process environment.
func FromEnv() (Config, error) {
	cfg := Config{
		KumaURL:             os.Getenv("KUMA_URL"),
		KumaUsername:        os.Getenv("KUMA_USERNAME"),
		KumaPassword:        os.Getenv("KUMA_PASSWORD"),
		StaticMonitorsPath:  envOr("STATIC_MONITORS_PATH", "/config/monitors.yaml"),
		ManagedTag:          envOr("MANAGED_TAG", "managed-by-uptime-operator"),
		ManagedTagColor:     envOr("MANAGED_TAG_COLOR", "#2563eb"),
		DefaultDeletePolicy: DeleteDeferred,
		DefaultDeleteGrace:  24 * time.Hour,
		ResyncInterval:      5 * time.Minute,
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

	if v := strings.ToLower(strings.TrimSpace(os.Getenv("DEFAULT_DELETE_POLICY"))); v != "" {
		if !ValidDeletePolicy(v) {
			return Config{}, fmt.Errorf("invalid DEFAULT_DELETE_POLICY %q (immediate, deferred, or retain)", v)
		}
		cfg.DefaultDeletePolicy = v
	}

	if v := strings.TrimSpace(os.Getenv("DEFAULT_DELETE_GRACE")); v != "" {
		d, err := ParseGrace(v)
		if err != nil {
			return Config{}, fmt.Errorf("invalid DEFAULT_DELETE_GRACE %q: %w", v, err)
		}
		cfg.DefaultDeleteGrace = d
	}

	return cfg, nil
}

// ParseGrace accepts a Go duration (`24h`, `90m`) or a bare number of hours (`24`).
func ParseGrace(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty duration")
	}
	if d, err := time.ParseDuration(raw); err == nil {
		if d <= 0 {
			return 0, fmt.Errorf("duration must be positive")
		}
		return d, nil
	}
	hours, err := strconv.ParseFloat(raw, 64)
	if err != nil || hours <= 0 {
		return 0, fmt.Errorf("want Go duration or positive hours, got %q", raw)
	}
	return time.Duration(hours * float64(time.Hour)), nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
