package config

import (
	"testing"
	"time"
)

func TestParseGrace(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"24h", 24 * time.Hour, false},
		{"90m", 90 * time.Minute, false},
		{"24", 24 * time.Hour, false},
		{"1.5", 90 * time.Minute, false},
		{"", 0, true},
		{"0", 0, true},
		{"-2h", 0, true},
		{"nope", 0, true},
	}
	for _, tc := range cases {
		got, err := ParseGrace(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("ParseGrace(%q) = %s, want error", tc.in, got)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Fatalf("ParseGrace(%q) = %s, %v want %s", tc.in, got, err, tc.want)
		}
	}
}

func TestFromEnvDeleteDefaults(t *testing.T) {
	t.Setenv("KUMA_URL", "https://kuma.example")
	t.Setenv("KUMA_USERNAME", "op")
	t.Setenv("KUMA_PASSWORD", "secret")
	t.Setenv("DEFAULT_DELETE_POLICY", "")
	t.Setenv("DEFAULT_DELETE_GRACE", "")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DefaultDeletePolicy != DeleteDeferred || cfg.DefaultDeleteGrace != 24*time.Hour {
		t.Fatalf("defaults: %+v", cfg)
	}

	t.Setenv("DEFAULT_DELETE_POLICY", "immediate")
	t.Setenv("DEFAULT_DELETE_GRACE", "6h")
	cfg, err = FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DefaultDeletePolicy != DeleteImmediate || cfg.DefaultDeleteGrace != 6*time.Hour {
		t.Fatalf("overrides: %+v", cfg)
	}

	t.Setenv("DEFAULT_DELETE_POLICY", "retain")
	cfg, err = FromEnv()
	if err != nil || cfg.DefaultDeletePolicy != DeleteRetain {
		t.Fatalf("retain: %+v %v", cfg, err)
	}

	t.Setenv("DEFAULT_DELETE_POLICY", "whenever")
	if _, err = FromEnv(); err == nil {
		t.Fatal("expected invalid policy error")
	}
}
