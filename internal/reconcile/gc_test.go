package reconcile

import (
	"testing"
	"time"

	"github.com/breml/go-uptime-kuma-client/monitor"

	"github.com/solid3dlab/uptime-operator/internal/config"
)

func TestDecideGC(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	missing := now.Add(-2 * time.Hour)

	cases := []struct {
		name  string
		state gcState
		want  gcAction
	}{
		{
			name:  "immediate deletes even with no timestamp",
			state: gcState{Policy: config.DeleteImmediate, Grace: 24 * time.Hour},
			want:  gcDelete,
		},
		{
			name:  "deferred without timestamp stamps",
			state: gcState{Policy: config.DeleteDeferred, Grace: 24 * time.Hour},
			want:  gcStamp,
		},
		{
			name:  "deferred inside grace holds",
			state: gcState{Policy: config.DeleteDeferred, Grace: 24 * time.Hour, MissingSince: &missing},
			want:  gcHold,
		},
		{
			name:  "deferred after grace deletes",
			state: gcState{Policy: config.DeleteDeferred, Grace: time.Hour, MissingSince: &missing},
			want:  gcDelete,
		},
		{
			name:  "deferred exactly at grace deletes",
			state: gcState{Policy: config.DeleteDeferred, Grace: 2 * time.Hour, MissingSince: &missing},
			want:  gcDelete,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := decideGC(tc.state, now); got != tc.want {
				t.Fatalf("decideGC() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestGCDescriptionRoundTrip(t *testing.T) {
	t.Parallel()
	missing := time.Date(2026, 8, 25, 15, 23, 48, 0, time.UTC)
	state := gcState{
		Policy:       config.DeleteDeferred,
		Grace:        24 * time.Hour,
		MissingSince: &missing,
	}
	got := parseGCDescription(ptrString(formatGCDescription(state)))
	if got.Policy != state.Policy || got.Grace != state.Grace {
		t.Fatalf("round trip policy/grace: %+v", got)
	}
	if got.MissingSince == nil || !got.MissingSince.Equal(missing) {
		t.Fatalf("round trip missing-since: %v", got.MissingSince)
	}
}

func TestParseGCDescriptionIgnoresJunk(t *testing.T) {
	t.Parallel()
	got := parseGCDescription(ptrString("hello world;delete-policy=immediate;delete-grace=90m"))
	if got.Policy != config.DeleteImmediate || got.Grace != 90*time.Minute {
		t.Fatalf("got %+v", got)
	}
	if parseGCDescription(nil).Policy != "" {
		t.Fatal("nil description should be empty")
	}
}

func TestPolicyFromAnnotations(t *testing.T) {
	t.Parallel()
	r := &Reconciler{cfg: config.Config{
		DefaultDeletePolicy: config.DeleteDeferred,
		DefaultDeleteGrace:  24 * time.Hour,
	}}

	got := r.policyFromAnnotations(nil)
	if got.Policy != config.DeleteDeferred || got.Grace != 24*time.Hour {
		t.Fatalf("defaults: %+v", got)
	}

	got = r.policyFromAnnotations(map[string]string{
		annDeletePolicy: "immediate",
		annDeleteGrace:  "6h",
	})
	if got.Policy != config.DeleteImmediate || got.Grace != 6*time.Hour {
		t.Fatalf("override: %+v", got)
	}

	got = r.policyFromAnnotations(map[string]string{
		annDeletePolicy: "nope",
		annDeleteGrace:  "24",
	})
	if got.Policy != config.DeleteDeferred || got.Grace != 24*time.Hour {
		t.Fatalf("invalid policy keeps default, grace 24 hours: %+v", got)
	}
}

func TestPolicyForOrphanUsesStoredState(t *testing.T) {
	t.Parallel()
	r := &Reconciler{cfg: config.Config{
		DefaultDeletePolicy: config.DeleteDeferred,
		DefaultDeleteGrace:  24 * time.Hour,
	}}
	missing := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	desc := formatGCDescription(gcState{
		Policy:       config.DeleteImmediate,
		Grace:        6 * time.Hour,
		MissingSince: &missing,
	})
	mon := monitor.Base{Description: &desc}

	got := r.policyForOrphan(mon, nil)
	if got.Policy != config.DeleteImmediate || got.Grace != 6*time.Hour {
		t.Fatalf("stored policy: %+v", got)
	}
	if got.MissingSince == nil || !got.MissingSince.Equal(missing) {
		t.Fatalf("stored missing-since: %v", got.MissingSince)
	}

	got = r.policyForOrphan(mon, map[string]string{annDeletePolicy: "deferred"})
	if got.Policy != config.DeleteDeferred {
		t.Fatalf("live annotation should win: %+v", got)
	}
	if got.MissingSince == nil || !got.MissingSince.Equal(missing) {
		t.Fatal("live annotation must keep missing-since")
	}

	got = r.policyForOrphan(monitor.Base{}, nil)
	if got.Policy != config.DeleteDeferred || got.Grace != 24*time.Hour {
		t.Fatalf("legacy monitor uses cluster default: %+v", got)
	}
}
