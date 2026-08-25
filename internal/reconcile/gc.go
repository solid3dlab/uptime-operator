package reconcile

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/breml/go-uptime-kuma-client/monitor"

	"github.com/solid3dlab/uptime-operator/internal/config"
)

const (
	annDeletePolicy = "uptime-kuma.io/delete-policy"
	annDeleteGrace  = "uptime-kuma.io/delete-grace"
)

type gcState struct {
	Policy       string
	Grace        time.Duration
	MissingSince *time.Time
}

type gcAction int

const (
	gcDelete gcAction = iota
	gcHold
	gcStamp
	gcRetain
)

func decideGC(state gcState, now time.Time) gcAction {
	switch state.Policy {
	case config.DeleteRetain:
		return gcRetain
	case config.DeleteDeferred:
		if state.MissingSince == nil {
			return gcStamp
		}
		if !now.Before(state.MissingSince.Add(state.Grace)) {
			return gcDelete
		}
		return gcHold
	default:
		return gcDelete
	}
}

func (r *Reconciler) defaultGCState() gcState {
	policy := r.cfg.DefaultDeletePolicy
	if policy == "" {
		policy = config.DeleteDeferred
	}
	grace := r.cfg.DefaultDeleteGrace
	if grace <= 0 {
		grace = 24 * time.Hour
	}
	return gcState{Policy: policy, Grace: grace}
}

func (r *Reconciler) policyFromAnnotations(anns map[string]string) gcState {
	state := r.defaultGCState()
	if anns == nil {
		return state
	}
	if v := strings.ToLower(strings.TrimSpace(anns[annDeletePolicy])); config.ValidDeletePolicy(v) {
		state.Policy = v
	}
	if v := strings.TrimSpace(anns[annDeleteGrace]); v != "" {
		if d, err := config.ParseGrace(v); err == nil {
			state.Grace = d
		}
	}
	return state
}

func (r *Reconciler) policyFromStatic(entry staticMonitor) gcState {
	return r.policyFromAnnotations(map[string]string{
		annDeletePolicy: entry.DeletePolicy,
		annDeleteGrace:  entry.DeleteGrace,
	})
}

func (r *Reconciler) policyForOrphan(mon monitor.Base, anns map[string]string) gcState {
	stored := parseGCDescription(mon.Description)
	state := stored
	if anns != nil {
		state = r.policyFromAnnotations(anns)
	} else {
		defaults := r.defaultGCState()
		if state.Policy == "" {
			state.Policy = defaults.Policy
		}
		if state.Grace <= 0 {
			state.Grace = defaults.Grace
		}
	}
	state.MissingSince = stored.MissingSince
	return state
}

func (r *Reconciler) gcOrphan(ctx context.Context, key string, mon monitor.Base, anns map[string]string) error {
	state := r.policyForOrphan(mon, anns)
	now := r.now()
	switch decideGC(state, now) {
	case gcRetain:
		r.log.Info("retaining orphan monitor", "key", key, "id", mon.ID)
		return nil
	case gcStamp:
		r.log.Info("deferring orphan monitor",
			"key", key, "id", mon.ID, "grace", formatGrace(state.Grace))
		state.MissingSince = &now
		return r.updateMonitorDescription(ctx, mon, formatGCDescription(state))
	case gcHold:
		remaining := state.MissingSince.Add(state.Grace).Sub(now)
		r.log.Info("holding orphan monitor",
			"key", key, "id", mon.ID,
			"missing_since", state.MissingSince.Format(time.RFC3339),
			"remaining", remaining.Round(time.Second).String())
		return nil
	default:
		r.log.Info("deleting orphan monitor",
			"key", key, "id", mon.ID, "policy", state.Policy)
		return r.kuma.DeleteMonitor(ctx, mon.ID)
	}
}

func (r *Reconciler) updateMonitorDescription(ctx context.Context, mon monitor.Base, desc string) error {
	switch mon.Type() {
	case "http":
		var m monitor.HTTP
		if err := mon.As(&m); err != nil {
			return err
		}
		m.ID = mon.ID
		m.Description = &desc
		return r.kuma.UpdateMonitor(ctx, &m)
	case "ping":
		var m monitor.Ping
		if err := mon.As(&m); err != nil {
			return err
		}
		m.ID = mon.ID
		m.Description = &desc
		return r.kuma.UpdateMonitor(ctx, &m)
	case "port":
		var m monitor.TCPPort
		if err := mon.As(&m); err != nil {
			return err
		}
		m.ID = mon.ID
		m.Description = &desc
		return r.kuma.UpdateMonitor(ctx, &m)
	default:
		return fmt.Errorf("cannot stamp description on monitor type %q", mon.Type())
	}
}

func formatGCDescription(state gcState) string {
	parts := []string{
		"delete-policy=" + state.Policy,
		"delete-grace=" + formatGrace(state.Grace),
	}
	if state.MissingSince != nil {
		parts = append(parts, "missing-since="+state.MissingSince.UTC().Format(time.RFC3339))
	}
	return strings.Join(parts, ";")
}

func parseGCDescription(desc *string) gcState {
	var state gcState
	if desc == nil || strings.TrimSpace(*desc) == "" {
		return state
	}
	for _, part := range strings.Split(*desc, ";") {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "delete-policy":
			v := strings.ToLower(strings.TrimSpace(value))
			if config.ValidDeletePolicy(v) {
				state.Policy = v
			}
		case "delete-grace":
			if d, err := config.ParseGrace(value); err == nil {
				state.Grace = d
			}
		case "missing-since":
			if t, err := time.Parse(time.RFC3339, strings.TrimSpace(value)); err == nil {
				utc := t.UTC()
				state.MissingSince = &utc
			}
		}
	}
	return state
}

func formatGrace(d time.Duration) string {
	if d <= 0 {
		return "24h"
	}
	if d%time.Hour == 0 {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return d.String()
}

func descriptionEqual(a, b *string) bool {
	return derefString(a) == derefString(b)
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func ptrString(s string) *string {
	return &s
}
