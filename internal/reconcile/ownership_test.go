package reconcile

import (
	"testing"

	"github.com/breml/go-uptime-kuma-client/monitor"
	kumtag "github.com/breml/go-uptime-kuma-client/tag"
)

func tagged(id int64, name string, tags ...string) monitor.Base {
	m := monitor.Base{ID: id, Name: name}
	for _, t := range tags {
		m.Tags = append(m.Tags, kumtag.MonitorTag{Name: t})
	}
	return m
}

func TestClassifyMonitorsKeepsOtherClustersOut(t *testing.T) {
	t.Parallel()
	const own, legacy = "managed-by-uptime-operator:solid3d", "managed-by-uptime-operator"
	mons := []monitor.Base{
		tagged(1, "web/Ingress/www", own),
		tagged(2, "mailpit/Ingress/mailpit", legacy),
		tagged(3, "mailpit/Ingress/mailpit-k9", "managed-by-uptime-operator:k9"),
		tagged(4, "web/Ingress/crashed"),
		tagged(5, "Manual check"),
		tagged(6, "web/Ingress/both", own, legacy),
	}

	owned, leg, untagged := classifyMonitors(mons, own, legacy)

	if len(owned) != 2 || owned["web/Ingress/www"].ID != 1 || owned["web/Ingress/both"].ID != 6 {
		t.Fatalf("owned = %v", owned)
	}
	if len(leg) != 1 || leg["mailpit/Ingress/mailpit"].ID != 2 {
		t.Fatalf("legacy = %v", leg)
	}
	if len(untagged) != 1 || untagged[0].ID != 4 {
		t.Fatalf("only tagless reconciler names may be adopted, got %v", untagged)
	}
}

func TestClassifyMonitorsWithoutLegacyTag(t *testing.T) {
	t.Parallel()
	owned, leg, _ := classifyMonitors([]monitor.Base{tagged(1, "ns/Ingress/a", "managed-by-uptime-operator")},
		"managed-by-uptime-operator", "")
	if len(owned) != 1 || len(leg) != 0 {
		t.Fatalf("owned=%v legacy=%v", owned, leg)
	}
}

func TestClaimLegacyNeedsNameAndTarget(t *testing.T) {
	t.Parallel()
	leg := map[string]monitor.Base{"mailpit/Ingress/mailpit": tagged(2, "mailpit/Ingress/mailpit", "legacy")}
	sameTarget := func(m monitor.Base) bool { return m.ID == 2 }
	otherTarget := func(monitor.Base) bool { return false }

	if _, ok := claimLegacy(leg, "mailpit/Ingress/mailpit", otherTarget); ok {
		t.Fatal("same key on another cluster's host must not be claimed")
	}
	if _, ok := claimLegacy(leg, "mailpit/Ingress/other", sameTarget); ok {
		t.Fatal("legacy monitors are not reclaimed by target alone")
	}
	if _, ok := claimLegacy(leg, "mailpit/Ingress/mailpit", nil); ok {
		t.Fatal("no matcher must not claim")
	}
	if m, ok := claimLegacy(leg, "mailpit/Ingress/mailpit", sameTarget); !ok || m.ID != 2 {
		t.Fatalf("matching legacy monitor: %v %v", m, ok)
	}
}
