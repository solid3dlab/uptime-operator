package reconcile

import (
	"context"
	"testing"

	"github.com/breml/go-uptime-kuma-client/monitor"
	"github.com/breml/go-uptime-kuma-client/notification"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestJoinURL(t *testing.T) {
	t.Parallel()
	cases := []struct {
		base, path, want string
	}{
		{"https://example.com", "", "https://example.com"},
		{"https://example.com", "/", "https://example.com"},
		{"https://example.com", "/up", "https://example.com/up"},
		{"https://example.com", "up", "https://example.com/up"},
		{"https://example.com/", "/healthz", "https://example.com/healthz"},
	}
	for _, tc := range cases {
		if got := joinURL(tc.base, tc.path); got != tc.want {
			t.Fatalf("joinURL(%q, %q) = %q, want %q", tc.base, tc.path, got, tc.want)
		}
	}
}

func TestParseHelpers(t *testing.T) {
	t.Parallel()
	if !parseBool("true") || !parseBool("YES") || parseBool("false") {
		t.Fatal("parseBool")
	}
	if parseMethod("") != "GET" || parseMethod("head") != "HEAD" || parseMethod("nope") != "GET" {
		t.Fatal("parseMethod")
	}
	codes := parseStatusCodes("200-299, 404")
	if len(codes) != 2 || codes[0] != "200-299" || codes[1] != "404" {
		t.Fatalf("parseStatusCodes: %#v", codes)
	}
	if parseInt("5", 10) != 5 || parseInt("x", 10) != 10 {
		t.Fatal("parseInt")
	}
	if parseInt64("15", 48, 1) != 15 || parseInt64("0", 48, 1) != 48 {
		t.Fatal("parseInt64")
	}
	names := parseCSV(" Slack, PagerDuty , ")
	if len(names) != 2 || names[0] != "Slack" || names[1] != "PagerDuty" {
		t.Fatalf("parseCSV: %#v", names)
	}
	if parseCSV("") != nil || parseCSV("  ") != nil {
		t.Fatal("parseCSV empty")
	}
}

func TestExtractIngressURL(t *testing.T) {
	t.Parallel()
	ing := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "ns"},
		Spec: networkingv1.IngressSpec{
			TLS: []networkingv1.IngressTLS{{Hosts: []string{"app.example.com", "www.example.com"}}},
			Rules: []networkingv1.IngressRule{
				{Host: "app.example.com"},
				{Host: "www.example.com"},
			},
		},
	}
	got, err := extractIngressURL(ing, "/up", "")
	if err != nil || got != "https://app.example.com/up" {
		t.Fatalf("first host: %q %v", got, err)
	}
	got, err = extractIngressURL(ing, "/up", "www.example.com")
	if err != nil || got != "https://www.example.com/up" {
		t.Fatalf("preferred host: %q %v", got, err)
	}
	if _, err = extractIngressURL(ing, "/", "missing.example.com"); err == nil {
		t.Fatal("expected error for unknown host")
	}
}

func TestHTTPNeedsUpdate(t *testing.T) {
	t.Parallel()
	desired := &monitor.HTTP{
		Base: monitor.Base{Interval: 60},
		HTTPDetails: monitor.HTTPDetails{
			URL: "https://a.example", Method: "GET", IgnoreTLS: true,
			MaxRedirects: 10, AcceptedStatusCodes: []string{"200-299"},
		},
	}
	cur := monitor.HTTP{
		HTTPDetails: monitor.HTTPDetails{
			URL: "https://a.example", Method: "GET", IgnoreTLS: false,
			MaxRedirects: 10, AcceptedStatusCodes: []string{"200-299"},
		},
	}
	existing := monitor.Base{Interval: 60}
	if !httpNeedsUpdate(cur, existing, desired) {
		t.Fatal("expected update when IgnoreTLS differs")
	}
	cur.IgnoreTLS = true
	if httpNeedsUpdate(cur, existing, desired) {
		t.Fatal("expected no update when fields match")
	}

	desired.ExpiryNotification = false
	desired.DomainExpiryNotification = false
	cur.ExpiryNotification = true
	if !httpNeedsUpdate(cur, existing, desired) {
		t.Fatal("expected update when expiry notification differs")
	}

	cur.ExpiryNotification = false
	desired.NotificationIDs = []int64{2, 1}
	existing.NotificationIDs = []int64{1}
	if !httpNeedsUpdate(cur, existing, desired) {
		t.Fatal("expected update when notification IDs differ")
	}
	existing.NotificationIDs = []int64{1, 2}
	if httpNeedsUpdate(cur, existing, desired) {
		t.Fatal("expected no update when notification IDs match in any order")
	}
}

func TestResolveNotificationIDs(t *testing.T) {
	t.Parallel()
	notifs := []notification.Base{
		{ID: 1, Name: "Slack", IsActive: true, IsDefault: true},
		{ID: 2, Name: "PagerDuty", IsActive: true},
		{ID: 3, Name: "Quiet", IsActive: false, IsDefault: true},
	}

	ids, err := resolveNotificationIDs(notifs, true, nil)
	if err != nil || len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("default only: %#v %v", ids, err)
	}

	ids, err = resolveNotificationIDs(notifs, false, []string{"pagerduty"})
	if err != nil || len(ids) != 1 || ids[0] != 2 {
		t.Fatalf("named only: %#v %v", ids, err)
	}

	ids, err = resolveNotificationIDs(notifs, true, []string{"PagerDuty", "Slack"})
	if err != nil || len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("union: %#v %v", ids, err)
	}

	if _, err = resolveNotificationIDs(notifs, false, []string{"Missing"}); err == nil {
		t.Fatal("expected missing channel error")
	}

	ids, err = resolveNotificationIDs(notifs, false, nil)
	if err != nil || len(ids) != 0 {
		t.Fatalf("none: %#v %v", ids, err)
	}
}

func TestEnsureGroupUsesCache(t *testing.T) {
	t.Parallel()
	r := &Reconciler{groups: map[string]int64{"prod": 7}}
	id, err := r.ensureGroup(context.Background(), "prod")
	if err != nil {
		t.Fatal(err)
	}
	if id == nil || *id != 7 {
		t.Fatalf("cached group: %v", id)
	}
	id, err = r.ensureGroup(context.Background(), "")
	if err != nil || id != nil {
		t.Fatalf("empty group should be a no-op: %v %v", id, err)
	}
}
