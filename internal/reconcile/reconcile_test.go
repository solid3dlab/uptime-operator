package reconcile

import (
	"testing"

	"github.com/breml/go-uptime-kuma-client/monitor"
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
}
