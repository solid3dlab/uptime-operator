package reconcile

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	kuma "github.com/breml/go-uptime-kuma-client"
	"github.com/breml/go-uptime-kuma-client/monitor"
	kumtag "github.com/breml/go-uptime-kuma-client/tag"
	"gopkg.in/yaml.v3"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/solid3dlab/uptime-operator/internal/config"
)

const (
	annEnabled  = "uptime-kuma.io/monitor"
	annType     = "uptime-kuma.io/monitor-type"
	annInterval = "uptime-kuma.io/monitor-interval"
	annGroup    = "uptime-kuma.io/monitor-group"
)

// Reconciler syncs annotated Ingresses and static YAML into Uptime Kuma.
type Reconciler struct {
	cfg    config.Config
	k8s    kubernetes.Interface
	kuma   *kuma.Client
	log    *slog.Logger
	tagID  int64
}

// New builds a reconciler bound to an authenticated Kuma client.
func New(cfg config.Config, k8s kubernetes.Interface, client *kuma.Client, log *slog.Logger) *Reconciler {
	if log == nil {
		log = slog.Default()
	}
	return &Reconciler{cfg: cfg, k8s: k8s, kuma: client, log: log}
}

// EnsureManagedTag finds or creates the ownership tag used for GC.
func (r *Reconciler) EnsureManagedTag(ctx context.Context) error {
	tags, err := r.kuma.GetTags(ctx)
	if err != nil {
		return fmt.Errorf("list tags: %w", err)
	}
	for _, t := range tags {
		if t.Name == r.cfg.ManagedTag {
			r.tagID = t.ID
			r.log.Info("using managed tag", "id", t.ID, "name", t.Name)
			return nil
		}
	}
	id, err := r.kuma.CreateTag(ctx, kumtag.Tag{
		Name:  r.cfg.ManagedTag,
		Color: r.cfg.ManagedTagColor,
	})
	if err != nil {
		return fmt.Errorf("create tag: %w", err)
	}
	r.tagID = id
	r.log.Info("created managed tag", "id", id, "name", r.cfg.ManagedTag)
	return nil
}

// ReconcileOnce performs one full sync cycle.
func (r *Reconciler) ReconcileOnce(ctx context.Context) error {
	r.log.Info("starting reconciliation")

	managed, err := r.managedMonitors(ctx)
	if err != nil {
		return err
	}
	seen := map[string]struct{}{}

	staticKeys, err := r.reconcileStatic(ctx, managed)
	if err != nil {
		r.log.Error("static monitors", "err", err)
	}
	for k := range staticKeys {
		seen[k] = struct{}{}
	}

	ings, err := r.k8s.NetworkingV1().Ingresses("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list ingresses: %w", err)
	}
	for i := range ings.Items {
		ing := &ings.Items[i]
		key := monitorKey(ing.Namespace, "Ingress", ing.Name)
		seen[key] = struct{}{}
		if err := r.reconcileIngress(ctx, ing, managed); err != nil {
			r.log.Error("reconcile ingress", "key", key, "err", err)
		}
	}

	for key, mon := range managed {
		if _, ok := seen[key]; ok {
			continue
		}
		r.log.Info("deleting orphan monitor", "key", key, "id", mon.ID)
		if err := r.kuma.DeleteMonitor(ctx, mon.ID); err != nil {
			r.log.Error("delete orphan", "key", key, "err", err)
		}
	}

	r.log.Info("reconciliation complete",
		"ingresses", len(ings.Items),
		"static", len(staticKeys),
		"seen", len(seen),
	)
	return nil
}

func (r *Reconciler) managedMonitors(ctx context.Context) (map[string]monitor.Base, error) {
	mons, err := r.kuma.GetMonitors(ctx)
	if err != nil {
		return nil, fmt.Errorf("get monitors: %w", err)
	}
	out := make(map[string]monitor.Base)
	for _, m := range mons {
		if hasTag(m.Tags, r.cfg.ManagedTag) {
			out[m.Name] = m
			continue
		}
		// Adopt reconciler-style names that lost their tag (crash between add+tag).
		if r.tagID != 0 && strings.Contains(m.Name, "/") {
			r.log.Info("adopting untagged monitor", "name", m.Name, "id", m.ID)
			if _, err := r.kuma.AddMonitorTag(ctx, r.tagID, m.ID, ""); err != nil {
				r.log.Warn("adopt failed", "name", m.Name, "err", err)
				continue
			}
			out[m.Name] = m
		}
	}
	return out, nil
}

func (r *Reconciler) reconcileIngress(ctx context.Context, ing *networkingv1.Ingress, managed map[string]monitor.Base) error {
	key := monitorKey(ing.Namespace, "Ingress", ing.Name)
	enabled := strings.EqualFold(ing.Annotations[annEnabled], "true")

	if !enabled {
		if existing, ok := managed[key]; ok {
			r.log.Info("removing monitor (annotation off)", "key", key)
			return r.kuma.DeleteMonitor(ctx, existing.ID)
		}
		return nil
	}

	url := extractIngressURL(ing)
	if url == "" {
		return fmt.Errorf("no host on ingress")
	}

	interval := parseInterval(ing.Annotations[annInterval], 60)
	group := ing.Annotations[annGroup]
	parent, err := r.ensureGroup(ctx, group)
	if err != nil {
		return err
	}

	desired := &monitor.HTTP{
		Base: monitor.Base{
			Name:          key,
			Interval:      interval,
			RetryInterval: 60,
			MaxRetries:    3,
			IsActive:      true,
			Parent:        parent,
		},
		HTTPDetails: monitor.HTTPDetails{
			URL:                 url,
			Method:              "GET",
			AcceptedStatusCodes: []string{"200-299"},
			MaxRedirects:        10,
		},
	}

	return r.ensureHTTP(ctx, key, desired, managed)
}

type staticFile struct {
	Monitors []staticMonitor `yaml:"monitors"`
}

type staticMonitor struct {
	Name                 string   `yaml:"name"`
	Type                 string   `yaml:"type"`
	URL                  string   `yaml:"url"`
	Hostname             string   `yaml:"hostname"`
	Port                 int      `yaml:"port"`
	Group                string   `yaml:"group"`
	Interval             int64    `yaml:"interval"`
	AcceptedStatusCodes  []string `yaml:"accepted_statuscodes"`
}

func (r *Reconciler) reconcileStatic(ctx context.Context, managed map[string]monitor.Base) (map[string]struct{}, error) {
	seen := map[string]struct{}{}
	raw, err := os.ReadFile(r.cfg.StaticMonitorsPath)
	if err != nil {
		if os.IsNotExist(err) {
			r.log.Info("no static monitors file", "path", r.cfg.StaticMonitorsPath)
			return seen, nil
		}
		return seen, err
	}
	var file staticFile
	if err := yaml.Unmarshal(raw, &file); err != nil {
		return seen, fmt.Errorf("parse static monitors: %w", err)
	}

	for _, entry := range file.Monitors {
		if entry.Name == "" {
			continue
		}
		key := "static/" + entry.Name
		seen[key] = struct{}{}
		interval := entry.Interval
		if interval == 0 {
			interval = 60
		}
		parent, err := r.ensureGroup(ctx, entry.Group)
		if err != nil {
			r.log.Error("static group", "name", entry.Name, "err", err)
			continue
		}
		typ := strings.ToLower(entry.Type)
		if typ == "" {
			typ = "http"
		}
		switch typ {
		case "http":
			codes := entry.AcceptedStatusCodes
			if len(codes) == 0 {
				codes = []string{"200-299"}
			}
			desired := &monitor.HTTP{
				Base: monitor.Base{
					Name: key, Interval: interval, RetryInterval: 60, MaxRetries: 3, IsActive: true, Parent: parent,
				},
				HTTPDetails: monitor.HTTPDetails{
					URL: entry.URL, Method: "GET", AcceptedStatusCodes: codes, MaxRedirects: 10,
				},
			}
			if entry.URL == "" {
				r.log.Warn("static http missing url", "name", entry.Name)
				continue
			}
			if err := r.ensureHTTP(ctx, key, desired, managed); err != nil {
				r.log.Error("static http", "key", key, "err", err)
			}
		case "ping":
			desired := &monitor.Ping{
				Base: monitor.Base{
					Name: key, Interval: interval, RetryInterval: 60, MaxRetries: 3, IsActive: true, Parent: parent,
				},
				PingDetails: monitor.PingDetails{Hostname: entry.Hostname},
			}
			if entry.Hostname == "" {
				r.log.Warn("static ping missing hostname", "name", entry.Name)
				continue
			}
			if err := r.ensurePing(ctx, key, desired, managed); err != nil {
				r.log.Error("static ping", "key", key, "err", err)
			}
		case "port":
			desired := &monitor.TCPPort{
				Base: monitor.Base{
					Name: key, Interval: interval, RetryInterval: 60, MaxRetries: 3, IsActive: true, Parent: parent,
				},
				TCPPortDetails: monitor.TCPPortDetails{
					Hostname: entry.Hostname,
					Port:     entry.Port,
				},
			}
			if entry.Hostname == "" || entry.Port == 0 {
				r.log.Warn("static port missing hostname/port", "name", entry.Name)
				continue
			}
			if err := r.ensurePort(ctx, key, desired, managed); err != nil {
				r.log.Error("static port", "key", key, "err", err)
			}
		default:
			r.log.Warn("unsupported static monitor type", "type", typ, "name", entry.Name)
		}
	}
	return seen, nil
}

func (r *Reconciler) ensureHTTP(ctx context.Context, key string, desired *monitor.HTTP, managed map[string]monitor.Base) error {
	existing, ok := managed[key]
	if !ok {
		r.log.Info("creating monitor", "key", key, "url", desired.URL)
		id, err := r.kuma.CreateMonitor(ctx, desired)
		if err != nil {
			return err
		}
		_, err = r.kuma.AddMonitorTag(ctx, r.tagID, id, "")
		return err
	}

	var cur monitor.HTTP
	if err := existing.As(&cur); err != nil {
		// Fall back to recreate-on-mismatch using base fields only.
		if existing.Interval == desired.Interval {
			return nil
		}
	} else if cur.URL == desired.URL && existing.Interval == desired.Interval {
		return nil
	}

	desired.ID = existing.ID
	r.log.Info("updating monitor", "key", key, "url", desired.URL)
	return r.kuma.UpdateMonitor(ctx, desired)
}

func (r *Reconciler) ensurePing(ctx context.Context, key string, desired *monitor.Ping, managed map[string]monitor.Base) error {
	existing, ok := managed[key]
	if !ok {
		r.log.Info("creating ping monitor", "key", key, "hostname", desired.Hostname)
		id, err := r.kuma.CreateMonitor(ctx, desired)
		if err != nil {
			return err
		}
		_, err = r.kuma.AddMonitorTag(ctx, r.tagID, id, "")
		return err
	}
	var cur monitor.Ping
	_ = existing.As(&cur)
	if cur.Hostname == desired.Hostname && existing.Interval == desired.Interval {
		return nil
	}
	desired.ID = existing.ID
	return r.kuma.UpdateMonitor(ctx, desired)
}

func (r *Reconciler) ensurePort(ctx context.Context, key string, desired *monitor.TCPPort, managed map[string]monitor.Base) error {
	existing, ok := managed[key]
	if !ok {
		r.log.Info("creating port monitor", "key", key, "hostname", desired.Hostname, "port", desired.Port)
		id, err := r.kuma.CreateMonitor(ctx, desired)
		if err != nil {
			return err
		}
		_, err = r.kuma.AddMonitorTag(ctx, r.tagID, id, "")
		return err
	}
	var cur monitor.TCPPort
	_ = existing.As(&cur)
	if cur.Hostname == desired.Hostname && cur.Port == desired.Port && existing.Interval == desired.Interval {
		return nil
	}
	desired.ID = existing.ID
	return r.kuma.UpdateMonitor(ctx, desired)
}

func (r *Reconciler) ensureGroup(ctx context.Context, name string) (*int64, error) {
	if name == "" {
		return nil, nil
	}
	mons, err := r.kuma.GetMonitors(ctx)
	if err != nil {
		return nil, err
	}
	for _, m := range mons {
		if m.Type() == "group" && m.Name == name {
			id := m.ID
			return &id, nil
		}
	}
	id, err := r.kuma.CreateMonitor(ctx, &monitor.Group{
		Base: monitor.Base{Name: name, IsActive: true, Interval: 60, RetryInterval: 60, MaxRetries: 3},
	})
	if err != nil {
		return nil, fmt.Errorf("create group %q: %w", name, err)
	}
	r.log.Info("created monitor group", "name", name, "id", id)
	return &id, nil
}

func monitorKey(ns, kind, name string) string {
	return fmt.Sprintf("%s/%s/%s", ns, kind, name)
}

func extractIngressURL(ing *networkingv1.Ingress) string {
	tlsHosts := map[string]struct{}{}
	for _, t := range ing.Spec.TLS {
		for _, h := range t.Hosts {
			tlsHosts[h] = struct{}{}
		}
	}
	for _, rule := range ing.Spec.Rules {
		if rule.Host == "" {
			continue
		}
		scheme := "http"
		if _, ok := tlsHosts[rule.Host]; ok {
			scheme = "https"
		}
		return scheme + "://" + rule.Host
	}
	return ""
}

func parseInterval(raw string, fallback int64) int64 {
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 20 {
		return fallback
	}
	return v
}

func hasTag(tags []kumtag.MonitorTag, name string) bool {
	for _, t := range tags {
		if t.Name == name {
			return true
		}
	}
	return false
}
