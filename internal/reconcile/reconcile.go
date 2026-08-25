package reconcile

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	kuma "github.com/breml/go-uptime-kuma-client"
	"github.com/breml/go-uptime-kuma-client/monitor"
	"github.com/breml/go-uptime-kuma-client/notification"
	kumtag "github.com/breml/go-uptime-kuma-client/tag"
	"gopkg.in/yaml.v3"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/solid3dlab/uptime-operator/internal/config"
)

// IngressLister is the only Kubernetes API the operator needs.
type IngressLister interface {
	List(ctx context.Context, opts metav1.ListOptions) (*networkingv1.IngressList, error)
}

const (
	annEnabled                = "uptime-kuma.io/monitor"
	annType                   = "uptime-kuma.io/monitor-type"
	annInterval               = "uptime-kuma.io/monitor-interval"
	annGroup                  = "uptime-kuma.io/monitor-group"
	annIgnoreTLS              = "uptime-kuma.io/ignore-tls"
	annPath                   = "uptime-kuma.io/path"
	annAcceptedStatusCodes    = "uptime-kuma.io/accepted-status-codes"
	annMethod                 = "uptime-kuma.io/method"
	annMaxRedirects           = "uptime-kuma.io/max-redirects"
	annTimeout                = "uptime-kuma.io/timeout"
	annRetryInterval          = "uptime-kuma.io/retry-interval"
	annMaxRetries             = "uptime-kuma.io/max-retries"
	annHost                   = "uptime-kuma.io/host"
	annUseDefaultNotification = "uptime-kuma.io/use-default-notification"
	annNotification           = "uptime-kuma.io/notification"
)

// Reconciler syncs annotated Ingresses and static YAML into Uptime Kuma.
type Reconciler struct {
	cfg           config.Config
	k8s           IngressLister
	kuma          *kuma.Client
	log           *slog.Logger
	now           func() time.Time
	tagID         int64
	groups        map[string]int64
	notifications []notification.Base
}

// New builds a reconciler bound to an authenticated Kuma client.
func New(cfg config.Config, k8s IngressLister, client *kuma.Client, log *slog.Logger) *Reconciler {
	if log == nil {
		log = slog.Default()
	}
	return &Reconciler{
		cfg:  cfg,
		k8s:  k8s,
		kuma: client,
		log:  log,
		now:  func() time.Time { return time.Now().UTC() },
	}
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
	r.notifications = r.kuma.GetNotifications(ctx)

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

	ings, err := r.k8s.List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list ingresses: %w", err)
	}
	orphanAnns := map[string]map[string]string{}
	for i := range ings.Items {
		ing := &ings.Items[i]
		key := monitorKey(ing.Namespace, "Ingress", ing.Name)
		if !strings.EqualFold(ing.Annotations[annEnabled], "true") {
			orphanAnns[key] = ing.Annotations
			continue
		}
		seen[key] = struct{}{}
		if err := r.reconcileIngress(ctx, ing, managed); err != nil {
			r.log.Error("reconcile ingress", "key", key, "err", err)
		}
	}

	for key, mon := range managed {
		if _, ok := seen[key]; ok {
			continue
		}
		if err := r.gcOrphan(ctx, key, mon, orphanAnns[key]); err != nil {
			r.log.Error("gc orphan", "key", key, "err", err)
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
	r.groups = make(map[string]int64)
	out := make(map[string]monitor.Base)
	for _, m := range mons {
		if m.Type() == "group" {
			r.groups[m.Name] = m.ID
		}
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

	url, err := extractIngressURL(ing, ing.Annotations[annPath], ing.Annotations[annHost])
	if err != nil {
		return err
	}

	interval := parseInt64(ing.Annotations[annInterval], 60, 20)
	ignoreTLS := parseBool(ing.Annotations[annIgnoreTLS])
	group := ing.Annotations[annGroup]
	parent, err := r.ensureGroup(ctx, group)
	if err != nil {
		return err
	}
	useDefaultNotification := parseBool(ing.Annotations[annUseDefaultNotification])
	notificationIDs, err := resolveNotificationIDs(
		r.notifications,
		useDefaultNotification,
		parseCSV(ing.Annotations[annNotification]),
	)
	if err != nil {
		return err
	}
	if useDefaultNotification && len(notificationIDs) == 0 && strings.TrimSpace(ing.Annotations[annNotification]) == "" {
		r.log.Warn("use-default-notification set but no default Kuma channel exists", "key", key)
	}

	desired := &monitor.HTTP{
		Base: monitor.Base{
			Name:            key,
			Description:     ptrString(formatGCDescription(r.policyFromAnnotations(ing.Annotations))),
			Interval:        interval,
			RetryInterval:   parseInt64(ing.Annotations[annRetryInterval], 60, 1),
			MaxRetries:      parseInt64(ing.Annotations[annMaxRetries], 3, 0),
			IsActive:        true,
			Parent:          parent,
			NotificationIDs: notificationIDs,
		},
		HTTPDetails: monitor.HTTPDetails{
			URL:                      url,
			Method:                   parseMethod(ing.Annotations[annMethod]),
			AcceptedStatusCodes:      parseStatusCodes(ing.Annotations[annAcceptedStatusCodes]),
			MaxRedirects:             parseInt(ing.Annotations[annMaxRedirects], 10),
			IgnoreTLS:                ignoreTLS,
			Timeout:                  parseInt64(ing.Annotations[annTimeout], 48, 1),
			ExpiryNotification:       !ignoreTLS,
			DomainExpiryNotification: !ignoreTLS,
		},
	}

	return r.ensureHTTP(ctx, key, desired, managed)
}

type staticFile struct {
	Monitors []staticMonitor `yaml:"monitors"`
}

type staticMonitor struct {
	Name                   string   `yaml:"name"`
	Type                   string   `yaml:"type"`
	URL                    string   `yaml:"url"`
	Hostname               string   `yaml:"hostname"`
	Port                   int      `yaml:"port"`
	Group                  string   `yaml:"group"`
	Interval               int64    `yaml:"interval"`
	AcceptedStatusCodes    []string `yaml:"accepted_statuscodes"`
	IgnoreTLS              bool     `yaml:"ignore_tls"`
	Method                 string   `yaml:"method"`
	MaxRedirects           int      `yaml:"max_redirects"`
	Timeout                int64    `yaml:"timeout"`
	RetryInterval          int64    `yaml:"retry_interval"`
	MaxRetries             int64    `yaml:"max_retries"`
	UseDefaultNotification bool     `yaml:"use_default_notification"`
	Notification           string   `yaml:"notification"`
	Notifications          []string `yaml:"notifications"`
	DeletePolicy           string   `yaml:"delete_policy"`
	DeleteGrace            string   `yaml:"delete_grace"`
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
		notificationIDs, err := resolveNotificationIDs(
			r.notifications,
			entry.UseDefaultNotification,
			append(parseCSV(entry.Notification), entry.Notifications...),
		)
		if err != nil {
			r.log.Error("static notification", "name", entry.Name, "err", err)
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
			redirects := entry.MaxRedirects
			if redirects == 0 {
				redirects = 10
			}
			retry := entry.RetryInterval
			if retry == 0 {
				retry = 60
			}
			retries := entry.MaxRetries
			if retries == 0 {
				retries = 3
			}
			timeout := entry.Timeout
			if timeout == 0 {
				timeout = 48
			}
			desired := &monitor.HTTP{
				Base: monitor.Base{
					Name: key, Description: ptrString(formatGCDescription(r.policyFromStatic(entry))),
					Interval: interval, RetryInterval: retry, MaxRetries: retries, IsActive: true, Parent: parent,
					NotificationIDs: notificationIDs,
				},
				HTTPDetails: monitor.HTTPDetails{
					URL: entry.URL, Method: parseMethod(entry.Method),
					AcceptedStatusCodes: codes, MaxRedirects: redirects, IgnoreTLS: entry.IgnoreTLS,
					Timeout: timeout, ExpiryNotification: !entry.IgnoreTLS, DomainExpiryNotification: !entry.IgnoreTLS,
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
					Name: key, Description: ptrString(formatGCDescription(r.policyFromStatic(entry))),
					Interval: interval, RetryInterval: 60, MaxRetries: 3, IsActive: true, Parent: parent,
					NotificationIDs: notificationIDs,
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
					Name: key, Description: ptrString(formatGCDescription(r.policyFromStatic(entry))),
					Interval: interval, RetryInterval: 60, MaxRetries: 3, IsActive: true, Parent: parent,
					NotificationIDs: notificationIDs,
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
		if existing.Interval == desired.Interval {
			return nil
		}
	} else if !httpNeedsUpdate(cur, existing, desired) {
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
	if cur.Hostname == desired.Hostname && existing.Interval == desired.Interval &&
		notificationIDsEqual(existing.NotificationIDs, desired.NotificationIDs) &&
		descriptionEqual(existing.Description, desired.Description) {
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
	if cur.Hostname == desired.Hostname && cur.Port == desired.Port && existing.Interval == desired.Interval &&
		notificationIDsEqual(existing.NotificationIDs, desired.NotificationIDs) &&
		descriptionEqual(existing.Description, desired.Description) {
		return nil
	}
	desired.ID = existing.ID
	return r.kuma.UpdateMonitor(ctx, desired)
}

func (r *Reconciler) ensureGroup(ctx context.Context, name string) (*int64, error) {
	if name == "" {
		return nil, nil
	}
	if id, ok := r.groups[name]; ok {
		return &id, nil
	}
	id, err := r.kuma.CreateMonitor(ctx, &monitor.Group{
		Base: monitor.Base{Name: name, IsActive: true, Interval: 60, RetryInterval: 60, MaxRetries: 3},
	})
	if err != nil {
		return nil, fmt.Errorf("create group %q: %w", name, err)
	}
	if r.groups == nil {
		r.groups = make(map[string]int64)
	}
	r.groups[name] = id
	r.log.Info("created monitor group", "name", name, "id", id)
	return &id, nil
}

func monitorKey(ns, kind, name string) string {
	return fmt.Sprintf("%s/%s/%s", ns, kind, name)
}

func extractIngressURL(ing *networkingv1.Ingress, path, preferredHost string) (string, error) {
	preferredHost = strings.TrimSpace(preferredHost)
	tlsHosts := map[string]struct{}{}
	for _, t := range ing.Spec.TLS {
		for _, h := range t.Hosts {
			tlsHosts[h] = struct{}{}
		}
	}
	urlFor := func(host string) string {
		scheme := "http"
		if _, ok := tlsHosts[host]; ok {
			scheme = "https"
		}
		return joinURL(scheme+"://"+host, path)
	}
	if preferredHost != "" {
		for _, rule := range ing.Spec.Rules {
			if rule.Host == preferredHost {
				return urlFor(preferredHost), nil
			}
		}
		return "", fmt.Errorf("host %q not on ingress", preferredHost)
	}
	for _, rule := range ing.Spec.Rules {
		if rule.Host == "" {
			continue
		}
		return urlFor(rule.Host), nil
	}
	return "", fmt.Errorf("no host on ingress")
}

func joinURL(base, path string) string {
	path = strings.TrimSpace(path)
	if path == "" || path == "/" {
		return base
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return strings.TrimRight(base, "/") + path
}

func parseBool(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func parseMethod(raw string) string {
	m := strings.ToUpper(strings.TrimSpace(raw))
	switch m {
	case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS":
		return m
	default:
		return "GET"
	}
}

func parseStatusCodes(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return []string{"200-299"}
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return []string{"200-299"}
	}
	return out
}

func parseInt(raw string, fallback int) int {
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 0 {
		return fallback
	}
	return v
}

func httpNeedsUpdate(cur monitor.HTTP, existing monitor.Base, desired *monitor.HTTP) bool {
	return cur.URL != desired.URL ||
		existing.Interval != desired.Interval ||
		existing.RetryInterval != desired.RetryInterval ||
		existing.MaxRetries != desired.MaxRetries ||
		cur.Timeout != desired.Timeout ||
		cur.IgnoreTLS != desired.IgnoreTLS ||
		cur.ExpiryNotification != desired.ExpiryNotification ||
		cur.DomainExpiryNotification != desired.DomainExpiryNotification ||
		cur.Method != desired.Method ||
		cur.MaxRedirects != desired.MaxRedirects ||
		!slices.Equal(cur.AcceptedStatusCodes, desired.AcceptedStatusCodes) ||
		!notificationIDsEqual(existing.NotificationIDs, desired.NotificationIDs) ||
		!descriptionEqual(existing.Description, desired.Description)
}

func parseInt64(raw string, fallback, min int64) int64 {
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < min {
		return fallback
	}
	return v
}

func parseCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// resolveNotificationIDs builds the monitor notificationIDList.
// useDefault enables whatever channels Kuma has marked Default (no name needed).
// names are extra channels looked up by display name.
func resolveNotificationIDs(notifs []notification.Base, useDefault bool, names []string) ([]int64, error) {
	ids := make([]int64, 0, len(notifs))
	seen := make(map[int64]struct{}, len(notifs))
	add := func(id int64) {
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if useDefault {
		for _, id := range defaultNotificationIDs(notifs) {
			add(id)
		}
	}

	var missing []string
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		id, ok := notificationIDByName(notifs, name)
		if !ok {
			missing = append(missing, name)
			continue
		}
		add(id)
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("notification channel(s) not found: %s", strings.Join(missing, ", "))
	}
	slices.Sort(ids)
	return ids, nil
}

func defaultNotificationIDs(notifs []notification.Base) []int64 {
	var ids []int64
	for _, n := range notifs {
		if n.IsActive && n.IsDefault {
			ids = append(ids, n.ID)
		}
	}
	return ids
}

func notificationIDByName(notifs []notification.Base, name string) (int64, bool) {
	want := strings.ToLower(name)
	for _, n := range notifs {
		if !n.IsActive {
			continue
		}
		if strings.ToLower(n.Name) == want {
			return n.ID, true
		}
	}
	return 0, false
}

func notificationIDsEqual(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	as := slices.Clone(a)
	bs := slices.Clone(b)
	slices.Sort(as)
	slices.Sort(bs)
	return slices.Equal(as, bs)
}

func hasTag(tags []kumtag.MonitorTag, name string) bool {
	for _, t := range tags {
		if t.Name == name {
			return true
		}
	}
	return false
}
