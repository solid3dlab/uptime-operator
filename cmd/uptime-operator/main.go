package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	kuma "github.com/breml/go-uptime-kuma-client"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	networkingclient "k8s.io/client-go/kubernetes/typed/networking/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/solid3dlab/uptime-operator/internal/config"
	"github.com/solid3dlab/uptime-operator/internal/reconcile"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(os.Getenv("LOG_LEVEL")),
	}))
	slog.SetDefault(log)

	if limit, ok := applyCgroupMemLimit(); ok {
		log.Info("memory limit", "gomemlimit_bytes", limit)
	}

	cfg, err := config.FromEnv()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	kubeCfg, err := kubeConfig()
	if err != nil {
		log.Error("kubernetes client", "err", err)
		os.Exit(1)
	}
	ings, err := newIngressLister(kubeCfg)
	if err != nil {
		log.Error("kubernetes client", "err", err)
		os.Exit(1)
	}
	routes, err := newHTTPRouteLister(kubeCfg)
	if err != nil {
		log.Error("kubernetes client", "err", err)
		os.Exit(1)
	}

	for {
		if ctx.Err() != nil {
			break
		}
		wait := cfg.ResyncInterval
		if err := runOnce(ctx, cfg, ings, routes, log); err != nil {
			log.Error("reconcile", "err", err)
			wait = 30 * time.Second
		}
		select {
		case <-ctx.Done():
		case <-time.After(wait):
		}
	}
	log.Info("shutting down")
}

// connectTimeout bounds the Socket.IO handshake. The process ctx only
// cancels on SIGTERM, so a stalled Kuma websocket would otherwise hang
// the loop forever.
const connectTimeout = 45 * time.Second

// reconcileTimeout covers tag lookup plus monitor upsert after connect.
const reconcileTimeout = 2 * time.Minute

func runOnce(ctx context.Context, cfg config.Config, ings reconcile.IngressLister, routes reconcile.HTTPRouteLister, log *slog.Logger) error {
	runCtx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()

	log.Info("connecting to uptime kuma", "url", cfg.KumaURL)
	client, err := kuma.New(runCtx, cfg.KumaURL, cfg.KumaUsername, cfg.KumaPassword, kuma.WithConnectTimeout(connectTimeout))
	if err != nil {
		return err
	}
	defer func() {
		_ = client.Disconnect()
		// Return idle heap to the OS so RSS drops between 5-minute syncs.
		debug.FreeOSMemory()
	}()

	rec := reconcile.New(cfg, ings, routes, client, log)
	if err := rec.EnsureManagedTag(runCtx); err != nil {
		return err
	}
	return rec.ReconcileOnce(runCtx)
}

func kubeConfig() (*rest.Config, error) {
	cfg, err := rest.InClusterConfig()
	if err == nil {
		return cfg, nil
	}
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	kube := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{})
	return kube.ClientConfig()
}

func newIngressLister(cfg *rest.Config) (reconcile.IngressLister, error) {
	client, err := networkingclient.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return client.Ingresses(""), nil
}

var httpRouteGVR = schema.GroupVersionResource{
	Group:    gatewayv1.GroupVersion.Group,
	Version:  gatewayv1.GroupVersion.Version,
	Resource: "httproutes",
}

type httpRouteLister struct {
	client dynamic.NamespaceableResourceInterface
}

func newHTTPRouteLister(cfg *rest.Config) (reconcile.HTTPRouteLister, error) {
	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &httpRouteLister{client: client.Resource(httpRouteGVR)}, nil
}

func (l *httpRouteLister) List(ctx context.Context, opts metav1.ListOptions) (*gatewayv1.HTTPRouteList, error) {
	raw, err := l.client.List(ctx, opts)
	if err != nil {
		return nil, err
	}
	data, err := raw.MarshalJSON()
	if err != nil {
		return nil, err
	}
	var out gatewayv1.HTTPRouteList
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// applyCgroupMemLimit sets GOMEMLIMIT to 90% of the container memory
// cap when the process did not set GOMEMLIMIT itself. This keeps the
// Go heap from growing to the cgroup OOM threshold between GCs.
func applyCgroupMemLimit() (int64, bool) {
	if os.Getenv("GOMEMLIMIT") != "" {
		return 0, false
	}
	for _, path := range []string{
		"/sys/fs/cgroup/memory.max",
		"/sys/fs/cgroup/memory/memory.limit_in_bytes",
	} {
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		s := strings.TrimSpace(string(raw))
		if s == "" || s == "max" {
			continue
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n <= 0 || n > 1<<40 {
			continue
		}
		limit := n * 90 / 100
		debug.SetMemoryLimit(limit)
		return limit, true
	}
	return 0, false
}

func parseLogLevel(v string) slog.Level {
	switch v {
	case "DEBUG", "debug":
		return slog.LevelDebug
	case "WARN", "warn", "WARNING", "warning":
		return slog.LevelWarn
	case "ERROR", "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
