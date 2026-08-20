package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	kuma "github.com/breml/go-uptime-kuma-client"
	networkingclient "k8s.io/client-go/kubernetes/typed/networking/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/solid3dlab/uptime-operator/internal/config"
	"github.com/solid3dlab/uptime-operator/internal/reconcile"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(os.Getenv("LOG_LEVEL")),
	}))
	slog.SetDefault(log)

	cfg, err := config.FromEnv()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	ings, err := newIngressLister()
	if err != nil {
		log.Error("kubernetes client", "err", err)
		os.Exit(1)
	}

	for {
		if ctx.Err() != nil {
			break
		}
		wait := cfg.ResyncInterval
		if err := runOnce(ctx, cfg, ings, log); err != nil {
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

func runOnce(ctx context.Context, cfg config.Config, ings reconcile.IngressLister, log *slog.Logger) error {
	log.Info("connecting to uptime kuma", "url", cfg.KumaURL)
	client, err := kuma.New(ctx, cfg.KumaURL, cfg.KumaUsername, cfg.KumaPassword)
	if err != nil {
		return err
	}
	defer func() {
		_ = client.Disconnect()
		// Return idle heap to the OS so RSS drops between 5-minute syncs.
		debug.FreeOSMemory()
	}()

	rec := reconcile.New(cfg, ings, client, log)
	if err := rec.EnsureManagedTag(ctx); err != nil {
		return err
	}
	return rec.ReconcileOnce(ctx)
}

func newIngressLister() (reconcile.IngressLister, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
		kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{})
		cfg, err = kubeConfig.ClientConfig()
		if err != nil {
			return nil, err
		}
	}
	client, err := networkingclient.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return client.Ingresses(""), nil
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
