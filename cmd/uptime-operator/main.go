package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	kuma "github.com/breml/go-uptime-kuma-client"
	"k8s.io/client-go/kubernetes"
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

	k8s, err := newK8sClient()
	if err != nil {
		log.Error("kubernetes client", "err", err)
		os.Exit(1)
	}

	for {
		if ctx.Err() != nil {
			break
		}
		if err := runLoop(ctx, cfg, k8s, log); err != nil {
			log.Error("operator loop ended", "err", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(30 * time.Second):
		}
	}
	log.Info("shutting down")
}

func runLoop(ctx context.Context, cfg config.Config, k8s kubernetes.Interface, log *slog.Logger) error {
	log.Info("connecting to uptime kuma", "url", cfg.KumaURL)
	client, err := kuma.New(ctx, cfg.KumaURL, cfg.KumaUsername, cfg.KumaPassword)
	if err != nil {
		return err
	}
	defer client.Disconnect()

	rec := reconcile.New(cfg, k8s, client, log)
	if err := rec.EnsureManagedTag(ctx); err != nil {
		return err
	}

	ticker := time.NewTicker(cfg.ResyncInterval)
	defer ticker.Stop()

	// Immediate first pass.
	if err := rec.ReconcileOnce(ctx); err != nil {
		log.Error("reconcile", "err", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := rec.ReconcileOnce(ctx); err != nil {
				log.Error("reconcile", "err", err)
			}
		}
	}
}

func newK8sClient() (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
		kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{})
		cfg, err = kubeConfig.ClientConfig()
		if err != nil {
			return nil, err
		}
	}
	return kubernetes.NewForConfig(cfg)
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
