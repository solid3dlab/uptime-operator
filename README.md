# uptime-operator

A tiny Kubernetes controller that keeps [Uptime Kuma](https://github.com/louislam/uptime-kuma)
HTTP monitors in sync with annotated Ingresses.

Designed for lean clusters: single static Go binary on `scratch`, no pip, no
runtime package installs. Typical footprint is tens of MiB RAM.

## How it works

```text
┌─────────────┐  list Ingresses   ┌──────────────────┐
│ Kubernetes  │ ─────────────────►│  uptime-operator │
│ API server  │                   │  (this binary)   │
└─────────────┘                   └────────┬─────────┘
                                           │ Socket.IO
                                           ▼
                                  ┌──────────────────┐
                                  │   Uptime Kuma    │
                                  │  create/update/  │
                                  │  delete monitors │
                                  └──────────────────┘
```

1. Every `RESYNC_INTERVAL` seconds (default 300), the operator:
   - lists all cluster Ingresses
   - loads optional static monitors from `/config/monitors.yaml`
   - connects to Uptime Kuma over Socket.IO and logs in
2. For each Ingress with `uptime-kuma.io/monitor: "true"`, it ensures a
   monitor named `namespace/Ingress/name` exists with the right URL/interval.
3. Monitors it owns are tagged `managed-by-uptime-operator`. Manual monitors
   without that tag are never touched.
4. If an Ingress loses the annotation or is deleted, the matching managed
   monitor is removed.

There are **no CRDs** in v1 — configuration is annotations + a ConfigMap.
That keeps RBAC tiny (read Ingresses only) and the control loop easy to reason
about.

## Annotations

| Annotation | Default | Meaning |
|---|---|---|
| `uptime-kuma.io/monitor` | — | `"true"` to manage |
| `uptime-kuma.io/monitor-interval` | `60` | check interval (seconds) |
| `uptime-kuma.io/monitor-group` | — | Kuma group name (created if missing) |
| `uptime-kuma.io/monitor-type` | `http` | reserved; Ingress path is HTTP |

## Environment

| Variable | Required | Description |
|---|---|---|
| `KUMA_URL` | yes | Kuma base URL (no trailing slash) |
| `KUMA_USERNAME` | yes | login user |
| `KUMA_PASSWORD` | yes | login password |
| `RESYNC_INTERVAL` | no | seconds between full syncs (default `300`) |
| `STATIC_MONITORS_PATH` | no | default `/config/monitors.yaml` |
| `LOG_LEVEL` | no | `DEBUG` / `INFO` / `WARN` / `ERROR` |

## Build

```bash
make build          # local binary → bin/uptime-operator
make image          # docker → ghcr.io/solid3dlab/uptime-operator:<git-sha>
```

Image: multi-stage, `CGO_ENABLED=0`, final stage `scratch` + CA certs, runs as
UID 65532.

## Deploy

Manifests live in the cluster repo under
`infrastructure/monitoring/uptime-operator/`. Credentials come from a
1Password item (`url` / `username` / `password`).
