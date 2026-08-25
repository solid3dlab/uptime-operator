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
   - connects to Uptime Kuma over Socket.IO, syncs, then disconnects
   - lists all cluster Ingresses
   - loads optional static monitors from `/config/monitors.yaml`
2. For each Ingress with `uptime-kuma.io/monitor: "true"`, it ensures a
   monitor named `namespace/Ingress/name` exists with the right URL/interval
   and notification channels. Kuma's UI enables Default channels on create;
   Socket.IO does not. `uptime-kuma.io/use-default-notification: "true"`
   enables whatever channel is marked **Default** in Kuma — the operator
   never takes that channel's name. `uptime-kuma.io/notification` attaches
   extra channels by name.
3. Monitors it owns are tagged `managed-by-uptime-operator`. Manual monitors
   without that tag are never touched.
4. If an Ingress loses the annotation or is deleted, the matching managed
   monitor is **not** dropped immediately by default. The operator keeps
   probing the last URL for `DEFAULT_DELETE_GRACE` (24h) so an accidental
   Helm uninstall — especially with Flux paused — still pages through
   Kuma. Set `uptime-kuma.io/delete-policy: "immediate"` when the monitor
   should die with the Ingress, or `"retain"` to never delete it (Kuma
   keeps probing until a human removes the monitor). The policy is written
   onto the monitor description so it survives Ingress deletion.

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
| `uptime-kuma.io/ignore-tls` | `false` | `"true"` skips TLS verify and cert/domain expiry alerts (Let's Encrypt staging) |
| `uptime-kuma.io/path` | `/` | path appended to the Ingress host |
| `uptime-kuma.io/method` | `GET` | HTTP method (`GET`, `HEAD`, …) |
| `uptime-kuma.io/accepted-status-codes` | `200-299` | comma-separated ranges or codes |
| `uptime-kuma.io/max-redirects` | `10` | follow this many redirects |
| `uptime-kuma.io/timeout` | `48` | request timeout (seconds) |
| `uptime-kuma.io/retry-interval` | `60` | seconds between retries after a failure |
| `uptime-kuma.io/max-retries` | `3` | retries before the monitor is DOWN |
| `uptime-kuma.io/host` | first rule | Ingress hostname to probe |
| `uptime-kuma.io/use-default-notification` | `false` | `"true"` enables Kuma's Default notification(s) on the monitor |
| `uptime-kuma.io/notification` | — | extra comma-separated Kuma notification channel names |
| `uptime-kuma.io/delete-policy` | `deferred` | `immediate` removes the monitor with the Ingress; `deferred` waits `delete-grace`; `retain` never deletes |
| `uptime-kuma.io/delete-grace` | `24h` | how long a deferred orphan stays in Kuma (`24h`, `90m`, or `24` hours) |

`use-default-notification` does not name a channel. It turns on every
active Kuma notification with **Default** checked, the same as the UI on
create. Named channels are matched case-insensitively; missing names fail
that Ingress's reconcile. The two annotations can be combined.

Static monitors use the same knobs:

```yaml
monitors:
  - name: Flux webhook
    url: https://example.com/
    use_default_notification: true
    notification: Slack
    notifications:
      - PagerDuty
    delete_policy: deferred
    delete_grace: 24h
```

## Environment

| Variable | Required | Description |
|---|---|---|
| `KUMA_URL` | yes | Kuma base URL (no trailing slash) |
| `KUMA_USERNAME` | yes | login user |
| `KUMA_PASSWORD` | yes | login password |
| `RESYNC_INTERVAL` | no | seconds between full syncs (default `300`) |
| `DEFAULT_DELETE_POLICY` | no | `deferred` (default), `immediate`, or `retain` for Ingresses without an override |
| `DEFAULT_DELETE_GRACE` | no | deferred orphan lifetime (default `24h`) |
| `STATIC_MONITORS_PATH` | no | default `/config/monitors.yaml` |
| `LOG_LEVEL` | no | `DEBUG` / `INFO` / `WARN` / `ERROR` |
| `GOMEMLIMIT` | no | Go heap cap (e.g. `58MiB`). Unset: 90% of the container memory limit |

## Authentication

Kuma has **no REST API for creating or updating monitors**. The operator
talks to the same Socket.IO login the web UI uses (`username` + `password`).
That has two consequences:

1. **2FA must be off** on the account the operator uses. The Socket.IO
   `login` event needs a one-time TOTP when 2FA is enabled, and this
   operator cannot supply one. Create a dedicated machine user without 2FA
   and keep 2FA on your personal admin account.
2. **A Kuma API key cannot replace the password.** API keys only protect
   `/metrics` and similar HTTP endpoints. They cannot authenticate Socket.IO
   or manage monitors.

Upstream:

- [Impossible to login to websocket with API token](https://github.com/louislam/uptime-kuma/issues/3107)
- [API keys and creating a monitor](https://github.com/louislam/uptime-kuma/issues/3625)
- [REST API write endpoints for monitors and tags](https://github.com/louislam/uptime-kuma/issues/7150)
- [`POST /api/monitor` is not implemented](https://github.com/louislam/uptime-kuma/issues/5935)
- [REST-to-Socket.IO bridge (not merged)](https://github.com/louislam/uptime-kuma/pull/7153)

A failed login with an empty error (`login: login: `) usually means the
user has 2FA enabled.

## Build

```bash
make build          # local binary → bin/uptime-operator
make image          # docker → ghcr.io/solid3dlab/uptime-operator:<git-sha>
```

Image: multi-stage, `CGO_ENABLED=0`, final stage `scratch` + CA certs, runs as
UID 65532.

## Release

Push a semver tag. CI writes the GitHub release notes from conventional
commits, updates `CHANGELOG.md`, and publishes

`ghcr.io/solid3dlab/uptime-operator:<version>` (tag `v0.1.0` → image `0.1.0`).

```bash
git tag v0.1.0
git push origin v0.1.0
```

Prerelease tags like `v0.1.0-rc.1` skip the changelog commit.

## Deploy

Manifests live in the cluster repo under
`infrastructure/monitoring/uptime-operator/`. Credentials come from a
1Password item (`url` / `username` / `password`).
