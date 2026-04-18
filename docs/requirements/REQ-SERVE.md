# Requirements: Application Serving via Cloudflare Tunnel (`ami-serve`)

**Date:** 2026-04-18
**Status:** ACTIVE
**Type:** Requirements
**Spec:** [SPEC-SERVE](../specifications/SPEC-SERVE.md)

---

## Background

Every app we publish today (AMI-STREAMS Matrix, AMI-PORTAL, future internal services) is exposed through a hand-assembled `cloudflared` config plus an ad-hoc systemd unit. The configs follow the same pattern — named tunnel, credentials-file auth, ingress rules, origin request tuning, `Restart=always` — but are copy-pasted between repos. There is no canonical way to say "publish this local port on this FQDN" from an operator's inventory.

`ami-serve` is the canonical way. It takes an inventory describing tunnels and per-app bindings, renders a `cloudflared` config plus a user-scoped systemd unit per tunnel, and manages the lifecycle through a single CLI.

---

## Core Requirements

### 1. Inventory Model

- **R-SERVE-001**: Inventory shall declare two separate lists: `dataops_serve_tunnels` (one entry per Cloudflare Tunnel) and `dataops_serve_instances` (one entry per app↔hostname binding).
- **R-SERVE-002**: Each instance shall reference exactly one tunnel by `name`. Unresolved references shall fail deployment before any side effect.
- **R-SERVE-003**: Multiple instances may share one tunnel; their ingress rules are concatenated in inventory order before the mandatory catch-all.
- **R-SERVE-004**: Instance and tunnel names shall be unique within their list. Duplicates shall fail validation.
- **R-SERVE-005**: Per-instance `origin_request` fields (`noTLSVerify`, `connectTimeout`, `tlsTimeout`, `keepAliveTimeout`, `httpHostHeader`) shall be optional and default to Cloudflare's documented defaults when omitted.

### 2. Tunnel Lifecycle

- **R-SERVE-010**: `ami-serve` shall **not** create, delete, or rotate tunnels. Tunnel creation (`cloudflared tunnel create`) and deletion are operator-performed out of band.
- **R-SERVE-011**: `ami-serve` shall verify the tunnel credentials file exists and is readable before starting a tunnel. A missing or unreadable file shall fail the deploy for that tunnel with a clear message.
- **R-SERVE-012**: The credentials file shall be stored at a path declared in the inventory (`credentials_file`) relative to the AMI root; file permissions shall be expected to be `0600`.
- **R-SERVE-013**: Only named tunnels (persistent UUID) are supported. Ad-hoc "quick tunnels" (`cloudflared tunnel --url ...`) are out of scope.

### 3. Ingress

- **R-SERVE-020**: A `cloudflared` config YAML shall be rendered per tunnel at a well-known path. The config shall include the tunnel UUID, credentials file path, metrics endpoint, and the ingress list.
- **R-SERVE-021**: The ingress list shall contain one rule per instance, in inventory order, followed by the mandatory catch-all `service: http_status:404`. `ami-serve` shall refuse to render a config without the catch-all.
- **R-SERVE-022**: Each ingress rule shall include `hostname`, `service` (derived from the instance's `upstream`), and any `originRequest` overrides the instance declares.
- **R-SERVE-023**: Path-based ingress routing is explicitly **not supported**. Operators needing path routing shall front the tunnel with a reverse proxy (Traefik, nginx, caddy) or use Cloudflare Workers.

### 4. DNS Routing

- **R-SERVE-030**: `ami-serve deploy --route-dns` shall invoke `cloudflared tunnel route dns <tunnel> <hostname>` once per instance to create or update the Cloudflare DNS CNAME.
- **R-SERVE-031**: Default `ami-serve deploy` (no `--route-dns`) shall **not** modify DNS. DNS routing is opt-in.
- **R-SERVE-032**: DNS routing shall be idempotent: re-running against an already-correct CNAME shall be a no-op. Mismatched bindings (hostname already points to a different tunnel or to an A record) shall fail with a clear error.
- **R-SERVE-033**: DNS routing requires the zone to be on Cloudflare nameservers. `ami-serve` shall not attempt nameserver changes.

### 5. Systemd Persistence

- **R-SERVE-040**: A user-scoped systemd unit shall be installed per tunnel at `~/.config/systemd/user/ami-serve-<tunnel>.service`.
- **R-SERVE-041**: The unit shall have `Restart=always`, `RestartSec=5`, `Type=simple`, and `WantedBy=default.target`.
- **R-SERVE-042**: When any instance on a tunnel declares `systemd_persist: true`, the deploy shall run `loginctl enable-linger $(id -un)` once per host so the unit starts at boot without an active session.
- **R-SERVE-043**: System-scoped units (`/etc/systemd/system/`) are out of scope; user-scope is the only supported mode.
- **R-SERVE-044**: The unit `ExecStart` shall invoke the bootstrapped `cloudflared` binary with `tunnel --config <rendered-config> run <tunnel-id>`, honouring the `replica` field when set.

### 6. Status & Health

- **R-SERVE-050**: `ami-serve status` shall report, per tunnel: systemd unit state (`active`/`inactive`/`failed`), uptime, the rendered config path, the list of hostnames served, and the metrics endpoint URL.
- **R-SERVE-051**: Health checking shall use `cloudflared`'s built-in Prometheus metrics endpoint (`--metrics localhost:<metrics_port>`), reading `cloudflared_tunnel_ha_connections` and `tunnel_connect_time_seconds`.
- **R-SERVE-052**: When the metrics endpoint is unreachable, `ami-serve status` shall mark the tunnel as DEGRADED (unit may be running but connectivity is unverifiable).

### 7. CLI Interface

- **R-SERVE-060**: `ami-serve` shall expose these subcommands: `deploy`, `stop`, `restart`, `status`, `route-dns`, `logs`.
- **R-SERVE-061**: `ami-serve` shall be registered as a DATAOPS extension via `projects/AMI-DATAOPS/extension.manifest.yaml`, with a `{python} {binary} --help` health check.
- **R-SERVE-062**: `ami-serve --ci` shall invoke non-interactive CI mode; when `--defaults FILE` is also supplied, `FILE` is used, otherwise a repository-provided default defaults file is used. (Mirrors the `ami-update` CLI convention.)
- **R-SERVE-063**: `ami-serve logs <tunnel>` shall tail `journalctl --user -u ami-serve-<tunnel>.service -f`.
- **R-SERVE-064**: `ami-serve` shall always resolve AMI_ROOT and operate from there, regardless of the caller's working directory.

### 8. Ansible Implementation

- **R-SERVE-070**: A single playbook `projects/AMI-DATAOPS/res/ansible/serve.yml` shall handle all lifecycle stages via tags: `deploy`, `stop`, `restart`, `status`, `route-dns`.
- **R-SERVE-071**: The playbook shall be idempotent: re-running `deploy` with no inventory changes shall produce no systemd or file-write activity.
- **R-SERVE-072**: Config and unit templates shall live at `projects/AMI-DATAOPS/res/ansible/templates/cloudflared-tunnel-config.yml.j2` and `projects/AMI-DATAOPS/res/ansible/templates/ami-serve-tunnel.service.j2` respectively.
- **R-SERVE-073**: The playbook shall support Ansible check mode (`--check`) for dry-run verification.

### 9. Exit Codes

- **R-SERVE-080**: Exit **0** on successful deploy / stop / restart / status / route-dns / logs invocation; also on interactive cancellation.
- **R-SERVE-081**: Exit **non-zero** on any of: unresolved tunnel reference, duplicate name in inventory, missing or unreadable credentials file, systemd unit installation failure, ansible-playbook non-zero return, route-dns failure.

---

## Constraints

- Python 3.11+ (DATAOPS is pinned).
- Reuses the bootstrapped `cloudflared` binary at `.boot-linux/bin/cloudflared`; never installs cloudflared itself.
- Reuses the existing `ami-tunnel` CLI shim (`ami/scripts/bin/ami_tunnel.py`) for `tunnel route dns` invocations.
- Ansible is invoked through the bootstrapped `ansible-playbook` binary; no pip install of ansible on the host.
- User-scoped systemd only (no root required).

## Non-Requirements

- **Tunnel creation or deletion.** Operator runs `cloudflared tunnel create <name>` / `delete` out of band.
- **Credential rotation.** Operator replaces the credentials file; `ami-serve` picks up the new one on next deploy.
- **Zero Trust Access policies.** `ami-serve` is transport only. Access gating (who can reach the exposed app) is configured in the Cloudflare Zero Trust dashboard.
- **TLS certificate management.** Cloudflare Tunnel handles edge TLS end to end; upstream TLS is controlled via `originRequest.noTLSVerify`.
- **Path-based routing.** Hostname-only. Use a reverse proxy or Cloudflare Workers for path routing.
- **Origin load balancing.** One upstream per instance. Multi-origin load balancing is a Cloudflare dashboard feature.
- **Replica orchestration.** The `replica` field passes `--replica <N>` through to `cloudflared`, but orchestrating replicas across multiple hosts is the operator's responsibility (run `ami-serve` on each host).
- **Zone-level DNS changes.** `ami-serve` only creates CNAMEs for hostnames already in a Cloudflare-managed zone. Nameserver delegation, zone creation, and registrar operations are out of scope.
