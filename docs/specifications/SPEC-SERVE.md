# `ami-serve`: Technical Specification

**Date:** 2026-04-18
**Status:** ACTIVE
**Type:** Specification
**Requirements:** [REQ-SERVE.md](../requirements/REQ-SERVE.md)

This specification describes behaviour, not code. For the implementation, see `projects/AMI-DATAOPS/ami/dataops/serve/` and `projects/AMI-DATAOPS/res/ansible/serve.yml`.

---

## 1. Pipeline

```
┌────────────────┐
│ 1. Load         │  Parse inventory: dataops_serve_tunnels + instances.
│    inventory    │  Validate: unique names, tunnel refs resolve, creds exist.
├────────────────┤
│ 2. Render       │  Per tunnel: emit cloudflared config.yml with its
│    config       │  instances' ingress rules + mandatory catch-all.
├────────────────┤
│ 3. Install      │  Write ~/.config/systemd/user/ami-serve-<tunnel>.service
│    unit         │  from template. systemctl --user daemon-reload.
├────────────────┤
│ 4. Linger       │  If any instance has systemd_persist: true, run
│    (optional)   │  `loginctl enable-linger $(id -un)` once.
├────────────────┤
│ 5. Start unit   │  systemctl --user enable --now ami-serve-<tunnel>.
├────────────────┤
│ 6. Route DNS    │  Only if --route-dns passed. Per instance:
│    (optional)   │  cloudflared tunnel route dns <tunnel> <hostname>.
├────────────────┤
│ 7. Verify       │  Hit metrics endpoint, confirm tunnel reports HA
│                 │  connections > 0. Report success/degraded/failed.
└────────────────┘
```

---

## 2. Inventory layout

Two lists declared in Ansible inventory (typically under `host_vars/<host>/serve.yml` or `group_vars/all/serve.yml`).

### 2.1 Tunnels

```yaml
dataops_serve_tunnels:
  - name: main
    tunnel_id: "320324fb-8c11-4e72-90f6-6b009b980bea"
    credentials_file: cloudflare/credentials.json   # relative to AMI_ROOT
    metrics_port: 5000
    replica: null                                   # null | 0..N
```

Required fields: `name`, `tunnel_id`, `credentials_file`. Others optional.

### 2.2 Instances

```yaml
dataops_serve_instances:
  - name: portal
    tunnel: main
    hostname: portal.p9q3fjcwcla0.uk
    upstream: http://localhost:3000
    origin_request:
      noTLSVerify: false
      connectTimeout: 30s
      tlsTimeout: 10s
      keepAliveTimeout: 30s
      httpHostHeader: portal.p9q3fjcwcla0.uk
    systemd_persist: true
```

Required fields: `name`, `tunnel`, `hostname`, `upstream`. `origin_request` and `systemd_persist` optional. A missing `origin_request` means "use cloudflared defaults".

---

## 3. Rendered config

Each tunnel produces one `cloudflared` YAML at
`~/.config/ami-serve/<tunnel-name>/config.yml`:

```yaml
tunnel: 320324fb-8c11-4e72-90f6-6b009b980bea
credentials-file: /home/ami/AMI-AGENTS/cloudflare/credentials.json
metrics: localhost:5000

ingress:
  - hostname: portal.p9q3fjcwcla0.uk
    service: http://localhost:3000
    originRequest:
      noTLSVerify: false
      connectTimeout: 30s
      tlsTimeout: 10s
      keepAliveTimeout: 30s
      httpHostHeader: portal.p9q3fjcwcla0.uk
  - hostname: api.p9q3fjcwcla0.uk
    service: http://localhost:8080
  - service: http_status:404
```

Catch-all is mandatory and synthesised by the template. An attempt to render without it is a bug.

---

## 4. Systemd unit

One unit per tunnel at `~/.config/systemd/user/ami-serve-<tunnel-name>.service`:

```ini
[Unit]
Description=AMI Serve tunnel: <tunnel-name>
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={AMI_ROOT}/.boot-linux/bin/cloudflared \
          tunnel --config {config-path} run {tunnel-id}
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
```

When `replica` is non-null, the ExecStart gains `--replica <N>`. When linger is required, the playbook runs `loginctl enable-linger $(id -un)` once per host.

---

## 5. CLI surface

```
ami-serve deploy [--route-dns] [--limit TUNNEL] [--check]
    Render configs, install units, start tunnels.
    --route-dns:  also call `cloudflared tunnel route dns` per instance.
    --limit:      restrict to one tunnel name.
    --check:      Ansible check mode (dry run, no writes).

ami-serve stop [TUNNEL | all]
    systemctl --user stop ami-serve-<tunnel>.service.

ami-serve restart [TUNNEL | all]
    systemctl --user restart ami-serve-<tunnel>.service.

ami-serve status [TUNNEL | all] [--json]
    Report per-tunnel: unit state, uptime, hostnames, metrics summary.

ami-serve route-dns [TUNNEL | all]
    Re-run `cloudflared tunnel route dns` for every instance.

ami-serve logs TUNNEL
    journalctl --user -u ami-serve-<tunnel>.service -f.

ami-serve --ci [--defaults FILE]
    Non-interactive mode for CI. Defaults file lives at
    ami/config/serve-defaults.yaml in AMI_ROOT when --defaults
    is omitted. Mirrors ami-update's CLI convention.
```

Argparse uses `prog="ami-serve"` so `--help` output contains the invoked name.

---

## 6. Status output

Example `ami-serve status`:

```
TUNNEL   UNIT          UPTIME    HOSTS                                METRICS
main     active        1d 4h     portal.p9q3fjcwcla0.uk, api.*        http://localhost:5000/metrics (2 HA conns)
edge     degraded      2m        docs.p9q3fjcwcla0.uk                 metrics unreachable
vpn      inactive      -         (none running)                       -
```

`--json` emits a structured list for scripting.

---

## 7. Ansible structure

| File | Purpose |
|------|---------|
| `res/ansible/serve.yml` | Main playbook. Tags: `deploy`, `stop`, `restart`, `status`, `route-dns`. |
| `res/ansible/templates/cloudflared-tunnel-config.yml.j2` | Renders per-tunnel `config.yml`. |
| `res/ansible/templates/ami-serve-tunnel.service.j2` | Renders per-tunnel user systemd unit. |

The playbook targets `hosts: localhost` by default (user-scope systemd). Multi-host deployments pass `--inventory` and `-l` to target each host individually.

---

## 8. Python package

`ami/dataops/serve/`:

- `cli.py` — argparse dispatcher.
- `main.py` — entry point (no shebang, no exec bit).
- `ansible.py` — invokes `ansible-playbook serve.yml --tags <stage>` with `-e` JSON overrides.
- `status.py` — systemd query + metrics scrape; returns typed `TunnelStatus` records.

---

## 9. Extension wiring

`projects/AMI-DATAOPS/extension.manifest.yaml` gains:

```yaml
  - name: ami-serve
    binary: projects/AMI-DATAOPS/ami/dataops/serve/main.py
    description: Publish apps on Cloudflare Tunnel FQDNs
    category: infra
    features: [deploy, stop, restart, status, route-dns, logs]
    bannerPriority: 220
    check:
      command: ["{python}", "{binary}", "--help"]
      healthExpect: "ami-serve"
      timeout: 5
```

---

## 10. Edge cases

| Case | Behaviour |
|------|-----------|
| Inventory references unknown tunnel | Fail before any side-effect, exit non-zero. |
| Duplicate instance or tunnel name | Fail validation, exit non-zero. |
| Credentials file missing | Per-tunnel failure; other tunnels proceed. |
| DNS CNAME already points at correct tunnel | `route-dns` is a no-op. |
| DNS CNAME points at different tunnel | `route-dns` fails with an explicit error. |
| DNS A record exists for the hostname | `route-dns` fails; operator must remove the A record. |
| Systemd unit already exists with different content | Overwrite, reload, restart. |
| `loginctl enable-linger` already active | Idempotent; no change. |
| Metrics endpoint unreachable | Status reports DEGRADED; unit may still be running. |
| `--check` mode | No systemd / file / DNS writes; prints planned actions. |
| Cloudflared binary missing | Fail deploy with instruction to run the bootstrap script. |
