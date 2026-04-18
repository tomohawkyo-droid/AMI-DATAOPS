"""Status query for ami-serve tunnels."""

from __future__ import annotations

import subprocess
import urllib.error
import urllib.request
from typing import NamedTuple


class TunnelStatus(NamedTuple):
    """Resolved state of a single tunnel."""

    name: str
    active_state: str  # systemd ActiveState: active/inactive/failed/unknown
    sub_state: str  # systemd SubState: running/dead/...
    metrics_ok: bool
    metrics_url: str | None
    extra: str


def query_systemd(tunnel_name: str) -> tuple[str, str]:
    """Return (ActiveState, SubState) for a user-scope ami-serve-<tunnel> unit."""
    unit = f"ami-serve-{tunnel_name}.service"
    try:
        result = subprocess.run(
            [
                "systemctl",
                "--user",
                "show",
                unit,
                "-p",
                "ActiveState",
                "-p",
                "SubState",
            ],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (subprocess.TimeoutExpired, OSError):
        return "unknown", "unknown"

    active = "unknown"
    sub = "unknown"
    for line in result.stdout.splitlines():
        if line.startswith("ActiveState="):
            active = line.split("=", 1)[1]
        elif line.startswith("SubState="):
            sub = line.split("=", 1)[1]
    return active, sub


def check_metrics(url: str, timeout: float = 2.0) -> tuple[bool, str]:
    """Hit cloudflared metrics endpoint; return (ok, summary)."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return False, f"unreachable: {exc}"

    ha_conns = 0
    for line in body.splitlines():
        if line.startswith("cloudflared_tunnel_ha_connections"):
            try:
                ha_conns = int(float(line.rsplit(maxsplit=1)[-1]))
            except ValueError:
                ha_conns = 0
            break
    return True, f"{ha_conns} HA conns"


def resolve_status(tunnel: dict[str, object]) -> TunnelStatus:
    """Produce a TunnelStatus for one tunnel spec."""
    name = str(tunnel["name"])
    active, sub = query_systemd(name)
    metrics_port = tunnel.get("metrics_port")
    if metrics_port:
        url = f"http://localhost:{metrics_port}/metrics"
        ok, summary = check_metrics(url)
        return TunnelStatus(
            name=name,
            active_state=active,
            sub_state=sub,
            metrics_ok=ok,
            metrics_url=url,
            extra=summary,
        )
    return TunnelStatus(
        name=name,
        active_state=active,
        sub_state=sub,
        metrics_ok=False,
        metrics_url=None,
        extra="no metrics_port configured",
    )
