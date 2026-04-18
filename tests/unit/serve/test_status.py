"""Tests for the status module (systemd query + metrics scrape)."""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock, patch

from ami.dataops.serve import status


class TestQuerySystemd:
    def test_parses_active_running(self) -> None:
        with patch(
            "ami.dataops.serve.status.subprocess.run",
            return_value=MagicMock(
                stdout="ActiveState=active\nSubState=running\n",
                returncode=0,
            ),
        ):
            active, sub = status.query_systemd("main")
        assert active == "active"
        assert sub == "running"

    def test_handles_missing_unit(self) -> None:
        with patch(
            "ami.dataops.serve.status.subprocess.run",
            return_value=MagicMock(stdout="", returncode=0),
        ):
            active, sub = status.query_systemd("nope")
        assert active == "unknown"
        assert sub == "unknown"

    def test_swallows_timeout(self) -> None:
        with patch(
            "ami.dataops.serve.status.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd=["systemctl"], timeout=5),
        ):
            active, sub = status.query_systemd("main")
        assert active == "unknown"
        assert sub == "unknown"


class TestCheckMetrics:
    def test_parses_ha_connections(self) -> None:
        body = (
            b"# HELP cloudflared_tunnel_ha_connections HA connections\n"
            b"cloudflared_tunnel_ha_connections 4\n"
        )
        fake_resp = MagicMock()
        fake_resp.read.return_value = body
        fake_resp.__enter__ = lambda self: self
        fake_resp.__exit__ = lambda *_: None
        with patch(
            "ami.dataops.serve.status.urllib.request.urlopen",
            return_value=fake_resp,
        ):
            ok, summary = status.check_metrics("http://localhost:5000/metrics")
        assert ok is True
        assert "4" in summary

    def test_unreachable_returns_false(self) -> None:
        with patch(
            "ami.dataops.serve.status.urllib.request.urlopen",
            side_effect=OSError("connection refused"),
        ):
            ok, summary = status.check_metrics("http://localhost:5000/metrics")
        assert ok is False
        assert "unreachable" in summary


class TestResolveStatus:
    def test_no_metrics_port_returns_degraded(self) -> None:
        with patch(
            "ami.dataops.serve.status.query_systemd",
            return_value=("active", "running"),
        ):
            result = status.resolve_status({"name": "main"})
        assert result.active_state == "active"
        assert result.metrics_ok is False
        assert "no metrics_port" in result.extra
