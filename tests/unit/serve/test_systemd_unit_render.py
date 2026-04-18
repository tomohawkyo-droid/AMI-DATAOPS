"""Render the ami-serve systemd unit template and assert its structure."""

from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader

_STATE_DIR = "/var/lib/ami-serve"


def _render(template_dir: Path, tunnel: dict[str, object]) -> str:
    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
    )
    template = env.get_template("ami-serve-tunnel.service.j2")
    return template.render(
        tunnel=tunnel,
        ami_root="/ami/root",
        cloudflared_bin="/ami/root/.boot-linux/bin/cloudflared",
        serve_state_dir=_STATE_DIR,
    )


class TestSystemdUnitRender:
    def test_basic_unit(self, serve_templates_dir: Path) -> None:
        rendered = _render(
            serve_templates_dir,
            {"name": "main", "tunnel_id": "abc-123", "replica": None},
        )
        assert "Description=AMI Serve tunnel: main" in rendered
        assert "Restart=always" in rendered
        assert "RestartSec=5" in rendered
        assert "WantedBy=default.target" in rendered
        assert " run abc-123" in rendered
        assert "--replica" not in rendered

    def test_replica_flag_included_when_set(self, serve_templates_dir: Path) -> None:
        rendered = _render(
            serve_templates_dir,
            {"name": "edge", "tunnel_id": "xyz-456", "replica": 2},
        )
        assert "--replica 2" in rendered
        assert " xyz-456" in rendered
        assert "--replica 2 xyz-456" in rendered

    def test_config_path_references_state_dir(self, serve_templates_dir: Path) -> None:
        rendered = _render(
            serve_templates_dir,
            {"name": "main", "tunnel_id": "abc-123", "replica": None},
        )
        assert f"{_STATE_DIR}/main/config.yml" in rendered
