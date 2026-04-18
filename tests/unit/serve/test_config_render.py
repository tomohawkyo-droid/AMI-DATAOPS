"""Render the cloudflared config template and assert its structure."""

from __future__ import annotations

from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader

_TUNNEL = {
    "name": "main",
    "tunnel_id": "tunnel-uuid",
    "credentials_file": "cloudflare/creds.json",
    "metrics_port": 5000,
    "replica": None,
}

_INSTANCES = [
    {
        "name": "portal",
        "tunnel": "main",
        "hostname": "portal.example.com",
        "upstream": "http://localhost:3000",
        "origin_request": {"noTLSVerify": False, "connectTimeout": "30s"},
    },
    {
        "name": "api",
        "tunnel": "main",
        "hostname": "api.example.com",
        "upstream": "http://localhost:8080",
    },
]


def _render(template_dir: Path) -> str:
    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
    )
    template = env.get_template("cloudflared-tunnel-config.yml.j2")
    return template.render(
        tunnel=_TUNNEL,
        tunnel_instances=_INSTANCES,
        ami_root="/ami/root",
    )


class TestCloudflaredConfigRender:
    def test_structure_parses_as_yaml(self, serve_templates_dir: Path) -> None:
        parsed = yaml.safe_load(_render(serve_templates_dir))
        assert parsed["tunnel"] == "tunnel-uuid"
        assert parsed["credentials-file"] == "/ami/root/cloudflare/creds.json"
        assert parsed["metrics"] == "localhost:5000"
        _expected_rules = 3  # two instances + catch-all
        assert len(parsed["ingress"]) == _expected_rules

    def test_catch_all_is_last(self, serve_templates_dir: Path) -> None:
        parsed = yaml.safe_load(_render(serve_templates_dir))
        last = parsed["ingress"][-1]
        assert last == {"service": "http_status:404"}

    def test_instance_order_preserved(self, serve_templates_dir: Path) -> None:
        parsed = yaml.safe_load(_render(serve_templates_dir))
        assert parsed["ingress"][0]["hostname"] == "portal.example.com"
        assert parsed["ingress"][1]["hostname"] == "api.example.com"

    def test_origin_request_only_when_declared(self, serve_templates_dir: Path) -> None:
        parsed = yaml.safe_load(_render(serve_templates_dir))
        # portal declared originRequest, api did not
        assert "originRequest" in parsed["ingress"][0]
        assert parsed["ingress"][0]["originRequest"]["connectTimeout"] == "30s"
        assert "originRequest" not in parsed["ingress"][1]

    def test_metrics_omitted_without_port(self, serve_templates_dir: Path) -> None:
        tunnel = dict(_TUNNEL)
        tunnel["metrics_port"] = None
        env = Environment(
            loader=FileSystemLoader(str(serve_templates_dir)),
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
        )
        rendered = env.get_template("cloudflared-tunnel-config.yml.j2").render(
            tunnel=tunnel, tunnel_instances=_INSTANCES, ami_root="/ami/root"
        )
        parsed = yaml.safe_load(rendered)
        assert "metrics" not in parsed
