"""Sender-side config for ami-report: local `dataops_report_sender_config`
plus the peer list that the TUI picks from.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator


class PeerEntry(BaseModel):
    """One entry in `dataops_report_peers` — where a bundle can be sent."""

    model_config = ConfigDict(extra="forbid")

    name: str
    endpoint: HttpUrl
    shared_secret_env_var: str


class SenderConfig(BaseModel):
    """Local `dataops_report_sender_config` — who am I + where to look for logs."""

    model_config = ConfigDict(extra="forbid")

    sender_id: str
    extra_roots: list[Path] = Field(default_factory=list)
    default_ci_defaults: Path | None = None

    @field_validator("extra_roots")
    @classmethod
    def _absolute_roots(cls, value: list[Path]) -> list[Path]:
        return [p.expanduser().absolute() for p in value]


class ReportConfig(BaseModel):
    """Bundled view of the sender config + its known peers."""

    model_config = ConfigDict(extra="forbid")

    sender: SenderConfig
    peers: list[PeerEntry] = Field(default_factory=list)

    @field_validator("peers")
    @classmethod
    def _unique_peer_names(cls, value: list[PeerEntry]) -> list[PeerEntry]:
        names = [p.name for p in value]
        if len(names) != len(set(names)):
            msg = f"duplicate peer names in dataops_report_peers: {names}"
            raise ValueError(msg)
        return value

    def peer(self, name: str) -> PeerEntry:
        for entry in self.peers:
            if entry.name == name:
                return entry
        msg = f"no peer named {name!r} in dataops_report_peers"
        raise KeyError(msg)


def load_report_config(path: Path) -> ReportConfig:
    """Parse `path` (YAML) into a ReportConfig or raise with a clear message."""
    if not path.is_file():
        msg = f"report config file not found: {path}"
        raise FileNotFoundError(msg)
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        msg = f"report config at {path} is not a YAML mapping"
        raise TypeError(msg)
    sender_dict = raw.get("dataops_report_sender_config")
    peers_list = raw.get("dataops_report_peers", [])
    if sender_dict is None:
        msg = "report config missing required key dataops_report_sender_config"
        raise ValueError(msg)
    return ReportConfig.model_validate({"sender": sender_dict, "peers": peers_list})
