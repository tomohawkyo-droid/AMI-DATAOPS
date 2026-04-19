"""Unit tests for ami.dataops.report.config."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from ami.dataops.report.config import (
    PeerEntry,
    ReportConfig,
    SenderConfig,
    load_report_config,
)


class TestReportConfig:
    def test_minimal(self) -> None:
        cfg = ReportConfig.model_validate(
            {
                "sender": {"sender_id": "alpha"},
                "peers": [],
            }
        )
        assert cfg.sender.sender_id == "alpha"
        assert cfg.peers == []

    def test_peer_lookup(self) -> None:
        cfg = ReportConfig.model_validate(
            {
                "sender": {"sender_id": "alpha"},
                "peers": [
                    {
                        "name": "bravo",
                        "endpoint": "https://intake.bravo.example.com/",
                        "shared_secret_env_var": "AMI_REPORT_SECRET_BRAVO",
                    }
                ],
            }
        )
        assert cfg.peer("bravo").shared_secret_env_var == "AMI_REPORT_SECRET_BRAVO"
        with pytest.raises(KeyError):
            cfg.peer("unknown")

    def test_duplicate_peer_names_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ReportConfig.model_validate(
                {
                    "sender": {"sender_id": "alpha"},
                    "peers": [
                        {
                            "name": "bravo",
                            "endpoint": "https://x.example.com/",
                            "shared_secret_env_var": "A",
                        },
                        {
                            "name": "bravo",
                            "endpoint": "https://y.example.com/",
                            "shared_secret_env_var": "B",
                        },
                    ],
                }
            )


class TestSenderConfig:
    def test_extra_roots_are_absoluted(self, tmp_path: Path) -> None:
        relative = (tmp_path / "nested").resolve()
        cfg = SenderConfig.model_validate(
            {"sender_id": "alpha", "extra_roots": [str(relative)]}
        )
        assert cfg.extra_roots == [relative]


class TestPeerEntry:
    def test_endpoint_must_be_valid_url(self) -> None:
        with pytest.raises(ValidationError):
            PeerEntry.model_validate(
                {
                    "name": "bravo",
                    "endpoint": "not-a-url",
                    "shared_secret_env_var": "A",
                }
            )

    def test_extra_field_forbidden(self) -> None:
        with pytest.raises(ValidationError):
            PeerEntry.model_validate(
                {
                    "name": "bravo",
                    "endpoint": "https://ok.example.com/",
                    "shared_secret_env_var": "A",
                    "unknown": "x",
                }
            )


class TestLoadReportConfig:
    def test_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            load_report_config(tmp_path / "no.yml")

    def test_non_mapping_rejected(self, tmp_path: Path) -> None:
        path = tmp_path / "c.yml"
        path.write_text("- a\n- b\n")
        with pytest.raises(TypeError):
            load_report_config(path)

    def test_missing_sender_section_rejected(self, tmp_path: Path) -> None:
        path = tmp_path / "c.yml"
        path.write_text("dataops_report_peers: []\n")
        with pytest.raises(ValueError, match="missing required key"):
            load_report_config(path)

    def test_round_trip(self, tmp_path: Path) -> None:
        path = tmp_path / "c.yml"
        path.write_text(
            "dataops_report_sender_config:\n"
            "  sender_id: alpha\n"
            "dataops_report_peers:\n"
            "  - name: bravo\n"
            "    endpoint: https://x.example.com/\n"
            "    shared_secret_env_var: AMI_REPORT_SECRET_BRAVO\n"
        )
        cfg = load_report_config(path)
        assert cfg.sender.sender_id == "alpha"
        assert cfg.peers[0].name == "bravo"
