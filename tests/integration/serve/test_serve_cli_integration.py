"""Integration test: ami-serve CLI entrypoint through to ansible invocation.

Runs main() end-to-end with subprocess mocked so no real ansible or
systemd calls happen. Exercises main.py, cli.py dispatch, ansible.py
command construction, and status.py fallbacks.
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ami.dataops.serve import cli

_ANSIBLE_FAILURE_RC = 7


@pytest.fixture
def ami_root(tmp_path: Path) -> Path:
    """Build a minimal AMI_ROOT layout that find_ami_root() will accept."""
    (tmp_path / "pyproject.toml").touch()
    (tmp_path / "projects").mkdir()
    (tmp_path / "projects" / "AMI-DATAOPS").mkdir()
    (tmp_path / "projects" / "AMI-DATAOPS" / "res").mkdir()
    (tmp_path / "projects" / "AMI-DATAOPS" / "res" / "ansible").mkdir()
    (
        tmp_path / "projects" / "AMI-DATAOPS" / "res" / "ansible" / "serve.yml"
    ).write_text("# stub")
    (tmp_path / ".boot-linux").mkdir()
    (tmp_path / ".boot-linux" / "bin").mkdir()
    (tmp_path / ".boot-linux" / "bin" / "ansible-playbook").touch()
    return tmp_path


class TestMainEntryPoint:
    def test_main_module_runs(
        self,
        ami_root: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        monkeypatch.setenv("AMI_ROOT", str(ami_root))
        monkeypatch.setattr(sys, "argv", ["ami-serve"])
        with (
            patch(
                "ami.dataops.serve.cli.os.chdir",
            ),
            pytest.raises(SystemExit) as exc,
        ):
            runpy.run_module("ami.dataops.serve.main", run_name="__main__")
        assert exc.value.code == 0
        assert "ami-serve" in capsys.readouterr().out


class TestDeploySubcommandEndToEnd:
    def test_deploy_invokes_ansible_playbook_with_correct_tags(
        self, ami_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AMI_ROOT", str(ami_root))

        with patch(
            "ami.dataops.serve.ansible.subprocess.run",
            return_value=MagicMock(returncode=0),
        ) as mock_run:
            rc = cli.main(["deploy", "--limit", "main", "--check"])

        assert rc == 0
        cmd = mock_run.call_args.args[0]
        assert cmd[-2:] == ["--tags", "deploy"] or "--tags" in cmd
        assert "--check" in cmd
        # Limit propagated as JSON extra_vars
        assert any("tunnel_limit" in arg and "main" in arg for arg in cmd)

    def test_deploy_with_route_dns_runs_both_tags(
        self, ami_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AMI_ROOT", str(ami_root))

        with patch(
            "ami.dataops.serve.ansible.subprocess.run",
            return_value=MagicMock(returncode=0),
        ) as mock_run:
            cli.main(["deploy", "--route-dns"])

        tag_pairs = [
            call.args[0][call.args[0].index("--tags") + 1]
            for call in mock_run.call_args_list
        ]
        assert tag_pairs == ["deploy", "route-dns"]


class TestLogsSubcommand:
    def test_logs_invokes_journalctl(
        self, ami_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AMI_ROOT", str(ami_root))

        with patch(
            "ami.dataops.serve.cli.subprocess.run",
            return_value=MagicMock(returncode=0),
        ) as mock_run:
            rc = cli.main(["logs", "main"])
        assert rc == 0
        cmd = mock_run.call_args.args[0]
        assert cmd[:3] == ["journalctl", "--user", "-u"]
        assert cmd[3] == "ami-serve-main.service"

    def test_logs_handles_missing_journalctl(
        self,
        ami_root: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        monkeypatch.setenv("AMI_ROOT", str(ami_root))

        with patch(
            "ami.dataops.serve.cli.subprocess.run",
            side_effect=FileNotFoundError("journalctl missing"),
        ):
            rc = cli.main(["logs", "main"])
        assert rc == 1
        assert "journalctl" in capsys.readouterr().err


class TestFailurePropagation:
    def test_ansible_failure_returncode_surfaces(
        self, ami_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AMI_ROOT", str(ami_root))

        with patch(
            "ami.dataops.serve.ansible.subprocess.run",
            return_value=MagicMock(returncode=_ANSIBLE_FAILURE_RC),
        ):
            rc = cli.main(["stop"])
        assert rc == _ANSIBLE_FAILURE_RC
