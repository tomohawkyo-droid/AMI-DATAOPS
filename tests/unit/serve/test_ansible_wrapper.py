"""Tests for the ansible-playbook subprocess wrapper."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from ami.dataops.serve import ansible as ansible_mod

_NONZERO_RC = 7


class TestRunPlaybook:
    def test_basic_invocation(self) -> None:
        with (
            patch("ami.dataops.serve.ansible.find_ami_root", return_value=MagicMock()),
            patch(
                "ami.dataops.serve.ansible.subprocess.run",
                return_value=MagicMock(returncode=0),
            ) as mock_run,
        ):
            rc = ansible_mod.run_playbook("deploy")
        assert rc == 0
        cmd = mock_run.call_args.args[0]
        assert cmd[-2:] == ["--tags", "deploy"]

    def test_limit_is_passed_as_extra_vars(self) -> None:
        with (
            patch("ami.dataops.serve.ansible.find_ami_root", return_value=MagicMock()),
            patch(
                "ami.dataops.serve.ansible.subprocess.run",
                return_value=MagicMock(returncode=0),
            ) as mock_run,
        ):
            ansible_mod.run_playbook("deploy", tunnel_limit="edge")
        cmd = mock_run.call_args.args[0]
        # The -e flag lives right after --tags deploy
        idx = cmd.index("-e")
        payload = json.loads(cmd[idx + 1])
        assert payload == {"tunnel_limit": "edge"}

    def test_check_flag_appended(self) -> None:
        with (
            patch("ami.dataops.serve.ansible.find_ami_root", return_value=MagicMock()),
            patch(
                "ami.dataops.serve.ansible.subprocess.run",
                return_value=MagicMock(returncode=0),
            ) as mock_run,
        ):
            ansible_mod.run_playbook("deploy", check=True)
        cmd = mock_run.call_args.args[0]
        assert "--check" in cmd

    def test_nonzero_propagated(self) -> None:
        with (
            patch("ami.dataops.serve.ansible.find_ami_root", return_value=MagicMock()),
            patch(
                "ami.dataops.serve.ansible.subprocess.run",
                return_value=MagicMock(returncode=_NONZERO_RC),
            ),
        ):
            rc = ansible_mod.run_playbook("stop")
        assert rc == _NONZERO_RC
