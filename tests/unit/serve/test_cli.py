"""Argparse-level smoke tests for ami-serve CLI."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from ami.dataops.serve import cli


class TestCliHelp:
    def test_help_contains_prog_name(self, capsys: pytest.CaptureFixture[str]) -> None:
        parser = cli._build_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["--help"])
        assert exc.value.code == 0
        out = capsys.readouterr().out
        assert "ami-serve" in out

    def test_unknown_command_exits_nonzero(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        parser = cli._build_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["bogus-cmd"])
        assert exc.value.code != 0


class TestDispatch:
    def test_deploy_invokes_playbook(self) -> None:
        with (
            patch("ami.dataops.serve.cli.run_playbook", return_value=0) as mock_run,
            patch(
                "ami.dataops.serve.cli.find_ami_root",
                return_value=__import__("pathlib").Path("/tmp/fake-ami"),
            ),
            patch("ami.dataops.serve.cli.os.chdir"),
        ):
            rc = cli.main(["deploy"])
        assert rc == 0
        mock_run.assert_called_once_with("deploy", tunnel_limit=None, check=False)

    def test_deploy_with_route_dns_calls_twice(self) -> None:
        with (
            patch("ami.dataops.serve.cli.run_playbook", return_value=0) as mock_run,
            patch(
                "ami.dataops.serve.cli.find_ami_root",
                return_value=__import__("pathlib").Path("/tmp/fake-ami"),
            ),
            patch("ami.dataops.serve.cli.os.chdir"),
        ):
            cli.main(["deploy", "--route-dns"])
        _expected_calls = 2  # deploy + route-dns
        assert mock_run.call_count == _expected_calls
        assert mock_run.call_args_list[0].args == ("deploy",)
        assert mock_run.call_args_list[1].args == ("route-dns",)

    def test_stop_with_limit(self) -> None:
        with (
            patch("ami.dataops.serve.cli.run_playbook", return_value=0) as mock_run,
            patch(
                "ami.dataops.serve.cli.find_ami_root",
                return_value=__import__("pathlib").Path("/tmp/fake-ami"),
            ),
            patch("ami.dataops.serve.cli.os.chdir"),
        ):
            cli.main(["stop", "--limit", "edge"])
        mock_run.assert_called_once_with("stop", tunnel_limit="edge")

    def test_no_command_prints_help(self, capsys: pytest.CaptureFixture[str]) -> None:
        with (
            patch(
                "ami.dataops.serve.cli.find_ami_root",
                return_value=__import__("pathlib").Path("/tmp/fake-ami"),
            ),
            patch("ami.dataops.serve.cli.os.chdir"),
        ):
            rc = cli.main([])
        assert rc == 0
        assert "ami-serve" in capsys.readouterr().out
