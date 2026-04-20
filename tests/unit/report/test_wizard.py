"""Unit tests for the ami-report interactive wizard.

Drive `wizard.run()` with fully-injected primitives so no real keyboard,
getpass, TUI dialog, or HTTP call is needed. Asserts the flow reaches
`post_bundle` with the correct manifest + secret + token when everything
goes well, and short-circuits cleanly on empty selections / cancels.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ami.dataops.report import wizard
from ami.dataops.report.config import PeerEntry
from ami.dataops.report.defaults import DEFAULT_PEER_NAME
from ami.dataops.report.scanner import FolderEntry, TreeEntry, scan_roots
from ami.dataops.report.transport import PostContext


class _StubInputs:
    """Bundled stub answers for _build_primitives.

    Plain class (not Pydantic / dataclass) so the captured-dict reference
    passes by identity and the post stub can mutate it — Pydantic would
    deep-copy the dict and break the mutation round-trip.
    """

    def __init__(self, **overrides: object) -> None:
        defaults = {
            "sender_input": "",
            "scope_answers": [],
            "scope_labels": None,
            "select_all_tree": True,
            "pick_peer_name": None,
            "secret_values": {},
            "confirm": True,
            "captured": None,
        }
        defaults.update(overrides)
        for key, value in defaults.items():
            object.__setattr__(self, key, value)


def _make_prompt(stub: _StubInputs) -> wizard.Prompter:
    answers_iter = iter(stub.scope_answers or [""])

    def _prompt(question: str, default: str) -> str:
        if "Sender ID" in question:
            return stub.sender_input or default
        return next(answers_iter, "")

    return _prompt


def _make_secret(stub: _StubInputs) -> wizard.SecretPrompter:
    def _secret(question: str) -> str:
        for key, value in stub.secret_values.items():
            if key in question:
                return value
        return "default-secret"

    return _secret


def _make_pick_tree(stub: _StubInputs) -> wizard.PickTreeFn:
    def _pick_tree(entries: list[TreeEntry]) -> list[TreeEntry]:
        if not stub.select_all_tree:
            return []
        for entry in entries:
            if isinstance(entry, FolderEntry) and entry.toggleable:
                return [entry]
        return [e for e in entries if e.toggleable]

    return _pick_tree


def _make_pick_peer(stub: _StubInputs) -> wizard.PickPeerFn:
    def _pick_peer(peers: list[PeerEntry]) -> PeerEntry | None:
        if stub.pick_peer_name is None:
            return None
        for peer in peers:
            if peer.name == stub.pick_peer_name:
                return peer
        return None

    return _pick_peer


def _make_pick_scope(stub: _StubInputs) -> wizard.PickScopeFn:
    def _pick_scope(labels: list[str], _preselected: list[str]) -> list[str] | None:
        return stub.scope_labels if stub.scope_labels is not None else [labels[0]]

    return _pick_scope


def _make_post(stub: _StubInputs) -> wizard.PostBundleFn:
    def _post(ctx: PostContext) -> dict[str, object]:
        if stub.captured is not None:
            stub.captured["ctx"] = ctx
        return {"status": "accept", "bundle_id": ctx.manifest.bundle_id}

    return _post


def _build_primitives(stub: _StubInputs) -> wizard.WizardPrimitives:
    return wizard.WizardPrimitives(
        prompt=_make_prompt(stub),
        secret_prompt=_make_secret(stub),
        pick_scope=_make_pick_scope(stub),
        pick_tree=_make_pick_tree(stub),
        pick_peer=_make_pick_peer(stub),
        confirm=lambda _message: stub.confirm,
        post_bundle=_make_post(stub),
    )


@pytest.fixture
def scratch_tree(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "app.log").write_text("alpha\n")
    (logs / "trace.ndjson").write_text('{"x":1}\n')
    monkeypatch.setenv("AMI_ROOT", str(tmp_path))
    return logs


class TestResolveSenderId:
    def test_accepts_default(self) -> None:
        result = wizard._resolve_sender_id(None, lambda _question, default: default)
        assert result != ""

    def test_rejects_invalid_then_accepts(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        answers = iter(["bad name!", "good-name"])
        value = wizard._resolve_sender_id(
            None, lambda _question, _default: next(answers)
        )
        assert value == "good-name"
        assert "must match" in capsys.readouterr().err


class TestEnsurePeerCredentials:
    def test_prompts_when_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        peer = PeerEntry.model_validate(
            {
                "name": "bravo",
                "endpoint": "https://b.example.com/",
                "shared_secret_env_var": "SECRET_B",
            }
        )
        monkeypatch.delenv("SECRET_B", raising=False)
        monkeypatch.delenv("AMI_REPORT_TOKENS__BRAVO", raising=False)
        answers = iter(["sec-val", "tok-val"])
        secret, token = wizard._ensure_peer_credentials(peer, lambda _q: next(answers))
        assert secret == "sec-val"
        assert token == "tok-val"

    def test_skips_when_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        peer = PeerEntry.model_validate(
            {
                "name": "bravo",
                "endpoint": "https://b.example.com/",
                "shared_secret_env_var": "SECRET_B",
            }
        )
        monkeypatch.setenv("SECRET_B", "env-secret")
        monkeypatch.setenv("AMI_REPORT_TOKENS__BRAVO", "env-token")
        calls: list[str] = []
        secret, token = wizard._ensure_peer_credentials(
            peer, lambda q: calls.append(q) or "never"
        )
        assert calls == []
        assert secret == "env-secret"
        assert token == "env-token"


class TestRunEndToEnd:
    def test_happy_path_reaches_post_bundle(
        self,
        scratch_tree: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("AMI_REPORT_SECRET_REPORTS", raising=False)
        monkeypatch.delenv("AMI_REPORT_TOKENS__REPORTS", raising=False)
        captured: dict = {}
        primitives = _build_primitives(
            _StubInputs(
                sender_input="alpha",
                scope_answers=[""],
                pick_peer_name=DEFAULT_PEER_NAME,
                secret_values={"Secret for reports": "sec", "Bearer token": "tok"},
                captured=captured,
            )
        )
        exit_code = wizard.run(config_path=None, primitives=primitives)
        assert exit_code == wizard.EXIT_OK
        ctx = captured["ctx"]
        assert isinstance(ctx, PostContext)
        assert ctx.manifest.sender_id == "alpha"
        assert len(ctx.manifest.files) >= 1
        assert ctx.bearer_token == "tok"

    def test_empty_tree_selection_exits_zero(self, scratch_tree: Path) -> None:
        primitives = _build_primitives(
            _StubInputs(
                sender_input="alpha",
                scope_answers=[""],
                select_all_tree=False,
                pick_peer_name=DEFAULT_PEER_NAME,
            )
        )
        assert wizard.run(config_path=None, primitives=primitives) == wizard.EXIT_OK

    def test_cancelled_peer_exits_zero(self, scratch_tree: Path) -> None:
        primitives = _build_primitives(
            _StubInputs(
                sender_input="alpha",
                scope_answers=[""],
                pick_peer_name=None,
            )
        )
        assert wizard.run(config_path=None, primitives=primitives) == wizard.EXIT_OK

    def test_cancelled_confirm_exits_zero(
        self, scratch_tree: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AMI_REPORT_SECRET_REPORTS", "x")
        monkeypatch.setenv("AMI_REPORT_TOKENS__REPORTS", "y")
        primitives = _build_primitives(
            _StubInputs(
                sender_input="alpha",
                scope_answers=[""],
                pick_peer_name=DEFAULT_PEER_NAME,
                confirm=False,
            )
        )
        assert wizard.run(config_path=None, primitives=primitives) == wizard.EXIT_OK


class TestScanRootsIntegration:
    """Quick sanity that scanner input fed to wizard produces folder + files."""

    def test_scan_produces_folder_and_files(self, scratch_tree: Path) -> None:
        entries = scan_roots([scratch_tree])
        assert any(isinstance(e, FolderEntry) for e in entries)
        assert any(e.toggleable for e in entries)
