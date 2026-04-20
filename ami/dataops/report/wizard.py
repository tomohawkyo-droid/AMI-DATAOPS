"""Interactive wizard launched when `ami-report` is invoked with no args.

Drives the operator through every field `send` needs, filling gaps from
sensible defaults (hostname for sender_id, built-in `reports` peer for
destination) and prompting only for what cannot be inferred. Returns the
CLI exit code directly so the caller can `sys.exit(wizard.run())`.
"""

from __future__ import annotations

import getpass
import json
import os
import re
import socket
import sys
from collections.abc import Callable
from pathlib import Path

import uuid_utils
from pydantic import BaseModel, ConfigDict

from ami.cli_components import dialogs
from ami.dataops.intake import validation
from ami.dataops.report import manifest as manifest_mod
from ami.dataops.report.bundling import build_bundle_tarball
from ami.dataops.report.config import (
    PeerEntry,
    ReportConfig,
    SenderConfig,
    load_report_config,
)
from ami.dataops.report.defaults import merge_default_peer
from ami.dataops.report.scanner import (
    CandidateFile,
    TreeEntry,
    expand_selection,
    scan_roots,
)
from ami.dataops.report.transport import (
    AuthRejected,
    NetworkError,
    PostContext,
    ValidationRejectedByPeer,
    post_bundle,
)
from ami.dataops.report.tui import pick_peer, pick_tree

SENDER_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
EXIT_OK = 0
EXIT_INVALID_ARGS = 2
EXIT_NETWORK_ERROR = 3
EXIT_AUTH_REJECTED = 4
EXIT_VALIDATION_REJECTED_PEER = 5
EXIT_LOCAL_PREFLIGHT_FAILED = 6

Prompter = Callable[[str, str], str]
SecretPrompter = Callable[[str], str]
PickScopeFn = Callable[[list[str], list[str]], list[str] | None]
PickTreeFn = Callable[[list[TreeEntry]], list[TreeEntry]]
PickPeerFn = Callable[[list[PeerEntry]], PeerEntry | None]
ConfirmFn = Callable[[str], bool]
PostBundleFn = Callable[[PostContext], dict[str, object]]


class WizardPrimitives(BaseModel):
    """Injectable I/O primitives so tests can drive the wizard deterministically."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    prompt: Prompter
    secret_prompt: SecretPrompter
    pick_scope: PickScopeFn
    pick_tree: PickTreeFn
    pick_peer: PickPeerFn
    confirm: ConfirmFn
    post_bundle: PostBundleFn


def _default_prompt(question: str, default: str) -> str:
    raw = input(f"{question} [{default}]: ").strip()
    return raw or default


def _default_secret_prompt(question: str) -> str:
    return getpass.getpass(f"{question}: ")


def _default_confirm(message: str) -> bool:
    return bool(dialogs.confirm(message, title="Send report?"))


def _default_pick_scope(labels: list[str], preselected: list[str]) -> list[str] | None:
    result = dialogs.multiselect(
        labels, title="Scope: which roots to scan", preselected=preselected
    )
    if result is None:
        return None
    return [str(item) for item in result]


def default_primitives() -> WizardPrimitives:
    return WizardPrimitives(
        prompt=_default_prompt,
        secret_prompt=_default_secret_prompt,
        pick_scope=_default_pick_scope,
        pick_tree=pick_tree,
        pick_peer=pick_peer,
        confirm=_default_confirm,
        post_bundle=post_bundle,
    )


def _resolve_sender_id(cfg: ReportConfig | None, prompt: Prompter) -> str:
    default = (
        cfg.sender.sender_id
        if cfg is not None and cfg.sender.sender_id
        else socket.gethostname() or "anonymous"
    )
    while True:
        value = prompt("Sender ID", default)
        if SENDER_ID_PATTERN.match(value):
            return value
        print(
            "error: sender_id must match ^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$",
            file=sys.stderr,
        )


def _resolve_scope(
    cfg: ReportConfig, prompt: Prompter, pick_scope: PickScopeFn
) -> list[Path]:
    ami_root = Path(os.environ.get("AMI_ROOT", "")).expanduser()
    candidates: list[tuple[str, Path]] = []
    if ami_root and (ami_root / "logs").is_dir():
        candidates.append(("AMI_ROOT/logs", ami_root / "logs"))
    candidates.extend(
        (f"configured: {extra_cfg}", extra_cfg) for extra_cfg in cfg.sender.extra_roots
    )
    if ami_root and ami_root.is_dir():
        candidates.append(("AMI_ROOT (entire workspace)", ami_root))
    roots: list[Path] = [path for _, path in candidates if path.exists()][:1]
    if candidates:
        labels = [f"{label} ({path})" for label, path in candidates]
        picked = pick_scope(labels, [labels[0]])
        if picked is None:
            return []
        roots = [path for label, path in candidates if f"{label} ({path})" in picked]
    while True:
        extra_input = prompt("Add custom path (blank to finish)", "")
        if not extra_input:
            break
        extra_path = Path(extra_input).expanduser().absolute()
        if not extra_path.exists():
            print(f"warning: {extra_path} does not exist; skipped", file=sys.stderr)
            continue
        if extra_path not in roots:
            roots.append(extra_path)
    return roots


def _load_or_default_config(
    config_path: Path | None, sender_id_fallback: str
) -> ReportConfig:
    if config_path is not None and config_path.is_file():
        cfg = load_report_config(config_path)
    else:
        cfg = ReportConfig(sender=SenderConfig(sender_id=sender_id_fallback), peers=[])
    return merge_default_peer(cfg)


def _ensure_peer_credentials(
    peer: PeerEntry, secret_prompt: SecretPrompter
) -> tuple[str, str]:
    token_env = f"AMI_REPORT_TOKENS__{peer.name.upper()}"
    secret_value = os.environ.get(peer.shared_secret_env_var)
    token_value = os.environ.get(token_env)
    if not secret_value:
        secret_value = secret_prompt(
            f"Secret for {peer.name} ({peer.shared_secret_env_var})"
        )
        os.environ[peer.shared_secret_env_var] = secret_value
    if not token_value:
        token_value = secret_prompt(f"Bearer token for {peer.name} ({token_env})")
        os.environ[token_env] = token_value
    return secret_value, token_value


def _common_source_root(files: list[CandidateFile]) -> Path:
    paths = [c.absolute_path for c in files]
    root = paths[0].parent
    while not all(_is_under(root, p) for p in paths):
        new = root.parent
        if new == root:
            break
        root = new
    return root


def _is_under(root: Path, path: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


class SendRequest(BaseModel):
    """Bundled inputs to `_send` so the signature stays under the arg cap."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    sender_id: str
    peer: PeerEntry
    expanded: list[CandidateFile]
    primitives: WizardPrimitives
    secret: str
    token: str


def _send(request: SendRequest) -> int:
    sender_id = request.sender_id
    peer = request.peer
    expanded = request.expanded
    primitives = request.primitives
    secret = request.secret
    token = request.token
    source_root = _common_source_root(expanded)
    try:
        for candidate in expanded:
            validation.probe_text_content(candidate.absolute_path)
    except validation.ValidationRejected as exc:
        print(f"local pre-flight failed: {exc}", file=sys.stderr)
        return EXIT_LOCAL_PREFLIGHT_FAILED
    bundle_id = str(uuid_utils.uuid7())
    manifest = manifest_mod.build_manifest(
        sender_id=sender_id,
        source_root=source_root,
        files=[c.absolute_path for c in expanded],
        bundle_id=bundle_id,
    )
    manifest_bytes = manifest_mod.canonical_manifest_bytes(manifest)
    signature = manifest_mod.sign_manifest(manifest_bytes, secret)
    summary = (
        f"Destination: {peer.name} ({peer.endpoint})\n"
        f"Bundle id:   {bundle_id}\n"
        f"Files:       {len(expanded)}\n"
    )
    if not primitives.confirm(summary):
        return EXIT_OK
    bundle_bytes = build_bundle_tarball(manifest, source_root)
    ctx = PostContext(
        endpoint=f"{peer.endpoint}v1/bundles",
        bearer_token=token,
        manifest=manifest,
        manifest_bytes=manifest_bytes,
        signature=signature,
        bundle_bytes=bundle_bytes,
    )
    try:
        receipt = primitives.post_bundle(ctx)
    except AuthRejected as exc:
        print(f"auth rejected: {exc}", file=sys.stderr)
        return EXIT_AUTH_REJECTED
    except ValidationRejectedByPeer as exc:
        print(f"validation reject {exc.reason_code}: {exc.detail}", file=sys.stderr)
        return EXIT_VALIDATION_REJECTED_PEER
    except NetworkError as exc:
        print(f"network error: {exc}", file=sys.stderr)
        return EXIT_NETWORK_ERROR
    print(json.dumps(receipt, indent=2))
    return EXIT_OK


def run(
    config_path: Path | None = None,
    primitives: WizardPrimitives | None = None,
) -> int:
    """Run the interactive wizard. Returns the CLI exit code."""
    prim = primitives or default_primitives()
    hostname = socket.gethostname() or "anonymous"
    initial_cfg = _load_or_default_config(config_path, hostname)
    sender_id = _resolve_sender_id(initial_cfg, prim.prompt)
    cfg = ReportConfig(
        sender=initial_cfg.sender.model_copy(update={"sender_id": sender_id}),
        peers=initial_cfg.peers,
    )
    roots = _resolve_scope(cfg, prim.prompt, prim.pick_scope)
    if not roots:
        print("no scan roots chosen; nothing to report", file=sys.stderr)
        return EXIT_OK
    entries = scan_roots(roots)
    if not entries:
        print("no candidate files found under the chosen roots", file=sys.stderr)
        return EXIT_OK
    selected = prim.pick_tree(entries)
    if not selected:
        return EXIT_OK
    expanded = expand_selection(selected, entries)
    if not expanded:
        print("selection expanded to zero files", file=sys.stderr)
        return EXIT_OK
    peer = prim.pick_peer(cfg.peers)
    if peer is None:
        return EXIT_OK
    secret, token = _ensure_peer_credentials(peer, prim.secret_prompt)
    return _send(
        SendRequest(
            sender_id=sender_id,
            peer=peer,
            expanded=expanded,
            primitives=prim,
            secret=secret,
            token=token,
        )
    )
