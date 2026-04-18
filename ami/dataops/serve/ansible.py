"""Thin wrapper over ansible-playbook invocations for ami-serve."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path


def find_ami_root() -> Path:
    """Resolve AMI_ROOT (env var, else walk up from this file)."""
    env = os.environ.get("AMI_ROOT")
    if env:
        return Path(env)
    current = Path(__file__).resolve()
    while current != current.parent:
        if (current / "pyproject.toml").exists() and (current / "projects").exists():
            return current
        current = current.parent
    msg = "Cannot determine AMI_ROOT"
    raise RuntimeError(msg)


def dataops_root(ami_root: Path) -> Path:
    """Return the AMI-DATAOPS project root."""
    return ami_root / "projects" / "AMI-DATAOPS"


def ansible_playbook(ami_root: Path) -> Path:
    """Return the bootstrapped ansible-playbook binary."""
    return ami_root / ".boot-linux" / "bin" / "ansible-playbook"


def run_playbook(
    tag: str,
    *,
    tunnel_limit: str | None = None,
    check: bool = False,
    extra_vars: dict[str, object] | None = None,
) -> int:
    """Invoke ansible-playbook serve.yml with the given tag.

    Returns the process exit code (0 on success).
    """
    ami_root = find_ami_root()
    playbook = dataops_root(ami_root) / "res" / "ansible" / "serve.yml"
    cmd: list[str] = [
        str(ansible_playbook(ami_root)),
        str(playbook),
        "--tags",
        tag,
    ]
    if check:
        cmd.append("--check")
    if tunnel_limit:
        cmd.extend(["-e", json.dumps({"tunnel_limit": tunnel_limit})])
    if extra_vars:
        cmd.extend(["-e", json.dumps(extra_vars)])
    result = subprocess.run(cmd, check=False, cwd=str(dataops_root(ami_root)))
    return result.returncode
