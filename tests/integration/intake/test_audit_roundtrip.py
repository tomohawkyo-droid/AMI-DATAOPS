"""Integration: drive audit.append + rotate_audit + verify_chain across
multiple rotations on a real filesystem and confirm the chain is unbroken.

Catches rotation edge cases that the unit suite exercises in isolation but
that only surface when the whole lifecycle runs start-to-finish.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ami.dataops.intake import audit
from ami.dataops.intake.audit import AuditAppendParams


def _params(bundle_id: str, **overrides: object) -> AuditAppendParams:
    defaults: dict[str, object] = {
        "event": "accept",
        "sender_id": "alpha",
        "bundle_id": bundle_id,
        "remote_addr": "127.0.0.1",
        "byte_count": 0,
        "file_count": 1,
        "reject_reason": None,
        "receipt_sha256": "a" * 64,
    }
    defaults.update(overrides)
    return AuditAppendParams.model_validate(defaults)


_ROTATION_BATCHES = 3
_RECORDS_PER_BATCH = 3


class TestAuditLifecycle:
    def test_three_rotations_chain_intact(self, tmp_path: Path) -> None:
        for batch in range(_ROTATION_BATCHES):
            for i in range(_RECORDS_PER_BATCH):
                audit.append_audit_record(tmp_path, _params(f"batch{batch}-rec{i}"))
            audit.rotate_audit(tmp_path)
        audit.append_audit_record(tmp_path, _params("final"))
        audit.verify_chain(tmp_path)

        sealed = sorted((tmp_path / "audit").glob("*.log"))
        assert len(sealed) == _ROTATION_BATCHES
        active = tmp_path / "audit.log"
        assert active.exists()
        assert active.stat().st_size > 0

    def test_tamper_on_sealed_file_detected_by_verify(self, tmp_path: Path) -> None:
        for i in range(2):
            audit.append_audit_record(tmp_path, _params(f"b{i}"))
        audit.rotate_audit(tmp_path)
        sealed = next((tmp_path / "audit").glob("*.log"))
        sealed.chmod(0o640)
        data = sealed.read_bytes()
        tampered = data.replace(b'"alpha"', b'"ALPHA"', 1)
        sealed.write_bytes(tampered)
        with pytest.raises(RuntimeError, match="chain broken"):
            audit.verify_chain(tmp_path)
