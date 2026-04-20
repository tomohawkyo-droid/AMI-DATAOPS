"""Integration: every rejection path of the intake daemon, end-to-end.

Drives the real FastAPI app through a live uvicorn (conftest fixture)
with hand-crafted malicious bundles and confirms each hits the correct
HTTP status + reason_code + audit event.
"""

from __future__ import annotations

import hashlib
import hmac
import io
import json
import tarfile

import httpx
import rfc8785
from fastapi import status
from pydantic import BaseModel

from ami.dataops.intake import audit

from .conftest import BEARER_TOKEN, SENDER_ID, SHARED_SECRET, LoopbackEnv

_BUNDLE_ID = "019daa7d-0000-7000-9000-000000000001"


def _sign(manifest_bytes: bytes, secret: str = SHARED_SECRET) -> str:
    digest = hmac.new(
        secret.encode("utf-8"), manifest_bytes, hashlib.sha256
    ).hexdigest()
    return f"sha256={digest}"


def _build_manifest(
    files: dict[str, bytes], *, bundle_id: str, sender_id: str = SENDER_ID
) -> bytes:
    manifest_dict = {
        "schema_version": 1,
        "sender_id": sender_id,
        "sent_at": "2026-04-20T08:12:00Z",
        "bundle_id": bundle_id,
        "source_root": "/tmp/src",
        "files": [
            {
                "relative_path": rel,
                "sha256": hashlib.sha256(body).hexdigest(),
                "size_bytes": len(body),
                "mtime": "2026-04-20T08:11:04Z",
            }
            for rel, body in files.items()
        ],
    }
    return rfc8785.dumps(manifest_dict) + b"\n"


def _build_tarball(files: dict[str, bytes], *, tar_mutator: object = None) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for rel, body in files.items():
            info = tarfile.TarInfo(name=rel)
            info.size = len(body)
            tar.addfile(info, io.BytesIO(body))
        if callable(tar_mutator):
            tar_mutator(tar)
    return buf.getvalue()


class _PostArgs(BaseModel):
    """Typed POST inputs so `_post` stays under the arg cap."""

    manifest_bytes: bytes
    tarball: bytes
    signature: str
    bundle_id: str = _BUNDLE_ID
    sender_id: str = SENDER_ID
    bearer: str = BEARER_TOKEN


def _post(loopback: LoopbackEnv, args: _PostArgs) -> httpx.Response:
    return httpx.post(
        f"{loopback.base_url}/v1/bundles",
        headers={
            "Authorization": f"Bearer {args.bearer}",
            "X-AMI-Sender-Id": args.sender_id,
            "X-AMI-Bundle-Id": args.bundle_id,
            "X-AMI-Signature": args.signature,
        },
        files={
            "manifest": ("m.json", args.manifest_bytes, "application/json"),
            "bundle": ("b.tar.gz", args.tarball, "application/gzip"),
        },
        timeout=10.0,
    )


def _args_for(files: dict[str, bytes], bundle_id: str = _BUNDLE_ID) -> _PostArgs:
    """Default `_PostArgs` for a file set: correct manifest + tarball + HMAC."""
    manifest = _build_manifest(files, bundle_id=bundle_id)
    return _PostArgs(
        manifest_bytes=manifest,
        tarball=_build_tarball(files),
        signature=_sign(manifest),
    )


class TestRejectsByReason:
    def test_ext_not_allowed(self, loopback: LoopbackEnv) -> None:
        response = _post(loopback, _args_for({"bad.exe": b"MZ\n"}))
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "ext_not_allowed" in response.text

    def test_path_traversal_rejected(self, loopback: LoopbackEnv) -> None:
        response = _post(loopback, _args_for({"../escape.log": b"boom\n"}))
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "path_unsafe" in response.text

    def test_symlink_member_rejected(self, loopback: LoopbackEnv) -> None:
        def _add_symlink(tar: tarfile.TarFile) -> None:
            info = tarfile.TarInfo(name="evil.log")
            info.type = tarfile.SYMTYPE
            info.linkname = "/etc/passwd"
            tar.addfile(info)

        files = {"ok.log": b"ok\n"}
        manifest = _build_manifest(files, bundle_id=_BUNDLE_ID)
        args = _PostArgs(
            manifest_bytes=manifest,
            tarball=_build_tarball(files, tar_mutator=_add_symlink),
            signature=_sign(manifest),
        )
        response = _post(loopback, args)
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "path_unsafe" in response.text

    def test_not_text_rejected(self, loopback: LoopbackEnv) -> None:
        response = _post(loopback, _args_for({"app.log": b"ok\x00bin\n"}))
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "not_text" in response.text

    def test_schema_version_mismatch(self, loopback: LoopbackEnv) -> None:
        files = {"app.log": b"ok\n"}
        manifest_dict = {
            "schema_version": 99,
            "sender_id": SENDER_ID,
            "sent_at": "2026-04-20T08:12:00Z",
            "bundle_id": _BUNDLE_ID,
            "source_root": "/tmp/src",
            "files": [
                {
                    "relative_path": "app.log",
                    "sha256": hashlib.sha256(b"ok\n").hexdigest(),
                    "size_bytes": 3,
                    "mtime": "2026-04-20T08:11:04Z",
                }
            ],
        }
        manifest = rfc8785.dumps(manifest_dict) + b"\n"
        response = _post(
            loopback,
            _PostArgs(
                manifest_bytes=manifest,
                tarball=_build_tarball(files),
                signature=_sign(manifest),
            ),
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "schema_unsupported" in response.text


class TestAuthRejects:
    def test_missing_bearer_is_401(self, loopback: LoopbackEnv) -> None:
        files = {"app.log": b"ok\n"}
        manifest = _build_manifest(files, bundle_id=_BUNDLE_ID)
        response = httpx.post(
            f"{loopback.base_url}/v1/bundles",
            headers={
                "X-AMI-Sender-Id": SENDER_ID,
                "X-AMI-Bundle-Id": _BUNDLE_ID,
                "X-AMI-Signature": _sign(manifest),
            },
            files={
                "manifest": ("m.json", manifest, "application/json"),
                "bundle": ("b.tar.gz", _build_tarball(files), "application/gzip"),
            },
            timeout=10.0,
        )
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_wrong_bearer_is_401(self, loopback: LoopbackEnv) -> None:
        args = _args_for({"app.log": b"ok\n"}).model_copy(update={"bearer": "wrong"})
        response = _post(loopback, args)
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_wrong_hmac_is_401(self, loopback: LoopbackEnv) -> None:
        files = {"app.log": b"ok\n"}
        manifest = _build_manifest(files, bundle_id=_BUNDLE_ID)
        response = _post(
            loopback,
            _PostArgs(
                manifest_bytes=manifest,
                tarball=_build_tarball(files),
                signature=_sign(manifest, secret="not-the-real-secret"),
            ),
        )
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_header_sender_mismatch_is_401(self, loopback: LoopbackEnv) -> None:
        files = {"app.log": b"ok\n"}
        manifest = _build_manifest(
            files, bundle_id=_BUNDLE_ID, sender_id="someone-else"
        )
        response = _post(
            loopback,
            _PostArgs(
                manifest_bytes=manifest,
                tarball=_build_tarball(files),
                signature=_sign(manifest),
                sender_id=SENDER_ID,
            ),
        )
        assert response.status_code == status.HTTP_401_UNAUTHORIZED


class TestIdempotency:
    def test_replay_returns_200(self, loopback: LoopbackEnv) -> None:
        files = {"app.log": b"ok\n"}
        manifest = _build_manifest(files, bundle_id=_BUNDLE_ID)
        tar = _build_tarball(files)
        args = _PostArgs(
            manifest_bytes=manifest,
            tarball=tar,
            signature=_sign(manifest),
        )
        first = _post(loopback, args)
        assert first.status_code == status.HTTP_202_ACCEPTED
        second = _post(loopback, args)
        assert second.status_code == status.HTTP_200_OK
        assert second.json()["bundle_id"] == _BUNDLE_ID


class TestAuditSideEffects:
    def test_reject_still_increments_audit_log(self, loopback: LoopbackEnv) -> None:
        _post(loopback, _args_for({"bad.exe": b"no-go\n"}))
        audit_log = loopback.intake_root / "audit.log"
        assert audit_log.exists()
        records = [json.loads(line) for line in audit_log.read_bytes().splitlines()]
        reject_records = [r for r in records if r["event"] == "reject"]
        assert len(reject_records) == 1
        assert reject_records[0]["reject_reason"] == "ext_not_allowed"
        audit.verify_chain(loopback.intake_root)
