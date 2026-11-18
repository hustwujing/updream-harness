import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile

import httpx
from fastapi.testclient import TestClient
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from sandbox_harness.domain import Harness
from sandbox_harness.runtime import RuntimeClient
from sandbox_harness.sandbox import Sandbox
from sandbox_runtime import app as runtime_app
from sandbox_runtime.instructions import RUNTIME_INSTRUCTIONS, runtime_instructions
from sandbox_runtime.runtime_bundle import activate_runtime_bundle
from sandbox_runtime.skills import bundle_root


ROOT = Path(__file__).resolve().parents[1]


def signing_key(tmp_path: Path) -> tuple[Path, Path]:
    private = Ed25519PrivateKey.generate()
    private_path = tmp_path / "signing.pem"; public_path = tmp_path / "public.pem"
    private_path.parent.mkdir(parents=True, exist_ok=True)
    private_path.write_bytes(private.private_bytes(serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    public_path.write_bytes(private.public_key().public_bytes(serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo))
    return private_path, public_path


def built_bundle(tmp_path: Path, key_pair: tuple[Path, Path] | None = None) -> tuple[bytes, bytes, str, Path]:
    private_path, public_path = key_pair or signing_key(tmp_path)
    output = subprocess.check_output([sys.executable, str(ROOT / "runtime/build_runtime_bundle.py"),
        "--output-dir", str(tmp_path), "--signing-key", str(private_path)], text=True)
    value = json.loads(output)
    return (Path(value["path"]).read_bytes(), Path(value["signature_path"]).read_bytes(),
        value["sha256"], public_path)


def test_runtime_bundle_build_is_deterministic_and_activation_is_atomic(tmp_path, monkeypatch):
    keys = signing_key(tmp_path / "keys")
    data, _, digest, _ = built_bundle(tmp_path / "published", keys)
    repeated, _, repeated_digest, _ = built_bundle(tmp_path / "repeated", keys)
    assert data == repeated and digest == repeated_digest == hashlib.sha256(data).hexdigest()
    assert (tmp_path / "published").stat().st_mode & 0o777 == 0o755
    activated = activate_runtime_bundle(data, digest, tmp_path / "run")
    assert activated["files"] > 1000
    assert (tmp_path / "run/runtime-bundles/current").resolve().name == digest
    assert (Path(activated["root"]) / "skills/manifest.json").is_file()
    monkeypatch.setenv("SANDBOX_RUNTIME_BUNDLE_ROOT", activated["root"])
    assert runtime_instructions() == RUNTIME_INSTRUCTIONS
    assert "sandbox-job job --job-id JOB_ID" in RUNTIME_INSTRUCTIONS
    assert "sandbox-job group --group-id GROUP_ID" in RUNTIME_INSTRUCTIONS
    assert "sandbox-job delegate --request JSON" in RUNTIME_INSTRUCTIONS
    assert "you must run `sandbox-job catalog`" in RUNTIME_INSTRUCTIONS
    assert "before any long research or implementation tool phase" in RUNTIME_INSTRUCTIONS
    assert "may be delegated with an empty `inputs` list" in RUNTIME_INSTRUCTIONS
    assert "Do not poll in a model loop" in RUNTIME_INSTRUCTIONS
    assert "`gpu_runtime_unavailable`" in RUNTIME_INSTRUCTIONS
    assert "Do not change parameters, create a replacement Job" in RUNTIME_INSTRUCTIONS
    assert "follow the selected Skill's required workflow" in RUNTIME_INSTRUCTIONS
    assert bundle_root() == Path(activated["root"]) / "skills"


def test_runtime_bundle_rejects_digest_mismatch_and_links(tmp_path):
    data, _, digest, _ = built_bundle(tmp_path / "published")
    with pytest.raises(ValueError, match="digest_mismatch"):
        activate_runtime_bundle(data, "0" * 64, tmp_path / "run")
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w") as archive:
        link = tarfile.TarInfo("skills/escape"); link.type = tarfile.SYMTYPE; link.linkname = "/etc/passwd"
        archive.addfile(link)
    malicious = payload.getvalue()
    with pytest.raises(ValueError, match="member_invalid"):
        activate_runtime_bundle(malicious, hashlib.sha256(malicious).hexdigest(), tmp_path / "run")


def test_runtime_bundle_endpoint_requires_active_digest_before_configuration(tmp_path, monkeypatch):
    data, signature, digest, public_path = built_bundle(tmp_path / "published")
    monkeypatch.setenv("SANDBOX_RUNTIME_BUNDLE_PUBLIC_KEY", str(public_path))
    monkeypatch.setattr(runtime_app, "RUN", tmp_path / "run")
    monkeypatch.setattr(runtime_app, "WORKSPACE", tmp_path / "workspace")
    monkeypatch.setattr(runtime_app, "OUTPUTS", tmp_path / "workspace/outputs")
    app = runtime_app.create_app()
    with TestClient(app) as client:
        mismatch = client.post("/v1/configure", json={"capability_token": "x" * 32,
            "gateway_url": "http://gateway:28600", "harness": "claude", "model": "model",
            "proxy_url": "http://172.31.0.1:28601", "runtime_bundle_digest": digest})
        unsigned = client.put("/v1/runtime-bundle", params={"sha256": digest}, content=data)
        installed = client.put("/v1/runtime-bundle", params={"sha256": digest}, content=data,
            headers={"X-Runtime-Bundle-Signature": signature.hex()})
        configured = client.post("/v1/configure", json={"capability_token": "x" * 32,
            "gateway_url": "http://gateway:28600", "harness": "claude", "model": "model",
            "proxy_url": "http://172.31.0.1:28601", "runtime_bundle_digest": digest})
    assert mismatch.status_code == 409
    assert unsigned.status_code == 400
    assert installed.status_code == 200 and installed.json()["sha256"] == digest
    assert configured.status_code == 200 and configured.json()["skills"]["skill_count"] == 0


@pytest.mark.asyncio
async def test_host_injects_bundle_once_then_configures(tmp_path):
    data, signature, digest, _ = built_bundle(tmp_path / "published")
    requests = []
    active = {"digest": ""}
    def respond(request: httpx.Request):
        requests.append(request)
        if request.method == "GET": return httpx.Response(200, json={"sha256": active["digest"]})
        if request.method == "PUT":
            assert hashlib.sha256(request.content).hexdigest() == digest
            assert bytes.fromhex(request.headers["X-Runtime-Bundle-Signature"]) == signature
            active["digest"] = digest; return httpx.Response(200, json={"sha256": digest})
        return httpx.Response(200, json={"configured": True})
    runtime = RuntimeClient(bundle_dir=tmp_path / "published", default_bundle_digest=digest,
        transport=httpx.MockTransport(respond))
    sandbox = Sandbox(id="sandbox", harness=Harness.CLAUDE, image="image",
        runtime_url="http://runtime", metadata={})
    await runtime.inject(sandbox, capability_token="x" * 32, gateway_url="http://gateway:28600",
        proxy_url="http://proxy:28601", harness="claude", model="model")
    assert [request.method for request in requests] == ["GET", "PUT", "POST"]
    assert json.loads(requests[-1].content)["runtime_bundle_digest"] == digest
