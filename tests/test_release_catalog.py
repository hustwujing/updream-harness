import importlib.util
import json
from pathlib import Path
import subprocess

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
import pytest


ROOT=Path(__file__).resolve().parents[1]


def release_module():
    spec=importlib.util.spec_from_file_location("record_release",ROOT/"deploy/record-release.py")
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def signed_catalog(tmp_path):
    private=Ed25519PrivateKey.generate();private_path=tmp_path/"private.pem";public_path=tmp_path/"public.pem"
    private_path.write_bytes(private.private_bytes(serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))
    public_path.write_bytes(private.public_key().public_bytes(serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo))
    digest="a"*64;catalog=tmp_path/"profiles.json"
    catalog.write_text(json.dumps({"runtime_dependency_profiles":[{"id":"general-cpu",
        "supported_harnesses":["codex"],"image_digests":{"codex":digest},
        "image_refs":{"codex":"registry/general-cpu-codex@sha256:"+digest}}]},
        sort_keys=True,separators=(",",":"))+"\n")
    signature=catalog.with_suffix(".json.sig")
    signature.write_bytes(subprocess.run(["openssl","pkeyutl","-sign","-rawin","-inkey",
        str(private_path),"-in",str(catalog)],check=True,stdout=subprocess.PIPE).stdout)
    return catalog,public_path


def complete_signed_catalog(tmp_path):
    catalog,public=signed_catalog(tmp_path)
    value=json.loads(catalog.read_text())
    digests={"codex":"b"*64,"claude":"c"*64,"deepseek":"d"*64}
    value["runtime_dependency_profiles"].append({"id":"primary-full-cpu",
        "supported_harnesses":list(digests),"image_digests":digests,
        "image_refs":{key:"registry/primary-"+key+"@sha256:"+digest
            for key,digest in digests.items()}})
    catalog.write_text(json.dumps(value,sort_keys=True,separators=(",",":"))+"\n")
    private=tmp_path/"private.pem"
    catalog.with_suffix(".json.sig").write_bytes(subprocess.run(
        ["openssl","pkeyutl","-sign","-rawin","-inkey",str(private),"-in",str(catalog)],
        check=True,stdout=subprocess.PIPE).stdout)
    return catalog,public


def test_release_publishes_verified_complete_profile_catalog(tmp_path):
    catalog,public=signed_catalog(tmp_path)
    target,digest,signature_digest=release_module().publish_profile_catalog(
        catalog,tmp_path/"published",public)
    assert target.name==digest+".json" and target.read_bytes()==catalog.read_bytes()
    assert target.with_suffix(".json.sig").is_file() and len(signature_digest)==64

    repeated=release_module().publish_profile_catalog(target,tmp_path/"published",public)
    assert repeated==(target,digest,signature_digest)


def test_release_rejects_corrupted_content_addressed_target(tmp_path):
    catalog,public=signed_catalog(tmp_path)
    target,_,_=release_module().publish_profile_catalog(catalog,tmp_path/"published",public)
    target.chmod(0o644);target.write_text("corrupted")
    with pytest.raises(ValueError,match="corrupted"):
        release_module().publish_profile_catalog(catalog,tmp_path/"published",public)


def test_release_rejects_tampered_profile_catalog(tmp_path):
    catalog,public=signed_catalog(tmp_path);catalog.write_text(catalog.read_text()+" ")
    with pytest.raises(subprocess.CalledProcessError):
        release_module().publish_profile_catalog(catalog,tmp_path/"published",public)


def test_release_derives_primary_runtime_images_from_signed_catalog(tmp_path):
    catalog,public=complete_signed_catalog(tmp_path)
    target,_,_=release_module().publish_profile_catalog(catalog,tmp_path/"published",public)
    images=release_module().primary_runtime_images(target)
    assert set(images)=={"codex","claude","deepseek"}
    assert all("primary-" in value and "@sha256:" in value for value in images.values())
