import json

import pytest

from sandbox_harness.media_catalog import MediaCatalog


def test_default_catalog_contains_only_real_probe_models():
    catalog = MediaCatalog()
    assert {(item["kind"], item["model"]) for item in catalog.public()} == {
        ("image", "seedream-5.0"),
        ("video", "sed2"),
        ("audio", "speech-2.8-turbo"),
    }


def test_catalog_rejects_sync_cancellation():
    with pytest.raises(ValueError, match="cannot be cancellable"):
        MediaCatalog(json.dumps([{
            "kind": "image", "model": "x", "provider": "p", "adapter": "a",
            "route": "/v1/providers/image", "execution": "sync",
            "cancellation": "supported", "progress": "none",
            "credential_env": "MEDIA_KEY",
        }]))


def test_public_catalog_does_not_expose_credential_environment():
    value = MediaCatalog().public()
    assert all("credential_env" not in item for item in value)
