import json

import pytest

from sandbox_harness.media_catalog import MediaCatalog, estimate_audio_duration_seconds


def test_default_catalog_contains_only_real_probe_models():
    catalog = MediaCatalog()
    assert {(item["kind"], item["model"]) for item in catalog.public()} == {
        ("image", "seedream-5.0"),
        ("video", "sed2"),
        ("audio", "speech-2.8-turbo"),
    }
    schemas = {item["kind"]: item["parameter_schema"] for item in catalog.public()}
    assert "aspect_ratio" in schemas["video"]["optional"] and "ratio" not in schemas["video"]["optional"]
    assert schemas["video"]["optional"]["duration"] == "integer 4..15 or -1"
    assert set(schemas["video"]["generation_modes"]) == {"text_to_video", "first_frame",
        "first_last_frame", "multimodal_reference", "edit_video", "extend_video"}
    assert schemas["image"]["generation_modes"]["reference_image"]["max_references"] == 14
    video=next(item for item in catalog.public("video") if item["model"]=="sed2")
    assert video["reference_schema"]["recommended_usage"]["first_frame"]=="composition"
    assert "first_frame" not in video["reference_schema"]["usages"]
    assert schemas["image"]["limits"]["min_total_pixels"] == 2560 * 1440
    assert "voice_id" in schemas["audio"]["optional"] and "voice_setting" not in schemas["audio"]["optional"]
    assert schemas["audio"]["limits"]["max_estimated_duration_seconds"] == 180


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


@pytest.mark.parametrize(("kind", "model", "parameters"), [
    ("image", "seedream-5.0", {"prompt": "test", "size": "2048x2048"}),
    ("image", "seedream-5.0", {"prompt": "test", "size": "2K"}),
    ("image", "seedream-5.0", {"prompt": "test", "size": "2560x1440"}),
    ("video", "sed2", {"prompt": "test", "duration": 5, "resolution": "480p",
        "aspect_ratio": "16:9", "generate_audio": False}),
    ("video", "sed2", {"prompt": "test", "duration": 15}),
    ("audio", "speech-2.8-turbo", {"text": "test", "voice_id": "male-qn-qingse",
        "speed": 1, "audio_format": "mp3"}),
])
def test_media_parameters_accept_only_enabled_model_contract(kind, model, parameters):
    MediaCatalog().validate_parameters(kind, model, parameters)


@pytest.mark.parametrize(("kind", "model", "parameters"), [
    ("image", "seedream-5.0", {"prompt": "", "size": "2048x2048"}),
    ("image", "seedream-5.0", {"prompt": "test", "provider_model": "bypass"}),
    ("image", "seedream-5.0", {"prompt": "test", "size": "1920x1080"}),
    ("image", "seedream-5.0", {"prompt": "test", "size": "1536x864"}),
    ("video", "sed2", {"prompt": "test", "duration": 999}),
    ("video", "sed2", {"prompt": "test", "duration": 3}),
    ("audio", "speech-2.8-turbo", {"text": "test", "audio_format": "exe"}),
    ("audio", "speech-2.8-turbo", {"text": "test", "voice_id": "Chinese_Mandarin_Explainer"}),
    ("audio", "speech-2.8-turbo", {"text": "你" * 900}),
])
def test_media_parameters_reject_invalid_or_provider_controlled_values(kind, model, parameters):
    with pytest.raises(ValueError):
        MediaCatalog().validate_parameters(kind, model, parameters)


def test_audio_duration_estimate_includes_speed_words_and_explicit_pauses():
    normal = estimate_audio_duration_seconds("这是中文。 hello world! <#2.5#>", 1)
    faster = estimate_audio_duration_seconds("这是中文。 hello world! <#2.5#>", 2)
    assert normal > faster >= 2.5


def test_seedance_reference_modes_are_gated_by_ark_asset_configuration():
    catalog=MediaCatalog(ark_assets_configured=False)
    video=next(item for item in catalog.public("video") if item["model"]=="sed2")
    assert video["reference_inputs_available"] is False
    reference={"role":"reference_image","usage":"subject","width":640,"height":480,
        "size":1000,"mime":"image/png","duration_seconds":None,"fps":None}
    with pytest.raises(ValueError,match="Ark Asset"):
        catalog.validate_parameters("video","sed2",{"prompt":"move","duration":5},
            generation_mode="multimodal_reference",references=[reference])
