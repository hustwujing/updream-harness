import json
from pathlib import Path

import pytest

from sandbox_harness.media_catalog import MediaCatalog, estimate_audio_duration_seconds
from sandbox_harness.media_references import inspect_reference


ROOT=Path(__file__).resolve().parents[1]


def test_default_catalog_contains_only_real_probe_models():
    catalog = MediaCatalog()
    assert {(item["kind"], item["model"]) for item in catalog.public()} == {
        ("image", "seedream-5.0"),
        ("video", "sed2"),
        ("video", "happyhorse-video-edit"),
        ("video", "wan-2.7-video-edit"),
        ("audio", "speech-2.8-turbo"),
        ("model3d", "v3.1-20260211"),
    }
    by_identity = {(item["kind"], item["model"]): item for item in catalog.public()}
    schemas = {kind: next(item["parameter_schema"] for item in catalog.public() if item["kind"] == kind)
        for kind in {item["kind"] for item in catalog.public()}}
    video_schema = by_identity[("video", "sed2")]["parameter_schema"]
    assert "aspect_ratio" in video_schema["optional"] and "ratio" not in video_schema["optional"]
    assert video_schema["optional"]["duration"] == "integer 4..15 or -1"
    assert set(video_schema["generation_modes"]) == {"text_to_video", "first_frame",
        "first_last_frame", "multimodal_reference", "edit_video", "extend_video"}
    assert video_schema["generation_modes"]["edit_video"]["fixed_parameters"] == {
        "aspect_ratio": "adaptive", "duration": -1}
    assert video_schema["generation_modes"]["extend_video"]["fixed_parameters"] == {
        "aspect_ratio": "adaptive"}
    assert video_schema["generation_modes"]["extend_video"]["required_parameters"] == ["extend_direction"]
    assert schemas["image"]["generation_modes"]["reference_image"]["max_references"] == 14
    video=next(item for item in catalog.public("video") if item["model"]=="sed2")
    assert video["reference_schema"]["recommended_usage"]["first_frame"]=="composition"
    assert "first_frame" not in video["reference_schema"]["usages"]
    assert schemas["image"]["limits"]["min_total_pixels"] == 2560 * 1440
    assert "voice_id" in schemas["audio"]["optional"] and "voice_setting" not in schemas["audio"]["optional"]
    assert schemas["audio"]["generation_modes"]["text_to_audio"]["references"] == 0
    assert schemas["audio"]["generation_modes"]["reference_voice"]["references"] == 1
    assert "voice_id" not in schemas["audio"]["generation_modes"]["reference_voice"]["allowed_parameters"]
    assert next(item for item in catalog.public("audio"))["reference_schema"]["roles"] == ["reference_audio"]
    assert schemas["audio"]["limits"]["max_estimated_duration_seconds"] == 180
    assert set(schemas["model3d"]["generation_modes"]) == {"text_to_model", "image_to_model",
        "multiview_to_model", "texture_model", "decimate_model", "convert_model"}
    text_mode = schemas["model3d"]["generation_modes"]["text_to_model"]
    assert "image_seed" in text_mode["allowed_parameters"]
    assert "target_format" not in text_mode["allowed_parameters"]
    assert text_mode["effects"]["quad=true"] == "output is FBX"
    assert schemas["model3d"]["limits"]["face_limit"]["smart_low_poly_triangle"] == 20_000


def test_shared_video_edit_fixture_satisfies_every_enabled_edit_provider():
    path=ROOT/'tests/real_cases/assets/video_edit_reference.mp4'
    reference=inspect_reference(path.read_bytes(),'reference_video',path.name)
    reference.update({'role':'reference_video','usage':'edit_source','size':path.stat().st_size})
    parameters={'prompt':'subtle color grade','duration':-1,'resolution':'720p',
        'aspect_ratio':'adaptive'}
    catalog=MediaCatalog()
    for model in ('happyhorse-video-edit','wan-2.7-video-edit'):
        catalog.validate_parameters('video',model,parameters,
            generation_mode='edit_video',references=[reference])


def test_video_catalog_exposes_model_selection_guidance_and_real_mode_boundaries():
    catalog = MediaCatalog(ark_assets_configured=False)
    videos = {item["model"]: item for item in catalog.public("video")}
    assert set(videos) == {"sed2", "happyhorse-video-edit", "wan-2.7-video-edit"}
    assert videos["sed2"]["reference_inputs_available"] is False
    assert videos["happyhorse-video-edit"]["reference_inputs_available"] is True
    assert set(videos["happyhorse-video-edit"]["parameter_schema"]["generation_modes"]) == {"edit_video"}
    assert set(videos["wan-2.7-video-edit"]["parameter_schema"]["generation_modes"]) == {"edit_video"}
    happy = videos["happyhorse-video-edit"]["parameter_schema"]
    wan = videos["wan-2.7-video-edit"]["parameter_schema"]
    assert happy["optional"]["resolution"] == ["720p", "1080p"]
    assert happy["defaults"]["resolution"] == "1080p"
    assert happy["generation_modes"]["edit_video"]["max_images"] == 5
    assert happy["limits"]["input_video_duration_seconds"] == "3..60"
    assert wan["generation_modes"]["edit_video"]["max_images"] == 4
    assert wan["limits"]["input_video_duration_seconds"] == "2..10"
    assert videos["happyhorse-video-edit"]["reference_schema"]["roles"] == [
        "reference_image", "reference_video"]
    assert all(item["guidance"]["strengths"] and item["guidance"]["limitations"]
        and item["guidance"]["selection_rules"] for item in videos.values())


@pytest.mark.parametrize(("model", "duration"), [
    ("happyhorse-video-edit", 60), ("wan-2.7-video-edit", 10)])
def test_dashscope_video_edit_does_not_require_ark_assets(model, duration):
    catalog = MediaCatalog(ark_assets_configured=False)
    source = {"role": "reference_video", "usage": "edit_source", "size": 2_000_000,
        "mime": "video/mp4", "duration_seconds": duration, "width": 1280, "height": 720,
        "fps": 29, "codec": "h264", "audio_codec": "aac"}
    catalog.validate_parameters("video", model,
        {"prompt": "sync lips", "duration": -1, "resolution": "720p", "aspect_ratio": "adaptive"},
        generation_mode="edit_video", references=[source])
    with pytest.raises(ValueError, match="does not support generation mode"):
        catalog.validate_parameters("video", model, {"prompt": "new scene", "duration": 5},
            generation_mode="text_to_video")


def test_dashscope_video_edit_enforces_each_provider_boundary():
    catalog = MediaCatalog(ark_assets_configured=False)
    source = {"role": "reference_video", "usage": "edit_source", "size": 2_000_000,
        "mime": "video/mp4", "duration_seconds": 12, "width": 1280, "height": 720,
        "fps": 29, "codec": "h264", "audio_codec": "aac"}
    parameters = {"prompt": "edit", "duration": -1, "resolution": "1080p",
        "aspect_ratio": "adaptive"}
    catalog.validate_parameters("video", "happyhorse-video-edit", parameters,
        generation_mode="edit_video", references=[source])
    with pytest.raises(ValueError, match="2..10"):
        catalog.validate_parameters("video", "wan-2.7-video-edit", parameters,
            generation_mode="edit_video", references=[source])
    with pytest.raises(ValueError, match="720p or 1080p"):
        catalog.validate_parameters("video", "happyhorse-video-edit",
            {**parameters, "resolution": "480p"}, generation_mode="edit_video",
            references=[{**source, "duration_seconds": 10}])
    with pytest.raises(ValueError, match="does not accept: generate_audio"):
        catalog.validate_parameters("video", "wan-2.7-video-edit",
            {**parameters, "generate_audio": True}, generation_mode="edit_video",
            references=[{**source, "duration_seconds": 10}])


def test_dashscope_video_edit_enforces_reference_image_limits():
    catalog = MediaCatalog()
    source = {"role": "reference_video", "usage": "edit_source", "size": 2_000_000,
        "mime": "video/quicktime", "duration_seconds": 10, "width": 720, "height": 1280,
        "fps": 30, "codec": "h264", "audio_codec": "aac"}
    image = {"role": "reference_image", "usage": "style", "size": 1_000_000,
        "mime": "image/png", "width": 512, "height": 512}
    parameters = {"prompt": "edit", "duration": -1, "aspect_ratio": "adaptive"}
    catalog.validate_parameters("video", "happyhorse-video-edit", parameters,
        generation_mode="edit_video", references=[source, *[dict(image) for _ in range(5)]])
    with pytest.raises(ValueError, match="up to 4"):
        catalog.validate_parameters("video", "wan-2.7-video-edit", parameters,
            generation_mode="edit_video", references=[source, *[dict(image) for _ in range(5)]])
    with pytest.raises(ValueError, match="format, size, dimension"):
        catalog.validate_parameters("video", "happyhorse-video-edit", parameters,
            generation_mode="edit_video", references=[source, {**image, "mime": "image/bmp"}])


def test_tripo_contract_rejects_invalid_reference_and_topology_combinations():
    catalog = MediaCatalog()
    catalog.validate_parameters("model3d", "v3.1-20260211", {"prompt": "a small cube",
        "face_limit": 20_000, "smart_low_poly": True}, generation_mode="text_to_model")
    image = {"role": "reference_image", "usage": "subject", "size": 1024,
        "mime": "image/png", "width": 512, "height": 512}
    catalog.validate_parameters("model3d", "v3.1-20260211", {},
        generation_mode="image_to_model", references=[image])
    with pytest.raises(ValueError, match="topology limit"):
        catalog.validate_parameters("model3d", "v3.1-20260211", {"prompt": "cube",
            "face_limit": 20_001, "smart_low_poly": True}, generation_mode="text_to_model")
    with pytest.raises(ValueError, match="2..4 unique views"):
        catalog.validate_parameters("model3d", "v3.1-20260211", {},
            generation_mode="multiview_to_model", references=[image])
    with pytest.raises(ValueError, match="input_job_id"):
        catalog.validate_parameters("model3d", "v3.1-20260211", {},
            generation_mode="texture_model")
    with pytest.raises(ValueError, match="Unsupported parameter.*image_to_model"):
        catalog.validate_parameters("model3d", "v3.1-20260211", {"prompt": "must not drift"},
            generation_mode="image_to_model", references=[image])


def test_tripo_contract_enforces_provider_parameter_dependencies():
    catalog = MediaCatalog(); job_id = "a" * 32
    catalog.validate_parameters("model3d", "v3.1-20260211", {"prompt": "cube",
        "texture": False, "image_seed": 7, "export_uv": False, "export_orientation": "+x"},
        generation_mode="text_to_model")
    with pytest.raises(ValueError, match="pbr=true conflicts"):
        catalog.validate_parameters("model3d", "v3.1-20260211",
            {"prompt": "cube", "texture": False, "pbr": True}, generation_mode="text_to_model")
    with pytest.raises(ValueError, match="texture=false does not accept"):
        catalog.validate_parameters("model3d", "v3.1-20260211",
            {"prompt": "cube", "texture": False, "texture_quality": "detailed"},
            generation_mode="text_to_model")
    with pytest.raises(ValueError, match="smart_uv requires"):
        catalog.validate_parameters("model3d", "v3.1-20260211",
            {"input_job_id": job_id, "face_limit": 5000, "topology_model": "v2.0", "smart_uv": True},
            generation_mode="decimate_model")
    with pytest.raises(ValueError, match="bake is unsupported"):
        catalog.validate_parameters("model3d", "v3.1-20260211",
            {"input_job_id": job_id, "face_limit": 5000, "topology_model": "v1.0", "bake": True},
            generation_mode="decimate_model")


@pytest.mark.parametrize("mime", ["image/gif", "image/tiff", "image/heic"])
def test_tripo_references_reject_provider_unsupported_image_formats(mime):
    reference = {"role": "reference_image", "usage": "subject", "size": 1024,
        "mime": mime, "width": 512, "height": 512}
    with pytest.raises(ValueError, match="PNG, JPEG or WebP"):
        MediaCatalog().validate_parameters("model3d", "v3.1-20260211", {},
            generation_mode="image_to_model", references=[reference])


@pytest.mark.parametrize("parameters", [
    {"prompt": "cube", "export_orientation": "forward"},
    {"prompt": "cube", "export_uv": "false"},
    {"input_job_id": "a" * 32, "texture_alignment": "closest"},
])
def test_tripo_contract_rejects_invalid_enum_and_boolean_values(parameters):
    mode = "texture_model" if "input_job_id" in parameters else "text_to_model"
    with pytest.raises(ValueError):
        MediaCatalog().validate_parameters("model3d", "v3.1-20260211", parameters,
            generation_mode=mode)


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


def test_explicit_catalog_inherits_builtin_model_guidance_and_modes():
    catalog = MediaCatalog(json.dumps([{
        "kind": "video", "model": "happyhorse-video-edit", "provider": "alibaba",
        "adapter": "happyhorse_video_edit", "route": "/v1/providers/video",
        "execution": "async", "cancellation": "unsupported", "progress": "provider",
        "credential_env": "SANDBOX_HARNESS_UPSTREAM_VIDEO_KEY",
    }]))
    item = catalog.public("video")[0]
    assert set(item["parameter_schema"]["generation_modes"]) == {"edit_video"}
    assert item["guidance"]["selection_rules"]


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


def test_voice_clone_reference_contract_enforces_provider_boundaries():
    catalog=MediaCatalog()
    reference={"role":"reference_audio","usage":"audio","size":1024,
        "mime":"audio/mpeg","duration_seconds":10,"codec":"mp3"}
    catalog.validate_parameters("audio","speech-2.8-turbo",{"text":"hello"},
        generation_mode="reference_voice",references=[reference])
    for invalid in ({**reference,"duration_seconds":9.99},
            {**reference,"duration_seconds":301},
            {**reference,"size":20*1024*1024+1},
            {**reference,"mime":"audio/ogg"}):
        with pytest.raises(ValueError,match="10..300"):
            catalog.validate_parameters("audio","speech-2.8-turbo",{"text":"hello"},
                generation_mode="reference_voice",references=[invalid])
    with pytest.raises(ValueError,match="does not accept voice_id"):
        catalog.validate_parameters("audio","speech-2.8-turbo",
            {"text":"hello","voice_id":"male-qn-qingse"},
            generation_mode="reference_voice",references=[reference])
    with pytest.raises(ValueError,match="exactly one"):
        catalog.validate_parameters("audio","speech-2.8-turbo",{"text":"hello"},
            generation_mode="reference_voice",references=[reference,reference])
    with pytest.raises(ValueError,match="use reference_voice"):
        catalog.validate_parameters("audio","speech-2.8-turbo",{"text":"hello"},
            generation_mode="text_to_audio",references=[reference])


def test_seedance_reference_modes_are_gated_by_ark_asset_configuration():
    catalog=MediaCatalog(ark_assets_configured=False)
    video=next(item for item in catalog.public("video") if item["model"]=="sed2")
    assert video["reference_inputs_available"] is False
    reference={"role":"reference_image","usage":"subject","width":640,"height":480,
        "size":1000,"mime":"image/png","duration_seconds":None,"fps":None}
    with pytest.raises(ValueError,match="Ark Asset"):
        catalog.validate_parameters("video","sed2",{"prompt":"move","duration":5},
            generation_mode="multimodal_reference",references=[reference])


def test_domain_enums_and_public_media_catalog_cannot_drift():
    from sandbox_harness.domain import MediaDraftCreate,MediaReferenceInput
    reference=MediaReferenceInput.model_json_schema()["properties"]
    roles=set(reference["role"]["enum"]);usages=set(reference["usage"]["enum"])
    draft=MediaDraftCreate.model_json_schema()["properties"]["generation_mode"]
    modes=set(next(value["enum"] for value in draft["anyOf"] if "enum" in value))
    public=MediaCatalog().public()
    catalog_roles=set().union(*(set(item["reference_schema"]["roles"]) for item in public))
    catalog_usages=set().union(*(set(item["reference_schema"]["usages"]) for item in public))
    catalog_modes=set().union(*(set(item["parameter_schema"]["generation_modes"]) for item in public))
    assert catalog_roles==roles and catalog_usages==usages and catalog_modes==modes
    for item in public:
        assert set(item["reference_schema"]["recommended_usage"].values()) <= usages


def test_parameter_shape_error_names_unknown_and_allowed_fields():
    with pytest.raises(ValueError,match=r"ratio.*allowed:.*aspect_ratio"):
        MediaCatalog.validate_parameter_shape("video",{"prompt":"test","ratio":"16:9"})
    with pytest.raises(ValueError,match=r"language_boost, voice_setting.*allowed:.*voice_id"):
        MediaCatalog.validate_parameter_shape("audio",{"text":"test","voice_setting":{},
            "language_boost":"Chinese"})
