import asyncio
import io

from PIL import Image
import pytest

from sandbox_harness.common.catalog.catalog import (HarnessCatalog)
from sandbox_harness.common.contracts.domain import (Harness, MediaReferenceInput, ThreadCreate)
from sandbox_harness.common.catalog.media_catalog import (MediaCatalog)
from sandbox_harness.common.catalog.media_references import (resolve_media_references)
from sandbox_harness.common.storage.objects import (ObjectStore, S3ObjectStore)
from test_support.legacy_store import (Store)


def image_reference(role="reference_image", usage="subject", *, identifier="f" * 32):
    return {"project_file_id": identifier, "path": "assets/reference.png", "version": 1,
        "sha256": "1" * 64, "size": 1000, "mime": "image/png", "object_ref": "sha256:" + "1" * 64,
        "role": role, "usage": usage, "width": 1024, "height": 1024,
        "duration_seconds": None, "fps": None}


def video_reference(usage="action", *, duration=5, identifier="e" * 32):
    return {"project_file_id": identifier, "path": "assets/reference.mp4", "version": 1,
        "sha256": "2" * 64, "size": 1000, "mime": "video/mp4", "object_ref": "sha256:" + "2" * 64,
        "role": "reference_video", "usage": usage, "width": 1280, "height": 720,
        "duration_seconds": duration, "fps": 24, "codec":"h264", "audio_codec":"aac"}


def audio_reference(*, duration=5):
    return {"project_file_id": "d" * 32, "path": "assets/reference.mp3", "version": 1,
        "sha256": "3" * 64, "size": 1000, "mime": "audio/mpeg", "object_ref": "sha256:" + "3" * 64,
        "role": "reference_audio", "usage": "audio", "width": None, "height": None,
        "duration_seconds": duration, "fps": None, "codec":"mp3", "audio_codec":""}


def test_media_catalog_enforces_discriminated_reference_modes():
    catalog = MediaCatalog()
    catalog.validate_parameters("image", "seedream-5.0", {"prompt":"edit","size":"2K"},
        generation_mode="reference_image", references=[image_reference()])
    catalog.validate_parameters("video", "sed2", {"prompt":"move","duration":5},
        generation_mode="first_frame", references=[image_reference("first_frame", "composition")])
    catalog.validate_parameters("video", "sed2", {"prompt":"edit","duration":-1,"aspect_ratio":"adaptive"},
        generation_mode="edit_video", references=[video_reference("edit_source")])
    catalog.validate_parameters("video", "sed2", {"prompt":"continue","duration":5,
        "aspect_ratio":"adaptive","extend_direction":"backward"}, generation_mode="extend_video",
        references=[video_reference("extend_source"), image_reference()])
    catalog.validate_parameters("video", "sed2", {"prompt":"sync","duration":5},
        generation_mode="multimodal_reference", references=[image_reference(), audio_reference()])
    with pytest.raises(ValueError, match="does not accept references"):
        catalog.validate_parameters("video", "sed2", {"prompt":"bad","duration":5},
            generation_mode="text_to_video", references=[image_reference()])
    with pytest.raises(ValueError, match="exactly one reference video"):
        catalog.validate_parameters("video", "sed2", {"prompt":"bad","duration":-1,
            "aspect_ratio":"adaptive"}, generation_mode="edit_video", references=[video_reference("action")])
    with pytest.raises(ValueError, match="require an image or video"):
        catalog.validate_parameters("video", "sed2", {"prompt":"bad","duration":5},
            generation_mode="multimodal_reference", references=[audio_reference()])
    with pytest.raises(ValueError, match="total at most 15"):
        catalog.validate_parameters("video", "sed2", {"prompt":"bad","duration":5},
            generation_mode="multimodal_reference", references=[video_reference(duration=10),
                video_reference(duration=10, identifier="c" * 32)])


def test_reference_count_and_media_metadata_boundaries_are_rejected_before_provider():
    catalog=MediaCatalog()
    images=[{**image_reference(identifier=f"{index:032x}"),"size":1000} for index in range(14)]
    catalog.validate_parameters("image","seedream-5.0",{"prompt":"merge","size":"2K"},
        generation_mode="reference_image",references=images)
    with pytest.raises(ValueError,match="1..14"):
        catalog.validate_parameters("image","seedream-5.0",{"prompt":"merge","size":"2K"},
            generation_mode="reference_image",references=images+[image_reference(identifier="f"*32)])
    nine=[image_reference(identifier=f"{index+20:032x}") for index in range(9)]
    catalog.validate_parameters("video","sed2",{"prompt":"move","duration":5},
        generation_mode="multimodal_reference",references=nine)
    with pytest.raises(ValueError,match="at most 9 images"):
        catalog.validate_parameters("video","sed2",{"prompt":"move","duration":5},
            generation_mode="multimodal_reference",references=nine+[image_reference(identifier="e"*32)])
    with pytest.raises(ValueError,match="codec"):
        catalog.validate_parameters("video","sed2",{"prompt":"move","duration":5},
            generation_mode="multimodal_reference",references=[{**video_reference(),"codec":"vp9"}])
    with pytest.raises(ValueError,match="dimensions"):
        catalog.validate_parameters("video","sed2",{"prompt":"move","duration":5},
            generation_mode="multimodal_reference",references=[{**video_reference(),"width":640,"height":480}])
    with pytest.raises(ValueError,match="First/last-frame"):
        catalog.validate_parameters("video","sed2",{"prompt":"move","duration":5},
            generation_mode="first_last_frame",references=[image_reference("first_frame"),
                image_reference("last_frame"),image_reference()])


@pytest.mark.asyncio
async def test_reference_resolution_freezes_owned_current_project_version(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "references.db")); await store.initialize()
    objects = ObjectStore(tmp_path / "objects")
    try:
        owner = "a" * 64
        profile = HarnessCatalog().resolve("codex", "gpt-5.6", "standard")
        thread = await store.create_thread(ThreadCreate(harness=Harness.CODEX, model=profile.model,
            reasoning_level="standard", title="References"), profile, owner)
        image = Image.new("RGB", (640, 480), "blue"); buffer = io.BytesIO(); image.save(buffer, format="PNG")
        data = buffer.getvalue(); object_ref, digest = objects.put(data)
        project_file, _ = await store.register_project_file(thread.id, path="assets/start.png",
            source_type="upload", source_turn_id="", sha256=digest, size=len(data),
            mime="application/octet-stream", object_ref=object_ref)
        snapshots = await resolve_media_references(store, objects, thread.id, owner,
            [MediaReferenceInput(project_path="/workspace/project/assets/start.png",
                role="first_frame", usage="composition")])
        assert snapshots[0]["project_file_id"] == project_file.id
        assert snapshots[0]["object_ref"] == object_ref and snapshots[0]["mime"] == "image/png"
        assert (snapshots[0]["width"], snapshots[0]["height"]) == (640, 480)
        await store.delete_project_file(project_file.id, owner)
        with pytest.raises(ValueError, match="registered ProjectFile"):
            await resolve_media_references(store, objects, thread.id, owner,
                [MediaReferenceInput(project_file_id=project_file.id, role="first_frame", usage="composition")])
    finally:
        await store.close()


def test_s3_media_upload_uses_public_endpoint_and_stable_aniforge_key():
    class Client:
        def generate_presigned_url(self, method, *, Params, ExpiresIn):
            assert method == "put_object" and Params["Bucket"] == "updream"
            assert Params["Key"] == "aniforge/image/11/" + "1" * 64 + ".png"
            assert Params["ContentType"] == "image/png" and ExpiresIn == 900
            return "https://boss-shjd.biliapi.net/signed-put"
    store = S3ObjectStore(bucket="updream", prefix="scope", client=Client(), public_client=Client())
    value=store.prepare_media_upload(kind="image",digest="1"*64,mime="image/png",size=123)
    assert value["put_url"].endswith("signed-put")
    assert value["public_url"] == ("https://boss-shjd.biliapi.net/updream/aniforge/image/11/"
        + "1" * 64 + ".png")
    assert value["headers"]["x-amz-meta-sha256"] == "1" * 64


def test_s3_media_upload_accepts_safe_configured_media_prefix():
    class Client:
        def generate_presigned_url(self, method, *, Params, ExpiresIn):
            assert method == "put_object" and ExpiresIn == 900
            assert Params["Key"] == "aniforge/wujing02/image/11/" + "1" * 64 + ".png"
            return "https://boss-shjd.biliapi.net/signed-put"

    store = S3ObjectStore(bucket="updream", prefix="scope", media_prefix="/aniforge/wujing02/",
        client=Client(), public_client=Client())
    value = store.prepare_media_upload(kind="image", digest="1" * 64,
        mime="image/png", size=123)
    assert value["public_url"] == ("https://boss-shjd.biliapi.net/updream/"
        "aniforge/wujing02/image/11/" + "1" * 64 + ".png")
    for media_prefix in ("", "/", "aniforge/../shared"):
        with pytest.raises(ValueError, match="non-empty safe path"):
            S3ObjectStore(bucket="updream", prefix="scope", media_prefix=media_prefix,
                client=Client(), public_client=Client())


def test_reference_selector_requires_exactly_one_identity():
    with pytest.raises(ValueError):
        MediaReferenceInput(role="reference_image", project_file_id="f" * 32,
            project_path="assets/a.png")
    with pytest.raises(ValueError):
        MediaReferenceInput(role="reference_image")
