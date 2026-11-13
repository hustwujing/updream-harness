import asyncio
from datetime import timedelta
import hashlib
import io
from pathlib import Path
import tarfile
from types import SimpleNamespace

from fastapi.testclient import TestClient
import pytest

from sandbox_harness.app import create_app
from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.config import Settings
from sandbox_harness.domain import Harness, ThreadCreate, TurnCreate, now
from sandbox_harness.objects import ObjectStore
from sandbox_harness.site_bundle import build_site_archive, validate_site_archive
from sandbox_harness.site_worker import SiteWorker
from sandbox_harness.store import Store
from sandbox_harness.sessions import COOKIE, Sessions


def site(root: Path, title: str = "Hello") -> tuple[bytes, list]:
    root.mkdir(parents=True)
    (root / "index.html").write_text(
        f"<!doctype html><title>{title}</title><script src=app.js></script><h1>{title}</h1>")
    (root / "app.js").write_text("document.body.dataset.ready='yes'")
    return build_site_archive(root)


async def context(tmp_path, title="Hello", owner="a" * 64):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "site.db")); await store.initialize()
    objects = ObjectStore(tmp_path / "objects")
    profile = HarnessCatalog().resolve("codex", "gpt-5.6", "standard")
    thread = await store.create_thread(ThreadCreate(harness=Harness.CODEX, model=profile.model,
        reasoning_level="standard", title="site"), profile, owner)
    turn, _, _ = await store.create_turn(thread.id,
        TurnCreate(message="build", idempotency_key="site-turn-key"), profile, owner)
    archive, files = site(tmp_path / ("source-" + title.lower()), title)
    reference, digest = objects.put(archive)
    staged = []
    for value in files:
        object_ref, _ = objects.put(value.data)
        staged.append({"path":value.path, "sha256":value.sha256, "size":value.size,
            "mime":value.mime, "object_ref":object_ref})
    draft, created = await store.create_site_draft(owner, thread.id, turn.id, name=title,
        slug="interactive-demo", idempotency_key="site-draft-" + title.lower(),
        archive_object_ref=reference, bundle_sha256=digest, archive_size=len(archive),
        file_count=len(files), total_size=sum(value.size for value in files),
        expires_at=now() + timedelta(hours=1), public_base_path="/sandbox-harness/sites",
        files=staged)
    return store, objects, owner, thread, turn, draft, archive


@pytest.mark.asyncio
async def test_site_job_is_durable_versioned_and_recoverable(tmp_path):
    store, objects, owner, thread, _, draft, _ = await context(tmp_path)
    settings = SimpleNamespace(site_worker_concurrency=2)
    first_worker = SiteWorker(settings, store, objects)
    try:
        job = await store.confirm_site_draft(draft.id, owner)
        repeated = await store.confirm_site_draft(draft.id, owner)
        assert repeated.id == job.id
        claimed = await store.claim_site_jobs(first_worker.owner, 1)
        assert len(claimed) == 1
        assert await store.release_site_job_leases(first_worker.owner) == 1
        replacement = SiteWorker(settings, store, objects)
        recovered = await store.claim_site_jobs(replacement.owner, 1)
        assert recovered[0]["id"] == job.id
        await replacement.process(recovered[0])
        completed = await store.get_site_job(job.id, owner)
        assert completed.status == "succeeded" and completed.phase == "activated"
        projects = await store.list_site_projects(thread.id, owner)
        assert len(projects) == 1 and projects[0].status == "active"
        value = await store.public_site_file(projects[0].slug, "index.html")
        assert objects.get(value["object_ref"], maximum=1024 * 1024).find(b"Hello") > 0

        second_archive, second_files = site(tmp_path / "source-second", "Second")
        second_ref, second_digest = objects.put(second_archive)
        second_staged=[]
        for value in second_files:
            object_ref,_=objects.put(value.data);second_staged.append({"path":value.path,
                "sha256":value.sha256,"size":value.size,"mime":value.mime,"object_ref":object_ref})
        second_draft, _ = await store.create_site_draft(owner, thread.id, "", name="Second",
            slug=projects[0].slug, idempotency_key="site-draft-second",
            archive_object_ref=second_ref, bundle_sha256=second_digest,
            archive_size=len(second_archive), file_count=len(second_files),
            total_size=sum(value.size for value in second_files),
            expires_at=now() + timedelta(hours=1), public_base_path="/sandbox-harness/sites",
            files=second_staged)
        second_job = await store.confirm_site_draft(second_draft.id, owner)
        claimed = await store.claim_site_jobs(replacement.owner, 1); await replacement.process(claimed[0])
        assert (await store.get_site_job(second_job.id, owner)).status == "succeeded"
        project = (await store.list_site_projects(thread.id, owner))[0]
        assert len(project.versions) == 2 and project.versions[0].active
        old = project.versions[1]
        rolled_back = await store.activate_site_version(project.id, old.id, owner)
        assert next(version for version in rolled_back.versions if version.id == old.id).active
        unpublished = await store.unpublish_site(project.id, owner)
        assert unpublished.status == "unpublished"
        assert await store.public_site_file(project.slug, "index.html") is None
    finally:
        await store.close()


def malicious_archive(name: str, *, symlink: bool = False) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        info = tarfile.TarInfo(name)
        if symlink:
            info.type = tarfile.SYMTYPE; info.linkname = "index.html"
            archive.addfile(info)
        else:
            content = b"bad"; info.size = len(content); archive.addfile(info, io.BytesIO(content))
    return output.getvalue()


def test_site_bundle_rejects_path_escape_links_and_missing_index(tmp_path):
    with pytest.raises(ValueError, match="invalid_site_path"):
        validate_site_archive(malicious_archive("../index.html"))
    with pytest.raises(ValueError, match="special_file"):
        validate_site_archive(malicious_archive("index.html", symlink=True))
    source = tmp_path / "source"; source.mkdir(); (source / "app.js").write_text("ok")
    with pytest.raises(ValueError, match="site_index_missing"):
        build_site_archive(source)
    (source / "index.html").write_text("ok"); (source / "link.js").symlink_to(source / "app.js")
    with pytest.raises(ValueError, match="site_source_symlink"):
        build_site_archive(source)


def test_public_site_serves_isolated_html_ranges_and_stops_after_unpublish(tmp_path):
    sessions = Sessions("site-public-session-secret"); owner, token, _ = sessions.resolve(None)
    settings = Settings(data_root=tmp_path, database_url="sqlite+aiosqlite:///" + str(tmp_path / "site.db"),
        redis_url="", backend="fake", object_backend="local", model_catalog_discovery=False,
        session_signing_key="site-public-session-secret")

    async def prepare():
        store, objects, _, thread, _, draft, _ = await context(tmp_path, owner=owner)
        worker = SiteWorker(SimpleNamespace(site_worker_concurrency=1), store, objects)
        await store.confirm_site_draft(draft.id, owner)
        claimed = await store.claim_site_jobs(worker.owner, 1); await worker.process(claimed[0])
        project = (await store.list_site_projects(thread.id, owner))[0]
        await store.close()
        return project, owner, draft.id

    project, owner, draft_id = asyncio.run(prepare())
    with TestClient(create_app(settings)) as client:
        client.cookies.set(COOKIE, token)
        preview = client.get(f"/api/sites/drafts/{draft_id}/preview/")
        assert preview.status_code == 200 and b"Hello" in preview.content
        assert "sandbox allow-scripts" in preview.headers["content-security-policy"]
        response = client.get(f"/sites/{project.slug}/")
        assert response.status_code == 200 and b"Hello" in response.content
        assert "sandbox allow-scripts" in response.headers["content-security-policy"]
        assert "sandbox_harness_session" not in response.cookies
        partial = client.get(f"/sites/{project.slug}/app.js", headers={"Range":"bytes=0-7"})
        assert partial.status_code == 206 and partial.headers["content-range"].startswith("bytes 0-7/")
        assert client.get(f"/sites/{project.slug}/missing.js").status_code == 404

    async def stop():
        store = Store(settings.database_url); await store.initialize()
        await store.unpublish_site(project.id, owner); await store.close()
    asyncio.run(stop())
    with TestClient(create_app(settings)) as client:
        assert client.get(f"/sites/{project.slug}/").status_code == 404
