from types import SimpleNamespace
import asyncio
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest
from sqlalchemy import select

from sandbox_harness.common.configuration.config import (Settings)
from sandbox_harness.services.job_worker.job_service import (resolved_node_profiles, serve)
from sandbox_harness.common.db.store import (NodeRow, Store)


ROOT=Path(__file__).resolve().parents[1]


def test_job_service_entrypoint_configures_dedicated_telemetry():
    source=(ROOT/"src/sandbox_harness/services/job_worker/job_service.py").read_text()
    assert 'configure("job-worker",settings.data_root/"logs")' in source


def runtime_profile(identity="general-cpu",reference=True,provider="opensandbox"):
    digests={"codex":"a"*64,"claude":"b"*64,"deepseek":"c"*64}
    refs=({harness:(f"registry/{identity}-{harness}@sha256:{digest}"
        if provider=="opensandbox" else f"template-{identity}-{harness}")
        for harness,digest in digests.items()} if reference else {})
    class RuntimeProfile(SimpleNamespace):
        def release(self,selected,harness):
            if selected!=self.provider or harness not in self.releases:
                raise ValueError("provider release unavailable")
            return self.releases[harness]
    return RuntimeProfile(id=identity,supported_harnesses=tuple(digests),
        provider=provider,releases={harness:SimpleNamespace(ref=ref,artifact_digest=digests[harness])
            for harness,ref in refs.items()})


def test_job_service_registers_every_digest_pinned_profile_harness_pair():
    values=resolved_node_profiles(SimpleNamespace(runtimes={"general":runtime_profile()},
        provider="opensandbox"))
    assert {(row.runtime_profile_id,row.harness) for row in values}=={
        ("general-cpu","codex"),("general-cpu","claude"),("general-cpu","deepseek")}
    assert all(row.image_ref.endswith("@sha256:"+row.image_digest) for row in values)


def test_job_service_accepts_provider_opaque_refs_and_refuses_incomplete_matrix():
    with pytest.raises(ValueError,match="unavailable"):
        resolved_node_profiles(SimpleNamespace(runtimes={"general":runtime_profile(reference=False)},
            provider="opensandbox"))
    value=runtime_profile();value.releases.pop("deepseek")
    with pytest.raises(ValueError,match="unavailable"):
        resolved_node_profiles(SimpleNamespace(runtimes={"general":value},provider="opensandbox"))
    opaque=resolved_node_profiles(SimpleNamespace(runtimes={"general":runtime_profile(provider="aliyun")},
        provider="aliyun"))
    assert all(row.image_ref.startswith("template-") for row in opaque)


async def test_job_service_registers_node_and_stops_without_deleting_shared_state(tmp_path):
    payload=json.loads((ROOT/"deploy/profile-catalog.example.json").read_text())
    raw=json.dumps(payload,sort_keys=True,separators=(",",":"));catalog=tmp_path/"profiles.json"
    catalog.write_text(raw);database="sqlite+aiosqlite:///"+str(tmp_path/"service.db")
    settings=Settings(backend="fake",data_root=tmp_path,database_url=database,redis_url="",
        object_backend="local",profile_catalog_path=catalog,
        profile_catalog_digest=hashlib.sha256(raw.encode()).hexdigest(),
        job_node_id="test-node",job_node_heartbeat_seconds=1)
    stop=asyncio.Event();task=asyncio.create_task(serve(settings,stop_event=stop))
    for _ in range(100):
        try:
            with sqlite3.connect(tmp_path/"service.db") as connection:
                row=connection.execute("select id from nodes where id='test-node'").fetchone()
        except sqlite3.OperationalError:row=None
        if row:break
        await asyncio.sleep(.01)
    else:pytest.fail("Job Service did not register its node")
    stop.set();await asyncio.wait_for(task,5)
    store=Store(database)
    try:
        await store.initialize()
        async with store.connections() as connection:
            row=(await connection.execute(select(NodeRow).where(NodeRow.id=="test-node"))).scalar_one()
        assert row.status=="ready" and row.generation>=1
    finally:await store.close()
