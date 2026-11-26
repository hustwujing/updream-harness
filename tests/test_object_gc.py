import os
from datetime import datetime, timezone

from sandbox_harness.common.storage.object_gc import ObjectGarbageCollector
from sandbox_harness.common.storage.objects import ObjectStore


async def test_object_gc_dry_run_grace_and_second_reference_check(tmp_path):
    store=ObjectStore(tmp_path/"objects")
    kept,_=store.put(b"kept")
    rescued,_=store.put(b"rescued-on-second-check")
    deleted,_=store.put(b"delete-me")
    for value in (kept,rescued,deleted):
        path=store._path(value.removeprefix("sha256:"))
        os.utime(path,(1,1))
    calls=0
    async def references():
        nonlocal calls;calls+=1
        return {kept} if calls==1 else {kept,rescued}
    audit=[]
    async def record(value):audit.append(value)
    gc=ObjectGarbageCollector(store,references,audit=record)

    dry=await gc.run(grace_seconds=60,dry_run=True)
    assert dry["candidate_count"]==2 and dry["deleted_count"]==0
    assert store.size(deleted,maximum=100)>0

    calls=0
    result=await gc.run(grace_seconds=60,dry_run=False)
    assert result["candidate_count"]==2
    assert result["rescued_count"]==1 and result["deleted_count"]==1
    assert store.size(rescued,maximum=100)>0
    try:store.size(deleted,maximum=100)
    except FileNotFoundError:pass
    else:raise AssertionError("unreferenced object was not deleted")
    assert len(audit)==1 and audit[0]["object_ref"]==deleted


async def test_object_gc_never_selects_objects_inside_grace_window(tmp_path):
    store=ObjectStore(tmp_path/"objects")
    recent,_=store.put(b"recent")
    gc=ObjectGarbageCollector(store,lambda:set())
    result=await gc.run(grace_seconds=3600,dry_run=False)
    assert result["candidate_count"]==0 and store.size(recent,maximum=100)==6
