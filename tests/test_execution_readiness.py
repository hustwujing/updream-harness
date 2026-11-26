import asyncio

import pytest

from sandbox_harness.common.catalog.execution_readiness import (ExecutionReadiness)


async def test_readiness_generation_changes_only_when_health_changes():
    values=[(False,'connection_refused'),(False,'connection_refused'),(True,'')]
    async def probe():return values.pop(0)
    readiness=ExecutionReadiness({'image.decompose':probe},ttl_seconds=0)
    first=(await readiness.snapshot())['image.decompose']
    second=(await readiness.snapshot())['image.decompose']
    third=(await readiness.snapshot())['image.decompose']
    assert not first['available'] and first['generation']==1
    assert not second['available'] and second['generation']==1
    assert third['available'] and third['generation']==2


async def test_unavailable_descriptor_is_rejected_with_stable_reason():
    async def probe():return False,'connection_refused'
    readiness=ExecutionReadiness({'image.decompose':probe},ttl_seconds=0)
    with pytest.raises(ValueError,match='descriptor_unavailable:image.decompose:connection_refused'):
        await readiness.require_available('image.decompose')
    assert (await readiness.require_available('media.render'))['available']


async def test_concurrent_readiness_requests_share_one_probe():
    calls=0
    async def probe():
        nonlocal calls;calls+=1;await asyncio.sleep(.01);return True,''
    readiness=ExecutionReadiness({'image.decompose':probe},ttl_seconds=30)
    await asyncio.gather(*(readiness.require_available('image.decompose') for _ in range(20)))
    assert calls==1
