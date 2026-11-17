import asyncio
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from sandbox_harness.app import create_app,supervise_warm_pool
from sandbox_harness.config import Settings


class App:
    def __init__(self):
        self.state=SimpleNamespace(pool_status='starting',pool_attempts=0,pool_last_error='')


async def test_warm_pool_retries_initial_and_partial_failure_then_recovers():
    app=App();release=asyncio.Event()
    class Pool:
        calls=0
        async def execution_available(self):return True
        async def fill(self):
            self.calls+=1
            if self.calls==1:raise RuntimeError('codex target unavailable')
            await release.wait()
    pool=Pool();task=asyncio.create_task(supervise_warm_pool(
        app,pool,initial_delay=.01,maximum_delay=.02,probe_interval=.02))
    for _ in range(100):
        if app.state.pool_status=='degraded' and pool.calls>=2:break
        await asyncio.sleep(.005)
    assert app.state.pool_status=='degraded'
    assert app.state.pool_attempts==2 and app.state.pool_last_error=='RuntimeError'
    release.set()
    for _ in range(100):
        if app.state.pool_status=='ready':break
        await asyncio.sleep(.005)
    assert app.state.pool_status=='ready' and app.state.pool_last_error==''
    task.cancel()
    with pytest.raises(asyncio.CancelledError):await task


async def test_warm_pool_repeated_failure_has_backoff_and_clean_shutdown():
    app=App()
    class Pool:
        calls=0
        async def execution_available(self):return False
        async def fill(self):self.calls+=1;raise ConnectionError('offline')
    pool=Pool();task=asyncio.create_task(supervise_warm_pool(
        app,pool,initial_delay=.02,maximum_delay=.04,probe_interval=.02))
    await asyncio.sleep(.07)
    assert 2<=pool.calls<=4
    assert app.state.pool_status=='unavailable' and app.state.pool_last_error=='ConnectionError'
    task.cancel()
    with pytest.raises(asyncio.CancelledError):await task


async def test_warm_pool_probe_exceptions_recover_without_supervisor_restart():
    app=App()
    class Pool:
        probes=0;fills=0
        async def fill(self):
            self.fills+=1
            if self.fills==1:raise RuntimeError("fill offline")
        async def execution_available(self):
            self.probes+=1
            if self.probes<3:raise ConnectionError("probe offline")
            return True
    pool=Pool();task=asyncio.create_task(supervise_warm_pool(
        app,pool,initial_delay=.01,maximum_delay=.02,probe_interval=.02))
    for _ in range(100):
        if app.state.pool_status=='ready':break
        await asyncio.sleep(.005)
    assert not task.done() and app.state.pool_status=='ready'
    assert app.state.execution_backend_available is True and pool.probes>=3 and pool.fills>=3
    task.cancel()
    with pytest.raises(asyncio.CancelledError):await task


def test_optional_warm_pool_degradation_is_visible_without_failing_api_readiness(tmp_path):
    settings=Settings(data_root=tmp_path,database_url='sqlite+aiosqlite:///'+str(tmp_path/'health.db'),
        redis_url='',backend='fake',object_backend='local',model_catalog_discovery=False,
        session_signing_key='warm-health-secret')
    app=create_app(settings)
    with TestClient(app) as client:
        app.state.pool_status='unavailable';app.state.execution_backend_available=False
        app.state.pool_attempts=3
        app.state.pool_last_error='ConnectionError'
        response=client.get('/api/health')
        assert response.status_code==503
        assert response.json()['status']=='degraded'
        assert response.json()['execution_backend_available'] is False
        assert response.json()['warm_pool_last_error']=='ConnectionError'
        app.state.pool_status='degraded';app.state.execution_backend_available=True
        response=client.get('/api/health')
        assert response.status_code==200 and response.json()['status']=='ok'
        assert response.json()['warm_pool_optional_degraded'] is True
