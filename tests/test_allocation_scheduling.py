import asyncio
from dataclasses import replace

import pytest
from datetime import timedelta
from sqlalchemy import text

from test_allocation_dispatch import fixture
from sandbox_harness.allocation_dispatch import AllocationClient, AllocationService
from sandbox_harness.sandbox import FakeSandboxBackend
from sandbox_harness.store import SandboxAllocationRow
from sandbox_harness.domain import now


async def unknown(store, client, context, request):
    identity = await client.enqueue(context, request)
    async with store.connections.begin() as db:
        row = await db.get(SandboxAllocationRow, identity)
        row.state = 'UNKNOWN'
    return identity


async def test_slow_create_does_not_block_unknown_reconciliation(tmp_path):
    class Provider(FakeSandboxBackend):
        def __init__(self):
            super().__init__()
            self.entered = asyncio.Event()
            self.finish = asyncio.Event()
            self.observed = asyncio.Event()

        async def create(self, request):
            self.entered.set()
            await self.finish.wait()
            return await super().create(request)

        async def reconcile_create(self, scope, allocation_id):
            self.observed.set()
            return await super().reconcile_create(scope, allocation_id)

    store, context, request = await fixture(tmp_path)
    provider = Provider()
    client = AllocationClient(store, provider)
    task = None
    try:
        prior = replace(request, allocation_id='unknown-allocation')
        await unknown(store, client, context, prior)
        await client.enqueue(context, request)
        task = asyncio.create_task(AllocationService(store, provider).run_once())
        await asyncio.wait_for(provider.entered.wait(), 2)
        await asyncio.wait_for(provider.observed.wait(), .5)
        assert not task.done(), 'observation must progress while creation is still held'
    finally:
        provider.finish.set()
        if task:
            await asyncio.gather(task, return_exceptions=True)
        await store.close()


async def test_missing_unknown_does_not_starve_later_visible_allocation(tmp_path):
    store, context, request = await fixture(tmp_path)
    provider = FakeSandboxBackend()
    client = AllocationClient(store, provider)
    try:
        await unknown(store, client, context, request)
        visible_request = replace(request, allocation_id='visible-allocation')
        await unknown(store, client, context, visible_request)
        visible = await provider.create(visible_request)
        service = AllocationService(store, provider)
        for _ in range(3):
            await service.reconcile_once(limit=1)
        assert (await client.snapshot(context, visible_request))['handle'] == visible.handle.to_mapping()
        assert (await client.snapshot(context, request))['state'] == 'UNKNOWN'
    finally:
        await store.close()


async def test_runner_refills_slots_and_observes_with_all_create_slots_busy(tmp_path):
    class Provider(FakeSandboxBackend):
        def __init__(self):
            super().__init__()
            self.entered = asyncio.Event()
            self.second = asyncio.Event()
            self.observed = asyncio.Event()
            self.finish = asyncio.Event()
            self.calls = 0

        async def create(self, request):
            self.calls += 1
            if request.allocation_id == 'held':
                self.entered.set()
                await self.finish.wait()
            else:
                self.second.set()
            return await super().create(request)

        async def reconcile_create(self, scope, allocation_id):
            self.observed.set()
            return await super().reconcile_create(scope, allocation_id)

    store, context, request = await fixture(tmp_path)
    provider = Provider()
    client = AllocationClient(store, provider)
    stop = asyncio.Event()
    runner = None
    try:
        first = replace(request, allocation_id='held')
        await client.enqueue(context, first)
        runner = asyncio.create_task(AllocationService(store, provider, lease_seconds=.3).run(stop, concurrency=1))
        await asyncio.wait_for(provider.entered.wait(), 2)
        await client.enqueue(context, request)
        await unknown(store, client, context, replace(request, allocation_id='unknown'))
        await asyncio.wait_for(provider.observed.wait(), 2)
        assert provider.calls == 1, 'full create slots must not admit a second RPC'
        provider.finish.set()
        await asyncio.wait_for(provider.second.wait(), 2)
        assert provider.calls == 2
        stop.set()
        await asyncio.wait_for(runner, 2)
    finally:
        stop.set()
        provider.finish.set()
        if runner:
            await asyncio.gather(runner, return_exceptions=True)
        await store.close()


async def test_original_create_receipt_fences_an_inflight_empty_observation(tmp_path):
    class Provider(FakeSandboxBackend):
        entered = asyncio.Event()
        finish = asyncio.Event()
        pause = False

        async def reconcile_create(self, scope, allocation_id):
            if not self.pause:
                return await super().reconcile_create(scope, allocation_id)
            self.entered.set()
            await self.finish.wait()
            return None

    store, context, request = await fixture(tmp_path)
    provider = Provider()
    client = AllocationClient(store, provider)
    task = None
    try:
        identity = await unknown(store, client, context, request)
        async with store.connections.begin() as db:
            row = await db.get(SandboxAllocationRow, identity)
            row.claim_owner = 'original'
        original = AllocationService(store, provider, owner='original')
        sandbox = await provider.create(request)
        provider.pause = True
        task = asyncio.create_task(AllocationService(store, provider).reconcile_once())
        await asyncio.wait_for(provider.entered.wait(), 2)
        await original._finish(identity, 'READY', handle=sandbox.handle)
        provider.finish.set()
        await task
        assert (await client.snapshot(context, request))['handle'] == sandbox.handle.to_mapping()
        await original._finish(identity, 'UNKNOWN')
        assert (await client.snapshot(context, request))['state'] == 'READY'
    finally:
        provider.finish.set()
        if task:
            await asyncio.gather(task, return_exceptions=True)
        await store.close()


async def test_stale_observer_cannot_publish_after_reconciliation_claim_replacement(tmp_path):
    class Provider(FakeSandboxBackend):
        entered = asyncio.Event()
        finish = asyncio.Event()
        pause = False

        async def reconcile_create(self, scope, allocation_id):
            if not self.pause:
                return await super().reconcile_create(scope, allocation_id)
            self.entered.set()
            await self.finish.wait()
            return await super().reconcile_create(scope, allocation_id)

    store, context, request = await fixture(tmp_path)
    provider = Provider()
    client = AllocationClient(store, provider)
    task = None
    try:
        identity = await unknown(store, client, context, request)
        await provider.create(request)
        provider.pause = True
        service = AllocationService(store, provider)
        task = asyncio.create_task(service.reconcile_once())
        await asyncio.wait_for(provider.entered.wait(), 2)
        async with store.connections.begin() as db:
            row = await db.get(SandboxAllocationRow, identity)
            row.reconcile_expires_at = now()-timedelta(seconds=1)
            row.reconcile_after = now()-timedelta(seconds=1)
        newer = await AllocationService(store, provider)._claim_reconciliations(1)
        provider.finish.set()
        await task
        assert (await client.snapshot(context, request))['state'] == 'UNKNOWN'
        await service._reconcile(newer[0])
        assert (await client.snapshot(context, request))['state'] == 'READY'
    finally:
        provider.finish.set()
        if task:
            await asyncio.gather(task, return_exceptions=True)
        await store.close()


async def test_sqlite_upgrade_preserves_issued_allocations(tmp_path):
    store, context, request = await fixture(tmp_path)
    client = AllocationClient(store, FakeSandboxBackend())
    try:
        identity = await unknown(store, client, context, request)
        async with store.engine.begin() as db:
            await db.execute(text('DROP INDEX ix_sandbox_allocations_due'))
            for column in ('reconcile_after', 'reconcile_attempts', 'reconcile_token', 'reconcile_expires_at'):
                await db.execute(text('ALTER TABLE sandbox_allocations DROP COLUMN '+column))
        await store.initialize()
        await store.initialize()
        value = await client.snapshot(context, request)
        assert value['state'] == 'UNKNOWN' and value['id'] == identity
        assert value['reconcile_attempts'] == 0 and value['reconcile_token'] == ''
        await AllocationService(store, client.backend).reconcile_once()
        assert (await client.snapshot(context, request))['state'] == 'UNKNOWN'
    finally:
        await store.close()


async def test_shutdown_leaves_issued_create_unknown_and_does_not_replay(tmp_path):
    class Provider(FakeSandboxBackend):
        entered = asyncio.Event()
        finish = asyncio.Event()
        calls = 0

        async def create(self, request):
            self.calls += 1
            self.entered.set()
            await self.finish.wait()
            return await super().create(request)

    store, context, request = await fixture(tmp_path)
    provider = Provider()
    client = AllocationClient(store, provider)
    stop = asyncio.Event()
    runner = None
    try:
        await client.enqueue(context, request)
        service = AllocationService(store, provider)
        assert service.lease_seconds == 30
        runner = asyncio.create_task(service.run(stop, concurrency=1))
        await asyncio.wait_for(provider.entered.wait(), 2)
        stop.set()
        await asyncio.wait_for(runner, 2)
        assert (await client.snapshot(context, request))['state'] == 'UNKNOWN'
        await AllocationService(store, provider).run_once()
        assert provider.calls == 1
        assert (await client.snapshot(context, request))['state'] == 'UNKNOWN'
    finally:
        stop.set()
        provider.finish.set()
        if runner:
            await asyncio.gather(runner, return_exceptions=True)
        await store.close()
