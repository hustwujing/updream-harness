import pytest
from sandbox_harness.common.catalog.catalog import (HarnessCatalog)
from sandbox_harness.common.contracts.domain import (Harness, ThreadCreate, TurnCreate, TurnState)
from test_support.legacy_store import (Store)


async def seed(store):
    profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
    thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,title='Unread'),profile,'owner')
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message='hello',idempotency_key='first-read-case'),profile,'owner')
    return profile,thread,turn


async def test_completed_turn_is_unread_until_viewed_and_survives_reload(tmp_path):
    url='sqlite+aiosqlite:///'+str(tmp_path/'read.db')
    store=Store(url);await store.initialize()
    try:
        _,thread,turn=await seed(store)
        assert not (await store.list_thread_summaries('owner'))[0]['unread_completion']
        await store.set_turn_state(turn.id,TurnState.COMPLETED)
        summary=(await store.list_thread_summaries('owner'))[0]
        assert summary['unread_completion'] and summary['latest_turn_id']==turn.id
        with pytest.raises(KeyError):await store.mark_thread_read(thread.id,turn.id,'other-owner')
        assert await store.mark_thread_read(thread.id,turn.id,'owner')==turn.id
        assert await store.mark_thread_read(thread.id,turn.id,'owner')==turn.id
        assert not (await store.list_thread_summaries('owner'))[0]['unread_completion']
    finally:await store.close()
    restored=Store(url);await restored.initialize()
    try:assert not (await restored.list_thread_summaries('owner'))[0]['unread_completion']
    finally:await restored.close()


async def test_running_turn_cannot_be_preread_and_old_read_cannot_hide_new_completion(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'race.db'));await store.initialize()
    try:
        profile,thread,first=await seed(store)
        await store.set_turn_state(first.id,TurnState.COMPLETED)
        await store.mark_thread_read(thread.id,first.id,'owner')
        await store.claim_next_turn(thread.id,first.id)
        second,_,_=await store.create_turn(thread.id,TurnCreate(message='again',idempotency_key='second-read-case'),profile,'owner')
        assert await store.mark_thread_read(thread.id,second.id,'owner')==first.id
        await store.set_turn_state(second.id,TurnState.COMPLETED)
        await store.mark_thread_read(thread.id,first.id,'owner')
        assert (await store.list_thread_summaries('owner'))[0]['unread_completion']
        await store.mark_thread_read(thread.id,second.id,'owner')
        await store.mark_thread_read(thread.id,first.id,'owner')
        assert not (await store.list_thread_summaries('owner'))[0]['unread_completion']
    finally:await store.close()
