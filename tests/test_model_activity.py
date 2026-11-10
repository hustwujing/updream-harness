import json
from types import SimpleNamespace
from sandbox_harness.model_activity import OutputProbe,ModelActivity
from sandbox_harness.domain import Harness,ThreadCreate,TurnCreate,TurnState
from sandbox_harness.store import Store
from sandbox_harness.catalog import HarnessCatalog

def test_probe_ignores_headers_and_metadata_until_effective_model_output():
    probe=OutputProbe()
    assert not probe.feed(b'data: {"type":"response.created"}\n\n')
    assert not probe.feed(b'data: {"type":"response.output_text.')
    assert probe.feed(b'delta","delta":"hello"}\n\n')
    assert not probe.feed(b'data: {"type":"response.completed"}\n\n')
    assert probe.completed and not probe.failed
    claude=OutputProbe()
    assert claude.feed(b'data: {"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"x"}}\n\n')
    assert not claude.feed(b'data: {"type":"error","error":{"type":"overloaded_error"}}\n\n')
    assert claude.failed

async def test_model_wait_stage_records_actual_model_and_updates_one_item(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'models.db'));await store.initialize()
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,title='test'),profile,'owner')
        turn,_,_=await store.create_turn(thread.id,TurnCreate(message='hello',idempotency_key='model-wait-test'),profile,'owner')
        activity=ModelActivity(store,SimpleNamespace(thread_id=thread.id,turn_id=turn.id,model='frozen-model'))
        await activity.start()
        item=(await store.items_after(thread.id))[-1]
        assert item.summary=='等待 frozen-model 响应' and item.status=='in_progress'
        await activity.observe(b'data: {"type":"response.output_text.delta","delta":"hi"}\n\n')
        changed=(await store.items_after(thread.id))[-1]
        assert changed.id==item.id and changed.summary=='frozen-model 正在输出'
        await activity.finish(503)
        final=(await store.items_after(thread.id))[-1]
        assert final.id==item.id and final.status=='failed'
        assert final.payload['http_status']==503 and final.payload['first_output_ms'] is not None
        assert 'hi' not in json.dumps(final.payload)
    finally:await store.close()


def test_malformed_observation_frames_do_not_break_model_stream():
    probe=OutputProbe()
    for frame in [[],{'type':[]},{'type':'content_block_delta','delta':'bad'},{'choices':['bad',{}]}]:
        assert not probe.feed(('data: '+json.dumps(frame)+'\n\n').encode())


def test_usage_logging_distinguishes_missing_and_reported_zero():
    probe=OutputProbe()
    assert probe.input_tokens is None and probe.output_tokens is None
    probe.feed(b'data: {"type":"message_start","message":{"usage":{"input_tokens":42}}}\n\n')
    probe.feed(b'data: {"type":"message_delta","usage":{"output_tokens":0}}\n\n')
    assert probe.input_tokens==42 and probe.output_tokens==0
    probe.feed(b'data: {"usage":{"input_tokens":"secret","output_tokens":-1}}\n\n')
    assert probe.input_tokens==42 and probe.output_tokens==0
