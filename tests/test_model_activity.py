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


async def test_child_job_model_activity_keeps_observability_without_primary_items():
    class Store:
        def __init__(self):self.persisted=0;self.updated=0
        async def persist_runtime_event(self,*args,**kwargs):
            self.persisted+=1
            return SimpleNamespace(item_id='primary-item')
        async def update_model_activity(self,*args,**kwargs):
            self.updated+=1
    store=Store()
    activity=ModelActivity(store,SimpleNamespace(thread_id='a'*32,turn_id='b'*32,
        job_id='c'*32,sandbox_id='sandbox-1',model='gpt-5.6'))
    await activity.start()
    await activity.observe(b'data: {"type":"response.output_text.delta","delta":"hi"}\n\n')
    await activity.observe(b'data: {"type":"response.completed","response":{"usage":'
        b'{"input_tokens":9,"output_tokens":3}}}\n\n')
    await activity.finish(200)
    assert activity.item_id is None and activity.finished
    assert store.persisted==0 and store.updated==0
    assert activity.first_output_ms is not None
    assert activity.probe.input_tokens==9 and activity.probe.output_tokens==3


async def test_latest_model_activity_error_returns_only_stable_failed_model_step(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'latest-model.db'));await store.initialize()
    try:
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard')
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model=profile.model,title='test'),profile,'owner')
        turn,_,_=await store.create_turn(thread.id,TurnCreate(message='hello',idempotency_key='latest-model'),profile,'owner')
        activity=ModelActivity(store,SimpleNamespace(thread_id=thread.id,turn_id=turn.id,model='gpt-5.6'))
        await activity.start();await activity.finish(429,'model_token_budget_exhausted')
        assert await store.latest_model_activity_error(turn.id)=='model_token_budget_exhausted'
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


def test_usage_normalizes_openai_claude_and_deepseek_cache_dimensions():
    rows=[
        b'data: {"type":"response.completed","response":{"usage":{"input_tokens":100,"output_tokens":5,"input_tokens_details":{"cached_tokens":80,"cache_write_tokens":10}}}}\n\n',
        b'data: {"type":"message_start","message":{"usage":{"input_tokens":10,"cache_read_input_tokens":80,"cache_creation_input_tokens":10}}}\n\n'
        b'data: {"type":"message_delta","usage":{"output_tokens":5}}\n\n',
        b'{"usage":{"prompt_tokens":100,"completion_tokens":5,"prompt_cache_hit_tokens":80,"prompt_cache_miss_tokens":20}}',
    ]
    expected=[(10,80,10,5,36),(10,80,10,5,36),(20,80,0,5,33)]
    for raw,value in zip(rows,expected):
        probe=OutputProbe();probe.feed(raw);probe.finish()
        usage=probe.token_usage
        assert (usage.ordinary_input_tokens,usage.cache_read_input_tokens,
            usage.cache_write_input_tokens,usage.output_tokens,
            usage.budget_charge(10,125))==value
        assert probe.input_tokens==100 and probe.output_tokens==5


def test_usage_ignores_malformed_cache_breakdown_without_losing_valid_totals():
    probe=OutputProbe();probe.feed(
        b'data: {"usage":{"input_tokens":42,"output_tokens":3,"input_tokens_details":{"cached_tokens":99}}}\n\n')
    assert probe.input_tokens==42 and probe.output_tokens==3
    assert probe.token_usage.ordinary_input_tokens==42
    assert probe.token_usage.cache_read_input_tokens==0
    probe.feed(b'data: {"usage":{"input_tokens":"secret","output_tokens":-1}}\n\n')
    assert probe.input_tokens==42 and probe.output_tokens==3
