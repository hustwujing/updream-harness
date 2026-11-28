import asyncio
from types import SimpleNamespace

from sandbox_harness.common.contracts.domain import EventKind,RuntimeEvent,RuntimeResult
from sandbox_harness.common.events.runtime_stream import RuntimeStreamBuffer
from sandbox_harness.services.job_worker.harness_job import SandboxChildBackend


async def test_runtime_stream_buffer_batches_text_and_flushes_before_control_event():
    persisted=[];live=[];acked=[]
    async def persist(value):
        persisted.append(value)
        return SimpleNamespace(item_id=(f'item-{value.runtime_sequence}'
            if value.kind==EventKind.TEXT_DELTA else ''))
    async def publish(value):live.append(value)
    async def acknowledge(sequence):acked.append(sequence)
    buffer=RuntimeStreamBuffer(execution_id='execution',persist_event=persist,
        persist_seconds=.03,live_seconds=.01,publish_live=publish,acknowledge=acknowledge)

    await buffer.add(RuntimeEvent(kind=EventKind.TEXT_DELTA,payload={'text':'a'},runtime_sequence=1))
    await buffer.add(RuntimeEvent(kind=EventKind.TEXT_DELTA,payload={'text':'b'},runtime_sequence=2))
    await buffer.add(RuntimeEvent(kind=EventKind.TEXT_DELTA,payload={'text':'c'},runtime_sequence=3))
    await asyncio.sleep(.04)
    await buffer.add(RuntimeEvent(kind=EventKind.TOOL_STARTED,
        payload={'name':'tool'},runtime_sequence=4))
    await buffer.close()

    assert [(value.kind,value.runtime_sequence,value.payload.get('text')) for value in persisted]==[
        (EventKind.TEXT_DELTA,1,'a'),(EventKind.TEXT_DELTA,3,'bc'),
        (EventKind.TOOL_STARTED,4,None)]
    assert persisted[1].payload['sequence_start']==2
    assert acked==[1,3,4]
    assert ''.join(part['text'] for value in live for part in value['parts'])=='abc'
    assert {value['item_id'] for value in live}=={'item-1'}


async def test_text_after_tool_boundary_targets_a_new_agent_item():
    live=[]
    async def persist(value):
        return SimpleNamespace(item_id=(f'item-{value.runtime_sequence}'
            if value.kind==EventKind.TEXT_DELTA else ''))
    async def publish(value):live.append(value)
    buffer=RuntimeStreamBuffer(execution_id='execution',persist_event=persist,
        persist_seconds=5,live_seconds=5,publish_live=publish)
    await buffer.add(RuntimeEvent(kind=EventKind.TEXT_DELTA,payload={'text':'before'},
        runtime_sequence=1))
    await buffer.add(RuntimeEvent(kind=EventKind.TOOL_STARTED,
        payload={'tool_use_id':'tool','name':'shell'},runtime_sequence=2))
    await buffer.add(RuntimeEvent(kind=EventKind.TEXT_DELTA,payload={'text':'after'},
        runtime_sequence=3))
    await buffer.close()
    assert [(value['item_id'],value['text']) for value in live]==[
        ('item-1','before'),('item-3','after')]


async def test_runtime_stream_buffer_never_acknowledges_failed_persistence():
    acked=[]
    async def persist(_value):raise RuntimeError('database_unavailable')
    async def acknowledge(sequence):acked.append(sequence)
    buffer=RuntimeStreamBuffer(execution_id='execution',persist_event=persist,
        persist_seconds=5,acknowledge=acknowledge)
    try:
        await buffer.add(RuntimeEvent(kind=EventKind.TEXT_DELTA,
            payload={'text':'a'},runtime_sequence=1))
    except RuntimeError as exc:
        assert str(exc)=='database_unavailable'
    else:raise AssertionError('persistence failure was hidden')
    assert acked==[]


async def test_runtime_stream_buffer_batches_tool_output_but_preserves_tool_boundaries():
    persisted=[]
    async def persist(value):persisted.append(value)
    buffer=RuntimeStreamBuffer(execution_id='execution',persist_event=persist,
        persist_seconds=5)
    await buffer.add(RuntimeEvent(kind=EventKind.TOOL_STARTED,
        payload={'tool_use_id':'tool-1','name':'shell'},runtime_sequence=1))
    await buffer.add(RuntimeEvent(kind=EventKind.TOOL_OUTPUT,
        payload={'tool_use_id':'tool-1','text':'a'},runtime_sequence=2))
    await buffer.add(RuntimeEvent(kind=EventKind.TOOL_OUTPUT,
        payload={'tool_use_id':'tool-1','text':'b'},runtime_sequence=3))
    await buffer.add(RuntimeEvent(kind=EventKind.TOOL_FINISHED,
        payload={'tool_use_id':'tool-1','name':'shell'},runtime_sequence=4))
    assert [(value.kind,value.runtime_sequence) for value in persisted]==[
        (EventKind.TOOL_STARTED,1),(EventKind.TOOL_OUTPUT,3),(EventKind.TOOL_FINISHED,4)]
    assert persisted[1].payload['text']=='ab' and persisted[1].payload['sequence_start']==2


async def test_child_collector_uses_the_shared_text_batching_and_ack_protocol():
    class Store:
        events=None
        def __init__(self):self.rows=[]
        async def job_runtime_sequence(self,*_args):return 0
        async def append_job_runtime_activity(self,job_id,execution_id,sequence,
                kind,payload,*,status):
            self.rows.append((job_id,execution_id,sequence,kind,payload,status))
    class Runtime:
        def __init__(self):self.acked=[]
        async def resume(self,*_args):
            for sequence,text in ((1,'a'),(2,'b'),(3,'c')):
                yield RuntimeEvent(kind=EventKind.TEXT_DELTA,payload={'text':text},
                    runtime_sequence=sequence)
            yield RuntimeResult(status='completed',runtime_sequence=4)
        async def acknowledge_execution(self,_sandbox,_execution_id,sequence):
            self.acked.append(sequence)
    backend=SandboxChildBackend.__new__(SandboxChildBackend)
    backend.store=Store();backend.runtime=Runtime()
    backend.settings=SimpleNamespace(stream_persist_seconds=5,
        stream_live_publish_seconds=.3)
    context=SimpleNamespace(job=SimpleNamespace(id='job-1'))
    await backend._collect(context,object(),'execution-1')
    assert [(row[2],row[3],row[4]['text']) for row in backend.store.rows]==[
        (1,'text_delta','a'),(3,'text_delta','bc')]
    assert backend.runtime.acked==[1,3]
