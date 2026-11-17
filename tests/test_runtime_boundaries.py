"""Process-level fault cases; these do not call a real model Provider."""
import asyncio
import json
import os
import sys

import pytest
from fastapi import HTTPException
import sandbox_runtime.app as runtime_app
from sandbox_runtime.adapters import HarnessCommand


def endpoint(app, path):
    return next(r.endpoint for r in app.routes if getattr(r, 'path', '') == path)


def runtime(tmp_path, monkeypatch, source):
    monkeypatch.setattr(runtime_app, 'RUN', tmp_path / 'run')
    monkeypatch.setattr(runtime_app, 'WORKSPACE', tmp_path / 'workspace')
    monkeypatch.setattr(runtime_app, 'OUTPUTS', tmp_path / 'workspace/outputs')
    monkeypatch.setenv('HARNESS_KIND', 'codex')
    runner = tmp_path / 'runner.py'
    runner.write_text(source)
    monkeypatch.setattr(runtime_app, 'command', lambda *args: HarnessCommand(
        [sys.executable, str(runner)], {}, streaming_input=True, control_input=True))
    return runtime_app.create_app()


async def completed(app):
    await asyncio.wait_for(asyncio.shield(app.state.execution.reader_task), 8)
    return app.state.execution.result


async def test_concurrent_start_submits_once_and_replays_after_completion(tmp_path, monkeypatch):
    receipt = tmp_path / 'receipt'
    app = runtime(tmp_path, monkeypatch, f'''import sys,json,time
msg=json.loads(sys.stdin.readline())
open({str(receipt)!r}, 'a').write('received\\n')
time.sleep(.15)
print(json.dumps({{'type':'harness.input.completed','input_ids':['first']}}),flush=True)
''')
    real_spawn = asyncio.create_subprocess_exec
    calls = []
    async def spawn(*args, **kwargs):
        calls.append(args)
        await asyncio.sleep(.03)
        return await real_spawn(*args, **kwargs)
    monkeypatch.setattr(runtime_app.asyncio, 'create_subprocess_exec', spawn)
    req = runtime_app.TurnRequest(message='hello', execution_id='one', input_id='first')
    start = endpoint(app, '/v1/turns')
    await asyncio.gather(*(start(req) for _ in range(12)))
    assert (await completed(app))['status'] == 'completed'
    await start(req)
    assert len(calls) == 1 and receipt.read_text() == 'received\n'
    with pytest.raises(HTTPException) as exc:
        await start(req.model_copy(update={'message': 'changed'}))
    assert exc.value.status_code == 409


async def test_rejected_execution_and_input_do_not_overwrite_attachments(tmp_path, monkeypatch):
    app = runtime(tmp_path, monkeypatch, 'import sys,time\nsys.stdin.readline()\ntime.sleep(60)\n')
    req = runtime_app.TurnRequest(message='first', execution_id='one', attachments=[{'name':'original'}])
    await endpoint(app, '/v1/turns')(req)
    try:
        for path, rejected in [('/v1/turns', req.model_copy(update={'execution_id':'other'})),
                ('/v1/inputs', req.model_copy(update={'execution_id':'other'})),
                ('/v1/inputs', req.model_copy(update={'message':'changed', 'attachments':[{'name':'bad'}]}))]:
            with pytest.raises(HTTPException) as exc:
                await endpoint(app, path)(rejected)
            assert exc.value.status_code == 409
        manifest = tmp_path / 'workspace/inbox/attachments.json'
        assert json.loads(manifest.read_text()) == [{'name':'original'}]
    finally:
        await endpoint(app, '/v1/cancel')()
        assert (await completed(app))['status'] == 'cancelled'


async def test_stream_eof_without_result_is_failure(tmp_path, monkeypatch):
    app = runtime(tmp_path, monkeypatch, 'import sys\nsys.stdin.readline()\nprint("[]",flush=True)\n')
    await endpoint(app, '/v1/turns')(runtime_app.TurnRequest(message='hello'))
    result = await completed(app)
    assert result['status'] == 'failed' and result['error_code'] == 'harness_result_missing'
    assert result['input_ids'] == ['initial']


async def test_non_object_stdout_does_not_destroy_valid_completion(tmp_path, monkeypatch):
    app = runtime(tmp_path, monkeypatch, '''import sys,json
sys.stdin.readline()
for value in ([], 1, None, {'type':'harness.input.completed','input_ids':['initial']}):
 print(json.dumps(value),flush=True)
''')
    await endpoint(app, '/v1/turns')(runtime_app.TurnRequest(message='hello'))
    assert (await completed(app))['status'] == 'completed'


async def test_each_control_input_gets_a_fresh_model_start_timeout(tmp_path,monkeypatch):
    app=runtime(tmp_path,monkeypatch,'''import sys,json,time
first=json.loads(sys.stdin.readline());time.sleep(.05)
print(json.dumps({'type':'harness.input.completed','input_ids':[first['id']]}),flush=True)
second=json.loads(sys.stdin.readline());time.sleep(5)
''')
    app.state.model_start_timeout_seconds=.2
    app.state.event_idle_timeout_seconds=2
    await endpoint(app,'/v1/turns')(runtime_app.TurnRequest(
        message='first',execution_id='timeouts',input_id='first'))
    await asyncio.sleep(.02)
    accepted=await endpoint(app,'/v1/inputs')(runtime_app.TurnRequest(
        message='second',execution_id='timeouts',input_id='second'))
    assert accepted['accepted']
    result=await asyncio.wait_for(completed(app),1.8)
    assert result['status']=='failed' and result['error_code']=='model_response_timeout'
    assert 'first' in result['input_ids'] and 'second' in result['input_ids']


async def test_invalid_nested_frame_stops_process_before_failure(tmp_path, monkeypatch):
    app = runtime(tmp_path, monkeypatch, '''import sys,json,time
sys.stdin.readline()
print(json.dumps({'method':'item/started','params':[1]}),flush=True)
time.sleep(60)
''')
    await endpoint(app, '/v1/turns')(runtime_app.TurnRequest(message='hello'))
    assert (await completed(app))['status'] == 'failed'
    assert app.state.execution.process.returncode is not None
    assert app.state.process is None


async def test_stop_kills_background_job_ignoring_sigterm(tmp_path, monkeypatch):
    app = runtime(tmp_path, monkeypatch, 'import sys,time\nsys.stdin.readline()\ntime.sleep(60)\n')
    await endpoint(app, '/v1/turns')(runtime_app.TurnRequest(message='hello'))
    marker = tmp_path / 'ready'
    job = await endpoint(app, '/v1/background-jobs')(runtime_app.BackgroundJobCreate(
        name='ignores-term', cwd=str(tmp_path/'workspace/project'), argv=[sys.executable, '-c',
        f'import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); open({str(marker)!r},"w").close(); time.sleep(60)']))
    for _ in range(100):
        if marker.exists(): break
        await asyncio.sleep(.01)
    assert marker.exists()
    await asyncio.wait_for(endpoint(app, '/v1/cancel')(), 7)
    assert (await completed(app))['status'] == 'cancelled'
    saved = app.state.background_jobs[job['id']]
    assert saved.status == 'cancelled' and saved.process.returncode is not None
    with pytest.raises(ProcessLookupError):
        os.kill(saved.pid, 0)


async def test_background_intent_persistence_failure_never_spawns_process(tmp_path,monkeypatch):
    app=runtime(tmp_path,monkeypatch,'import sys,time\nsys.stdin.readline()\ntime.sleep(60)\n')
    await endpoint(app,'/v1/turns')(runtime_app.TurnRequest(message='hello'))
    calls=0;original=runtime_app.asyncio.create_subprocess_exec
    async def spawn(*args,**kwargs):
        nonlocal calls;calls+=1;return await original(*args,**kwargs)
    monkeypatch.setattr(runtime_app.asyncio,'create_subprocess_exec',spawn)
    monkeypatch.setattr(runtime_app,'atomic_json',lambda *args:(_ for _ in ()).throw(OSError('disk full')))
    with pytest.raises(OSError,match='disk full'):
        await endpoint(app,'/v1/background-jobs')(runtime_app.BackgroundJobCreate(
            name='never-spawn',argv=[sys.executable,'-c','import time;time.sleep(60)'],
            cwd=str(tmp_path/'workspace/project')))
    assert calls==0 and not app.state.background_jobs
    await endpoint(app,'/v1/cancel')();await completed(app)


async def test_background_binding_persistence_failure_kills_spawned_group_and_terminalizes_job(tmp_path,monkeypatch):
    app=runtime(tmp_path,monkeypatch,'import sys,time\nsys.stdin.readline()\ntime.sleep(60)\n')
    await endpoint(app,'/v1/turns')(runtime_app.TurnRequest(message='hello'))
    original=runtime_app.atomic_json;calls=0
    def fail_second(path,value):
        nonlocal calls;calls+=1
        if calls==2:raise OSError('binding disk full')
        return original(path,value)
    monkeypatch.setattr(runtime_app,'atomic_json',fail_second)
    with pytest.raises(OSError,match='binding disk full'):
        await endpoint(app,'/v1/background-jobs')(runtime_app.BackgroundJobCreate(
            name='publish-fails',argv=[sys.executable,'-c','import time;time.sleep(60)'],
            cwd=str(tmp_path/'workspace/project')))
    job=next(iter(app.state.background_jobs.values()))
    assert job.status=='failed' and job.process.returncode is not None and job.task is None
    with pytest.raises(ProcessLookupError):os.kill(job.pid,0)
    saved=json.loads((tmp_path/'workspace/state/background-jobs'/f'{job.id}.json').read_text())
    assert saved['status']=='failed'
    await endpoint(app,'/v1/cancel')();await completed(app)


async def test_background_watcher_creation_failure_kills_bound_process(tmp_path,monkeypatch):
    app=runtime(tmp_path,monkeypatch,'import sys,time\nsys.stdin.readline()\ntime.sleep(60)\n')
    await endpoint(app,'/v1/turns')(runtime_app.TurnRequest(message='hello'))
    original=runtime_app.asyncio.create_task
    def create_task(value,*args,**kwargs):
        if str(kwargs.get('name','')).startswith('background-job-'):
            value.close();raise RuntimeError('watcher unavailable')
        return original(value,*args,**kwargs)
    monkeypatch.setattr(runtime_app.asyncio,'create_task',create_task)
    with pytest.raises(RuntimeError,match='watcher unavailable'):
        await endpoint(app,'/v1/background-jobs')(runtime_app.BackgroundJobCreate(
            name='watcher-fails',argv=[sys.executable,'-c','import time;time.sleep(60)'],
            cwd=str(tmp_path/'workspace/project')))
    job=next(iter(app.state.background_jobs.values()))
    assert job.status=='failed' and job.process.returncode is not None
    with pytest.raises(ProcessLookupError):os.kill(job.pid,0)
    await endpoint(app,'/v1/cancel')();await completed(app)


async def test_background_publish_failure_after_watcher_kills_and_waits_group(tmp_path,monkeypatch):
    app=runtime(tmp_path,monkeypatch,'import sys,time\nsys.stdin.readline()\ntime.sleep(60)\n')
    await endpoint(app,'/v1/turns')(runtime_app.TurnRequest(message='hello'))
    app.state.execution.journal_bytes=runtime_app.RUNTIME_EVENT_JOURNAL_LIMIT
    with pytest.raises(RuntimeError,match='journal.*exceeded'):
        await endpoint(app,'/v1/background-jobs')(runtime_app.BackgroundJobCreate(
            name='journal-fails',argv=[sys.executable,'-c','import time;time.sleep(60)'],
            cwd=str(tmp_path/'workspace/project')))
    job=next(iter(app.state.background_jobs.values()))
    assert job.status=='failed' and job.task.done() and job.process.returncode is not None
    with pytest.raises(ProcessLookupError):os.kill(job.pid,0)
    await endpoint(app,'/v1/cancel')();await completed(app)


async def test_runtime_restart_quiesces_surviving_background_process_group(tmp_path,monkeypatch):
    app=runtime(tmp_path,monkeypatch,'import sys,time\nsys.stdin.readline()\ntime.sleep(60)\n')
    await endpoint(app,'/v1/turns')(runtime_app.TurnRequest(message='hello'))
    marker=tmp_path/'survivor-ready'
    value=await endpoint(app,'/v1/background-jobs')(runtime_app.BackgroundJobCreate(
        name='survivor',argv=[sys.executable,'-c',
            f'import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);open({str(marker)!r},"w").close();time.sleep(60)'],
        cwd=str(tmp_path/'workspace/project')))
    for _ in range(100):
        if marker.exists():break
        await asyncio.sleep(.01)
    assert marker.exists();pid=value['pid']
    restarted=runtime_app.create_app()
    recovered=restarted.state.background_jobs[value['id']]
    assert recovered.status=='interrupted' and recovered.recovery_detail=='process_quiesced'
    assert recovered.pid_start_time and recovered.boot_id
    with pytest.raises(ProcessLookupError):os.kill(pid,0)
    await endpoint(app,'/v1/cancel')();await completed(app)


def test_runtime_recovery_never_kills_reused_pid_identity(tmp_path,monkeypatch):
    monkeypatch.setattr(runtime_app,'RUN',tmp_path/'run')
    monkeypatch.setattr(runtime_app,'WORKSPACE',tmp_path/'workspace')
    monkeypatch.setattr(runtime_app,'OUTPUTS',tmp_path/'workspace/outputs')
    root=tmp_path/'workspace/state/background-jobs';root.mkdir(parents=True)
    identity='reused';runtime_app.atomic_json(root/f'{identity}.json',{
        'id':identity,'name':'reused','argv':['false'],'cwd':str(tmp_path),
        'execution_id':'old','replaces':'','status':'running','pid':os.getpid(),
        'pid_start_time':'definitely-not-current','boot_id':runtime_app.boot_identity(),
        'returncode':None,'created_at':'','completed_at':'','log_path':str(root/'reused.log')})
    app=runtime_app.create_app();job=app.state.background_jobs[identity]
    assert job.status=='interrupted'
    assert job.recovery_detail=='process_absent_or_identity_changed'
    os.kill(os.getpid(),0)


async def test_native_interaction_ids_are_scoped_to_execution(tmp_path, monkeypatch):
    app = runtime(tmp_path, monkeypatch, '''import sys,json
msg=json.loads(sys.stdin.readline())
print(json.dumps({'type':'harness.interaction.requested','id':'same-native-id','interaction_kind':'approval','prompt':'Allow?'}),flush=True)
reply=json.loads(sys.stdin.readline())
assert reply['id']=='same-native-id'
print(json.dumps({'type':'harness.input.completed','input_ids':[msg['id']]}),flush=True)
''')
    class Request:
        async def json(self): return {'action':'approve','answers':{}}
    async def prompt_id():
        async with asyncio.timeout(3):
            while True:
                for event in app.state.execution.events:
                    if event.get('kind')=='waiting_user':return event['payload']['interaction_id']
                await asyncio.sleep(.01)
    start=endpoint(app,'/v1/turns');answer=endpoint(app,'/v1/interactions/{interaction_id}')
    await start(runtime_app.TurnRequest(message='first',execution_id='one'))
    first=await prompt_id()
    await answer(first,Request())
    assert (await completed(app))['status']=='completed'
    assert (await answer(first,Request()))['replayed']
    await start(runtime_app.TurnRequest(message='second',execution_id='two'))
    second=await prompt_id()
    try:
        assert first!=second
        with pytest.raises(HTTPException) as exc:await answer(first,Request())
        assert exc.value.status_code==409
        assert app.state.execution.result is None
        await answer(second,Request())
        assert (await completed(app))['status']=='completed'
    finally:
        await endpoint(app,'/v1/cancel')()


async def test_interaction_drain_failure_remains_unknown_and_never_false_replays(tmp_path,monkeypatch):
    app=runtime(tmp_path,monkeypatch,'''import sys,json,time
msg=json.loads(sys.stdin.readline())
print(json.dumps({'type':'harness.interaction.requested','id':'native','interaction_kind':'approval','prompt':'Allow?'}),flush=True)
time.sleep(60)
''')
    class Request:
        def __init__(self,action='approve'):self.action=action
        async def json(self):return {'action':self.action,'answers':{}}
    class Stdin:
        writes=0;closed=False
        def write(self,value):self.writes+=1
        async def drain(self):raise ConnectionError('drain outcome unknown')
        def is_closing(self):return self.closed
        def close(self):self.closed=True
    await endpoint(app,'/v1/turns')(runtime_app.TurnRequest(message='first',execution_id='one'))
    async with asyncio.timeout(3):
        while True:
            waiting=[row for row in app.state.execution.events if row.get('kind')=='waiting_user']
            if waiting:break
            await asyncio.sleep(.01)
    identity=waiting[0]['payload']['interaction_id'];stdin=Stdin()
    app.state.execution.process.stdin=stdin
    answer=endpoint(app,'/v1/interactions/{interaction_id}')
    with pytest.raises(HTTPException,match='interaction_delivery_unknown') as first:
        await answer(identity,Request())
    assert first.value.status_code==409
    assert app.state.execution.interaction_responses[identity]['state']=='unknown'
    with pytest.raises(HTTPException,match='interaction_delivery_unknown'):
        await answer(identity,Request())
    assert stdin.writes==1
    with pytest.raises(HTTPException,match='different response'):
        await answer(identity,Request('reject'))
    await endpoint(app,'/v1/cancel')();await completed(app)


async def test_terminal_execution_releases_pending_generic_interaction(tmp_path, monkeypatch):
    app=runtime(tmp_path,monkeypatch,'import sys,time\nsys.stdin.readline()\ntime.sleep(60)\n')
    await endpoint(app,'/v1/turns')(runtime_app.TurnRequest(message='hold'))
    pending=asyncio.create_task(endpoint(app,'/v1/native-interactions')(
        runtime_app.NativeInteractionCreate(kind='question',prompt='Pick?')))
    async with asyncio.timeout(3):
        while not app.state.interaction_waiters:await asyncio.sleep(.01)
    await endpoint(app,'/v1/cancel')()
    with pytest.raises(HTTPException) as exc:await pending
    assert exc.value.status_code==409
    assert not app.state.interaction_waiters
    await completed(app)
