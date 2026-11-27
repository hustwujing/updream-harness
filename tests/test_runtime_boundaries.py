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


async def test_runtime_restart_replays_disk_journal_then_reports_controlled_interruption(tmp_path,monkeypatch):
    monkeypatch.setattr(runtime_app,'RUN',tmp_path/'run')
    monkeypatch.setattr(runtime_app,'WORKSPACE',tmp_path/'workspace')
    monkeypatch.setattr(runtime_app,'OUTPUTS',tmp_path/'workspace/outputs')
    from sandbox_runtime.event_journal import ExecutionEventJournal
    journal=ExecutionEventJournal(runtime_app.RUN/'execution-journals','lost-execution')
    journal.append({'kind':'text_delta','payload':{'text':'partial'},'runtime_sequence':1})
    app=runtime_app.create_app()
    status=await endpoint(app,'/v1/executions/{execution_id}')('lost-execution')
    assert status['state']=='completed' and status['last_sequence']==2
    assert status['result']['error_code']=='runtime_process_restarted'
    response=await endpoint(app,'/v1/executions/{execution_id}/events')('lost-execution',0)
    body=b''.join([chunk.encode() if isinstance(chunk,str) else chunk
        async for chunk in response.body_iterator]).decode()
    rows=[json.loads(line) for line in body.splitlines()]
    assert rows[0]['payload']['text']=='partial'
    assert rows[-1]['result']['error_code']=='runtime_process_restarted'


async def test_overlapping_project_upload_retry_keeps_its_own_temporary_file(tmp_path,monkeypatch):
    from starlette.requests import ClientDisconnect
    app=runtime(tmp_path,monkeypatch,'')
    upload=endpoint(app,'/v1/project-files/file')
    payload=b'immutable-project-content'
    first_entered=asyncio.Event();second_entered=asyncio.Event()
    disconnect=asyncio.Event();finish=asyncio.Event()
    class Request:
        headers={'content-length':str(len(payload))}
        def __init__(self,first):self.first=first
        async def stream(self):
            if self.first:
                yield payload[:5]
                first_entered.set();await disconnect.wait()
                raise ClientDisconnect()
            yield payload
            second_entered.set();await finish.wait()
    first=asyncio.create_task(upload(Request(True),'artifacts/result.bin',len(payload)))
    second=None
    try:
        await asyncio.wait_for(first_entered.wait(),2)
        second=asyncio.create_task(upload(Request(False),'artifacts/result.bin',len(payload)))
        await asyncio.wait_for(second_entered.wait(),2)
        disconnect.set()
        with pytest.raises(ClientDisconnect):await first
        finish.set();result=await second
        assert result['bytes']==len(payload)
        target=tmp_path/'workspace/project/artifacts/result.bin'
        assert target.read_bytes()==payload
        assert sorted(p.name for p in target.parent.iterdir())==['result.bin']
    finally:
        disconnect.set();finish.set()
        await asyncio.gather(*(v for v in (first,second) if v),return_exceptions=True)


async def test_background_inventory_exposes_only_transitively_unresolved_failures(tmp_path,monkeypatch):
    app=runtime(tmp_path,monkeypatch,'')
    failed=runtime_app.BackgroundJob(id='a'*32,name='render',argv=['false'],
        cwd=str(tmp_path/'workspace/project'),execution_id='execution',status='failed',returncode=1)
    replacement=runtime_app.BackgroundJob(id='b'*32,name='render-fixed',argv=['true'],
        cwd=str(tmp_path/'workspace/project'),execution_id='execution',replaces=failed.id,
        status='completed',returncode=0)
    unrelated=runtime_app.BackgroundJob(id='c'*32,name='inspect',argv=['false'],
        cwd=str(tmp_path/'workspace/project'),execution_id='execution',status='failed',returncode=2)
    app.state.background_jobs={job.id:job for job in (failed,replacement,unrelated)}
    inventory=next(route.endpoint for route in app.routes
        if getattr(route,'path','')=='/v1/background-jobs' and 'GET' in route.methods)
    value=await inventory()
    assert [row['id'] for row in value['items']]==[failed.id,replacement.id,unrelated.id]
    assert [row['id'] for row in value['unresolved']]==[unrelated.id]
    assert [row['id'] for row in value['replacement_targets']]==[unrelated.id]


async def test_background_start_requires_latest_unresolved_leaf_as_replacement(tmp_path,monkeypatch):
    app=runtime(tmp_path,monkeypatch,'import sys,time\nsys.stdin.readline()\ntime.sleep(60)\n')
    await endpoint(app,'/v1/turns')(runtime_app.TurnRequest(message='hello'))
    execution=app.state.execution
    root=runtime_app.BackgroundJob(id='a'*32,name='root',argv=['false'],
        cwd=str(tmp_path/'workspace/project'),execution_id=execution.id,status='failed',returncode=1)
    leaf=runtime_app.BackgroundJob(id='b'*32,name='leaf',argv=['false'],
        cwd=str(tmp_path/'workspace/project'),execution_id=execution.id,replaces=root.id,
        status='failed',returncode=1)
    app.state.background_jobs={root.id:root,leaf.id:leaf}
    start=endpoint(app,'/v1/background-jobs')
    try:
        with pytest.raises(HTTPException) as missing:
            await start(runtime_app.BackgroundJobCreate(name='unlinked',argv=['true'],
                cwd=str(tmp_path/'workspace/project')))
        assert missing.value.status_code==409
        assert missing.value.detail['replacement_target_ids']==[leaf.id]
        with pytest.raises(HTTPException) as branched:
            await start(runtime_app.BackgroundJobCreate(name='branched',argv=['true'],
                cwd=str(tmp_path/'workspace/project'),replaces=root.id))
        assert branched.value.status_code==409
        replacement=await start(runtime_app.BackgroundJobCreate(name='replacement',argv=['true'],
            cwd=str(tmp_path/'workspace/project'),replaces=leaf.id))
        await app.state.background_jobs[replacement['id']].task
        assert runtime_app.unresolved_background_jobs(list(app.state.background_jobs.values()))==[]
    finally:
        await endpoint(app,'/v1/cancel')();await completed(app)


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


async def test_background_wait_observation_timeout_does_not_cancel_job(tmp_path,monkeypatch):
    app=runtime(tmp_path,monkeypatch,'import sys,time\nsys.stdin.readline()\ntime.sleep(60)\n')
    await endpoint(app,'/v1/turns')(runtime_app.TurnRequest(message='hello'))
    value=await endpoint(app,'/v1/background-jobs')(runtime_app.BackgroundJobCreate(
        name='bounded-wait',argv=[sys.executable,'-c','import time;time.sleep(.2)'],
        cwd=str(tmp_path/'workspace/project'),timeout_seconds=3,terminate_grace_seconds=.05))
    wait=endpoint(app,'/v1/background-jobs/{job_id}/wait')
    response=await wait(value['id'],timeout_seconds=.03,heartbeat_seconds=.01)
    rows=[json.loads(chunk) async for chunk in response.body_iterator]
    assert rows[-1]['type']=='wait_timeout' and rows[-1]['job']['status']=='running'
    assert app.state.background_jobs[value['id']].status=='running'
    response=await wait(value['id'],timeout_seconds=None,heartbeat_seconds=.01)
    rows=[json.loads(chunk) async for chunk in response.body_iterator]
    assert rows[-1]['type']=='terminal' and rows[-1]['job']['status']=='completed'
    await endpoint(app,'/v1/cancel')();await completed(app)


async def test_background_deadline_kills_process_ignoring_sigterm_and_reports_timed_out(tmp_path,monkeypatch):
    app=runtime(tmp_path,monkeypatch,'import sys,time\nsys.stdin.readline()\ntime.sleep(60)\n')
    await endpoint(app,'/v1/turns')(runtime_app.TurnRequest(message='hello'))
    marker=tmp_path/'deadline-ready'
    value=await endpoint(app,'/v1/background-jobs')(runtime_app.BackgroundJobCreate(
        name='deadline',argv=[sys.executable,'-c',
            f'import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);open({str(marker)!r},"w").close();time.sleep(60)'],
        cwd=str(tmp_path/'workspace/project'),timeout_seconds=.15,terminate_grace_seconds=.05))
    job=app.state.background_jobs[value['id']]
    await asyncio.wait_for(job.task,3)
    assert marker.exists() and job.status=='timed_out' and job.returncode==124
    receipt=json.loads((tmp_path/'workspace/state/background-jobs'/f'{job.id}.supervisor.json').read_text())
    assert receipt['status']=='timed_out' and receipt['sigterm_sent'] and receipt['sigkill_sent']
    with pytest.raises(ProcessLookupError):os.kill(job.pid,0)
    await endpoint(app,'/v1/cancel')();await completed(app)


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
    assert job.status=='cancelled' and job.task.done() and job.process.returncode is not None
    with pytest.raises(ProcessLookupError):os.kill(job.pid,0)
    await endpoint(app,'/v1/cancel')();await completed(app)


async def test_runtime_restart_quiesces_surviving_background_process_group(tmp_path,monkeypatch):
    app=runtime(tmp_path,monkeypatch,'import sys,time\nsys.stdin.readline()\ntime.sleep(60)\n')
    await endpoint(app,'/v1/turns')(runtime_app.TurnRequest(message='hello'))
    marker=tmp_path/'survivor-ready'
    value=await endpoint(app,'/v1/background-jobs')(runtime_app.BackgroundJobCreate(
        name='survivor',argv=[sys.executable,'-c',
            f'import os,signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);open({str(marker)!r},"w").write(str(os.getpid()));time.sleep(60)'],
        cwd=str(tmp_path/'workspace/project')))
    for _ in range(100):
        if marker.exists():break
        await asyncio.sleep(.01)
    assert marker.exists();pid=value['pid'];child_pid=int(marker.read_text())
    restarted=runtime_app.create_app()
    recovered=restarted.state.background_jobs[value['id']]
    assert recovered.status=='interrupted' and recovered.recovery_detail=='process_quiesced'
    assert recovered.pid_start_time and recovered.boot_id
    with pytest.raises(ProcessLookupError):os.kill(pid,0)
    with pytest.raises(ProcessLookupError):os.kill(child_pid,0)
    await endpoint(app,'/v1/cancel')();await completed(app)


async def test_starting_intent_recovers_supervisor_receipt_after_runtime_crash_window(tmp_path,monkeypatch):
    monkeypatch.setattr(runtime_app,'RUN',tmp_path/'run')
    monkeypatch.setattr(runtime_app,'WORKSPACE',tmp_path/'workspace')
    monkeypatch.setattr(runtime_app,'OUTPUTS',tmp_path/'workspace/outputs')
    root=tmp_path/'workspace/state/background-jobs';root.mkdir(parents=True)
    receipt=root/'crash.supervisor.json'
    supervisor=await asyncio.create_subprocess_exec(sys.executable,'-m',
        'sandbox_runtime.background_supervisor','--receipt',str(receipt),'--',
        sys.executable,'-c','import time;time.sleep(60)',start_new_session=True)
    for _ in range(200):
        if receipt.is_file():break
        await asyncio.sleep(.01)
    assert receipt.is_file()
    runtime_app.atomic_json(root/'crash.json',{'id':'crash','name':'crash-window',
        'argv':[sys.executable,'-c','import time;time.sleep(60)'],'cwd':str(tmp_path),
        'execution_id':'old','replaces':'','status':'starting','pid':0,
        'pid_start_time':'','boot_id':'','returncode':None,'created_at':'',
        'completed_at':'','log_path':str(root/'crash.log'),
        'supervisor_receipt':str(receipt)})
    app=runtime_app.create_app();job=app.state.background_jobs['crash']
    assert job.status=='interrupted' and job.recovery_detail=='process_quiesced'
    await supervisor.wait()
    with pytest.raises(ProcessLookupError):os.kill(supervisor.pid,0)


def test_runtime_recovery_preserves_terminal_supervisor_receipt(tmp_path,monkeypatch):
    monkeypatch.setattr(runtime_app,'RUN',tmp_path/'run')
    monkeypatch.setattr(runtime_app,'WORKSPACE',tmp_path/'workspace')
    monkeypatch.setattr(runtime_app,'OUTPUTS',tmp_path/'workspace/outputs')
    root=tmp_path/'workspace/state/background-jobs';root.mkdir(parents=True)
    receipt=root/'terminal.supervisor.json'
    runtime_app.atomic_json(receipt,{'pid':999999,'pid_start_time':'gone','boot_id':runtime_app.boot_identity(),
        'status':'timed_out','returncode':124,'completed_at':'2026-11-20T00:00:00Z',
        'reason':'timed_out','sigterm_sent':True,'sigkill_sent':True})
    runtime_app.atomic_json(root/'terminal.json',{'id':'terminal','name':'deadline','argv':['false'],
        'cwd':str(tmp_path),'execution_id':'old','replaces':'','status':'running','pid':999999,
        'pid_start_time':'gone','boot_id':runtime_app.boot_identity(),'returncode':None,
        'created_at':'','completed_at':'','log_path':str(root/'terminal.log'),
        'supervisor_receipt':str(receipt),'timeout_seconds':1,'terminate_grace_seconds':.1,
        'deadline_at':1})
    job=runtime_app.create_app().state.background_jobs['terminal']
    assert job.status=='timed_out' and job.returncode==124
    assert job.completed_at=='2026-11-20T00:00:00Z'
    assert job.termination_detail=={'reason':'timed_out','sigterm_sent':True,'sigkill_sent':True}


async def test_daemonized_setsid_descendant_keeps_job_running_until_supervisor_cancels_it(tmp_path,monkeypatch):
    app=runtime(tmp_path,monkeypatch,'import sys,time\nsys.stdin.readline()\ntime.sleep(60)\n')
    await endpoint(app,'/v1/turns')(runtime_app.TurnRequest(message='hello'))
    pidfile=tmp_path/'daemon.pid'
    source=("import os,time\npid=os.fork()\n"
        "if pid==0:\n os.setsid();os.close(0);os.close(1);os.close(2);"
        f"open({str(pidfile)!r},'w').write(str(os.getpid()));time.sleep(60);os._exit(0)\n"
        "os._exit(0)\n")
    value=await endpoint(app,'/v1/background-jobs')(runtime_app.BackgroundJobCreate(
        name='daemonizes',argv=[sys.executable,'-c',source],
        cwd=str(tmp_path/'workspace/project')))
    daemon=None
    for _ in range(200):
        try:daemon=int(pidfile.read_text().strip())
        except (FileNotFoundError,ValueError):await asyncio.sleep(.01)
        else:break
    assert daemon is not None
    await asyncio.sleep(.1)
    job=app.state.background_jobs[value['id']]
    assert job.status=='running' and job.process.returncode is None
    os.kill(daemon,0)
    await endpoint(app,'/v1/background-jobs/{job_id}/cancel')(job.id)
    assert job.status=='cancelled' and job.process.returncode is not None
    with pytest.raises(ProcessLookupError):os.kill(daemon,0)
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
