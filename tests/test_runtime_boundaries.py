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
