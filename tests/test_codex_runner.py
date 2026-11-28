import json
import os
from pathlib import Path
import subprocess
import sys
import time


def test_codex_runner_uses_turn_steer_for_live_appended_input(tmp_path):
    fake = tmp_path / "codex"
    fake.write_text("""#!/usr/bin/env python3
import json,sys,threading,time
lock=threading.Lock()
def send(value):
    with lock: print(json.dumps(value),flush=True)
def complete():
    time.sleep(.35);send({'method':'turn/completed','params':{'turn':{'id':'turn-1','status':'completed'}}})
for line in sys.stdin:
    value=json.loads(line);method=value.get('method');identity=value.get('id')
    if method=='initialize':send({'id':identity,'result':{}})
    elif method=='thread/start':send({'id':identity,'result':{'thread':{'id':'thread-1'}}})
    elif method=='thread/resume':send({'id':identity,'result':{'thread':{'id':value['params']['threadId']}}})
    elif method=='turn/start':
        send({'id':identity,'result':{'turn':{'id':'turn-1'}}});threading.Thread(target=complete,daemon=True).start()
    elif method=='turn/steer':
        send({'id':identity,'result':{'turnId':'turn-1'}});send({'method':'fake/steered','params':{'text':value['params']['input'][0]['text']}})
""")
    fake.chmod(0o755)
    environment = {**os.environ, "PATH": str(tmp_path) + os.pathsep + os.environ["PATH"],
        "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src"), "HARNESS_REASONING_EFFORT": "high"}
    process = subprocess.Popen([sys.executable, "-m", "sandbox_runtime.codex_runner", "--model", "model"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=environment)
    process.stdin.write(json.dumps({"type":"input","id":"one","message":"first"}) + "\n"); process.stdin.flush()
    time.sleep(.08)
    process.stdin.write(json.dumps({"type":"input","id":"two","message":"second"}) + "\n"); process.stdin.close()
    output = process.stdout.read(); errors = process.stderr.read(); process.wait(timeout=5)
    assert process.returncode == 0, errors
    rows = [json.loads(line) for line in output.splitlines()]
    assert any(row.get("method") == "fake/steered" and row["params"]["text"] == "second" for row in rows)
    completed = next(row for row in rows if row.get("type") == "harness.input.completed")
    assert completed["input_ids"] == ["one", "two"] and completed["thread_id"] == "thread-1"
    assert completed["status"] == "completed" and completed["error_code"] == ""


def test_codex_runner_forwards_user_question_and_waits_for_exact_response(tmp_path):
    fake = tmp_path / "codex"
    fake.write_text("""#!/usr/bin/env python3
import json,sys
def send(value): print(json.dumps(value),flush=True)
turn='turn-question'
for line in sys.stdin:
 value=json.loads(line);method=value.get('method');identity=value.get('id')
 if method=='initialize':send({'id':identity,'result':{}})
 elif method=='thread/start':send({'id':identity,'result':{'thread':{'id':'thread-question'}}})
 elif method=='thread/resume':send({'id':identity,'result':{'thread':{'id':value['params']['threadId']}}})
 elif method=='turn/start':
  send({'id':identity,'result':{'turn':{'id':turn}}})
  send({'id':900,'method':'item/tool/requestUserInput','params':{'message':'Choose format','questions':[{'id':'format','question':'Format?','options':['mp4','webm']}]}})
 elif identity==900:
  send({'method':'fake/answer','params':value.get('result')})
  send({'method':'turn/completed','params':{'turn':{'id':turn,'status':'completed'}}})
""")
    fake.chmod(0o755)
    environment = {**os.environ, "PATH": str(tmp_path) + os.pathsep + os.environ["PATH"],
        "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")}
    process = subprocess.Popen([sys.executable, "-m", "sandbox_runtime.codex_runner", "--model", "model"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=environment)
    process.stdin.write(json.dumps({"type":"input","id":"one","message":"first"}) + "\n"); process.stdin.flush()
    rows = []
    while True:
        row = json.loads(process.stdout.readline()); rows.append(row)
        if row.get("type") == "harness.interaction.requested":
            break
    assert rows[-1]["interaction_kind"] == "question" and rows[-1]["id"] == "codex-900"
    process.stdin.write(json.dumps({"type":"interaction_response","id":"codex-900",
        "response":{"action":"answer","answers":{"format":"mp4"}}}) + "\n")
    process.stdin.close(); output = process.stdout.read(); errors = process.stderr.read(); process.wait(timeout=5)
    assert process.returncode == 0, errors
    rows += [json.loads(line) for line in output.splitlines()]
    answer = next(row for row in rows if row.get("method") == "fake/answer")
    assert answer["params"] == {"answers":{"format":"mp4"}}
    assert any(row.get("type")=="harness.interaction.applied"
        and row.get("id")=="codex-900" for row in rows)
    assert any(row.get("type") == "harness.input.completed" for row in rows)


def test_codex_runner_does_not_expose_app_server_stderr(tmp_path):
    fake=tmp_path/"codex";secret="provider-url?token=PRIVATE-RUNNER-SECRET"
    fake.write_text(f'''#!/usr/bin/env python3
import sys
for line in sys.stdin:
    sys.stderr.write({(secret+chr(10))!r});sys.stderr.flush();raise SystemExit(17)
''')
    fake.chmod(0o755)
    environment={**os.environ,"PATH":str(tmp_path)+os.pathsep+os.environ["PATH"],
        "PYTHONPATH":str(Path(__file__).resolve().parents[1]/"src")}
    process=subprocess.run([sys.executable,"-m","sandbox_runtime.codex_runner","--model","model"],
        input="",stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,env=environment,timeout=5)
    assert process.returncode!=0 and secret not in process.stdout and secret in process.stderr
    rows=[json.loads(line) for line in process.stdout.splitlines()]
    failure=next(row for row in rows if row.get("type")=="error")
    assert failure["error_code"]=="harness_runner_failed"
    assert failure["diagnostics"]["exception_type"] in {"CodexAppServerExited","BrokenPipeError"}
    assert set(failure["diagnostics"])<=set(
        ["exception_type","stderr_line_count","stderr_sha256"])


def test_codex_runner_classifies_missing_resumed_session_without_raw_rpc_detail(tmp_path):
    fake=tmp_path/'codex';secret='PRIVATE NATIVE SESSION ERROR'
    fake.write_text(f'''#!/usr/bin/env python3
import json,sys
for line in sys.stdin:
 value=json.loads(line);method=value.get("method");identity=value.get("id")
 if method=="initialize":print(json.dumps({{"id":identity,"result":{{}}}}),flush=True)
 elif method=="thread/resume":print(json.dumps({{"id":identity,"error":{{"code":-32004,"message":{secret!r}}}}}),flush=True)
''')
    fake.chmod(0o755)
    environment={**os.environ,'PATH':str(tmp_path)+os.pathsep+os.environ['PATH'],
        'PYTHONPATH':str(Path(__file__).resolve().parents[1]/'src')}
    process=subprocess.run([sys.executable,'-m','sandbox_runtime.codex_runner','--model','model',
        '--thread-id','missing-thread'],input='',stdout=subprocess.PIPE,stderr=subprocess.PIPE,
        text=True,env=environment,timeout=5)
    assert process.returncode!=0 and secret not in process.stdout
    failure=next(row for row in map(json.loads,process.stdout.splitlines())
        if row.get('type')=='error')
    assert failure['error_code']=='native_session_unavailable'
    assert failure['message']=='Saved Codex session is unavailable'
    assert failure['input_ids']==[]
    assert failure['diagnostics']=={'exception_type':'CodexRpcError',
        'rpc_method':'thread/resume','rpc_code':-32004}


def test_codex_runner_verifies_session_after_graceful_app_server_shutdown(tmp_path):
    fake=tmp_path/'codex';marker=tmp_path/'persisted-thread'
    fake.write_text(f'''#!/usr/bin/env python3
import json,os,sys
marker={str(marker)!r}
def send(value):print(json.dumps(value),flush=True)
for line in sys.stdin:
 value=json.loads(line);method=value.get('method');identity=value.get('id')
 if method=='initialize':send({{'id':identity,'result':{{}}}})
 elif method=='thread/start':send({{'id':identity,'result':{{'thread':{{'id':'durable-thread'}}}}}})
 elif method=='thread/resume':
  if os.path.exists(marker):send({{'id':identity,'result':{{'thread':{{'id':value['params']['threadId']}}}}}})
  else:send({{'id':identity,'error':{{'code':-32004,'message':'not persisted'}}}})
 elif method=='turn/start':
  send({{'id':identity,'result':{{'turn':{{'id':'turn'}}}}}})
  send({{'method':'turn/completed','params':{{'turn':{{'id':'turn','status':'completed'}}}}}})
open(marker,'w').write('durable-thread')
''')
    fake.chmod(0o755)
    environment={**os.environ,'PATH':str(tmp_path)+os.pathsep+os.environ['PATH'],
        'PYTHONPATH':str(Path(__file__).resolve().parents[1]/'src')}
    process=subprocess.run([sys.executable,'-m','sandbox_runtime.codex_runner','--model','model'],
        input=json.dumps({'type':'input','id':'one','message':'persist'})+'\n',
        stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,env=environment,timeout=10)
    assert process.returncode==0,process.stderr
    rows=list(map(json.loads,process.stdout.splitlines()))
    completed=next(row for row in rows if row.get('type')=='harness.input.completed')
    assert completed['thread_id']=='durable-thread' and marker.read_text()=='durable-thread'


def test_codex_runner_reports_accepted_input_when_post_turn_resume_fails(tmp_path):
    fake=tmp_path/'codex';starts=tmp_path/'app-server-starts'
    fake.write_text(f'''#!/usr/bin/env python3
import json,os,sys
starts={str(starts)!r}
count=int(open(starts).read())+1 if os.path.exists(starts) else 1
open(starts,'w').write(str(count))
def send(value):print(json.dumps(value),flush=True)
for line in sys.stdin:
 value=json.loads(line);method=value.get('method');identity=value.get('id')
 if method=='initialize':send({{'id':identity,'result':{{}}}})
 elif method=='thread/start':send({{'id':identity,'result':{{'thread':{{'id':'fragile-thread'}}}}}})
 elif method=='thread/resume':
  send({{'id':identity,'error':{{'code':-32004,'message':'not persisted'}}}})
 elif method=='turn/start':
  send({{'id':identity,'result':{{'turn':{{'id':'turn'}}}}}})
  send({{'method':'turn/completed','params':{{'turn':{{'id':'turn','status':'completed'}}}}}})
''')
    fake.chmod(0o755)
    environment={**os.environ,'PATH':str(tmp_path)+os.pathsep+os.environ['PATH'],
        'PYTHONPATH':str(Path(__file__).resolve().parents[1]/'src')}
    process=subprocess.run([sys.executable,'-m','sandbox_runtime.codex_runner','--model','model'],
        input=json.dumps({'type':'input','id':'accepted-one','message':'persist'})+'\n',
        stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,env=environment,timeout=10)
    assert process.returncode!=0,process.stderr
    failure=next(row for row in map(json.loads,process.stdout.splitlines())
        if row.get('type')=='error')
    assert failure['error_code']=='native_session_unavailable'
    assert failure['input_ids']==['accepted-one']
    assert starts.read_text()=='2'
