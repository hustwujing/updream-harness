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
