import json
import os
from pathlib import Path
import subprocess
import sys


def test_deepseek_runner_reuses_one_session_for_multiple_inputs(tmp_path):
    package = tmp_path / "deepseek_harness"; package.mkdir()
    package.joinpath("__init__.py").write_text("""from types import SimpleNamespace
class Session:
    def __init__(self,id):self.id=id
    def run(self,text,on_notification=None):
        if on_notification:on_notification(SimpleNamespace(model_dump=lambda mode='json':{'method':'session.event','payload':{'sessionId':self.id,'event':{'type':'message','data':{'text':text}}}}))
        return SimpleNamespace(final_response='reply:'+text,finish_reason='completed')
class DeepSeekHarness:
    def __init__(self,**kwargs):self.session=None
    def start(self):pass
    def start_session(self,id):self.session=Session(id);return self.session
    def resume_session(self,id):self.session=Session(id);return self.session
    def close(self):pass
""")
    environment = {**os.environ, "PYTHONPATH": str(tmp_path) + os.pathsep + str(Path(__file__).resolve().parents[1] / "src"),
        "DSH_HOME": str(tmp_path / "dsh"), "DEEPSEEK_BASE_URL": "http://gateway", "DEEPSEEK_API_KEY": "test",
        "DEEPSEEK_REASONING_EFFORT": "high"}
    commands = "".join(json.dumps({"type":"input","id":str(index),"message":text}) + "\n"
        for index,text in enumerate(("first","second"),1))
    result = subprocess.run([sys.executable, "-m", "sandbox_runtime.deepseek_runner", "--model", "model"],
        input=commands, text=True, capture_output=True, env=environment, timeout=5)
    assert result.returncode == 0, result.stderr
    rows = [json.loads(line) for line in result.stdout.splitlines()]
    replies = [row["text"] for row in rows if row.get("type") == "assistant_delta"]
    assert len(replies) == 2 and replies[0].endswith("User request:\nfirst") and replies[1].endswith("User request:\nsecond")
    assert all("sandbox-media commit" in value for value in replies)
    assert all(row["snapshot"] is True for row in rows if row.get("type") == "assistant_delta")
    assert [row["input_ids"] for row in rows if row.get("type") == "harness.input.completed"] == [["1"], ["2"]]
    assert len({row["thread_id"] for row in rows if row.get("thread_id")}) == 1


def test_deepseek_runner_emits_explicit_failed_completion_before_exit(tmp_path):
    package=tmp_path/'deepseek_harness';package.mkdir()
    package.joinpath('__init__.py').write_text("""from types import SimpleNamespace
class Session:
    def run(self,text,on_notification=None):return SimpleNamespace(final_response='partial',finish_reason='error')
class DeepSeekHarness:
    def __init__(self,**kwargs):pass
    def start(self):pass
    def start_session(self,id):return Session()
    def resume_session(self,id):return Session()
    def close(self):pass
""")
    environment={**os.environ,'PYTHONPATH':str(tmp_path)+os.pathsep+str(Path(__file__).resolve().parents[1]/'src'),
        'DSH_HOME':str(tmp_path/'dsh'),'DEEPSEEK_BASE_URL':'http://gateway',
        'DEEPSEEK_API_KEY':'test','DEEPSEEK_REASONING_EFFORT':'high'}
    result=subprocess.run([sys.executable,'-m','sandbox_runtime.deepseek_runner','--model','model'],
        input=json.dumps({'type':'input','id':'failed-input','message':'work'})+'\n',text=True,
        capture_output=True,env=environment,timeout=5)
    assert result.returncode==0,result.stderr
    rows=[json.loads(line) for line in result.stdout.splitlines()]
    completed=next(row for row in rows if row.get('type')=='harness.input.completed')
    assert completed=={'type':'harness.input.completed','input_ids':['failed-input'],
        'thread_id':next(row['thread_id'] for row in rows if row.get('type')=='thread.started'),
        'status':'failed','error_code':'harness_failed',
        'error_detail':'DeepSeek Harness finished with error'}
