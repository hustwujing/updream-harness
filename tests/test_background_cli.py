import json
from io import BytesIO
import sys
from urllib.error import HTTPError

import pytest

from sandbox_runtime import background_cli


def test_background_status_selects_latest_job_by_name(monkeypatch, capsys):
    monkeypatch.setattr(background_cli, "call", lambda method, path="", value=None: {"items": [
        {"id":"old", "name":"render", "status":"completed"},
        {"id":"new", "name":"render", "status":"running"},
    ]})
    monkeypatch.setattr(sys, "argv", ["sandbox-background", "status", "--name", "render"])
    background_cli.main()
    assert json.loads(capsys.readouterr().out) == {"id":"new", "name":"render", "status":"running"}


def test_background_status_rejects_unknown_job(monkeypatch):
    monkeypatch.setattr(background_cli, "call", lambda method, path="", value=None: {"items": []})
    monkeypatch.setattr(sys, "argv", ["sandbox-background", "status", "missing"])
    with pytest.raises(SystemExit):
        background_cli.main()


def test_background_start_forwards_explicit_replacement(monkeypatch, capsys):
    captured = {}
    monkeypatch.setattr(background_cli.os, "getcwd", lambda: "/workspace/project/work/phase3")
    monkeypatch.setattr(background_cli, "call", lambda method, path="", value=None:
        captured.update(method=method, value=value) or {"id":"new"})
    monkeypatch.setattr(sys, "argv", ["sandbox-background", "start", "--name", "retry",
        "--replaces", "a" * 32, "--", "python", "render.py"])
    background_cli.main()
    assert captured == {"method":"POST", "value":{"name":"retry", "argv":["python","render.py"],
        "cwd":"/workspace/project/work/phase3", "replaces":"a" * 32}}
    assert json.loads(capsys.readouterr().out) == {"id":"new"}


def test_background_start_explicit_cwd_overrides_shell_directory(monkeypatch, capsys):
    captured={}
    monkeypatch.setattr(background_cli.os,"getcwd",lambda:"/workspace/project/work/phase3")
    monkeypatch.setattr(background_cli,"call",lambda method,path="",value=None:
        captured.update(method=method,value=value) or {"id":"new"})
    monkeypatch.setattr(sys,"argv",["sandbox-background","start","--name","render",
        "--cwd","/workspace/project/work/render","--","python","render.py"])
    background_cli.main()
    assert captured["value"]["cwd"]=="/workspace/project/work/render"
    assert json.loads(capsys.readouterr().out)=={"id":"new"}


def test_background_start_forwards_deadline_and_termination_grace(monkeypatch, capsys):
    captured={}
    monkeypatch.setattr(background_cli,"call",lambda method,path="",value=None:
        captured.update(value=value) or {"id":"new"})
    monkeypatch.setattr(sys,"argv",["sandbox-background","start","--name","compile",
        "--timeout","90","--terminate-grace","3","--","make","all"])
    background_cli.main()
    assert captured["value"]["timeout_seconds"]==90
    assert captured["value"]["terminate_grace_seconds"]==3
    assert json.loads(capsys.readouterr().out)=={"id":"new"}


def test_background_wait_keeps_one_invocation_and_prints_only_terminal_result(monkeypatch,capsys):
    monkeypatch.setattr(background_cli,"stream",lambda path:[
        {"type":"heartbeat","job":{"id":"job","status":"running"}},
        {"type":"terminal","job":{"id":"job","status":"completed","returncode":0}},
    ])
    monkeypatch.setattr(sys,"argv",["sandbox-background","wait","job"])
    background_cli.main()
    captured=capsys.readouterr()
    assert json.loads(captured.out)=={"id":"job","status":"completed","returncode":0}
    assert "background job job is running" in captured.err


def test_background_wait_observation_timeout_is_not_an_error(monkeypatch,capsys):
    monkeypatch.setattr(background_cli,"stream",lambda path:[
        {"type":"wait_timeout","job":{"id":"job","status":"running"}},
    ])
    monkeypatch.setattr(sys,"argv",["sandbox-background","wait","job","--timeout","2"])
    background_cli.main()
    assert json.loads(capsys.readouterr().out)=={
        "id":"job","status":"running","wait_timed_out":True}


def test_expected_http_error_is_structured_without_traceback(monkeypatch,capsys):
    error=HTTPError(background_cli.BASE,422,"Unprocessable",{},BytesIO(json.dumps({"detail":{
        "code":"replacement_target_not_terminal","message":"Wait for the target first",
        "retryable":False,"allowed_actions":["wait","cancel"]}}).encode()))
    monkeypatch.setattr(background_cli.urllib.request,"urlopen",lambda *args,**kwargs:(_ for _ in ()).throw(error))
    monkeypatch.setattr(sys,"argv",["sandbox-background","cancel","missing"])
    with pytest.raises(SystemExit) as stopped:
        background_cli.main()
    captured=capsys.readouterr()
    assert stopped.value.code==2 and not captured.out
    assert json.loads(captured.err)=={"code":"replacement_target_not_terminal",
        "message":"Wait for the target first","retryable":False,
        "allowed_actions":["wait","cancel"],"http_status":422}
    assert "Traceback" not in captured.err
