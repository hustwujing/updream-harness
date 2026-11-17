import json
import sys

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
