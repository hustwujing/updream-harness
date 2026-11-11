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
