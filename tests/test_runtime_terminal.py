import json

import pytest

from fastapi.testclient import TestClient

from sandbox_runtime import app as runtime_app
from sandbox_runtime import pty_service


def payload(frame: bytes) -> bytes:
    if frame and frame[0] == 3 and len(frame) >= 9:
        return frame[9:]
    if frame and frame[0] in {1, 2}:
        return frame[1:]
    return b""


def test_runtime_pty_supports_input_replay_resize_and_delete(tmp_path,monkeypatch):
    workspace=tmp_path/"workspace"
    monkeypatch.setattr(runtime_app,"WORKSPACE",workspace)
    monkeypatch.setattr(runtime_app,"OUTPUTS",workspace/"outputs")
    monkeypatch.setattr(runtime_app,"RUN",tmp_path/"run")
    app=runtime_app.create_app()

    with TestClient(app) as client:
        created=client.post("/v1/pty",json={
            "cwd":str(workspace/"project"),"command":"/bin/bash"})
        assert created.status_code==200
        session_id=created.json()["session_id"]
        with client.websocket_connect(f"/v1/pty/{session_id}/ws?since=0") as socket:
            socket.send_text(json.dumps({"type":"resize","cols":100,"rows":30}))
            socket.send_bytes(b"\x00echo RUNTIME_PTY_OK\n")
            output=bytearray()
            for _ in range(20):
                output.extend(payload(socket.receive_bytes()))
                if b"RUNTIME_PTY_OK" in output:break
            assert b"RUNTIME_PTY_OK" in output
        for _ in range(5):
            with client.websocket_connect(f"/v1/pty/{session_id}/ws?since=0") as replay:
                assert b"RUNTIME_PTY_OK" in payload(replay.receive_bytes())
        status=client.get(f"/v1/pty/{session_id}")
        assert status.status_code==200 and status.json()["running"] is True
        deleted=client.delete(f"/v1/pty/{session_id}")
        assert deleted.status_code==200 and deleted.json()["deleted"] is True
        assert client.get(f"/v1/pty/{session_id}").status_code==404


@pytest.mark.asyncio
async def test_runtime_pty_retries_when_nonblocking_input_would_block(monkeypatch):
    session=pty_service.PtySession.__new__(pty_service.PtySession)
    session.exit_code=None;session.master_fd=42
    writes=[];waits=[]
    def write(descriptor,data):
        writes.append((descriptor,bytes(data)))
        if len(writes)==1:
            raise BlockingIOError
        return len(data)
    async def writable(descriptor):
        waits.append(descriptor)
    monkeypatch.setattr(pty_service.os,"write",write)
    monkeypatch.setattr(pty_service,"_writable",writable)

    await session.write(b"input")

    assert waits==[42]
    assert writes==[(42,b"input"),(42,b"input")]
