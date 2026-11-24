import hashlib
from pathlib import Path

from sandbox_runtime.artifact_cli import main


class Response:
    def __init__(self,data:bytes,*,declared_size:int|None=None,digest:str|None=None,status:int=200):
        self.data=data;self.status_code=status;self.is_success=200<=status<300
        self.headers={"content-length":str(len(data) if declared_size is None else declared_size),
            "x-updream-artifact-sha256":digest or hashlib.sha256(data).hexdigest()}
        self._text="not found" if status>=400 else "";self.was_read=False
    def read(self):self.was_read=True;return self.data
    @property
    def text(self):
        if self.status_code>=400 and not self.was_read:raise RuntimeError("ResponseNotRead")
        return self._text
    def iter_bytes(self):yield self.data[:3];yield self.data[3:]


class Stream:
    def __init__(self,response):self.response=response
    def __enter__(self):return self.response
    def __exit__(self,*args):return None


def environment(tmp_path,monkeypatch,response):
    token=tmp_path/'token';token.write_text('signed-token')
    monkeypatch.setenv('HARNESS_GATEWAY_URL','http://gateway')
    monkeypatch.setenv('HARNESS_CAPABILITY_TOKEN_FILE',str(token))
    monkeypatch.setattr('sandbox_runtime.artifact_cli.httpx.stream',
        lambda *args,**kwargs:Stream(response))


def partials(path:Path):return list(path.parent.glob('.'+path.name+'.*.partial'))


def test_artifact_fetch_streams_and_atomically_replaces_destination(tmp_path,monkeypatch):
    data=b'reviewed-artifact';response=Response(data);environment(tmp_path,monkeypatch,response)
    output=tmp_path/'result.bin';output.write_bytes(b'old')
    assert main(['fetch','--artifact-id','a'*32,'--output',str(output)])==0
    assert output.read_bytes()==data and partials(output)==[]


def test_artifact_fetch_is_atomic_on_truncated_response(tmp_path,monkeypatch):
    output=tmp_path/'result.bin';output.write_bytes(b'old')
    environment(tmp_path,monkeypatch,Response(b'short',declared_size=10))
    assert main(['fetch','--artifact-id','a'*32,'--output',str(output)])==1
    assert output.read_bytes()==b'old' and partials(output)==[]


def test_artifact_fetch_rejects_digest_mismatch(tmp_path,monkeypatch):
    output=tmp_path/'result.bin';output.write_bytes(b'old')
    environment(tmp_path,monkeypatch,Response(b'same-length',digest='0'*64))
    assert main(['fetch','--artifact-id','a'*32,'--output',str(output)])==1
    assert output.read_bytes()==b'old' and partials(output)==[]


def test_artifact_fetch_cleans_up_after_http_failure(tmp_path,monkeypatch):
    output=tmp_path/'result.bin';output.write_bytes(b'old')
    response=Response(b'',status=404);environment(tmp_path,monkeypatch,response)
    assert main(['fetch','--artifact-id','a'*32,'--output',str(output)])==1
    assert response.was_read is True
    assert output.read_bytes()==b'old' and partials(output)==[]


def test_artifact_fetch_rejects_directory_destination(tmp_path,monkeypatch):
    output=tmp_path/'directory';output.mkdir();environment(tmp_path,monkeypatch,Response(b'data'))
    assert main(['fetch','--artifact-id','a'*32,'--output',str(output)])==2
    assert output.is_dir() and partials(output)==[]
