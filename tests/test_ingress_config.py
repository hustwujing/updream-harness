from pathlib import Path


ROOT=Path(__file__).resolve().parents[1]


def test_direct_and_frpc_nginx_ingress_are_explicit_and_separate():
    direct=(ROOT/'deploy/nginx.conf').read_text()
    frpc=(ROOT/'deploy/nginx-frpc.conf').read_text()
    assert 'X-Updream-Ingress direct' in direct
    assert 'listen 127.0.0.1:18090 proxy_protocol' in frpc
    assert 'X-Updream-Ingress frpc' in frpc
    assert 'X-Real-IP $proxy_protocol_addr' in frpc
    assert 'X-Forwarded-For $proxy_protocol_addr' in frpc
    assert 'proxy_pass http://127.0.0.1:22766/' in frpc
