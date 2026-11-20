from pathlib import Path


ROOT=Path(__file__).resolve().parents[1]


def test_direct_nginx_preserves_streams_websockets_uploads_and_prefix():
    source=(ROOT/"deploy/nginx.conf").read_text()
    assert "location ^~ /sandbox-harness-platform-parallel/" in source
    assert "proxy_pass http://127.0.0.1:24766/;" in source
    assert "X-Forwarded-Prefix /sandbox-harness-platform-parallel" in source
    assert "X-Updream-Ingress direct" in source
    assert "proxy_http_version 1.1" in source
    assert "Upgrade $http_upgrade" in source
    assert "proxy_buffering off" in source and "proxy_request_buffering off" in source
    assert "client_max_body_size 512m" in source


def test_frpc_ingress_is_private_login_only_and_has_no_registration_route_override():
    source=(ROOT/"deploy/nginx-frpc.conf").read_text()
    assert "listen 127.0.0.1:18091 proxy_protocol" in source
    assert "X-Updream-Ingress frpc" in source
    assert "proxy_pass http://127.0.0.1:24766/;" in source
    assert "proxy_buffering off" in source and "Upgrade $http_upgrade" in source
    assert "location / {" in source and "return 404" in source
    app=(ROOT/"src/sandbox_harness/app.py").read_text()
    assert 'request.state.ingress == "frpc"' in app
    assert 'registration_not_available' in app


def test_shared_frpc_location_preserves_parallel_prefix_and_trusted_ingress():
    source=(ROOT/"deploy/nginx-frpc-shared-location.conf").read_text()
    assert "location ^~ /sandbox-harness-platform-parallel/" in source
    assert "proxy_pass http://127.0.0.1:24766/;" in source
    assert "X-Forwarded-Prefix /sandbox-harness-platform-parallel" in source
    assert "X-Updream-Ingress frpc" in source
    assert "X-Real-IP $proxy_protocol_addr" in source
    assert "X-Forwarded-For $proxy_protocol_addr" in source
    assert "proxy_request_buffering off" in source and "proxy_buffering off" in source
    assert "Upgrade $http_upgrade" in source


def test_shared_frpc_location_preserves_ali_prefix_and_isolated_upstream():
    source=(ROOT/"deploy/nginx-frpc-ali-shared-location.conf").read_text()
    assert "absolute_redirect off" in source
    assert "location ^~ /sandbox-harness-platform-parallel-ali/" in source
    assert "proxy_pass http://127.0.0.1:24767/;" in source
    assert "X-Forwarded-Prefix /sandbox-harness-platform-parallel-ali" in source
    assert "X-Updream-Ingress frpc" in source
    assert "X-Real-IP $proxy_protocol_addr" in source
    assert "X-Forwarded-For $proxy_protocol_addr" in source
    assert "proxy_request_buffering off" in source
    assert "proxy_buffering off" in source
    assert "Upgrade $http_upgrade" in source
    assert "client_max_body_size 512m" in source
    for value in ("28433","29882","29883","6379"):
        assert value not in source


def test_direct_ali_nginx_preserves_prefix_and_isolated_upstream():
    source=(ROOT/"deploy/nginx-ali.conf").read_text()
    assert "absolute_redirect off" in source
    assert "location ^~ /sandbox-harness-platform-parallel-ali/" in source
    assert "proxy_pass http://127.0.0.1:24767/;" in source
    assert "X-Forwarded-Prefix /sandbox-harness-platform-parallel-ali" in source
    assert "X-Updream-Ingress direct" in source
    assert "X-Real-IP $remote_addr" in source
    assert "X-Forwarded-For $proxy_add_x_forwarded_for" in source
    assert "proxy_request_buffering off" in source
    assert "proxy_buffering off" in source
    assert "Upgrade $http_upgrade" in source
    assert "client_max_body_size 512m" in source
    for value in ("28433","29882","29883","6379"):
        assert value not in source


def test_ingress_does_not_expose_gateway_or_database_ports():
    source="".join((ROOT/path).read_text() for path in (
        "deploy/nginx.conf","deploy/nginx-frpc.conf","deploy/nginx-frpc-shared-location.conf",
        "deploy/nginx-frpc-ali-shared-location.conf","deploy/nginx-ali.conf"))
    for value in ("28432","29880","29881","6379"):
        assert value not in source
