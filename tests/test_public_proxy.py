import base64

import pytest

from sandbox_harness.public_proxy import proxy_token, public_address


def test_proxy_allows_public_and_rejects_internal_addresses():
    assert public_address("1.1.1.1")
    for address in ("127.0.0.1", "169.254.169.254", "10.0.0.1", "172.31.0.1", "192.168.1.1", "::1"):
        assert not public_address(address)


def test_proxy_requires_dynamic_capability_basic_auth():
    token = "signed.turn.capability"
    encoded = base64.b64encode((token + ":").encode()).decode()
    assert proxy_token({"proxy-authorization": "Basic " + encoded}) == token
    with pytest.raises(ValueError, match="required"):
        proxy_token({})
