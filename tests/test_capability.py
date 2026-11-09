import pytest

from sandbox_harness.capability import CapabilityCodec
from sandbox_harness.domain import Harness


def test_capability_is_bound_to_route_and_expiry():
    codec = CapabilityCodec("a" * 32)
    token, claims = codec.issue(thread_id="thread", turn_id="turn", sandbox_id="sandbox",
        harness=Harness.CODEX, model="model", routes=["/v1/models/codex"], ttl_seconds=60,
        max_requests=10, max_public_requests=100, token_budget=1000)
    assert codec.verify(token, route="/v1/models/codex/responses", now=claims.issued_at).turn_id == "turn"
    with pytest.raises(ValueError, match="outside"):
        codec.verify(token, route="/v1/providers/video", now=claims.issued_at)
    with pytest.raises(ValueError, match="expired"):
        codec.verify(token, now=claims.expires_at)
    payload, signature = token.split(".", 1)
    tampered = payload + "." + ("A" if signature[0] != "A" else "B") + signature[1:]
    with pytest.raises(ValueError, match="signature"):
        codec.verify(tampered, now=claims.issued_at)
