import pytest

from sandbox_harness.common.security.capability import (CapabilityCodec)
from sandbox_harness.common.contracts.domain import (Harness)


def test_capability_is_bound_to_route_and_expiry():
    codec = CapabilityCodec("a" * 32)
    token, claims = codec.issue(thread_id="thread", turn_id="turn", sandbox_id="sandbox",
        harness=Harness.CODEX, model="model", routes=["/v1/models/codex"], ttl_seconds=60,
        max_requests=10, max_public_requests=100, token_budget=1000, network_access="public")
    verified = codec.verify(token, route="/v1/models/codex/responses", now=claims.issued_at)
    assert verified.turn_id == "turn" and verified.network_access == "public"
    with pytest.raises(ValueError, match="outside"):
        codec.verify(token, route="/v1/providers/video", now=claims.issued_at)
    with pytest.raises(ValueError, match="expired"):
        codec.verify(token, now=claims.expires_at)
    kid, payload, signature = token.split(".")
    tampered = kid+"."+payload+"."+("A" if signature[0] != "A" else "B")+signature[1:]
    with pytest.raises(ValueError, match="signature"):
        codec.verify(tampered, now=claims.issued_at)


def test_capability_defaults_to_restricted_network_access():
    codec = CapabilityCodec("b" * 32)
    token, claims = codec.issue(thread_id="thread", turn_id="turn", sandbox_id="sandbox",
        harness=Harness.CLAUDE, model="model", routes=["/v1/public-http"], ttl_seconds=60,
        max_requests=10, max_public_requests=100, token_budget=1000)
    assert claims.network_access == "restricted"
    assert codec.verify(token, now=claims.issued_at).network_access == "restricted"


def test_capability_key_rotation_accepts_only_current_and_previous_kids():
    old=CapabilityCodec("o"*32,kid="old")
    old_token,old_claims=old.issue(thread_id="thread",turn_id="turn",sandbox_id="sandbox",
        harness=Harness.CODEX,model="model",routes=["/v1/models/codex"],ttl_seconds=60,
        max_requests=10,max_public_requests=100,token_budget=1000)
    rotating=CapabilityCodec("n"*32,kid="current",previous_keys={"old":"o"*32})
    assert rotating.verify(old_token,now=old_claims.issued_at).kid=="old"
    new_token,new_claims=rotating.issue(thread_id="thread",turn_id="turn",sandbox_id="sandbox",
        harness=Harness.CODEX,model="model",routes=["/v1/models/codex"],ttl_seconds=60,
        max_requests=10,max_public_requests=100,token_budget=1000,
        issued_at=old_claims.issued_at)
    assert new_token.startswith("current.") and new_claims.kid=="current"
    assert rotating.verify(new_token,now=new_claims.issued_at).kid=="current"
    after=CapabilityCodec("n"*32,kid="current")
    with pytest.raises(ValueError,match="key id"):
        after.verify(old_token,now=old_claims.issued_at)
    with pytest.raises(ValueError,match="key id"):
        after.verify("unknown."+new_token.split(".",1)[1],now=new_claims.issued_at)
