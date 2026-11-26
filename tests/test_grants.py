import json

import pytest

from sandbox_harness.common.contracts.domain import (Harness)
from sandbox_harness.common.security.grants import (GrantAuthority, JobGrantRequest, TurnGrantRequest)


def authority() -> GrantAuthority:
    return GrantAuthority("s" * 32)


def turn_request() -> TurnGrantRequest:
    return TurnGrantRequest(
        thread_id="a" * 32, turn_id="b" * 32, sandbox_id="sandbox-1",
        harness=Harness.CODEX, model="gpt-5.6", routes=("/v1/jobs",),
        network_access="restricted", ttl_seconds=60, max_requests=10,
        max_public_requests=10, token_budget=1000,
        artifact_ids=("c" * 32, "d" * 32), skill_ids=("writing", "slides"),
        capability_ids=("image.generate",), operation_ids=("artifact.inspect",),
        child_profile_ids=("child-general",),runtime_profile_ids=("general-cpu",),
        network_origins=("image-provider",), output_scopes=("preview:candidate",),
        budget={"submissions":1},
    )


def lifetime_job_request(ttl):
    return JobGrantRequest(group_id='e'*32,job_id='f'*32,generation=1,
        source_identity='sandbox-1',descriptor_id='artifact.inspect',descriptor_version='1',
        descriptor_digest='1'*64,operations=('status',),parameter_digest='2'*64,
        routes=('/v1/jobs',),ttl_seconds=ttl,max_requests=1,max_public_requests=1,token_budget=1)


def test_child_grant_uses_one_clock_sample_for_check_and_signed_expiry(monkeypatch):
    from types import SimpleNamespace
    import sandbox_harness.common.security.grants as grants
    import sandbox_harness.common.security.capability as capability
    ticks=iter([1000,1030,1031]);clock=SimpleNamespace(time=lambda:next(ticks))
    monkeypatch.setattr(grants,'time',clock);monkeypatch.setattr(capability,'time',clock)
    issuer=authority();_,parent=issuer.issue_turn(turn_request())
    _,child=issuer.issue_job(parent,lifetime_job_request(30))
    assert child.issued_at==1030 and child.expires_at==parent.expires_at==1060


def test_explicit_child_ttl_cannot_exceed_remaining_parent_lifetime(monkeypatch):
    from types import SimpleNamespace
    import sandbox_harness.common.security.grants as grants
    import sandbox_harness.common.security.capability as capability
    clock=SimpleNamespace(time=lambda:1000)
    monkeypatch.setattr(grants,'time',clock);monkeypatch.setattr(capability,'time',clock)
    issuer=authority();_,parent=issuer.issue_turn(turn_request())
    clock.time=lambda:1001
    with pytest.raises(ValueError,match='exceeds parent'):
        issuer.issue_job(parent,lifetime_job_request(60))


@pytest.mark.parametrize('elapsed',[59,60,61])
def test_inherited_child_lifetime_stops_at_parent_expiry(monkeypatch,elapsed):
    from types import SimpleNamespace
    import sandbox_harness.common.security.grants as grants
    import sandbox_harness.common.security.capability as capability
    clock=SimpleNamespace(time=lambda:1000)
    monkeypatch.setattr(grants,'time',clock);monkeypatch.setattr(capability,'time',clock)
    issuer=authority();_,parent=issuer.issue_turn(turn_request());clock.time=lambda:1000+elapsed
    if elapsed<60:
        _,child=issuer.issue_job(parent,lifetime_job_request(None))
        assert child.expires_at==1060 and child.issued_at==1059
    else:
        with pytest.raises(ValueError,match='exceeds parent'):
            issuer.issue_job(parent,lifetime_job_request(None))


@pytest.mark.parametrize('expansion',[
    {'max_requests':11},{'budget':{'submissions':2}},
    {'source_identity':'other-sandbox'},{'routes':('/v1/ungranted',)},
])
def test_inherited_child_lifetime_preserves_scope_checks(expansion):
    issuer=authority();_,parent=issuer.issue_turn(turn_request())
    with pytest.raises(ValueError,match='exceeds parent'):
        issuer.issue_job(parent,lifetime_job_request(None).model_copy(update=expansion))


def test_job_grant_is_exactly_scoped_and_host_secrets_are_absent():
    grants=authority();_,parent=grants.issue_turn(turn_request())
    token,claims=grants.issue_job(parent,JobGrantRequest(
        group_id="e"*32,job_id="f"*32,generation=3,source_identity="sandbox-1",
        descriptor_id="image.generate",descriptor_version="1",descriptor_digest="1"*64,
        operations=("submit","status"),artifact_ids=("c"*32,),skill_ids=("writing",),
        capability_ids=("image.generate",),operation_ids=(),network_origins=("image-provider",),
        child_profile_id="child-general",runtime_profile_id="general-cpu",
        output_scopes=("preview:candidate",),parameter_digest="2"*64,budget={"submissions":1},
        routes=("/v1/jobs",),ttl_seconds=30,max_requests=5,max_public_requests=1,token_budget=100,
    ))
    verified=grants.verify_call(token,route="/v1/jobs/submit",thread_id="a"*32,turn_id="b"*32,
        group_id="e"*32,job_id="f"*32,generation=3,sandbox_id="sandbox-1",
        source_identity="sandbox-1",descriptor_id="image.generate",descriptor_version="1",
        descriptor_digest="1"*64,operation="submit",artifact_ids={"c"*32},skill_ids={"writing"},
        capability_id="image.generate",network_origin="image-provider",
        output_scope="preview:candidate",parameter_digest="2"*64,
        child_profile_id="child-general",runtime_profile_id="general-cpu",now=claims.issued_at)
    assert verified.child_profile_id=="child-general" and verified.runtime_profile_id=="general-cpu"
    assert verified.job_id=="f"*32 and verified.generation==3
    assert verified.parent_nonce==parent.nonce and verified.parent_token_budget==parent.token_budget
    payload=json.loads(__import__('base64').urlsafe_b64decode(token.split('.')[0]+'=='))
    assert not ({'api_key','credential','base_url','secret'} & set(payload))


@pytest.mark.parametrize(('field','value'),[
    ('job_id','0'*32),('generation',4),('sandbox_id','sandbox-2'),('source_identity','sandbox-2'),
    ('descriptor_id','video.generate'),('descriptor_version','2'),('descriptor_digest','3'*64),
    ('operation','cancel'),('artifact_ids',{'d'*32}),('skill_ids',{'slides'}),
    ('capability_id','video.generate'),('network_origin','other-provider'),
    ('output_scope','delivery:delivery'),('parameter_digest','4'*64),
    ('child_profile_id','other-child'),('runtime_profile_id','other-runtime'),
])
def test_job_grant_rejects_every_scope_expansion(field,value):
    grants=authority();_,parent=grants.issue_turn(turn_request())
    token,claims=grants.issue_job(parent,JobGrantRequest(
        group_id="e"*32,job_id="f"*32,generation=3,source_identity="sandbox-1",
        descriptor_id="image.generate",descriptor_version="1",descriptor_digest="1"*64,
        operations=("submit","status"),artifact_ids=("c"*32,),skill_ids=("writing",),
        capability_ids=("image.generate",),network_origins=("image-provider",),
        child_profile_id="child-general",runtime_profile_id="general-cpu",
        output_scopes=("preview:candidate",),parameter_digest="2"*64,budget={"submissions":1},
        routes=("/v1/jobs",),ttl_seconds=30,max_requests=5,max_public_requests=1,token_budget=100,
    ))
    call=dict(route="/v1/jobs/submit",thread_id="a"*32,turn_id="b"*32,group_id="e"*32,
        job_id="f"*32,generation=3,sandbox_id="sandbox-1",source_identity="sandbox-1",
        descriptor_id="image.generate",descriptor_version="1",descriptor_digest="1"*64,
        operation="submit",artifact_ids={"c"*32},skill_ids={"writing"},
        capability_id="image.generate",network_origin="image-provider",
        output_scope="preview:candidate",parameter_digest="2"*64,now=claims.issued_at)
    call.update(child_profile_id="child-general",runtime_profile_id="general-cpu")
    call[field]=value
    with pytest.raises(ValueError,match="grant scope"):
        grants.verify_call(token,**call)


def test_job_grant_must_be_subset_of_turn_grant_and_nonce_can_be_revoked():
    grants=authority();_,parent=grants.issue_turn(turn_request())
    invalid=JobGrantRequest(group_id="e"*32,job_id="f"*32,generation=1,
        source_identity="sandbox-1",descriptor_id="video.generate",descriptor_version="1",
        descriptor_digest="1"*64,operations=("submit",),artifact_ids=("9"*32,),
        skill_ids=("unknown",),capability_ids=("video.generate",),network_origins=("other",),
        output_scopes=("video:delivery",),parameter_digest="2"*64,budget={},
        routes=("/v1/jobs",),ttl_seconds=30,max_requests=1,max_public_requests=1,token_budget=1)
    with pytest.raises(ValueError,match="parent grant"):
        grants.issue_job(parent,invalid)

    token,claims=grants.issue_turn(turn_request())
    with pytest.raises(ValueError,match="revoked"):
        grants.verify_call(token,route="/v1/jobs",thread_id="a"*32,turn_id="b"*32,
            sandbox_id="sandbox-1",revoked_nonces={claims.nonce},now=claims.issued_at)
