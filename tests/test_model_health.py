import httpx
import pytest
from sandbox_harness.model_health import classify_response, reduce_health
from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.config import Settings
from sandbox_harness.model_discovery import ModelDiscovery
from sandbox_harness.domain import Harness


def observation(status, error=None, index=0):
    return {'status':status,'available':status!='unavailable','error_code':error,'latency_ms':12,'checked_at':f'check-{index}'}


def test_transient_failures_and_recovery_require_consecutive_observations():
    state=reduce_health({},observation('available'))
    for i in range(2):
        state=reduce_health(state,observation('busy','timeout',i))
        assert state['status']=='available' and state['available']
    state=reduce_health(state,observation('busy','http_503',3))
    assert state['status']=='busy' and state['available']
    state=reduce_health(state,observation('available',index=4))
    assert state['status']=='busy'
    state=reduce_health(state,observation('busy','http_429',5))
    state=reduce_health(state,observation('available',index=6))
    assert state['status']=='busy'
    state=reduce_health(state,observation('available',index=7))
    assert state['status']=='available'


def test_hard_rejection_is_not_cleared_by_timeout_or_single_success():
    state=reduce_health({},observation('unavailable','model_route_missing'))
    assert not state['available']
    state=reduce_health(state,observation('busy','timeout'))
    assert not state['available'] and state['blocking_reason']=='model_route_missing'
    state=reduce_health(state,observation('available'))
    assert not state['available']
    state=reduce_health(state,observation('available'))
    assert state['available'] and state['blocking_reason'] is None


@pytest.mark.parametrize('status,body,outcome,reason',[
    (503,{'error':{'message':'overloaded'}},'busy','http_503'),
    (429,{'error':{'message':'rate limited'}},'busy','http_429'),
    (400,{'error':{'code':'invalid_request_error','message':'max_output_tokens too small'}},'busy','http_400'),
    (400,{'error':{'code':'model_not_found'}},'unavailable','model_not_supported'),
    (500,{'error':{'message':'no cluster found for model: deepseek-r1'}},'unavailable','model_route_missing'),
    (500,{'error':{'message':'internal error'}},'busy','http_500'),
    (401,{'error':{'message':'invalid key'}},'unavailable','access_denied'),
    (404,{},'unavailable','model_route_missing'),
    (405,None,'unavailable','model_route_missing'),
    (400,{'error':{'message':'unsupported endpoint route'}},'unavailable','model_route_missing'),
    (200,{'error':{'type':'overloaded_error'}},'busy','invalid_probe_response'),
])
def test_classifier_uses_reason_instead_of_treating_every_error_as_unsupported(status,body,outcome,reason):
    assert classify_response(httpx.Response(status,json=body))==(outcome,reason)


async def test_probe_streaks_survive_restart_and_busy_model_remains_selectable(tmp_path):
    status=503
    def handle(request):
        if request.method=='GET':return httpx.Response(200,json={'data':[{'id':'gpt-5.6'}]})
        return httpx.Response(status,json={'ok':status==200})
    settings=Settings(data_root=tmp_path,upstream_codex='https://upstream',upstream_claude='',upstream_deepseek='')
    catalog=HarnessCatalog();discovery=ModelDiscovery(settings,catalog,httpx.MockTransport(handle))
    await discovery.refresh();await discovery.refresh()
    restored=HarnessCatalog();next_discovery=ModelDiscovery(settings,restored,httpx.MockTransport(handle))
    assert next_discovery.load_snapshot()
    await next_discovery.refresh()
    assert restored.availability[(Harness.CODEX,'gpt-5.6')]['status']=='busy'
    assert restored.resolve('codex','gpt-5.6','standard').model=='gpt-5.6'
    status=200
    await next_discovery.refresh()
    assert restored.availability[(Harness.CODEX,'gpt-5.6')]['status']=='busy'
    await next_discovery.refresh()
    assert restored.availability[(Harness.CODEX,'gpt-5.6')]['status']=='available'


async def test_confirmed_unsupported_model_is_rejected_at_submission(tmp_path):
    def handle(request):
        if request.method=='GET':return httpx.Response(200,json={'data':[{'id':'gpt-5.6'}]})
        return httpx.Response(400,json={'error':{'code':'model_not_found'}})
    settings=Settings(data_root=tmp_path,upstream_codex='https://upstream',upstream_claude='',upstream_deepseek='')
    catalog=HarnessCatalog();discovery=ModelDiscovery(settings,catalog,httpx.MockTransport(handle))
    await discovery.refresh()
    with pytest.raises(ValueError,match='not supported'):
        catalog.resolve('codex','gpt-5.6','standard')
