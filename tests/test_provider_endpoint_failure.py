import pytest

from sandbox_harness.common.configuration.config import (Settings)
from sandbox_harness.common.execution.job_adapters import (JobExecutionContext)
from sandbox_harness.common.contracts.job_contracts import (JobState)
from sandbox_harness.common.contracts.job_errors import (normalize_job_error)
from sandbox_harness.services.media_worker.media_errors import (classify_provider_error)
from sandbox_harness.services.job_worker.media_job_adapter import (MediaProviderJobAdapter)
from sandbox_harness.common.storage.objects import (ObjectStore)
from test_media_job_adapter import Catalog,setup
from test_media_worker import Client,Response,worker


def test_closed_endpoint_is_configuration_failure_not_invalid_user_input():
    assert classify_provider_error(400,'InvalidEndpoint.ClosedEndpoint')=='provider_endpoint_unavailable'
    assert classify_provider_error(400,'InvalidParameter')=='provider_input_invalid'
    assert classify_provider_error(503,'')=='provider_service_unavailable'


async def test_closed_endpoint_is_definitively_rejected_without_resubmission(monkeypatch):
    value=worker();client=Client([Response({'error':{'code':'InvalidEndpoint.ClosedEndpoint'}},400)])
    async def factory():return client
    monkeypatch.setattr(value,'_client',factory)
    result=await value._ark_image({'id':'job','generation':1,'model':'seedream-5.0','parameters':{'prompt':'test'}})
    assert result.status=='failed'
    assert result.error_code=='provider_endpoint_unavailable'
    assert len(client.posts)==1


@pytest.mark.parametrize('already_accepted',[False,True])
async def test_media_adapter_retains_confirmed_provider_rejection_semantics(tmp_path,already_accepted):
    store,thread,job,attempt,request=await setup(tmp_path)
    adapter=MediaProviderJobAdapter(Settings(backend='fake',data_root=tmp_path),store,ObjectStore(tmp_path/'objects'),Catalog())
    context=JobExecutionContext(job=job,request=request,generation=attempt.generation)
    try:
        accepted=await adapter.submit(context);claimed=(await store.claim_media_jobs('media-worker'))[0]
        await store.update_media_job(claimed['id'],'media-worker',claimed['generation'],status='failed',phase='terminal',error_code='provider_endpoint_unavailable',
            provider_job_id='upstream-job' if already_accepted else '')
        status=await adapter.status(context,accepted.external_execution_id)
        assert status.terminal and status.state==JobState.FAILED
        result=normalize_job_error(status.result,side_effect_state='accepted')
        assert result['failed_phase']=='submission'
        assert result['side_effect_state']==('accepted' if already_accepted else 'none')
        assert not result['retryable'] and result['retry_class']=='none'
    finally:await store.close()


@pytest.mark.parametrize('code,expected',[('provider_output_unavailable','completed'),('provider_submission_outcome_unknown','unknown')])
def test_existing_execution_proof_does_not_downgrade_completed_or_unknown(code,expected):
    from datetime import datetime,timezone
    from sandbox_harness.common.db.store import (Store, MediaJobRow)
    row=MediaJobRow(id='job',thread_id='thread',kind='image',model='image-model',provider='provider',adapter='adapter',
        execution_mode='async',cancellation_mode='supported',status='failed',phase='terminal',
        error_code=code,provider_job_id='upstream-job',output_role='candidate',created_at=datetime.now(timezone.utc))
    assert Store._media_job_view(row).side_effect_state==expected
