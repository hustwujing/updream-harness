import pytest

from sandbox_harness.common.contracts.job_contracts import (JobKind,JobState)
from sandbox_harness.common.execution.job_adapters import (
    AdapterStatus,AdapterSubmission,PreAcceptFailure,SubmissionOutcomeUnknown)
from sandbox_harness.common.execution.job_worker import (JobWorker)
from test_job_worker import (FakeAdapter,request,setup)


@pytest.mark.parametrize("boundary",("preflight","submit_unknown","accepted_status"))
async def test_external_execution_fault_matrix_never_blindly_replays(tmp_path,boundary):
    store,_,job=await setup(tmp_path,request("fault-"+boundary,kind=JobKind.PROVIDER))
    class Adapter(FakeAdapter):
        async def preflight(self,context):
            self.preflight_calls+=1
            if boundary=="preflight":
                raise PreAcceptFailure("invalid",error_code="provider_input_invalid",
                    failed_phase="input_validation",retryable=False)
        async def submit(self,context):
            self.submit_calls+=1
            if boundary=="submit_unknown":raise SubmissionOutcomeUnknown("lost")
            return AdapterSubmission("external-one",{"accepted":True})
        async def status(self,context,external_execution_id):
            self.status_calls+=1
            assert external_execution_id=="external-one"
            if boundary=="accepted_status" and self.status_calls==1:
                raise ConnectionError("temporary status outage")
            return AdapterStatus(True,JobState.SUCCEEDED,{"ok":True})
    adapter=Adapter([]);worker=JobWorker(store,{"fake":adapter},node_id="node",
        worker_id="worker",poll_seconds=.01)
    try:
        await worker.run_once();current=await store.get_job(job.id)
        if boundary=="preflight":
            assert current.state==JobState.FAILED and adapter.submit_calls==0
        elif boundary=="submit_unknown":
            assert current.state==JobState.UNKNOWN and adapter.submit_calls==1
            assert await worker.run_once()==0 and adapter.submit_calls==1
        else:
            assert current.state in {JobState.WAITING_EXTERNAL,JobState.UNKNOWN}
            acceptance=await store.latest_job_acceptance(job.id)
            assert acceptance.external_execution_id=="external-one" and adapter.submit_calls==1
    finally:await store.close()
