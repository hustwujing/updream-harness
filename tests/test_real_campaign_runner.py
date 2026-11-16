import ast
import importlib.util
from pathlib import Path

import pytest

ROOT=Path(__file__).resolve().parents[1]


def test_campaign_runner_has_bounded_parallel_stop_and_resume_protocol():
    source=(ROOT/"tests/run_real_campaign.py").read_text();ast.parse(source)
    for value in ("--concurrency","default=8","--resume-report","DEFECT_CLASSES",
            "defect.set()","progress-interval","average_seconds","running","--affected-case"):
        assert value in source
    assert "min(8,args.concurrency)" in source
    assert 'default="deep"' in source
    assert "program_failure" in source and "non_program_failure" in source


def test_intervention_cases_use_messages_not_product_keyword_rules():
    source=(ROOT/"tests/run_real_campaign.py").read_text()
    assert "INTERVENTIONS" in source and "delivery_mode\":\"queue" in source
    assert "re.compile" not in source and "regex" not in source.lower()


def test_parallel_protocol_runner_never_fakes_unimplemented_success():
    source=(ROOT/"tests/run_real_parallel_protocol.py").read_text();ast.parse(source)
    assert "protocol_handler_not_implemented" in source
    assert "HANDLERS" in source and "'P02'" in source and "'P12'" in source
    assert all(f"'P{index:02d}'" in source for index in range(1,21))
    assert "classification':'program_failure'" in source


def campaign_module():
    path=Path(__file__).with_name("run_real_campaign.py")
    spec=importlib.util.spec_from_file_location("real_campaign_runner",path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


class Response:
    def __init__(self,value):self.value=value;self.status_code=200
    def json(self):return self.value
    def raise_for_status(self):pass


class MediaClient:
    def __init__(self):
        self.confirmed=False;self.job_polls=0;self.continuation_polls=0

    async def get(self,path):
        if path.endswith("/turns"):
            if self.confirmed and self.job_polls>=2:
                self.continuation_polls+=1
                continuation_state="COMPLETED" if self.continuation_polls>=2 else "RUNNING"
                return Response([{"id":"turn-1","state":"COMPLETED"},
                    {"id":"turn-2","state":continuation_state}])
            return Response([{"id":"turn-1","state":"COMPLETED"}])
        if path.endswith("/media/drafts"):
            return Response([{"id":"draft-1","status":
                "confirmed" if self.confirmed else "awaiting_confirmation"}])
        if path.endswith("/media/jobs"):
            self.job_polls+=1
            return Response([{"id":"job-1","status":"succeeded" if self.job_polls>=2 else "running",
                "phase":"provider_running","error_code":""}])
        if path=="api/turns/turn-2":
            return Response({"id":"turn-2","state":"COMPLETED"})
        raise AssertionError(path)

    async def post(self,path):
        assert path=="api/media/drafts/draft-1/confirm"
        self.confirmed=True;return Response({"id":"job-1","status":"created"})


@pytest.mark.asyncio
async def test_media_campaign_confirms_waits_for_job_and_continuation(monkeypatch):
    campaign=campaign_module();client=MediaClient()
    async def no_sleep(_):pass
    monkeypatch.setattr(campaign.asyncio,"sleep",no_sleep)
    running={"S27":{"state":"RUNNING"}}
    final,jobs=await campaign.settle_media_workflow(client,"thread-1",
        {"id":"turn-1","state":"COMPLETED"},running,"S27",10,allow_external=True)
    assert client.confirmed and client.job_polls>=2 and client.continuation_polls>=2
    assert final=={"id":"turn-2","state":"COMPLETED"}
    assert jobs[0]["status"]=="succeeded"


def test_media_provider_failure_is_not_prompt_failure():
    campaign=campaign_module()
    assert campaign.classify({"state":"COMPLETED"},[],[{
        "status":"failed","error_code":"provider_service_unavailable"}]) == (
            "non_program_failure","provider_service_unavailable")
    assert campaign.classify({"state":"COMPLETED"},[],[{
        "status":"failed","error_code":"artifact_registration_failed"}]) == (
            "program_failure","artifact_registration_failed")
