import ast
import importlib.util
import io
from pathlib import Path

import pytest
from PIL import Image

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
    assert 'INTERVENTION_ASSETS={"intervention_replace_source":["poster.png"]}' in source
    assert '"attachments":intervention_attachments' in source


def test_parallel_protocol_runner_never_fakes_unimplemented_success():
    source=(ROOT/"tests/run_real_parallel_protocol.py").read_text();ast.parse(source)
    assert "protocol_handler_not_implemented" in source
    assert "HANDLERS" in source and "'P02'" in source and "'P12'" in source
    assert all(f"'P{index:02d}'" in source for index in range(1,21))
    assert "classification':'program_failure'" in source


def test_dynamic_resize_case_grades_the_post_intervention_contract():
    catalog=__import__('json').loads((ROOT/'tests/real_cases/cases.json').read_text())
    case=next(row for row in catalog['cases'] if row['id']=='R02')
    assert case['dynamic_hook']=='intervention_resize_format'
    assert case['expect']=={'kind':'image','width':256,'height':128,
        'extensions':['.png','.webp'],'min_images':2}


def test_caption_intervention_contains_content_timing_and_style():
    source=(ROOT/'tests/run_real_campaign.py').read_text()
    assert '0–2秒显示“PORTRAIT”，2–4秒显示“ROCKET”' in source


def test_portrait_intervention_grades_the_post_intervention_contract():
    catalog=__import__('json').loads((ROOT/'tests/real_cases/cases.json').read_text())
    case=next(row for row in catalog['cases'] if row['id']=='R09')
    assert case['dynamic_hook']=='intervention_portrait'
    assert case['expect']=={'kind':'video','width':360,'height':640,'duration':8}


def campaign_module():
    path=Path(__file__).with_name("run_real_campaign.py")
    spec=importlib.util.spec_from_file_location("real_campaign_runner",path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


class Response:
    def __init__(self,value):self.value=value;self.content=value if isinstance(value,bytes) else b"";self.status_code=200
    def json(self):return self.value
    def raise_for_status(self):pass


class MediaClient:
    def __init__(self):
        self.confirmed=False;self.job_polls=0;self.continuation_polls=0

    async def get(self,path,params=None):
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
        if path.endswith("/job-group-drafts"):return Response([])
        if path.endswith("/execution-graph"):return Response({"nodes":[]})
        if path=="api/turns/turn-2":
            return Response({"id":"turn-2","state":"COMPLETED"})
        raise AssertionError(path)

    async def post(self,path):
        assert path=="api/media/drafts/draft-1/confirm"
        self.confirmed=True;return Response({"id":"job-1","status":"created"})


class ArtifactClient:
    def __init__(self,values):self.values=values
    async def get(self,path):return Response(self.values[path.rsplit("/",1)[-1]])


class JobGroupClient:
    def __init__(self):self.graph_polls=0
    async def get(self,path,params=None):
        if path.endswith('/turns'):
            turns=[{'id':'turn-1','state':'COMPLETED'}]
            if self.graph_polls>=3:turns.append({'id':'turn-2','state':'COMPLETED'})
            return Response(turns)
        if path.endswith('/job-group-drafts') or path.endswith('/media/drafts') or path.endswith('/media/jobs'):
            return Response([])
        if path.endswith('/execution-graph'):
            self.graph_polls+=1;state='RUNNING' if self.graph_polls<3 else 'CONTINUED'
            return Response({'nodes':[{'id':'group-1','type':'job_group','state':state}]})
        raise AssertionError(path)


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


@pytest.mark.asyncio
async def test_campaign_waits_for_job_group_continuation_and_quiescence(monkeypatch):
    campaign=campaign_module();client=JobGroupClient()
    async def no_sleep(_):pass
    monkeypatch.setattr(campaign.asyncio,'sleep',no_sleep)
    running={'C16':{'state':'RUNNING'}}
    final,jobs=await campaign.settle_media_workflow(client,'thread-1',
        {'id':'turn-1','state':'COMPLETED'},running,'C16',10,allow_external=True)
    assert client.graph_polls>=5 and final=={'id':'turn-2','state':'COMPLETED'} and jobs==[]


def test_media_provider_failure_is_not_prompt_failure():
    campaign=campaign_module()
    assert campaign.classify({"state":"COMPLETED"},[],[{
        "status":"failed","error_code":"provider_service_unavailable"}]) == (
            "non_program_failure","provider_service_unavailable")
    assert campaign.classify({"state":"COMPLETED"},[],[{
        "status":"failed","error_code":"artifact_registration_failed"}]) == (
            "program_failure","artifact_registration_failed")


def test_silence_gate_accepts_codec_floor_but_rejects_audible_signal():
    campaign=campaign_module()
    assert campaign.volume_is_silent("mean_volume: -91.0 dB\nmax_volume: -91.0 dB")
    assert campaign.volume_is_silent("max_volume: -inf dB")
    assert not campaign.volume_is_silent("max_volume: -42.3 dB")


@pytest.mark.asyncio
async def test_artifact_validator_checks_image_dimensions_and_text_contract():
    campaign=campaign_module();image=io.BytesIO();Image.new("RGBA",(32,24),(1,2,3,0)).save(image,"PNG")
    client=ArtifactClient({"image-1":image.getvalue(),"text-1":b"Blue square final video"})
    artifacts=[{"summary":"result.png","payload":{"artifact_id":"image-1","mime":"image/png"}},
        {"summary":"report.txt","payload":{"artifact_id":"text-1","mime":"text/plain"}}]
    errors,evidence=await campaign.validate_artifacts(client,{"expect":{"kind":"image","width":32,
        "height":24,"alpha":True,"extra_text":True}},artifacts)
    assert errors==[] and evidence[0]["alpha"] is True
    errors,_=await campaign.validate_artifacts(client,{"expect":{"kind":"image","width":64,
        "height":24,"terms":["missing"]}},artifacts)
    assert "missing_term:missing" in errors and any(row.startswith("dimensions:") for row in errors)
