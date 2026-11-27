import ast
import importlib.util
import io
from pathlib import Path
import tarfile
import time

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
    assert "write_report_checkpoint" in source and "os.fsync" in source
    assert "temporary.replace(target)" in source


def test_real_case_catalog_includes_every_inherited_d_case_with_provenance():
    catalog=__import__('json').loads((ROOT/'tests/real_cases/cases.json').read_text())
    groups={prefix:[] for prefix in ('S','C','R','D','P','K','XDB','EVO')}
    for case in catalog['cases']:
        prefix=('XDB' if case['id'].startswith('XDB') else 'EVO'
            if case['id'].startswith('EVO') else case['id'][0])
        groups[prefix].append(case)
    assert {key:len(value) for key,value in groups.items()}=={
        'S':31,'C':30,'R':17,'D':51,'P':26,'K':14,'XDB':6,'EVO':18}
    assert len(catalog['cases'])==193 and catalog['runnable_case_count']==193
    assert not catalog['superseded_cases']
    assert [case['id'] for case in groups['XDB']]==[
        f'XDB{index:02d}' for index in range(1,7)]
    assert all(case['executor']=='cross_database' for case in groups['XDB'])
    assert all(case['executor']=='evolution' for case in groups['EVO'])
    assert [case['id'] for case in groups['D']]==[f'D{index:02d}' for index in range(1,52)]
    assert all(case['executor']=='inherited_protocol' for case in groups['D'])
    assert all(case['provenance']==f"sandbox-harness-platform-plus:{case['id']}"
        and case['source_project']=='sandbox-harness-platform-plus'
        and case['source_definition_sha256'] and case['parallel_adaptation']
        for case in groups['D'])


def test_intervention_cases_use_messages_not_product_keyword_rules():
    source=(ROOT/"tests/run_real_campaign.py").read_text()
    assert "INTERVENTIONS" in source and "/inputs" in source and "/interactions/" in source
    assert "re.compile" not in source and "regex" not in source.lower()
    assert 'INTERVENTION_ASSETS={"intervention_replace_source":["poster.png"]}' in source
    assert 'deliver_intervention(client,thread,turn,message,intervention_attachments)' in source
    assert '"attachments":attachments' in source
    assert 'AFTER_ARTIFACT_INTERVENTIONS' in source and "intervention_artifact_timeout" in source
    assert '"intervention_feedback"' in source
    assert 'current.json()["state"] not in TERMINAL' in source
    assert 'stable_terminal_polls>=stable_polls' in source


def test_parallel_protocol_runner_never_fakes_unimplemented_success():
    source=(ROOT/"tests/run_real_parallel_protocol.py").read_text();ast.parse(source)
    assert "protocol_handler_not_implemented" in source
    assert "HANDLERS" in source and "'P02'" in source and "'P12'" in source
    assert all(f"'P{index:02d}'" in source for index in range(1,28))
    assert "compile_delegation" in source and "metadata_required':False" in source
    assert 'parser.add_argument(\'--timeout\'' in source
    assert 'parser.add_argument(\'--intervention-delay\'' in source
    assert "classification':'program_failure'" in source
    assert "in {'program_failure','optimizable_prompt'}:raise SystemExit(2)" in source
    assert source.count("_,continuations=await wait_continuations(client,thread['id'],maximum=1)")==5
    assert all(f"'K{index:02d}'" in source for index in range(1,15))
    assert 'case_skill_coverage' in source and all(
        f'case_k{index:02d}' in source for index in range(7,12))
    assert "SERIAL_CASES={'P13','P19','P21','P27','K08','AS01'}" in source
    assert "'video.generate'" in source and "'video.edit'" in source
    assert "'audio.generate'" in source and "'model3d.transform'" in source
    assert 'REST versus gRPC, monolith versus microservices, and SQL versus NoSQL' in source
    campaign=(ROOT/"tests/run_real_campaign.py").read_text()
    assert '"--harness",args.harness' in campaign
    assert '"--model",args.model' in campaign


def test_real_protocol_service_controls_are_deployment_configurable():
    campaign=(ROOT/'tests/run_real_campaign.py').read_text()
    inherited=(ROOT/'tests/run_real_inherited_protocol.py').read_text()
    protocol=(ROOT/'tests/run_real_parallel_protocol.py').read_text()
    for option in ('--job-worker-unit','--job-worker-start-command','--api-unit'):
        assert option in campaign
        assert option in inherited
        assert option in protocol
    assert "service_unit(args,'job_worker')" in protocol
    assert "service_unit(args,'api')" in protocol
    assert "service_command(args,'job_worker','start')" in protocol
    assert "'sandbox-harness-parallel-ali-job-worker.service'" not in protocol
    assert "'sandbox-harness-parallel-ali-api.service'" not in protocol


def test_campaign_routes_cross_database_cases_to_the_dedicated_runner():
    campaign=(ROOT/'tests/run_real_campaign.py').read_text()
    assert 'async def cross_database_case(' in campaign
    assert 'run_real_cross_database.py' in campaign
    assert 'case["executor"]=="cross_database"' in campaign


def test_evo03_kill_recovers_persisted_retained_sandbox_expiry():
    source=(ROOT/'tests/run_real_evolution.py').read_text()
    body=source[source.index('async def evo03'):source.index('async def evo04')]
    assert 'retained_until' in body
    assert 'kill_restart(args.platform_worker_unit)' in body
    assert 'retained_sandbox_released' in body


def test_runtime_recovery_evolution_cases_have_real_handlers():
    source=(ROOT/'tests/run_real_evolution.py').read_text();ast.parse(source)
    assert all(f'async def evo{index:02d}' in source for index in range(16,19))
    assert "range(1,19)" in source
    assert 'LostRestoreResponseTransport' in source
    assert 'sandbox_workspace_restore_unavailable' in source
    assert 'speech-2.8-turbo' in source


def test_k11_multiview_uses_four_views_of_one_subject():
    source=(ROOT/'tests/run_real_parallel_protocol.py').read_text()
    body=source[source.index('async def case_k11'):source.index('async def thread_artifact_names')]
    assert "k11-multiview" in body and "def render(name,yaw)" in body
    assert "ref(views['front'],'front_view')" in body
    assert "ref(views['left'],'left_view')" in body
    assert "ref(views['back'],'back_view')" in body
    assert "ref(views['right'],'right_view')" in body
    assert "ref(poster,'front_view'),ref(rocket,'left_view')" not in body


def test_dynamic_resize_case_grades_the_post_intervention_contract():
    catalog=__import__('json').loads((ROOT/'tests/real_cases/cases.json').read_text())
    case=next(row for row in catalog['cases'] if row['id']=='R02')
    assert case['dynamic_hook']=='intervention_resize_format'
    assert case['expect']=={'kind':'image','width':256,'height':128,
        'extensions':['.png','.webp'],'min_images':2}


def test_caption_intervention_contains_content_timing_and_style():
    source=(ROOT/'tests/run_real_campaign.py').read_text()
    assert '0–2秒显示“PORTRAIT”，2–4秒显示“ROCKET”' in source


def test_thumbnail_intervention_fixes_the_fit_decision():
    source=(ROOT/'tests/run_real_campaign.py').read_text()
    assert '追加320×180 JPEG缩略图，保持比例居中裁切' in source


def test_feedback_intervention_identifies_the_local_repair():
    source=(ROOT/'tests/run_real_campaign.py').read_text()
    assert '只修复 portrait 半边右下角的黑色头盔和台座区域' in source
    assert 'rocket 半边保持不变，不需要再询问' in source


def test_portrait_intervention_grades_the_post_intervention_contract():
    catalog=__import__('json').loads((ROOT/'tests/real_cases/cases.json').read_text())
    case=next(row for row in catalog['cases'] if row['id']=='R09')
    assert case['dynamic_hook']=='intervention_portrait'
    assert case['expect']=={'kind':'video','width':360,'height':640,'duration':8}


def test_vertical_replan_grades_the_post_intervention_contract():
    catalog=__import__('json').loads((ROOT/'tests/real_cases/cases.json').read_text())
    case=next(row for row in catalog['cases'] if row['id']=='R13')
    assert case['dynamic_hook']=='intervention_pause_replan'
    assert case['expect']=={'kind':'image','width':64,'height':128}


def campaign_module():
    path=Path(__file__).with_name("run_real_campaign.py")
    spec=importlib.util.spec_from_file_location("real_campaign_runner",path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def test_skill_product_archive_validation_rejects_links_and_traversal():
    path=ROOT/'tests/run_real_parallel_protocol.py'
    spec=importlib.util.spec_from_file_location('parallel_protocol_archive',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    def archive(member,data=b'product'):
        stream=io.BytesIO()
        with tarfile.open(fileobj=stream,mode='w:gz') as output:
            member.size=len(data);output.addfile(member,io.BytesIO(data))
        return stream.getvalue()
    regular=tarfile.TarInfo('product/result.md')
    assert module.validate_skill_product_archive(archive(regular))['files']==['product/result.md']
    traversal=tarfile.TarInfo('../escape.txt')
    with pytest.raises(AssertionError,match='path invalid'):
        module.validate_skill_product_archive(archive(traversal))
    link=tarfile.TarInfo('product/link');link.type=tarfile.SYMTYPE;link.linkname='/etc/passwd'
    with pytest.raises(AssertionError,match='path invalid'):
        module.validate_skill_product_archive(archive(link,b''))


def test_skill_product_archive_accepts_safe_dot_root_directory():
    path=ROOT/'tests/run_real_parallel_protocol.py'
    spec=importlib.util.spec_from_file_location('parallel_protocol_dot_archive',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    stream=io.BytesIO()
    with tarfile.open(fileobj=stream,mode='w:gz') as output:
        root=tarfile.TarInfo('.');root.type=tarfile.DIRTYPE;output.addfile(root)
        file=tarfile.TarInfo('./result.md');data=b'product';file.size=len(data)
        output.addfile(file,io.BytesIO(data))
    assert module.validate_skill_product_archive(stream.getvalue())['files']==['result.md']


def test_skill_coverage_objective_requires_successful_background_cleanup():
    path=ROOT/'tests/run_real_parallel_protocol.py'
    spec=importlib.util.spec_from_file_location('parallel_protocol_background_rule',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    objective=module.skill_coverage_objective('webapp-testing','render a local page')
    assert 'must exit successfully' in objective
    assert 'do not cancel a registered background job' in objective


def test_campaign_report_distinguishes_clean_run_from_composite_reruns(tmp_path):
    module=campaign_module();target=tmp_path/'report.json';catalog=tmp_path/'catalog.json'
    module.write_report_checkpoint(target,revision='new',catalog=catalog,selected_count=2,
        started_count=1,stopped_for_defect=False,results={
            'K09':{'classification':'success','run_revision':'old'},
            'K10':{'classification':'success','run_revision':'new'}})
    assert __import__('json').loads(target.read_text())['verification_scope']=='composite_verified'
    module.write_report_checkpoint(target,revision='new',catalog=catalog,selected_count=2,
        started_count=2,stopped_for_defect=False,results={
            'K09':{'classification':'success','run_revision':'new'},
            'K10':{'classification':'success','run_revision':'new'}})
    assert __import__('json').loads(target.read_text())['verification_scope']=='clean_run'


class Response:
    def __init__(self,value):self.value=value;self.content=value if isinstance(value,bytes) else b"";self.status_code=200
    def json(self):return self.value
    def raise_for_status(self):pass


class InterventionClient:
    def __init__(self,state,interaction=None):
        self.state=state;self.interaction=interaction;self.posts=[]
    async def get(self,path,params=None):
        if path=="api/turns/turn":return Response({"id":"turn","state":self.state})
        if path=="api/threads/thread/interactions":return Response([self.interaction] if self.interaction else [])
        raise AssertionError(path)
    async def post(self,path,json):
        self.posts.append((path,json))
        if path.endswith("/inputs"):return Response({"id":"input"})
        if "/interactions/" in path:return Response({"status":"responded"})
        if path.endswith("/turns"):return Response({"id":"successor"})
        raise AssertionError(path)


@pytest.mark.asyncio
async def test_intervention_uses_active_input_structured_response_and_terminal_successor():
    campaign=campaign_module();thread={"id":"thread"};turn={"id":"turn"}
    running=InterventionClient("RUNNING")
    value=await campaign.deliver_intervention(running,thread,turn,"change",[])
    assert value["mode"]=="input" and running.posts[0][0]=="api/turns/turn/inputs"
    interaction={"id":"decision","turn_id":"turn","status":"pending","kind":"question",
        "request":{"questions":[{"id":"choice"}]}}
    waiting=InterventionClient("AWAITING_USER",interaction)
    value=await campaign.deliver_intervention(waiting,thread,turn,"new direction",[])
    assert value["mode"]=="interaction"
    assert waiting.posts[0][1]["action"]=="answer" and waiting.posts[0][1]["answers"]=={
        "choice":"new direction"}
    completed=InterventionClient("COMPLETED")
    value=await campaign.deliver_intervention(completed,thread,turn,"next",[])
    assert value=={"mode":"turn","turn_id":"successor"}
    assert completed.posts[0][1]["delivery_mode"]=="queue"


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


class MediaContinuationRaceClient:
    def __init__(self):
        self.confirmed=False;self.job_polls=0;self.graph_polls=0

    async def get(self,path,params=None):
        if path.endswith('/turns'):
            turns=[{'id':'turn-1','state':'COMPLETED'}]
            if self.confirmed:turns.append({'id':'unrelated-continuation','state':'COMPLETED'})
            if self.graph_polls>=4:turns.append({'id':'media-continuation','state':'COMPLETED'})
            return Response(turns)
        if path.endswith('/media/drafts'):
            return Response([{'id':'draft-1','status':
                'confirmed' if self.confirmed else 'awaiting_confirmation'}])
        if path.endswith('/media/jobs'):
            self.job_polls+=1
            return Response([{'id':'job-1','status':'succeeded' if self.job_polls>=2 else 'running',
                'phase':'committed' if self.job_polls>=2 else 'provider_polling','error_code':''}])
        if path.endswith('/job-group-drafts'):return Response([])
        if path.endswith('/execution-graph'):
            self.graph_polls+=1
            state='CONTINUED' if self.graph_polls>=4 else 'WAITING_EXTERNAL'
            return Response({'nodes':[{'id':'media-batch:turn-1','type':'media_batch','state':state}]})
        raise AssertionError(path)

    async def post(self,path):
        assert path=='api/media/drafts/draft-1/confirm'
        self.confirmed=True;return Response({'id':'job-1','status':'created'})


class PaginatedItemClient:
    async def get(self,path,params=None):
        assert path.endswith('/items');after=int(params['after']);limit=int(params['limit'])
        rows=[{'sequence':index,'type':'tool'} for index in range(after+1,min(after+limit,500)+1)]
        if after>=500:rows=[{'sequence':501,'type':'artifact'}] if after==500 else []
        return Response(rows)


class DelayedArtifactClient:
    def __init__(self):self.item_polls=0
    async def get(self,path,params=None):
        if path.endswith('/items'):
            self.item_polls+=1
            return Response([] if self.item_polls<2 else [
                {'sequence':1,'type':'artifact','summary':'result.png'}])
        if path=='api/turns/turn-1':return Response({'id':'turn-1','state':'COMPLETED'})
        if path.endswith('/turns'):return Response([{'id':'turn-1','state':'COMPLETED'}])
        if path.endswith('/execution-graph'):return Response({'nodes':[]})
        raise AssertionError(path)


class ContinuationGapClient:
    def __init__(self):self.item_polls=0
    async def get(self,path,params=None):
        if path.endswith('/items'):
            self.item_polls+=1
            return Response([] if self.item_polls<6 else [
                {'sequence':9,'type':'artifact','summary':'final.png'}])
        if path=='api/turns/turn-1':return Response({'id':'turn-1','state':'COMPLETED'})
        if path.endswith('/turns'):
            rows=[{'id':'turn-1','state':'COMPLETED'}]
            if 2<=self.item_polls<6:rows.append({'id':'continuation','state':'RUNNING'})
            return Response(rows)
        if path.endswith('/execution-graph'):return Response({'nodes':[]})
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


@pytest.mark.asyncio
async def test_campaign_does_not_mistake_unrelated_turn_for_media_continuation(monkeypatch):
    campaign=campaign_module();client=MediaContinuationRaceClient()
    async def no_sleep(_):pass
    monkeypatch.setattr(campaign.asyncio,'sleep',no_sleep)
    running={'C30':{'state':'RUNNING'}}
    final,jobs=await campaign.settle_media_workflow(client,'thread-1',
        {'id':'turn-1','state':'COMPLETED'},running,'C30',10,allow_external=True)
    assert client.graph_polls>=4
    assert final=={'id':'media-continuation','state':'COMPLETED'}
    assert jobs==[{'id':'job-1','status':'succeeded','phase':'committed','error_code':''}]


@pytest.mark.asyncio
async def test_item_reader_paginates_to_tail_artifacts():
    campaign=campaign_module();items=await campaign.list_all_items(PaginatedItemClient(),'thread-1')
    assert len(items)==501 and items[-1]=={'sequence':501,'type':'artifact'}


@pytest.mark.asyncio
async def test_after_delivery_intervention_waits_through_terminal_artifact_visibility_race(monkeypatch):
    campaign=campaign_module();client=DelayedArtifactClient()
    async def no_sleep(_):pass
    monkeypatch.setattr(campaign.asyncio,'sleep',no_sleep)
    value=await campaign.wait_for_first_artifact(client,'thread-1','turn-1',
        time.monotonic()+10)
    assert value is True and client.item_polls==2


@pytest.mark.asyncio
async def test_after_delivery_intervention_tracks_internal_continuations(monkeypatch):
    campaign=campaign_module();client=ContinuationGapClient()
    async def no_sleep(_):pass
    monkeypatch.setattr(campaign.asyncio,'sleep',no_sleep)
    value=await campaign.wait_for_first_artifact(client,'thread-1','turn-1',
        time.monotonic()+10)
    assert value is True and client.item_polls==6


def test_media_provider_failure_is_not_prompt_failure():
    campaign=campaign_module()
    assert campaign.classify({"state":"COMPLETED"},[],[{
        "status":"failed","error_code":"provider_service_unavailable"}]) == (
            "non_program_failure","provider_service_unavailable")
    assert campaign.classify({"state":"COMPLETED"},[],[{
        "status":"failed","error_code":"artifact_registration_failed"}]) == (
        "program_failure","artifact_registration_failed")
    classification,reason=campaign.classify({"state":"COMPLETED"},[{"type":"artifact"}],[{
        "status":"failed","error_code":"provider_endpoint_unavailable"}])
    assert campaign.combine_job_outcome(classification,reason,None,["min_images"]) == (
        "non_program_failure","provider_endpoint_unavailable")


def test_durable_job_failure_preserves_model_budget_root_cause():
    campaign=campaign_module()
    assert campaign.classify_job_failures([{'state':'FAILED','result':{
        'error_code':'harness_failed',
        'error_detail':'API Error: 429 {"detail":"model_token_budget_exhausted"}'}}]) == (
            'non_program_failure','model_token_budget_exhausted')
    assert campaign.classify_job_failures([{'state':'FAILED','result':{
        'error_code':'harness_failed','error_detail':'invalid child output'}}]) == (
            'program_failure','harness_failed')


def test_authoritative_awaiting_user_is_a_terminal_prompt_outcome():
    campaign=campaign_module()
    assert "AWAITING_USER" in campaign.TERMINAL
    assert campaign.classify({"state":"AWAITING_USER","error_detail":""},[]) == (
        "optimizable_prompt","unclear_user_intervention")


@pytest.mark.parametrize('code',['submission_outcome_unknown','external_status_unavailable'])
def test_completed_primary_does_not_hide_internal_job_defects(code):
    campaign=campaign_module()
    outcome=campaign.classify_job_failures([{'state':'UNKNOWN','result':{'error_code':code}}])
    assert campaign.combine_job_outcome('success','',outcome)==('program_failure',code)


def test_completed_primary_can_recover_from_non_program_job_failure():
    campaign=campaign_module()
    outcome=campaign.classify_job_failures([{'state':'FAILED','result':{
        'error_code':'model_rate_limited'}}])
    assert campaign.combine_job_outcome('success','',outcome)==('success','')
    assert campaign.combine_job_outcome('program_failure','runtime_execution_failed',outcome)==(
        'program_failure','runtime_execution_failed')


def test_non_program_child_failure_cannot_hide_invalid_final_artifact():
    campaign=campaign_module()
    assert campaign.combine_job_outcome('success','',
        ('non_program_failure','model_rate_limited'),['image_dimensions'])==(
            'program_failure','artifact_contract:image_dimensions')


def test_waiting_user_report_includes_the_actual_pending_prompt():
    campaign=campaign_module()
    reason=campaign.waiting_interaction_reason([{'turn_id':'turn-1','status':'pending',
        'request':{'prompt':'请上传逐词转写文件'}}],'turn-1')
    assert reason=='unexpected_user_decision:请上传逐词转写文件'


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


@pytest.mark.asyncio
async def test_artifact_validator_checks_declared_image_source_order(tmp_path):
    campaign=campaign_module();left=tmp_path/'left.png';right=tmp_path/'right.png'
    Image.new('RGB',(40,40),(220,20,20)).save(left)
    Image.new('RGB',(40,40),(20,20,220)).save(right)
    composite=io.BytesIO();image=Image.new('RGB',(80,40));image.paste(Image.open(right),(0,0));image.paste(
        Image.open(left),(40,0));image.save(composite,'PNG')
    artifacts=[{'summary':'result.png','payload':{'artifact_id':'image-1','mime':'image/png'}}]
    errors,evidence=await campaign.validate_artifacts(ArtifactClient({'image-1':composite.getvalue()}),
        {'expect':{'kind':'image','width':80,'height':40,
            'source_order':['left.png','right.png']}},artifacts,asset_root=tmp_path)
    assert errors==['source_order:result.png:0:right.png:left.png',
        'source_order:result.png:1:left.png:right.png']
    assert evidence[0]['source_matches']==['right.png','left.png']
