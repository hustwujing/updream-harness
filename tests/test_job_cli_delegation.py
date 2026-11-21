import pytest

from sandbox_runtime.job_cli import DELEGATION_EXAMPLE,compile_delegation,main


def catalog():
    return {
        'child_execution_profiles':[{'id':'child-general','supported_harnesses':['codex'],
            'allowed_skill_ids':['media-processing'],'job_template':{'kind':'harness',
                'adapter':'child-harness','descriptor_id':'delegate.harness','descriptor_version':'1',
                'descriptor_digest':'d'*64,'child_profile_id':'child-general'}}],
        'skills':[{'id':'media-processing','supported_harnesses':['codex'],
            'required_capabilities':[],'network_origins':[]}],
        'operations':[{'id':'image.segment'}],'capabilities':[{'id':'image.generate'}],
        'input_artifacts':[{'artifact_id':'a'*32,'project_path':'assets/source.png'}],
    }


def identity():
    return {'HARNESS_THREAD_ID':'b'*32,'HARNESS_TURN_ID':'c'*32,
        'HARNESS_KIND':'codex','HARNESS_MODEL':'gpt-5.6'}


def test_artifact_delegation_requires_at_least_one_output():
    value=compile_delegation({'tasks':[{'id':'icons','objective':'Create icons',
        'inputs':['assets/source.png'],'skill_ids':['media-processing'],
        'result_mode':'artifacts','output_disposition':'delivery'}]},catalog(),identity())
    assert value['jobs'][0]['expected_outputs'][0]['min_items']==1


def test_mixed_delegation_keeps_artifacts_optional():
    value=compile_delegation({'tasks':[{'id':'analysis','objective':'Analyze and optionally emit',
        'result_mode':'mixed','output_disposition':'workspace'}]},catalog(),identity())
    assert value['jobs'][0]['expected_outputs'][0]['min_items']==0


def test_incompatible_skill_fails_before_child_sandbox_creation():
    value=catalog();value['child_execution_profiles'][0]['allowed_skill_ids'].append('codex-ppt')
    value['skills'].append({'id':'codex-ppt','supported_harnesses':['codex'],
        'required_capabilities':['image.generate'],
        'network':{'mode':'required','origins':['image-provider']}})
    with pytest.raises(ValueError,match='outside child-general: codex-ppt'):
        compile_delegation({'tasks':[{'id':'deck','objective':'Build deck',
            'skill_ids':['codex-ppt'],'result_mode':'artifacts',
            'output_disposition':'delivery'}]},value,identity())
    compiled=compile_delegation({'tasks':[{'id':'deck','objective':'Build deck',
        'skill_ids':['codex-ppt'],'capability_ids':['image.generate'],
        'result_mode':'artifacts','output_disposition':'delivery'}]},value,identity())
    assert compiled['jobs'][0]['resource_requirements']['required_capability_ids']==['image.generate']
    assert compiled['jobs'][0]['network_origins']==['image-provider']


def test_child_tool_whitelist_compiles_into_job_requirements():
    value=compile_delegation({'tasks':[{'id':'icons','objective':'Segment icons',
        'operation_ids':['image.segment'],'result_mode':'artifacts',
        'output_disposition':'candidate'}]},catalog(),identity())
    assert value['jobs'][0]['resource_requirements']['required_operation_ids']==['image.segment']


@pytest.mark.parametrize('result_mode',['artifacts','mixed'])
def test_artifact_delegation_requires_explicit_output_disposition(result_mode):
    with pytest.raises(ValueError,match='output_disposition is required'):
        compile_delegation({'tasks':[{'id':'result','objective':'Create result',
            'result_mode':result_mode}]},catalog(),identity())


def test_text_delegation_may_omit_output_disposition():
    value=compile_delegation({'tasks':[{'id':'analysis','objective':'Analyze',
        'result_mode':'text'}]},catalog(),identity())
    assert value['jobs'][0]['expected_outputs']==[]


def test_text_delegation_rejects_output_disposition():
    with pytest.raises(ValueError,match='output_disposition is invalid for text'):
        compile_delegation({'tasks':[{'id':'analysis','objective':'Analyze',
            'result_mode':'text','output_disposition':'delivery'}]},catalog(),identity())


def test_delegation_example_shows_final_artifact_delivery():
    artifact=next(task for task in DELEGATION_EXAMPLE['tasks']
        if task['result_mode']=='artifacts')
    assert artifact['output_disposition']=='delivery'


def test_primary_inspect_fails_locally_with_artifact_fetch_guidance(monkeypatch,capsys):
    monkeypatch.setenv('HARNESS_CHILD_MODE','0')
    monkeypatch.setattr('sandbox_runtime.job_cli.httpx.get',
        lambda *args,**kwargs:pytest.fail('inspect must not call Gateway from Primary'))
    assert main(['inspect','--artifact-id','a'*32])==2
    output=capsys.readouterr().out
    assert 'sandbox-artifact fetch' in output and 'Job-only' in output
