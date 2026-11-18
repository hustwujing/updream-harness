from sandbox_runtime.job_cli import compile_delegation


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
        'result_mode':'artifacts'}]},catalog(),identity())
    assert value['jobs'][0]['expected_outputs'][0]['min_items']==1


def test_mixed_delegation_keeps_artifacts_optional():
    value=compile_delegation({'tasks':[{'id':'analysis','objective':'Analyze and optionally emit',
        'result_mode':'mixed'}]},catalog(),identity())
    assert value['jobs'][0]['expected_outputs'][0]['min_items']==0


def test_incompatible_skill_fails_before_child_sandbox_creation():
    value=catalog();value['child_execution_profiles'][0]['allowed_skill_ids'].append('codex-ppt')
    value['skills'].append({'id':'codex-ppt','supported_harnesses':['codex'],
        'required_capabilities':['image.generate'],
        'network':{'mode':'required','origins':['image-provider']}})
    import pytest
    with pytest.raises(ValueError,match='outside child-general: codex-ppt'):
        compile_delegation({'tasks':[{'id':'deck','objective':'Build deck',
            'skill_ids':['codex-ppt'],'result_mode':'artifacts'}]},value,identity())
    compiled=compile_delegation({'tasks':[{'id':'deck','objective':'Build deck',
        'skill_ids':['codex-ppt'],'capability_ids':['image.generate'],
        'result_mode':'artifacts'}]},value,identity())
    assert compiled['jobs'][0]['resource_requirements']['required_capability_ids']==['image.generate']
    assert compiled['jobs'][0]['network_origins']==['image-provider']


def test_child_tool_whitelist_compiles_into_job_requirements():
    value=compile_delegation({'tasks':[{'id':'icons','objective':'Segment icons',
        'operation_ids':['image.segment'],'result_mode':'artifacts'}]},catalog(),identity())
    assert value['jobs'][0]['resource_requirements']['required_operation_ids']==['image.segment']
