from sandbox_harness.common.contracts.job_errors import (JOB_ERROR_CONTRACTS, job_error_payload)
from sandbox_harness.services.media_worker.media_errors import (MEDIA_ERROR_GUIDANCE)


def test_all_provider_errors_and_core_executor_errors_have_public_contracts():
    required={
        *MEDIA_ERROR_GUIDANCE,
        'adapter_not_registered','artifact_collection_failed','audio_stream_missing',
        'child_sandbox_lost','controlled_result_unresolved','gpu_runtime_unavailable',
        'infrastructure_capacity_wait','job_timeout_before_submission',
        'job_timeout_outcome_unknown','job_timeout_submission_outcome_unknown',
        'media_probe_failed','no_target_found','operation_outcome_unknown',
        'operation_start_failed','reconciliation_handle_missing','reconciliation_not_supported',
        'submission_outcome_unknown','submission_outcome_unknown_after_worker_loss',
        'worker_internal_error',
    }
    assert required<=set(JOB_ERROR_CONTRACTS)
    assert all(value.message and value.guidance and value.phase and value.side_effect_state
        for value in JOB_ERROR_CONTRACTS.values())


def test_error_payload_is_safe_actionable_and_preserves_structured_overrides():
    value=job_error_payload('audio_stream_missing',detail='decoder stack must remain internal')
    assert value=={
        'error_code':'audio_stream_missing',
        'error_detail':'decoder stack must remain internal',
        'error_message':'输入媒体没有可转写的音轨',
        'error_guidance':'请提供含音轨的视频或独立音频文件后重新创建任务。',
        'failed_phase':'input_validation','retryable':False,'retry_class':'none',
        'side_effect_state':'none',
    }
    overridden=job_error_payload('provider_service_unavailable',phase='status',
        retryable=False,retry_class='manual',side_effect_state='accepted')
    assert overridden['failed_phase']=='status' and overridden['retryable'] is False
    assert overridden['retry_class']=='manual' and overridden['side_effect_state']=='accepted'


def test_unknown_error_code_uses_safe_fallback_without_hiding_identity():
    value=job_error_payload('future_adapter_failure',phase='collection',
        side_effect_state='completed')
    assert value['error_code']=='future_adapter_failure'
    assert value['error_message']=='任务未能完成'
    assert value['failed_phase']=='collection'
    assert value['side_effect_state']=='completed'
    assert '重试' in value['error_guidance'] or '状态' in value['error_guidance']
