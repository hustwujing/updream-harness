from __future__ import annotations

from dataclasses import dataclass


ERROR_PHASES=frozenset({'input_validation','authorization','capacity','submission','status',
    'execution','collection','validation','review','cancellation','reconciliation','cleanup',
    'internal'})
SIDE_EFFECT_STATES=frozenset({'none','possible','accepted','completed','unknown'})


class ArtifactValidationError(ValueError):
    """A deterministic content/contract rejection cannot improve on collection retry."""
    def __init__(self,error_code:str,detail:str=''):
        self.error_code=error_code
        super().__init__(detail or error_code)


class ArtifactCollectionCardinalityError(RuntimeError):
    """A frozen collection count cannot be repaired by reading the same result."""
    def __init__(self,artifact_role:str,minimum:int,maximum:int,actual:int):
        super().__init__('artifact_collection_cardinality_invalid')
        self.result={'output_contract':{'artifact_role':artifact_role,'min_items':minimum,
            'max_items':maximum,'actual_items':actual}}


@dataclass(frozen=True)
class JobErrorContract:
    message:str
    guidance:str
    phase:str
    retryable:bool=False
    retry_class:str='none'
    side_effect_state:str='none'

    def __post_init__(self):
        if self.phase not in ERROR_PHASES:raise ValueError('invalid_job_error_phase')
        if self.side_effect_state not in SIDE_EFFECT_STATES:
            raise ValueError('invalid_job_error_side_effect_state')


def _contract(message,guidance,phase,*,retryable=False,retry_class='none',side_effect='none'):
    return JobErrorContract(message,guidance,phase,retryable,retry_class,side_effect)


JOB_ERROR_CONTRACTS={
    # Provider request and execution failures.
    'provider_input_invalid':_contract('外部服务拒绝了输入参数或素材',
        '请检查素材格式、参数范围和引用关系，修正后创建新任务。','input_validation'),
    'provider_content_policy_rejected':_contract('外部服务的内容策略拒绝了本次请求',
        '不要原样重试；请更换合规内容或改用本地非生成式处理。','authorization'),
    'provider_authentication_failed':_contract('外部服务鉴权失败',
        '需要管理员检查 Provider 凭据，当前任务不应由用户重复提交。','authorization'),
    'provider_permission_denied':_contract('外部服务账号没有所需能力',
        '需要管理员开通能力或选择其他已启用的 Provider。','authorization'),
    'provider_endpoint_unavailable':_contract('外部服务端点已关闭或不可用',
        '需要管理员恢复端点或更换已验证可用的配置；不要修改用户输入或自动重复提交。','submission'),
    'provider_rate_limited':_contract('外部服务正在限流',
        '平台会对原任务退避重试，请勿创建重复任务。','submission',retryable=True,
        retry_class='backoff'),
    'provider_service_unavailable':_contract('外部服务暂时不可用',
        '保留当前任务并等待服务恢复；若任务已被接受，只能继续查询原任务。','status',
        retryable=True,retry_class='backoff',side_effect='possible'),
    'provider_submission_outcome_unknown':_contract('外部服务的提交结果未知',
        '请求可能已经生效，系统必须使用原幂等键或任务句柄对账，严禁盲目重提。','submission',
        retryable=True,retry_class='reconcile_only',side_effect='unknown'),
    'provider_processing_failed':_contract('外部服务已接受任务，但处理失败',
        '请查看稳定错误代码并调整输入或处理方式后创建替代任务。','execution',
        side_effect='accepted'),
    'provider_job_expired':_contract('外部任务在生成结果前已过期',
        '检查任务规模和服务限制后再创建新任务。','execution',side_effect='accepted'),
    'provider_result_unresolved':_contract('外部任务结果尚未确认',
        '保留原任务句柄并继续对账，不要重新提交相同请求。','reconciliation',
        retryable=True,retry_class='reconcile_only',side_effect='unknown'),
    'provider_poll_unavailable':_contract('暂时无法查询外部任务状态',
        '平台会继续查询原任务，请勿重复提交。','status',retryable=True,
        retry_class='backoff',side_effect='accepted'),
    'provider_poll_deferred':_contract('外部任务状态查询已延后',
        '平台会按服务建议继续查询原任务。','status',retryable=True,
        retry_class='backoff',side_effect='accepted'),
    'provider_output_unavailable':_contract('外部任务已完成，但成果暂时无法下载',
        '平台应继续收集原任务成果，无需重新生成。','collection',retryable=True,
        retry_class='collect_same_execution',side_effect='completed'),
    'provider_result_invalid':_contract('外部服务返回的成果未通过校验',
        '该成果不能发布；请保留证据并创建修复或替代任务。','validation',
        side_effect='completed'),
    'provider_cancel_outcome_unknown':_contract('外部任务的取消结果未知',
        '继续查询原任务，不能假定取消成功，也不能直接重复提交。','cancellation',
        retryable=True,retry_class='reconcile_only',side_effect='unknown'),
    'media_reference_unavailable':_contract('参考素材暂时无法提供给外部服务',
        '保留原 ProjectFile，检查素材发布状态后重试原流程。','submission',retryable=True,
        retry_class='same_input'),
    'media_reference_policy_rejected':_contract('外部服务拒绝了参考素材',
        '不要重试相同字节；请更换已授权素材或改用本地处理。','authorization'),
    'voice_clone_input_invalid':_contract('参考音频不符合音色克隆要求',
        '请使用 10–300 秒、20 MB 内的清晰 MP3、M4A 或 WAV。','input_validation'),
    'voice_clone_permission_denied':_contract('当前账号没有音色克隆权限',
        '需要管理员开通权限或改用普通语音。','authorization'),
    'voice_clone_failed':_contract('外部服务未能提取参考音色',
        '请检查音频清晰度和使用授权后更换素材。','execution',side_effect='accepted'),

    # Controlled Operations and sandbox execution.
    'child_allocation_failed':_contract('子任务执行环境分配失败，尚未提交执行',
        '检查资源与配置；平台会保留未确认的分配记录并继续清理。',
        'submission'),
    'child_allocation_timeout':_contract('子任务执行环境分配超时，尚未提交执行',
        '任务未开始执行；平台会继续确认和清理未确定的沙箱资源。',
        'submission'),
    'allocation_recovery_exhausted':_contract('执行环境创建结果无法确认，自动恢复次数已用尽',
        '任务尚未开始执行；平台保留原分配记录并继续清理，需要管理员检查沙箱服务。',
        'reconciliation'),
    'allocation_replacement_capacity_unavailable':_contract('没有额外容量恢复执行环境',
        '任务尚未开始执行；未确认的旧分配仍占用额度，平台继续对账和清理。','capacity'),
    'child_preparation_failed':_contract('子任务执行环境准备失败，尚未开始执行',
        '平台仅可按已冻结的提交前重试策略重试；持续失败时需检查执行环境。',
        'submission',retryable=True,retry_class='bounded_pre_accept'),
    'audio_stream_missing':_contract('输入媒体没有可转写的音轨',
        '请提供含音轨的视频或独立音频文件后重新创建任务。','input_validation'),
    'media_probe_failed':_contract('无法读取输入媒体的流信息',
        '请检查文件是否完整、格式是否受支持，再使用同一素材创建替代任务。','input_validation'),
    'no_target_found':_contract('图像中没有找到指定目标',
        '请使用更宽泛的类别、明确框选或点选目标，不要原样重试相同选择器。','execution'),
    'gpu_runtime_unavailable':_contract('GPU 执行环境暂时不可用',
        '平台应在健康 GPU 节点恢复后重试同一不可变任务。','execution',retryable=True,
        retry_class='node_health_change'),
    'operation_start_failed':_contract('受控任务未能启动',
        '平台只可按 Descriptor 的确定性重试策略重试；重复失败后需要管理员检查运行环境。',
        'submission',retryable=True,retry_class='bounded_pre_accept'),
    'operation_outcome_unknown':_contract('受控任务的执行结果无法确认',
        '使用原执行句柄对账；确认无副作用前不要创建重复任务。','reconciliation',
        retryable=True,retry_class='reconcile_only',side_effect='unknown'),
    'controlled_result_unresolved':_contract('受控任务成果无法恢复',
        '检查原执行目录和结果清单，不能把未确认成果发布给用户。','reconciliation',
        side_effect='unknown'),

    # Durable worker, Child Harness, timeout and collection boundaries.
    'infrastructure_capacity_wait':_contract('当前没有满足要求的执行节点容量',
        '平台会等待兼容节点，无需用户重复提交。','capacity',retryable=True,
        retry_class='capacity_wait'),
    'pre_accept_failure':_contract('任务在外部提交前失败',
        '平台会按 Descriptor 的有界重试策略处理，不会重复已接受的外部任务。','submission',
        retryable=True,retry_class='bounded_pre_accept'),
    'adapter_not_registered':_contract('当前节点没有注册所需执行器',
        '需要管理员部署或启用对应 Adapter。','capacity'),
    'reconciliation_handle_missing':_contract('缺少可用于对账的外部任务句柄',
        '不能自动重提；需要根据审计记录确认外部副作用。','reconciliation',
        side_effect='unknown'),
    'reconciliation_not_supported':_contract('该外部能力不支持自动对账',
        '需要人工核对外部系统状态，再决定是否创建替代任务。','reconciliation',
        side_effect='unknown'),
    'submission_outcome_unknown':_contract('任务提交结果未知',
        '系统不会自动重复提交；请等待对账或人工核实。','submission',retryable=True,
        retry_class='reconcile_only',side_effect='unknown'),
    'submission_outcome_unknown_after_worker_loss':_contract('Worker 中断后无法确认提交结果',
        '检查原外部执行记录并对账，严禁直接重提。','reconciliation',
        side_effect='unknown'),
    'artifact_mime_not_allowed':_contract('成果格式不符合已确认的产物契约',
        '请生成契约允许的格式，或明确修正输出契约后创建替代任务。','validation',
        side_effect='completed'),
    'artifact_mime_mismatch':_contract('成果声明的格式与实际内容不一致',
        '请修正成果内容或格式声明后创建替代任务。','validation',side_effect='completed'),
    'artifact_item_too_large':_contract('成果文件超过允许的大小',
        '请减小或拆分成果文件，再创建符合大小限制的替代任务。','validation',
        side_effect='completed'),
    'artifact_collection_too_large':_contract('成果集合超过允许的总大小',
        '请减小成果集合，或明确修正总大小限制后创建替代任务。','validation',
        side_effect='completed'),
    'artifact_collection_failed':_contract('任务已执行，但成果收集失败',
        '保留原执行句柄和成果位置，从收集阶段恢复，不要重复执行外部任务。','collection',
        retryable=True,retry_class='collect_same_execution',side_effect='completed'),
    'artifact_collection_cardinality_invalid':_contract('成果文件数量不符合已确认的产物契约',
        'min_items/max_items 按每个集合内的文件数计数；检查实际文件数量与契约，'
        '明确修正输入或输出契约后创建替代任务。不要原样重试收集。','validation',
        side_effect='completed'),
    'external_status_unavailable':_contract('暂时无法确认任务状态',
        '保留原执行句柄并继续查询；不要创建重复任务。','status',retryable=True,
        retry_class='reconcile_only',side_effect='unknown'),
    'child_sandbox_lost':_contract('子任务沙箱已丢失',
        '优先从检查点恢复；没有检查点时由父任务决定是否创建替代任务。','execution',
        retryable=True,retry_class='checkpoint_or_replace',side_effect='possible'),
    'job_timeout_before_submission':_contract('任务在提交前已超时',
        '外部副作用尚未发生，可在检查预算和时限后安全创建替代任务。','submission'),
    'job_timeout_submission_outcome_unknown':_contract('任务提交过程中超时，结果未知',
        '必须先对账原请求，不能直接重提。','submission',retryable=True,
        retry_class='reconcile_only',side_effect='unknown'),
    'job_timeout_outcome_unknown':_contract('已接受的任务超时，最终结果未知',
        '继续查询或取消原任务；不能假定失败后重新提交。','status',retryable=True,
        retry_class='reconcile_only',side_effect='unknown'),
    'worker_internal_error':_contract('执行器发生内部错误',
        '任务已停止，管理员应根据查询编号检查内部日志后再决定是否重试。','internal',
        side_effect='possible'),
}


FALLBACK=JobErrorContract('任务未能完成','请保留当前任务状态和错误代码，确认是否可重试后再操作。',
    'execution',False,'none','possible')


def job_error_payload(error_code:str,*,detail:str='',phase:str|None=None,
                      retryable:bool|None=None,retry_class:str|None=None,
                      side_effect_state:str|None=None,extra:dict|None=None)->dict:
    contract=JOB_ERROR_CONTRACTS.get(error_code,FALLBACK)
    selected_phase=phase or contract.phase
    selected_side_effect=side_effect_state or contract.side_effect_state
    if selected_phase not in ERROR_PHASES:raise ValueError('invalid_job_error_phase')
    if selected_side_effect not in SIDE_EFFECT_STATES:
        raise ValueError('invalid_job_error_side_effect_state')
    value={'error_code':error_code,'error_message':contract.message,
        'error_guidance':contract.guidance,'failed_phase':selected_phase,
        'retryable':contract.retryable if retryable is None else bool(retryable),
        'retry_class':retry_class or contract.retry_class,
        'side_effect_state':selected_side_effect}
    if detail:value['error_detail']=detail
    if extra:value.update(extra)
    return value


def normalize_job_error(result:dict|None,*,default_code:str='worker_internal_error',
                        phase:str|None=None,retryable:bool|None=None,
                        retry_class:str|None=None,side_effect_state:str|None=None)->dict:
    source=dict(result or {});code=str(source.get('error_code') or default_code)
    detail=str(source.get('error_detail') or '')
    value=job_error_payload(code,detail=detail,phase=str(source.get('failed_phase') or phase or '') or None,
        retryable=(source.get('retryable') if 'retryable' in source else retryable),
        retry_class=str(source.get('retry_class') or retry_class or '') or None,
        side_effect_state=str(source.get('side_effect_state') or side_effect_state or '') or None)
    return {**source,**value}
