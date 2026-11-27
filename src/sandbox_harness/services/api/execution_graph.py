from __future__ import annotations
from sandbox_harness.common.contracts.domain import now

import hashlib
import json
import re
from datetime import datetime, timezone

from sqlalchemy import case,func,or_,select

from sandbox_harness.common.contracts.domain import (TERMINAL, TurnState)
from sandbox_harness.common.contracts.job_errors import (JOB_ERROR_CONTRACTS)
from sandbox_harness.common.contracts.public_errors import (public_error_reason)
from sandbox_harness.common.db.models import (InteractionRow, JobDisplayActivityRow, JobArtifactRow,
    JobAttemptRow, JobGroupDraftRow, ItemRow, JobGroupRow, JobOutboxRow, JobRow, MediaBatchRow,
    MediaDraftRow, MediaJobRow, ResourceReservationRow, ReviewGateRow, ThreadRow, TurnOrderRow,
    TurnRow, utc_datetime)


RUNNING_JOB_STATES={'CREATED','QUEUED','RUNNING','WAITING_EXTERNAL','CANCELLING','PAUSING'}
TERMINAL_JOB_STATES={'SUCCEEDED','FAILED','CANCELLED','BLOCKED','UNKNOWN'}
MAX_TURNS=50;MAX_DRAFTS=100;MAX_GROUPS=200;MAX_JOBS=300;MAX_INTERACTIONS=100
MAX_ARTIFACT_SUMMARIES=200;MAX_NODES=900;MAX_EDGES=1400
MAX_MEDIA_BATCHES=100;MAX_MEDIA_DRAFTS=300;MAX_MEDIA_JOBS=300
MAX_MODEL_USAGE_ROWS=5000
TOKEN_USAGE_FIELDS=('ordinary_input_tokens','cache_read_input_tokens',
    'cache_write_input_tokens','output_tokens')


def iso(value):return utc_datetime(value).isoformat() if value else None


def duration_ms(start,end):
    if not start:return 0
    finish=utc_datetime(end) if end else datetime.now(timezone.utc)
    return max(0,int((finish-utc_datetime(start)).total_seconds()*1000))


def public_error_summary(code:str)->str:
    contract=JOB_ERROR_CONTRACTS.get(code)
    if contract:return contract.message
    if code in {'model_rate_limited','rate_limited','provider_rate_limited'}:
        return '模型服务限流，请稍后重试'
    if code in {'model_not_supported','model_route_missing','model_not_found'}:
        return '所选模型当前不可用，请选择可用模型'
    if code in {'provider_timeout','job_timeout','task_timeout'}:
        return '外部服务超时，可安全重试或继续对账'
    if code:return '任务执行失败，请根据错误代码重试'
    return ''


def media_draft_state(value:str)->str:
    return {'awaiting_confirmation':'AWAITING_CONFIRMATION','confirmed':'CONFIRMED',
        'rejected':'REJECTED','expired':'EXPIRED','cancelled':'CANCELLED'}.get(value,value.upper())


def media_job_state(value:str)->str:
    return {'created':'CREATED','queued':'QUEUED','running':'RUNNING','waiting_external':'WAITING_EXTERNAL',
        'cancelling':'CANCELLING','succeeded':'SUCCEEDED','failed':'FAILED','cancelled':'CANCELLED',
        'unknown':'UNKNOWN'}.get(value,value.upper())


def group_progress(groups,state_rows):
    values={row.id:{'terminal':0,'total':row.member_count,'succeeded':0,
        'failed':0,'cancelled':0,'blocked':0,'unknown':0,'paused':0} for row in groups}
    keys={'SUCCEEDED':'succeeded','FAILED':'failed','CANCELLED':'cancelled',
        'BLOCKED':'blocked','UNKNOWN':'unknown'}
    for group_id,state,count in state_rows:
        progress=values.get(group_id)
        if not progress:continue
        if state in TERMINAL_JOB_STATES:progress['terminal']+=int(count)
        if state in keys:progress[keys[state]]+=int(count)
        if state=='PAUSED':progress['paused']+=int(count)
    return values


def remaining_jobs(rows):
    return [{'job_id':row.id,'local_job_id':row.local_job_id,'descriptor_id':row.descriptor_id,
        'state':row.state,'started_at':iso(row.created_at)} for row in rows
        if row.state in RUNNING_JOB_STATES]


def aggregate_model_activity(rows):
    latest={}
    for row in rows:
        try:payload=json.loads(row.payload_json or '{}')
        except (TypeError,ValueError):continue
        request_id=str(payload.get('model_request_id') or '')
        if not request_id:continue
        existing=latest.get(request_id)
        started_at=(min(utc_datetime(existing[2]),utc_datetime(row.created_at))
            if existing else utc_datetime(row.created_at))
        if (not existing or (utc_datetime(row.updated_at),row.sequence)>
                (utc_datetime(existing[0].updated_at),existing[0].sequence)):
            latest[request_id]=(row,payload,started_at)
        else:latest[request_id]=(existing[0],existing[1],started_at)
    values={}
    selected=sorted(latest.items(),key=lambda item:(
        item[1][2],item[1][0].sequence))
    for request_id,(row,payload,started_at) in selected:
        key=(row.turn_id,str(payload.get('job_id') or ''))
        value=values.setdefault(key,{'token_usage':{field:0 for field in TOKEN_USAGE_FIELDS},
            'model_rounds':[]})
        counts={}
        for field in TOKEN_USAGE_FIELDS:
            count=payload.get(field)
            counts[field]=(count if isinstance(count,int) and not isinstance(count,bool) and count>=0 else 0)
            value['token_usage'][field]+=counts[field]
        status=row.status if row.status in {'in_progress','completed','failed','interrupted'} else 'in_progress'
        phase=str(payload.get('phase') or '')
        if phase not in {'waiting','streaming','completed','failed','interrupted'}:
            phase='completed' if status=='completed' else 'failed' if status in {'failed','interrupted'} else 'waiting'
        terminal=status in {'completed','failed','interrupted'}
        elapsed=payload.get('elapsed_ms')
        if not isinstance(elapsed,(int,float)) or isinstance(elapsed,bool) or elapsed<0:
            elapsed=duration_ms(row.created_at,row.updated_at if terminal else None)
        first_output=payload.get('first_output_ms')
        if not isinstance(first_output,(int,float)) or isinstance(first_output,bool) or first_output<0:
            first_output=None
        error_code=str(payload.get('error_code') or '')
        if not re.fullmatch(r'[a-z][a-z0-9_.-]{0,159}',error_code):error_code=''
        value['model_rounds'].append({'index':len(value['model_rounds'])+1,
            'request_id':request_id,'model':str(payload.get('model') or ''),'phase':phase,
            'status':status,'started_at':iso(started_at),'ended_at':iso(row.updated_at) if terminal else None,
            'duration_ms':int(elapsed),'first_output_ms':int(first_output) if first_output is not None else None,
            **counts,'error_code':error_code})
    return values


def wait_barrier(group,progress,remaining=()):
    complete=progress['terminal']>=progress['total']
    paused=not complete and progress['paused']>=progress['total']-progress['terminal']
    return {'id':'wait_barrier:'+group.id,'type':'wait_barrier','role':'子任务等待','visual_role':'wait_barrier',
        'title':('全部子任务已返回' if complete else '子任务已暂停' if paused else '正在等待全部子任务返回')+
            f" · {progress['terminal']}/{progress['total']}",
        'state':'COMPLETED' if complete else 'PAUSED' if paused else 'RUNNING','location':'control_plane',
        'location_label':'协调器','started_at':iso(group.created_at),
        'ended_at':iso(group.terminal_at) if complete else None,
        'duration_ms':duration_ms(group.created_at,group.terminal_at) if complete else 0,
        'retries':0,'activity_count':0,'turn_id':group.origin_turn_id,
        'group_id':group.id,'progress':progress,'remaining_jobs':list(remaining)}


class ExecutionGraphProjector:
    def __init__(self,store):self.store=store

    async def waits(self,thread_id:str,owner_id:str)->list[dict]:
        async with self.store.core.connections() as db:
            exists=(await db.execute(select(ThreadRow.id).where(ThreadRow.id==thread_id,
                ThreadRow.owner_id==owner_id))).scalar_one_or_none()
            if not exists:raise KeyError('thread_not_found')
            groups=list(reversed(list((await db.execute(select(JobGroupRow).where(
                JobGroupRow.thread_id==thread_id).order_by(JobGroupRow.created_at.desc())
                .limit(MAX_GROUPS))).scalars().all())))
            identities={row.id for row in groups}
            state_rows=(await db.execute(select(JobRow.group_id,JobRow.state,func.count()).where(
                JobRow.group_id.in_(identities or {''})).group_by(JobRow.group_id,JobRow.state))).all()
            active=list((await db.execute(select(JobRow).where(JobRow.group_id.in_(identities or {''}),
                JobRow.state.in_(RUNNING_JOB_STATES)).order_by(JobRow.created_at))).scalars().all())
        progress=group_progress(groups,state_rows)
        by_group={identity:[] for identity in identities}
        for row in active:by_group[row.group_id].append(row)
        return [wait_barrier(group,progress[group.id],remaining_jobs(by_group[group.id])) for group in groups]

    async def snapshot(self,thread_id:str,owner_id:str,*,cursor:str='',include_history:bool=False,
                       history_before:str='')->dict:
        async with self.store.core.connections() as db:
            exists=(await db.execute(select(ThreadRow.id).where(ThreadRow.id==thread_id,
                ThreadRow.owner_id==owner_id))).scalar_one_or_none()
            projection_job_ids=set((await db.execute(select(JobRow.id).where(
                JobRow.thread_id==thread_id).order_by(JobRow.created_at.desc()).limit(
                MAX_JOBS))).scalars().all()) if exists else set()
        if not exists:raise KeyError('thread_not_found')
        await self.store.ensure_job_display_activities(projection_job_ids)
        async with self.store.core.connections() as db:
            thread=(await db.execute(select(ThreadRow).where(ThreadRow.id==thread_id,
                ThreadRow.owner_id==owner_id))).scalar_one_or_none()
            if not thread:raise KeyError('thread_not_found')
            total_turns=int((await db.execute(select(func.count()).select_from(TurnRow).where(
                TurnRow.thread_id==thread_id))).scalar_one())
            turn_query=select(TurnRow).join(TurnOrderRow,TurnOrderRow.turn_id==TurnRow.id).where(
                TurnRow.thread_id==thread_id)
            if include_history and history_before:
                boundary=select(TurnOrderRow.sequence).where(
                    TurnOrderRow.turn_id==history_before).scalar_subquery()
                turn_query=turn_query.where(TurnOrderRow.sequence<boundary)
            recent=list((await db.execute(turn_query.order_by(TurnOrderRow.sequence.desc())
                .limit(MAX_TURNS if include_history else 5))).scalars().all())
            has_older=False
            if include_history and recent:
                oldest_sequence=select(TurnOrderRow.sequence).where(
                    TurnOrderRow.turn_id==recent[-1].id).scalar_subquery()
                has_older=bool((await db.execute(select(func.count()).select_from(TurnOrderRow).where(
                    TurnOrderRow.thread_id==thread_id,TurnOrderRow.sequence<oldest_sequence))).scalar_one())
            active=list((await db.execute(select(TurnRow).where(TurnRow.thread_id==thread_id,
                TurnRow.state.not_in([state.value for state in TERMINAL])).limit(MAX_TURNS)))
                .scalars().all())
            turns=sorted({row.id:row for row in recent+active}.values(),key=lambda row:row.created_at)
            turn_ids={row.id for row in turns}
            media_origin_ids={row.idempotency_key.removeprefix('media-continuation-') for row in turns
                if row.idempotency_key.startswith('media-continuation-')}
            continued_outboxes=list((await db.execute(select(JobOutboxRow).where(
                JobOutboxRow.thread_id==thread_id,
                JobOutboxRow.continuation_turn_id.in_(turn_ids or {''}))
                .order_by(JobOutboxRow.created_at.desc()).limit(MAX_GROUPS))).scalars().all())
            continued_group_ids={row.group_id for row in continued_outboxes}
            all_drafts=list((await db.execute(select(JobGroupDraftRow).where(
                JobGroupDraftRow.thread_id==thread_id,or_(
                    JobGroupDraftRow.origin_turn_id.in_(turn_ids or {''}),
                    JobGroupDraftRow.state.in_(['DRAFT','AWAITING_CONFIRMATION'])))
                .order_by(JobGroupDraftRow.created_at.desc()).limit(MAX_DRAFTS))).scalars().all())
            drafts=list(reversed(all_drafts))
            groups=list(reversed(list((await db.execute(select(JobGroupRow).where(
                JobGroupRow.thread_id==thread_id,or_(JobGroupRow.origin_turn_id.in_(turn_ids or {''}),
                    JobGroupRow.state.in_(['COMMITTED','RUNNING']),
                    JobGroupRow.id.in_(continued_group_ids or {''})))
                .order_by(JobGroupRow.created_at.desc())
                .limit(MAX_GROUPS))).scalars().all())))
            draft_by_id={row.id:row for row in all_drafts}
            referenced={row.draft_id for row in groups if row.draft_id}-set(draft_by_id)
            if referenced:
                for row in (await db.execute(select(JobGroupDraftRow).where(
                        JobGroupDraftRow.id.in_(referenced)))).scalars().all():
                    draft_by_id[row.id]=row
            for group in groups:
                if group.draft_id and group.draft_id in draft_by_id:
                    drafts.append(draft_by_id[group.draft_id])
            drafts=list({row.id:row for row in drafts}.values())
            ancestor_turn_ids={row.origin_turn_id for row in groups+drafts}-turn_ids
            if ancestor_turn_ids:
                ancestors=list((await db.execute(select(TurnRow).where(
                    TurnRow.id.in_(ancestor_turn_ids)))).scalars().all())
                turns=sorted({row.id:row for row in turns+ancestors}.values(),key=lambda row:row.created_at)
                turn_ids={row.id for row in turns}
            group_ids={row.id for row in groups}
            outboxes=list((await db.execute(select(JobOutboxRow).where(
                JobOutboxRow.group_id.in_(group_ids or {''})))).scalars().all())
            jobs=list(reversed(list((await db.execute(select(JobRow).where(
                JobRow.group_id.in_(group_ids or {''})).order_by(JobRow.created_at.desc(),
                    JobRow.local_job_id).limit(MAX_JOBS))).scalars().all())))
            job_state_rows=(await db.execute(select(JobRow.group_id,JobRow.state,func.count()).where(
                JobRow.group_id.in_(group_ids or {''})).group_by(JobRow.group_id,JobRow.state))).all()
            job_ids={row.id for row in jobs}
            model_usage_rows=list((await db.execute(select(ItemRow).where(
                ItemRow.turn_id.in_(turn_ids or {''}),ItemRow.type=='plan',
                ItemRow.payload_json.like('%"model_request_id"%')).order_by(
                ItemRow.created_at.desc()).limit(MAX_MODEL_USAGE_ROWS+1))).scalars().all())
            interactions=list((await db.execute(select(InteractionRow).where(
                InteractionRow.thread_id==thread_id,or_(
                    InteractionRow.turn_id.in_(turn_ids or {''}),
                    InteractionRow.status.in_(['pending','responding'])))
                .order_by(InteractionRow.created_at.desc())
                .limit(MAX_INTERACTIONS))).scalars().all())
            interactions=list(reversed(interactions))
            reservations=list((await db.execute(select(ResourceReservationRow).where(
                ResourceReservationRow.job_id.in_(job_ids or {''})))).scalars().all())
            activity_rows=(await db.execute(select(JobDisplayActivityRow.job_id,func.count()).where(
                JobDisplayActivityRow.job_id.in_(job_ids or {''})).group_by(
                JobDisplayActivityRow.job_id))).all()
            retry_rows=(await db.execute(select(JobAttemptRow.job_id,func.sum(case(
                (JobAttemptRow.outcome=='PRE_ACCEPT_RETRY',1),
                (JobAttemptRow.retry_kind.in_(['recovery','reconcile','collection']),1),else_=0))).where(
                    JobAttemptRow.job_id.in_(job_ids or {''})).group_by(JobAttemptRow.job_id))).all()
            attempt_rows=(await db.execute(select(JobAttemptRow.job_id,func.count(),
                func.count(func.distinct(func.nullif(JobAttemptRow.external_execution_id,'')))).where(
                    JobAttemptRow.job_id.in_(job_ids or {''})).group_by(JobAttemptRow.job_id))).all()
            artifacts=list((await db.execute(select(JobArtifactRow).where(
                JobArtifactRow.job_id.in_(job_ids or {''})).order_by(
                JobArtifactRow.job_id,JobArtifactRow.artifact_role,JobArtifactRow.position)
                .limit(MAX_ARTIFACT_SUMMARIES))).scalars().all())
            artifact_counts=dict((await db.execute(select(JobArtifactRow.job_id,func.count()).where(
                JobArtifactRow.job_id.in_(job_ids or {''})).group_by(JobArtifactRow.job_id))).all())
            gates=list((await db.execute(select(ReviewGateRow).where(
                ReviewGateRow.reviewer_job_id.in_(job_ids or {''})))).scalars().all())
        async with self.store.media.connections() as db:
            media_batches=list(reversed(list((await db.execute(select(MediaBatchRow).where(
                MediaBatchRow.thread_id==thread_id,or_(
                    MediaBatchRow.turn_id.in_(turn_ids|media_origin_ids or {''}),
                    MediaBatchRow.status.in_(['open','sealed','delivering','blocked'])))
                .order_by(MediaBatchRow.created_at.desc()).limit(MAX_MEDIA_BATCHES))).scalars().all())))
            batch_turn_ids={row.turn_id for row in media_batches}
            media_drafts=list(reversed(list((await db.execute(select(MediaDraftRow).where(
                MediaDraftRow.thread_id==thread_id,or_(
                    MediaDraftRow.turn_id.in_(batch_turn_ids or {''}),
                    MediaDraftRow.status=='awaiting_confirmation')).order_by(
                        MediaDraftRow.created_at.desc()).limit(MAX_MEDIA_DRAFTS))).scalars().all())))
            media_draft_ids={row.id for row in media_drafts}
            media_jobs=list(reversed(list((await db.execute(select(MediaJobRow).where(
                MediaJobRow.thread_id==thread_id,
                MediaJobRow.draft_id.in_(media_draft_ids or {''})).order_by(
                    MediaJobRow.created_at.desc()).limit(MAX_MEDIA_JOBS))).scalars().all())))
        media_ancestor_ids={row.turn_id for row in media_batches if row.turn_id}-turn_ids
        if media_ancestor_ids:
            async with self.store.core.connections() as db:
                ancestors=list((await db.scalars(select(TurnRow).where(
                    TurnRow.id.in_(media_ancestor_ids)))).all())
            turns=sorted({row.id:row for row in turns+ancestors}.values(),key=lambda row:row.created_at)
            turn_ids={row.id for row in turns}
        reservation_by_job={row.job_id:row for row in reservations}
        usage_truncated=len(model_usage_rows)>MAX_MODEL_USAGE_ROWS
        model_activity=aggregate_model_activity(model_usage_rows[:MAX_MODEL_USAGE_ROWS])
        activity_count={identity:int(count) for identity,count in activity_rows}
        retry_count={identity:int(count or 0) for identity,count in retry_rows}
        attempt_counts={identity:(int(count or 0),int(external or 0))
            for identity,count,external in attempt_rows}
        artifacts_by_job={identity:[] for identity in job_ids}
        for row in artifacts:artifacts_by_job[row.job_id].append({'artifact_id':row.artifact_id,
            'artifact_role':row.artifact_role,'filename':row.filename,'mime':row.mime,
            'size':row.size,'sha256':row.sha256,'output_disposition':row.output_disposition})
        remaining_by_group={identity:[] for identity in group_ids}
        for row in jobs:
            if row.state in RUNNING_JOB_STATES:remaining_by_group[row.group_id].append(row)
        nodes=[];edges=[]
        outbox_by_continuation={row.continuation_turn_id:row for row in outboxes
            if row.continuation_turn_id}
        media_continuation_by_origin={row.idempotency_key.removeprefix('media-continuation-'):row.id
            for row in turns if row.idempotency_key.startswith('media-continuation-')}
        progress_by_group=group_progress(groups,job_state_rows)
        for turn in turns:
            user_id=f'turn:{turn.id}:input';primary_id=f'turn:{turn.id}:primary'
            activity=model_activity.get((turn.id,''),{
                'token_usage':{field:0 for field in TOKEN_USAGE_FIELDS},'model_rounds':[]})
            continuation_title=('并行任务结果已返回' if turn.id in outbox_by_continuation else
                '媒体任务结果已返回' if turn.internal and turn.idempotency_key.startswith('media-continuation-')
                else '后台结果已返回')
            nodes.append({'id':user_id,'type':'continuation' if turn.internal else 'user_turn',
                'role':'任务续接' if turn.internal else '用户输入','title':continuation_title if turn.internal else turn.public_message[:120],
                'visual_role':'continuation' if turn.internal else 'user',
                'state':'COMPLETED','location':'control_plane','location_label':'控制面',
                'started_at':iso(turn.created_at),'ended_at':iso(turn.created_at),'duration_ms':0,
                'retries':0,'activity_count':0,'turn_id':turn.id})
            nodes.append({'id':primary_id,'type':'primary_harness','role':'主任务执行员',
                'title':f'{turn.harness} · {turn.model}','state':turn.state,'visual_role':'primary_harness',
                'harness_view':True,'objective':turn.public_message or turn.message,
                'harness':turn.harness,'model':turn.model,
                'token_usage':activity['token_usage'],'model_rounds':activity['model_rounds'],
                'model_rounds_truncated':usage_truncated,
                'token_usage_truncated':usage_truncated,
                'location':'sandbox' if turn.sandbox_id else 'scheduler',
                'location_label':'Cloud Sandbox' if turn.sandbox_id else '等待调度',
                'started_at':iso(turn.created_at),'ended_at':iso(turn.updated_at) if TurnState(turn.state) in TERMINAL else None,
                'duration_ms':duration_ms(turn.created_at,turn.updated_at) if TurnState(turn.state) in TERMINAL else 0,
                'retries':0,'recovery_count':max(0,turn.recovery_generation-1),
                'model_request_count':len(activity['model_rounds']),
                'activity_count':0,'turn_id':turn.id})
            edges.append({'from':user_id,'to':primary_id,'kind':'submitted_by'})
        for draft in drafts:
            identity='draft:'+draft.id
            draft_state=('EXPIRED' if draft.state in {'DRAFT','AWAITING_CONFIRMATION'}
                and utc_datetime(draft.expires_at)<=now() else draft.state)
            nodes.append({'id':identity,'type':'job_group_draft','role':'任务确认',
                'title':'并行任务等待确认','state':draft_state,'visual_role':'job_confirmation',
                'location':'control_plane','location_label':'控制面',
                'started_at':iso(draft.created_at),'ended_at':iso(draft.updated_at) if draft_state in {'REJECTED','EXPIRED','CONFIRMED'} else None,
                'duration_ms':duration_ms(draft.created_at,draft.updated_at) if draft_state in {'REJECTED','EXPIRED','CONFIRMED'} else 0,
                'retries':0,'activity_count':0,'turn_id':draft.origin_turn_id})
            if draft.origin_turn_id in turn_ids:edges.append({'from':f'turn:{draft.origin_turn_id}:primary','to':identity,'kind':'submitted_by'})
        for group in groups:
            identity='group:'+group.id
            progress=progress_by_group[group.id]
            nodes.append({'id':identity,'type':'job_group','role':'并行任务组','title':f'{group.member_count} 个并行任务',
                'state':group.state,'visual_role':'job_group','location':'control_plane','location_label':'协调器',
                'started_at':iso(group.created_at),'ended_at':iso(group.terminal_at),
                'duration_ms':duration_ms(group.created_at,group.terminal_at) if group.terminal_at else 0,'retries':0,
                'activity_count':0,'turn_id':group.origin_turn_id,'progress':progress})
            parent=('draft:'+group.draft_id if group.draft_id else f'turn:{group.origin_turn_id}:primary')
            edges.append({'from':parent,'to':identity,'kind':'contains' if group.draft_id else 'submitted_by'})
            barrier_id='wait_barrier:'+group.id
            nodes.append(wait_barrier(group,progress,remaining_jobs(remaining_by_group[group.id])))
            edges.append({'from':identity,'to':barrier_id,'kind':'waits_on'})
        for outbox in outboxes:
            if outbox.continuation_turn_id in turn_ids and outbox.group_id in group_ids:
                edges.append({'from':'wait_barrier:'+outbox.group_id,
                    'to':f'turn:{outbox.continuation_turn_id}:input','kind':'continued_by'})
        for job in jobs:
            reservation=reservation_by_job.get(job.id);resources=json.loads(reservation.gpu_ids_json) if reservation else []
            request=json.loads(job.request_json);requirements=request.get('resource_requirements') or {}
            parameters=request.get('parameters') or {}
            runtime_profile=request.get('runtime_profile_id') or ''
            role=('质量审查员' if job.descriptor_id=='review.submit' else '子 Harness'
                if job.kind=='harness' else '外部能力任务' if job.kind=='provider' else '任务执行员')
            visual_role=('reviewer' if job.descriptor_id=='review.submit' else 'child_harness'
                if job.kind=='harness' else 'external_capability' if job.kind=='provider' else 'worker')
            location=('sandbox' if job.kind=='harness' else 'external_provider' if job.kind=='provider'
                else 'gpu_worker' if resources else 'host_worker')
            labels={'sandbox':'Cloud Sandbox','external_provider':'外部服务','gpu_worker':'GPU Worker','host_worker':'宿主 Worker'}
            location_label=labels[location]+((' · '+runtime_profile) if location=='sandbox' and runtime_profile else '')
            try:result=json.loads(job.result_json or '{}')
            except (TypeError,ValueError):result={}
            error_code=(str(result.get('error_code') or '')
                if job.state in {'FAILED','BLOCKED','UNKNOWN'} else '')
            contract=JOB_ERROR_CONTRACTS.get(error_code)
            activity=model_activity.get((job.origin_turn_id,job.id),{
                'token_usage':{field:0 for field in TOKEN_USAGE_FIELDS},'model_rounds':[]})
            nodes.append({'id':'job:'+job.id,'type':'job','role':role,
                'title':f'{job.local_job_id} · {job.descriptor_id}','state':job.state,
                'visual_role':visual_role,
                'harness_view':job.kind=='harness',
                'objective':str(parameters.get('objective') or job.local_job_id),
                'harness':str(requirements.get('harness') or parameters.get('harness') or ''),
                'model':str(parameters.get('model') or ''),
                'token_usage':activity['token_usage'] if job.kind=='harness' else None,
                'model_rounds':activity['model_rounds'] if job.kind=='harness' else [],
                'model_rounds_truncated':usage_truncated if job.kind=='harness' else False,
                'token_usage_truncated':usage_truncated if job.kind=='harness' else False,
                'location':location,'location_label':location_label,
                'started_at':iso(job.created_at),'ended_at':iso(job.terminal_at),
                'duration_ms':duration_ms(job.created_at,job.terminal_at) if job.terminal_at else 0,
                'retries':0,'retry_count':retry_count.get(job.id,0),
                'attempt_count':attempt_counts.get(job.id,(0,0))[0],
                'external_execution_count':attempt_counts.get(job.id,(0,0))[1],
                'activity_count':activity_count.get(job.id,0),'job_id':job.id,'turn_id':job.origin_turn_id,
                'error_code':error_code,
                'error_summary':str(result.get('error_message') or public_error_summary(error_code)),
                'error_reason':public_error_reason(result.get('error_detail')),
                'error_guidance':str(result.get('error_guidance') or (
                    contract.guidance if contract else '')),
                'failed_phase':str(result.get('failed_phase') or ''),
                'retryable':bool(result.get('retryable',False)),
                'retry_class':str(result.get('retry_class') or ''),
                'side_effect_state':str(result.get('side_effect_state') or ''),
                'artifacts':artifacts_by_job.get(job.id,[]),
                'artifact_count':artifact_counts.get(job.id,0),
                'artifacts_truncated':artifact_counts.get(job.id,0)>len(artifacts_by_job.get(job.id,[])),
                'resources':{'cpu':reservation.cpu if reservation else None,
                    'memory_mb':reservation.memory_mb if reservation else None,'gpu_ids':resources,
                    'runtime_profile_id':runtime_profile,'child_profile_id':request.get('child_profile_id') or '',
                    'harness':requirements.get('harness') or parameters.get('harness') or '',
                    'model':parameters.get('model') or '',
                    'skill_ids':request.get('skill_ids') or [],
                    'capability_ids':requirements.get('required_capability_ids') or [],
                    'network_origins':request.get('network_origins') or []}})
            edges.append({'from':'group:'+job.group_id,'to':'job:'+job.id,'kind':'contains'})
            edges.append({'from':'job:'+job.id,'to':'wait_barrier:'+job.group_id,'kind':'reported_to'})
        media_jobs_by_draft={row.draft_id:row for row in media_jobs}
        for batch in media_batches:
            batch_id='media-batch:'+batch.turn_id
            batch_state={'open':'RUNNING','sealed':'WAITING_EXTERNAL','delivering':'WAITING_EXTERNAL',
                'continued':'CONTINUED','blocked':'BLOCKED'}.get(batch.status,batch.status.upper())
            nodes.append({'id':batch_id,'type':'media_batch','role':'媒体任务组','title':'媒体生成批次',
                'state':batch_state,'visual_role':'media_group','location':'control_plane','location_label':'协调器',
                'started_at':iso(batch.created_at),'ended_at':iso(batch.continued_at),
                'duration_ms':duration_ms(batch.created_at,batch.continued_at) if batch.continued_at else 0,
                'retries':0,'delivery_attempt_count':max(0,batch.attempt_count),
                'activity_count':0,'turn_id':batch.turn_id})
            edges.append({'from':f'turn:{batch.turn_id}:primary','to':batch_id,'kind':'submitted_by'})
            continuation_id=media_continuation_by_origin.get(batch.turn_id)
            if continuation_id:
                edges.append({'from':batch_id,'to':f'turn:{continuation_id}:input','kind':'continued_by'})
        for draft in media_drafts:
            draft_id='media-draft:'+draft.id;state=media_draft_state(draft.status)
            expired=state=='AWAITING_CONFIRMATION' and utc_datetime(draft.expires_at)<=now()
            state='EXPIRED' if expired else state
            nodes.append({'id':draft_id,'type':'media_draft','role':'媒体任务确认',
                'title':f'{draft.kind} · {draft.model}','state':state,'visual_role':'media_confirmation',
                'location':'control_plane','location_label':'控制面','started_at':iso(draft.created_at),
                'ended_at':iso(draft.updated_at) if state in {'CONFIRMED','REJECTED','EXPIRED','CANCELLED'} else None,
                'duration_ms':duration_ms(draft.created_at,draft.updated_at)
                    if state in {'CONFIRMED','REJECTED','EXPIRED','CANCELLED'} else 0,
                'retries':0,'activity_count':0,'turn_id':draft.turn_id})
            edges.append({'from':'media-batch:'+draft.turn_id,'to':draft_id,'kind':'contains'})
            media=media_jobs_by_draft.get(draft.id)
            if not media:continue
            state=media_job_state(media.status);error_code=media.error_code if state in {'FAILED','UNKNOWN'} else ''
            contract=JOB_ERROR_CONTRACTS.get(error_code)
            nodes.append({'id':'media-job:'+media.id,'type':'media_job','role':'媒体生成任务',
                'title':f'{media.kind} · {media.model}','state':state,'visual_role':'media_generation','location':'external_provider',
                'location_label':'外部服务','started_at':iso(media.created_at),'ended_at':iso(media.finished_at),
                'duration_ms':duration_ms(media.created_at,media.finished_at) if media.finished_at else 0,
                'retries':0,'submission_count':1 if media.provider_started_at else 0,
                'status_check_count':max(0,int(media.generation or 0)-1),
                'provider_job_short_id':media.provider_job_id[:8] if media.provider_job_id else '',
                'provider_recovered':bool(media.provider_job_id and int(media.generation or 0)>1),
                'activity_count':0,'turn_id':media.turn_id,
                'media_job_id':media.id,'progress':media.progress,'phase':media.phase,
                'error_code':error_code,'error_summary':public_error_summary(error_code),
                'error_reason':public_error_reason(media.error_detail),
                'error_guidance':contract.guidance if contract else '',
                'failed_phase':contract.phase if contract else '',
                'retryable':contract.retryable if contract else False,
                'retry_class':contract.retry_class if contract else '',
                'side_effect_state':contract.side_effect_state if contract else ''})
            edges.append({'from':draft_id,'to':'media-job:'+media.id,'kind':'contains'})
        for gate in gates:
            identity='review-gate:'+gate.reviewer_job_id
            nodes.append({'id':identity,'type':'review_gate','role':'审查发布门',
                'title':gate.artifact_role,'state':gate.state.upper(),'visual_role':'review_gate',
                'location':'control_plane','location_label':'控制面',
                'started_at':iso(gate.created_at),'ended_at':iso(gate.updated_at)
                    if gate.state!='pending' else None,
                'duration_ms':duration_ms(gate.created_at,gate.updated_at)
                    if gate.state!='pending' else 0,'retries':0,'activity_count':0,
                'job_id':gate.reviewer_job_id,'source_job_id':gate.source_job_id,
                'required':gate.required,'warning':gate.warning})
            edges.append({'from':'job:'+gate.reviewer_job_id,'to':identity,'kind':'review_verdict'})
            if gate.source_job_id in job_ids:
                edges.append({'from':'job:'+gate.source_job_id,
                    'to':'job:'+gate.reviewer_job_id,'kind':'reviewed_by'})
        for interaction in interactions:
            identity='interaction:'+interaction.id
            nodes.append({'id':identity,'type':'interaction','role':'用户决策','title':interaction.kind,
                'state':interaction.status,'visual_role':'user_decision','location':'control_plane','location_label':'控制面',
                'started_at':iso(interaction.created_at),'ended_at':iso(interaction.updated_at) if interaction.status in {'responded','expired'} else None,
                'duration_ms':duration_ms(interaction.created_at,interaction.updated_at) if interaction.status in {'responded','expired'} else 0,
                'retries':0,'activity_count':0,'turn_id':interaction.turn_id})
            edges.append({'from':f'turn:{interaction.turn_id}:primary','to':identity,'kind':'requested_interaction'})
        original_nodes=len(nodes);original_edges=len(edges)
        active_states=RUNNING_JOB_STATES|{'COMMITTED','DRAFT','AWAITING_CONFIRMATION','pending','responding','PENDING','REVIEW_BLOCKED','OPTIONAL_WARNING','REQUIRED_UNAVAILABLE'}
        type_priority={'interaction':0,'review_gate':1,'job':2,'job_group':3,
            'media_job':2,'job_group_draft':4,'media_draft':4,'media_batch':3,
            'wait_barrier':1,'primary_harness':5,'user_turn':6,'continuation':6}
        node_by_id={node['id']:node for node in nodes};parents={}
        for edge in edges:parents.setdefault(edge['to'],[]).append(edge['from'])
        seeds=sorted((node for node in nodes if node.get('state') in active_states),key=lambda node:(
            type_priority.get(node.get('type'),9),str(node.get('started_at') or '')),reverse=False)
        selected=[];selected_ids=set()
        def retain(identity):
            if identity in selected_ids or identity not in node_by_id or len(selected)>=MAX_NODES:return
            for parent in parents.get(identity,[]):retain(parent)
            if identity not in selected_ids and len(selected)<MAX_NODES:
                selected_ids.add(identity);selected.append(node_by_id[identity])
        for node in seeds:retain(node['id'])
        for node in sorted(nodes,key=lambda value:str(value.get('started_at') or ''),reverse=True):
            if len(selected)>=MAX_NODES:break
            retain(node['id'])
        nodes=[node for node in nodes if node['id'] in selected_ids]
        node_ids={node['id'] for node in nodes}
        edges=[edge for edge in edges if edge['from'] in node_ids and edge['to'] in node_ids][:MAX_EDGES]
        metadata={'truncated':original_nodes>len(nodes) or original_edges>len(edges),
            'node_limit':MAX_NODES,'edge_limit':MAX_EDGES,
            'artifact_summary_limit':MAX_ARTIFACT_SUMMARIES,
            'media_job_limit':MAX_MEDIA_JOBS,
            'history_turn_limit':MAX_TURNS,
            'next_history_before':recent[-1].id if include_history and has_older else None,
            'history_before':history_before or None}
        payload={'nodes':nodes,'edges':edges,'metadata':metadata}
        graph_cursor=hashlib.sha256(json.dumps(payload,sort_keys=True,separators=(',',':'),default=str).encode()).hexdigest()
        if cursor==graph_cursor:return {'cursor':graph_cursor,'reset':False,'nodes':[],'edges':[]}
        return {'cursor':graph_cursor,'reset':True,'nodes':nodes,'edges':edges,
            'metadata':metadata,'historical_available':not include_history and total_turns>len(turns)}
