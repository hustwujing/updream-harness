from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from sqlalchemy import select

from .domain import TERMINAL, TurnState
from .store import (InteractionRow, JobActivityRow, JobArtifactRow, JobGroupDraftRow, JobGroupRow, JobRow,
    ResourceReservationRow, ThreadRow, TurnRow, utc_datetime)


RUNNING_JOB_STATES={'CREATED','QUEUED','RUNNING','WAITING_EXTERNAL','CANCELLING'}


def iso(value):return utc_datetime(value).isoformat() if value else None


def duration_ms(start,end):
    if not start:return 0
    finish=utc_datetime(end) if end else datetime.now(timezone.utc)
    return max(0,int((finish-utc_datetime(start)).total_seconds()*1000))


class ExecutionGraphProjector:
    def __init__(self,store):self.store=store

    async def snapshot(self,thread_id:str,owner_id:str,*,cursor:str='',include_history:bool=False)->dict:
        async with self.store.connections() as db:
            thread=(await db.execute(select(ThreadRow).where(ThreadRow.id==thread_id,
                ThreadRow.owner_id==owner_id))).scalar_one_or_none()
            if not thread:raise KeyError('thread_not_found')
            turns=list((await db.execute(select(TurnRow).where(TurnRow.thread_id==thread_id)
                .order_by(TurnRow.created_at,TurnRow.id))).scalars().all())
            if not include_history and len(turns)>5:
                active={row.id for row in turns if TurnState(row.state) not in TERMINAL}
                keep={row.id for row in turns[-5:]}|active
                turns=[row for row in turns if row.id in keep]
            turn_ids={row.id for row in turns}
            drafts=list((await db.execute(select(JobGroupDraftRow).where(
                JobGroupDraftRow.thread_id==thread_id).order_by(JobGroupDraftRow.created_at))).scalars().all())
            groups=list((await db.execute(select(JobGroupRow).where(JobGroupRow.thread_id==thread_id)
                .order_by(JobGroupRow.created_at))).scalars().all())
            if not include_history:
                drafts=[row for row in drafts if row.origin_turn_id in turn_ids
                    or row.state in {'DRAFT','AWAITING_CONFIRMATION'}]
                groups=[row for row in groups if row.origin_turn_id in turn_ids
                    or row.state in {'COMMITTED','RUNNING'}]
            group_ids={row.id for row in groups}
            jobs=list((await db.execute(select(JobRow).where(JobRow.group_id.in_(group_ids or {''}))
                .order_by(JobRow.created_at,JobRow.local_job_id))).scalars().all())
            job_ids={row.id for row in jobs}
            interactions=list((await db.execute(select(InteractionRow).where(
                InteractionRow.thread_id==thread_id).order_by(InteractionRow.created_at))).scalars().all())
            if not include_history:interactions=[row for row in interactions if row.turn_id in turn_ids]
            reservations=list((await db.execute(select(ResourceReservationRow).where(
                ResourceReservationRow.job_id.in_(job_ids or {''})))).scalars().all())
            activities=list((await db.execute(select(JobActivityRow).where(
                JobActivityRow.job_id.in_(job_ids or {''})))).scalars().all())
            artifacts=list((await db.execute(select(JobArtifactRow).where(
                JobArtifactRow.job_id.in_(job_ids or {''})).order_by(
                JobArtifactRow.job_id,JobArtifactRow.artifact_role,JobArtifactRow.position))).scalars().all())
        reservation_by_job={row.job_id:row for row in reservations}
        activity_count={identity:0 for identity in job_ids}
        for row in activities:activity_count[row.job_id]=activity_count.get(row.job_id,0)+1
        artifacts_by_job={identity:[] for identity in job_ids}
        for row in artifacts:artifacts_by_job[row.job_id].append({'artifact_id':row.artifact_id,
            'artifact_role':row.artifact_role,'filename':row.filename,'mime':row.mime,
            'size':row.size,'sha256':row.sha256,'output_disposition':row.output_disposition})
        nodes=[];edges=[]
        for turn in turns:
            user_id=f'turn:{turn.id}:input';primary_id=f'turn:{turn.id}:primary'
            nodes.append({'id':user_id,'type':'continuation' if turn.internal else 'user_turn',
                'role':'任务续接' if turn.internal else '用户输入','title':'后台结果已返回' if turn.internal else turn.public_message[:120],
                'state':'COMPLETED','location':'control_plane','location_label':'控制面',
                'started_at':iso(turn.created_at),'ended_at':iso(turn.created_at),'duration_ms':0,
                'retries':0,'activity_count':0,'turn_id':turn.id})
            nodes.append({'id':primary_id,'type':'primary_harness','role':'主任务执行员',
                'title':f'{turn.harness} · {turn.model}','state':turn.state,
                'location':'opensandbox' if turn.sandbox_id else 'scheduler',
                'location_label':'OpenSandbox' if turn.sandbox_id else '等待调度',
                'started_at':iso(turn.created_at),'ended_at':iso(turn.updated_at) if TurnState(turn.state) in TERMINAL else None,
                'duration_ms':duration_ms(turn.created_at,turn.updated_at) if TurnState(turn.state) in TERMINAL else 0,
                'retries':turn.recovery_generation,'activity_count':0,'turn_id':turn.id})
            edges.append({'from':user_id,'to':primary_id,'kind':'submitted_by'})
        for draft in drafts:
            identity='draft:'+draft.id
            nodes.append({'id':identity,'type':'job_group_draft','role':'任务确认',
                'title':'并行任务等待确认','state':draft.state,'location':'control_plane','location_label':'控制面',
                'started_at':iso(draft.created_at),'ended_at':iso(draft.updated_at) if draft.state in {'REJECTED','EXPIRED','CONFIRMED'} else None,
                'duration_ms':duration_ms(draft.created_at,draft.updated_at) if draft.state in {'REJECTED','EXPIRED','CONFIRMED'} else 0,
                'retries':0,'activity_count':0,'turn_id':draft.origin_turn_id})
            if draft.origin_turn_id in turn_ids:edges.append({'from':f'turn:{draft.origin_turn_id}:primary','to':identity,'kind':'submitted_by'})
        for group in groups:
            identity='group:'+group.id
            nodes.append({'id':identity,'type':'job_group','role':'并行任务组','title':f'{group.member_count} 个并行任务',
                'state':group.state,'location':'control_plane','location_label':'协调器',
                'started_at':iso(group.created_at),'ended_at':iso(group.terminal_at),
                'duration_ms':duration_ms(group.created_at,group.terminal_at) if group.terminal_at else 0,'retries':0,
                'activity_count':0,'turn_id':group.origin_turn_id})
            parent=('draft:'+group.draft_id if group.draft_id else f'turn:{group.origin_turn_id}:primary')
            edges.append({'from':parent,'to':identity,'kind':'contains' if group.draft_id else 'submitted_by'})
        for job in jobs:
            reservation=reservation_by_job.get(job.id);resources=json.loads(reservation.gpu_ids_json) if reservation else []
            role=('质量审查员' if job.descriptor_id=='review.submit' else '委托任务执行员'
                if job.kind=='harness' else '外部能力任务' if job.kind=='provider' else '任务执行员')
            location=('opensandbox' if job.kind=='harness' else 'external_provider' if job.kind=='provider'
                else 'gpu_worker' if resources else 'host_worker')
            labels={'opensandbox':'OpenSandbox','external_provider':'外部服务','gpu_worker':'GPU Worker','host_worker':'宿主 Worker'}
            nodes.append({'id':'job:'+job.id,'type':'job','role':role,
                'title':f'{job.local_job_id} · {job.descriptor_id}','state':job.state,
                'location':location,'location_label':labels[location],
                'started_at':iso(job.created_at),'ended_at':iso(job.terminal_at),
                'duration_ms':duration_ms(job.created_at,job.terminal_at) if job.terminal_at else 0,'retries':max(0,job.generation-1),
                'activity_count':activity_count.get(job.id,0),'job_id':job.id,'turn_id':job.origin_turn_id,
                'artifacts':artifacts_by_job.get(job.id,[]),
                'resources':{'cpu':reservation.cpu if reservation else None,
                    'memory_mb':reservation.memory_mb if reservation else None,'gpu_ids':resources}})
            edges.append({'from':'group:'+job.group_id,'to':'job:'+job.id,'kind':'contains'})
        for interaction in interactions:
            identity='interaction:'+interaction.id
            nodes.append({'id':identity,'type':'interaction','role':'用户决策','title':interaction.kind,
                'state':interaction.status,'location':'control_plane','location_label':'控制面',
                'started_at':iso(interaction.created_at),'ended_at':iso(interaction.updated_at) if interaction.status in {'responded','expired'} else None,
                'duration_ms':duration_ms(interaction.created_at,interaction.updated_at) if interaction.status in {'responded','expired'} else 0,
                'retries':0,'activity_count':0,'turn_id':interaction.turn_id})
            edges.append({'from':f'turn:{interaction.turn_id}:primary','to':identity,'kind':'requested_interaction'})
        payload={'nodes':nodes,'edges':edges}
        graph_cursor=hashlib.sha256(json.dumps(payload,sort_keys=True,separators=(',',':'),default=str).encode()).hexdigest()
        if cursor==graph_cursor:return {'cursor':graph_cursor,'reset':False,'nodes':[],'edges':[]}
        return {'cursor':graph_cursor,'reset':True,'nodes':nodes,'edges':edges,
            'historical_available':not include_history and len(turn_ids)<len((await self.store.list_turns(thread_id,owner_id)))}
