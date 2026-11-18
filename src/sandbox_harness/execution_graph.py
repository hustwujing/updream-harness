from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from sqlalchemy import case,func,or_,select

from .domain import TERMINAL, TurnState
from .store import (InteractionRow, JobActivityRow, JobArtifactRow, JobAttemptRow, JobGroupDraftRow, JobGroupRow, JobRow,
    ResourceReservationRow,ReviewGateRow,ThreadRow,TurnOrderRow,TurnRow,now,utc_datetime)


RUNNING_JOB_STATES={'CREATED','QUEUED','RUNNING','WAITING_EXTERNAL','CANCELLING','PAUSING'}
MAX_TURNS=50;MAX_DRAFTS=100;MAX_GROUPS=200;MAX_JOBS=300;MAX_INTERACTIONS=100
MAX_ARTIFACT_SUMMARIES=200;MAX_NODES=900;MAX_EDGES=1400


def iso(value):return utc_datetime(value).isoformat() if value else None


def duration_ms(start,end):
    if not start:return 0
    finish=utc_datetime(end) if end else datetime.now(timezone.utc)
    return max(0,int((finish-utc_datetime(start)).total_seconds()*1000))


class ExecutionGraphProjector:
    def __init__(self,store):self.store=store

    async def snapshot(self,thread_id:str,owner_id:str,*,cursor:str='',include_history:bool=False,
                       history_before:str='')->dict:
        async with self.store.connections() as db:
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
            all_drafts=list((await db.execute(select(JobGroupDraftRow).where(
                JobGroupDraftRow.thread_id==thread_id,or_(
                    JobGroupDraftRow.origin_turn_id.in_(turn_ids or {''}),
                    JobGroupDraftRow.state.in_(['DRAFT','AWAITING_CONFIRMATION'])))
                .order_by(JobGroupDraftRow.created_at.desc()).limit(MAX_DRAFTS))).scalars().all())
            drafts=list(reversed(all_drafts))
            groups=list(reversed(list((await db.execute(select(JobGroupRow).where(
                JobGroupRow.thread_id==thread_id,or_(JobGroupRow.origin_turn_id.in_(turn_ids or {''}),
                    JobGroupRow.state.in_(['COMMITTED','RUNNING'])))
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
            jobs=list(reversed(list((await db.execute(select(JobRow).where(
                JobRow.group_id.in_(group_ids or {''})).order_by(JobRow.created_at.desc(),
                    JobRow.local_job_id).limit(MAX_JOBS))).scalars().all())))
            job_ids={row.id for row in jobs}
            interactions=list((await db.execute(select(InteractionRow).where(
                InteractionRow.thread_id==thread_id,or_(
                    InteractionRow.turn_id.in_(turn_ids or {''}),
                    InteractionRow.status.in_(['pending','responding'])))
                .order_by(InteractionRow.created_at.desc())
                .limit(MAX_INTERACTIONS))).scalars().all())
            interactions=list(reversed(interactions))
            reservations=list((await db.execute(select(ResourceReservationRow).where(
                ResourceReservationRow.job_id.in_(job_ids or {''})))).scalars().all())
            activity_rows=(await db.execute(select(JobActivityRow.job_id,func.count()).where(
                JobActivityRow.job_id.in_(job_ids or {''})).group_by(JobActivityRow.job_id))).all()
            retry_rows=(await db.execute(select(JobAttemptRow.job_id,func.sum(case(
                (JobAttemptRow.outcome=='PRE_ACCEPT_RETRY',1),
                (JobAttemptRow.retry_kind.in_(['recovery','reconcile']),1),else_=0))).where(
                    JobAttemptRow.job_id.in_(job_ids or {''})).group_by(JobAttemptRow.job_id))).all()
            artifacts=list((await db.execute(select(JobArtifactRow).where(
                JobArtifactRow.job_id.in_(job_ids or {''})).order_by(
                JobArtifactRow.job_id,JobArtifactRow.artifact_role,JobArtifactRow.position)
                .limit(MAX_ARTIFACT_SUMMARIES))).scalars().all())
            artifact_counts=dict((await db.execute(select(JobArtifactRow.job_id,func.count()).where(
                JobArtifactRow.job_id.in_(job_ids or {''})).group_by(JobArtifactRow.job_id))).all())
            gates=list((await db.execute(select(ReviewGateRow).where(
                ReviewGateRow.reviewer_job_id.in_(job_ids or {''})))).scalars().all())
        reservation_by_job={row.job_id:row for row in reservations}
        activity_count={identity:int(count) for identity,count in activity_rows}
        retry_count={identity:int(count or 0) for identity,count in retry_rows}
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
                'retries':max(0,turn.recovery_generation-1),'activity_count':0,'turn_id':turn.id})
            edges.append({'from':user_id,'to':primary_id,'kind':'submitted_by'})
        for draft in drafts:
            identity='draft:'+draft.id
            draft_state=('EXPIRED' if draft.state in {'DRAFT','AWAITING_CONFIRMATION'}
                and utc_datetime(draft.expires_at)<=now() else draft.state)
            nodes.append({'id':identity,'type':'job_group_draft','role':'任务确认',
                'title':'并行任务等待确认','state':draft_state,'location':'control_plane','location_label':'控制面',
                'started_at':iso(draft.created_at),'ended_at':iso(draft.updated_at) if draft_state in {'REJECTED','EXPIRED','CONFIRMED'} else None,
                'duration_ms':duration_ms(draft.created_at,draft.updated_at) if draft_state in {'REJECTED','EXPIRED','CONFIRMED'} else 0,
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
            request=json.loads(job.request_json);requirements=request.get('resource_requirements') or {}
            runtime_profile=request.get('runtime_profile_id') or ''
            role=('质量审查员' if job.descriptor_id=='review.submit' else '子 Harness'
                if job.kind=='harness' else '外部能力任务' if job.kind=='provider' else '任务执行员')
            location=('opensandbox' if job.kind=='harness' else 'external_provider' if job.kind=='provider'
                else 'gpu_worker' if resources else 'host_worker')
            labels={'opensandbox':'OpenSandbox','external_provider':'外部服务','gpu_worker':'GPU Worker','host_worker':'宿主 Worker'}
            location_label=labels[location]+((' · '+runtime_profile) if location=='opensandbox' and runtime_profile else '')
            nodes.append({'id':'job:'+job.id,'type':'job','role':role,
                'title':f'{job.local_job_id} · {job.descriptor_id}','state':job.state,
                'location':location,'location_label':location_label,
                'started_at':iso(job.created_at),'ended_at':iso(job.terminal_at),
                'duration_ms':duration_ms(job.created_at,job.terminal_at) if job.terminal_at else 0,
                'retries':retry_count.get(job.id,0),
                'activity_count':activity_count.get(job.id,0),'job_id':job.id,'turn_id':job.origin_turn_id,
                'artifacts':artifacts_by_job.get(job.id,[]),
                'artifact_count':artifact_counts.get(job.id,0),
                'artifacts_truncated':artifact_counts.get(job.id,0)>len(artifacts_by_job.get(job.id,[])),
                'resources':{'cpu':reservation.cpu if reservation else None,
                    'memory_mb':reservation.memory_mb if reservation else None,'gpu_ids':resources,
                    'runtime_profile_id':runtime_profile,'child_profile_id':request.get('child_profile_id') or '',
                    'harness':requirements.get('harness') or request.get('parameters',{}).get('harness') or '',
                    'model':request.get('parameters',{}).get('model') or '',
                    'skill_ids':request.get('skill_ids') or [],
                    'capability_ids':requirements.get('required_capability_ids') or [],
                    'network_origins':request.get('network_origins') or []}})
            edges.append({'from':'group:'+job.group_id,'to':'job:'+job.id,'kind':'contains'})
        for gate in gates:
            identity='review-gate:'+gate.reviewer_job_id
            nodes.append({'id':identity,'type':'review_gate','role':'审查发布门',
                'title':gate.artifact_role,'state':gate.state.upper(),
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
                'state':interaction.status,'location':'control_plane','location_label':'控制面',
                'started_at':iso(interaction.created_at),'ended_at':iso(interaction.updated_at) if interaction.status in {'responded','expired'} else None,
                'duration_ms':duration_ms(interaction.created_at,interaction.updated_at) if interaction.status in {'responded','expired'} else 0,
                'retries':0,'activity_count':0,'turn_id':interaction.turn_id})
            edges.append({'from':f'turn:{interaction.turn_id}:primary','to':identity,'kind':'requested_interaction'})
        original_nodes=len(nodes);original_edges=len(edges)
        active_states=RUNNING_JOB_STATES|{'COMMITTED','DRAFT','AWAITING_CONFIRMATION','pending','responding','PENDING','REVIEW_BLOCKED','OPTIONAL_WARNING','REQUIRED_UNAVAILABLE'}
        type_priority={'interaction':0,'review_gate':1,'job':2,'job_group':3,
            'job_group_draft':4,'primary_harness':5,'user_turn':6,'continuation':6}
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
            'history_turn_limit':MAX_TURNS,
            'next_history_before':recent[-1].id if include_history and has_older else None,
            'history_before':history_before or None}
        payload={'nodes':nodes,'edges':edges,'metadata':metadata}
        graph_cursor=hashlib.sha256(json.dumps(payload,sort_keys=True,separators=(',',':'),default=str).encode()).hexdigest()
        if cursor==graph_cursor:return {'cursor':graph_cursor,'reset':False,'nodes':[],'edges':[]}
        return {'cursor':graph_cursor,'reset':True,'nodes':nodes,'edges':edges,
            'metadata':metadata,'historical_available':not include_history and total_turns>len(turns)}
