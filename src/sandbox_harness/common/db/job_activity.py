"""Job Activity persistence; transaction boundaries are preserved."""
from __future__ import annotations
import json
from datetime import (datetime)
from sqlalchemy import (select)
from sandbox_harness.common.contracts.domain import (identifier, now)
from sandbox_harness.common.events.activity_projection import (merge_projection_payload, projection_instruction)
from .models import (JobActivityRow, JobDisplayActivityRow, JobRow, canonical_json, utc_datetime)


class JobActivityTransactions:
    @staticmethod
    def job_activity_view(row: JobActivityRow)->dict:
        return {'id':row.id,'job_id':row.job_id,'sequence':row.sequence,'kind':row.kind,
            'status':row.status,'summary':row.summary,'payload':json.loads(row.payload_json),
            'created_at':utc_datetime(row.created_at),'updated_at':utc_datetime(row.updated_at)}


    @staticmethod
    def job_display_activity_view(row:JobDisplayActivityRow)->dict:
        return {'id':row.id,'job_id':row.job_id,'sequence':row.sequence,
            'cursor_sequence':row.cursor_sequence,'kind':row.kind,'status':row.status,
            'summary':row.summary,'payload':json.loads(row.payload_json),'raw_count':row.raw_count,
            'created_at':utc_datetime(row.created_at),'updated_at':utc_datetime(row.updated_at)}


    async def _project_job_activity(self,db,job_id:str,sequence:int,kind:str,payload:dict,
                                    status:str,timestamp:datetime)->None:
        instruction=projection_instruction(kind,payload,status,sequence)
        if not instruction:return
        row=(await db.execute(select(JobDisplayActivityRow).where(
            JobDisplayActivityRow.job_id==job_id,
            JobDisplayActivityRow.projection_key==instruction.key).with_for_update())).scalar_one_or_none()
        if row:
            row.cursor_sequence=max(row.cursor_sequence,sequence);row.raw_count+=1
            row.kind=instruction.kind
            if not (row.status in {'completed','failed','interrupted'}
                    and instruction.status=='in_progress'):
                row.status=instruction.status
            if instruction.summary not in {'tool','tool_output','执行阶段'} or not row.summary:
                row.summary=instruction.summary[:1000]
            row.payload_json=canonical_json(merge_projection_payload(
                json.loads(row.payload_json),instruction));row.updated_at=timestamp
        else:
            row=JobDisplayActivityRow(id=identifier(),job_id=job_id,
                projection_key=instruction.key,sequence=sequence,cursor_sequence=sequence,
                kind=instruction.kind,status=instruction.status,summary=instruction.summary[:1000],
                payload_json=canonical_json(merge_projection_payload({},instruction)),raw_count=1,
                created_at=timestamp,updated_at=timestamp);db.add(row)


    async def append_job_activity(self, job_id: str, sequence: int, kind: str,
                                  payload: dict, *, status: str = "in_progress") -> dict:
        async with self.connections.begin() as db:
            job=await db.get(JobRow,job_id)
            if not job:raise KeyError('job_not_found')
            row=(await db.execute(select(JobActivityRow).where(JobActivityRow.job_id==job_id,
                JobActivityRow.sequence==sequence).with_for_update())).scalar_one_or_none()
            timestamp=now();summary=str(payload.get('name') or payload.get('text') or kind)[:1000]
            if row:
                if row.kind!=kind or json.loads(row.payload_json)!=payload:
                    raise RuntimeError('job_activity_sequence_conflict')
                return self.job_activity_view(row)
            row=JobActivityRow(id=identifier(),job_id=job_id,sequence=sequence,kind=kind,
                status=status,summary=summary,payload_json=canonical_json(payload),
                created_at=timestamp,updated_at=timestamp);db.add(row)
            await self._project_job_activity(db,job_id,sequence,kind,payload,status,timestamp)
            await db.flush()
            return self.job_activity_view(row)


    async def list_job_activities(self,job_id:str,after:int=0,limit:int=500)->list[dict]:
        async with self.connections() as db:
            rows=(await db.execute(select(JobActivityRow).where(JobActivityRow.job_id==job_id,
                JobActivityRow.sequence>after).order_by(JobActivityRow.sequence).limit(
                min(max(limit,1),1000)))).scalars().all()
            return [self.job_activity_view(row) for row in rows]


    async def ensure_job_display_activities(self,job_ids:set[str]|tuple[str,...]|list[str])->None:
        identities=set(job_ids)
        if not identities:return
        async with self.connections.begin() as db:
            projected=set((await db.execute(select(JobDisplayActivityRow.job_id).where(
                JobDisplayActivityRow.job_id.in_(identities)).distinct())).scalars().all())
            for job_id in sorted(identities-projected):
                rows=list((await db.execute(select(JobActivityRow).where(
                    JobActivityRow.job_id==job_id).order_by(JobActivityRow.sequence))).scalars().all())
                for row in rows:
                    await self._project_job_activity(db,row.job_id,row.sequence,row.kind,
                        json.loads(row.payload_json),row.status,utc_datetime(row.updated_at))


    async def list_job_display_activities(self,job_id:str,after:int=0,
                                          limit:int=500)->list[dict]:
        await self.ensure_job_display_activities({job_id})
        async with self.connections() as db:
            rows=(await db.execute(select(JobDisplayActivityRow).where(
                JobDisplayActivityRow.job_id==job_id,
                JobDisplayActivityRow.cursor_sequence>after).order_by(
                JobDisplayActivityRow.sequence).limit(min(max(limit,1),1000)))).scalars().all()
            return [self.job_display_activity_view(row) for row in rows]

