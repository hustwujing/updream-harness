"""Job Artifacts persistence; transaction boundaries are preserved."""
from __future__ import annotations
import json
import hashlib
from sqlalchemy import (select)
from sqlalchemy.ext.asyncio import (AsyncSession)
from sandbox_harness.common.contracts.domain import (ItemType, identifier, now)
from sandbox_harness.common.contracts.job_errors import (ArtifactCollectionCardinalityError, ArtifactValidationError)
from sandbox_harness.common.contracts.job_contracts import (JobState, JobRequest, TERMINAL_JOB_STATES, JobArtifactOutput, OutputDisposition)
from .models import (ArtifactRow, ItemRow, JobArtifactRow, JobGroupRow, JobRow, ReviewGateRow, ThreadRow, TurnRow, canonical_json)


class JobArtifactsTransactions:
    @staticmethod
    def job_artifact_view(row: JobArtifactRow) -> JobArtifactOutput:
        return JobArtifactOutput(
            artifact_id=row.artifact_id, artifact_role=row.artifact_role,
            item_key=row.item_key, filename=row.filename, position=row.position,
            output_disposition=OutputDisposition(row.output_disposition), sha256=row.sha256,
            size=row.size, mime=row.mime, object_ref=row.object_ref,
        )


    async def get_reviewed_job_artifact_for_thread(self,artifact_id:str,
                                                   thread_id:str)->JobArtifactOutput|None:
        async with self.connections() as db:
            row=(await db.execute(select(JobArtifactRow).join(
                JobRow,JobRow.id==JobArtifactRow.job_id).where(
                JobArtifactRow.artifact_id==artifact_id,
                JobArtifactRow.state=='reviewed',
                JobRow.thread_id==thread_id).order_by(
                JobArtifactRow.created_at,JobArtifactRow.id).limit(1))).scalar_one_or_none()
        return self.job_artifact_view(row) if row else None


    async def bind_job_artifact(self, value, *, sha256: str, size: int,
                                mime: str, object_ref: str) -> JobArtifactOutput:
        async with self.connections() as lookup:
            group_id = (await lookup.execute(select(JobRow.group_id).where(
                JobRow.id == value.job_id))).scalar_one_or_none()
        if not group_id:
            raise KeyError("job_not_found")
        async with self.job_lock(f"group:{group_id}"):
            async with self.connections.begin() as db:
                await db.get(JobGroupRow, group_id, with_for_update=True)
                job = await db.get(JobRow, value.job_id, with_for_update=True)
                if job.generation != value.generation or JobState(job.state) in TERMINAL_JOB_STATES:
                    raise RuntimeError("stale_job_generation")
                request = JobRequest.model_validate_json(job.request_json)
                contract = next((item for item in request.expected_outputs
                    if item.artifact_role == value.artifact_role), None)
                if not contract:
                    raise RuntimeError("artifact_role_not_granted")
                if value.output_disposition != contract.output_disposition:
                    raise RuntimeError("artifact_disposition_not_granted")
                if size > contract.max_item_bytes:
                    raise ArtifactValidationError("artifact_item_too_large")
                from sandbox_harness.common.artifacts.artifact_gateway import (MimeRegistry)
                if not MimeRegistry.matches(mime, contract.mime_patterns):
                    raise ArtifactValidationError("artifact_mime_not_allowed")
                existing = (await db.execute(select(JobArtifactRow).where(
                    JobArtifactRow.job_id == job.id,
                    JobArtifactRow.artifact_role == value.artifact_role,
                    JobArtifactRow.item_key == value.item_key,
                ).with_for_update())).scalar_one_or_none()
                if existing:
                    identity = (existing.position, existing.filename, existing.requested_disposition,
                        existing.sha256, existing.size, existing.mime, existing.object_ref)
                    expected = (value.position, value.filename, value.output_disposition.value,
                        sha256, size, mime, object_ref)
                    if identity != expected:
                        raise RuntimeError("artifact_item_key_conflict")
                    return self.job_artifact_view(existing)
                position_conflict = (await db.execute(select(JobArtifactRow.id).where(
                    JobArtifactRow.job_id == job.id,
                    JobArtifactRow.artifact_role == value.artifact_role,
                    JobArtifactRow.position == value.position,
                ))).scalar_one_or_none()
                if position_conflict:
                    raise RuntimeError("artifact_position_conflict")
                artifact_id = identifier()
                timestamp = now()
                db.add(ArtifactRow(
                    id=artifact_id, turn_id=job.origin_turn_id,
                    path=f"jobs/{job.id}/{value.filename}", sha256=sha256, size=size,
                    mime=mime, object_ref=object_ref, created_at=timestamp,
                ))
                row = JobArtifactRow(
                    id=identifier(), job_id=job.id, artifact_id=artifact_id,
                    artifact_role=value.artifact_role, item_key=value.item_key,
                    filename=value.filename, position=value.position,
                    requested_disposition=value.output_disposition.value,
                    output_disposition=OutputDisposition.CANDIDATE.value,
                    sha256=sha256, size=size, mime=mime, object_ref=object_ref,
                    validation_receipts_json="[]", review_receipts_json="[]",
                    state="committed", created_at=timestamp,
                )
                db.add(row)
                await self._record_usage_locked(db,idempotency_key="artifact:"+artifact_id,
                    owner_id=job.owner_id,thread_id=job.thread_id,turn_id=job.origin_turn_id,
                    job_id=job.id,kind="object_bytes",amount=size,unit="byte",source="artifact")
                await db.flush()
                return self.job_artifact_view(row)


    async def list_job_artifacts(self, job_id: str,
                                 artifact_role: str | None = None) -> list[JobArtifactOutput]:
        async with self.connections() as db:
            query = select(JobArtifactRow).where(JobArtifactRow.job_id == job_id)
            if artifact_role:
                query = query.where(JobArtifactRow.artifact_role == artifact_role)
            rows = (await db.execute(query.order_by(
                JobArtifactRow.artifact_role, JobArtifactRow.position))).scalars().all()
            return [self.job_artifact_view(row) for row in rows]


    async def resolve_review_candidates(self, owner_id: str, thread_id: str,
                                        artifact_ids: list[str], *, allow_mixed: bool = False) -> dict:
        async with self.connections() as db:
            rows=list((await db.execute(select(JobArtifactRow).where(
                JobArtifactRow.artifact_id.in_(artifact_ids)))).scalars().all())
            if allow_mixed and (len(rows)!=len(artifact_ids)
                    or len({(row.job_id,row.artifact_role) for row in rows})!=1):
                return await self._resolve_advisory_review_candidates_locked(
                    db,owner_id,thread_id,artifact_ids)
            if not rows:
                artifacts=list((await db.execute(select(ArtifactRow).join(TurnRow,
                    TurnRow.id==ArtifactRow.turn_id).join(ThreadRow,
                    ThreadRow.id==TurnRow.thread_id).where(ArtifactRow.id.in_(artifact_ids),
                    ThreadRow.id==thread_id,ThreadRow.owner_id==owner_id))).scalars().all())
                if len(artifacts)!=len(artifact_ids):raise KeyError("review_artifact_not_found")
                by_id={row.id:row for row in artifacts}
                return {"candidates":[{"artifact_id":identity,"artifact_role":"candidate",
                    "sha256":by_id[identity].sha256,"mime":by_id[identity].mime,
                    "size":by_id[identity].size,"validation_receipts":[],
                    "validation_contract":"none"} for identity in artifact_ids],"target":None}
            if len(rows)!=len(artifact_ids):raise RuntimeError("review_collection_mixed_provenance")
            source={(row.job_id,row.artifact_role) for row in rows}
            if len(source)!=1:raise RuntimeError("review_collection_mixed_provenance")
            job_id,role=next(iter(source));job=await db.get(JobRow,job_id)
            if not job or job.owner_id!=owner_id or job.thread_id!=thread_id:
                raise KeyError("review_artifact_not_found")
            collection=list((await db.execute(select(JobArtifactRow).where(
                JobArtifactRow.job_id==job_id,JobArtifactRow.artifact_role==role)
                .order_by(JobArtifactRow.position))).scalars().all())
            if [row.artifact_id for row in collection]!=artifact_ids:
                raise RuntimeError("review_requires_complete_ordered_collection")
            request=JobRequest.model_validate_json(job.request_json)
            contract=next((item for item in request.expected_outputs if item.artifact_role==role),None)
            if not contract:raise RuntimeError("review_output_contract_missing")
            receipts=json.loads(collection[0].validation_receipts_json or "[]")
            if any(json.loads(row.validation_receipts_json or "[]")!=receipts for row in collection):
                raise RuntimeError("review_validation_receipts_inconsistent")
            by_validator={str(item.get("validator")):item for item in receipts}
            digests=[row.sha256 for row in collection]
            if any(name not in by_validator or by_validator[name].get("status")!="pass"
                    for name in contract.validators):
                raise RuntimeError("artifact_validation_required")
            if any(item.get("artifact_digests") not in (None,digests) for item in receipts):
                raise RuntimeError("artifact_validation_digest_mismatch")
            return {"candidates":[{"artifact_id":row.artifact_id,"artifact_role":role,
                "sha256":row.sha256,"mime":row.mime,"size":row.size,
                "validation_receipts":receipts,"validation_contract":(
                    "required" if contract.validators else "none")} for row in collection],
                "target":{"source_job_id":job.id,"source_generation":job.generation,
                    "artifact_role":role,"artifact_ids":[row.artifact_id for row in collection],
                    "artifact_digests":digests,"review_policy":contract.review_policy,
                    "requested_disposition":contract.output_disposition.value,
                    "gate_eligible":all(row.state=="awaiting_review" and
                        row.output_disposition==OutputDisposition.CANDIDATE.value for row in collection)}}


    async def _resolve_advisory_review_candidates_locked(self,db:AsyncSession,owner_id:str,
            thread_id:str,artifact_ids:list[str])->dict:
        candidates=[]
        for identity in artifact_ids:
            row=(await db.execute(select(JobArtifactRow,JobRow).join(JobRow,
                JobRow.id==JobArtifactRow.job_id).where(JobArtifactRow.artifact_id==identity,
                    JobRow.owner_id==owner_id,JobRow.thread_id==thread_id))).first()
            if row:
                artifact,job=row;receipts=json.loads(artifact.validation_receipts_json or "[]")
                candidates.append({"artifact_id":artifact.artifact_id,
                    "artifact_role":artifact.artifact_role,"sha256":artifact.sha256,
                    "mime":artifact.mime,"size":artifact.size,"validation_receipts":receipts,
                    "validation_contract":"available" if receipts else "none"})
                continue
            generic=(await db.execute(select(ArtifactRow).join(TurnRow,
                TurnRow.id==ArtifactRow.turn_id).join(ThreadRow,
                ThreadRow.id==TurnRow.thread_id).where(ArtifactRow.id==identity,
                    ThreadRow.id==thread_id,ThreadRow.owner_id==owner_id))).scalar_one_or_none()
            if not generic:raise KeyError("review_artifact_not_found")
            candidates.append({"artifact_id":generic.id,"artifact_role":"candidate",
                "sha256":generic.sha256,"mime":generic.mime,"size":generic.size,
                "validation_receipts":[],"validation_contract":"none"})
        return {"candidates":candidates,"target":None}


    async def create_review_gate(self, reviewer_job_id: str, target: dict,
                                 required: bool) -> None:
        async with self.connections.begin() as db:
            existing=await db.get(ReviewGateRow,reviewer_job_id,with_for_update=True)
            encoded_ids=canonical_json(target["artifact_ids"])
            encoded_digests=canonical_json(target["artifact_digests"])
            if existing:
                identity=(existing.source_job_id,existing.source_generation,existing.artifact_role,
                    existing.artifact_ids_json,existing.artifact_digests_json,existing.required)
                expected=(target["source_job_id"],target["source_generation"],target["artifact_role"],
                    encoded_ids,encoded_digests,required)
                if identity!=expected:raise RuntimeError("review_gate_conflict")
                return
            db.add(ReviewGateRow(reviewer_job_id=reviewer_job_id,
                source_job_id=target["source_job_id"],source_generation=target["source_generation"],
                artifact_role=target["artifact_role"],artifact_ids_json=encoded_ids,
                artifact_digests_json=encoded_digests,required=required,state="pending",
                receipt_json="{}",warning="",created_at=now(),updated_at=now()))


    async def record_policy_review_unavailable(self,source_job_id:str,generation:int,
                                               error:str)->list[dict]:
        job=await self.get_job(source_job_id);request=await self.get_job_request(source_job_id)
        if not job or not request or job.generation!=generation:
            raise RuntimeError("stale_job_generation")
        results=[]
        for contract in request.expected_outputs:
            if not contract.requires_review:continue
            rows=await self.list_job_artifacts(source_job_id,contract.artifact_role)
            if not rows:continue
            gate_id=hashlib.sha256(("policy-dispatch:"+source_job_id+":"+
                contract.artifact_role).encode()).hexdigest()[:32]
            target={"source_job_id":source_job_id,"source_generation":generation,
                "artifact_role":contract.artifact_role,
                "artifact_ids":[row.artifact_id for row in rows],
                "artifact_digests":[row.sha256 for row in rows]}
            await self.create_review_gate(gate_id,target,contract.review_policy!="optional")
            results.append(await self.apply_review_gate(gate_id,
                execution_error="review_dispatch_unavailable:"+error))
        return results


    async def apply_review_gate(self, reviewer_job_id: str, *, receipt: dict | None = None,
                                execution_error: str = "", _db:AsyncSession|None=None) -> dict:
        if _db is None:
            async with self.connections.begin() as db:
                return await self.apply_review_gate(reviewer_job_id,receipt=receipt,
                    execution_error=execution_error,_db=db)
        db=_db
        gate=await db.get(ReviewGateRow,reviewer_job_id,with_for_update=True)
        if not gate:return {"state":"unlinked"}
        encoded=canonical_json(receipt or {"execution_error":execution_error})
        if gate.state in {"published","review_blocked","optional_warning"}:
            if gate.receipt_json!=encoded:raise RuntimeError("review_gate_terminal_conflict")
            return {"state":gate.state,"warning":gate.warning}
        source=await db.get(JobRow,gate.source_job_id,with_for_update=True)
        if not source or source.generation!=gate.source_generation:
            raise RuntimeError("stale_review_source_generation")
        rows=list((await db.execute(select(JobArtifactRow).where(
            JobArtifactRow.job_id==gate.source_job_id,
            JobArtifactRow.artifact_role==gate.artifact_role)
            .order_by(JobArtifactRow.position).with_for_update())).scalars().all())
        ids=[row.artifact_id for row in rows];digests=[row.sha256 for row in rows]
        if ids!=json.loads(gate.artifact_ids_json) or digests!=json.loads(gate.artifact_digests_json):
            raise RuntimeError("review_target_identity_changed")
        if receipt and receipt.get("artifact_digests")!=digests:
            raise RuntimeError("artifact_review_digest_mismatch")
        verdict=str((receipt or {}).get("verdict", ""))
        publish=verdict=="PASS" or (bool(execution_error) and not gate.required)
        warning=("optional_review_unavailable:"+execution_error if execution_error and not gate.required else "")
        for row in rows:
            row.review_receipts_json=canonical_json([receipt or {
                "verdict":"UNAVAILABLE","error":execution_error}])
            row.state="reviewed" if publish else "review_blocked"
        if publish:
            for row in rows:
                row.output_disposition=row.requested_disposition
                if row.output_disposition in {OutputDisposition.WORKSPACE.value,
                        OutputDisposition.DELIVERY.value}:
                    await self._register_project_file_locked(db,source.thread_id,
                        path=f"artifacts/jobs/{source.id}/{row.filename}",source_type="artifact",
                        source_turn_id=source.origin_turn_id,sha256=row.sha256,size=row.size,
                        mime=row.mime,object_ref=row.object_ref)
                if (row.output_disposition==OutputDisposition.DELIVERY.value
                        and JobState(source.state) in TERMINAL_JOB_STATES):
                    visible=(await db.execute(select(ItemRow.id).where(ItemRow.thread_id==source.thread_id,
                        ItemRow.type==ItemType.ARTIFACT.value,
                        ItemRow.payload_json.like(f'%"artifact_id": "{row.artifact_id}"%'))
                        .limit(1))).scalar_one_or_none()
                    if not visible:
                        sequence=await self.next_item_sequence(db,source.thread_id);timestamp=now()
                        db.add(ItemRow(id=identifier(),thread_id=source.thread_id,
                            turn_id=source.origin_turn_id,sequence=sequence,type=ItemType.ARTIFACT.value,
                            status="completed",summary=row.filename,payload_json=json.dumps({
                                "artifact_id":row.artifact_id,"mime":row.mime,"size":row.size,
                                "partial":False,"job_id":source.id,"artifact_role":row.artifact_role,
                                "local_job_id":source.local_job_id}),
                            created_at=timestamp,updated_at=timestamp))
            gate.state="published" if verdict=="PASS" else "optional_warning"
        else:
            gate.state="required_unavailable" if execution_error else "review_blocked"
        gate.receipt_json=encoded;gate.warning=warning;gate.updated_at=now()
        return {"state":gate.state,"warning":warning}


    async def get_review_gate(self,reviewer_job_id:str)->dict|None:
        async with self.connections() as db:
            row=await db.get(ReviewGateRow,reviewer_job_id)
            return ({"reviewer_job_id":row.reviewer_job_id,"source_job_id":row.source_job_id,
                "source_generation":row.source_generation,"artifact_role":row.artifact_role,
                "artifact_ids":json.loads(row.artifact_ids_json),
                "artifact_digests":json.loads(row.artifact_digests_json),"required":row.required,
                "state":row.state,"receipt":json.loads(row.receipt_json or "{}"),
                "warning":row.warning} if row else None)


    async def finalize_job_artifact_collection(self, job_id: str, generation: int,
                                               artifact_role: str, *,
                                               validation_receipts: list[dict] | None = None,
                                               review_receipt: dict | None = None) -> dict:
        async with self.connections() as lookup:
            group_id = (await lookup.execute(select(JobRow.group_id).where(
                JobRow.id == job_id))).scalar_one_or_none()
        if not group_id:
            raise KeyError("job_not_found")
        async with self.job_lock(f"group:{group_id}"):
            async with self.connections.begin() as db:
                await db.get(JobGroupRow, group_id, with_for_update=True)
                job = await db.get(JobRow, job_id, with_for_update=True)
                if job.generation != generation or JobState(job.state) in TERMINAL_JOB_STATES:
                    raise RuntimeError("stale_job_generation")
                request = JobRequest.model_validate_json(job.request_json)
                contract = next((item for item in request.expected_outputs
                    if item.artifact_role == artifact_role), None)
                if not contract:
                    raise RuntimeError("artifact_role_not_granted")
                rows = list((await db.execute(select(JobArtifactRow).where(
                    JobArtifactRow.job_id == job_id,
                    JobArtifactRow.artifact_role == artifact_role,
                ).order_by(JobArtifactRow.position).with_for_update())).scalars().all())
                if not contract.min_items <= len(rows) <= contract.max_items:
                    raise ArtifactCollectionCardinalityError(artifact_role,
                        contract.min_items,contract.max_items,len(rows))
                if [row.position for row in rows] != list(range(len(rows))):
                    raise RuntimeError("artifact_collection_order_invalid")
                if sum(row.size for row in rows) > contract.max_total_bytes:
                    raise ArtifactValidationError("artifact_collection_too_large")
                receipts = validation_receipts or []
                by_validator = {str(item.get("validator")): item for item in receipts}
                if any(name not in by_validator or by_validator[name].get("status") != "pass"
                       for name in contract.validators):
                    raise RuntimeError("artifact_validation_required")
                digests = [row.sha256 for row in rows]
                if any(item.get("artifact_digests") not in (None, digests) for item in receipts):
                    raise RuntimeError("artifact_validation_digest_mismatch")
                if (contract.requires_review and review_receipt
                        and review_receipt.get("artifact_digests") != digests):
                    raise RuntimeError("artifact_review_digest_mismatch")
                review_passed = (not contract.requires_review or bool(review_receipt)
                    and review_receipt.get("verdict") == "PASS")
                for row in rows:
                    row.validation_receipts_json = canonical_json(receipts)
                    row.review_receipts_json = canonical_json([review_receipt] if review_receipt else [])
                if not review_passed:
                    for row in rows:row.state="awaiting_review"
                    return {"artifact_role": artifact_role, "state": "awaiting_review",
                        "output_disposition": "candidate", "items": [
                            self.job_artifact_view(row).model_dump(mode="json") for row in rows]}
                for row in rows:
                    row.state="reviewed"
                    row.output_disposition = row.requested_disposition
                    if row.output_disposition in {OutputDisposition.WORKSPACE.value,
                                                  OutputDisposition.DELIVERY.value}:
                        project_path = f"artifacts/jobs/{job.id}/{row.filename}"
                        await self._register_project_file_locked(db, job.thread_id,
                            path=project_path, source_type="artifact", source_turn_id=job.origin_turn_id,
                            sha256=row.sha256, size=row.size, mime=row.mime, object_ref=row.object_ref)
                await db.flush()
                return {"artifact_role": artifact_role, "state": "published",
                    "output_disposition": contract.output_disposition.value,
                    "manifest": {"count": len(rows), "total_bytes": sum(row.size for row in rows),
                        "items": [{"item_key": row.item_key, "position": row.position,
                            "filename": row.filename, "sha256": row.sha256,
                            "size": row.size, "mime": row.mime} for row in rows]},
                    "items": [self.job_artifact_view(row).model_dump(mode="json") for row in rows]}
