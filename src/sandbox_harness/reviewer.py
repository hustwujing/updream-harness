from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .job_adapters import AdapterStatus, AdapterSubmission, JobExecutionContext
from .job_contracts import JobArtifactInput, JobGroupCreate, JobKind, JobRequest, JobState


class ReviewTrigger(StrEnum):
    USER = "user"
    PRIMARY = "primary"
    PUBLICATION_POLICY = "publication_policy"
    CAPABILITY_POLICY = "capability_policy"


class ReviewVerdictKind(StrEnum):
    PASS = "PASS"
    REVISE = "REVISE"
    EVIDENCE_REQUIRED = "EVIDENCE_REQUIRED"
    BLOCKED = "BLOCKED"


class ReviewCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    artifact_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    artifact_role: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    mime: str
    validation_receipts: tuple[dict[str, Any], ...] = ()


class ReviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    thread_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    origin_turn_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    trigger: ReviewTrigger | None = None
    required: bool = False
    harness: str = "codex"
    model: str = ""
    skill_ids: tuple[str, ...] = ()
    candidates: tuple[ReviewCandidate, ...] = Field(min_length=1, max_length=100)
    rubric: str = Field(min_length=1, max_length=20000)
    idempotency_key: str = Field(min_length=8, max_length=160)


class ReviewSubmission(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    artifact_ids: tuple[str,...] = Field(min_length=1,max_length=100)
    artifact_roles: dict[str,str] = Field(default_factory=dict)
    required: bool = False
    harness: str = Field(pattern=r"^(codex|claude|deepseek)$")
    model: str = Field(min_length=1,max_length=160)
    skill_ids: tuple[str,...] = ()
    rubric: str = Field(min_length=1,max_length=20000)
    idempotency_key: str = Field(min_length=8,max_length=160)


class ReviewVerdict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    verdict: ReviewVerdictKind
    artifact_digests: tuple[str, ...]
    summary: str = Field(default="", max_length=4000)
    findings: tuple[dict[str, Any], ...] = ()
    missing_evidence: tuple[str, ...] = ()


class ReviewerCoordinator:
    def __init__(self, job_coordinator):self.job_coordinator=job_coordinator

    @staticmethod
    def _validators_passed(value: ReviewRequest) -> bool:
        return all(all(receipt.get('status')=='pass' for receipt in candidate.validation_receipts)
            for candidate in value.candidates)

    @staticmethod
    def _runtime_requirements(candidates:tuple[ReviewCandidate,...])->dict[str,list[str]]:
        mimes={item.mime.split(';',1)[0].lower() for item in candidates};commands=set();modules=set()
        if any(mime.startswith(('image/','audio/','video/')) for mime in mimes):
            commands.update(('ffmpeg','ffprobe','python3'));modules.update(('PIL','numpy'))
        if any(mime=='application/pdf' or 'officedocument' in mime for mime in mimes):
            commands.update(('libreoffice','pandoc','pdftoppm','pdftotext','qpdf','python3'))
            modules.update(('PIL','pdfplumber','pypdf'))
        if any(mime in {'text/html','application/xhtml+xml'} for mime in mimes):
            commands.update(('chromium','node','python3'));modules.add('playwright')
        return {'commands':sorted(commands),'python_modules':sorted(modules)}

    async def prepare(self,owner_id:str,value:ReviewRequest):
        if value.trigger is None:
            return None
        if not self._validators_passed(value):
            raise RuntimeError('deterministic_validation_not_passed')
        digests=[candidate.sha256 for candidate in value.candidates]
        request=JobRequest(local_job_id='review',kind=JobKind.HARNESS,adapter='reviewer-harness',
            descriptor_id='review.submit',descriptor_version='1',
            descriptor_digest=__import__('hashlib').sha256(b'review.submit:1').hexdigest(),
            idempotency_key=value.idempotency_key+'-job',
            parameters={'harness':value.harness,'model':value.model,'objective':value.rubric,
                'review_context':{'artifact_digests':digests,'required':value.required,
                    'trigger':value.trigger.value}},
            input_artifacts=tuple(JobArtifactInput(artifact_id=candidate.artifact_id,
                artifact_role=candidate.artifact_role) for candidate in value.candidates),
            child_profile_id='reviewer-readonly',skill_ids=value.skill_ids,
            network_origins=(),resource_requirements={'harness':value.harness,'cpu':1,
                'memory_mb':1024,'disk_mb':1024,**self._runtime_requirements(value.candidates)},
            budget_requirements={})
        return await self.job_coordinator.prepare_group(owner_id,JobGroupCreate(
            thread_id=value.thread_id,origin_turn_id=value.origin_turn_id,
            idempotency_key=value.idempotency_key,jobs=[request]))

    async def prepare_submission(self,owner_id:str,thread_id:str,turn_id:str,value:ReviewSubmission):
        candidates=[]
        for identity in value.artifact_ids:
            artifact=await self.job_coordinator.store.get_artifact(identity)
            if not artifact:raise KeyError('review_artifact_not_found')
            candidates.append(ReviewCandidate(artifact_id=artifact.id,
                artifact_role=value.artifact_roles.get(identity,'candidate'),sha256=artifact.sha256,
                mime=artifact.mime,validation_receipts=()))
        return await self.prepare(owner_id,ReviewRequest(thread_id=thread_id,origin_turn_id=turn_id,
            trigger=ReviewTrigger.PRIMARY,required=value.required,harness=value.harness,model=value.model,
            skill_ids=value.skill_ids,candidates=tuple(candidates),rubric=value.rubric,
            idempotency_key=value.idempotency_key))


class ReviewerJobAdapter:
    def __init__(self,delegate):self.delegate=delegate
    async def submit(self,context:JobExecutionContext)->AdapterSubmission:return await self.delegate.submit(context)
    async def status(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        return await self.delegate.status(context,external_execution_id)
    async def cancel(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        return await self.delegate.cancel(context,external_execution_id)
    async def reconcile(self,context:JobExecutionContext,external_execution_id:str)->AdapterStatus:
        return await self.delegate.reconcile(context,external_execution_id)
    async def cleanup(self,context:JobExecutionContext,external_execution_id:str)->None:
        await self.delegate.cleanup(context,external_execution_id)
    async def cleanup_external(self,context:JobExecutionContext,external_execution_id:str)->None:
        await self.delegate.cleanup_external(context,external_execution_id)
    async def release_capacity(self,context:JobExecutionContext)->None:
        await self.delegate.release_capacity(context)
    async def collect(self,context:JobExecutionContext,external_execution_id:str,status:AdapterStatus)->dict:
        result=await self.delegate.collect(context,external_execution_id,status)
        if status.state!=JobState.SUCCEEDED:return result
        verdict=ReviewVerdict.model_validate(result.get('review',result))
        expected=tuple(context.request.parameters['review_context']['artifact_digests'])
        if verdict.artifact_digests!=expected:raise ValueError('review_artifact_digest_mismatch')
        return {'review':verdict.model_dump(mode='json')}
