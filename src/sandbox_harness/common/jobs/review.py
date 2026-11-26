from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from sandbox_harness.common.contracts.job_contracts import (JobArtifactInput, JobGroupCreate, JobKind, JobRequest, JobState, REVIEWER_HARNESS_DESCRIPTOR)


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
    size: int = Field(default=0, ge=0)
    validation_receipts: tuple[dict[str, Any], ...] = ()
    validation_contract: str = "caller_supplied"


class ReviewTarget(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    source_job_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    source_generation: int = Field(ge=0)
    artifact_role: str
    artifact_ids: tuple[str,...]
    artifact_digests: tuple[str,...]
    review_policy: str
    requested_disposition: str


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
    target: ReviewTarget | None = None


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
        total_bytes=sum(candidate.size for candidate in value.candidates)
        if total_bytes>8*1024*1024*1024:raise ValueError("review_candidate_total_too_large")
        disk_mb=max(1024,(total_bytes+1024*1024-1)//(1024*1024)+512)
        adapter,descriptor_id,descriptor_version,descriptor_digest=REVIEWER_HARNESS_DESCRIPTOR
        request=JobRequest(local_job_id='review',kind=JobKind.HARNESS,adapter=adapter,
            descriptor_id=descriptor_id,descriptor_version=descriptor_version,
            descriptor_digest=descriptor_digest,
            idempotency_key=value.idempotency_key+'-job',
            parameters={'harness':value.harness,'model':value.model,'objective':value.rubric,
                'review_context':{'artifact_digests':digests,'required':value.required,
                    'trigger':value.trigger.value,
                    'candidates':[candidate.model_dump(mode='json') for candidate in value.candidates],
                    'target':value.target.model_dump(mode='json') if value.target else None}},
            input_artifacts=tuple(JobArtifactInput(artifact_id=candidate.artifact_id,
                artifact_role=candidate.artifact_role) for candidate in value.candidates),
            child_profile_id='reviewer-readonly',skill_ids=value.skill_ids,
            network_origins=(),resource_requirements={'harness':value.harness,'cpu':1,
                'memory_mb':1024,'disk_mb':disk_mb,**self._runtime_requirements(value.candidates)},
            budget_requirements={})
        prepared=await self.job_coordinator.prepare_group(owner_id,JobGroupCreate(
            thread_id=value.thread_id,origin_turn_id=value.origin_turn_id,
            idempotency_key=value.idempotency_key,jobs=[request]))
        return prepared

    async def prepare_submission(self,owner_id:str,thread_id:str,turn_id:str,value:ReviewSubmission,
                                 trigger:ReviewTrigger=ReviewTrigger.PRIMARY):
        resolved=await self.job_coordinator.store.resolve_review_candidates(owner_id,thread_id,
            list(value.artifact_ids),allow_mixed=not value.required);target=resolved['target']
        if target and not target.pop('gate_eligible',False):
            if value.required or target.get('review_policy')=='required':
                raise RuntimeError('required_review_target_already_published')
            target=None
        if value.required and not target:
            raise RuntimeError('required_review_requires_unpublished_candidate_collection')
        candidates=[ReviewCandidate.model_validate(item) for item in resolved['candidates']]
        required=value.required or bool(target and target.get('review_policy')=='required')
        return await self.prepare(owner_id,ReviewRequest(thread_id=thread_id,origin_turn_id=turn_id,
            trigger=trigger,required=required,harness=value.harness,model=value.model,
            skill_ids=value.skill_ids,candidates=tuple(candidates),rubric=value.rubric,
            idempotency_key=value.idempotency_key,target=ReviewTarget.model_validate(target) if target else None))


