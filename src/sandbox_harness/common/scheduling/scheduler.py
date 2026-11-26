from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import select,exists

from sandbox_harness.common.contracts.domain import (identifier, now)
from sandbox_harness.common.contracts.job_contracts import (TERMINAL_JOB_STATES)
from sandbox_harness.common.db.store import (JobRow, JobAttemptRow, JobCleanupRow, NodeRow, ResourceReservationRow, utc_datetime)


class GPUInventory(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str = Field(min_length=1, max_length=100)
    type: str = Field(min_length=1, max_length=100)
    memory_mb: int = Field(ge=1)
    memory_free_mb: int = Field(default=0, ge=0)
    utilization_percent: int = Field(default=0, ge=0, le=100)
    mode: str = Field(default="exclusive", pattern=r"^(exclusive|shared|mig)$")


class NodeProfile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    runtime_profile_id: str
    harness: str = Field(pattern=r"^(codex|claude|deepseek)$")
    image_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    image_ref: str = ""
    warm_slots: int = Field(default=0, ge=0)


class NodeInventory(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    node_id: str = Field(min_length=1, max_length=160)
    generation: int = Field(ge=1)
    cpu_total: int = Field(ge=1)
    memory_mb_total: int = Field(ge=128)
    disk_mb_total: int = Field(ge=128)
    load: float = Field(default=0, ge=0)
    profiles: tuple[NodeProfile, ...] = Field(min_length=1)
    gpus: tuple[GPUInventory, ...] = ()
    cached_bundle_digests: tuple[str, ...] = ()

    @model_validator(mode="after")
    def unique_inventory(self):
        if len({gpu.id for gpu in self.gpus}) != len(self.gpus):
            raise ValueError("Duplicate GPU identity")
        identities=[(value.runtime_profile_id,value.harness) for value in self.profiles]
        if len(set(identities))!=len(identities):raise ValueError("Duplicate Runtime Profile identity")
        return self


class ResourceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    child_profile_id: str
    runtime_profile_id: str
    harness: str = Field(pattern=r"^(codex|claude|deepseek)$")
    image_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    cpu: int = Field(default=1, ge=1)
    memory_mb: int = Field(default=512, ge=1)
    disk_mb: int = Field(default=1024, ge=1)
    gpu_count: int = Field(default=0, ge=0, le=16)
    gpu_type: str = ""
    gpu_mode: str = ""
    gpu_memory_mb: int = Field(default=0, ge=0)
    required_bundle_digests: tuple[str, ...] = ()
    affinity_node_id: str = ""


class ResourceReservation(BaseModel):
    id: str
    job_id: str
    job_generation: int
    node_id: str
    node_generation: int
    child_profile_id: str
    runtime_profile_id: str
    harness: str
    image_digest: str
    image_ref: str = ""
    cpu: int
    memory_mb: int
    disk_mb: int
    gpu_ids: tuple[str, ...]
    state: str
    expires_at: datetime


class Scheduler:
    def __init__(self, store, *, node_ttl_seconds: int = 60, reservation_seconds: int = 120):
        self.store=store;self.node_ttl_seconds=node_ttl_seconds;self.reservation_seconds=reservation_seconds

    @staticmethod
    def _view(row: ResourceReservationRow) -> ResourceReservation:
        return ResourceReservation(id=row.id,job_id=row.job_id,job_generation=row.job_generation,
            node_id=row.node_id,node_generation=row.node_generation,child_profile_id=row.child_profile_id,
            runtime_profile_id=row.runtime_profile_id,harness=row.harness,image_digest=row.image_digest,
            image_ref=row.image_ref,
            cpu=row.cpu,memory_mb=row.memory_mb,disk_mb=row.disk_mb,
            gpu_ids=tuple(json.loads(row.gpu_ids_json)),state=row.state,expires_at=utc_datetime(row.expires_at))

    async def register_node(self, value: NodeInventory) -> None:
        async with self.store.connections.begin() as db:
            row=await db.get(NodeRow,value.node_id,with_for_update=True);timestamp=now()
            if row and value.generation < row.generation:raise RuntimeError("stale_node_generation")
            if row:
                row.generation=value.generation;row.status="ready";row.inventory_json=value.model_dump_json()
                row.heartbeat_at=timestamp;row.updated_at=timestamp
            else:db.add(NodeRow(id=value.node_id,generation=value.generation,status="ready",
                inventory_json=value.model_dump_json(),heartbeat_at=timestamp,created_at=timestamp,updated_at=timestamp))

    async def reap_stale_reservations(self,node_id:str,node_generation:int,
                                      live_sandbox_job_ids:set[str])->list[str]:
        async with self.store.connections.begin() as db:
            node=await db.get(NodeRow,node_id,with_for_update=True)
            if not node or node.generation!=node_generation:raise RuntimeError("stale_node_generation")
            query=(select(ResourceReservationRow).join(JobRow,JobRow.id==ResourceReservationRow.job_id)
                .where(ResourceReservationRow.node_id==node_id,
                    ResourceReservationRow.node_generation!=node_generation,
                    ResourceReservationRow.state.in_(["reserved","running"]),
                    ResourceReservationRow.expires_at<now(),
                    JobRow.state.in_([state.value for state in TERMINAL_JOB_STATES]),
                    ~exists(select(JobAttemptRow.id).where(JobAttemptRow.job_id==JobRow.id,
                        JobAttemptRow.outcome.in_(['SUBMITTING','UNKNOWN']))),
                    ~exists(select(JobCleanupRow.id).where(JobCleanupRow.job_id==JobRow.id,
                        JobCleanupRow.state!='completed',JobCleanupRow.phase=='external_quiescence'))))
            if live_sandbox_job_ids:
                query=query.where(ResourceReservationRow.job_id.not_in(live_sandbox_job_ids))
            rows=list((await db.execute(query.order_by(ResourceReservationRow.created_at)
                .with_for_update(of=ResourceReservationRow))).scalars().all())
            timestamp=now()
            for row in rows:
                row.state="released";row.released_at=timestamp;row.expires_at=timestamp
            return [row.id for row in rows]

    async def reserve(self, job_id: str, job_generation: int,
                      request: ResourceRequest) -> ResourceReservation:
        async with self.store.connections.begin() as db:
            existing=(await db.execute(select(ResourceReservationRow).where(
                ResourceReservationRow.job_id==job_id,
                ResourceReservationRow.job_generation==job_generation).with_for_update())).scalar_one_or_none()
            if existing:
                expected=(request.child_profile_id,request.runtime_profile_id,request.harness,request.image_digest)
                actual=(existing.child_profile_id,existing.runtime_profile_id,existing.harness,existing.image_digest)
                if expected!=actual:raise RuntimeError("resource_reservation_conflict")
                if existing.state in {"reserved","running"}:return self._view(existing)
            nodes=list((await db.execute(select(NodeRow).where(NodeRow.status=="ready",
                NodeRow.heartbeat_at>now()-timedelta(seconds=self.node_ttl_seconds))
                .order_by(NodeRow.id).with_for_update())).scalars().all())
            active=list((await db.execute(select(ResourceReservationRow).where(
                ResourceReservationRow.state.in_(["reserved","running"])
                ).with_for_update())).scalars().all())
            candidates=[]
            for node in nodes:
                inventory=NodeInventory.model_validate_json(node.inventory_json)
                profile=next((value for value in inventory.profiles
                    if value.runtime_profile_id==request.runtime_profile_id
                    and value.harness==request.harness and value.image_digest==request.image_digest),None)
                if not profile:continue
                used=[value for value in active if value.node_id==node.id]
                if (sum(value.cpu for value in used)+request.cpu>inventory.cpu_total
                    or sum(value.memory_mb for value in used)+request.memory_mb>inventory.memory_mb_total
                    or sum(value.disk_mb for value in used)+request.disk_mb>inventory.disk_mb_total):continue
                used_gpu={identity for value in used for identity in json.loads(value.gpu_ids_json)}
                available=[gpu for gpu in inventory.gpus if gpu.id not in used_gpu
                    and (not request.gpu_type or gpu.type==request.gpu_type)
                    and (not request.gpu_mode or gpu.mode==request.gpu_mode)
                    and (gpu.memory_free_mb or gpu.memory_mb)>=request.gpu_memory_mb]
                available.sort(key=lambda gpu:(gpu.utilization_percent,
                    -(gpu.memory_free_mb or gpu.memory_mb),gpu.id))
                if len(available)<request.gpu_count:continue
                gpu_ids=tuple(gpu.id for gpu in available[:request.gpu_count])
                cached=len(set(request.required_bundle_digests)&set(inventory.cached_bundle_digests))
                score=(0 if node.id==request.affinity_node_id and request.affinity_node_id else 1,
                    0 if profile.warm_slots>0 else 1,-cached,inventory.load,node.id)
                candidates.append((score,node,inventory,gpu_ids))
            if not candidates:raise RuntimeError("no_compatible_node_capacity")
            _,node,inventory,gpu_ids=min(candidates,key=lambda value:value[0]);timestamp=now()
            selected_profile=next(value for value in inventory.profiles
                if value.runtime_profile_id==request.runtime_profile_id and value.harness==request.harness)
            if existing:
                row=existing
                row.node_id=node.id;row.node_generation=node.generation
                row.image_ref=selected_profile.image_ref
                row.cpu=request.cpu;row.memory_mb=request.memory_mb;row.disk_mb=request.disk_mb
                row.gpu_ids_json=json.dumps(gpu_ids);row.state="reserved"
                row.expires_at=timestamp+timedelta(seconds=self.reservation_seconds)
                row.released_at=None
            else:
                row=ResourceReservationRow(id=identifier(),job_id=job_id,job_generation=job_generation,
                    node_id=node.id,node_generation=node.generation,child_profile_id=request.child_profile_id,
                    runtime_profile_id=request.runtime_profile_id,harness=request.harness,
                    image_digest=request.image_digest,image_ref=selected_profile.image_ref,
                    cpu=request.cpu,memory_mb=request.memory_mb,
                    disk_mb=request.disk_mb,gpu_ids_json=json.dumps(gpu_ids),state="reserved",
                    expires_at=timestamp+timedelta(seconds=self.reservation_seconds),created_at=timestamp,released_at=None)
                db.add(row)
            await db.flush();return self._view(row)

    async def active_reservation(self,job_id:str)->ResourceReservation|None:
        async with self.store.connections() as db:
            row=(await db.execute(select(ResourceReservationRow).where(
                ResourceReservationRow.job_id==job_id,
                ResourceReservationRow.state.in_(["reserved","running"])).order_by(
                ResourceReservationRow.job_generation.desc()).limit(1))).scalar_one_or_none()
            return self._view(row) if row else None

    async def mark_running(self,reservation_id: str,node_id: str,node_generation: int)->ResourceReservation:
        async with self.store.connections.begin() as db:
            row=await db.get(ResourceReservationRow,reservation_id,with_for_update=True)
            if not row or row.node_id!=node_id or row.node_generation!=node_generation:
                raise RuntimeError("stale_resource_reservation")
            row.state="running";row.expires_at=now()+timedelta(seconds=self.reservation_seconds)
            await db.flush();return self._view(row)

    async def renew(self,reservation_id:str,node_id:str,node_generation:int)->ResourceReservation:
        async with self.store.connections.begin() as db:
            row=await db.get(ResourceReservationRow,reservation_id,with_for_update=True)
            if (not row or row.node_id!=node_id or row.node_generation!=node_generation
                    or row.state not in {'reserved','running'}):
                raise RuntimeError('stale_resource_reservation')
            row.expires_at=now()+timedelta(seconds=self.reservation_seconds)
            await db.flush();return self._view(row)

    @asynccontextmanager
    async def hold(self,reservation_id:str,node_id:str,node_generation:int):
        await self.renew(reservation_id,node_id,node_generation)
        owner=asyncio.current_task();lost=asyncio.Event()
        async def heartbeat():
            while True:
                await asyncio.sleep(max(.1,self.reservation_seconds/3))
                try:await self.renew(reservation_id,node_id,node_generation)
                except Exception:
                    lost.set();owner.cancel();return
        task=asyncio.create_task(heartbeat(),name="resource-heartbeat-"+reservation_id)
        try:
            yield
        except asyncio.CancelledError as exc:
            if lost.is_set():raise RuntimeError("resource_reservation_lost") from exc
            raise
        finally:
            task.cancel();await asyncio.gather(task,return_exceptions=True)

    async def release(self,reservation_id: str,node_id: str,node_generation: int)->bool:
        async with self.store.connections.begin() as db:
            row=await db.get(ResourceReservationRow,reservation_id,with_for_update=True)
            if not row:return False
            if row.node_id!=node_id or row.node_generation!=node_generation:
                raise RuntimeError("stale_resource_reservation")
            if row.state=="released":return False
            row.state="released";row.released_at=now();row.expires_at=now();return True

    async def reservation_released(self,reservation_id:str,node_id:str,node_generation:int,*,
                                   job_id:str,job_generation:int)->bool:
        """Check exact reservation identity for an idempotent cleanup, without renewing it."""
        async with self.store.connections() as db:
            row=await db.get(ResourceReservationRow,reservation_id)
            if (not row or row.node_id!=node_id or row.node_generation!=node_generation
                    or row.job_id!=job_id or row.job_generation!=job_generation):
                raise RuntimeError('stale_resource_reservation')
            return row.state=='released'
