from __future__ import annotations

import asyncio
import hashlib
import json
import uuid

from .domain import TurnCreate
from .observability import event


class ContinuationDispatcher:
    def __init__(self, store, broker, *, max_groups: int = 32, max_jobs: int = 100,
                 max_bytes: int = 1024 * 1024, lease_seconds: int = 60,
                 owner: str = "", after_submit=None):
        self.store = store
        self.broker = broker
        self.max_groups = max_groups
        self.max_jobs = max_jobs
        self.max_bytes = max_bytes
        self.lease_seconds = lease_seconds
        self.owner = owner or "job-continuation-" + uuid.uuid4().hex
        self.after_submit = after_submit
        self.stop_event = asyncio.Event()

    async def run_once(self) -> int:
        batch = await self.store.claim_job_outbox_batch(
            self.owner, lease_seconds=self.lease_seconds, max_groups=self.max_groups,
            max_jobs=self.max_jobs, max_bytes=self.max_bytes)
        if not batch:
            return 0
        outbox_ids = sorted(item["outbox_id"] for item in batch)
        digest = batch[0]["batch_digest"]
        facts = [{"group_id": item["group_id"], "result_digest": item["result_digest"],
            "results": item["results"]} for item in batch]
        message = ("[job_groups_completed] Factual terminal results from durable parallel groups. "
            "Do not assume they still satisfy later user intent; inspect and decide what to do next. "
            + json.dumps(facts, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        thread_id = batch[0]["thread_id"]
        try:
            group = await self.store.get_job_group(batch[0]["group_id"])
            origin = await self.store.get_turn(group.origin_turn_id) if group else None
            turn = await self.broker.submit(thread_id, TurnCreate(
                message=message, idempotency_key="job-continuation-" + digest,
                delivery_mode="queue"), network_access=(origin.network_access if origin else "restricted"),
                internal=True)
            if self.after_submit:
                await self.after_submit(turn, batch)
            await self.store.finish_job_outbox_batch(outbox_ids, self.owner,
                continuation_turn_id=turn.id, delivered=True)
            event("job.continuation.created", thread_id=thread_id, turn_id=turn.id,
                group_count=len(batch), job_count=sum(len(item["results"]) for item in batch))
            return len(batch)
        except Exception as exc:
            permanent=isinstance(exc,RuntimeError) and str(exc) in {
                "thread_archived","active_jobs_prevent_archival"}
            await self.store.finish_job_outbox_batch(outbox_ids, self.owner,
                continuation_turn_id="", delivered=False, error=type(exc).__name__,
                permanent=permanent)
            raise

    async def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                count = await self.run_once()
            except Exception as exc:
                event("job.continuation.failed", level=40, exception_type=type(exc).__name__)
                count = 0
            if count:
                continue
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=1)
            except asyncio.TimeoutError:
                pass
