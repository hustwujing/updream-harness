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

    @staticmethod
    def _bounded_user_context(value:dict,max_bytes:int=64*1024)->str:
        if not value:return '{}'
        def truncate(text:str,limit:int)->str:
            raw=text.encode()
            return text if len(raw)<=limit else raw[:limit].decode(errors='ignore')+'...[truncated]'
        bounded={'source_turn_id':value.get('source_turn_id',''),
            'initial_message':truncate(str(value.get('initial_message','')),24*1024),
            'applied_inputs':[]}
        for row in reversed(value.get('applied_inputs',[])):
            candidate={**row,'message':truncate(str(row.get('message','')),16*1024)}
            updated={**bounded,'applied_inputs':[candidate,*bounded['applied_inputs']]}
            encoded=json.dumps(updated,ensure_ascii=False,sort_keys=True,separators=(',',':'))
            if len(encoded.encode())>max_bytes:break
            bounded=updated
        return json.dumps(bounded,ensure_ascii=False,sort_keys=True,separators=(',',':'))

    async def run_once(self) -> int:
        batch = await self.store.claim_job_outbox_batch(
            self.owner, lease_seconds=self.lease_seconds, max_groups=self.max_groups,
            max_jobs=self.max_jobs, max_bytes=self.max_bytes)
        if not batch:
            return 0
        primary=[];nested=[]
        for item in batch:
            parent_job_id=await self.store.child_parent_for_group(item['group_id'])
            (nested if parent_job_id else primary).append((item,parent_job_id))
        for item,parent_job_id in nested:
            await self.store.ready_child_continuation(parent_job_id,item['group_id'],
                item['outbox_id'],{'group_id':item['group_id'],
                    'result_digest':item['result_digest'],'results':item['results']})
            await self.store.finish_job_outbox_batch([item['outbox_id']],self.owner,
                continuation_turn_id=parent_job_id,delivered=True)
            event('job.child_continuation.ready',thread_id=item['thread_id'],
                job_id=parent_job_id,group_id=item['group_id'])
        batch=[item for item,_ in primary]
        if not batch:return len(nested)
        outbox_ids = sorted(item["outbox_id"] for item in batch)
        digest = batch[0]["batch_digest"]
        facts = [{"group_id": item["group_id"], "result_digest": item["result_digest"],
            "results": item["results"]} for item in batch]
        thread_id = batch[0]["thread_id"]
        user_context=self._bounded_user_context(
            await self.store.authoritative_user_context(thread_id))
        message = ("[authoritative_user_inputs] These program-recorded inputs are authoritative. "
            "Never deny an applied input, restore an older requirement, or let a late result override it. "
            +user_context+"\n\n[job_groups_completed] Factual terminal results from durable parallel groups. "
            "Do not assume they still satisfy later user intent; inspect and decide what to do next. "
            "A row with result_available=true intentionally omits the full durable result to keep this "
            "continuation bounded. Now that continuation has arrived, fetch details once with "
            "`sandbox-job job --job-id JOB_ID` or fetch the group with "
            "`sandbox-job group --group-id GROUP_ID` before deciding; never guess omitted output. "
            "For a reviewed candidate that already satisfies the request, use "
            "`sandbox-artifact fetch --artifact-id ID --output PATH`, verify it, then copy the "
            "intended final file into `/workspace/outputs`; do not rerun a successful Job only "
            "to change publication disposition. "
            + json.dumps(facts, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
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
            return len(batch)+len(nested)
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
