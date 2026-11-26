"""Views persistence; transaction boundaries are preserved."""
from __future__ import annotations
import json
from sandbox_harness.common.contracts.domain import (ThreadView, ThreadControlState, ThreadControlView, PauseParticipantState, PauseParticipantView, TurnInputView, TurnView, now)
from sandbox_harness.common.contracts.job_contracts import (JobAttemptReceipt, JobGroupDraftState, JobGroupDraftView, JobGroupState, JobGroupView, JobKind, JobOutboxView, JobState, JobView, BudgetAccountView, BudgetLedgerView, BudgetReservationView)
from sandbox_harness.common.contracts.public_errors import public_turn_failure
from .models import (BudgetAccountRow, BudgetLedgerRow, BudgetReservationRow, JobAttemptRow, JobGroupDraftRow, JobGroupRow, JobOutboxRow, JobRow, PauseParticipantRow, ThreadControlRow, ThreadRow, TurnInputRow, TurnRow, utc_datetime)


class ViewsTransactions:
    @staticmethod
    def job_group_draft_view(row: JobGroupDraftRow) -> JobGroupDraftView:
        state=(JobGroupDraftState.EXPIRED.value if row.state in {
            JobGroupDraftState.DRAFT.value,JobGroupDraftState.AWAITING_CONFIRMATION.value}
            and utc_datetime(row.expires_at)<=now() else row.state)
        return JobGroupDraftView(
            id=row.id, owner_id=row.owner_id, thread_id=row.thread_id,
            origin_turn_id=row.origin_turn_id, idempotency_key=row.idempotency_key,
            request_digest=row.request_digest, state=JobGroupDraftState(state),
            requires_confirmation=row.requires_confirmation,
            authorization_gaps=tuple(json.loads(row.authorization_gaps_json)),
            expires_at=utc_datetime(row.expires_at), confirmed_group_id=row.confirmed_group_id,
            created_at=utc_datetime(row.created_at), updated_at=utc_datetime(row.updated_at),
        )


    @staticmethod
    def job_group_view(row: JobGroupRow) -> JobGroupView:
        return JobGroupView(
            id=row.id, owner_id=row.owner_id, thread_id=row.thread_id,
            origin_turn_id=row.origin_turn_id, draft_id=row.draft_id or "",
            idempotency_key=row.idempotency_key, request_digest=row.request_digest,
            state=JobGroupState(row.state), member_count=row.member_count,
            suppress_continuation=row.suppress_continuation,
            terminal_at=utc_datetime(row.terminal_at) if row.terminal_at else None,
            continued_at=utc_datetime(row.continued_at) if row.continued_at else None,
            created_at=utc_datetime(row.created_at), updated_at=utc_datetime(row.updated_at),
        )


    @staticmethod
    def job_view(row: JobRow) -> JobView:
        return JobView(
            id=row.id, group_id=row.group_id, local_job_id=row.local_job_id,
            owner_id=row.owner_id, thread_id=row.thread_id, origin_turn_id=row.origin_turn_id,
            kind=JobKind(row.kind), adapter=row.adapter, idempotency_key=row.idempotency_key,
            request_digest=row.request_digest, state=JobState(row.state), generation=row.generation,
            cancel_requested=row.cancel_requested, replaces_job_id=row.replaces_job_id,
            reconciles_job_id=row.reconciles_job_id, result=json.loads(row.result_json),
            terminal_at=utc_datetime(row.terminal_at) if row.terminal_at else None,
            created_at=utc_datetime(row.created_at), updated_at=utc_datetime(row.updated_at),
        )


    @staticmethod
    def job_attempt_view(row: JobAttemptRow) -> JobAttemptReceipt:
        return JobAttemptReceipt(
            id=row.id, job_id=row.job_id, generation=row.generation, worker_id=row.worker_id,
            node_id=row.node_id, executor_kind=row.executor_kind,retry_kind=row.retry_kind,
            request_digest=row.request_digest,
            accepted_at=utc_datetime(row.accepted_at) if row.accepted_at else None,
            external_execution_id=row.external_execution_id,
            acceptance_receipt=json.loads(row.acceptance_receipt_json),
            started_at=utc_datetime(row.started_at),
            finished_at=utc_datetime(row.finished_at) if row.finished_at else None,
            outcome=row.outcome, error_code=row.error_code, error_detail=row.error_detail,
        )


    @staticmethod
    def job_outbox_view(row: JobOutboxRow) -> JobOutboxView:
        return JobOutboxView(
            id=row.id, group_id=row.group_id, thread_id=row.thread_id, state=row.state,
            result_digest=row.result_digest, continuation_turn_id=row.continuation_turn_id,
            created_at=utc_datetime(row.created_at), updated_at=utc_datetime(row.updated_at),
        )


    @staticmethod
    def budget_account_view(row: BudgetAccountRow) -> BudgetAccountView:
        return BudgetAccountView(
            id=row.id, owner_id=row.owner_id, thread_id=row.thread_id,
            budget_key=row.budget_key, limit=row.limit, reserved=row.reserved,
            consumed=row.consumed, created_at=utc_datetime(row.created_at),
            updated_at=utc_datetime(row.updated_at),
        )


    @staticmethod
    def budget_reservation_view(row: BudgetReservationRow) -> BudgetReservationView:
        return BudgetReservationView(
            id=row.id, account_id=row.account_id, group_id=row.group_id,
            authorization_snapshot_id=row.authorization_snapshot_id,
            budget_key=row.budget_key, reserved=row.reserved, consumed=row.consumed,
            released=row.released, state=row.state, created_at=utc_datetime(row.created_at),
            updated_at=utc_datetime(row.updated_at),
        )


    @staticmethod
    def budget_ledger_view(row: BudgetLedgerRow) -> BudgetLedgerView:
        return BudgetLedgerView(
            id=row.id, reservation_id=row.reservation_id, group_id=row.group_id,
            job_id=row.job_id, action=row.action, amount=row.amount,
            idempotency_key=row.idempotency_key, detail=json.loads(row.detail_json),
            created_at=utc_datetime(row.created_at),
        )


    @staticmethod
    def thread_view(row: ThreadRow) -> ThreadView:
        return ThreadView.model_validate({key: getattr(row, key) for key in ThreadView.model_fields})


    @staticmethod
    def thread_control_view(row: ThreadControlRow) -> ThreadControlView:
        return ThreadControlView(thread_id=row.thread_id, state=ThreadControlState(row.state),
            generation=row.generation, operation_id=row.operation_id, degraded=row.degraded,
            detail=json.loads(row.detail_json),
            requested_at=utc_datetime(row.requested_at) if row.requested_at else None,
            completed_at=utc_datetime(row.completed_at) if row.completed_at else None,
            updated_at=utc_datetime(row.updated_at))


    @staticmethod
    def pause_participant_view(row: PauseParticipantRow) -> PauseParticipantView:
        return PauseParticipantView(id=row.id, thread_id=row.thread_id,
            generation=row.generation, kind=row.kind, participant_id=row.participant_id,
            state=PauseParticipantState(row.state), checkpoint_ref=row.checkpoint_ref,
            native_thread_id=row.native_thread_id,
            external_execution_id=row.external_execution_id,
            detail=json.loads(row.detail_json), created_at=utc_datetime(row.created_at),
            updated_at=utc_datetime(row.updated_at))


    @staticmethod
    def turn_view(row: TurnRow) -> TurnView:
        failure=public_turn_failure(code=row.error_code or "",detail=row.error_detail or "",
            turn_id=row.id)
        stage={"QUEUED":"queued","RESTORING":"sandbox_wait","STARTING":"sandbox_wait",
            "CHECKPOINTING":"collection","CLEANING":"collection","FAILED":"blocked",
            "PAUSED":"blocked","CANCELLED":"cancelled","COMPLETED":"completed"}.get(row.state)
        if not stage:stage="recovery" if row.recovery_generation>1 else "harness"
        return TurnView.model_validate({
            **{key: (row.public_message if key == "message" else row.message
                if key == "runtime_message" else getattr(row, key))
                for key in TurnView.model_fields if key not in {
                    "attachments","error_code","error_message","error_action","troubleshooting_id",
                    "execution_stage"}},
            **failure,
            "execution_stage":stage,
            "attachments": json.loads(row.attachments_json),
        })


    @staticmethod
    def input_view(row: TurnInputRow) -> TurnInputView:
        return TurnInputView.model_validate({
            **{key: getattr(row, key) for key in TurnInputView.model_fields if key != "attachments"},
            "attachments": json.loads(row.attachments_json),
        })
