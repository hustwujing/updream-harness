from types import SimpleNamespace

from sandbox_harness.common.contracts.domain import TurnState
from sandbox_harness.common.contracts.job_contracts import JobKind, JobState
from sandbox_harness.common.db.api_repositories import ApiRepositories


def turn(state=TurnState.RUNNING,generation=1):
    return SimpleNamespace(id="turn",state=state,recovery_generation=generation)


def test_execution_stage_projects_authoritative_provider_and_collection_states():
    provider_job=SimpleNamespace(origin_turn_id="turn",state=JobState.WAITING_EXTERNAL,
        kind=JobKind.PROVIDER)
    media=SimpleNamespace(turn_id="turn",status="running",phase="provider_polling")
    collecting=SimpleNamespace(turn_id="turn",status="running",phase="download_primary")
    assert ApiRepositories.execution_stage(turn(),[provider_job],[],[])=="provider"
    assert ApiRepositories.execution_stage(turn(),[],[media],[])=="provider"
    assert ApiRepositories.execution_stage(turn(),[],[collecting],[])=="collection"


def test_execution_stage_prioritizes_recovery_and_terminal_authority():
    assert ApiRepositories.execution_stage(turn(generation=2),[],[],[])=="recovery"
    assert ApiRepositories.execution_stage(turn(TurnState.RESTORING),[],[],[])=="sandbox_wait"
    assert ApiRepositories.execution_stage(turn(TurnState.CHECKPOINTING),[],[],[])=="collection"
    assert ApiRepositories.execution_stage(turn(TurnState.FAILED),[],[],[])=="blocked"
    assert ApiRepositories.execution_stage(turn(TurnState.COMPLETED),[],[],[])=="completed"
