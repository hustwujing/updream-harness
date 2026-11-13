from sandbox_harness.catalog import HarnessCatalog
from datetime import timedelta

from sandbox_harness.domain import EventKind, Harness, ItemType, RuntimeEvent, ThreadCreate, TurnCreate, TurnInputCreate, TurnState, now
from sandbox_harness.store import Store, ThreadRow, TurnRow, valid_project_file_path
import pytest


async def test_internal_turn_is_auditable_but_marked_hidden_from_user_chat(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "internal-turn.db")); await store.initialize()
    try:
        catalog = HarnessCatalog(); profile = catalog.resolve("codex", "gpt-5.6", "standard"); owner = "a" * 64
        thread = await store.create_thread(ThreadCreate(harness=Harness.CODEX, model=profile.model,
            reasoning_level="standard", title="Internal"), profile, owner)
        value = TurnCreate(message="[media_group_completed] payload",
            idempotency_key="media-continuation-test", delivery_mode="queue")
        turn, created, _ = await store.create_turn(thread.id, value, profile, owner, internal=True)
        assert created and turn.internal
        item = (await store.items_after(thread.id))[0]
        assert item.type == ItemType.USER_MESSAGE and item.payload["internal"] is True
        duplicate, created, _ = await store.create_turn(thread.id, value, profile, owner, internal=True)
        assert not created and duplicate.id == turn.id
        with pytest.raises(RuntimeError, match="idempotency_conflict"):
            await store.create_turn(thread.id, value, profile, owner, internal=False)
    finally:
        await store.close()


async def test_thread_turn_items_and_idempotency(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "store.db"))
    await store.initialize()
    try:
        catalog = HarnessCatalog()
        profile = catalog.resolve("claude", "claude-4.6-sonnet", "deep")
        owner = "a" * 64
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="deep", title="Thread"), profile, owner)
        value = TurnCreate(message="hello", idempotency_key="idempotency-1")
        turn, created, start = await store.create_turn(thread.id, value, profile, owner,
            network_access="public")
        assert created and start and turn.reasoning_effort == "high" and turn.network_access == "public"
        duplicate, created, _ = await store.create_turn(thread.id, value, profile, owner,
            network_access="public")
        assert not created and duplicate.id == turn.id
        with pytest.raises(RuntimeError, match="idempotency_conflict"):
            await store.create_turn(thread.id, value, profile, owner, network_access="restricted")
        with pytest.raises(RuntimeError, match="idempotency_conflict"):
            await store.create_turn(thread.id,
                TurnCreate(message="different", idempotency_key="idempotency-1"), profile, owner)
        appended, created = await store.create_turn_input(turn.id,
            TurnInputCreate(message="follow up", idempotency_key="input-key-1"), owner)
        pending_item = (await store.items_after(thread.id))[1]
        assert pending_item.payload["input_status"] == "pending"
        assert pending_item.payload["delivery_mode"] == "pending"
        repeated, created_again = await store.create_turn_input(turn.id,
            TurnInputCreate(message="follow up", idempotency_key="input-key-1"), owner)
        assert created and not created_again and repeated.id == appended.id
        with pytest.raises(RuntimeError, match="idempotency_conflict"):
            await store.create_turn_input(turn.id,
                TurnInputCreate(message="different", idempotency_key="input-key-1"), owner)
        claimed = await store.next_pending_input(turn.id)
        assert claimed.id == appended.id and claimed.status == "pending" and claimed.delivery_mode == "pending"
        await store.set_input_status(claimed.id, "delivered", "native_stream")
        delivered_item = (await store.items_after(thread.id))[1]
        assert delivered_item.status == "in_progress"
        assert delivered_item.payload["input_status"] == "delivered"
        assert delivered_item.payload["delivery_mode"] == "native_stream"
        await store.set_input_status(claimed.id, "applied")
        assert (await store.list_turn_inputs(turn.id))[0].status == "applied"
        applied_item = (await store.items_after(thread.id))[1]
        assert applied_item.status == "completed" and applied_item.payload["input_status"] == "applied"
        delivered_event, input_event = (await store.events_after(turn.id, 0))[:2]
        assert delivered_event.kind == EventKind.ITEM_UPDATED
        assert delivered_event.payload["input_status"] == "delivered"
        assert input_event.kind == EventKind.ITEM_UPDATED
        assert input_event.payload == {"item_id": input_event.item_id,
            "status":"completed", "input_id":claimed.id,
            "input_status":"applied", "delivery_mode":""}
        agent = await store.append_agent_delta(thread.id, turn.id, "hel")
        agent = await store.append_agent_delta(thread.id, turn.id, "lo")
        assert agent.summary == "hello"
        event = await store.append_event(turn.id, EventKind.TEXT_DELTA, {"text": "lo"}, item_id=agent.id)
        assert (await store.events_after(turn.id, input_event.sequence))[0].sequence == event.sequence
        tool = await store.append_item(thread.id, turn.id, ItemType.TOOL, status="in_progress",
            summary="Bash", payload={"tool_use_id": "tool-1"})
        finished = await store.finish_tool_item(turn.id, "tool-1", {
            "tool_use_id": "tool-1", "name": "Bash", "result": "ok",
        })
        assert finished.id == tool.id and finished.status == "completed"
        items = await store.items_after(thread.id)
        assert [item.type.value for item in items] == ["user_message", "user_message", "agent_message", "tool"]
        await store.set_turn_state(turn.id, TurnState.COMPLETED)
        assert (await store.get_turn(turn.id)).state == TurnState.COMPLETED
        lingering = await store.append_item(thread.id, turn.id, ItemType.PLAN, status="in_progress",
            summary="late step")
        await store.set_turn_state(turn.id, TurnState.COMPLETED)
        assert next(item for item in await store.items_after(thread.id) if item.id == lingering.id).status == "interrupted"
    finally:
        await store.close()


async def test_background_job_finished_event_updates_started_activity(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "background-activity.db")); await store.initialize()
    try:
        catalog = HarnessCatalog(); profile = catalog.resolve("codex", "gpt-5.6", "standard"); owner = "a" * 64
        thread = await store.create_thread(ThreadCreate(harness=Harness.CODEX, model=profile.model,
            reasoning_level="standard", title="Background"), profile, owner)
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="render", idempotency_key="background-event-key"), profile, owner)
        identity = "b" * 32
        await store.persist_runtime_event(thread.id, turn.id, RuntimeEvent(kind=EventKind.STEP_STARTED,
            payload={"name":"后台任务 · render", "background_job_id":identity}, runtime_sequence=1), "execution")
        await store.persist_runtime_event(thread.id, turn.id, RuntimeEvent(kind=EventKind.STEP_FINISHED,
            payload={"name":"后台任务 · render", "background_job_id":identity,
                "status":"completed", "returncode":0}, runtime_sequence=2), "execution")
        rows = [item for item in await store.items_after(thread.id) if item.type == ItemType.PLAN]
        assert len(rows) == 1 and rows[0].status == "completed"
        assert rows[0].payload["background_job_id"] == identity and rows[0].payload["returncode"] == 0
        await store.set_turn_state(turn.id, TurnState.COMPLETED)
        assert rows[0].id == (await store.items_after(thread.id))[1].id
        assert (await store.items_after(thread.id))[1].status == "completed"
    finally:
        await store.close()


async def test_thread_archive_filters_blocks_active_work_and_can_restore(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "archive.db"))
    await store.initialize()
    try:
        catalog = HarnessCatalog(); profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
        owner = "a" * 64
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Archive"), profile, owner)
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="hello", idempotency_key="archive-turn-key"), profile, owner)
        with pytest.raises(RuntimeError, match="active_thread_cannot_be_archived"):
            await store.set_thread_archived(thread.id, owner, archived=True)
        await store.set_turn_state(turn.id, TurnState.COMPLETED)
        archived = await store.set_thread_archived(thread.id, owner, archived=True)
        assert archived.archived_at is not None
        assert await store.list_threads(owner) == []
        assert [value.id for value in await store.list_threads(owner, archived=True)] == [thread.id]
        with pytest.raises(RuntimeError, match="thread_archived"):
            await store.create_turn(thread.id,
                TurnCreate(message="blocked", idempotency_key="archive-blocked-key"), profile, owner)
        restored = await store.set_thread_archived(thread.id, owner, archived=False)
        assert restored.archived_at is None
        assert [value.id for value in await store.list_threads(owner)] == [thread.id]
    finally:
        await store.close()


async def test_initialize_adds_archived_at_to_existing_database(tmp_path):
    import sqlite3
    database = tmp_path / "archive-migration.db"
    store = Store("sqlite+aiosqlite:///" + str(database));await store.initialize();await store.close()
    with sqlite3.connect(database) as connection:
        connection.execute("ALTER TABLE threads DROP COLUMN archived_at")
    upgraded = Store("sqlite+aiosqlite:///" + str(database))
    await upgraded.initialize()
    try:
        async with upgraded.engine.connect() as connection:
            from sqlalchemy import inspect
            columns = await connection.run_sync(lambda value: {column["name"] for column in inspect(value).get_columns("threads")})
        assert "archived_at" in columns
    finally:
        await upgraded.close()


async def test_initialize_adds_restricted_network_access_to_existing_turns(tmp_path):
    import sqlite3
    database = tmp_path / "network-access-migration.db"
    store = Store("sqlite+aiosqlite:///" + str(database));await store.initialize()
    catalog = HarnessCatalog();profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
    thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
        reasoning_level="standard", title="Migration"), profile, "a" * 64)
    turn,_,_=await store.create_turn(thread.id,TurnCreate(message="hello",idempotency_key="migration-turn"),
        profile,"a" * 64)
    await store.close()
    with sqlite3.connect(database) as connection:
        connection.execute("ALTER TABLE turns DROP COLUMN network_access")
    upgraded=Store("sqlite+aiosqlite:///"+str(database));await upgraded.initialize()
    try:
        restored=await upgraded.get_turn(turn.id)
        assert restored.network_access == "restricted"
    finally:await upgraded.close()


async def test_turn_lease_generation_fences_stale_broker_writes(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "fencing.db"))
    await store.initialize()
    try:
        catalog = HarnessCatalog(); profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
        owner = "a" * 64
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Fence"), profile, owner)
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="hello", idempotency_key="fencing-turn-key"), profile, owner)
        first = await store.claim_turn(turn.id, "first", 30)
        await store.release_turn_lease(turn.id, "first", first.recovery_generation)
        second = await store.claim_turn(turn.id, "second", 30)
        assert second.recovery_generation > first.recovery_generation
        try:
            await store.set_turn_state(turn.id, TurnState.RUNNING,
                lease_owner="first", recovery_generation=first.recovery_generation)
        except RuntimeError as exc:
            assert str(exc) == "stale_turn_lease"
        else:
            raise AssertionError("A stale broker changed the Turn state")
    finally:
        await store.close()


async def test_expired_sqlite_lease_can_be_claimed_without_timezone_error(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "expired-lease.db"))
    await store.initialize()
    try:
        catalog = HarnessCatalog(); profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
        owner = "a" * 64
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Expired lease"), profile, owner)
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="hello", idempotency_key="expired-lease-key"), profile, owner)
        first = await store.claim_turn(turn.id, "dead-broker", 30)
        async with store.connections.begin() as db:
            row = await db.get(TurnRow, turn.id, with_for_update=True)
            row.lease_expires_at = now() - timedelta(seconds=120)
        second = await store.claim_turn(turn.id, "new-broker", 30)
        assert second is not None
        assert second.lease_owner == "new-broker"
        assert second.recovery_generation > first.recovery_generation
    finally:
        await store.close()


async def test_item_and_event_writes_can_be_fenced_by_turn_lease(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "output-fence.db"))
    await store.initialize()
    try:
        catalog = HarnessCatalog(); profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
        owner = "a" * 64
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Output fence"), profile, owner)
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="hello", idempotency_key="output-fence-key"), profile, owner)
        first = await store.claim_turn(turn.id, "first", 30)
        await store.release_turn_lease(turn.id, "first", first.recovery_generation)
        await store.claim_turn(turn.id, "second", 30)
        with pytest.raises(RuntimeError, match="stale_turn_lease"):
            await store.append_item(thread.id, turn.id, ItemType.ERROR, status="failed",
                lease_owner="first", recovery_generation=first.recovery_generation)
        with pytest.raises(RuntimeError, match="stale_turn_lease"):
            await store.append_event(turn.id, EventKind.ERROR, {"code":"stale"},
                lease_owner="first", recovery_generation=first.recovery_generation)
        assert [item.type for item in await store.items_after(thread.id)] == [ItemType.USER_MESSAGE]
        assert await store.events_after(turn.id, 0) == []
    finally:
        await store.close()


async def test_project_files_keep_versions_and_return_only_the_current_content(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "files.db"))
    await store.initialize()
    try:
        catalog = HarnessCatalog(); profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
        owner = "a" * 64
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Files"), profile, owner)
        first, created = await store.register_project_file(thread.id, path="assets/reference.png",
            source_type="upload", source_turn_id="one", sha256="1" * 64, size=10,
            mime="image/png", object_ref="sha256:first")
        with pytest.raises(ValueError, match="managed project path"):
            await store.register_project_file(thread.id, path="assets/..", source_type="upload",
                source_turn_id="bad", sha256="3" * 64, size=1,
                mime="text/plain", object_ref="sha256:bad")
        assert not valid_project_file_path("assets/..", "upload")
        assert not valid_project_file_path("../outside", "upload")
        duplicate, duplicate_created = await store.register_project_file(thread.id, path="assets/reference.png",
            source_type="upload", source_turn_id="two", sha256="1" * 64, size=10,
            mime="image/png", object_ref="sha256:first")
        second, replaced = await store.register_project_file(thread.id, path="assets/reference.png",
            source_type="upload", source_turn_id="three", sha256="2" * 64, size=20,
            mime="image/png", object_ref="sha256:second")
        assert created and not duplicate_created and replaced
        assert duplicate.id == first.id and second.version == 2
        current = await store.current_project_files(thread.id, owner)
        assert len(current) == 1 and current[0].id == second.id
        assert await store.get_project_file_for_owner(second.id, "b" * 64) is None
        renamed = await store.rename_project_file(second.id, owner, "assets/renamed.png")
        assert renamed.path == "assets/renamed.png"
        assert [item.path for item in await store.current_project_files(thread.id, owner)] == ["assets/renamed.png"]
        deleted = await store.delete_project_file(renamed.id, owner)
        assert deleted.deleted and await store.current_project_files(thread.id, owner) == []
        restored, restored_created = await store.register_project_file(thread.id, path="assets/renamed.png",
            source_type="upload", source_turn_id="four", sha256="2" * 64, size=20,
            mime="image/png", object_ref="sha256:second")
        assert restored_created and restored.version == 3
        history = await store.project_file_history(thread.id, "assets/renamed.png", owner)
        assert [item.version for item in history] == [3, 2, 1]
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="use current file", idempotency_key="file-snapshot-turn"), profile, owner)
        claimed = await store.claim_turn(turn.id, "snapshot-worker", 30)
        snapshot = await store.freeze_turn_project_files(turn.id, thread.id,
            lease_owner="snapshot-worker", recovery_generation=claimed.recovery_generation)
        assert [(item.path, item.version, item.sha256) for item in snapshot] == [
            ("assets/renamed.png", 3, "2" * 64)]
        newer, _ = await store.register_project_file(thread.id, path="assets/renamed.png",
            source_type="upload", source_turn_id="five", sha256="5" * 64, size=50,
            mime="image/png", object_ref="sha256:fifth")
        frozen_again = await store.freeze_turn_project_files(turn.id, thread.id,
            lease_owner="snapshot-worker", recovery_generation=claimed.recovery_generation)
        assert frozen_again[0].project_file_id == restored.id
        assert (await store.current_project_files(thread.id, owner))[0].id == newer.id
        restored_history, restored_created = await store.restore_project_file_version(restored.id, owner)
        assert restored_created and restored_history.version == 5 and restored_history.sha256 == "2" * 64
        same, same_created = await store.restore_project_file_version(restored.id, owner)
        assert not same_created and same.id == restored_history.id
        artifact,artifact_file,artifact_created=await store.publish_artifact(thread.id,turn.id,
            path="result.txt",sha256="9"*64,size=6,mime="text/plain",object_ref="sha256:result",
            partial=False,lease_owner="snapshot-worker",recovery_generation=claimed.recovery_generation)
        replay,replay_file,replay_created=await store.publish_artifact(thread.id,turn.id,
            path="result.txt",sha256="9"*64,size=6,mime="text/plain",object_ref="sha256:result",
            partial=False,lease_owner="snapshot-worker",recovery_generation=claimed.recovery_generation)
        assert artifact_created and not replay_created and replay.id==artifact.id
        assert replay_file.id==artifact_file.id
        artifact_items=[item for item in await store.items_after(thread.id) if item.type==ItemType.ARTIFACT]
        assert len(artifact_items)==1 and artifact_items[0].payload["artifact_id"]==artifact.id
        empty_thread=await store.create_thread(ThreadCreate(harness=Harness.CLAUDE,model=profile.model,
            reasoning_level="standard",title="Empty snapshot"),profile,owner)
        empty_turn,_,_=await store.create_turn(empty_thread.id,
            TurnCreate(message="empty",idempotency_key="empty-snapshot-turn"),profile,owner)
        empty_claim=await store.claim_turn(empty_turn.id,"empty-worker",30)
        assert await store.freeze_turn_project_files(empty_turn.id,empty_thread.id,
            lease_owner="empty-worker",recovery_generation=empty_claim.recovery_generation)==[]
        await store.register_project_file(empty_thread.id,path="assets/later.txt",source_type="upload",
            source_turn_id="",sha256="8"*64,size=1,mime="text/plain",object_ref="sha256:later")
        assert await store.freeze_turn_project_files(empty_turn.id,empty_thread.id,
            lease_owner="empty-worker",recovery_generation=empty_claim.recovery_generation)==[]
        assert (await store.get_turn(empty_turn.id)).project_files_frozen is True
        applied=await store.apply_managed_file_versions(empty_thread.id,owner,[
            {"path":"assets/later.txt","kind":"missing","expected_version":1},
            {"path":"artifacts/manual.txt","kind":"untracked","expected_version":None,
             "source_type":"artifact","sha256":"7"*64,"size":7,"mime":"text/plain",
             "object_ref":"sha256:manual"}])
        assert len(applied)==2 and {item.deleted for item in applied}=={False,True}
        assert [item.path for item in await store.current_project_files(empty_thread.id,owner)]==[
            "artifacts/manual.txt"]
        with pytest.raises(RuntimeError,match="version_conflict"):
            await store.apply_managed_file_versions(empty_thread.id,owner,[
                {"path":"assets/later.txt","kind":"missing","expected_version":1},
                {"path":"artifacts/never.txt","kind":"untracked","expected_version":None,
                 "source_type":"artifact","sha256":"6"*64,"size":1,"mime":"text/plain",
                 "object_ref":"sha256:never"}])
        assert "artifacts/never.txt" not in {item.path for item in await store.current_project_files(
            empty_thread.id,owner)}
    finally:
        await store.close()


async def test_interaction_response_is_owner_scoped_and_idempotent(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "interactions.db")); await store.initialize()
    try:
        catalog = HarnessCatalog(); profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
        owner = "a" * 64
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Interaction"), profile, owner)
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="hello", idempotency_key="interaction-store-key"), profile, owner)
        claimed = await store.claim_turn(turn.id, "worker", 30)
        await store.start_runtime_execution(turn.id, "execution-one", "worker", claimed.recovery_generation)
        await store.set_turn_state(turn.id, TurnState.AWAITING_USER,
            lease_owner="worker", recovery_generation=claimed.recovery_generation)
        interaction, created = await store.upsert_interaction(thread.id, turn.id, "execution-one",
            "native-one", "approval", {"prompt":"Allow?"})
        duplicate, duplicate_created = await store.upsert_interaction(thread.id, turn.id, "execution-one",
            "native-one", "approval", {"prompt":"Allow?"})
        assert created and not duplicate_created and duplicate.id == interaction.id
        try:
            await store.claim_interaction_response(interaction.id, "b" * 64, "response-key", {"action":"approve"})
        except KeyError:
            pass
        else:
            raise AssertionError("Foreign owner resolved an interaction")
        responding, accepted = await store.claim_interaction_response(interaction.id, owner,
            "response-key", {"action":"approve"})
        assert accepted and responding.status == "responding"
        await store.finish_interaction_response(interaction.id, delivered=True)
        repeated, accepted_again = await store.claim_interaction_response(interaction.id, owner,
            "response-key", {"action":"approve"})
        assert not accepted_again and repeated.status == "responded"
    finally:
        await store.close()


async def test_expired_interaction_status_is_committed_before_error(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "expired-interaction.db")); await store.initialize()
    try:
        catalog = HarnessCatalog(); profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
        owner = "a" * 64
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Expired"), profile, owner)
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="hello", idempotency_key="expired-turn-key"), profile, owner)
        interaction, _ = await store.upsert_interaction(thread.id, turn.id, "old-execution",
            "native-expired", "approval", {"prompt":"Allow?"})
        await store.set_turn_state(turn.id, TurnState.RUNNING)
        with pytest.raises(RuntimeError, match="interaction_expired"):
            await store.claim_interaction_response(interaction.id, owner, "expired-response-key", {"action":"approve"})
        assert (await store.list_interactions(thread.id, owner))[0].status == "expired"
        with pytest.raises(RuntimeError, match="interaction_not_pending"):
            await store.claim_interaction_response(interaction.id, owner, "second-response-key", {"action":"approve"})
    finally:
        await store.close()


async def test_interaction_response_claim_expires_and_preserves_idempotency(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "interaction-claim.db")); await store.initialize()
    try:
        catalog = HarnessCatalog(); profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
        owner = "a" * 64
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Response claim"), profile, owner)
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="hello", idempotency_key="response-claim-turn"), profile, owner)
        await store.set_turn_state(turn.id, TurnState.AWAITING_USER)
        async with store.connections.begin() as db:
            row = await db.get(TurnRow, turn.id, with_for_update=True)
            row.runtime_execution_id = "execution-one"
        interaction, _ = await store.upsert_interaction(thread.id, turn.id, "execution-one",
            "native-response", "question", {"prompt":"Format?"})
        response = {"action":"answer", "answers":{"format":"mp4"}}
        claimed, accepted = await store.claim_interaction_response(interaction.id, owner,
            "response-claim-key", response, claim_owner="broker-one", lease_seconds=30)
        assert accepted and claimed.status == "responding"
        with pytest.raises(RuntimeError, match="interaction_response_in_progress"):
            await store.claim_interaction_response(interaction.id, owner,
                "response-claim-key", response, claim_owner="broker-two", lease_seconds=30)
        async with store.connections.begin() as db:
            from sandbox_harness.store import InteractionRow
            row = await db.get(InteractionRow, interaction.id, with_for_update=True)
            row.response_expires_at = now() - timedelta(seconds=1)
        reclaimed, accepted = await store.claim_interaction_response(interaction.id, owner,
            "fresh-browser-key", response, claim_owner="broker-two", lease_seconds=30)
        assert accepted and reclaimed.status == "responding"
        assert reclaimed.response_key == "fresh-browser-key"
        await store.finish_interaction_response(interaction.id, delivered=True, claim_owner="broker-two")
        with pytest.raises(RuntimeError, match="idempotency_conflict"):
            await store.claim_interaction_response(interaction.id, owner,
                "fresh-browser-key", {"action":"answer", "answers":{"format":"webm"}})
    finally:
        await store.close()


async def test_unapplied_inputs_are_failed_with_their_transcript_items(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "unapplied-input.db")); await store.initialize()
    try:
        catalog = HarnessCatalog(); profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
        owner = "a" * 64
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Unapplied input"), profile, owner)
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="hello", idempotency_key="unapplied-turn-key"), profile, owner)
        value, _ = await store.create_turn_input(turn.id,
            TurnInputCreate(message="follow up", idempotency_key="unapplied-input-key"), owner)
        assert (await store.items_after(thread.id))[-1].status == "in_progress"
        await store.fail_unapplied_inputs(turn.id)
        assert (await store.get_turn_input(value.id)).status == "failed"
        assert (await store.items_after(thread.id))[-1].status == "failed"
    finally:
        await store.close()


async def test_concurrent_thread_items_receive_unique_contiguous_sequences(tmp_path):
    import asyncio
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "item-sequences.db")); await store.initialize()
    try:
        catalog = HarnessCatalog(); profile = catalog.resolve("codex", "gpt-5.6", "standard")
        owner = "a" * 64
        thread = await store.create_thread(ThreadCreate(harness=Harness.CODEX, model=profile.model,
            reasoning_level="standard", title="Items"), profile, owner)
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="initial", idempotency_key="item-sequence-turn"), profile, owner)
        await store.set_turn_state(turn.id, TurnState.RUNNING)
        await asyncio.gather(*(store.append_item(thread.id, turn.id, ItemType.TOOL,
            status="completed", summary=f"tool-{index}") for index in range(20)))
        sequences = [item.sequence for item in await store.items_after(thread.id)]
        assert sequences == list(range(1, 22))
    finally:
        await store.close()


async def test_legacy_unsafe_attachment_name_remains_readable_but_cannot_be_submitted(tmp_path):
    import json
    from sandbox_harness.domain import identifier, now
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "legacy-attachment.db")); await store.initialize()
    try:
        thread_id, turn_id, timestamp = identifier(), identifier(), now()
        unsafe = [{"name":"..", "object_ref":"sha256:" + "a" * 64,
            "sha256":"a" * 64, "size":1, "mime":"text/plain"}]
        async with store.connections.begin() as db:
            db.add(ThreadRow(id=thread_id, owner_id="a" * 64, harness="codex", model="gpt-5.6",
                reasoning_level="standard", reasoning_effort="medium", title="Legacy",
                next_item_sequence=0, created_at=timestamp, updated_at=timestamp))
            db.add(TurnRow(id=turn_id, thread_id=thread_id, idempotency_key="legacy-unsafe-key",
                message="old", attachments_json=json.dumps(unsafe), harness="codex", model="gpt-5.6",
                reasoning_level="standard", reasoning_effort="medium", delivery_mode="start",
                state=TurnState.COMPLETED.value, created_at=timestamp, updated_at=timestamp))
        assert (await store.get_turn(turn_id)).attachments[0].name == ".."
        with pytest.raises(ValueError, match="attachment filename"):
            TurnCreate(message="new", idempotency_key="new-unsafe-key", attachments=unsafe)
    finally:
        await store.close()


async def test_thread_summaries_include_latest_turn_state(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "summaries.db")); await store.initialize()
    try:
        catalog = HarnessCatalog(); profile = catalog.resolve("claude", "claude-4.6-sonnet", "standard")
        owner = "a" * 64
        thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
            reasoning_level="standard", title="Summary"), profile, owner)
        empty = await store.list_thread_summaries(owner)
        assert empty[0]["latest_turn_state"] == ""
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="hello", idempotency_key="summary-turn-key"), profile, owner)
        await store.set_turn_state(turn.id, TurnState.RUNNING)
        rows = await store.list_thread_summaries(owner)
        assert rows[0]["latest_turn_state"] == "RUNNING"
        assert rows[0]["latest_turn_updated_at"] is not None
    finally:
        await store.close()


async def test_thread_deletion_rejects_active_work_and_removes_owned_records(tmp_path):
    store = Store("sqlite+aiosqlite:///" + str(tmp_path / "delete-thread.db")); await store.initialize()
    try:
        catalog = HarnessCatalog(); profile = catalog.resolve("codex", "gpt-5.6", "standard")
        owner = "a" * 64
        thread = await store.create_thread(ThreadCreate(harness=Harness.CODEX, model=profile.model,
            reasoning_level="standard", title="Delete"), profile, owner)
        turn, _, _ = await store.create_turn(thread.id,
            TurnCreate(message="hello", idempotency_key="delete-thread-key"), profile, owner)
        await store.append_event(turn.id, EventKind.STATE, {"state":"QUEUED"})
        await store.register_project_file(thread.id, path="assets/input.txt", source_type="upload",
            source_turn_id=turn.id, sha256="1" * 64, size=1, mime="text/plain", object_ref="sha256:one")
        with pytest.raises(RuntimeError, match="active_thread"):
            await store.delete_thread(thread.id, owner)
        with pytest.raises(KeyError):
            await store.delete_thread(thread.id, "b" * 64)
        await store.set_turn_state(turn.id, TurnState.COMPLETED)
        assert await store.delete_thread(thread.id, owner) == [turn.id]
        assert await store.get_thread(thread.id, owner) is None
        assert await store.get_turn(turn.id) is None
        assert await store.events_after(turn.id, 0) == []
        assert await store.current_project_files(thread.id, owner) == []
    finally:
        await store.close()
