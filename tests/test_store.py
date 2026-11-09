from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.domain import EventKind, Harness, ItemType, ThreadCreate, TurnCreate, TurnInputCreate, TurnState
from sandbox_harness.store import Store


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
        turn, created, start = await store.create_turn(thread.id, value, profile, owner)
        assert created and start and turn.reasoning_effort == "high"
        duplicate, created, _ = await store.create_turn(thread.id, value, profile, owner)
        assert not created and duplicate.id == turn.id
        appended, created = await store.create_turn_input(turn.id,
            TurnInputCreate(message="follow up", idempotency_key="input-key-1"), owner)
        repeated, created_again = await store.create_turn_input(turn.id,
            TurnInputCreate(message="follow up", idempotency_key="input-key-1"), owner)
        assert created and not created_again and repeated.id == appended.id
        claimed = await store.next_pending_input(turn.id)
        assert claimed.id == appended.id and claimed.status == "delivered" and claimed.delivery_mode == "deferred_safe_point"
        await store.set_input_status(claimed.id, "applied")
        assert (await store.list_turn_inputs(turn.id))[0].status == "applied"
        agent = await store.append_agent_delta(thread.id, turn.id, "hel")
        agent = await store.append_agent_delta(thread.id, turn.id, "lo")
        assert agent.summary == "hello"
        event = await store.append_event(turn.id, EventKind.TEXT_DELTA, {"text": "lo"}, item_id=agent.id)
        assert (await store.events_after(turn.id, 0))[0].sequence == event.sequence
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
    finally:
        await store.close()
