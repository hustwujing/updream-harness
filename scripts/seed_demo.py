import asyncio

from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.domain import Harness, ItemType, ThreadCreate, TurnCreate, TurnState
from sandbox_harness.store import Store


async def main():
    store = Store("sqlite+aiosqlite:///.data/dev.db")
    await store.initialize()
    catalog = HarnessCatalog()
    profile = catalog.resolve("claude", "claude-4.6-sonnet", "deep")
    owner = "demo".ljust(64, "0")
    thread = await store.create_thread(ThreadCreate(harness=Harness.CLAUDE, model=profile.model,
        reasoning_level="deep", title="制作一支产品介绍短片"), profile, owner)
    turn, _, _ = await store.create_turn(thread.id, TurnCreate(message="把上传的视频整理成 30 秒产品介绍，保留对白并加上简洁字幕。",
        idempotency_key="demo-turn-0001"), profile, owner)
    await store.set_turn_state(turn.id, TurnState.COMPLETED)
    await store.append_agent_delta(thread.id, turn.id, "我已经检查了素材，并完成了 30 秒版本。对白保持清晰，字幕使用了紧凑的双行排版。")
    await store.complete_agent_item(turn.id, "completed")
    await store.append_item(thread.id, turn.id, ItemType.PLAN, status="completed", summary="分析素材与对白区间",
        payload={"detail":"读取视频、音轨和已有字幕"})
    await store.append_item(thread.id, turn.id, ItemType.TOOL, status="completed", summary="运行媒体处理工具",
        payload={"detail":"裁剪、混音、烧入字幕并导出"})
    await store.append_item(thread.id, turn.id, ItemType.ARTIFACT, status="completed", summary="product-intro.mp4",
        payload={"artifact_id":"demo", "mime":"video/mp4", "size":18432000})
    await store.close()
    print(thread.id)


if __name__ == "__main__":
    asyncio.run(main())
