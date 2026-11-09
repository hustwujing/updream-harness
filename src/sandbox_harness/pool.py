from __future__ import annotations

import asyncio
from collections import defaultdict

from .domain import Harness, identifier
from .sandbox import Sandbox, SandboxBackend


class SandboxPool:
    def __init__(self, backend: SandboxBackend, images: dict[Harness, str], targets: dict[Harness, int]):
        self.backend = backend
        self.images = images
        self.targets = targets
        self._warm: dict[Harness, list[Sandbox]] = defaultdict(list)
        self._attached: dict[str, Sandbox] = {}
        self._lock = asyncio.Lock()

    async def fill(self) -> None:
        for harness, target in self.targets.items():
            while len(self._warm[harness]) < target:
                sandbox = await self.backend.create(
                    harness,
                    self.images[harness],
                    {"sandbox-harness.io/pool": "warm", "sandbox-harness.io/turn": "warm-" + identifier(), "sandbox-harness.io/harness": harness.value},
                    {},
                )
                sandbox.warm = True
                self._warm[harness].append(sandbox)

    async def acquire(self, *, thread_id: str, turn_id: str, harness: Harness, environment: dict[str, str]) -> Sandbox:
        async with self._lock:
            attached = self._attached.get(thread_id)
            if attached and attached.harness == harness:
                await self.backend.bind(attached, thread_id, turn_id)
                attached.turn_id = turn_id
                return attached
            if attached:
                await self.backend.delete(attached)
                self._attached.pop(thread_id, None)
            sandbox = self._warm[harness].pop() if self._warm[harness] else None
            if sandbox:
                # Pristine warm instances do not contain a user token. Runtime injection
                # happens through its control endpoint after ownership is frozen.
                await self.backend.bind(sandbox, thread_id, turn_id)
                sandbox.warm = False
            else:
                sandbox = await self.backend.create(
                    harness,
                    self.images[harness],
                    {"sandbox-harness.io/thread": thread_id, "sandbox-harness.io/turn": turn_id, "sandbox-harness.io/harness": harness.value},
                    environment,
                )
            sandbox.thread_id = thread_id
            sandbox.turn_id = turn_id
            self._attached[thread_id] = sandbox
            return sandbox

    async def release(self, sandbox: Sandbox, *, keep_attached: bool) -> None:
        async with self._lock:
            if keep_attached:
                self._attached[sandbox.thread_id] = sandbox
                return
            self._attached.pop(sandbox.thread_id, None)
            await self.backend.delete(sandbox)

    async def close(self) -> None:
        async with self._lock:
            values = [*self._attached.values(), *(item for rows in self._warm.values() for item in rows)]
            self._attached.clear()
            self._warm.clear()
        for sandbox in values:
            await self.backend.delete(sandbox)
