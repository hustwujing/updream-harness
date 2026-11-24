from __future__ import annotations

import asyncio
import errno
import fcntl
import os
from pathlib import Path
import pty
import signal
import struct
import termios
import uuid


MAX_REPLAY_BYTES = 4 * 1024 * 1024


async def _readable(descriptor: int) -> None:
    loop = asyncio.get_running_loop()
    ready = loop.create_future()
    loop.add_reader(descriptor, lambda: not ready.done() and ready.set_result(None))
    try:
        await ready
    finally:
        loop.remove_reader(descriptor)


async def _writable(descriptor: int) -> None:
    loop = asyncio.get_running_loop()
    ready = loop.create_future()
    loop.add_writer(descriptor, lambda: not ready.done() and ready.set_result(None))
    try:
        await ready
    finally:
        loop.remove_writer(descriptor)


class PtySession:
    def __init__(self, identity: str, process: asyncio.subprocess.Process,
                 master_fd: int):
        self.id = identity
        self.process = process
        self.master_fd = master_fd
        self.buffer = bytearray()
        self.base_offset = 0
        self.next_offset = 0
        self.exit_code: int | None = None
        self.condition = asyncio.Condition()
        self.reader = asyncio.create_task(self._read(), name="pty-reader-" + identity)

    @property
    def running(self) -> bool:
        return self.exit_code is None

    async def _append(self, data: bytes) -> None:
        async with self.condition:
            self.buffer.extend(data)
            self.next_offset += len(data)
            if len(self.buffer) > MAX_REPLAY_BYTES:
                removed = len(self.buffer) - MAX_REPLAY_BYTES
                del self.buffer[:removed]
                self.base_offset += removed
            self.condition.notify_all()

    async def _read(self) -> None:
        try:
            while True:
                await _readable(self.master_fd)
                try:
                    data = os.read(self.master_fd, 64 * 1024)
                except BlockingIOError:
                    continue
                except OSError as exc:
                    if exc.errno == errno.EIO:
                        break
                    raise
                if not data:
                    break
                await self._append(data)
        finally:
            self.exit_code = await self.process.wait()
            async with self.condition:
                self.condition.notify_all()

    async def read_after(self, offset: int) -> tuple[int, bytes, bool, int | None]:
        async with self.condition:
            while self.running and offset >= self.next_offset:
                await self.condition.wait()
            start = min(max(offset, self.base_offset), self.next_offset)
            index = start - self.base_offset
            return start, bytes(self.buffer[index:]), self.running, self.exit_code

    async def write(self, data: bytes) -> None:
        if not self.running:
            raise RuntimeError("pty_session_exited")
        view = memoryview(data)
        while view:
            try:
                written = os.write(self.master_fd, view)
            except BlockingIOError:
                await _writable(self.master_fd)
                continue
            view = view[written:]

    def resize(self, cols: int, rows: int) -> None:
        if not 1 <= cols <= 1000 or not 1 <= rows <= 1000:
            raise ValueError("pty_size_invalid")
        fcntl.ioctl(self.master_fd, termios.TIOCSWINSZ,
            struct.pack("HHHH", rows, cols, 0, 0))

    async def close(self) -> None:
        if self.process.returncode is None:
            try:
                os.killpg(self.process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(self.process.wait(), 2)
            except asyncio.TimeoutError:
                try:
                    os.killpg(self.process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                await self.process.wait()
        try:
            os.close(self.master_fd)
        except OSError:
            pass
        await asyncio.gather(self.reader, return_exceptions=True)


class PtyManager:
    def __init__(self, workspace: Path):
        self.workspace = workspace.resolve()
        self.sessions: dict[str, PtySession] = {}

    async def create(self, cwd: str, command: str) -> PtySession:
        target = Path(cwd).resolve()
        if (target != self.workspace and self.workspace not in target.parents) or not target.is_dir():
            raise ValueError("pty_cwd_invalid")
        if command not in {"/bin/bash", "/bin/sh"}:
            raise ValueError("pty_command_invalid")
        master, slave = pty.openpty()
        os.set_blocking(master, False)
        try:
            process = await asyncio.create_subprocess_exec(command, cwd=target,
                env={**os.environ, "TERM": "xterm-256color"}, stdin=slave,
                stdout=slave, stderr=slave, start_new_session=True)
        except BaseException:
            os.close(master)
            raise
        finally:
            os.close(slave)
        identity = uuid.uuid4().hex
        session = PtySession(identity, process, master)
        self.sessions[identity] = session
        return session

    def get(self, identity: str) -> PtySession | None:
        return self.sessions.get(identity)

    async def delete(self, identity: str) -> bool:
        session = self.sessions.pop(identity, None)
        if not session:
            return False
        await session.close()
        return True

    async def close(self) -> None:
        values = list(self.sessions.values())
        self.sessions.clear()
        await asyncio.gather(*(session.close() for session in values),
            return_exceptions=True)
