from __future__ import annotations

import argparse
from collections import deque
import json
import os
import queue
import subprocess
import sys
import threading


class Bridge:
    def __init__(self):
        self.process = subprocess.Popen(["codex", "app-server", "--listen", "stdio://"], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1, env=os.environ)
        self.messages = queue.Queue(); self.forwarded = queue.Queue(); self.errors = deque(maxlen=50); self.sequence = 0
        threading.Thread(target=self._read, daemon=True).start()
        threading.Thread(target=self._read_errors, daemon=True).start()

    def _read(self):
        for line in iter(lambda: self.process.stdout.readline(16 * 1024 * 1024 + 1), ""):
            if len(line.encode()) > 16 * 1024 * 1024:
                self.messages.put({"method": "runner/error", "params": {"message": "Codex frame exceeds limit"}}); break
            try: value = json.loads(line)
            except ValueError: continue
            if isinstance(value, dict): self.messages.put(value)
        self.messages.put(None)

    def _read_errors(self):
        for line in iter(lambda: self.process.stderr.readline(4096), ""): self.errors.append(line[:2000])

    def write(self, value):
        self.process.stdin.write(json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n"); self.process.stdin.flush()

    def respond(self, message):
        method = message.get("method", "")
        if "requestApproval" in method:
            result = {"permissions": {}} if "permissions" in method else {"decision": "decline"}
        elif "requestUserInput" in method: result = {"answers": {}}
        else:
            self.write({"id": message["id"], "error": {"code": -32601, "message": "Interaction is not authorized"}}); return
        self.write({"id": message["id"], "result": result})

    @staticmethod
    def emit(message): print(json.dumps(message, ensure_ascii=False, separators=(",", ":")), flush=True)

    def rpc(self, method, params):
        self.sequence += 1; identity = self.sequence; self.write({"id": identity, "method": method, "params": params})
        while True:
            message = self.messages.get()
            if message is None: raise RuntimeError("Codex app-server exited: " + "".join(self.errors)[-2000:])
            if message.get("id") == identity and "method" not in message:
                if "error" in message: raise RuntimeError(str(message["error"].get("message", "Codex RPC failed")))
                return message.get("result", {})
            if "id" in message and "method" in message: self.respond(message)
            else:
                self.emit(message); self.forwarded.put(message)

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try: self.process.wait(5)
            except subprocess.TimeoutExpired: self.process.kill(); self.process.wait(5)


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--model", required=True); parser.add_argument("--thread-id", default="")
    args = parser.parse_args(); controls = queue.Queue(); eof = threading.Event()

    def read_controls():
        for line in sys.stdin:
            try: value = json.loads(line)
            except ValueError: continue
            if isinstance(value, dict) and value.get("type") == "input": controls.put(value)
        eof.set()

    threading.Thread(target=read_controls, daemon=True).start(); bridge = Bridge()
    try:
        bridge.rpc("initialize", {"clientInfo": {"name": "sandbox-harness-platform", "version": "0.1.0"}})
        bridge.write({"method": "initialized", "params": {}})
        params = {"cwd": "/workspace/project", "model": args.model, "approvalPolicy": "never", "sandbox": "danger-full-access"}
        if args.thread_id: params["threadId"] = args.thread_id
        opened = bridge.rpc("thread/resume" if args.thread_id else "thread/start", params); thread_id = opened["thread"]["id"]
        bridge.emit({"type": "thread.started", "thread_id": thread_id})
        while not eof.is_set() or not controls.empty():
            try: first = controls.get(timeout=.1)
            except queue.Empty: continue
            started = bridge.rpc("turn/start", {"threadId": thread_id, "cwd": "/workspace/project", "approvalPolicy": "never",
                "sandboxPolicy": {"type": "externalSandbox", "networkAccess": "enabled"},
                "input": [{"type": "text", "text": first["message"]}], "model": args.model,
                "effort": os.environ.get("HARNESS_REASONING_EFFORT", "high")})
            turn_id = started["turn"]["id"]; input_ids = [first["id"]]; completed = False; completed_turn = {}
            while not completed:
                while True:
                    try: extra = controls.get_nowait()
                    except queue.Empty: break
                    bridge.rpc("turn/steer", {"threadId": thread_id, "expectedTurnId": turn_id,
                        "input": [{"type": "text", "text": extra["message"]}], "clientUserMessageId": extra["id"]})
                    input_ids.append(extra["id"])
                while True:
                    try: forwarded = bridge.forwarded.get_nowait()
                    except queue.Empty: break
                    if forwarded.get("method") == "turn/completed" and forwarded.get("params", {}).get("turn", {}).get("id") == turn_id:
                        completed_turn = forwarded.get("params", {}).get("turn", {})
                        completed = True
                if completed: break
                try: message = bridge.messages.get(timeout=.05)
                except queue.Empty: continue
                if message is None: raise RuntimeError("Codex app-server exited: " + "".join(bridge.errors)[-2000:])
                if "id" in message and "method" in message: bridge.respond(message); continue
                bridge.emit(message)
                if message.get("method") == "turn/completed" and message.get("params", {}).get("turn", {}).get("id") == turn_id:
                    completed_turn = message.get("params", {}).get("turn", {})
                    completed = True
            error = completed_turn.get("error") or {}
            bridge.emit({"type": "harness.input.completed", "input_ids": input_ids, "thread_id": thread_id,
                "status": completed_turn.get("status", "completed"),
                "error_code": error.get("codexErrorInfo") or "",
                "error_detail": error.get("message") or ""})
    except Exception as exc:
        bridge.emit({"type": "error", "message": type(exc).__name__ + ": " + str(exc)}); raise
    finally: bridge.close()


if __name__ == "__main__": main()
