from __future__ import annotations

import argparse
from collections import deque
import hashlib
import json
import os
import queue
import subprocess
import sys
import threading

from .adapters import normalize_error_code
from .instructions import runtime_instructions


class CodexAppServerExited(RuntimeError):
    def __init__(self,errors):
        values=list(errors);raw=''.join(values).encode(errors='replace')
        self.diagnostics={'exception_type':type(self).__name__,
            'stderr_line_count':len(values),'stderr_sha256':hashlib.sha256(raw).hexdigest()}
        super().__init__('Codex app-server exited')


class CodexRpcError(RuntimeError):
    def __init__(self,method,value):
        self.method=method
        code=value.get('code') if isinstance(value,dict) else None
        self.diagnostics={'exception_type':type(self).__name__,'rpc_method':method,
            **({'rpc_code':code} if isinstance(code,int) else {})}
        super().__init__('Codex RPC failed')


def runner_failure(exc, input_ids=()):
    diagnostics={'exception_type':type(exc).__name__}
    if isinstance(exc,CodexAppServerExited):diagnostics=exc.diagnostics
    if isinstance(exc,CodexRpcError):diagnostics=exc.diagnostics
    unavailable=isinstance(exc,CodexRpcError) and exc.method=='thread/resume'
    return {'type':'error','error_code':(
        'native_session_unavailable' if unavailable else 'harness_runner_failed'),
        'message':('Saved Codex session is unavailable' if unavailable
            else 'Harness runner failed'),'diagnostics':diagnostics,
        'input_ids':list(dict.fromkeys(str(value) for value in input_ids if value))}


class Bridge:
    def __init__(self, interaction_responses, interaction_condition):
        self.process = subprocess.Popen(["codex", "app-server", "--listen", "stdio://"], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1, env=os.environ)
        self.messages = queue.Queue(); self.forwarded = queue.Queue(); self.errors = deque(maxlen=50); self.sequence = 0
        threading.Thread(target=self._read, daemon=True).start()
        self.error_reader=threading.Thread(target=self._read_errors,daemon=True)
        self.error_reader.start()
        self.interaction_responses = interaction_responses
        self.interaction_condition = interaction_condition

    def _read(self):
        for line in iter(lambda: self.process.stdout.readline(16 * 1024 * 1024 + 1), ""):
            if len(line.encode()) > 16 * 1024 * 1024:
                self.messages.put({"method": "runner/error", "params": {"message": "Codex frame exceeds limit"}}); break
            try: value = json.loads(line)
            except ValueError: continue
            if isinstance(value, dict): self.messages.put(value)
        self.messages.put(None)

    def _read_errors(self):
        for line in iter(lambda: self.process.stderr.readline(4096), ""):
            self.errors.append(line[:2000]);sys.stderr.write(line);sys.stderr.flush()

    def exited(self):
        self.error_reader.join(.2)
        return CodexAppServerExited(self.errors)

    def write(self, value):
        self.process.stdin.write(json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n"); self.process.stdin.flush()

    def respond(self, message):
        method = message.get("method", "")
        is_question = "requestUserInput" in method
        if "requestApproval" not in method and not is_question:
            self.write({"id": message["id"], "error": {"code": -32601, "message": "Interaction is not authorized"}}); return
        interaction_id = "codex-" + str(message["id"])
        params = message.get("params") or {}
        self.emit({"type": "harness.interaction.requested", "id": interaction_id,
            "interaction_kind": "question" if is_question else "approval",
            "prompt": params.get("reason") or params.get("message") or method,
            "options": params.get("options") or [], "questions": params.get("questions") or []})
        with self.interaction_condition:
            ready = self.interaction_condition.wait_for(
                lambda: interaction_id in self.interaction_responses, timeout=86400,
            )
            if not ready:
                self.write({"id": message["id"], "error": {"code": -32001, "message": "Interaction expired"}}); return
            response = self.interaction_responses.pop(interaction_id)
        if is_question:
            result = {"answers": response.get("answers") or {}}
        elif "permissions" in method:
            result = {"permissions": params.get("permissions") or {}} if response.get("action") == "approve" else {"permissions": {}}
        else:
            result = {"decision": "accept" if response.get("action") == "approve" else "decline"}
        self.write({"id": message["id"], "result": result})
        self.emit({"type":"harness.interaction.applied","id":interaction_id})

    @staticmethod
    def emit(message): print(json.dumps(message, ensure_ascii=False, separators=(",", ":")), flush=True)

    def rpc(self, method, params):
        self.sequence += 1; identity = self.sequence; self.write({"id": identity, "method": method, "params": params})
        while True:
            message = self.messages.get()
            if message is None:raise self.exited()
            if message.get("id") == identity and "method" not in message:
                if "error" in message:raise CodexRpcError(method,message.get('error'))
                return message.get("result", {})
            if "id" in message and "method" in message: self.respond(message)
            else:
                self.emit(message); self.forwarded.put(message)

    def close(self):
        if self.process.poll() is None:
            try:
                if self.process.stdin and not self.process.stdin.closed:self.process.stdin.close()
                self.process.wait(5)
            except (BrokenPipeError,subprocess.TimeoutExpired):
                if self.process.poll() is None:self.process.terminate()
                try:self.process.wait(2)
                except subprocess.TimeoutExpired:self.process.kill();self.process.wait(2)


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--model", required=True); parser.add_argument("--thread-id", default="")
    args = parser.parse_args(); controls = queue.Queue(); eof = threading.Event()
    interaction_responses = {}; interaction_condition = threading.Condition()

    def read_controls():
        for line in sys.stdin:
            try: value = json.loads(line)
            except ValueError: continue
            if isinstance(value, dict) and value.get("type") == "input": controls.put(value)
            elif isinstance(value, dict) and value.get("type") == "interaction_response":
                with interaction_condition:
                    interaction_responses[str(value.get("id", ""))] = value.get("response") or {}
                    interaction_condition.notify_all()
        eof.set()

    threading.Thread(target=read_controls, daemon=True).start();bridge=None
    def open_bridge(thread_id=''):
        value=Bridge(interaction_responses,interaction_condition)
        try:
            value.rpc("initialize",{"clientInfo":{"name":"sandbox-harness-platform","version":"0.1.0"}})
            value.write({"method":"initialized","params":{}})
            params={"cwd":"/workspace/project","model":args.model,"approvalPolicy":"never",
                "sandbox":"danger-full-access","developerInstructions":runtime_instructions()}
            if thread_id:params["threadId"]=thread_id
            opened=value.rpc("thread/resume" if thread_id else "thread/start",params)
            return value,opened["thread"]["id"]
        except BaseException:
            value.close();raise
    accepted_input_ids=[]
    try:
        bridge,thread_id=open_bridge(args.thread_id)
        bridge.emit({"type": "thread.started", "thread_id": thread_id})
        while not eof.is_set() or not controls.empty():
            try: first = controls.get(timeout=.1)
            except queue.Empty: continue
            started = bridge.rpc("turn/start", {"threadId": thread_id, "cwd": "/workspace/project", "approvalPolicy": "never",
                "sandboxPolicy": {"type": "externalSandbox", "networkAccess": "enabled"},
                "input": [{"type": "text", "text": first["message"]}], "model": args.model,
                "effort": os.environ.get("HARNESS_REASONING_EFFORT", "high")})
            turn_id = started["turn"]["id"]; input_ids = [first["id"]]
            accepted_input_ids.append(first["id"]); completed = False; completed_turn = {}
            while not completed:
                while True:
                    try: extra = controls.get_nowait()
                    except queue.Empty: break
                    bridge.rpc("turn/steer", {"threadId": thread_id, "expectedTurnId": turn_id,
                        "input": [{"type": "text", "text": extra["message"]}], "clientUserMessageId": extra["id"]})
                    input_ids.append(extra["id"]);accepted_input_ids.append(extra["id"])
                while True:
                    try: forwarded = bridge.forwarded.get_nowait()
                    except queue.Empty: break
                    if forwarded.get("method") == "turn/completed" and forwarded.get("params", {}).get("turn", {}).get("id") == turn_id:
                        completed_turn = forwarded.get("params", {}).get("turn", {})
                        completed = True
                if completed: break
                try: message = bridge.messages.get(timeout=.05)
                except queue.Empty: continue
                if message is None:raise bridge.exited()
                if "id" in message and "method" in message: bridge.respond(message); continue
                bridge.emit(message)
                if message.get("method") == "turn/completed" and message.get("params", {}).get("turn", {}).get("id") == turn_id:
                    completed_turn = message.get("params", {}).get("turn", {})
                    completed = True
            error = completed_turn.get("error") or {}
            status=completed_turn.get("status", "completed")
            native_code=error.get("codexErrorInfo")
            bridge.close();bridge=None
            verification,verified_thread_id=open_bridge(thread_id)
            verification.close()
            if verified_thread_id!=thread_id:raise CodexRpcError('thread/resume',{'code':-32004})
            Bridge.emit({"type": "harness.input.completed", "input_ids": input_ids, "thread_id": thread_id,
                "status": status,"error_code": (normalize_error_code(native_code)
                    if native_code else "harness_failed" if status=="failed" else ""),
                "error_detail": error.get("message") or ""})
            return 0
    except Exception as exc:
        Bridge.emit(runner_failure(exc,accepted_input_ids));return 1
    finally:
        if bridge is not None:bridge.close()


if __name__ == "__main__":raise SystemExit(main())
