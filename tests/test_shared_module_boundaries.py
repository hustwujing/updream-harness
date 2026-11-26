from __future__ import annotations

import ast
import inspect
import os
from pathlib import Path

import pytest


def test_controlled_file_transport_is_owned_by_common():
    from sandbox_harness.services.controlled_worker.controlled_media_adapter import (staged_file_chunks)
    assert staged_file_chunks.__module__ == "sandbox_harness.common.storage.files"


def test_media_process_and_network_policy_are_owned_by_common():
    from sandbox_harness.services.media_worker.media_worker import (_process_start, normalize_host, public_address)
    assert _process_start.__module__ == "sandbox_harness.common.process.identity"
    assert normalize_host.__module__ == "sandbox_harness.common.security.network"
    assert public_address.__module__ == "sandbox_harness.common.security.network"
    assert _process_start(os.getpid()).isdigit()
    assert normalize_host("EXAMPLE.COM.") == "example.com"
    assert not public_address("127.0.0.1")


def test_allocation_client_does_not_import_allocation_service():
    from sandbox_harness.services.allocation_worker.allocation_dispatch import (AllocationClient)
    assert AllocationClient.__module__ == "sandbox_harness.common.sandbox.allocation_client"
    source = inspect.getsource(inspect.getmodule(AllocationClient))
    tree = ast.parse(source)
    assert not any(isinstance(node, ast.ClassDef) and node.name == "AllocationService"
                   for node in ast.walk(tree))


def test_review_coordination_is_independent_of_execution_adapter():
    from sandbox_harness.services.job_worker.reviewer import (ReviewerCoordinator, ReviewerJobAdapter)
    assert ReviewerCoordinator.__module__ == "sandbox_harness.common.jobs.review"
    assert ReviewerCoordinator.__module__ != ReviewerJobAdapter.__module__


def test_media_error_contract_does_not_require_provider_transport():
    from sandbox_harness.common.contracts.media_errors import media_error_guidance
    assert media_error_guidance.__module__ == "sandbox_harness.common.contracts.media_errors"


async def test_shared_file_stream_preserves_bytes_and_partial_tail(tmp_path):
    from sandbox_harness.services.controlled_worker.controlled_media_adapter import (staged_file_chunks)
    path = tmp_path / "bytes"
    path.write_bytes(bytes(range(256)) * 17)
    chunks = [chunk async for chunk in staged_file_chunks(path, 1024)]
    assert b"".join(chunks) == path.read_bytes()
    assert [len(chunk) for chunk in chunks] == [1024, 1024, 1024, 1024, 256]
