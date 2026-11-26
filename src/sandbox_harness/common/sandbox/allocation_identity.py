"""Pure allocation identity and immutable request encoding shared by authorities."""
from dataclasses import asdict
import hashlib
import json
from sandbox_harness.common.contracts.domain import Harness
from sandbox_harness.common.contracts.sandbox_contracts import SandboxCreateRequest, SandboxProfile


def allocation_key(namespace,provider,allocation_id):
    return hashlib.sha256(json.dumps([namespace,provider,allocation_id],separators=(',',':')).encode()).hexdigest()


def encode_request(request):
    return json.dumps({'harness':request.harness.value,'profile':asdict(request.profile),
        'environment':dict(request.environment),'ttl_seconds':request.ttl_seconds,
        'allocation_id':request.allocation_id},sort_keys=True,separators=(',',':'))


def decode_request(encoded):
    value=json.loads(encoded)
    return SandboxCreateRequest(harness=Harness(value['harness']),profile=SandboxProfile(**value['profile']),
        environment=value['environment'],ttl_seconds=value['ttl_seconds'],allocation_id=value['allocation_id'])

