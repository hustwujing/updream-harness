from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import time
from collections.abc import Iterable

from sandbox_harness.common.observability.observability import (carrier)
from sandbox_harness.common.contracts.domain import (CapabilityClaims, Harness, identifier)


def _encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


class CapabilityCodec:
    def __init__(self, key: str, *, kid: str = "current",
                 previous_keys: dict[str, str] | None = None):
        values={kid:key,**(previous_keys or {})}
        if (not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,31}",kid)
                or len(values)!=1+len(previous_keys or {})
                or any(not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,31}",name)
                    or not isinstance(value,str) or len(value)<16
                    for name,value in values.items())):
            raise ValueError("capability_keyring_invalid")
        self.kid=kid
        self.keys={name:value.encode() for name,value in values.items()}

    @classmethod
    def from_settings(cls,settings):
        return cls(settings.gateway_signing_key,kid=settings.gateway_signing_key_id,
            previous_keys=settings.gateway_previous_signing_keys)

    def issue(
        self,
        *,
        thread_id: str,
        turn_id: str,
        sandbox_id: str,
        harness: Harness,
        model: str,
        routes: Iterable[str],
        ttl_seconds: int,
        max_requests: int,
        max_public_requests: int,
        token_budget: int,
        parent_nonce: str = "",
        parent_token_budget: int = 0,
        network_access: str = "restricted",
        scope: str = "turn",
        authorization_version: int = 0,
        group_id: str = "",
        job_id: str = "",
        generation: int = 0,
        source_identity: str = "",
        descriptor_id: str = "",
        descriptor_version: str = "",
        descriptor_digest: str = "",
        operations: Iterable[str] = (),
        artifact_ids: Iterable[str] = (),
        skill_ids: Iterable[str] = (),
        capability_ids: Iterable[str] = (),
        operation_ids: Iterable[str] = (),
        child_profile_ids: Iterable[str] = (),
        runtime_profile_ids: Iterable[str] = (),
        child_profile_id: str = "",
        runtime_profile_id: str = "",
        network_origins: Iterable[str] = (),
        output_scopes: Iterable[str] = (),
        parameter_digest: str = "",
        budget: dict[str, int] | None = None,
        issued_at: int | None = None,
    ) -> tuple[str, CapabilityClaims]:
        issued = int(time.time()) if issued_at is None else issued_at
        claims = CapabilityClaims(
            kid=self.kid,
            traceparent=carrier().get("traceparent", ""),
            thread_id=thread_id,
            turn_id=turn_id,
            sandbox_id=sandbox_id,
            harness=harness,
            model=model,
            routes=list(routes),
            network_access=network_access,
            max_requests=max_requests,
            max_public_requests=max_public_requests,
            token_budget=token_budget,
            parent_nonce=parent_nonce,
            parent_token_budget=parent_token_budget,
            issued_at=issued,
            expires_at=issued + ttl_seconds,
            nonce=identifier(),
            scope=scope,
            authorization_version=authorization_version,
            group_id=group_id,
            job_id=job_id,
            generation=generation,
            source_identity=source_identity,
            descriptor_id=descriptor_id,
            descriptor_version=descriptor_version,
            descriptor_digest=descriptor_digest,
            operations=list(operations),
            artifact_ids=list(artifact_ids),
            skill_ids=list(skill_ids),
            capability_ids=list(capability_ids),
            operation_ids=list(operation_ids),
            child_profile_ids=list(child_profile_ids),
            runtime_profile_ids=list(runtime_profile_ids),
            child_profile_id=child_profile_id,
            runtime_profile_id=runtime_profile_id,
            network_origins=list(network_origins),
            output_scopes=list(output_scopes),
            parameter_digest=parameter_digest,
            budget=budget or {},
        )
        payload = json.dumps(claims.model_dump(mode="json"), separators=(",", ":"), sort_keys=True).encode()
        signature = hmac.new(self.keys[self.kid], payload, hashlib.sha256).digest()
        return self.kid+"."+_encode(payload)+"."+_encode(signature), claims

    def verify(self, token: str, *, route: str = "", now: int | None = None) -> CapabilityClaims:
        try:
            kid,payload_text,signature_text=token.split(".")
            payload = _decode(payload_text)
            signature = _decode(signature_text)
        except (ValueError, TypeError) as exc:
            raise ValueError("Malformed capability token") from exc
        key=self.keys.get(kid)
        if key is None:raise ValueError("Unknown capability key id")
        expected = hmac.new(key, payload, hashlib.sha256).digest()
        if not hmac.compare_digest(expected, signature):
            raise ValueError("Invalid capability signature")
        claims = CapabilityClaims.model_validate_json(payload)
        if claims.kid!=kid:raise ValueError("Capability key id mismatch")
        current = int(time.time()) if now is None else now
        if current >= claims.expires_at:
            raise ValueError("Capability token expired")
        if route and not any(route == prefix or route.startswith(prefix.rstrip("/") + "/") for prefix in claims.routes):
            raise ValueError("Route is outside capability scope")
        return claims
