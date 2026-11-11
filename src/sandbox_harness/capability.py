from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from collections.abc import Iterable

from .observability import carrier
from .domain import CapabilityClaims, Harness, identifier


def _encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


class CapabilityCodec:
    def __init__(self, key: str):
        self.key = key.encode()

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
        network_access: str = "restricted",
    ) -> tuple[str, CapabilityClaims]:
        issued = int(time.time())
        claims = CapabilityClaims(
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
            issued_at=issued,
            expires_at=issued + ttl_seconds,
            nonce=identifier(),
        )
        payload = json.dumps(claims.model_dump(mode="json"), separators=(",", ":"), sort_keys=True).encode()
        signature = hmac.new(self.key, payload, hashlib.sha256).digest()
        return _encode(payload) + "." + _encode(signature), claims

    def verify(self, token: str, *, route: str = "", now: int | None = None) -> CapabilityClaims:
        try:
            payload_text, signature_text = token.split(".", 1)
            payload = _decode(payload_text)
            signature = _decode(signature_text)
        except (ValueError, TypeError) as exc:
            raise ValueError("Malformed capability token") from exc
        expected = hmac.new(self.key, payload, hashlib.sha256).digest()
        if not hmac.compare_digest(expected, signature):
            raise ValueError("Invalid capability signature")
        claims = CapabilityClaims.model_validate_json(payload)
        current = int(time.time()) if now is None else now
        if current >= claims.expires_at:
            raise ValueError("Capability token expired")
        if route and not any(route == prefix or route.startswith(prefix.rstrip("/") + "/") for prefix in claims.routes):
            raise ValueError("Route is outside capability scope")
        return claims
