from __future__ import annotations

import hashlib
import hmac
import re
import secrets


COOKIE = "sandbox_harness_session"
IDENTITY = re.compile(r"^[a-f0-9]{64}$")


class Sessions:
    def __init__(self, secret: str):
        self.secret = hashlib.sha256(secret.encode()).digest()

    def sign(self, identity: str) -> str:
        signature = hmac.new(self.secret, identity.encode(), hashlib.sha256).hexdigest()
        return identity + "." + signature

    def resolve(self, token: str | None) -> tuple[str, str, bool]:
        token = token or ""
        if "." in token:
            identity, signature = token.rsplit(".", 1)
            if IDENTITY.fullmatch(identity) and hmac.compare_digest(self.sign(identity), token):
                return hashlib.sha256(identity.encode()).hexdigest(), token, False
        identity = secrets.token_hex(32)
        return hashlib.sha256(identity.encode()).hexdigest(), self.sign(identity), True
