"""Pure public-destination policy shared by outbound transports."""
from __future__ import annotations

import ipaddress
import re

DOMAIN_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")

def normalize_host(value: str) -> str:
    host = value.strip().strip("[]").rstrip(".").lower()
    if not host or "://" in host or any(character in host for character in "/?#@"):
        raise ValueError("Invalid proxy destination")
    try:
        return str(ipaddress.ip_address(host))
    except ValueError:
        try:
            host = host.encode("idna").decode("ascii")
        except UnicodeError as exc:
            raise ValueError("Invalid proxy destination") from exc
        if len(host) > 253 or not all(DOMAIN_LABEL.fullmatch(label) for label in host.split(".")):
            raise ValueError("Invalid proxy destination")
        return host


def public_address(value: str) -> bool:
    address = ipaddress.ip_address(value)
    return address.is_global and not address.is_multicast

