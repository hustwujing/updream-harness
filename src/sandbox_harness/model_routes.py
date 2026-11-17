from __future__ import annotations

from dataclasses import dataclass
import json
import os
import re
from urllib.parse import urlsplit

from .config import Settings
from .domain import Harness


@dataclass(frozen=True)
class ModelRoute:
    base_url: str
    credential_env: str

    @property
    def credential(self) -> str:
        return os.environ.get(self.credential_env, "")


class ModelRoutes:
    def __init__(self, settings: Settings):
        self.settings = settings
        raw = json.loads(settings.model_routes_json) if settings.model_routes_json else {}
        if not isinstance(raw, dict):
            raise ValueError("Model routes must be an object")
        self.overrides: dict[tuple[Harness, str], ModelRoute] = {}
        for identity, value in raw.items():
            if not isinstance(identity, str) or "/" not in identity or not isinstance(value, dict):
                raise ValueError("Invalid model route")
            harness_name, model = identity.split("/", 1)
            harness = Harness(harness_name)
            base_url = self.canonical_base_url(str(value.get("base_url") or ""))
            credential_env = str(value.get("credential_env") or "")
            if (not base_url or not model or len(model) > 160
                    or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", credential_env)):
                raise ValueError("Invalid model route identity or credential environment")
            self.overrides[(harness, model)] = ModelRoute(base_url, credential_env)
        self.defaults={harness:ModelRoute(self.canonical_base_url(
            getattr(settings,"upstream_"+harness.value)),
            "SANDBOX_HARNESS_UPSTREAM_"+harness.value.upper()+"_KEY") for harness in Harness}

    @staticmethod
    def canonical_base_url(value:str)->str:
        value=value.strip()
        if not value:return ""
        parsed=urlsplit(value)
        if (parsed.scheme not in {"http","https"} or not parsed.netloc
                or parsed.username or parsed.password or parsed.query or parsed.fragment):
            raise ValueError("Invalid model route base URL")
        path=parsed.path.rstrip("/")
        if "//" in path:raise ValueError("Invalid model route base URL")
        return parsed.scheme+"://"+parsed.netloc+path

    def default(self, harness: Harness | str) -> ModelRoute:
        harness = Harness(harness)
        return self.defaults[harness]

    def resolve(self, harness: Harness | str, model: str) -> ModelRoute:
        harness = Harness(harness)
        return self.overrides.get((harness, model), self.default(harness))

    def configured_models(self, harness: Harness | str) -> list[str]:
        harness = Harness(harness)
        return sorted(model for (candidate, model) in self.overrides if candidate == harness)
