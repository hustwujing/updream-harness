from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
from typing import Any

import httpx

from .catalog import HarnessCatalog
from .config import Settings
from .domain import Harness


logger = logging.getLogger(__name__)


def compatible_models(harness: Harness | str, identifiers: list[str]) -> list[str]:
    harness = Harness(harness)
    if harness == Harness.CODEX:
        allowed = lambda value: value.startswith(("gpt-5", "gpt-6")) and not any(
            marker in value for marker in ("-audio", "-realtime", "-chat")
        )
    elif harness == Harness.CLAUDE:
        allowed = lambda value: value.startswith("claude-")
    else:
        allowed = lambda value: value.startswith("deepseek-")
    return sorted(dict.fromkeys(value for value in identifiers if allowed(value)))


def parse_model_ids(value: Any) -> list[str]:
    rows = value.get("data", value.get("models", [])) if isinstance(value, dict) else value
    if not isinstance(rows, list):
        raise ValueError("Model catalog response is not a list")
    result = []
    for row in rows:
        if isinstance(row, dict):
            identifier = row.get("id") or row.get("name")
            if isinstance(identifier, str) and identifier:
                result.append(identifier)
    return result


class ModelDiscovery:
    def __init__(self, settings: Settings, catalog: HarnessCatalog, transport: httpx.AsyncBaseTransport | None = None):
        self.settings = settings
        self.catalog = catalog
        self.transport = transport
        self.snapshot = settings.data_root / "model-catalog.json"

    def load_snapshot(self) -> bool:
        try:
            value = json.loads(self.snapshot.read_text())
            if value.get("version") != 1 or not isinstance(value.get("models"), dict):
                raise ValueError("Unsupported model catalog snapshot")
            discovered = {
                Harness(key): compatible_models(key, identifiers)
                for key, identifiers in value["models"].items()
                if key in {item.value for item in Harness} and isinstance(identifiers, list)
            }
            self.catalog.replace_models(discovered)
            return bool(discovered)
        except FileNotFoundError:
            return False
        except Exception:
            logger.exception("Ignoring invalid model catalog snapshot")
            return False

    async def refresh(self) -> dict[Harness, list[str]]:
        async with httpx.AsyncClient(
            timeout=self.settings.model_catalog_timeout_seconds,
            trust_env=False,
            transport=self.transport,
        ) as client:
            results = await asyncio.gather(*(
                self._fetch(client, harness) for harness in Harness
            ), return_exceptions=True)
        discovered: dict[Harness, list[str]] = {}
        for harness, result in zip(Harness, results, strict=True):
            if isinstance(result, Exception):
                logger.warning("Model catalog discovery failed for %s: %s", harness.value, result)
            elif result:
                discovered[harness] = result
        if discovered:
            self.catalog.replace_models(discovered)
            self._save_snapshot()
        return discovered

    async def _fetch(self, client: httpx.AsyncClient, harness: Harness) -> list[str]:
        base = getattr(self.settings, "upstream_" + harness.value).rstrip("/")
        if not base:
            return []
        key = os.environ.get("SANDBOX_HARNESS_UPSTREAM_" + harness.value.upper() + "_KEY", "")
        headers = {"authorization": "Bearer " + key, "x-api-key": key} if key else {}
        url = base + ("/models" if base.endswith("/v1") else "/v1/models")
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        models = compatible_models(harness, parse_model_ids(response.json()))
        if not models:
            raise ValueError("Upstream returned no compatible models")
        return models

    def _save_snapshot(self) -> None:
        value = {
            "version": 1,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "models": {
                harness.value: [model.id for model in option.models]
                for harness, option in self.catalog.options.items()
            },
        }
        self.snapshot.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.snapshot.with_suffix(".tmp")
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
        temporary.replace(self.snapshot)
