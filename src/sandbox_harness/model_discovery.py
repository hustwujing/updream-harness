from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import time
from typing import Any

import httpx

from .catalog import HarnessCatalog
from .config import Settings
from .domain import Harness
from .model_routes import ModelRoutes
from .model_health import classify_response, reduce_health


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
        self.routes = ModelRoutes(settings)
        self.last_refresh_at: str | None = None
        self.last_discovered = 0
        self.last_available = 0

    def load_snapshot(self) -> bool:
        try:
            value = json.loads(self.snapshot.read_text())
            if value.get("version") not in (1, 2, 3) or not isinstance(value.get("models"), dict):
                raise ValueError("Unsupported model catalog snapshot")
            discovered = {
                Harness(key): compatible_models(key, identifiers)
                for key, identifiers in value["models"].items()
                if key in {item.value for item in Harness} and isinstance(identifiers, list)
            }
            self.catalog.replace_models(discovered)
            if value.get("version") in (2, 3):
                availability = {}
                for harness_name, models in value.get("availability", {}).items():
                    if harness_name not in {item.value for item in Harness} or not isinstance(models, dict):
                        continue
                    for model, state in models.items():
                        if isinstance(state, dict) and state.get("status") in {"available", "busy", "unknown", "unavailable"}:
                            if value.get("version") == 2 and state.get("status") == "unavailable":
                                # Legacy snapshots lack the reason body / streak evidence.
                                state = {**state, "status": "unknown", "available": True}
                            availability[(Harness(harness_name), model)] = state
                self.catalog.replace_availability(availability)
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
                if self.settings.model_probe_enabled:
                    availability = await self._probe_all(client, discovered)
                    availability = {key: reduce_health(self.catalog.availability.get(key, {}), state,
                        failures=self.settings.model_probe_failure_threshold,
                        successes=self.settings.model_probe_recovery_threshold) for key, state in availability.items()}
                    self.catalog.replace_availability(availability)
                    self.last_available = sum(state["status"] == "available" for state in availability.values())
                else:
                    self.last_available = sum(len(models) for models in discovered.values())
                self.last_discovered = sum(len(models) for models in discovered.values())
                self.last_refresh_at = datetime.now(timezone.utc).isoformat()
                self._save_snapshot()
        return discovered

    def status(self) -> dict[str, Any]:
        return {"enabled": self.settings.model_catalog_discovery,
                "probe_enabled": self.settings.model_probe_enabled,
                "last_refresh_at": self.last_refresh_at,
                "discovered": self.last_discovered,
                "available": self.last_available}

    async def _fetch(self, client: httpx.AsyncClient, harness: Harness) -> list[str]:
        route = self.routes.default(harness)
        base = route.base_url
        if not base:
            return self.routes.configured_models(harness)
        key = route.credential
        headers = {"authorization": "Bearer " + key, "x-api-key": key} if key else {}
        url = base + ("/models" if base.endswith("/v1") else "/v1/models")
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        models = compatible_models(harness, parse_model_ids(response.json()) + self.routes.configured_models(harness))
        if not models:
            raise ValueError("Upstream returned no compatible models")
        return models

    async def _probe_all(self, client: httpx.AsyncClient,
                         discovered: dict[Harness, list[str]]) -> dict[tuple[Harness, str], dict[str, Any]]:
        semaphore = asyncio.Semaphore(self.settings.model_probe_concurrency)

        async def limited(harness: Harness, model: str):
            async with semaphore:
                return (harness, model), await self._probe(client, harness, model)

        rows = await asyncio.gather(*(
            limited(harness, model)
            for harness, models in discovered.items()
            for model in models
        ))
        return dict(rows)

    async def _probe(self, client: httpx.AsyncClient, harness: Harness, model: str) -> dict[str, Any]:
        route = self.routes.resolve(harness, model)
        base = route.base_url
        key = route.credential
        headers = {"authorization": "Bearer " + key, "x-api-key": key} if key else {}
        if harness == Harness.CODEX:
            url = base + "/responses"
            body = {"model": model, "input": "Reply OK.", "max_output_tokens": 8, "stream": False}
        else:
            url = base + ("/messages" if base.endswith("/v1") else "/v1/messages")
            headers["anthropic-version"] = "2023-06-01"
            body = {"model": model, "max_tokens": 8, "messages": [{"role": "user", "content": "Reply OK."}],
                    "stream": False}
        started = time.monotonic()
        checked_at = datetime.now(timezone.utc).isoformat()
        try:
            response = await client.post(url, headers=headers, json=body,
                                         timeout=self.settings.model_probe_timeout_seconds)
            latency_ms = round((time.monotonic() - started) * 1000)
            status, reason = classify_response(response)
            return {"status": status, "available": status != "unavailable", "checked_at": checked_at,
                    "latency_ms": latency_ms, "error_code": reason}
        except httpx.TimeoutException:
            return {"status": "busy", "available": True, "checked_at": checked_at,
                    "latency_ms": round((time.monotonic() - started) * 1000), "error_code": "timeout"}
        except httpx.HTTPError as exc:
            logger.warning("Model availability probe failed for %s/%s: %s", harness.value, model, exc)
            return {"status": "busy", "available": True, "checked_at": checked_at,
                    "latency_ms": round((time.monotonic() - started) * 1000), "error_code": "network_error"}

    def _save_snapshot(self) -> None:
        value = {
            "version": 3,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "models": {
                harness.value: [model.id for model in option.models]
                for harness, option in self.catalog.options.items()
            },
            "availability": {
                harness.value: {
                    model.id: self.catalog.availability[(harness, model.id)]
                    for model in option.models if (harness, model.id) in self.catalog.availability
                }
                for harness, option in self.catalog.options.items()
            },
        }
        self.snapshot.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.snapshot.with_suffix(".tmp")
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
        temporary.replace(self.snapshot)
