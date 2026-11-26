from __future__ import annotations

import asyncio
import copy
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import time
from typing import Any

import httpx

from sandbox_harness.common.observability.observability import (event)
from sandbox_harness.common.catalog.catalog import (HarnessCatalog)
from sandbox_harness.common.configuration.config import (Settings)
from sandbox_harness.common.contracts.domain import (Harness)
from sandbox_harness.common.models.model_routes import (ModelRoutes)
from sandbox_harness.common.models.model_health import (classify_response, reduce_health)


logger = logging.getLogger(__name__)


def compatible_models(harness: Harness | str, identifiers: list[str]) -> list[str]:
    harness = Harness(harness)
    if harness == Harness.CODEX:
        allowed = lambda value: value.startswith(("gpt-5", "gpt-6")) and not any(
            marker in value for marker in ("-audio", "-realtime", "-chat")
        )
    elif harness == Harness.CLAUDE:
        allowed = lambda value: value.startswith(("claude-", "origin/claude-"))
    else:
        allowed = lambda value: value.startswith("deepseek-")
    return sorted(dict.fromkeys(value for value in identifiers
        if isinstance(value,str) and 0<len(value)<=160 and allowed(value)))


def bounded_models(harness:Harness|str,identifiers:list[str],priority:list[str],limit:int)->tuple[list[str],bool]:
    compatible=compatible_models(harness,identifiers+priority);allowed=set(compatible)
    ordered=[]
    for value in priority:
        if value in allowed and value not in ordered:ordered.append(value)
    ordered.extend(value for value in compatible if value not in ordered)
    return ordered[:limit],len(ordered)>limit


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
        self.snapshot = settings.model_catalog_snapshot_path or settings.data_root / "model-catalog.json"
        self.routes = ModelRoutes(settings)
        self.last_refresh_at: str | None = None
        self.last_attempt_at: str | None = None
        self.last_discovered = 0
        self.last_available = 0
        self.inventory_status={harness:{"status":"unknown","stale":True,
            "error":"","last_success_at":None,"truncated":False,"discovered_count":0}
            for harness in Harness}

    def load_snapshot(self) -> bool:
        previous=(copy.deepcopy(self.catalog.options),copy.deepcopy(self.catalog.known_models),
            copy.deepcopy(self.catalog.availability),copy.deepcopy(self.catalog.inventory_status),
            self.last_refresh_at,self.last_attempt_at,self.last_discovered,self.last_available,
            copy.deepcopy(self.inventory_status))
        try:
            value = json.loads(self.snapshot.read_text())
            if value.get("version") not in (1, 2, 3, 4) or not isinstance(value.get("models"), dict):
                raise ValueError("Unsupported model catalog snapshot")
            if set(value["models"])!={harness.value for harness in Harness}:
                raise ValueError("Incomplete model catalog snapshot")
            discovered={}
            for harness in Harness:
                identifiers=value["models"].get(harness.value)
                if not isinstance(identifiers,list):raise ValueError("Incomplete model catalog snapshot")
                if any(not isinstance(identifier,str) or not identifier or len(identifier)>160
                        for identifier in identifiers):
                    raise ValueError("Invalid model catalog snapshot identity")
                priority=self.routes.configured_models(harness)+[self.catalog.options[harness].default_model]
                models,truncated=bounded_models(harness,identifiers,priority,
                    self.settings.model_catalog_max_models_per_harness)
                if not models:raise ValueError("Snapshot has no compatible models")
                discovered[harness]=models
            self.catalog.replace_models(discovered)
            availability = {}
            if value.get("version") in (2, 3, 4):
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
            metadata=value.get("metadata",{}) if value.get("version")==4 else {}
            inventory=metadata.get("inventory",{}) if isinstance(metadata,dict) else {}
            restored={}
            for harness in Harness:
                state=inventory.get(harness.value,{}) if isinstance(inventory,dict) else {}
                restored[harness]={"status":state.get("status","snapshot"),"stale":True,
                    "error":state.get("error",""),"last_success_at":state.get("last_success_at"),
                    "truncated":bool(state.get("truncated",False)),
                    "discovered_count":int(state.get("discovered_count",0))}
            self.inventory_status=restored;self.catalog.inventory_status=copy.deepcopy(restored)
            self.last_refresh_at=metadata.get("last_refresh_at") if isinstance(metadata,dict) else value.get("updated_at")
            self.last_attempt_at=metadata.get("last_attempt_at") if isinstance(metadata,dict) else value.get("updated_at")
            self.last_discovered=int(metadata.get("discovered",sum(map(len,discovered.values())))) if isinstance(metadata,dict) else sum(map(len,discovered.values()))
            self.last_available=int(metadata.get("available",sum(
                state.get("status")=="available" for state in availability.values()))) if isinstance(metadata,dict) else sum(
                    state.get("status")=="available" for state in availability.values())
            return bool(discovered)
        except FileNotFoundError:
            return False
        except Exception:
            (self.catalog.options,self.catalog.known_models,self.catalog.availability,
                self.catalog.inventory_status,self.last_refresh_at,self.last_attempt_at,
                self.last_discovered,self.last_available,self.inventory_status)=previous
            logger.exception("Ignoring invalid model catalog snapshot")
            return False

    async def refresh(self) -> dict[Harness, list[str]]:
        previous=(copy.deepcopy(self.catalog.options),copy.deepcopy(self.catalog.known_models),
            copy.deepcopy(self.catalog.availability),copy.deepcopy(self.catalog.inventory_status),
            self.last_refresh_at,self.last_attempt_at,self.last_discovered,self.last_available,
            copy.deepcopy(self.inventory_status))
        attempted_at=datetime.now(timezone.utc).isoformat()
        async with httpx.AsyncClient(
            timeout=self.settings.model_catalog_timeout_seconds,
            trust_env=False,
            transport=self.transport,
        ) as client:
            results = await asyncio.gather(*(
                self._fetch(client, harness) for harness in Harness
            ), return_exceptions=True)
            discovered: dict[Harness, list[str]] = {};inventory={};all_fresh=True
            for harness, result in zip(Harness, results, strict=True):
                protected=self.routes.configured_models(harness)+[
                    self.catalog.options[harness].default_model]
                if isinstance(result, Exception):
                    event("model.catalog.discovery_failed",level=logging.WARNING,harness=harness.value,exc=result)
                    fallback=protected+[model.id for model in self.catalog.options[harness].models]
                    models,truncated=bounded_models(harness,[],fallback,
                        self.settings.model_catalog_max_models_per_harness)
                    discovered[harness]=models;all_fresh=False
                    prior=self.inventory_status.get(harness,{})
                    inventory[harness]={"status":"stale","stale":True,
                        "error":type(result).__name__,"last_success_at":prior.get("last_success_at"),
                        "truncated":truncated,"discovered_count":prior.get("discovered_count",0)}
                else:
                    models,truncated=bounded_models(harness,result,protected,
                        self.settings.model_catalog_max_models_per_harness)
                    discovered[harness]=models
                    route_configured=bool(self.routes.default(harness).base_url)
                    status="fresh" if route_configured else "unconfigured"
                    inventory[harness]={"status":status,"stale":False,"error":"",
                        "last_success_at":attempted_at if route_configured else None,
                        "truncated":truncated,"discovered_count":len(compatible_models(harness,result))
                            if route_configured else 0}
            try:
                availability={}
                if self.settings.model_probe_enabled:
                    observations=await self._probe_all(client,discovered)
                    availability={key:reduce_health(self.catalog.availability.get(key,{}),state,
                        failures=self.settings.model_probe_failure_threshold,
                        successes=self.settings.model_probe_recovery_threshold)
                        for key,state in observations.items()}
                self.catalog.replace_models(discovered)
                if availability:self.catalog.replace_availability(availability)
                self.inventory_status=inventory;self.catalog.inventory_status=copy.deepcopy(inventory)
                self.last_available=(sum(state["status"]=="available" for state in availability.values())
                    if self.settings.model_probe_enabled else sum(map(len,discovered.values())))
                self.last_discovered=sum(state["discovered_count"] for state in inventory.values()
                    if state["status"]=="fresh")
                self.last_attempt_at=attempted_at
                if all_fresh:self.last_refresh_at=attempted_at
                self._save_snapshot()
            except Exception:
                (self.catalog.options,self.catalog.known_models,self.catalog.availability,
                    self.catalog.inventory_status,self.last_refresh_at,self.last_attempt_at,
                    self.last_discovered,self.last_available,self.inventory_status)=previous
                raise
        return discovered

    def status(self) -> dict[str, Any]:
        return {"enabled": self.settings.model_catalog_discovery,
                "probe_enabled": self.settings.model_probe_enabled,
                "last_refresh_at": self.last_refresh_at,
                "last_attempt_at":self.last_attempt_at,
                "discovered": self.last_discovered,
                "available": self.last_available,
                "stale":any(state.get("stale") for state in self.inventory_status.values()),
                "inventory":{harness.value:state for harness,state in self.inventory_status.items()}}

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
        if not base:
            return {"status":"unavailable","available":False,"checked_at":checked_at,
                "latency_ms":0,"error_code":"model_route_unconfigured"}
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
            event("model.probe.failed",level=logging.WARNING,harness=harness.value,model=model,exc=exc)
            return {"status": "busy", "available": True, "checked_at": checked_at,
                    "latency_ms": round((time.monotonic() - started) * 1000), "error_code": "network_error"}

    def _save_snapshot(self) -> None:
        value = {
            "version": 4,
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
            "metadata":{"last_refresh_at":self.last_refresh_at,
                "last_attempt_at":self.last_attempt_at,"discovered":self.last_discovered,
                "available":self.last_available,
                "inventory":{harness.value:state for harness,state in self.inventory_status.items()}},
        }
        self.snapshot.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.snapshot.with_suffix(".tmp")
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
        temporary.replace(self.snapshot)
