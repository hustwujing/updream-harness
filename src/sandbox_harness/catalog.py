from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .domain import Harness
from .job_catalog import JobCatalog
from .job_contracts import CHILD_HARNESS_DESCRIPTOR
from .profiles import ProfileResolver


DEFAULT_CATALOG = {
    "codex": {
        "label": "Codex",
        "default_model": "gpt-5.6",
        "models": [{"id": "gpt-5.6", "label": "GPT-5.6", "reasoning": [
            {"id": "quick", "label": "快速", "native": "low"},
            {"id": "standard", "label": "标准", "native": "medium"},
            {"id": "deep", "label": "深入", "native": "high"},
            {"id": "maximum", "label": "最大", "native": "xhigh"},
        ]}],
    },
    "claude": {
        "label": "Claude",
        "default_model": "claude-4.6-sonnet",
        "models": [{"id": "claude-4.6-sonnet", "label": "Claude 4.6 Sonnet", "reasoning": [
            {"id": "quick", "label": "快速", "native": "low"},
            {"id": "standard", "label": "标准", "native": "medium"},
            {"id": "deep", "label": "深入", "native": "high"},
        ]}, {"id": "origin/claude-opus-5-5", "label": "原厂claude-opus-5-5", "reasoning": [
            {"id": "quick", "label": "快速", "native": "low"},
            {"id": "standard", "label": "标准", "native": "medium"},
            {"id": "deep", "label": "深入", "native": "high"},
        ]}],
    },
    "deepseek": {
        "label": "DeepSeek",
        "default_model": "deepseek-v4.1-flash",
        "models": [{"id": "deepseek-v4.1-flash", "label": "DeepSeek 4.1 Flash", "reasoning": [
            {"id": "standard", "label": "标准", "native": "high"},
            {"id": "deep", "label": "深入", "native": "high"},
        ]}],
    },
}


class ReasoningOption(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,31}$")
    label: str = Field(min_length=1, max_length=50)
    native: str = Field(min_length=1, max_length=50, exclude=True)


class ModelOption(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(min_length=1, max_length=160)
    label: str = Field(min_length=1, max_length=100)
    reasoning: list[ReasoningOption] = Field(min_length=1, max_length=8)

    @model_validator(mode="after")
    def unique_reasoning(self):
        if len({item.id for item in self.reasoning}) != len(self.reasoning):
            raise ValueError("Duplicate public reasoning level")
        return self


class HarnessOption(BaseModel):
    model_config = ConfigDict(extra="forbid")
    label: str = Field(min_length=1, max_length=50)
    default_model: str
    models: list[ModelOption] = Field(min_length=1, max_length=30)

    @model_validator(mode="after")
    def valid_default(self):
        if self.default_model not in {item.id for item in self.models}:
            raise ValueError("Harness default model is not declared")
        return self


@dataclass(frozen=True)
class ResolvedProfile:
    harness: Harness
    model: str
    reasoning_level: str
    reasoning_effort: str


class HarnessCatalog:
    def __init__(self, raw: str | None = None, *, job_catalog: JobCatalog | None = None,
                 profile_resolver: ProfileResolver | None = None):
        value = json.loads(raw) if raw else DEFAULT_CATALOG
        if set(value) != {item.value for item in Harness}:
            raise ValueError("Catalog must configure codex, claude and deepseek")
        self.options = {Harness(key): HarnessOption.model_validate(option) for key, option in value.items()}
        self.known_models = {
            harness: {model.id: model for model in option.models}
            for harness, option in self.options.items()
        }
        self.availability: dict[tuple[Harness, str], dict[str, Any]] = {}
        self.inventory_status: dict[Harness, dict[str,Any]] = {
            harness:{"status":"unknown","stale":True,"error":"","last_success_at":None}
            for harness in Harness}
        self.job_catalog = job_catalog
        self.profile_resolver = profile_resolver

    def public_execution_metadata(self) -> dict[str, list[dict]]:
        if not self.job_catalog or not self.profile_resolver:
            return {"operations": [], "capabilities": [], "skills": [],
                "child_execution_profiles": []}
        adapter,descriptor_id,descriptor_version,descriptor_digest=CHILD_HARNESS_DESCRIPTOR
        return {
            "operations": [{"id": row.id, "version": row.version,
                "digest": row.digest, "execution_mode": row.execution_mode,
                "parameter_schema":row.parameter_schema,
                "input_roles": list(row.input_roles), "output_roles": list(row.output_roles),
                "resources":row.resources,"billable":row.billable,"side_effects":row.side_effects,
                "runtime_profile_id":row.runtime_profile_id,"runtime_harness":row.runtime_harness,
                "guidance":row.guidance}
                for row in self.job_catalog.payload.operations],
            "capabilities": [{"id": row.id, "version": row.version,
                "digest": row.digest, "operations": list(row.operations),
                "parameter_schema":row.parameter_schema,
                "input_roles": list(row.input_roles), "output_roles": list(row.output_roles),
                "resources":row.resources,"billable":row.billable,"side_effects":row.side_effects}
                for row in self.job_catalog.payload.capabilities],
            "skills": [{"id": row.id, "version": row.version, "digest": row.digest,
                "supported_harnesses": list(row.supported_harnesses),
                "required_capabilities": list(row.required_capabilities),
                "network_origins": list(row.network_origins), "requires_gpu": row.requires_gpu}
                for row in self.profile_resolver.skills.values()],
            "child_execution_profiles": [{"id": row.id,
                "supported_harnesses": list(row.supported_harnesses),
                "max_delegation_depth": row.max_delegation_depth,
                "job_template":{"kind":"harness","adapter":adapter,
                    "descriptor_id":descriptor_id,"descriptor_version":descriptor_version,
                    "descriptor_digest":descriptor_digest,"child_profile_id":row.id},
                "parameter_schema":{"type":"object","required":["harness","model","objective"],
                    "properties":{"harness":{"enum":list(row.supported_harnesses)},
                        "model":{"type":"string","minLength":1},
                        "objective":{"type":"string","minLength":1}},"additionalProperties":True}}
                for row in self.profile_resolver.children.values()],
        }

    def replace_models(self, discovered: dict[Harness | str, list[str]]) -> None:
        candidate_options=dict(self.options)
        candidate_known={harness:dict(models) for harness,models in self.known_models.items()}
        for raw_harness, identifiers in discovered.items():
            harness = Harness(raw_harness)
            option = candidate_options[harness]
            template = option.models[0]
            unique = list(dict.fromkeys(identifier for identifier in identifiers if identifier))
            if not unique:
                continue
            if len(unique)>30:raise ValueError("Harness model inventory exceeds limit")
            models = []
            for identifier in unique:
                model = candidate_known[harness].get(identifier) or ModelOption(
                    id=identifier,
                    label=model_label(identifier),
                    reasoning=template.reasoning,
                )
                candidate_known[harness][identifier] = model
                models.append(model)
            default_model = option.default_model if option.default_model in unique else unique[0]
            models.sort(key=lambda model: (model.id != default_model, model.id))
            candidate_options[harness]=HarnessOption(label=option.label,
                default_model=default_model,models=models)
        self.options=candidate_options;self.known_models=candidate_known

    def replace_availability(self, results: dict[tuple[Harness, str], dict[str, Any]]) -> None:
        candidate=dict(self.availability)
        for (raw_harness,model),state in results.items():
            harness=Harness(raw_harness)
            if model not in {item.id for item in self.options[harness].models}:
                raise ValueError("Availability references unknown model")
            if state.get("status") not in {"available","busy","unknown","unavailable"}:
                raise ValueError("Invalid model availability state")
            candidate[(harness,model)]=dict(state)
        self.availability=candidate

    def is_available(self, harness: Harness | str, model: str) -> bool:
        state = self.availability.get((Harness(harness), model))
        return state is None or state.get("status") != "unavailable"

    def public(self) -> list[dict]:
        result = []
        for harness, option in self.options.items():
            available = any(self.is_available(harness, model.id) for model in option.models)
            result.append({"id": harness.value, "label": option.label, "default_model": option.default_model,
                "available": available,
                "inventory":self.inventory_status.get(harness,{"status":"unknown","stale":True}),
                "models": [{"id": model.id, "label": model.label,
                            **self.availability.get((harness, model.id), {"status": "unknown", "available": True}),
                            "reasoning": [{"id": level.id, "label": level.label} for level in model.reasoning]}
                           for model in option.models]})
        return result

    def resolve(self, harness: Harness | str, model: str | None, reasoning_level: str | None) -> ResolvedProfile:
        harness = Harness(harness)
        option = self.options[harness]
        model_id = model or option.default_model
        selected = next((item for item in option.models if item.id == model_id), None)
        if not selected:
            raise ValueError("Model is not configured for the selected Harness")
        if not self.is_available(harness, selected.id):
            raise ValueError("Model is not supported or authorized on the configured route; select another model")
        level_id = reasoning_level or selected.reasoning[min(1, len(selected.reasoning) - 1)].id
        level = next((item for item in selected.reasoning if item.id == level_id), None)
        if not level:
            raise ValueError("Reasoning level is not supported by the selected Harness model")
        return ResolvedProfile(harness, selected.id, level.id, level.native)


def model_label(identifier: str) -> str:
    words = identifier.replace("_", "-").split("-")
    labels = {"gpt": "GPT", "claude": "Claude", "deepseek": "DeepSeek", "codex": "Codex"}
    return " ".join(labels.get(word.lower(), word.capitalize()) for word in words)
