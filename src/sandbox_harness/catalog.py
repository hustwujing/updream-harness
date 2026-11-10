from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .domain import Harness


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
    def __init__(self, raw: str | None = None):
        value = json.loads(raw) if raw else DEFAULT_CATALOG
        if set(value) != {item.value for item in Harness}:
            raise ValueError("Catalog must configure codex, claude and deepseek")
        self.options = {Harness(key): HarnessOption.model_validate(option) for key, option in value.items()}
        self.known_models = {
            harness: {model.id: model for model in option.models}
            for harness, option in self.options.items()
        }
        self.availability: dict[tuple[Harness, str], dict[str, Any]] = {}

    def replace_models(self, discovered: dict[Harness | str, list[str]]) -> None:
        for raw_harness, identifiers in discovered.items():
            harness = Harness(raw_harness)
            option = self.options[harness]
            template = option.models[0]
            unique = list(dict.fromkeys(identifier for identifier in identifiers if identifier))
            if not unique:
                continue
            models = []
            for identifier in unique:
                model = self.known_models[harness].get(identifier) or ModelOption(
                    id=identifier,
                    label=model_label(identifier),
                    reasoning=template.reasoning,
                )
                self.known_models[harness][identifier] = model
                models.append(model)
            default_model = option.default_model if option.default_model in unique else unique[0]
            models.sort(key=lambda model: (model.id != default_model, model.id))
            self.options[harness] = option.model_copy(update={"default_model": default_model, "models": models})

    def replace_availability(self, results: dict[tuple[Harness, str], dict[str, Any]]) -> None:
        self.availability.update(results)

    def is_available(self, harness: Harness | str, model: str) -> bool:
        state = self.availability.get((Harness(harness), model))
        return state is None or state.get("status") != "unavailable"

    def public(self) -> list[dict]:
        result = []
        for harness, option in self.options.items():
            available = any(self.is_available(harness, model.id) for model in option.models)
            result.append({"id": harness.value, "label": option.label, "default_model": option.default_model,
                "available": available,
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
