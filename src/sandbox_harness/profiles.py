from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from pydantic import BaseModel, ConfigDict, Field, model_validator


class SkillDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str = Field(min_length=1, max_length=160)
    version: str = Field(min_length=1, max_length=64)
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    supported_harnesses: tuple[str, ...] = Field(min_length=1)
    commands: tuple[str, ...] = ()
    python_modules: tuple[str, ...] = ()
    node_modules: tuple[str, ...] = ()
    required_capabilities: tuple[str, ...] = ()
    network_origins: tuple[str, ...] = ()
    requires_gpu: bool = False


class ChildExecutionProfile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str = Field(min_length=1, max_length=100)
    supported_harnesses: tuple[str, ...] = Field(min_length=1)
    allowed_skill_ids: tuple[str, ...] = ()
    allowed_capability_ids: tuple[str, ...] = ()
    allowed_operation_ids: tuple[str, ...] = ()
    allowed_network_origins: tuple[str, ...] = ()
    max_delegation_depth: int = Field(default=1, ge=0, le=4)
    fallback_profile_ids: tuple[str, ...] = ()


class RuntimeDependencyProfile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str = Field(min_length=1, max_length=100)
    supported_harnesses: tuple[str, ...] = Field(min_length=1)
    image_digests: dict[str, str]
    commands: tuple[str, ...] = ()
    python_modules: tuple[str, ...] = ()
    node_modules: tuple[str, ...] = ()
    dependency_capabilities: tuple[str, ...] = ()
    network_origins: tuple[str, ...] = ()
    gpu: bool = False
    priority: int = Field(default=100, ge=0, le=10000)

    @model_validator(mode="after")
    def exact_images(self):
        if set(self.image_digests) != set(self.supported_harnesses):
            raise ValueError("image_digests must exactly cover supported_harnesses")
        if any(len(value) != 64 or any(character not in "0123456789abcdef" for character in value)
               for value in self.image_digests.values()):
            raise ValueError("Runtime image digest must be a 64-character SHA-256")
        return self


class ProfileRequirements(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    commands: tuple[str, ...] = ()
    python_modules: tuple[str, ...] = ()
    node_modules: tuple[str, ...] = ()
    required_capability_ids: tuple[str, ...] = ()
    network_origins: tuple[str, ...] = ()
    requires_gpu: bool = False


class ProfileCatalogPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: int = Field(default=1, ge=1, le=1)
    child_execution_profiles: tuple[ChildExecutionProfile, ...]
    runtime_dependency_profiles: tuple[RuntimeDependencyProfile, ...]


@dataclass(frozen=True)
class ResolvedExecutionProfile:
    child_profile: ChildExecutionProfile
    runtime_profile: RuntimeDependencyProfile
    skill_ids: tuple[str, ...]
    skill_digests: dict[str, str]
    required_capability_ids: tuple[str, ...]
    network_origins: tuple[str, ...]
    handoff_required: bool


class ProfileResolver:
    def __init__(self, *, children: list[ChildExecutionProfile],
                 runtimes: list[RuntimeDependencyProfile], skills: list[SkillDescriptor]):
        self.children = self._unique(children, "ChildExecutionProfile")
        self.runtimes = self._unique(runtimes, "RuntimeDependencyProfile")
        self.skills = self._unique(skills, "Skill")

    @classmethod
    def from_json(cls, raw: str, *, expected_digest: str, skill_manifest: dict) -> "ProfileResolver":
        actual = hashlib.sha256(raw.encode()).hexdigest()
        if actual != expected_digest:
            raise ValueError("profile catalog digest mismatch")
        payload = ProfileCatalogPayload.model_validate(json.loads(raw))
        skills = [SkillDescriptor(
            id=row["id"], version=row["version"], digest=row["content_sha256"],
            supported_harnesses=tuple(row["supported_harnesses"]),
            commands=tuple(row.get("dependencies", {}).get("commands", [])),
            python_modules=tuple(row.get("dependencies", {}).get("python_modules", [])),
            node_modules=tuple(row.get("dependencies", {}).get("node_modules", [])),
            required_capabilities=tuple(row.get("required_capabilities", [])),
            network_origins=tuple(row.get("network_origins", [])),
            requires_gpu=bool(row.get("requires_gpu", False)),
        ) for row in skill_manifest["skills"]]
        return cls(children=list(payload.child_execution_profiles),
            runtimes=list(payload.runtime_dependency_profiles), skills=skills)

    @staticmethod
    def _unique(rows, label: str):
        result = {row.id: row for row in rows}
        if len(result) != len(rows):
            raise ValueError(f"Duplicate {label} ID")
        return result

    @staticmethod
    def _supports(profile: RuntimeDependencyProfile, harness: str,
                  requirements: ProfileRequirements) -> bool:
        return bool(
            harness in profile.supported_harnesses
            and requirements.requires_gpu == profile.gpu
            and set(requirements.commands) <= set(profile.commands)
            and set(requirements.python_modules) <= set(profile.python_modules)
            and set(requirements.node_modules) <= set(profile.node_modules)
            and set(requirements.required_capability_ids) <= set(profile.dependency_capabilities)
            and set(requirements.network_origins) <= set(profile.network_origins)
        )

    @staticmethod
    def probe(profile: RuntimeDependencyProfile, *, commands: set[str],
              python_modules: set[str], node_modules: set[str]) -> dict[str, tuple[str, ...]]:
        return {
            "commands": tuple(sorted(set(profile.commands) - commands)),
            "python_modules": tuple(sorted(set(profile.python_modules) - python_modules)),
            "node_modules": tuple(sorted(set(profile.node_modules) - node_modules)),
        }

    def resolve(self, harness: str, child_profile_id: str, skill_ids: tuple[str, ...],
                requirements: ProfileRequirements, *, current_runtime_profile_id: str = "",
                allow_fallback: bool = False) -> ResolvedExecutionProfile:
        try:
            child = self.children[child_profile_id]
        except KeyError as exc:
            raise KeyError("Unknown ChildExecutionProfile") from exc
        if harness not in child.supported_harnesses:
            raise ValueError("ChildExecutionProfile does not support Harness")
        selected: list[SkillDescriptor] = []
        for skill_id in dict.fromkeys(skill_ids):
            try:
                skill = self.skills[skill_id]
            except KeyError as exc:
                raise KeyError("Unknown Skill: " + skill_id) from exc
            if skill_id not in child.allowed_skill_ids:
                raise ValueError("Skill is not authorized by ChildExecutionProfile")
            if harness not in skill.supported_harnesses:
                raise ValueError("Skill does not support Harness")
            selected.append(skill)
        commands = set(requirements.commands)
        python_modules = set(requirements.python_modules)
        node_modules = set(requirements.node_modules)
        capabilities = set(requirements.required_capability_ids)
        network = set(requirements.network_origins)
        requires_gpu = requirements.requires_gpu
        for skill in selected:
            commands.update(skill.commands)
            python_modules.update(skill.python_modules)
            node_modules.update(skill.node_modules)
            capabilities.update(skill.required_capabilities)
            network.update(skill.network_origins)
            requires_gpu = requires_gpu or skill.requires_gpu
        if not capabilities <= set(child.allowed_capability_ids):
            raise ValueError("Capability is not authorized by ChildExecutionProfile")
        if not network <= set(child.allowed_network_origins):
            raise ValueError("Network origin is not authorized by ChildExecutionProfile")
        combined = ProfileRequirements(
            commands=tuple(sorted(commands)), python_modules=tuple(sorted(python_modules)),
            node_modules=tuple(sorted(node_modules)),
            required_capability_ids=tuple(sorted(capabilities)),
            network_origins=tuple(sorted(network)), requires_gpu=requires_gpu,
        )
        fallback_ids = set(child.fallback_profile_ids)
        candidates = [profile for profile in self.runtimes.values()
            if profile.id not in fallback_ids and self._supports(profile, harness, combined)]
        if not candidates and allow_fallback:
            candidates = [self.runtimes[profile_id] for profile_id in child.fallback_profile_ids
                if profile_id in self.runtimes and self._supports(self.runtimes[profile_id], harness, combined)]
        if not candidates:
            raise ValueError("No RuntimeDependencyProfile satisfies frozen requirements")
        def score(profile: RuntimeDependencyProfile):
            extras = (len(set(profile.commands) - commands)
                + len(set(profile.python_modules) - python_modules)
                + len(set(profile.node_modules) - node_modules)
                + len(set(profile.dependency_capabilities) - capabilities)
                + len(set(profile.network_origins) - network))
            return extras, profile.priority, profile.id
        runtime = min(candidates, key=score)
        return ResolvedExecutionProfile(
            child_profile=child, runtime_profile=runtime,
            skill_ids=tuple(sorted(skill.id for skill in selected)),
            skill_digests={skill.id: skill.digest for skill in sorted(selected, key=lambda row: row.id)},
            required_capability_ids=tuple(sorted(capabilities)), network_origins=tuple(sorted(network)),
            handoff_required=bool(current_runtime_profile_id and current_runtime_profile_id != runtime.id),
        )
