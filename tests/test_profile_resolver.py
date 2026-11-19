import hashlib
import json
from pathlib import Path

import pytest

from sandbox_harness.profiles import (
    ChildExecutionProfile,
    ProfileRequirements,
    ProfileResolver,
    RuntimeDependencyProfile,
    SkillDescriptor,
)


ROOT = Path(__file__).resolve().parents[1]


def resolver() -> ProfileResolver:
    skills = [
        SkillDescriptor(id="writing", version="1", digest="a" * 64,
            supported_harnesses=("codex", "claude", "deepseek"),
            summary="Writing",
            commands=("python3",), python_modules=(), node_modules=(),
            required_capabilities=(), network_origins=(), requires_gpu=False),
        SkillDescriptor(id="slides", version="1", digest="b" * 64,
            supported_harnesses=("codex", "claude"),summary="Slides",
            network_mode="required",commands=("libreoffice",),
            python_modules=("pptx",), node_modules=("pptxgenjs",),
            required_capabilities=("image.generate",), network_origins=("images",),
            requires_gpu=False),
    ]
    children = [ChildExecutionProfile(
        id="child-general", supported_harnesses=("codex", "claude", "deepseek"),
        allowed_skill_ids=("writing", "slides"), allowed_capability_ids=("image.generate",),
        allowed_operation_ids=("artifact.inspect",), allowed_network_origins=("images",),
        max_delegation_depth=1, fallback_profile_ids=("primary-full-cpu",),
    )]
    profiles = [
        RuntimeDependencyProfile(id="general-cpu", supported_harnesses=("codex", "claude", "deepseek"),
            image_digests={"codex": "c" * 64, "claude": "d" * 64, "deepseek": "e" * 64},
            commands=("python3",), python_modules=(), node_modules=(),
            dependency_capabilities=(), network_origins=(), gpu=False, priority=10),
        RuntimeDependencyProfile(id="office", supported_harnesses=("codex", "claude"),
            image_digests={"codex": "f" * 64, "claude": "1" * 64},
            commands=("python3", "libreoffice"), python_modules=("pptx",),
            node_modules=("pptxgenjs",), dependency_capabilities=("image.generate",),
            network_origins=("images",), gpu=False, priority=20),
        RuntimeDependencyProfile(id="primary-full-cpu", supported_harnesses=("codex", "claude", "deepseek"),
            image_digests={"codex": "2" * 64, "claude": "3" * 64, "deepseek": "4" * 64},
            commands=("python3", "libreoffice", "node"), python_modules=("pptx",),
            node_modules=("pptxgenjs",), dependency_capabilities=("image.generate",),
            network_origins=("images",), gpu=False, priority=100),
    ]
    return ProfileResolver(children=children, runtimes=profiles, skills=skills)


def test_resolver_unions_skill_dependencies_and_selects_minimal_profile():
    result = resolver().resolve("codex", "child-general", ("writing", "slides"), ProfileRequirements())
    assert result.runtime_profile.id == "office"
    assert result.required_capability_ids == ("image.generate",)
    assert result.network_origins == ("images",)
    assert result.skill_digests == {"slides": "b" * 64, "writing": "a" * 64}


def test_resolver_rejects_unknown_unauthorized_and_unsupported_combinations():
    value = resolver()
    with pytest.raises(KeyError, match="Unknown Skill"):
        value.resolve("codex", "child-general", ("missing",), ProfileRequirements())
    with pytest.raises(ValueError, match="does not support Harness"):
        value.resolve("deepseek", "child-general", ("slides",), ProfileRequirements())
    with pytest.raises(ValueError, match="not authorized"):
        value.resolve("codex", "child-general", ("writing",),
            ProfileRequirements(required_capability_ids=("video.generate",)))


def test_profile_change_requires_explicit_handoff_but_skill_change_does_not():
    value = resolver()
    same = value.resolve("codex", "child-general", ("writing",), ProfileRequirements(),
        current_runtime_profile_id="general-cpu")
    assert not same.handoff_required
    changed = value.resolve("codex", "child-general", ("slides",), ProfileRequirements(),
        current_runtime_profile_id="general-cpu")
    assert changed.runtime_profile.id == "office" and changed.handoff_required


def test_profile_fallback_must_be_explicitly_authorized():
    value = resolver()
    with pytest.raises(ValueError, match="No RuntimeDependencyProfile"):
        value.resolve("deepseek", "child-general", ("writing",),
            ProfileRequirements(commands=("node",)), allow_fallback=False)
    result = value.resolve("deepseek", "child-general", ("writing",),
        ProfileRequirements(commands=("node",)), allow_fallback=True)
    assert result.runtime_profile.id == "primary-full-cpu"


def test_example_profile_catalog_is_digest_verified_and_covers_initial_profiles():
    raw=(ROOT/'deploy/profile-catalog.example.json').read_text()
    digest=hashlib.sha256(raw.encode()).hexdigest()
    manifest=json.loads((ROOT/'runtime/skills/manifest.json').read_text())
    value=ProfileResolver.from_json(raw,expected_digest=digest,skill_manifest=manifest)
    assert {'primary-full-cpu','general-cpu','media-cpu','office','web-browser','data-analysis'} <= set(value.runtimes)
    assert set(value.skills)=={row['id'] for row in manifest['skills']}
    capabilities={'image.generate','video.generate','video.edit','audio.generate',
        'model3d.generate','model3d.transform'}
    assert set(value.children['child-general'].allowed_capability_ids)==capabilities
    assert set(value.runtimes['primary-full-cpu'].dependency_capabilities)==capabilities
    assert set(value.children['child-general'].allowed_network_origins)=={
        'image-provider','video-provider','audio-provider','model3d-provider'}
    media=value.resolve('claude','child-general',('media-processing',),ProfileRequirements())
    assert media.runtime_profile.id=='media-cpu'
    with pytest.raises(ValueError,match='profile catalog digest'):
        ProfileResolver.from_json(raw,expected_digest='0'*64,skill_manifest=manifest)
