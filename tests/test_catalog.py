import json
import pytest

from sandbox_harness.catalog import HarnessCatalog


def test_public_reasoning_level_maps_to_native_harness_effort():
    catalog = HarnessCatalog()
    assert catalog.resolve("codex", "gpt-5.6", "maximum").reasoning_effort == "xhigh"
    assert catalog.resolve("claude", "claude-4.6-sonnet", "deep").reasoning_effort == "high"
    assert catalog.resolve("deepseek", "deepseek-v4.1-flash", "standard").reasoning_effort == "medium"
    public = catalog.public()
    assert all("native" not in level for harness in public for model in harness["models"] for level in model["reasoning"])


def test_unsupported_model_or_reasoning_level_is_rejected():
    catalog = HarnessCatalog()
    with pytest.raises(ValueError, match="Model"):
        catalog.resolve("claude", "invented", "deep")
    with pytest.raises(ValueError, match="Reasoning"):
        catalog.resolve("deepseek", "deepseek-v4.1-flash", "maximum")

