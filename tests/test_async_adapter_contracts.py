from sandbox_harness.common.catalog.media_catalog import (DEFAULT_MEDIA_CATALOG,MediaCatalog)
from sandbox_harness.common.execution.job_adapters import (BaseJobAdapter)
from sandbox_harness.common.models.model_routes import (ModelRoutes)
from sandbox_harness.common.sandbox.sandbox import (FakeSandboxBackend)
from sandbox_harness.common.sandbox.sandbox_aliyun import (AliyunSandboxBackend)
from sandbox_harness.common.sandbox.sandbox_opensandbox import (OpenSandboxBackend)
from sandbox_harness.services.controlled_worker.controlled_media_adapter import (ControlledMediaOperationAdapter)
from sandbox_harness.services.job_worker.harness_job import (HarnessJobAdapter)
from sandbox_harness.services.job_worker.image_decompose_adapter import (ImageDecomposeAdapter)
from sandbox_harness.services.job_worker.media_job_adapter import (MediaProviderJobAdapter)
from sandbox_harness.services.job_worker.media_render_adapter import (MediaRenderOperationAdapter)
from sandbox_harness.services.job_worker.reviewer import (ReviewerJobAdapter)
from sandbox_harness.services.site_worker.site_worker import (SiteWorker)


def test_every_external_async_adapter_declares_explicit_preflight():
    adapters=(HarnessJobAdapter,ControlledMediaOperationAdapter,ImageDecomposeAdapter,
        MediaProviderJobAdapter,MediaRenderOperationAdapter,ReviewerJobAdapter)
    assert all("preflight" in value.__dict__ for value in adapters)
    assert BaseJobAdapter.preflight not in {value.__dict__["preflight"] for value in adapters}
    assert "preflight" in SiteWorker.__dict__
    assert "preflight" in ModelRoutes.__dict__


def test_every_sandbox_backend_declares_create_preflight():
    assert all("preflight_create" in value.__dict__ for value in (
        FakeSandboxBackend,OpenSandboxBackend,AliyunSandboxBackend))


def test_every_enabled_media_adapter_has_exact_registered_contract():
    catalog=MediaCatalog()
    assert {item.adapter for item in DEFAULT_MEDIA_CATALOG}==catalog.contracts.adapters
    for item in DEFAULT_MEDIA_CATALOG:
        assert catalog.contracts.resolve(item.adapter).kind==item.kind
