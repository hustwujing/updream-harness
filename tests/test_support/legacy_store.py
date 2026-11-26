"""Test-only single-database composition for legacy transaction regression tests."""
from sandbox_harness.common.db.models import *  # noqa: F403
from sandbox_harness.common.db.base import BaseConnections
from test_support.upgrades import UpgradesTransactions
from sandbox_harness.common.db.capabilities import CapabilitiesTransactions
from sandbox_harness.common.db.views import ViewsTransactions
from sandbox_harness.common.db.pause import PauseTransactions
from sandbox_harness.common.db.budgets import BudgetsTransactions
from sandbox_harness.common.db.job_activity import JobActivityTransactions
from sandbox_harness.common.db.job_groups import JobGroupsTransactions
from sandbox_harness.common.db.job_attempts import JobAttemptsTransactions
from sandbox_harness.common.db.allocations import AllocationsTransactions
from sandbox_harness.common.db.cleanup import CleanupTransactions
from sandbox_harness.common.db.job_terminal import JobTerminalTransactions
from sandbox_harness.common.db.job_artifacts import JobArtifactsTransactions
from sandbox_harness.common.db.auth import AuthTransactions
from sandbox_harness.common.db.events import EventsTransactions
from sandbox_harness.common.db.threads import ThreadsTransactions
from sandbox_harness.common.db.sandbox import SandboxTransactions
from sandbox_harness.common.db.turns import TurnsTransactions
from sandbox_harness.common.db.files import FilesTransactions
from sandbox_harness.common.db.interactions import InteractionsTransactions
from sandbox_harness.common.db.media_references import MediaReferencesTransactions
from sandbox_harness.common.db.media_drafts import MediaDraftsTransactions
from sandbox_harness.common.db.media_jobs import MediaJobsTransactions
from sandbox_harness.common.db.sites import SitesTransactions
from sandbox_harness.common.db.usage import UsageTransactions
from sandbox_harness.common.db.provider_circuits import ProviderCircuitTransactions


class Store(BaseConnections, UpgradesTransactions, CapabilitiesTransactions,
            ViewsTransactions, PauseTransactions, BudgetsTransactions,
            JobActivityTransactions, JobGroupsTransactions, JobAttemptsTransactions,
            AllocationsTransactions, CleanupTransactions, JobTerminalTransactions,
            JobArtifactsTransactions, AuthTransactions, EventsTransactions,
            UsageTransactions,
            ProviderCircuitTransactions,
            ThreadsTransactions, SandboxTransactions, TurnsTransactions,
            FilesTransactions, InteractionsTransactions, MediaReferencesTransactions,
            MediaDraftsTransactions, MediaJobsTransactions, SitesTransactions):
    """Preserves historical transaction tests without shipping a production facade."""
