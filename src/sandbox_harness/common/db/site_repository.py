"""Sites authority used by the independently deployed SiteWorker.

The worker calls only generation-fenced, site-local methods. API command
admission remains a separate conversion task and is not exposed as a worker
credential.
"""
from .domain_database import DomainDatabase
from .sites import SitesTransactions


class SiteRepository(DomainDatabase, SitesTransactions):
    def __init__(self, database_url: str):
        super().__init__("sites", database_url)
