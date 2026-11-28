#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json

from sandbox_harness.common.configuration.databases import DatabaseSettings
from sandbox_harness.common.db.checkpoint_migration import migrate_checkpoint_versions


async def main():
    database_url=DatabaseSettings().for_service('platform-worker')['core']
    print(json.dumps(await migrate_checkpoint_versions(database_url),sort_keys=True))


if __name__=='__main__':asyncio.run(main())
