#!/usr/bin/env /usr/bin/python

import asyncio

from core.app import App
from core.db import get_db
from server.ws_server import get_websocket
from core.app_logging import getLogger

LOG = getLogger(__name__)


async def main():
    "connect DB and start servers"
    LOG.debug(f"{App.status=}")
    async with (
        get_db() as db,
        get_websocket() as ws,
    ):
        LOG.info("App running")

        await asyncio.Future()


LOG.debug(f"{__name__} (main) module initialized")

if __name__ == "__main__":
    try:
        App.initialize()
        asyncio.run(main())
    except KeyboardInterrupt:
        LOG.info("Stopped by KeyboardInterrupt")
