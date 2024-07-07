#!/usr/bin/env /usr/bin/python

import asyncio

from core.exceptions import DBRestart
from core.app import App
from database.db import get_db
from server.ws_server import get_websocket
from core.app_logging import getLogger

LOG = getLogger(__name__)


async def main():
    "connect DB and start servers"
    LOG.debug(f"{App.status=}")
    async with get_websocket() as ws:
        # LOG.debug(f"got websocket {ws=}")
        while True:
            try:
                async with get_db() as db:
                    # LOG.debug(f"got {db=}")
                    LOG.info(f"App running. (Status: {App.status})")

                    await asyncio.Future()
            except DBRestart:
                LOG.info("DB restarted")


LOG.debug(f"{__name__} (main) module initialized")

if __name__ == "__main__":
    try:
        App.initialize()
        asyncio.run(main())
    except KeyboardInterrupt:
        LOG.info("Stopped by KeyboardInterrupt")
