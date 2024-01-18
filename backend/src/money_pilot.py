#!/usr/bin/env /usr/bin/python

import asyncio

from core.app import app
from core.db import get_db
from server.ws_server import get_websocket
from core.app_logging import getLogger

LOG = getLogger(__name__)


async def main():
    LOG.debug(f"{app.status=}")
    async with (
        get_db() as db,
        get_websocket() as ws,
    ):
        LOG.info("App running")

        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOG.info("Stopped by KeyboardInterrupt")
