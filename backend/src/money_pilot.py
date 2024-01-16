#!/usr/bin/env /usr/bin/python

import asyncio

from core.status import app, Status
from core.config import get_db_config
from core.db import get_db
from server.ws_server import get_websocket


async def main():
    app.status = Status.STATUS_UNCONFIGURED
    db_cfg = get_db_config()
    # print(f"{app.status=}, {db_cfg=}")

    async with (
        get_db(db_cfg) as db,
        get_websocket() as ws,
    ):
        # print(f"{app.status=}")
        print("App running")

        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by KeyboardInterrupt")
