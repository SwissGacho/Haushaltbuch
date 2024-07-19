#!/usr/bin/env /usr/bin/python

import asyncio

from core.app_logging import getLogger

LOG = getLogger(__name__)

from core.exceptions import DBRestart, DBSchemaError, ConfigurationError
from core.app import App
from core.status import Status
from core.config import Config
from database.db import get_db
from server.ws_server import get_websocket


async def main():
    "connect DB and start servers"
    LOG.debug(f"{App.status=}")
    async with get_websocket() as ws:
        # LOG.debug(f"got websocket {ws=}")
        while True:
            LOG.debug(f"Start DB, config")
            App.status_object.status = (
                Status.STATUS_DB_CFG
                if App.configuration.get(Config.CONFIG_DB)
                else Status.STATUS_NO_DB
            )
            try:
                async with get_db() as db:
                    App.db_restart.clear()
                    if db:
                        # LOG.debug("DB available")
                        App.db_available.set()
                        try:
                            await App.db_ready()
                        except ConfigurationError as exc:
                            LOG.error(f"Error reading configuration from DB ({exc})")
                            App.status_object.status = Status.STATUS_NO_DB
                    else:
                        # LOG.debug("DB NOT available.")
                        App.db_failure.set()
                    LOG.info(f"App running. (Status: {App.status})")
                    await App.db_request_restart.wait()
                    App.db_request_restart.clear()

            except DBRestart:
                LOG.warning("DB Restart Exception")
            except DBSchemaError as exc:
                App.status_object.status = Status.STATUS_NO_DB
                LOG.error(f"DB unusable. ({exc})")
            LOG.info("DB restarting")
            App.db_available.clear()
            App.db_failure.clear()
            App.db_restart.set()


LOG.debug(f"{__name__} (main) module initialized")

if __name__ == "__main__":
    try:
        App.initialize(__file__)
        asyncio.run(main())
    except KeyboardInterrupt:
        LOG.info("Stopped by KeyboardInterrupt")
