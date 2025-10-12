#!/usr/bin/env /usr/bin/python

import sys
import asyncio

from core.app_logging import getLogger

LOG = getLogger(__name__)

from core.exceptions import DBRestart, DBSchemaError, ConfigurationError
from core.app import App
from core.status import Status
from core.configuration.config import Config
from core.configuration.db_config import DBConfig
from core.util import check_environment
from database.db_manager import get_db
from server.ws_server import get_websocket


def wait_for_keyboard_interrupt():
    """Block until Ctrl+C is pressed (Windows only)."""
    import msvcrt

    while msvcrt.getch() != b"\x03":  # Ctrl+C
        pass


async def main():
    "connect DB and start servers"
    LOG.debug(f"{App.status=}")
    kb_task = None
    if sys.platform == "win32":
        # Start keyboard interrupt watcher in background thread
        kb_task = asyncio.create_task(asyncio.to_thread(wait_for_keyboard_interrupt))
    async with get_websocket():
        # LOG.debug(f"got websocket {ws=}")
        while True:
            # LOG.debug("Start DB, config")
            App.status_object.status = (
                Status.STATUS_DB_CFG
                if DBConfig.db_configuration
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
                    tasks = [asyncio.create_task(App.db_request_restart.wait())]
                    if kb_task:
                        tasks.append(kb_task)
                    done, _ = await asyncio.wait(
                        tasks, return_when=asyncio.FIRST_COMPLETED
                    )
                    if kb_task and kb_task in done:
                        raise KeyboardInterrupt
                    App.db_request_restart.clear()

            except DBSchemaError as exc:
                App.status_object.status = Status.STATUS_NO_DB
                LOG.error(f"DB unusable. ({exc})")
                break
            LOG.info("DB restarting")
            App.db_available.clear()
            App.db_failure.clear()
            App.db_restart.set()


# LOG.debug(f"{__name__} (main) module initialized")
if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    check_environment()
    try:
        App.initialize(__file__)
        asyncio.run(main())
    except KeyboardInterrupt:
        LOG.info("Stopped by KeyboardInterrupt")
