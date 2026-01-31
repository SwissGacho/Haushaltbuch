#!/usr/bin/env /usr/bin/python
"""Main module for Money Pilot backend application."""

import sys
import os
import asyncio
from asyncio import (
    create_task as aio_create_task,
    to_thread as aio_to_thread,
    wait as aio_wait,
    FIRST_COMPLETED,
)
from typing import Optional

from core.app_logging import getLogger

LOG = getLogger(__name__)

from core.exceptions import DBSchemaError, ConfigurationError
from core.app import App
from core.status import Status
from core.configuration.db_config import DBConfig
from core.util import check_environment
from database.db_manager import get_db
from server.ws_server import get_websocket


def wait_for_keyboard_interrupt():
    """Block until Ctrl+C is pressed (Windows only)."""
    import msvcrt  # pylint: disable=import-outside-toplevel

    while msvcrt.getch() != b"\x03":  # Ctrl+C
        pass


async def main():
    "connect DB and start servers"
    app_version = os.getenv("VERSION", "development")
    LOG.info(f"Starting Money Pilot backend application - Version: {app_version}")
    LOG.debug(f"{App.status=}")
    kb_task: Optional[asyncio.Task] = None
    if sys.platform == "win32":
        # Start keyboard interrupt watcher in background thread
        kb_task = aio_create_task(
            aio_to_thread(wait_for_keyboard_interrupt), name="kb_watcher"
        )
    async with get_websocket() as ws:
        # LOG.debug(f"got websocket {ws=}")
        while True:
            # LOG.debug("Start DB, config")
            App.status = (
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
                            App.status = Status.STATUS_NO_DB
                    else:
                        # LOG.debug("DB NOT available.")
                        App.db_failure.set()
                    LOG.info(f"App running. (Status: {App.status})")
                    tasks = [
                        aio_create_task(
                            App.db_request_restart.wait(), name="await_db_restart"
                        )
                    ]
                    if kb_task:
                        tasks.append(kb_task)
                    # LOG.debug(f"Waiting for tasks: {[t.get_name() for t in tasks]}")
                    done, _ = await aio_wait(tasks, return_when=FIRST_COMPLETED)
                    # LOG.debug(f"Done tasks: {[t.get_name() for t in done]}")
                    if kb_task and kb_task in done:
                        raise KeyboardInterrupt
                    App.db_request_restart.clear()

            except DBSchemaError as exc:
                App.status = Status.STATUS_NO_DB
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
