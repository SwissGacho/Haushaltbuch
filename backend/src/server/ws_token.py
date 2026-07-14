"""Manage tokens with limited lifetime"""

from typing import Optional
from datetime import datetime, timedelta
from secrets import token_hex as token

from core.app_logging import getLogger, log_exit, Logger, redact, VERBOSE_DEBUG

LOG: Logger = getLogger(__name__)

from core.base_objects import BaseObject


class WSToken(BaseObject):
    """A session or connection token with limited lifetime.
    If inactive_seconds_timeout is set, the token is valid
    for a limited time after creation or last usage.
    """

    _all_tokens = set()

    def __init__(
        self, tok: str | None = None, inactive_seconds_timeout: Optional[int] = 300
    ) -> None:
        self.token = tok or token(16)
        self._valid_for_seconds = inactive_seconds_timeout
        self._expires = (
            datetime.now() + timedelta(seconds=inactive_seconds_timeout)
            if inactive_seconds_timeout
            else None
        )
        if tok is None:
            self._all_tokens.add(self)

    @classmethod
    def check_token(cls, tok: str) -> bool:
        "check if token is valid"
        now = datetime.now()
        for tkn in cls._all_tokens:
            # pylint: disable=protected-access
            if tok == tkn.token:
                if not tkn._valid_for_seconds:
                    return True
                if tkn._expires > now:
                    tkn._expires = datetime.now() + timedelta(
                        seconds=tkn._valid_for_seconds
                    )
                    LOG.log(
                        VERBOSE_DEBUG,
                        f"{redact({'Token': tkn.token})} is valid until {tkn._expires}",
                    )
                    return True
        return False

    def __eq__(self, __value: object) -> bool:
        return __value == self.token

    def __hash__(self) -> int:
        return hash(self.token)

    def __str__(self) -> str:
        return self.token

    def __repr__(self) -> str:
        return f"<Token(valid_for_seconds={self._valid_for_seconds}) valid until {self._expires}>"


log_exit(LOG)
