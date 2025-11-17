"""Manage tokens with limited lifetime"""

from datetime import datetime, timedelta
from secrets import token_hex as token

from core.app_logging import getLogger, log_exit, Logger

LOG: Logger = getLogger(__name__)

from core.base_objects import BaseObject


class WSToken(BaseObject):
    "a session or connection token with limited lifetime"

    _all_tokens = set()

    def __init__(self, tok: str | None = None, valid_for_seconds: int = 300) -> None:
        self.token = tok or token(16)
        self._valid_for_seconds = valid_for_seconds
        self._expires = datetime.now() + timedelta(seconds=valid_for_seconds)
        if tok is None:
            self._all_tokens.add(self)

    @classmethod
    def check_token(cls, tok: str) -> bool:
        "check if token is valid"
        now = datetime.now()
        for tkn in cls._all_tokens:
            # pylint: disable=protected-access
            if tok == tkn.token and tkn._expires > now:
                tkn._expires = datetime.now() + timedelta(
                    seconds=tkn._valid_for_seconds
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
        return f"<Token(valid_for_seconds={self._valid_for_seconds}) valid untill {self._expires}>"


log_exit(LOG)
