""" Manage tokens with limited lifetime"""

from datetime import datetime, timedelta
from secrets import token_hex as token
from core.base_objects import BaseObject
from core.app_logging import getLogger

LOG = getLogger(__name__)


class WSToken(BaseObject):
    "a session or connection token with limited lifetime"
    _all_tokens = set()

    def __init__(self, valid_for_seconds: int = 300) -> None:
        self.token = token(16)
        self._valid_for_seconds = valid_for_seconds
        self._expires = datetime.now() + timedelta(seconds=valid_for_seconds)
        self._all_tokens.add(self)

    @classmethod
    def check_token(cls, token: str) -> bool:
        "check if token is valid"
        now = datetime.now()
        for tkn in cls._all_tokens:
            if token == tkn.token and tkn._expires > now:
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


# LOG.debug("module imported")
