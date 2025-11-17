"""Check validity of login and actions."""

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from core.app import App
from core.const import SINGLE_USER_NAME
from core.status import Status
from data.management.user import User
from messages.message import MessageAttribute
from database.sql_expression import ColumnName


async def check_login(login_message: dict) -> User:
    "Check login permission for for user and return User object"
    username = (
        login_message.get(MessageAttribute.WS_ATTR_USER)
        if App.status == Status.STATUS_MULTI_USER
        else SINGLE_USER_NAME
    )
    LOG.debug(f"check_login() for {username}")
    matching_users = await User.get_matching_ids({ColumnName("name"): username})
    matching_count = len(matching_users)
    if matching_count > 1:
        raise ValueError(f"multiple users with name '{username}' found")
    if matching_count < 1:
        raise PermissionError(f"User '{username}' not found.")
    user = await User(bo_id=matching_users[0]).fetch()
    LOG.debug(f"check_login() -> {repr(user)}")
    LOG.debug(f"{type(user.role)=}; {user.role if user.role else 'No role'}")
    LOG.debug(f"{user._data=}")  # pylint: disable=protected-access
    return user


log_exit(LOG)
