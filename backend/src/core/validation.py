""" Check validity of login and actions.
"""

from core.app_logging import getLogger, logExit

LOG = getLogger(__name__)

from data.management.user import User
from messages.message import MessageAttribute
from database.sqlexpression import ColumnName


async def check_login(login_message: dict) -> User:
    "Check login permission for for user and return User object"
    username = login_message.get(MessageAttribute.WS_ATTR_USER)
    LOG.debug(f"check_login() for {username}")
    matching_users = await User.get_matching_ids({ColumnName("name"): username})
    matching_count = len(matching_users)
    if matching_count > 1:
        raise ValueError(f"multiple users with name '{username}' found")
    if matching_count < 1:
        raise PermissionError(f"User '{username} not found.")
    user = await User(id=matching_users[0]).fetch()
    LOG.debug(f"check_login() -> {user}")
    return user


logExit(LOG)
