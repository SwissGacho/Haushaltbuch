"""Mixin classes for business objects."""

from inspect import iscoroutinefunction
from typing import Optional

from core.app_logging import (
    getLogger,
    log_exit,
    DEBUG,
    VERBOSE_DEBUG,
    redact,
    pprint_lines,
)

LOG = getLogger(__name__)

from server.ws_connection_base import SessionBase
from database.sql import SQL


class Specialized:
    """Mixin class for specialized business objects.
    BOs derived from a specialized BO are considered
    to be a specialization without using this Mixin.

    Use it like this:
    class MyGenericBO(PersistentBusinessObject):
        ...
    class MySpecializedBO( Specialized, MyGenericBO):
        ...
    class MyVerySpecializedBO(MySpecializedBO):
        ...
    """


class Singleton:
    """Mixin class for singleton business objects.
    Singleton BOs are BOs of which there should be exactly one instance in the database.
    """

    async def fetch_mixin(
        self, id=None, newest=None, session: Optional[SessionBase] = None
    ):
        """Fetch the content for a singleton BO instance from the DB.
        If 'id' is given, fetch the identified object
        If 'id' omitted and 'newest'=True fetch the object with highest id
        If the object is not found in the DB the current instance is stored
        in the DB and returned with a new id.
        """
        LOG.debug(f"Singleton.fetch({id=}, {newest=})")
        fetch_self = getattr(self, "fetch_self", None)
        store = getattr(self, "store", None)
        if not (iscoroutinefunction(fetch_self) and iscoroutinefunction(store)):
            raise TypeError(
                f"Singleton.fetch_mixin: Expected PersistentBusinessObject, got {type(self).__name__}"
            )
        if id is None:
            id = getattr(self, "id", None)
        if id is None:
            newest = True
        async with SQL() as sql:
            await fetch_self(sql, id=id, newest=newest, session=session)

        if getattr(self, "id", None) is None:
            LOG.debug(
                f"{self}.business_values_as_dict: No {type(self).__name__} found for user {getattr(session, 'user', None)}."
                " Returning a new empty one."
            )
            if hasattr(self, "user_id"):
                self.user_id = getattr(session, "user", None)
            await store(session=session)

        return self


class Personal:
    """Mixin class for personal business objects.
    Personal BOs are BOs that are specific to a user and have a user_id attribute.
    Personal BOs are only accessible to the user they belong to and are not visible to other users.
    """


class AdminOnly:
    """Mixin class for admin-only business objects.
    Admin-only BOs are BOs that are only accessible to users with the admin role.
    Admin-only BOs are not visible to other users.
    """

    ADMIN_ONLY = True
