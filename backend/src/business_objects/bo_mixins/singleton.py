from typing import Optional

from core.exceptions import CannotStoreEmptyBO
from database.sql import SQL
from server.ws_connection_base import SessionBase
from business_objects.bo_mixins.bo_mixin import LOG, MixinBase


from inspect import iscoroutinefunction


class Singleton(MixinBase):
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
        next_mixin = getattr(super(), "fetch_mixin", None)
        if next_mixin and not iscoroutinefunction(next_mixin):
            raise TypeError(
                f"Singleton.fetch_mixin: Next fetch_mixin in MRO is not a coroutine function in {type(self).__name__}"
            )
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
            await (next_mixin or fetch_self)(sql, id=id, newest=newest, session=session)

        if getattr(self, "id", None) is None:
            LOG.debug(
                f"{self}.business_values_as_dict: No {type(self).__name__} found for user {getattr(session, 'user', None)}."
                " Returning a new empty one."
            )
            if hasattr(self, "user_id"):
                self.user_id = getattr(session, "user", None)
            await store(session=session)

        return self

    async def store_mixin(self, session: Optional[SessionBase] = None):
        """Store the business object in the database.
        If 'self.id is None' a new row is inserted
        Else the existing row is updated
        """
        LOG.debug(
            f"Singleton.store({session.user if session else 'N/A'})",
        )
        if getattr(self, "id", None) is None:
            count_rows = getattr(self, "count_rows", None)
            if not iscoroutinefunction(count_rows):
                raise TypeError(
                    f"Singleton.store_mixin: Expected PersistentBusinessObject with count_rows method, got {type(self).__name__}"
                )
            existing_count = await count_rows(session=session)
            if existing_count > 0:
                raise CannotStoreEmptyBO(
                    f"Cannot insert {self} as it is a Singleton and already exists in the DB"
                )
        return await super().store_mixin(session=session)
