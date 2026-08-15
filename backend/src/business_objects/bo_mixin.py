"""Mixin classes for business objects."""

from inspect import iscoroutinefunction
from typing import Iterable, Optional, Sequence, cast

from core.app_logging import (
    getLogger,
    log_exit,
    DEBUG,
    VERBOSE_DEBUG,
    redact,
    pprint_lines,
)

LOG = getLogger(__name__)

from core.exceptions import CannotStoreEmptyBO
from server.ws_connection_base import SessionBase
from database.sql import SQL
from business_objects.business_object_base import BOBase
from database.sql_expression import (
    Eq,
    SQLExpression,
    In,
    Concat,
    ColumnName,
    SQLString,
    Value,
)


class MixinBase:
    """Base class for mixin classes.
    Mixin classes are used to add additional functionality to business objects.
    They are not meant to be instantiated directly, but to be used as base classes for business objects.
    """

    @classmethod
    def is_specializing(cls) -> bool:
        """Return True if this class is a specialization of another business object class."""
        return False

    @classmethod
    def skip_create_table(cls) -> bool:
        """Return True if this class should not create a table in the database."""
        return False

    @classmethod
    def _specialist_conditions(cls, gen_cls, user) -> Sequence[SQLExpression]:
        """Return a list of SQLExpression objects that restrict the selection of specialized BOs
        to those that are relevant and accessible for the given user.
        """
        if not getattr(gen_cls, "specialists", None):
            return []
        is_admin = getattr(user, "is_admin", False)
        if not user:
            tested_value = ColumnName("bo_name")
            valid_values: list[SQLExpression] = [
                SQLString(str(bo_type_name()))
                for s in gen_cls.specialists
                if callable(bo_type_name := getattr(s, "bo_type_name", None))
                and not issubclass(s, Personal)
            ]
            if not valid_values:
                raise ValueError(
                    f"MixinBase._specialist_conditions: No valid specialists found for {gen_cls.__name__} with no user provided"
                )
        else:
            tested_value = Concat(ColumnName("bo_name"), SQLString("."), Value(user))
            valid_values = [
                Concat(
                    SQLString(str(bo_type_name())),
                    SQLString("."),
                    (ColumnName("user_id") if issubclass(s, Personal) else Value(user)),
                )
                for s in gen_cls.specialists
                if callable(bo_type_name := getattr(s, "bo_type_name", None))
                and (not issubclass(s, AdminOnly) or is_admin)
            ]
            if not valid_values:
                LOG.warning(
                    f"MixinBase._specialist_conditions: No valid specialists found for {gen_cls.__name__} and user {user}"
                )
                return []
        return [In(tested_value, valid_values)]

    @classmethod
    def special_conditions(cls, gen_cls, user) -> Sequence[SQLExpression]:
        """Return a list of SQLExpressions that restrict the selection of BOs with Mixins
        to those that are relevant and accessible for the given user.
        """
        conds: list[SQLExpression] = list(
            MixinBase._specialist_conditions(gen_cls, user)
        ) + [
            item
            for c in MixinBase.__subclasses__()
            if issubclass(cls, c)
            and callable(sc := getattr(c, "special_conditions_mixin", None))
            for item in cast(Iterable, sc(gen_cls, user))
        ]
        return conds

    async def store_mixin(self, session: Optional[SessionBase] = None):
        """Store the business object in the database.
        If 'self.id is None' a new row is inserted
        Else the existing row is updated
        """
        LOG.debug(
            f"MixinBase.store({session.user if session else 'N/A'})",
        )
        insert_self = getattr(self, "insert_self", None)
        update_self = getattr(self, "update_self", None)
        if not (iscoroutinefunction(insert_self) and iscoroutinefunction(update_self)):
            raise TypeError(
                f"MixinBase.store_mixin: Expected PersistentBusinessObject, got {type(self).__name__}"
            )
        if getattr(self, "id", None) is None:
            await insert_self(session)
        else:
            await update_self(session)


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


class Personal(MixinBase):
    """Mixin class for personal business objects.
    Personal BOs are BOs that are specific to a user and have a user_id attribute.
    Personal BOs are only accessible to the user they belong to and are not visible to other users.
    """

    @classmethod
    def special_conditions_mixin(cls, gen_cls, user) -> Sequence[SQLExpression]:
        """Return a list of SQLExpression objects that restrict the selection of personal BOs
        to those that are relevant for the given user."""
        if not user:
            raise ValueError(
                f"Personal.special_conditions_mixin: No user provided for {gen_cls.__name__}"
            )
        c = Eq("user_id", Value(user))
        return [c]

    @classmethod
    def skip_create_table(cls) -> bool:
        """Return True if this class should not create a table in the database."""
        if (
            desc := getattr(cls, "attribute_descriptions", None)
        ) is None or "user_id" not in [d.name for d in desc()]:
            raise TypeError(
                f"PersistentBusinessObject.sql_create_table(): {cls.__name__} is a Personal BO but has no 'user_id' attribute"
            )
        return super().skip_create_table()

    async def store_mixin(self, session: Optional[SessionBase] = None):
        """Store the business object in the database.
        If 'self.id is None' a new row is inserted
        Else the existing row is updated
        """
        LOG.debug(
            f"Personal.store({session.user if session else 'N/A'})",
        )
        if session is None or not hasattr(session, "user") or session.user is None:
            raise ValueError(
                f"Personal.store_mixin: No user in session for storing {type(self).__name__}"
            )
        if not hasattr(self, "user_id"):
            raise ValueError(
                f"Personal.store_mixin: {type(self).__name__} has no user_id attribute"
            )
        user_id = getattr(self, "user_id", None)
        if user_id != session.user:
            if (
                not isinstance(session.user, type(user_id))
                or getattr(user_id, "id", None) != session.user.id
            ):
                LOG.error(
                    f"Personal.store_mixin: {type(self).__name__} user_id ({user_id}) "
                    f"does not match session user ({session.user})"
                )
            setattr(self, "user_id", session.user)

        return await super().store_mixin(session=session)


class AdminOnly(MixinBase):
    """Mixin class for admin-only business objects.
    Admin-only BOs are BOs that are only accessible to users with the admin role.
    Admin-only BOs are not visible to other users.
    """

    ADMIN_ONLY = True


class Specialized(MixinBase):
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

    @classmethod
    def is_specializing(cls) -> bool:
        """Return True if this class is a specialization of another business object class."""
        return True

    @classmethod
    def register_specialist_mixin(cls):
        """Register this class as a specialist of its generic base classes."""
        specialists: set[type[BOBase]] = {cast(type[BOBase], cls)}
        for super_cls in cls.__mro__:
            if issubclass(super_cls, BOBase) and (
                spec := getattr(super_cls, "specialists", None)
            ):
                specialists.add(super_cls)
                setattr(
                    super_cls,
                    "specialists",
                    cast(set[type[BOBase]], spec) | specialists,
                )
                if (
                    not hasattr(super_cls, "is_specializing")
                ) or not super_cls.is_specializing():
                    break

    @classmethod
    def skip_create_table(cls) -> bool:
        """Return True if this class should not create a table in the database."""
        return True
