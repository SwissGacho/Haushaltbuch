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

from .admin_only import AdminOnly
from .Personal import Personal

LOG = getLogger(__name__)

from server.ws_connection_base import SessionBase
from database.sql_expression import (
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
