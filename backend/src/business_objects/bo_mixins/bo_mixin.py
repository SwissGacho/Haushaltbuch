"""Baseclass for mixin classes for business objects."""

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

from business_objects.bo_descriptors import AttributeDescription
from core.util import check_property
from database.sql import SQL
from database.sql_expression import (
    SQLExpression,
    In,
    Eq,
    Concat,
    ColumnName,
    SQLString,
    Value,
)
from server.ws_connection_base import SessionBase


class MixinBase:
    """Base class for mixin classes.
    Mixin classes are used to add additional functionality to business objects.
    They are not meant to be instantiated directly, but to be used as base classes for business objects.
    """

    @classmethod
    def is_specializing(cls) -> bool:
        """Return True if this class is a specialization of another business object class."""
        # LOG.log(VERBOSE_DEBUG, f"MixinBase.is_specializing({cls.__name__})  -> False")
        return False

    @classmethod
    def is_personal(cls) -> bool:
        """Return True if this class is a personal business object class."""
        return False

    @classmethod
    def is_admin_only(cls) -> bool:
        """Return True if this class is an admin-only business object class."""
        return False

    @classmethod
    def skip_create_table(cls) -> bool:
        """Return True if this class should not create a table in the database."""
        return False

    @classmethod
    def add_specialized_attributes_as_dict(
        cls, bo_cls, attrs: dict[str, type]
    ) -> dict[str, type]:
        """Add specialized attributes to the given attrs dictionary.
        The attrs dictionary maps attribute names to their types.
        """
        for specialized in getattr(bo_cls, "specialists", []):
            attrs.update(
                {
                    a.name: a.data_type
                    for a in specialized.attribute_descriptions(
                        include_specialized=False
                    )
                    if a.name not in attrs
                }
            )
        return attrs

    @classmethod
    def specialized_attribute_descriptions(
        cls, bo_cls, descriptions: list[AttributeDescription]
    ) -> list[AttributeDescription]:
        """Return the list of attribute descriptions for this business object class.
        If 'include_specialized' is True, also include the attributes of specialized BOs.
        """
        for specialized in getattr(bo_cls, "specialists", []):
            descriptions += [
                a
                for a in specialized.attribute_descriptions(include_specialized=False)
                if a.name not in [d.name for d in descriptions]
            ]
        return descriptions

    # @classmethod
    # def _specialist_conditions(cls, gen_cls, user) -> Sequence[SQLExpression]:
    #     """Return a list of SQLExpression objects that restrict the selection of specialized BOs
    #     to those that are relevant and accessible for the given user.
    #     """
    #     if not getattr(gen_cls, "specialists", None):
    #         return []
    #     is_admin = getattr(user, "is_admin", False)
    #     if not user:
    #         tested_value = ColumnName("bo_name")
    #         valid_values: list[SQLExpression] = [
    #             SQLString(str(bo_type_name()))
    #             for s in gen_cls.specialists
    #             if callable(bo_type_name := getattr(s, "bo_type_name", None))
    #             and not check_property(s, "is_personal")
    #         ]
    #         if not valid_values:
    #             raise ValueError(
    #                 f"MixinBase._specialist_conditions: No valid specialists found for {gen_cls.__name__} with no user provided"
    #             )
    #     else:
    #         tested_value = Concat(ColumnName("bo_name"), SQLString("."), Value(user))
    #         valid_values = [
    #             Concat(
    #                 SQLString(str(bo_type_name())),
    #                 SQLString("."),
    #                 (
    #                     ColumnName("user_id")
    #                     if check_property(s, "is_personal")
    #                     else Value(user)
    #                 ),
    #             )
    #             for s in gen_cls.specialists
    #             if callable(bo_type_name := getattr(s, "bo_type_name", None))
    #             and (is_admin or not check_property(s, "is_admin_only"))
    #         ]
    #         if not valid_values:
    #             LOG.warning(
    #                 f"MixinBase._specialist_conditions: No valid specialists found for {gen_cls.__name__} and user {user}"
    #             )
    #             return []
    #     return [In(tested_value, valid_values)]

    @classmethod
    def specialist_conditions(cls, gen_cls, user) -> Sequence[SQLExpression]:
        """Return a list of SQLExpression objects that restrict the selection of specialized BOs
        to those that are relevant and accessible for the given user.
        """
        conds: list[SQLExpression] = []
        specialists = getattr(gen_cls, "specialists", None) or [gen_cls]
        spec_names = set()
        for specialist_cls in specialists:
            spec_names |= {
                str(
                    bn()
                    if callable((bn := getattr(specialist_cls, "bo_type_name", None)))
                    else specialist_cls.__name__
                )
            }
            conds += [
                item
                for c in MixinBase.__subclasses__()
                if issubclass(specialist_cls, c)
                and callable(sc := getattr(c, "specialist_conditions_mixin", None))
                for item in cast(Iterable, sc(specialist_cls, user))
            ]
        return conds + [In(ColumnName("bo_name"), [SQLString(sn) for sn in spec_names])]

    @classmethod
    def special_conditions(cls, gen_cls, user) -> Sequence[SQLExpression]:
        """Return a list of SQLExpressions that restrict the selection of BOs with Mixins
        to those that are relevant and accessible for the given user.
        """
        conds: list[SQLExpression] = []
        conds += list(MixinBase.specialist_conditions(gen_cls, user))
        # conds += [Eq(SQLString("1"), SQLString("1"))]
        # conds += list(MixinBase._specialist_conditions(gen_cls, user))
        conds += [
            item
            for c in MixinBase.__subclasses__()
            if issubclass(gen_cls, c)
            and callable(sc := getattr(c, "special_conditions_mixin", None))
            for item in cast(Iterable, sc(gen_cls, user))
        ]
        return conds

    async def fetch_mixin(
        self, sql: SQL, id=None, newest=None, session: Optional[SessionBase] = None
    ):
        """Fetch the content for a BO instance from the DB."""
        LOG.debug(
            f"MixinBase.fetch_mixin({id=}, {newest=}, {session.user if session else 'N/A'})"
        )
        fetch_self = getattr(self, "fetch_self", None)
        if not iscoroutinefunction(fetch_self):
            raise TypeError(
                f"MixinBase.fetch_mixin: Expected PersistentBusinessObject, got {type(self).__name__}"
            )
        await fetch_self(sql, id=id, newest=newest, session=session)

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


log_exit(LOG)
