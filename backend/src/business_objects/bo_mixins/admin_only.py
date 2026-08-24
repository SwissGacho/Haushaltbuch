"""Mixin class for admin-only business objects."""

from typing import Sequence

from core.app_logging import (
    getLogger,
    log_exit,
    DEBUG,
    VERBOSE_DEBUG,
    redact,
    pprint_lines,
)

from database.sql_expression import ColumnName, Eq, Not, SQLExpression, SQLString

LOG = getLogger(__name__)

from business_objects.bo_mixins.bo_mixin import MixinBase


class AdminOnly(MixinBase):
    """Mixin class for admin-only business objects.
    Admin-only BOs are BOs that are only accessible to users with the admin role.
    Admin-only BOs are not visible to other users.
    """

    @classmethod
    def is_admin_only(cls) -> bool:
        """Return True if this class is an admin-only business object class."""
        return True

    @classmethod
    def specialist_conditions_mixin(
        cls, specialist_cls, user
    ) -> Sequence[SQLExpression]:
        """Return a list of SQLExpression objects that prevent the selection
        of adminonly BOs for users without admin privileges."""
        if not user or bool(getattr(user, "is_admin", False)):
            return []
        return [
            Not(
                Eq(
                    ColumnName("bo_name"),
                    SQLString(str(specialist_cls.bo_type_name())),
                )
            )
        ]


log_exit(LOG)
