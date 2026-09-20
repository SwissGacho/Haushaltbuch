"""Mixin class for personal business objects."""

from typing import Sequence, Optional

from core.app_logging import (
    getLogger,
    log_exit,
    DEBUG,
    ERROR,
    VERBOSE_DEBUG,
    redact,
    pprint_lines,
)

LOG = getLogger(__name__)

from business_objects.bo_mixins.bo_mixin import MixinBase
from database.sql_expression import Eq, SQLExpression, Value
from server.ws_connection_base import SessionBase


class Personal(MixinBase):
    """Mixin class for personal business objects.
    Personal BOs are BOs that are specific to a user and have a user_id attribute.
    Personal BOs are only accessible to the user they belong to and are not visible to other users.
    """

    @classmethod
    def is_personal(cls) -> bool:
        """Return True if this class is a personal business object class."""
        return True

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
                LOG.log(
                    DEBUG if user_id is None else ERROR,
                    f"Personal.store_mixin: {type(self).__name__} user_id ({user_id}) "
                    f"does not match session user ({session.user})",
                )
            setattr(self, "user_id", session.user)

        return await super().store_mixin(session=session)


log_exit(LOG)
