"""Mixin class for specializing business objects."""

from typing import cast

from core.app_logging import (
    getLogger,
    log_exit,
    DEBUG,
    VERBOSE_DEBUG,
    redact,
    pprint_lines,
)

LOG = getLogger(__name__)

from business_objects.business_object_base import BOBase
from business_objects.bo_mixins.bo_mixin import MixinBase


class Specializing(MixinBase):
    """Mixin class for specialized business objects.
    BOs derived from a specialized BO are considered
    to be a specialization without using this mixin.

    Use it like this:
    class MyGenericBO(PersistentBusinessObject):
        ...
    class MySpecializingBO(Specializing, MyGenericBO):
        ...
    class MyVerySpecializingBO(MySpecializingBO):
        ...
    """

    @classmethod
    def is_specializing(cls) -> bool:
        """Return True if this class is a specialization of another business object class."""
        # LOG.log(VERBOSE_DEBUG, f"Specializing.is_specializing({cls.__name__})  -> True")
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
                    not callable(is_spec := getattr(super_cls, "is_specializing", None))
                ) or not is_spec():
                    break

    @classmethod
    def skip_create_table(cls) -> bool:
        """Return True if this class should not create a table in the database."""
        return True


log_exit(LOG)
