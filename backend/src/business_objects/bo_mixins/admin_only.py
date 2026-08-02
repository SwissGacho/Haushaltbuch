from core.app_logging import (
    getLogger,
    log_exit,
    DEBUG,
    VERBOSE_DEBUG,
    redact,
    pprint_lines,
)

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

    # ADMIN_ONLY = True
