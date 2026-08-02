from business_objects.bo_mixins.bo_mixin import MixinBase


class AdminOnly(MixinBase):
    """Mixin class for admin-only business objects.
    Admin-only BOs are BOs that are only accessible to users with the admin role.
    Admin-only BOs are not visible to other users.
    """

    ADMIN_ONLY = True
