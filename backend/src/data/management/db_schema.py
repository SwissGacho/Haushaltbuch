"""Maintain database schemas"""

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from business_objects.persistant_business_object import PersistentBusinessObject
from business_objects.bo_descriptors import BOInt


class DBSchema(PersistentBusinessObject):
    "Version of the DB schema"

    _table = "schema_versions"
    version_nr = BOInt()


log_exit(LOG)
