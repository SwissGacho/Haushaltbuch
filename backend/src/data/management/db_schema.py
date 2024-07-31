""" Maintain database schemas """

from typing import Optional

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from core.app_logging import getLogger
from persistance.business_object_base import BOBase
from persistance.bo_descriptors import BOInt


class DBSchema(BOBase):
    "Version of the DB schema"
    _table = "schema_versions"
    version_nr = BOInt()


log_exit(LOG)
