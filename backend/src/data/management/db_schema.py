""" Maintain database schemas """

from persistance.business_object_base import BOBase
from persistance.bo_descriptors import BOInt
from core.app_logging import getLogger

LOG = getLogger(__name__)


class DBSchema(BOBase):
    _table = "schema_versions"
    version_nr = BOInt()
