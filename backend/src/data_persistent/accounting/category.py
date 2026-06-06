from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from business_objects.bo_semantic_role import BOSemanticRole
from business_objects.business_attribute_base import BaseFlag
from business_objects.persistant_business_object import PersistentBusinessObject
from business_objects.bo_descriptors import (
    BODatetime,
    BODict,
    BOList,
    BORelation,
    BOSelf,
    BOStr,
    BOFlag,
    BOInt,
    BODate,
    BODecimal,
)


class Category(PersistentBusinessObject):
    _table = "categories"
    name = BOStr(semantic_role=BOSemanticRole.BONAME)
    parent_category = BORelation(BOSelf)
    is_root_bo: bool = True


log_exit(LOG)
