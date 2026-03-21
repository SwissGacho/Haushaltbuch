"""Business Object with all possible attribute types for testing purposes"""

from enum import auto

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from business_objects.business_attribute_base import BaseFlag
from business_objects.persistant_business_object import PersistentBusinessObject
from business_objects.bo_descriptors import (
    BODatetime,
    BODict,
    BOList,
    BORelation,
    BOStr,
    BOFlag,
    BOInt,
    BODate,
)


class UltimateFlag(BaseFlag):
    "Ultimate Flag for testing purposes"

    OPTION_1 = auto()
    OPTION_2 = auto()
    OPTION_3 = auto()


class UltimateRelatedTestObject(PersistentBusinessObject):
    "Related Business Object for testing purposes"

    name = BOStr()


class UltimateTestObject(PersistentBusinessObject):
    "Business Object with all possible attribute types for testing purposes"

    bo_int = BOInt()
    bo_str = BOStr()
    bo_datetime = BODatetime()
    bo_date = BODate()
    bo_dict = BODict()
    bo_list = BOList()
    bo_rel = BORelation(UltimateRelatedTestObject)
    bo_flag = BOFlag(UltimateFlag)


log_exit(LOG)
