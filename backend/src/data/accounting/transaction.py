"""Business Object with all possible attribute types for testing purposes"""

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
from data.accounting.account import Account


class Transaction(PersistentBusinessObject):
    transaction_datetime = BODatetime()
    account = BORelation(Account)
    counterparty = BOStr()
    amount = BODecimal()
    balance = BODecimal()


log_exit(LOG)
