"""A single item on a transaction, representing a single line on an invoice or a single entry in a cash book, containing an amount and a category"""

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
from bom_persistent.account.transaction import Transaction
from bom_persistent.account.category import Category


class TransactionItem(PersistentBusinessObject):
    transaction = BORelation(Transaction)
    counterparty = BOStr()
    amount = BODecimal()
    category = BORelation(Category)
    description = BOStr()


log_exit(LOG)
