"""Transaction, representing a single financial transaction from one account to another"""

from .transaction_item import TransactionItem
from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from business_objects.bo_semantic_role import BOSemanticRole
from business_objects.business_attribute_base import BaseFlag
from business_objects.persistent_business_object import PersistentBusinessObject
from business_objects.bo_descriptors import (
    BODatetime,
    BODict,
    BODescriptorList,
    BORelation,
    BOSelf,
    BOStr,
    BOFlag,
    BOInt,
    BODate,
    BODecimal,
)
from bom_persistent.account.account import (
    Account,
    DefaultCreditAccount,
    DefaultDebitAccount,
)


class Transaction(PersistentBusinessObject):
    transaction_datetime = BODatetime()
    debit_account = BORelation(Account)
    credit_account = BORelation(Account)
    counterparty = BOStr()
    balance = BODecimal()

    async def store(self, attributes: list[str] | None = None) -> None:
        "Store the object in the database"

        # If credit or debit account is not set, set it to the default account
        if not self.debit_account and not self.credit_account:
            raise RuntimeError(
                "No debit or credit account specified. At least one must be specified."
            )
        if not self.debit_account:
            default_debit_account = await DefaultDebitAccount().fetch(newest=True)
            await default_debit_account.store()
            if default_debit_account:
                self.debit_account = default_debit_account
            else:
                raise RuntimeError(
                    "No debit account specified and no default debit account found."
                )
        if not self.credit_account:
            default_credit_account = await DefaultCreditAccount().fetch(newest=True)
            await default_credit_account.store()
            if default_credit_account:
                self.credit_account = default_credit_account
            else:
                raise RuntimeError(
                    "No credit account specified and no default credit account found."
                )


log_exit(LOG)
