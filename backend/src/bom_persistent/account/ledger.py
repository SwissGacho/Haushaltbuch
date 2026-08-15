"""Transaction, representing a single financial transaction from one account to another"""

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from business_objects.bo_semantic_role import BOSemanticRole
from business_objects.persistent_business_object import PersistentBusinessObject
from bom_persistent.account.category import Category
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


class LedgerEntry(PersistentBusinessObject):
    _table = "ledger_entries"
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
            self.debit_account = await DefaultDebitAccount().fetch_singleton()
        if not self.credit_account:
            self.credit_account = await DefaultCreditAccount().fetch_singleton()


class Posting(PersistentBusinessObject):
    ledger_entry = BORelation(LedgerEntry)
    amount = BODecimal()
    category = BORelation(Category)
    description = BOStr()


log_exit(LOG)
