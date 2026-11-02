"""Transaction, representing a single financial transaction from one account to another"""

from typing import Optional
from contextlib import nullcontext

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from database.sql import SQLTransaction
from bom_persistent.account.category import Category
from bom_persistent.account.account import (
    Account,
    DefaultCreditAccount,
    DefaultDebitAccount,
)
from business_objects.bo_semantic_role import (  # pylint: disable=unused-import
    BOSemanticRole,
)
from business_objects.persistent_business_object import PersistentBusinessObject
from business_objects.bo_descriptors import (  # pylint: disable=unused-import
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
from server.ws_connection_base import SessionBase


class LedgerEntry(PersistentBusinessObject):
    _table = "ledger_entries"
    transaction_datetime = BODatetime()
    debit_account = BORelation(Account)
    credit_account = BORelation(Account)
    counterparty = BOStr()
    balance = BODecimal()

    async def store(
        self,
        session: Optional[SessionBase] = None,
        transaction: Optional[SQLTransaction] = None,
    ) -> None:
        "Store the object in the database"

        # If credit or debit account is not set, set it to the default account
        if not self.debit_account and not self.credit_account:
            raise RuntimeError(
                "No debit or credit account specified. At least one must be specified."
            )
        async with (
            nullcontext(transaction) if transaction is not None else SQLTransaction()
        ) as transaction:
            if not self.debit_account:
                self.debit_account = await DefaultDebitAccount().fetch(
                    transaction_or_sql=transaction
                )
            if not self.credit_account:
                self.credit_account = await DefaultCreditAccount().fetch(
                    transaction_or_sql=transaction
                )
            await super().store(session=session, transaction=transaction)


class Posting(PersistentBusinessObject):
    ledger_entry = BORelation(LedgerEntry)
    amount = BODecimal()
    category = BORelation(Category)
    description = BOStr()


log_exit(LOG)
