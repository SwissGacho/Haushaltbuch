"""Test suite for the ledger business objects"""

import unittest
from unittest.mock import AsyncMock, Mock, patch

from bom_persistent.account.account import Account
from bom_persistent.account.ledger import LedgerEntry, Posting

MODULE = "bom_persistent.account.ledger"


class Test_100_LedgerEntry_store(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.conn = Mock(name="db_conn")
        self.tx = AsyncMock(name="transaction")
        self.tx.connection = self.conn
        self.tx.__aenter__ = AsyncMock(return_value=self.tx)
        self.tx.__aexit__ = AsyncMock(return_value=False)
        self.tx.add_after_commit = Mock()
        self.tx.add_after_rollback = Mock()
        self.MockSQLTx = Mock(name="SQLTransaction", return_value=self.tx)

        self.default_debit = Account()
        self.default_credit = Account()
        self.MockDebit = Mock(name="DefaultDebitAccount")
        self.MockDebit.return_value.fetch = AsyncMock(return_value=self.default_debit)
        self.MockCredit = Mock(name="DefaultCreditAccount")
        self.MockCredit.return_value.fetch = AsyncMock(
            return_value=self.default_credit
        )

        patcher = patch.multiple(
            MODULE,
            SQLTransaction=self.MockSQLTx,
            DefaultDebitAccount=self.MockDebit,
            DefaultCreditAccount=self.MockCredit,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        self.super_store = AsyncMock(name="PersistentBusinessObject.store")
        store_patcher = patch(
            "business_objects.persistent_business_object."
            "PersistentBusinessObject.store",
            new=self.super_store,
        )
        store_patcher.start()
        self.addCleanup(store_patcher.stop)

        self.debit = Account()
        self.credit = Account()
        self.session = Mock(name="session")

    async def test_101_no_accounts_raises(self):
        entry = LedgerEntry()

        with self.assertRaises(RuntimeError):
            await entry.store(session=self.session)

        self.MockSQLTx.assert_not_called()
        self.super_store.assert_not_awaited()

    async def test_102_both_accounts_set_does_not_fetch_defaults(self):
        entry = LedgerEntry(debit_account=self.debit, credit_account=self.credit)

        await entry.store(session=self.session)

        self.MockDebit.return_value.fetch.assert_not_awaited()
        self.MockCredit.return_value.fetch.assert_not_awaited()
        self.assertIs(entry.debit_account, self.debit)
        self.assertIs(entry.credit_account, self.credit)

    async def test_103_missing_credit_account_fetches_default_credit(self):
        entry = LedgerEntry(debit_account=self.debit)

        await entry.store(session=self.session)

        self.MockCredit.return_value.fetch.assert_awaited_once_with(
            transaction=self.tx
        )
        self.MockDebit.return_value.fetch.assert_not_awaited()
        self.assertIs(entry.credit_account, self.default_credit)
        self.assertIs(entry.debit_account, self.debit)

    async def test_104_missing_debit_account_fetches_default_debit(self):
        entry = LedgerEntry(credit_account=self.credit)

        await entry.store(session=self.session)

        self.MockDebit.return_value.fetch.assert_awaited_once_with(
            transaction=self.tx
        )
        self.MockCredit.return_value.fetch.assert_not_awaited()
        self.assertIs(entry.debit_account, self.default_debit)
        self.assertIs(entry.credit_account, self.credit)

    async def test_105_without_transaction_opens_one_and_passes_transaction(self):
        entry = LedgerEntry(debit_account=self.debit, credit_account=self.credit)

        await entry.store(session=self.session)

        self.MockSQLTx.assert_called_once_with()
        self.tx.__aenter__.assert_awaited_once_with()
        self.tx.__aexit__.assert_awaited_once_with(None, None, None)
        self.super_store.assert_awaited_once_with(
            session=self.session, transaction=self.tx
        )

    async def test_106_provided_transaction_is_reused(self):
        entry = LedgerEntry(debit_account=self.debit)

        await entry.store(session=self.session, transaction=self.tx)

        self.MockSQLTx.assert_not_called()
        self.tx.__aenter__.assert_not_awaited()
        self.tx.__aexit__.assert_not_awaited()
        self.MockCredit.return_value.fetch.assert_awaited_once_with(
            transaction=self.tx
        )
        self.super_store.assert_awaited_once_with(
            session=self.session, transaction=self.tx
        )

    async def test_109_default_account_fetch_failure_propagates(self):
        self.MockCredit.return_value.fetch.side_effect = ValueError("no default")
        entry = LedgerEntry(debit_account=self.debit)

        with self.assertRaises(ValueError):
            await entry.store(session=self.session)

        self.super_store.assert_not_awaited()
        self.tx.__aexit__.assert_awaited_once()


class Test_200_Ledger_attributes(unittest.TestCase):
    def test_201_ledger_entry_table(self):
        self.assertEqual(LedgerEntry.table, "ledger_entries")

    def test_202_ledger_entry_attributes(self):
        names = {d.name for d in LedgerEntry.attribute_descriptions()}

        self.assertTrue(
            {
                "transaction_datetime",
                "debit_account",
                "credit_account",
                "counterparty",
                "balance",
            }
            <= names
        )

    def test_203_posting_attributes(self):
        names = {d.name for d in Posting.attribute_descriptions()}

        self.assertTrue({"ledger_entry", "amount", "category", "description"} <= names)
