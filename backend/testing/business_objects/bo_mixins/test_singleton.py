"""Tests for business_objects.bo_mixins.singleton."""

import unittest
from unittest.mock import AsyncMock, Mock, patch

from core.exceptions import CannotStoreEmptyBO
from business_objects.bo_mixins.singleton import Singleton


class _SingletonTarget(Singleton):
    def __init__(
        self, bo_id=None, count=0, has_user_id=False, update_return_value=None
    ):
        self.id = bo_id
        self._count = count
        self.fetch_calls = []
        self.store_calls = []
        self.insert_calls = []
        self.update_calls = []
        self.update_return_value = (
            {} if update_return_value is None else update_return_value
        )
        if has_user_id:
            self.user_id = None

    async def fetch_self(self, transaction, id=None, newest=None, session=None):
        self.fetch_calls.append((transaction, id, newest, session))

    async def store(self, session=None, transaction=None):
        self.store_calls.append((session, transaction))
        if self.id is None:
            self.id = 999

    async def count_rows(self, session=None, db_conn=None):
        return self._count

    async def insert_self(self, transaction, session=None):
        self.insert_calls.append((transaction, session))

    async def update_self(self, transaction, session=None):
        self.update_calls.append((transaction, session))
        return self.update_return_value


class _SingletonInvalidFetch(Singleton):
    def __init__(self):
        self.id = None

    def fetch_self(self, transaction, id=None, newest=None, session=None):
        return None

    async def store(self, session=None, transaction=None):
        return None


class _SingletonInvalidCountRows(Singleton):
    def __init__(self):
        self.id = None

    def count_rows(self, session=None, transaction=None):
        return 0

    async def insert_self(self, transaction, session=None):
        return None

    async def update_self(self, transaction, session=None):
        return None


class _SyncNextMixin:
    def fetch_mixin(self, transaction, id=None, newest=None, session=None):
        return None


class _SingletonWithSyncNext(Singleton, _SyncNextMixin):
    async def fetch_self(self, transaction, id=None, newest=None, session=None):
        return None

    async def store(self, session=None, transaction=None):
        return None


class TestSingletonMixin(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_mixin_defaults_to_newest_when_no_id(self):
        target = _SingletonTarget(bo_id=None, count=0)
        mock_transaction = Mock(name="mock_transaction")

        await target.fetch_mixin(
            transaction=mock_transaction, id=None, newest=None, session=None
        )

        self.assertEqual(1, len(target.fetch_calls))
        transaction, _, newest, _ = target.fetch_calls[0]
        self.assertIs(transaction, mock_transaction)
        self.assertTrue(newest)

    async def test_fetch_mixin_stores_when_not_found(self):
        target = _SingletonTarget(bo_id=None, count=0, has_user_id=True)
        session = Mock(name="session")
        session.user = Mock(name="user")
        mock_transaction = Mock(name="mock_transaction")

        await target.fetch_mixin(
            transaction=mock_transaction, id=None, newest=None, session=session
        )

        self.assertEqual(999, target.id)
        self.assertEqual([(session, mock_transaction)], target.store_calls)

    async def test_fetch_mixin_raises_if_fetch_self_not_coroutine(self):
        with self.assertRaises(TypeError):
            await _SingletonInvalidFetch().fetch_mixin(
                transaction=Mock(name="mock_transaction"), session=None
            )

    async def test_store_mixin_raises_if_count_rows_not_coroutine(self):
        with self.assertRaises(TypeError):
            await _SingletonInvalidCountRows().store_mixin(
                transaction=Mock(name="mock_transaction"), session=None
            )

    async def test_store_mixin_raises_if_singleton_already_exists(self):
        target = _SingletonTarget(bo_id=None, count=1)

        with self.assertRaises(CannotStoreEmptyBO):
            await target.store_mixin(
                transaction=Mock(name="mock_transaction"), session=None
            )

    async def test_store_mixin_inserts_when_new_and_none_exists(self):
        target = _SingletonTarget(bo_id=None, count=0)
        mock_transaction = Mock(name="mock_transaction")
        session = Mock(name="session")

        result = await target.store_mixin(transaction=mock_transaction, session=session)

        self.assertEqual([(mock_transaction, session)], target.insert_calls)
        self.assertEqual([], target.update_calls)
        self.assertEqual({}, result)

    async def test_store_mixin_updates_when_id_exists(self):
        expected_old_values = {"some_attr": "old value"}
        target = _SingletonTarget(
            bo_id=5, count=1, update_return_value=expected_old_values
        )
        mock_transaction = Mock(name="mock_transaction")
        session = Mock(name="session")

        result = await target.store_mixin(transaction=mock_transaction, session=session)

        self.assertEqual([], target.insert_calls)
        self.assertEqual([(mock_transaction, session)], target.update_calls)
        self.assertEqual(expected_old_values, result)
