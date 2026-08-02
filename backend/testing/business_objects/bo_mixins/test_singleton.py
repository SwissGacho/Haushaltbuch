"""Tests for business_objects.bo_mixins.singleton."""

import unittest
from unittest.mock import AsyncMock, Mock, patch

from core.exceptions import CannotStoreEmptyBO
from business_objects.bo_mixins.singleton import Singleton


class _SingletonTarget(Singleton):
    def __init__(self, bo_id=None, count=0, has_user_id=False):
        self.id = bo_id
        self._count = count
        self.fetch_calls = []
        self.store_calls = []
        self.insert_calls = []
        self.update_calls = []
        if has_user_id:
            self.user_id = None

    async def fetch_self(self, sql, id=None, newest=None, session=None):
        self.fetch_calls.append((sql, id, newest, session))

    async def store(self, session=None):
        self.store_calls.append(session)
        if self.id is None:
            self.id = 999

    async def count_rows(self, session=None):
        return self._count

    async def insert_self(self, session=None):
        self.insert_calls.append(session)

    async def update_self(self, session=None):
        self.update_calls.append(session)


class _SingletonInvalidFetch(Singleton):
    def __init__(self):
        self.id = None

    def fetch_self(self, sql, id=None, newest=None, session=None):
        return None

    async def store(self, session=None):
        return None


class _SingletonInvalidCountRows(Singleton):
    def __init__(self):
        self.id = None

    def count_rows(self, session=None):
        return 0

    async def insert_self(self, session=None):
        return None

    async def update_self(self, session=None):
        return None


class _SyncNextMixin:
    def fetch_mixin(self, sql, id=None, newest=None, session=None):
        return None


class _SingletonWithSyncNext(Singleton, _SyncNextMixin):
    async def fetch_self(self, sql, id=None, newest=None, session=None):
        return None

    async def store(self, session=None):
        return None


class TestSingletonMixin(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_mixin_defaults_to_newest_when_no_id(self):
        target = _SingletonTarget(bo_id=None, count=0)
        mock_sql = Mock(name="mock_sql")
        mock_sql.__aenter__ = AsyncMock(return_value=mock_sql)
        mock_sql.__aexit__ = AsyncMock(return_value=None)

        with patch("business_objects.bo_mixins.singleton.SQL", return_value=mock_sql):
            await target.fetch_mixin(id=None, newest=None, session=None)

        self.assertEqual(1, len(target.fetch_calls))
        _, _, newest, _ = target.fetch_calls[0]
        self.assertTrue(newest)

    async def test_fetch_mixin_stores_when_not_found(self):
        target = _SingletonTarget(bo_id=None, count=0, has_user_id=True)
        session = Mock(name="session")
        session.user = Mock(name="user")
        mock_sql = Mock(name="mock_sql")
        mock_sql.__aenter__ = AsyncMock(return_value=mock_sql)
        mock_sql.__aexit__ = AsyncMock(return_value=None)

        with patch("business_objects.bo_mixins.singleton.SQL", return_value=mock_sql):
            await target.fetch_mixin(id=None, newest=None, session=session)

        self.assertIs(target.user_id, session.user)
        self.assertEqual([session], target.store_calls)

    async def test_fetch_mixin_raises_if_fetch_self_not_coroutine(self):
        with self.assertRaises(TypeError):
            await _SingletonInvalidFetch().fetch_mixin(session=None)

    async def test_fetch_mixin_raises_if_next_mixin_is_not_coroutine(self):
        with self.assertRaises(TypeError):
            await _SingletonWithSyncNext().fetch_mixin(session=None)

    async def test_store_mixin_raises_if_count_rows_not_coroutine(self):
        with self.assertRaises(TypeError):
            await _SingletonInvalidCountRows().store_mixin(session=None)

    async def test_store_mixin_raises_if_singleton_already_exists(self):
        target = _SingletonTarget(bo_id=None, count=1)

        with self.assertRaises(CannotStoreEmptyBO):
            await target.store_mixin(session=None)

    async def test_store_mixin_inserts_when_new_and_none_exists(self):
        target = _SingletonTarget(bo_id=None, count=0)
        session = Mock(name="session")

        await target.store_mixin(session=session)

        self.assertEqual([session], target.insert_calls)
        self.assertEqual([], target.update_calls)

    async def test_store_mixin_updates_when_id_exists(self):
        target = _SingletonTarget(bo_id=5, count=1)
        session = Mock(name="session")

        await target.store_mixin(session=session)

        self.assertEqual([], target.insert_calls)
        self.assertEqual([session], target.update_calls)
