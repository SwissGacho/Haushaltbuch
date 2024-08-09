""" Data descriptors used in business objects """

import json
from enum import Flag, auto

from datetime import date, datetime
from core.app_logging import getLogger

LOG = getLogger(__name__)


class BOColumnFlag(Flag):
    "Column flag of a BO attribute"
    BOC_NONE = 0
    BOC_NOT_NULL = auto()
    BOC_PK = auto()
    BOC_INC = auto()
    BOC_PK_INC = BOC_PK | BOC_INC
    BOC_FK = auto()
    BOC_UNIQUE = auto()
    BOC_DEFAULT = auto()
    BOC_CURRENT_TS = auto()
    BOC_DEFAULT_CURR = BOC_DEFAULT | BOC_CURRENT_TS


class BOBaseBase:
    "Base for BOBase to circumvent circular import"


class _PersistantAttr:
    def __init__(
        self, flag: BOColumnFlag = BOColumnFlag.BOC_NONE, flag_values: dict = {}
    ) -> None:
        self._flag = flag
        self._flag_values = flag_values

    @classmethod
    def data_type(cls):
        raise NotImplementedError

    def __set_name__(self, owner, name):
        self.my_name = name
        # LOG.debug(
        #     f"PersistantAttr.__set_name__({owner=}, {name=})"
        #     f" {self.__class__.data_type()=} {self._flag=} {self._flag_values=}"
        # )
        cols = (name, self.__class__.data_type(), self._flag, self._flag_values)
        if not owner._attributes.get(owner.__name__):
            owner._attributes[owner.__name__] = []
        owner._attributes[owner.__name__].append(cols)

    def __get__(self, obj, objtype=None):
        return obj._data.get(self.my_name)

    def __set__(self, obj, value):
        self.validate(value)
        obj._data[self.my_name] = value

    def validate(self, value):
        pass


# pylint: disable=missing-class-docstring
class BOInt(_PersistantAttr):
    @classmethod
    def data_type(cls):
        return int

    def validate(self, value):
        return isinstance(value, int)


class BOStr(_PersistantAttr):
    @classmethod
    def data_type(cls):
        return str

    def validate(self, value):
        return isinstance(value, str)


class BODatetime(_PersistantAttr):
    @classmethod
    def data_type(cls):
        return datetime

    def validate(self, value):
        return isinstance(value, datetime)


class BODate(_PersistantAttr):
    @classmethod
    def data_type(cls):
        return date

    def validate(self, value):
        return isinstance(value, date)


class BODict(_PersistantAttr):
    @classmethod
    def data_type(cls):
        return dict

    def validate(self, value):
        try:
            json.dumps(value, separators=(",", ":"))
        except (ValueError, TypeError, RecursionError) as exc:
            raise TypeError(f"{value} is not serializable by JSON: {exc}") from exc
        return isinstance(value, dict)


class BOList(_PersistantAttr):
    @classmethod
    def data_type(cls):
        return list

    def validate(self, value):
        try:
            json.dumps(value, separators=(",", ":"))
        except (ValueError, TypeError, RecursionError) as exc:
            raise TypeError(f"{value} is not serializable by JSON: {exc}") from exc
        return isinstance(value, list)


class BORelation(_PersistantAttr):
    def __init__(
        self, flag: BOColumnFlag = BOColumnFlag.BOC_FK, flag_values: dict = None
    ) -> None:
        flag |= BOColumnFlag.BOC_FK
        relation = flag_values.get("relation")
        # LOG.debug(f"{relation=}")
        if not issubclass(relation, BOBaseBase):
            raise TypeError("BO relation should be derived from BOBase.")

        super().__init__(flag, flag_values)

    @classmethod
    def data_type(cls):
        return BOBaseBase
