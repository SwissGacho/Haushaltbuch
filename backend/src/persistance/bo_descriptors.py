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
        self, flag: BOColumnFlag = BOColumnFlag.BOC_NONE, **flag_values
    ) -> None:
        self._flag = flag
        self._flag_values = flag_values
        self.my_name = None

    @classmethod
    def data_type(cls):
        "Datatype of attribute"
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
        if obj is None:
            return self
        return obj._data.get(self.my_name)

    def __set__(self, obj, value):
        if not self.validate(value):
            raise ValueError(f"'{value}' invalid to set attribute {self.my_name}")
        # if value is None and BOColumnFlag.BOC_NOT_NULL in self._flag:
        #     raise ValueError(
        #         "Value must not be 'None' for 'NOT NULL' attribute {self.my_name}"
        #     )
        obj._data[self.my_name] = value

    def validate(self, value) -> bool:
        "Validate 'value' for assignability."
        return value is None


# pylint: disable=missing-class-docstring
class BOInt(_PersistantAttr):
    @classmethod
    def data_type(cls):
        return int

    def validate(self, value):
        return value is None or isinstance(value, int)


class BOStr(_PersistantAttr):
    @classmethod
    def data_type(cls):
        return str

    def validate(self, value):
        return value is None or isinstance(value, str)


class BODatetime(_PersistantAttr):
    @classmethod
    def data_type(cls):
        return datetime

    def __set__(self, obj, value):
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        return super().__set__(obj=obj, value=value)

    def validate(self, value):
        return (
            value is None
            or isinstance(value, datetime)
        )


class BODate(_PersistantAttr):
    @classmethod
    def data_type(cls):
        return date

    def __set__(self, obj, value):
        if isinstance(value, str):
            value = date.fromisoformat(value)
        return super().__set__(obj=obj, value=value)

    def validate(self, value):
        return (
            value is None
            or isinstance(value, date)
        )


class BODict(_PersistantAttr):
    @classmethod
    def data_type(cls):
        return dict

    def validate(self, value):
        if not isinstance(value, dict):
            return value is None
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
        if not isinstance(value, list):
            return value is None
        try:
            json.dumps(value, separators=(",", ":"))
        except (ValueError, TypeError, RecursionError) as exc:
            raise TypeError(f"{value} is not serializable by JSON: {exc}") from exc
        return isinstance(value, list)


class BORelation(_PersistantAttr):
    def __init__(
        self, relation: type[BOBaseBase], flag: BOColumnFlag = BOColumnFlag.BOC_FK
    ) -> None:
        flag |= BOColumnFlag.BOC_FK
        # LOG.debug(f"{relation=}")
        if not issubclass(relation, BOBaseBase):
            raise TypeError("BO relation should be derived from BOBase.")

        super().__init__(flag, relation=relation)

    @classmethod
    def data_type(cls):
        return BOBaseBase

    def validate(self, value):
        return value is None or isinstance(value, BOBaseBase)
