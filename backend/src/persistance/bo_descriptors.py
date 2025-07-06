"""Data descriptors used in business objects"""

import json
from enum import Flag, auto

from datetime import date, datetime
from core.app_logging import getLogger

LOG = getLogger(__name__)

from persistance.business_attribute_base import BaseFlag


class BOColumnFlag(Flag):
    "Column flag of a BO attribute"

    BOC_NONE = 0
    BOC_NOT_NULL = auto()
    BOC_PK = auto()
    BOC_INC = auto()
    BOC_PK_INC = auto()
    BOC_FK = auto()
    BOC_UNIQUE = auto()
    BOC_DEFAULT = auto()
    BOC_CURRENT_TS = auto()
    BOC_DEFAULT_CURR = auto()


class BOBaseBase:
    "Base for BOBase to circumvent circular import"

    @classmethod
    def add_attribute(
        cls,
        attribute_name: str,
        data_type: type,
        constraint_flag: BOColumnFlag,
        **flag_values,
    ):
        pass


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
        # assert issubclass(owner, BOBaseBase)
        owner.add_attribute(
            name,
            self.__class__.data_type(),
            self._flag or BOColumnFlag.BOC_NONE,
            **(self._flag_values or {}),
        )

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        return obj._data.get(self.my_name)

    def __set__(self, obj, value) -> None:
        if not self.validate(value):
            raise ValueError(
                f"'{value}' invalid to set attribute {self.my_name} of type {self.__class__.__name__}"
            )
        obj._data[self.my_name] = value

    def validate(self, value) -> bool:
        "Validate 'value' for assignability."
        if value is None and BOColumnFlag.BOC_NOT_NULL in self._flag:
            raise ValueError(
                f"Value must not be 'None' for 'NOT NULL' attribute {self.my_name}"
            )
        return value is None


# pylint: disable=missing-class-docstring
class BOInt(_PersistantAttr):
    @classmethod
    def data_type(cls):
        return int

    def validate(self, value):
        return super().validate(value) or isinstance(value, int)


class BOId(BOInt):
    def __set__(self, obj, value):
        if self.my_name in obj._data and obj._data[self.my_name] is not None:
            raise ValueError("Cannot set id of existing object")
        obj.__class__.register_instance(obj)
        super().__set__(obj=obj, value=value)


class BOStr(_PersistantAttr):
    @classmethod
    def data_type(cls):
        return str

    def validate(self, value):
        return super().validate(value) or isinstance(value, str)


class BODatetime(_PersistantAttr):
    @classmethod
    def data_type(cls):
        return datetime

    def __set__(self, obj, value):
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        return super().__set__(obj=obj, value=value)

    def validate(self, value):
        return super().validate(value) or isinstance(value, datetime)


class BODate(_PersistantAttr):
    @classmethod
    def data_type(cls):
        return date

    def __set__(self, obj, value):
        if isinstance(value, str):
            value = date.fromisoformat(value)
        return super().__set__(obj=obj, value=value)

    def validate(self, value):
        return super().validate(value) or isinstance(value, date)


class BODict(_PersistantAttr):
    @classmethod
    def data_type(cls):
        return dict

    def validate(self, value):
        if not isinstance(value, dict):
            return super().validate(value)
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
            return super().validate(value)
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
        relation = self._flag_values.get("relation")
        return (
            super().validate(value)
            or isinstance(relation, type)
            and isinstance(value, relation)
        )


class BOFlag(_PersistantAttr):

    def __init__(
        self, flag_type: type[Flag], flag: BOColumnFlag = BOColumnFlag.BOC_NONE
    ) -> None:
        # LOG.debug(f"{relation=}")
        if not issubclass(flag_type, BaseFlag):
            raise TypeError("BO Flag should be derived from BaseFlag.")

        super().__init__(flag, flag_type=flag_type)

    @classmethod
    def data_type(cls):
        return BaseFlag

    def validate(self, value):
        return super().validate(value) or isinstance(value, Flag)

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        base_value = super().__get__(obj, objtype)
        if isinstance(base_value, str):
            return self._flag_values["flag_type"].flags(base_value)
        if not isinstance(base_value, self._flag_values["flag_type"]):
            raise ValueError(
                f"Attribute {self.my_name} of {obj.__class__.__name__} is not of type {self._flag_values['flag_type']}"
            )
        return base_value
