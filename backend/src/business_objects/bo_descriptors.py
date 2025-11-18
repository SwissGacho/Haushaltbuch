"""Data descriptors used in business objects"""

import json
from enum import Flag, StrEnum, auto

from datetime import date, datetime
from business_objects.business_object_base import AttributeDescription
from core.app_logging import getLogger

LOG = getLogger(__name__)

from business_objects.business_attribute_base import BaseFlag


class AttributeType(StrEnum):
    """Attribute type identifiers for business objects."""

    ATYPE_INT = "int"
    ATYPE_STR = "str"
    ATYPE_DATE = "date"
    ATYPE_DATETIME = "datetime"
    ATYPE_DICT = "dict"
    ATYPE_LIST = "list"
    ATYPE_FLAG = "flag"
    ATYPE_RELATION = "relation"


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
        attribute_type: AttributeType,
        is_technical: bool = False,
        **flag_values,
    ):
        "Register an attribute in the business object descriptor"

    @classmethod
    def attribute_descriptions(cls) -> list[AttributeDescription]:
        "list of attribute descriptions"
        raise NotImplementedError

    @classmethod
    def bo_type_name(cls) -> str:
        "Get the name of this business object type"
        return cls._name()

    @classmethod
    def _name(cls) -> str:
        return cls.__name__.lower()


class _PersistantAttr[T]:
    def __init__(
        self,
        flag: BOColumnFlag = BOColumnFlag.BOC_NONE,
        is_technical: bool = False,
        **flag_values,
    ) -> None:
        self._flag = flag
        self._flag_values = flag_values
        self.my_name = None
        self.is_technical: bool = is_technical

    @classmethod
    def attribute_type(cls) -> AttributeType:
        "Attribute type as string"
        LOG.warning("attribute_type not implemented")
        raise NotImplementedError

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
            attribute_type=self.__class__.attribute_type(),
            is_technical=self.is_technical,
            **(self._flag_values or {}),
        )

    def __get__(self, obj, objtype=None) -> T:
        if obj is None:
            return self  # type: ignore[return-value]
        return obj._data.get(self.my_name)

    def __set__(self, obj, value) -> None:
        if not self.validate(value):
            raise ValueError(
                f"'{value}' invalid to set attribute {self.my_name} "
                f"of type {self.__class__.__name__}"
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
class BOInt(_PersistantAttr[int]):

    @classmethod
    def attribute_type(cls) -> AttributeType:
        return AttributeType.ATYPE_INT

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


class BOStr(_PersistantAttr[str]):

    @classmethod
    def attribute_type(cls) -> AttributeType:
        return AttributeType.ATYPE_STR

    @classmethod
    def data_type(cls):
        return str

    def validate(self, value):
        return super().validate(value) or isinstance(value, str)


class BODatetime(_PersistantAttr[datetime]):

    @classmethod
    def attribute_type(cls) -> AttributeType:
        return AttributeType.ATYPE_DATETIME

    @classmethod
    def data_type(cls):
        return datetime

    def __set__(self, obj, value):
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        return super().__set__(obj=obj, value=value)

    def validate(self, value):
        return super().validate(value) or isinstance(value, datetime)


class BODate(_PersistantAttr[date]):

    @classmethod
    def attribute_type(cls) -> AttributeType:
        return AttributeType.ATYPE_DATE

    @classmethod
    def data_type(cls):
        return date

    def __set__(self, obj, value):
        if isinstance(value, str):
            value = date.fromisoformat(value)
        return super().__set__(obj=obj, value=value)

    def validate(self, value):
        return super().validate(value) or isinstance(value, date)


class BODict(_PersistantAttr[dict]):

    @classmethod
    def attribute_type(cls) -> AttributeType:
        return AttributeType.ATYPE_DICT

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


class BOList(_PersistantAttr[list]):

    @classmethod
    def attribute_type(cls) -> AttributeType:
        return AttributeType.ATYPE_LIST

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


class BORelation(_PersistantAttr[BOBaseBase]):

    @classmethod
    def attribute_type(cls) -> AttributeType:
        return AttributeType.ATYPE_RELATION

    def __init__(
        self,
        relation: type[BOBaseBase],
        flag: BOColumnFlag = BOColumnFlag.BOC_FK,
        is_technical: bool = False,
    ) -> None:
        flag |= BOColumnFlag.BOC_FK
        # LOG.debug(f"{relation=}")
        self._relation = relation
        if not issubclass(relation, BOBaseBase):
            raise TypeError("BO relation should be derived from BOBase.")

        super().__init__(flag, relation=relation, is_technical=is_technical)

    @classmethod
    def data_type(cls) -> type[BOBaseBase]:
        return BOBaseBase

    def __set_name__(self, owner, name):
        self.my_name = name
        # LOG.debug(
        #     f"PersistantAttr.__set_name__({owner=}, {name=})"
        #     f" {self.__class__.data_type()=} {self._flag=} {self._flag_values=}"
        # )
        # assert issubclass(owner, BOBaseBase)
        owner.add_attribute(
            name,
            BOBaseBase,  # self._relation,
            self._flag or BOColumnFlag.BOC_NONE,
            attribute_type=self.__class__.attribute_type(),
            is_technical=self.is_technical,
            **(self._flag_values or {}),
        )

    def validate(self, value):
        relation = self._flag_values.get("relation")
        return (
            super().validate(value)
            or isinstance(relation, type)
            and isinstance(value, relation)
        )


class BOFlag(_PersistantAttr[Flag]):

    @classmethod
    def attribute_type(cls) -> AttributeType:
        return AttributeType.ATYPE_FLAG

    def __init__(
        self, flag_type: type[Flag], flag: BOColumnFlag = BOColumnFlag.BOC_NONE
    ) -> None:
        # LOG.debug(f"{flag_type=}; {flag=}")
        if not issubclass(flag_type, BaseFlag):
            raise TypeError("BO Flag should be derived from BaseFlag.")

        super().__init__(flag, flag_type=flag_type)

    @classmethod
    def data_type(cls):
        return BaseFlag

    def validate(self, value):
        return super().validate(value) or isinstance(
            value, self._flag_values["flag_type"]
        )

    def __set__(self, obj, value) -> None:
        """Set value of attribute, converting from str if needed"""
        # LOG.debug(f"Setting BOFlag to {value}")
        if isinstance(value, str):
            value = self._flag_values["flag_type"].flags(value)
        # LOG.debug(f"   converted BOFlag to {value}")
        super().__set__(obj=obj, value=value)
