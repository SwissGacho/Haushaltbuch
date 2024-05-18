""" Data descriptors used in business objects """

from datetime import date, datetime
from core.app_logging import getLogger

LOG = getLogger(__name__)


class PersistantAttr:
    def __init__(self, primary_key=False, auto_inc=False, current_dt=False) -> None:
        self._pk = primary_key
        self._auto_inc = auto_inc
        self._current_date_time = current_dt

    def __set_name__(self, owner, name):
        self.my_name = name
        # LOG.debug(f"__set_name__ {owner=} {name} {self.data_type()=} {self._pk=} {self._auto_inc=}")
        if self._pk:
            cols = (name, self.data_type(), "pkinc" if self._auto_inc else "pk")
        elif self._current_date_time:
            cols = (name, self.data_type(), "dt")
        else:
            cols = (name, self.data_type(), None)
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


class BO_int(PersistantAttr):
    @classmethod
    def data_type(cls):
        return int

    def validate(self, value):
        return isinstance(value, int)


class BO_str(PersistantAttr):
    @classmethod
    def data_type(cls):
        return str

    def validate(self, value):
        return isinstance(value, str)


class BO_datetime(PersistantAttr):
    @classmethod
    def data_type(cls):
        return datetime

    def validate(self, value):
        return isinstance(value, datetime)


class BO_date(PersistantAttr):
    @classmethod
    def data_type(cls):
        return date

    def validate(self, value):
        return isinstance(value, date)
