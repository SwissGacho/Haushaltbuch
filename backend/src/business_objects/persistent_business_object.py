"""Base class for persistent Business Objects

Persistent Business Objects are stored in the database and represent the
application's data model."""

import copy
import json
import pprint
from typing import Any, Type, Self, Optional
from datetime import date, datetime, UTC
from core.app_logging import (
    getLogger,
    log_exit,
    DEBUG,
    VERBOSE_DEBUG,
    redact,
    pprint_lines,
)

LOG = getLogger(__name__)

from core.exceptions import CannotStoreEmptyBO
from core.util import _classproperty
from database.sql import SQL, SQLTransaction
from database.sql_expression import (
    SQLExpression,
    ColumnName,
    SQLString,
    And,
    In,
    Eq,
    Filter,
)
from database.sql_statement import CreateTable, NamedValueListList, Value
from business_objects.bo_descriptors import BOBaseBase, AttributeDescription
from business_objects.business_object_base import BOBase
from business_objects.business_attribute_base import BaseFlag


class Specialized:
    """Mixin class for specialized business objects.
    BOs derived from a specialized BO are considered
    to be a specialization without using this Mixin.

    Use it like this:
    class MyGenericBO(PersistentBusinessObject):
        ...
    class MySpecializedBO( Specialized, MyGenericBO):
        ...
    class MyVerySpecializedBO(MySpecializedBO):
        ...
    """


class Singleton:
    """Mixin class for singleton business objects.
    Singleton BOs are BOs of which there should only be one instance in the database.
    """


class PersistentBusinessObject(BOBase):
    """Base class for persistent Business Objects.
    Every subclass will be registered in a table in the database."""

    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.specialists: set[type[PersistentBusinessObject]] = set()

    @classmethod
    def is_specializing(cls: Type[Self]) -> bool:
        """Return True if this class is a specialization of another business object class."""
        return issubclass(cls, Specialized) and cls is not Specialized

    @classmethod
    def register_bo_class(cls):
        "Register the Business Object."
        if cls.is_specializing():
            specs: set[type[PersistentBusinessObject]] = {cls}
            for super_cls in cls.__mro__:
                if hasattr(super_cls, "specialists") and issubclass(
                    super_cls, PersistentBusinessObject
                ):
                    specs.add(super_cls)
                    super_cls.specialists = super_cls.specialists | specs
                    if (
                        not hasattr(super_cls, "is_specializing")
                    ) or not super_cls.is_specializing():
                        break
        super().register_bo_class()

    @classmethod
    def attributes_as_dict(cls, include_specialized: bool = False) -> dict[str, type]:
        """dict of BO attribute types with attribute names as keys.
        If 'include_specialized' is True, also include the attributes of specialized BOs.
        """
        attrs = super().attributes_as_dict()
        if include_specialized and getattr(cls, "specialists", None):
            for specialized in cls.specialists:
                attrs.update(
                    {
                        a.name: a.data_type
                        for a in specialized.attribute_descriptions(
                            include_specialized=False
                        )
                        if a.name not in attrs
                    }
                )
        return attrs

    @classmethod
    def attribute_descriptions(
        cls, include_specialized: bool = False
    ) -> list[AttributeDescription]:
        """Return the list of attribute descriptions for this business object class.
        If 'include_specialized' is True, also include the attributes of specialized BOs.
        """
        descriptions = super().attribute_descriptions()
        if include_specialized and getattr(cls, "specialists", None):
            for specialized in cls.specialists:
                descriptions += [
                    a
                    for a in specialized.attribute_descriptions(
                        include_specialized=False
                    )
                    if a.name not in [d.name for d in descriptions]
                ]
        return descriptions

    @classmethod
    def navigation_header(
        cls, ref: AttributeDescription | str | None = None
    ) -> dict[str, str] | None:
        """Return a navigation header for this business object class.
        'ref' can be used to specify a reference name if this BO is referenced by another BO.
        Return None if this BO should not be included in the navigation."""
        ref_name = ref.name if isinstance(ref, AttributeDescription) else ref
        if ref_name:
            return {
                "name": f"{cls.__name__.lower()}.{ref_name}",
                "display_name": f"{cls.__name__} ({ref_name})",
            }
        return {"name": cls.__name__.lower(), "display_name": cls.__name__}

    # pylint: disable=no-self-argument
    @_classproperty
    def all_business_objects(
        cls: Type[Self],  # type: ignore[reportGeneralTypeIssues]
    ) -> dict[str, type[BOBase]]:
        "Set of registered Business Objects"
        return {
            _name: _cls
            for _name, _cls in BOBase.all_business_objects.items()  # pylint: disable=no-member
            if issubclass(_cls, PersistentBusinessObject)
        }

    @classmethod
    def convert_from_db(cls, value, typ, subtyp):
        "convert a value of type 'typ' read from the DB"
        # LOG.debug(
        #     f"PersistentBusinessObject.convert_from_db({value=}, {type(value)=}, {typ=}, {subtyp=})"
        # )
        if value is None:
            return None
        if typ == date and isinstance(value, str):
            return date.fromisoformat(value)
        if typ == datetime and isinstance(value, (str, datetime)):
            dt = value if isinstance(value, datetime) else datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)
        if typ in [dict, list] and isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError as exc:
                LOG.error(
                    f"PersistentBusinessObject.convert_from_db: JSONDecodeError: {exc}"
                )
        if typ == BOBaseBase and isinstance(value, int):
            relation = subtyp.get("relation") if isinstance(subtyp, dict) else None
            if isinstance(relation, type) and issubclass(relation, BOBase):
                return relation(bo_id=value)
            LOG.error(
                f"PersistentBusinessObject.convert_from_db: Cannot convert {value} to class {relation}"
            )
        if (
            isinstance(typ, type)
            and issubclass(typ, BaseFlag)
            and isinstance(value, str)
        ):
            value = subtyp["flag_type"].flags(
                value
            )  # TODO: Probably also check if subtype is valid and has 'flag_type' key
        return copy.deepcopy(value)

    @classmethod
    async def sql_create_table(cls):
        "Create a DB table for this class"
        if issubclass(cls, Specialized):
            LOG.debug(
                f"PersistentBusinessObject.sql_create_table(): {cls.__name__} is a Specialized BO, skipping table creation"
            )
            return
        LOG.debug(f"PersistentBusinessObject.sql_create_table(): {cls.table=}")
        LOG.log(VERBOSE_DEBUG, f"   collect attributes for {cls.__name__}")
        async with SQLTransaction() as txaction:
            create_table: CreateTable = txaction.sql().create_table(cls.table)
            for description in cls.attribute_descriptions(include_specialized=True):
                for line in pprint_lines(description):
                    LOG.log(VERBOSE_DEBUG, f"   - {line}")
                create_table.column(
                    name=description.name,
                    data_type=description.data_type,
                    constraint=description.constraint,
                    **description.constraint_values,
                )
            await create_table.execute()

    @classmethod
    def _filter_conditions(
        cls, conditions: Optional[SQLExpression] = None
    ) -> SQLExpression | None:
        if getattr(cls, "specialists", None):
            cond = In(
                ColumnName("bo_name"),
                [SQLString(s.bo_type_name()) for s in cls.specialists],
            )
            if conditions:
                return And([cond, conditions])
            return cond
        return conditions

    @classmethod
    async def count_rows(cls, conditions: Optional[dict] = None) -> int:
        """Count the number of existing business objects in the DB table matching the conditions"""
        async with SQL() as sql:
            select = sql.select(["count(*) as count"]).from_(cls.table)
            filter_conditions: SQLExpression | None = cls._filter_conditions(
                Filter(conditions) if conditions else None
            )
            if filter_conditions:
                select.where(filter_conditions)
            result = await (await select.execute()).fetchone()
        LOG.debug(
            f"PersistentBusinessObject.count_rows({conditions=}) "
            f"{result=} -> return {result['count']}"
        )
        return result["count"]

    @classmethod
    async def get_matching_ids(cls, conditions: dict | None = None):
        """Get the ids of business objects matching the conditions"""
        async with SQL() as sql:
            select = sql.select(["id"]).from_(cls.table)
            filter_conditions: SQLExpression | None = cls._filter_conditions(
                Filter(conditions) if conditions else None
            )
            if filter_conditions:
                select.where(filter_conditions)
            result = await (await select.execute()).fetchall()
        # LOG.debug(f"PersistentBusinessObject.get_matching_ids({conditions=}) -> {result=}")
        return [id["id"] for id in result]

    @classmethod
    async def get_matching_objects(
        cls, conditions: dict | None = None, attributes: list[str] | None = None
    ) -> list[BOBase]:
        """Get the business objects matching the conditions"""
        if attributes:
            cols = [
                a
                for a in attributes
                if a in cls.attributes_as_dict(include_specialized=True)
            ]
            if "id" not in cols:
                cols.append("id")
            if "bo_name" not in cols and getattr(cls, "specialists", None):
                cols.append("bo_name")
        else:
            cols = None
        async with SQL() as sql:
            select = sql.select(cols).from_(cls.table)
            filter_conditions: SQLExpression | None = cls._filter_conditions(
                Filter(conditions) if conditions else None
            )
            if filter_conditions:
                select.where(filter_conditions)
            result = await (await select.execute()).fetchall()
        LOG.debug(
            f"PersistentBusinessObject.get_matching_objects(conditions={redact(conditions)}, {attributes=}) "
            f"-> {len(result)} objects"
        )
        if LOG.isEnabledFor(VERBOSE_DEBUG):
            for line in pprint_lines(redact(result)):
                LOG.log(VERBOSE_DEBUG, f"  {line}")
        descriptions = {
            d.name: d for d in cls.attribute_descriptions(include_specialized=True)
        }
        objects: list[BOBase] = []
        for obj in result:
            bo_name = None
            converted: dict[str, Any] = {}
            for key, value in obj.items():
                if key == "id":
                    continue
                if key == "bo_name":
                    bo_name = value
                description = descriptions.get(key)
                if description is None:
                    converted[key] = value
                    continue
                converted[key] = cls.convert_from_db(
                    value,
                    description.data_type,
                    description.constraint_values,
                )
            LOG.debug(
                f"Creating {(cls if bo_name is None else cls.get_business_object_by_name(bo_name)).__name__} object with id={obj.get('id')}"
            )
            objects.append(
                (cls if bo_name is None else cls.get_business_object_by_name(bo_name))(
                    bo_id=obj.get("id"), **converted
                )
            )
        return objects

    async def fetch(self, id=None, newest=None):
        """Fetch the content for a business object instance from the DB.
        If 'id' is given, fetch the identified object
        If 'id' omitted and 'newest'=True fetch the object with highest id
        If the oject is not found in the DB return the instance unchanged
        """
        # LOG.debug(f"PersistentBusinessObject.fetch({id=}, {newest=})")
        if id is None:
            id = self.id
        if id is None and newest is None:
            LOG.debug(f"fetching {self} without id or newest")
            return self
        # LOG.debug(f"fetching {self} with {id=}, {newest=}")
        async with SQL() as sql:
            await self._fetch_self(sql, id=id, newest=newest)
        return self

    async def _fetch_self(self, sql: SQL, id=None, newest=None):
        select = sql.select([], True).from_(self.table)
        filter_conditions = self._filter_conditions(
            Eq("id", id)
            if id is not None
            else (
                SQLExpression(f"id = (SELECT MAX(id) FROM {self.table})")
                if newest
                else None
            )
        )
        if filter_conditions:
            select.where(filter_conditions)
        # LOG.debug(f"BOBase._fetch_self: {select=} // {select.get_sql()=}")
        self._db_data = await (await select.execute()).fetchone()

        if self._db_data:
            if LOG.isEnabledFor(VERBOSE_DEBUG):
                LOG.log(
                    VERBOSE_DEBUG, f"{self.__class__.__name__}._fetch_self: _db_data="
                )
                for line in pprint_lines(self._db_data):
                    LOG.log(VERBOSE_DEBUG, f" -  {line}")
            for description in self.attribute_descriptions():
                self._data[description.name] = PersistentBusinessObject.convert_from_db(
                    self._db_data.get(description.name),
                    description.data_type,
                    description.constraint_values,
                )
            if LOG.isEnabledFor(VERBOSE_DEBUG):
                LOG.log(VERBOSE_DEBUG, f"{self.__class__.__name__}._fetch_self: _data=")
                for line in pprint_lines(self._data):
                    LOG.log(VERBOSE_DEBUG, f" -  {line}")
            self.register_instance(self)
        # LOG.debug(f"Fetched {self} from DB: {self._data=}")

    async def store(self):
        """Store the business object in the database.
        If 'self.id is None' a new row is inserted
        Else the existing row is updated
        """
        if self.id is None:
            await self._insert_self()
        else:
            await self._update_self()
        await super().store()

    async def business_values_as_dict(self) -> dict[str, Any]:
        # LOG.debug(f"{self}.business_values_as_dict: {self.id=}")
        await self.fetch(self.id)
        return await super().business_values_as_dict()

    async def _insert_self(self):
        assert self.id is None, "id must be None for insert operation"
        if isinstance(self, Singleton):
            existing_count = await self.count_rows()
            if existing_count > 0:
                raise CannotStoreEmptyBO(
                    f"Cannot insert {self} as it is a Singleton and already exists in the DB"
                )
        self.bo_name = self.bo_type_name()
        values_to_store: NamedValueListList = [
            (k, v) for k, v in self._data.items() if k != "id" and v is not None
        ]
        if not values_to_store:
            raise CannotStoreEmptyBO(f"Cannot store {self._data=} as it has no values")
        LOG.debug(f"Inserting new {self} into DB")
        async with SQLTransaction() as txaction:
            self.id = (
                await (
                    await (
                        txaction.sql()
                        .insert(self.table)
                        .rows(values_to_store)
                        .returning("id")
                    ).execute()
                ).fetchone()
            ).get("id")
            # read the new row back to get any default values set by the DB
            await self._fetch_self(txaction.sql(), id=self.id)

    async def _update_self(self):
        assert self.id is not None, "id must not be None for update operation"
        if LOG.isEnabledFor(VERBOSE_DEBUG):
            LOG.log(VERBOSE_DEBUG, f"{self.__class__.__name__}._update_self: _data=")
            for line in pprint_lines(self._data):
                LOG.log(VERBOSE_DEBUG, f" -  {line}")
        async with SQLTransaction() as txaction:
            value_class = Value
            update = txaction.sql().update(self.table).where(Eq("id", self.id))
            changes = False
            descriptions = {d.name: d for d in self.attribute_descriptions()}
            for k, v in self._data.items():
                if k not in (
                    "bo_name",
                    "id",
                ) and v != PersistentBusinessObject.convert_from_db(
                    self._db_data.get(k),
                    descriptions[k].data_type,
                    descriptions[k].constraint_values,
                ):
                    changes = True
                    update.assignment(k, value_class(k, v))
            k = "last_updated"
            if changes and not (k in self._data and self._data[k]):
                self._data[k] = datetime.now().astimezone(UTC)
                update.assignment(k, value_class(k, self._data[k]))
            try:
                if changes:
                    await update.execute()
            finally:
                # read the row back to get any changes made by the DB (e.g. triggers)
                await self._fetch_self(txaction.sql(), id=self.id)


log_exit(LOG)
