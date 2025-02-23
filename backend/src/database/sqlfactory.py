"""Factory for generating SQL queries."""


class SQLFactory:
    """Factory class for generating SQL queries.
    Implementations for specific SQL dialects should inherit from this class."""

    @classmethod
    def get_sql_class(cls, sql_cls: type):
        """Return a class for the SQL dialect.
        Implementations for specific SQL dialects should override this method."""
        return sql_cls
