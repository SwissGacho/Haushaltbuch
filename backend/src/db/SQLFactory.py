
class SQLFactory():

    @classmethod
    def getClass(cls, sql_cls: type):
        "Return a class for the SQL dialect. Implementations for specific SQL dialects should override this method."
        return sql_cls