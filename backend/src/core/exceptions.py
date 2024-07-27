""" module holding Exception definitions """


class OperationalError(Exception):
    "Error during DB operation"


class DBSchemaError(Exception):
    "Invalid DB schema detected."


class DBRestart(Exception):
    "signal that the DB has been reconfigured"


class ConnectionClosed(Exception):
    "a Web socket connection has been closed unexpectedly"


class ConfigurationError(Exception):
    "The configuration (as read from DB) is invalid"


class DataError(Exception):
    "DB consistancy error"
