""" module holding Exception definitions """


class OperationalError(Exception):
    "Error during DB operation"


class DBRestart(Exception):
    "signal that the DB has been reconfigured"


class ConnectionClosed(Exception):
    "a Web socket connection has been closed unexpectedly"
