"""Global constants"""

APPNAME = "moneypilot"
APPDESC = f"""'{APPNAME}' helps you keeping control over the money you have available.
            Record your expenses and compare to your planned budget.
        """
DBCFG_FILE_NAME = "configuration.json"
CONFIG_DBCFG_FILE = "dbcfg_file"

WEBSOCKET_PORT = 8765

SINGLE_USER_NAME = "<single_user>"

# Monetary precision policy shared across DB backends.
DEFAULT_DECIMAL_SCALE_DIGITS = 4
MAX_DECIMAL_TOTAL_DIGITS = 18
