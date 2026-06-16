"""Global constants"""

APPNAME = "moneypilot"
APPDESC = f"""'{APPNAME}' helps you keeping control over the money you have available.
            Record your expenses and compare to your planned budget.
        """

FILE_CONFIG_BOID = -1
CMDLINE_CONFIG_BOID = -2

FILECFG_FILE_NAME = "configuration.json"
CONFIG_FILECFG_FILE = "filecfg_file"

WEBSOCKET_PORT = 8765

SINGLE_USER_NAME = "<single_user>"

# Monetary precision policy shared across DB backends.
DEFAULT_DECIMAL_SCALE_DIGITS = 4
MAX_DECIMAL_TOTAL_DIGITS = 18
