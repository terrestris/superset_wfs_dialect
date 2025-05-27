import csv
import datetime
from typing import Optional, Dict
import os

class SQLLogger:

    def log_sql(self, operation: str, parameters: Optional[Dict] = None):
        """Logs the SQL operation and parameters to a CSV file."""

        if os.environ.get("ENABLE_SQL_LOGGING", "false").lower() != "true":
            return

        with open("/app/sql_log.csv", "a", newline="", encoding="utf-8") as logfile:
            writer = csv.writer(logfile, delimiter=";", quotechar="'")
            params_str = "---"
            if parameters:
                params_str = ""
                if isinstance(parameters, dict):
                    params_str = ",".join(f"{k}={v}" for k, v in parameters.items())
                else:
                    params_str = ",".join(str(p) for p in parameters)
            operation = operation.replace('\n', '').replace('\r', '')
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow([timestamp, operation, params_str])
