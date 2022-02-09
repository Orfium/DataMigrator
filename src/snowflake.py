import snowflake.connector

from config import (
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_WH,
    SNOWFLAKE_DB,
)


class SnowFlakeExecutor:
    """
    Allows execution of queries in Snowflake, the proper way to execute queries is
    using context manager:

    e.g.:
        with SnowFlakeExecutor() as db:
            db.execute.....
    """

    __instance = None

    def __init__(self):
        self._connect()

    def _connect(self):
        if SnowFlakeExecutor.__instance is None:
            # make the connection
            self._conn = snowflake.connector.connect(
                user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PASSWORD,
                account=SNOWFLAKE_ACCOUNT,
                warehouse=SNOWFLAKE_WH,
                role=SNOWFLAKE_ROLE,
                database=SNOWFLAKE_DB,
            )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn.close()

    def execute_query(self, query):
        cs = self._conn.cursor()
        cs.execute(query)
        cs.close()
