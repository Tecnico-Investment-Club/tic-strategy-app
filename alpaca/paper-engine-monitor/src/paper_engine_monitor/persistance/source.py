"""Source datasource."""

import logging
from typing import Any, List, Tuple

import psycopg2
from psycopg2.extensions import connection as Connection, cursor as Cursor
from psycopg2.extras import execute_values

from paper_engine_monitor._encoders import cast_money
from paper_engine_monitor._types import File, Record

logger = logging.getLogger(__name__)


class Source(object):
    """Source data source."""

    _connection: Connection
    _cursor: Cursor

    def __init__(self, connection_string: str) -> None:
        """Postgres' data source.

        Args:
            connection_string: Definitions to connect with data source.
        """
        self._connection = psycopg2.connect(dsn=connection_string)
        self._connection.autocommit = False
        self._tx_in_progress: bool = False
        self._tx_cursor = None
        money_type = psycopg2.extensions.new_type((790,), "MONEY", cast_money)
        psycopg2.extensions.register_type(money_type, self._connection)

    def connect(self) -> None:
        """Connects to data source."""
        url = self.ping_datasource()
        logger.info(f"{self.__class__.__name__} connected to: {url}.")

    def ping_datasource(self) -> str:
        """Pings data source."""
        cursor = self.cursor
        cursor.execute(
            "SELECT CONCAT("
            "current_user,'@',inet_server_addr(),':',"
            "inet_server_port(),' - ',version()"
            ") as v"
        )

        return cursor.fetchone()[0]

    @property
    def cursor(self) -> psycopg2.extensions.cursor:
        """Gets postgres cursor."""
        if self._tx_in_progress and self._tx_cursor is not None:
            cursor = self._tx_cursor
        else:
            cursor = self._connection.cursor()

        return cursor

    def begin_transaction(self) -> None:
        """Begins a transaction."""
        self._tx_cursor = self.cursor
        self._tx_in_progress = True

    def commit_transaction(self) -> None:
        """Commits a transaction."""
        self._connection.commit()
        self._tx_in_progress = False

    def rollback_transaction(self) -> None:
        """Rolls back a transaction."""
        self._connection.rollback()
        self._tx_in_progress = False

    def disconnect(self) -> None:
        """Disconnects data source connection."""
        self._connection.close()
        self._tx_in_progress = False

    def fetch_one(self, query: str, variable: Any = None) -> Record:
        """Execute query, fetching one record."""
        cursor = self.cursor
        if variable:
            cursor.execute(query, variable)
        else:
            cursor.execute(query)
        res = cursor.fetchone()

        return res if res else None

    def get_file(self, query: str, variable: Any = None) -> File:
        """Retrieves file from source based on range of event ids."""
        cursor = self.cursor
        if variable:
            cursor.execute(query, variable)
        else:
            cursor.execute(query)
        res = cursor.fetchall()

        return res

    def fetch_execute(self, instruction: str, key_list: List[Tuple]) -> List[Tuple]:
        """Fetches records of the provided keys.

        Args:
            instruction: sql query.
            key_list: list of keys.

        Returns:
            Records with the provided keys.
        """
        cursor = self.cursor
        res = execute_values(cur=cursor, sql=instruction, argslist=key_list, fetch=True)

        return res
