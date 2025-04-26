"""Source datasource."""

import logging
import time
from typing import Any, Dict, List, Tuple

import psycopg2
from psycopg2.extensions import connection as Connection, cursor as Cursor
from psycopg2.extras import execute_values

from paper_engine_strategy._encoders import cast_money
from paper_engine_strategy._types import File, Record

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

    def fetch(self, query: str, variable: Any = None) -> File:
        """Execute query, fetching one record."""
        MAX_RETRIES = 20
        RETRY_DELAY = 1  # Adjust as needed

        for attempt in range(MAX_RETRIES):
            cursor = self.cursor
            try:
                if variable:
                    cursor.execute(query, variable)
                else:
                    cursor.execute(query)
                res = cursor.fetchall()

                return res if res else None

            except Exception as e:  # Ideally, catch a more specific exception
                # Rollback the transaction
                cursor.connection.rollback()

                if attempt < MAX_RETRIES - 1:  # No need to sleep after the last attempt
                    time.sleep(RETRY_DELAY)
                    logger.debug(f"Retrying for the {attempt + 1}th attempt")
                else:
                    raise e  # If you've exhausted all retries, raise the exception.

        # If it gets here, then all retries have been exhausted
        raise Exception("All attempts to retrieve the file have failed after retrying.")

    def get_file(self, query: str, reading_range: Dict[str, int]) -> File:
        """Retrieves file from source based on range of event ids.

        Args:
            query: Query.
            reading_range: Dict with the event log to start and stop reading.

        Returns:
            File from source.
        """
        # logger.debug(f"Last event ID persisted: {reading_range['start_at']}.")

        cursor = self.cursor
        cursor.execute(query=query, vars=reading_range)
        res = cursor.fetchall()

        return res

    def fetch_execute(self, instruction: str, key_list: List[Tuple]) -> List[Tuple]:
        """Fetches records of the provided keys."""
        MAX_RETRIES = 20
        RETRY_DELAY = 1  # Adjust as needed

        for attempt in range(MAX_RETRIES):
            # self.ping_datasource()
            cursor = self.cursor
            try:
                res = execute_values(
                    cur=cursor, sql=instruction, argslist=key_list, fetch=True
                )

                return res

            except Exception as e:  # Ideally, catch a more specific exception
                # Rollback the transaction
                cursor.connection.rollback()

                if attempt < MAX_RETRIES - 1:  # No need to sleep after the last attempt
                    time.sleep(RETRY_DELAY)
                    logger.debug(f"Retrying for the {attempt}th attempt")
                else:
                    raise e  # If you've exhausted all retries, raise the exception.

        # If it gets here, then all retries have been exhausted
        raise Exception("All attempts to retrieve the file have failed after retrying.")
