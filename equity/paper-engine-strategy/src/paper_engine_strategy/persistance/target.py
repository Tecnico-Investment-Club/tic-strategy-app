"""Postgres datasource."""

import logging
from typing import Any, Dict, Iterator, List, Optional, Tuple

import psycopg2
from psycopg2.extensions import connection as Connection, cursor as Cursor
from psycopg2.extras import execute_values

from paper_engine_strategy._encoders import cast_money
from paper_engine_strategy._types import File, Keys

logger = logging.getLogger(__name__)


class Target(object):
    """Postgres data source."""

    _connection: Connection
    _cursor: Cursor

    def __init__(self, connection_string: str) -> None:
        """Postgres' data source.

        Args:
            connection_string: Definitions to connect with data source.
        """
        self._connection_string: str = connection_string
        self._in_progress: bool = False

    def connect(self) -> None:
        """Connects to data source."""
        self._connection = psycopg2.connect(dsn=self._connection_string)
        self._connection.autocommit = False
        money_type = psycopg2.extensions.new_type((790,), "MONEY", cast_money)
        psycopg2.extensions.register_type(money_type, self._connection)
        url = self.ping_datasource()
        logger.info(f"{self.__class__.__name__} connected to: {url}")

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
        if self._in_progress and self._cursor is not None:
            cursor = self._cursor
        else:
            cursor = self._connection.cursor()

        return cursor

    def begin_transaction(self) -> None:
        """Begins a transaction."""
        self._cursor = self.cursor
        self._in_progress = True

    def commit_transaction(self) -> None:
        """Commits a transaction."""
        self._connection.commit()
        self._in_progress = False

    def rollback_transaction(self) -> None:
        """Rolls back a transaction."""
        self._connection.rollback()
        self._in_progress = False

    def disconnect(self) -> None:
        """Disconnects data source connection."""
        url = self.ping_datasource()
        self._connection.close()
        self._in_progress = False
        logger.info(f"{self.__class__.__name__} disconnected from: {url}")

    def get_strategy_id(self, strategy_hash: str) -> Optional[int]:
        """Get portfolio_optimization id related to provided params."""
        query = (
            "SELECT strategy_id " "FROM strategy_config " "WHERE strategy_hash = %s;"
        )
        cursor = self.cursor
        cursor.execute(query=query, vars=(strategy_hash,))
        res = cursor.fetchone()

        return res[0] if res else None

    def get_next_strategy_id(self) -> int:
        """Gets next delivery id.

        Returns:
            Next delivery id.
        """
        cursor = self.cursor
        # ALTER SEQUENCE delivery_id_strategy_seq RESTART;
        cursor.execute("SELECT NEXTVAL('strategy_id_seq');")
        res = cursor.fetchone()

        return res[0]

    def get_next_delivery_id(self) -> int:
        """Gets next delivery id.

        Returns:
            Next delivery id.
        """
        cursor = self.cursor
        # ALTER SEQUENCE delivery_id_strategy_seq RESTART;
        cursor.execute("SELECT NEXTVAL('delivery_id_strategy_seq');")
        res = cursor.fetchone()

        return res[0]

    def get_next_event_id(self, n: int = 1) -> Iterator[int]:
        """Gets next event id.

        Args:
            n: Number of event ids to get.

        Yields:
            Next event id.
        """
        cursor = self.cursor
        # ALTER SEQUENCE event_id_delivery_s3_seq RESTART;
        cursor.execute(
            "SELECT NEXTVAL('event_id_strategy_seq') "
            "FROM GENERATE_SERIES(1, %(n_event_ids)s);",
            vars={"n_event_ids": n},
        )

        event_id = cursor.fetchone()
        while event_id is not None:
            yield event_id[0]
            event_id = cursor.fetchone()

    def get_current_state(self, query: str, args: Keys = None) -> List[Tuple]:
        """Gets current state of entity records.

        Args:
            query: Query to fetch entity current state records.
            args: List of tuples, each containing a key of corresponding entity
                to fetch state.

        Returns:
            Next delivery id.
        """
        cursor = self.cursor
        if args:
            records = execute_values(cur=cursor, sql=query, argslist=args, fetch=True)
        else:
            cursor = self.cursor
            cursor.execute(query=query, vars=args)
            records = cursor.fetchall()

        return records

    def persist_delivery(self, args: Dict[str, Any]) -> None:
        """Persist delivery state.

        Args:
            args: Delivery metadata to persist.
        """
        query = (
            "INSERT INTO loader_strategy ("
            "delivery_id, "
            "delivery_ts, runtime"
            ") VALUES ("
            "%(delivery_id)s, "
            "%(delivery_ts)s, %(runtime)s"
            ");"
        )
        cursor = self.cursor
        cursor.execute(query=query, vars=args)

    def execute(self, instruction: str, logs: List[Tuple]) -> None:
        """Executes an instruction (CREATE, AMEND, REMOVE) for given logs.

        Args:
            instruction: Instruction to execute (CREATE, AMEND, REMOVE).
            logs: Event logs to apply in instruction.
        """
        if logs:
            cursor = self.cursor
            execute_values(cur=cursor, sql=instruction, argslist=logs)
