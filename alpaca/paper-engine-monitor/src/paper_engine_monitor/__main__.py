"""Loader execution class."""

import argparse
import ast
from datetime import datetime
from decimal import Decimal
import hashlib
import json
import logging
import os
import secrets
from sys import stdout
import time
from typing import (
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
)

from paper_engine_monitor._types import File, Key, Keys
import paper_engine_monitor.broker as broker
import paper_engine_monitor.model as model
from paper_engine_monitor.model.base import State
from paper_engine_monitor.model.entity import Entity
from paper_engine_monitor.persistance import source, target
import paper_engine_monitor.queries as queries
from paper_engine_monitor.queries.base import BaseQueries
import paper_engine_monitor.queries.source_queries as source_queries


logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=stdout,
)
logger = logging.getLogger(__name__)


class Loader:
    """Loader main class."""

    _account_id: str
    _portfolio_id: int
    _initial_portfolio_value: Optional[int]
    _source: source.Source
    _target: target.Target
    _broker: broker.Alpaca
    _notifications: bool
    _dry_run: bool
    _min_sleep: int
    _max_sleep: int

    _entities: Set[Entity] = {
        Entity.PORTFOLIO,
        Entity.PORTFOLIO_LATEST,
        Entity.PORTFOLIO_CONTROL,
        Entity.POSITION,
        Entity.POSITION_LATEST,
    }
    _queries: Dict[Entity, BaseQueries] = {
        Entity.PORTFOLIO: queries.PortfolioQueries(),
        Entity.PORTFOLIO_LATEST: queries.PortfolioLatestQueries(),
        Entity.PORTFOLIO_CONTROL: queries.PortfolioControlQueries(),
        Entity.POSITION: queries.PositionQueries(),
        Entity.POSITION_LATEST: queries.PositionLatestQueries(),
    }
    _state: Dict[Entity, Type[State]] = {
        Entity.PORTFOLIO: model.Portfolio,
        Entity.PORTFOLIO_LATEST: model.PortfolioLatest,
        Entity.PORTFOLIO_CONTROL: model.PortfolioControl,
        Entity.POSITION: model.Position,
        Entity.POSITION_LATEST: model.PositionLatest,
    }

    def setup(self, args: argparse.Namespace) -> None:
        """Prepares loader components.

        Args:
            args: Variables given by user when starting loader process.

        Connects to source, all possible targets and notification components.
        """
        self._dry_run = args.dry_run
        self._min_sleep = args.min_sleep
        self._max_sleep = args.max_sleep

        # prepare persistence FROM
        self._source = source.Source(args.source)

        # prepare persistence
        self._target = target.Target(args.target)

        # ALPACA
        self._broker = broker.Alpaca(args.api_key, args.secret_key)
        self._account_id = hashlib.sha256(
            (args.api_key + args.secret_key).encode("utf-8")
        ).hexdigest()
        self._portfolio_id = args.portfolio_id
        self._initial_portfolio_value = args.initial_portfolio_value

        sql_directory = "/db"

        for filename in os.listdir(sql_directory):
            if filename == "db.sql":
                continue
            file_path = os.path.join(sql_directory, filename)
            with open(file_path, "r") as sql_file:
                sql_script = sql_file.read()
                self._source.init_tables(sql_script)


    def tear_down(self) -> None:
        """Cleans loader settings.

        Disconnects from source, target and notification components.
        """
        # cleanup and leave
        self._source.disconnect()
        self._target.disconnect()

    def run(self, args: argparse.Namespace) -> None:
        """Checks the command line arguments for execution type.

        This process can be executed as a service or just once.

        Args:
            args: Variables given by user when starting loader process.
        """
        self.setup(args=args)

        self._source.connect()
        self._target.connect()

        # type of execution
        if args.run_as_service:
            self.run_service()
        else:
            self.run_once()

        self.tear_down()

    def run_service(self) -> None:
        """Runs the application as a continuous process.

        Polls the source every X seconds for new records.
        Once new records are found, operations are applied to the "target" components.
        """
        logger.info(
            f"Running as a service. "
            f"source_polling_interval=[{self._min_sleep},{self._max_sleep}]."
        )

        continue_watching_folder = True
        while continue_watching_folder:
            try:
                self.run_once()
                t = secrets.choice(
                    [self._min_sleep, self._max_sleep]
                    + [i for i in range(self._min_sleep, self._max_sleep)]
                )
                time.sleep(t)
            except Exception as e:
                logger.warning("error while importing:", e)
                continue_watching_folder = False

        logger.info("Terminating...")

    def run_once(self) -> None:
        """Runs the synchronization process once."""
        start_time: datetime = datetime.utcnow()
        end_time: datetime

        portfolio_value = self._broker.get_portfolio_value()
        position_records = self.get_position_records(self._portfolio_id, portfolio_value)
        portfolio_records = self.get_portfolio_records(self._portfolio_id)
        control_records: File = [
            (
                self._portfolio_id,
                datetime.utcnow(),
            )
        ]
        delivery_id: int = self._target.get_next_delivery_id()
        delivery: Dict = {
            Entity.PORTFOLIO: self.process(
                delivery_id=delivery_id,
                entity=Entity.PORTFOLIO,
                file=portfolio_records
            ),
            Entity.PORTFOLIO_LATEST: self.process(
                delivery_id=delivery_id,
                entity=Entity.PORTFOLIO_LATEST,
                file=portfolio_records,
            ),
            Entity.PORTFOLIO_CONTROL: self.process(
                delivery_id=delivery_id,
                entity=Entity.PORTFOLIO_CONTROL,
                file=control_records,
            ),
            Entity.POSITION: self.process(
                delivery_id=delivery_id,
                entity=Entity.POSITION,
                file=position_records
            ),
            Entity.POSITION_LATEST: self.process(
                delivery_id=delivery_id,
                entity=Entity.POSITION_LATEST,
                file=position_records,
            ),
        }

        # persist delivery
        if not self._dry_run:
            self.persist_delivery(
                delivery_id=delivery_id,
                start_time=start_time,
                delivery=delivery,
            )

        del delivery

        end_time = datetime.utcnow()
        logger.info(
            f"Delivery {delivery_id}: processed ({end_time - start_time} seconds)."
        )

    def get_position_records(self, portfolio_id: int, portfolio_value: Decimal) -> File:
        """Get position records from broker."""
        positions = self._broker.get_all_positions()
        res: File = []
        ts = datetime.utcnow()
        for asset_id, pos in positions.items():
            side = 1 if pos["side"] == "long" else -1
            asset_id_type = "CRYPTO_TICKER" if asset_id[-3:] == "USD" else "STOCK_TICKER"
            res.append(
                (
                    portfolio_id,
                    side,
                    asset_id_type,
                    asset_id,
                    ts,
                    pos["notional"] / portfolio_value,
                    pos["quantity"],
                    pos["notional"],
                )
            )
        return res

    def get_prev_portfolio(self, portfolio_id: int) -> Optional[model.PortfolioLatest]:
        """Get previous state of the portfolio."""
        db_response = self._target.get_current_state(
            queries.PortfolioLatestQueries.LOAD_STATE, [(portfolio_id,)]
        )
        if not db_response:
            return None
        portfolio_record = db_response[0]
        portfolio_obj = model.PortfolioLatest.from_target(portfolio_record)
        return portfolio_obj

    def get_portfolio_records(self, portfolio_id: int) -> File:
        """Get portfolio records."""
        curr_portfolio_value = self._broker.get_portfolio_value()
        curr_long_value = self._broker.get_portfolio_value(side="LONG")
        curr_short_value = self._broker.get_portfolio_value(side="SHORT")

        long_wgt = curr_long_value / curr_portfolio_value
        short_wgt = curr_short_value / curr_portfolio_value

        prev_portfolio = self.get_prev_portfolio(portfolio_id)
        if not prev_portfolio and not self._initial_portfolio_value:
            prev_portfolio_value = curr_portfolio_value
            prev_long_value = curr_long_value
            prev_short_value = curr_short_value

            prev_cum_rtn = Decimal(0)
            prev_long_cum_rtn = Decimal(0)
            prev_short_cum_rtn = Decimal(0)

        elif not prev_portfolio:
            prev_portfolio_value = Decimal(self._initial_portfolio_value)
            prev_long_value = Decimal(0)
            prev_short_value = Decimal(0)

            prev_cum_rtn = Decimal(0)
            prev_long_cum_rtn = Decimal(0)
            prev_short_cum_rtn = Decimal(0)

        else:
            prev_portfolio_value = prev_portfolio.notional
            prev_long_value = prev_portfolio.long_notional
            prev_short_value = prev_portfolio.short_notional

            prev_cum_rtn = prev_portfolio.cum_rtn
            prev_long_cum_rtn = prev_portfolio.long_cum_rtn
            prev_short_cum_rtn = prev_portfolio.short_cum_rtn

        rtn = self.compute_return(curr_portfolio_value, prev_portfolio_value)
        long_rtn = self.compute_return(curr_long_value, prev_long_value)
        short_rtn = self.compute_return(curr_short_value, prev_short_value, side="SHORT")

        cum_rtn = self.compute_cum_return(prev_cum_rtn, rtn)
        long_cum_rtn = self.compute_cum_return(prev_long_cum_rtn, long_rtn)
        short_cum_rtn = self.compute_cum_return(prev_short_cum_rtn, short_rtn)

        records: File = [
            (
                portfolio_id,
                datetime.utcnow(),
                curr_long_value,
                curr_short_value,
                curr_portfolio_value,
                long_wgt,
                short_wgt,
                long_rtn,
                long_cum_rtn,
                short_rtn,
                short_cum_rtn,
                rtn,
                cum_rtn,
            )
        ]
        return records

    @staticmethod
    def compute_return(
        curr_value: Decimal, prev_value: Decimal, side: str = None
    ) -> Decimal:
        """Compute return from portfolio values."""
        if curr_value and prev_value:
            if side == "SHORT":
                return -((curr_value - prev_value) / prev_value)
            else:
                return (curr_value - prev_value) / prev_value
        else:
            return Decimal(0)

    @staticmethod
    def compute_cum_return(prev_cum_rtn: Decimal, rtn: Decimal) -> Decimal:
        """Compute cumulative return."""
        return (1 + prev_cum_rtn) * (1 + rtn) - 1

    def process(self, delivery_id: int, entity: Entity, file: File) -> Dict:
        """Processes entity records present in delivery.

        Args:
            delivery_id: Delivery id.
            entity: Entity type.
            file: Entity records.

        Returns:
            A dictionary with EventType (CREATE, AMEND, REMOVE) as key and a list
            containing the event logs for each event type as value.
        """
        logger.info(f"Delivery {delivery_id}: processing {entity}...")

        query: BaseQueries = self._queries[entity]
        state_type: Type[State] = self._state[entity]

        # previous state
        keys: Keys = state_type.list_ids_from_source(records=file)
        # fetch records from state with keys
        if entity in [Entity.POSITION_LATEST]:
            prev_state: List[Tuple] = self._target.get_current_state(
                query=query.LOAD_FULL_STATE, args=[(self._portfolio_id,)]
            )
        else:
            prev_state: List[Tuple] = self._target.get_current_state(
                query=query.LOAD_STATE, args=keys
            )
        prev_keys: List[Key] = [state_type.from_target(record=r).key for r in prev_state]

        # current state
        curr_records: List[State] = [
            state_type.from_source(record=record) for record in file
        ]
        it_event_id = self._target.get_next_event_id(n=len(curr_records))
        for r, event_id in zip(curr_records, it_event_id):
            r.event_id = event_id
            r.delivery_id = delivery_id

        curr_keys: List[Key] = [r.key for r in curr_records]
        keys_to_remove: List[Key] = list(set(prev_keys) - set(curr_keys))

        del file

        return {"records": curr_records, "keys_to_remove": keys_to_remove}

    def persist_delivery(
        self,
        delivery_id: int,
        start_time: datetime,
        delivery: Dict,
    ) -> None:
        """Persists records present in delivery.

        Args:
            delivery_id: Delivery id.
            start_time: Time delivery started.
            delivery: Delivery to process.
        """
        self._target.begin_transaction()

        for entity, content in delivery.items():
            self.persist_postgres(
                entity=entity,
                records=content["records"],
                keys_to_remove=content["keys_to_remove"]
            )

        end_time: datetime = datetime.utcnow()
        self._target.persist_delivery(
            args={
                "delivery_id": delivery_id,
                "delivery_ts": datetime.utcnow(),
                "runtime": end_time - start_time,
            }
        )

        self._target.commit_transaction()
        logger.info(f"Delivery {delivery_id}: persisted to postgres.")

    def persist_postgres(self, entity: Entity, records: List[State], keys_to_remove: Keys) -> None:
        """Persists records of entity to postgres.

        Args:
            entity: Entity which events are going to be persisted.
            records: Records to persist.
            keys_to_remove: Keys to remove.
        """
        query: BaseQueries = self._queries[entity]

        # if multiple instructions have the same primary key
        # db instructions can't be in batch
        batch_records = len(set(r.key for r in records)) == len(records)

        # PERSIST RECORDS
        if batch_records:
            self._target.execute(
                instruction=query.UPSERT,
                logs=[r.as_tuple() for r in records],
            )
        else:
            for r in records:
                self._target.execute(
                    instruction=query.UPSERT,
                    logs=[r.as_tuple()],
                )

        # REMOVE
        self._target.execute(
            instruction=query.DELETE,
            logs=[k for k in keys_to_remove],
        )


def parse_args() -> argparse.Namespace:
    """Parses user input arguments when starting loading process."""
    parser = argparse.ArgumentParser(
        prog="python ./src/paper_engine_monitor/__main__.py"
    )

    parser.add_argument(
        "--service",
        dest="run_as_service",
        action="store_true",
        required=False,
        help="Enable continuous pooling of source.",
    )
    parser.add_argument(
        "--no-service",
        dest="run_as_service",
        action="store_false",
        required=False,
        help="Disable continuous pooling of source.",
    )
    parser.set_defaults(
        run_as_service=ast.literal_eval(os.environ.get("RUN_AS_SERVICE", "True"))
    )

    parser.add_argument(
        "--source",
        dest="source",
        type=str,
        required=False,
        default=os.environ.get("SOURCE"),
        help="AWS access credentials. e.g.: "
        "bucket_endpoint=bucket_endpoint "
        "bucket_id=bucket_id bucket_key=bucket_key "
        "bucket_name=bucket_name bucket_path=bucket_path",
    )

    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        required=False,
        help="Disable data persistence (parse only).",
    )
    parser.add_argument(
        "--no-dry-run",
        dest="dry_run",
        action="store_false",
        required=False,
        help="Enable data persistence (default).",
    )
    parser.set_defaults(dry_run=ast.literal_eval(os.environ.get("DRY_RUN", "False")))

    parser.add_argument(
        "--target",
        dest="target",
        type=str,
        required=False,
        default=os.environ.get("TARGET"),
        help="Postgres connection URL. e.g.: "
        "user=username password=password host=localhost port=5432 dbname=paper_engine",
    )

    parser.add_argument(
        "--min_sleep",
        dest="min_sleep",
        default=int(os.getenv("MIN_SLEEP", default=15)),
        type=int,
        required=False,
        help="Loader minimum time to sleep between iterations.",
    )

    parser.add_argument(
        "--max_sleep",
        dest="max_sleep",
        default=int(os.getenv("MAX_SLEEP", default=30)),
        type=int,
        required=False,
        help="Loader maximum time to sleep between iterations.",
    )

    parser.add_argument(
        "--portfolio_id",
        dest="portfolio_id",
        default=int(os.getenv("PORTFOLIO_ID")),
        type=int,
        required=False,
        help="Portfolio ID of the tracked portfolio.",
    )

    parser.add_argument(
        "--initial_portfolio_value",
        dest="initial_portfolio_value",
        default=os.getenv("INITIAL_PORTFOLIO_VALUE"),
        type=str,
        required=False,
        help="Initial value of tracked Portfolio.",
    )

    parser.add_argument(
        "--api_key",
        dest="api_key",
        default=os.getenv("API_KEY"),
        type=str,
        required=False,
        help="API key.",
    )

    parser.add_argument(
        "--secret_key",
        dest="secret_key",
        default=os.getenv("SECRET_KEY"),
        type=str,
        required=False,
        help="Secret key.",
    )

    a = parser.parse_args()

    return a


if __name__ == "__main__":
    # Start up the prometheus server to expose the metrics.
    parsed_args = parse_args()

    loader = Loader()
    loader.run(parsed_args)
