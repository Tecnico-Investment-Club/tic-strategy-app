"""Loader execution class."""

import argparse
import ast
import json
from datetime import datetime
import hashlib
import logging
import os
import secrets
from sys import stdout
import time
from typing import Any, Dict, List, Set, Tuple, Type, Optional

import paper_engine_strategy._filters as filters
from paper_engine_strategy._types import File, Key, Keys
import paper_engine_strategy._date_helpers as date_helpers
import paper_engine_strategy._helpers as helpers
import paper_engine_strategy.model as model
from paper_engine_strategy.model.base import State
from paper_engine_strategy.model.source_model.spot_prices import SpotPrices
from paper_engine_strategy.model.entity import Entity
from paper_engine_strategy.persistance import source, target
import paper_engine_strategy.queries as queries
from paper_engine_strategy.queries.base import BaseQueries
from paper_engine_strategy.queries.source_queries import SpotPricesQueries

import paper_engine_strategy.strategy as strat
from paper_engine_strategy.strategy.base import BaseStrategy

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=stdout,
)
logger = logging.getLogger(__name__)


class Loader:
    """Loader main class."""

    _strategy_type: str
    _asset_type: str
    _interval: str
    _lookback: int
    _strategy_config: Dict[str, Any]

    # ---
    _strategy_id: Optional[int]
    _strategy: BaseStrategy

    _source: source.Source
    _target: target.Target
    _dry_run: bool
    _min_sleep: int
    _max_sleep: int

    _new_strategy: bool

    _entities: Set[Entity] = {
        Entity.STRATEGY,
        Entity.STRATEGY_CONFIG,
        Entity.STRATEGY_CONTROL,
        Entity.STRATEGY_LATEST,
    }
    _queries: Dict[Entity, BaseQueries] = {
        Entity.STRATEGY: queries.StrategyQueries(),
        Entity.STRATEGY_CONFIG: queries.StrategyConfigQueries(),
        Entity.STRATEGY_CONTROL: queries.StrategyControlQueries(),
        Entity.STRATEGY_LATEST: queries.StrategyLatestQueries(),
    }
    _state: Dict[Entity, Type[State]] = {
        Entity.STRATEGY: model.Strategy,
        Entity.STRATEGY_CONFIG: model.StrategyConfig,
        Entity.STRATEGY_CONTROL: model.StrategyControl,
        Entity.STRATEGY_LATEST: model.StrategyLatest,
    }
    _strategies: Dict[str, BaseStrategy] = {
        "PO_HURST_EXPONENT": strat.POHurstExpStrategy,
    }
    _schemas: Dict[str, str] = {
        "STOCK": "alpaca",
        "CRYPTO": "binance"
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

        self._strategy_type = args.strategy_type
        self._strategy_config = json.loads(args.strategy_config)
        self._strategy = self._strategies[self._strategy_type]

        self._asset_type = args.asset_type
        self._interval = args.interval
        self._lookback = args.lookback

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
        logger.info(f"paper-engine-strategy")

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

        logger.debug("Resolving Strategy ID...")
        self._new_strategy = False
        strategy_hash = self.get_strat_hash()
        self._strategy_id = self._target.get_strategy_id(strategy_hash)
        config_records: File = []
        if not self._strategy_id:
            self._new_strategy = True
            self._strategy_id = self._target.get_next_strategy_id()
            config_records = self.get_config_records(strategy_hash)

        logger.debug("Fetching latest data...")
        latest_datadate = self.check_new_data()
        if not self._new_strategy and not latest_datadate:
            logger.info("No new data to make decisions.")
            return

        strategy_data = self.get_strategy_data(latest_datadate)
        strategy_data = self.filter_data(strategy_data)
        prev_wgts = self.get_prev_wgts()

        logger.debug("Running Strategy...")
        strategy_records = self.run_strategy(strategy_data, prev_wgts)

        control_records: File = [(self._strategy_id, datetime.utcnow())]

        delivery_id: int = self._target.get_next_delivery_id()
        delivery: Dict = {
            Entity.STRATEGY: self.process(
                delivery_id=delivery_id,
                entity=Entity.STRATEGY,
                file=strategy_records
            ),
            Entity.STRATEGY_LATEST: self.process(
                delivery_id=delivery_id,
                entity=Entity.STRATEGY_LATEST,
                file=strategy_records,
            ),
            Entity.STRATEGY_CONFIG: self.process(
                delivery_id=delivery_id,
                entity=Entity.STRATEGY_CONFIG,
                file=config_records,
            ),
            Entity.STRATEGY_CONTROL: self.process(
                delivery_id=delivery_id,
                entity=Entity.STRATEGY_CONTROL,
                file=control_records,
            ),
        }

        # persist delivery
        if not self._dry_run:
            self.persist_delivery(
                delivery_id=delivery_id,
                start_time=start_time,
                delivery=delivery,
            )

        del delivery, strategy_data

        end_time = datetime.utcnow()
        logger.info(
            f"Delivery {delivery_id}: processed ({end_time - start_time} seconds)."
        )

    def check_new_data(self):
        schema = self._schemas[self._asset_type]
        query = SpotPricesQueries.LOAD_LATEST.format(schema=schema, interval=self._interval)
        latest_datadate = self._source.get_file(query)
        if not latest_datadate:
            return None
        latest_datadate = latest_datadate[0][0]

        latest_control = self._target.get_current_state(
                query=queries.StrategyControlQueries.LOAD_STATE,
                args=[(self._strategy_id,)],
            )
        if not latest_control:
            return latest_datadate

        latest_control_ts = latest_control[0][1]
        if latest_control_ts < latest_datadate:
            return latest_datadate
        else:
            return None

    def get_strategy_data(self, latest_date) -> List[SpotPrices]:
        if self._asset_type == "CRYPTO":
            start_date = date_helpers.go_days_back(latest_date, n=self._lookback)
        else:
            start_date = date_helpers.go_business_days_back(latest_date, n=self._lookback)
        schema = self._schemas[self._asset_type]
        query = SpotPricesQueries.LOAD_RECORDS.format(schema=schema, interval=self._interval)

        raw_records = self._source.get_file(query, variable=(start_date,))
        records = [SpotPrices.from_source(r) for r in raw_records]
        return records

    def get_prev_wgts(self):
        e = Entity.STRATEGY_LATEST
        query: BaseQueries = self._queries[e]
        all_prev_wgts: List[Tuple] = self._target.get_current_state(
            query=query.LOAD_FULL_STATE,
        )
        prev_wgts = [self._state[e].from_target(r) for r in all_prev_wgts if r[0] == self._strategy_id]
        return prev_wgts

    def filter_data(self, strategy_data: List[SpotPrices]):
        if self._asset_type == "STOCK":
            res = [s for s in strategy_data if s.symbol in filters.STOCK]
        elif self._asset_type == "CRYPTO":
            res = [s for s in strategy_data if s.symbol[-4:] == 'USDT']
            for s in res:
                s.symbol = helpers.binance_2_alpaca_symbol(s.symbol)
            res = [s for s in strategy_data if s.symbol in filters.CRYPTO]
        else:
            return None
        return res

    def run_strategy(self, data, prev_wgts=None) -> File:
        strategy = self._strategy.setup(self._strategy_config)
        raw_strategy_records = strategy.get_weights(data, prev_wgts)
        strategy_records = [
            [
                self._strategy_id,
                f'{self._asset_type}_TICKER',  # TICKER HARDCODED HERE
                s[0],  # asset_id
                s[1],  # datadate
                datetime.utcnow(),
                s[2],  # weight
                1,     # decision -> LONG ONLY HARDCODED HERE
            ]
            for s in raw_strategy_records
        ]
        return strategy_records

    def get_strat_hash(self) -> str:
        """Get portfolio_optimization hash from portfolio_optimization params."""
        ordered_keys = sorted(self._strategy_config.keys())
        strat_string = (
            f"{self._strategy_type}"
            f"{self._asset_type}"
            f"{self._interval}"
            f"{self._lookback}"
        )
        for k in ordered_keys:
            strat_string += f"{self._strategy_config[k]}"
        strategy_hash = hashlib.sha256(strat_string.encode("utf-8")).hexdigest()

        return strategy_hash

    def get_config_records(self, strategy_hash: str) -> File:
        """Get portfolio_optimization configuration records."""
        config_records: File = [
            (
                self._strategy_id,
                self._strategy_type,
                self._asset_type,
                self._interval,
                self._lookback,
                self._strategy_config,
                strategy_hash,
            )
        ]
        return config_records

    def process(self, delivery_id: int, entity: Entity, file: File) -> Dict:
        """Processes entity records present in delivery.

        Args:
            delivery_id: Delivery id.
            entity: Entity type.
            file: Entity records.

        Returns:
            A dictionary with records and keys to remove.
        """
        logger.info(f"Delivery {delivery_id}: processing {entity}...")

        query: BaseQueries = self._queries[entity]
        state_type: Type[State] = self._state[entity]

        # previous state
        keys: Keys = state_type.list_ids_from_source(records=file)
        # fetch records from state with keys
        if entity == Entity.STRATEGY_LATEST:
            prev_state: List[Tuple] = self._target.get_current_state(
                query=query.LOAD_FULL_STATE
            )
        else:
            prev_state: List[Tuple] = self._target.get_current_state(
                query=query.LOAD_STATE, args=keys
            )
        prev_keys: List[Key] = [
            state_type.from_target(record=r).key for r in prev_state
        ]

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
                keys_to_remove=content["keys_to_remove"],
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

    def persist_postgres(
        self, entity: Entity, records: List[State], keys_to_remove: Keys
    ) -> None:
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
        prog="python ./src/paper_engine_strategy/__main__.py"
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
        "--lookback",
        dest="lookback",
        default=os.getenv("LOOKBACK", default=180),
        type=int,
        required=False,
        help="Lookback of the strategy.",
    )

    parser.add_argument(
        "--strategy_type",
        dest="strategy_type",
        default=os.getenv("STRATEGY_TYPE"),
        type=str,
        required=False,
        help="Type of Strategy.",
    )

    parser.add_argument(
        "--asset_type",
        dest="asset_type",
        default=os.getenv("ASSET_TYPE", "CRYPTO"),
        type=str,
        required=False,
        help="Strategy asset type.",
    )

    parser.add_argument(
        "--interval",
        dest="interval",
        default=os.getenv("INTERVAL", "1d"),
        type=str,
        required=False,
        help="Data interval.",
    )

    parser.add_argument(
        "--strategy_config",
        dest="strategy_config",
        default=os.getenv("STRATEGY_CONFIG", '{}'),
        type=str,
        required=False,
        help="Strategy configuration JSON.",
    )


    a = parser.parse_args()

    return a


if __name__ == "__main__":
    parsed_args = parse_args()

    loader = Loader()
    loader.run(parsed_args)
