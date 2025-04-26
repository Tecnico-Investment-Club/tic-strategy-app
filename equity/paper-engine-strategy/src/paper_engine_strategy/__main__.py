"""Loader execution class."""

import argparse
import ast
from datetime import datetime
import hashlib
import logging
import os
import secrets
from sys import stdout
import time
from typing import Any, Dict, List, Set, Tuple, Type

from tqdm import tqdm

from paper_engine_strategy import __version__
from paper_engine_strategy._types import File, Key, Keys

import paper_engine_strategy.model as model
from paper_engine_strategy.model.base import State
from paper_engine_strategy.model.entity import Entity
from paper_engine_strategy.persistance import source, target
import paper_engine_strategy.queries as queries
from paper_engine_strategy.queries.base import BaseQueries

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=stdout,
)
logger = logging.getLogger(__name__)


class Loader:
    """Loader main class."""

    _lookback: int

    _source: source.Source
    _target: target.Target
    _dry_run: bool
    _min_sleep: int
    _max_sleep: int
    _strat_params: Dict[str, Any]

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

    def setup(self, args: argparse.Namespace) -> None:
        """Prepares loader components.

        Args:
            args: Variables given by user when starting loader process.

        Connects to source, all possible targets and notification components.
        """
        self._dry_run = args.dry_run
        self._min_sleep = args.min_sleep
        self._max_sleep = args.max_sleep
        self._lookback = args.lookback

        # prepare persistence FROM
        self._source = source.Source(args.source)

        # prepare persistence
        self._target = target.Target(args.target)

        # TODO: ADD PO STRAT PARAMS
        self._strat_params = {
            "strategy_name": args.strategy_name.lower(),
        }

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
        logger.info(f"paper-engine-portfolio_optimization v{__version__}")

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

        logger.debug("Resolving portfolio_optimization ID...")
        strategy_hash = self.get_strat_hash()
        strategy_id = self._target.get_strategy_id(strategy_hash)
        config_records: File = []
        if not strategy_id:
            strategy_id = self._target.get_next_strategy_id()
            config_records = self.get_config_records(strategy_id, strategy_hash)

        self._strat_params["strategy_id"] = strategy_id


        # TODO: GET DATA
        logger.debug("Fetching latest data...")
        strategy_data = self.get_strategy_data(strategy_id)
        if not strategy_data:
            logger.debug("No new data to make decisions.")
            return

        # TODO FILTER DATA USING LOCAL FILES

        # TODO: RUN PO CODE
        logger.debug("Running portfolio_optimization...")
        strategy_records = self.run_strategy()

        control_records: File = [
            (
                strategy_id,
                datetime.utcnow(),
            )
        ]

        delivery_id: int = self._target.get_next_delivery_id()
        delivery: Dict = {
            Entity.STRATEGY: self.process(
                delivery_id=delivery_id, entity=Entity.STRATEGY, file=strategy_records
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

    def get_strat_hash(self) -> str:
        """Get portfolio_optimization hash from portfolio_optimization params."""
        # TODO: ADD PO STRAT PARAMS
        strategy_hash = hashlib.sha256(
            (
                self._strat_params["strategy_name"].upper()
            ).encode("utf-8")
        ).hexdigest()

        return strategy_hash

    def get_config_records(self, strategy_id: int, strategy_hash: str) -> File:
        """Get portfolio_optimization configuration records."""
        # TODO: ADD PO STRAT PARAMS
        config_records: File = [
            (
                strategy_id,
                self._strat_params["strategy_name"].upper(),
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
        help="Type of portfolio_optimization.",
    )

    parser.add_argument(
        "--strategy_name",
        dest="strategy_name",
        default=os.getenv("STRATEGY_NAME"),
        type=str,
        required=False,
        help="Strategy name.",
    )

    a = parser.parse_args()

    return a


if __name__ == "__main__":
    parsed_args = parse_args()

    loader = Loader()
    loader.run(parsed_args)
