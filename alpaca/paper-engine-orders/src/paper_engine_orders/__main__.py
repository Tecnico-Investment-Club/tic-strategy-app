"""Loader execution class."""

import argparse
import ast
from datetime import datetime
from datetime import time as dt_time
from decimal import Decimal
import hashlib
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

import pytz


from paper_engine_orders._types import File, Key, Keys, Record
import paper_engine_orders.broker as broker
import paper_engine_orders.model as model
from paper_engine_orders.model.base import State
from paper_engine_orders.model.entity import Entity
import paper_engine_orders.model.source_model as source_model
from paper_engine_orders.persistance import source, target
import paper_engine_orders.queries as queries
from paper_engine_orders.queries.base import BaseQueries
import paper_engine_orders.queries.source_queries as source_queries
from paper_engine_orders.queries.source_queries import StrategyQueries
from paper_engine_orders.weighting import Weighting


logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=stdout,
)
logger = logging.getLogger(__name__)


us_market_tz = pytz.timezone("America/New_York")
US_MARKET_OPEN = dt_time(hour=9, minute=30, tzinfo=us_market_tz)
US_MARKET_CLOSE = dt_time(hour=16, minute=0, tzinfo=us_market_tz)


class Loader:
    """Loader main class."""

    _account_id: str
    _source: source.Source
    _target: target.Target
    _broker: broker.Alpaca
    _dry_run: bool
    _dry_orders: bool
    _min_sleep: int
    _max_sleep: int
    _portfolio_name: str
    _cash_allocation: Decimal
    _strategy_id: int
    _crypto: bool

    _entities: Set[Entity] = {
        Entity.ORDERS,
        Entity.ORDERS_LATEST,
        Entity.ORDERS_CONFIG,
        Entity.ORDERS_CONTROL,
    }
    _queries: Dict[Entity, BaseQueries] = {
        Entity.ORDERS: queries.OrdersQueries(),
        Entity.ORDERS_LATEST: queries.OrdersLatestQueries(),
        Entity.ORDERS_CONFIG: queries.OrdersConfigQueries(),
        Entity.ORDERS_CONTROL: queries.OrdersControlQueries(),
    }
    _state: Dict[Entity, Type[State]] = {
        Entity.ORDERS: model.Orders,
        Entity.ORDERS_LATEST: model.OrdersLatest,
        Entity.ORDERS_CONFIG: model.OrdersConfig,
        Entity.ORDERS_CONTROL: model.OrdersControl,
    }

    def setup(self, args: argparse.Namespace) -> None:
        """Prepares loader components.

        Args:
            args: Variables given by user when starting loader process.

        Connects to source, all possible targets and notification components.
        """
        self._dry_run = args.dry_run
        self._dry_orders = args.dry_orders
        self._min_sleep = args.min_sleep
        self._max_sleep = args.max_sleep

        # prepare persistence FROM
        self._source = source.Source(args.source)

        # prepare persistence
        self._target = target.Target(args.target)

        # ALPACA CONNECTION
        self._broker = broker.Alpaca(args.api_key, args.secret_key)
        self._account_id = hashlib.sha256(
            (args.api_key + args.secret_key).encode("utf-8")
        ).hexdigest()

        self._portfolio_name = args.portfolio_name
        self._strategy_id = args.strategy_id
        self._cash_allocation = Decimal(args.cash_allocation)

        self._crypto = True
        self._broker.crypto = self._crypto
        
        logger.info(
            f"Setup complete. Portfolio: {self._portfolio_name}, Strategy ID: {self._strategy_id}, "
            f"Cash Alloc: {self._cash_allocation}, Dry Run: {self._dry_run}, Dry Orders: {self._dry_orders}"
        )

        sql_directory = "/project/db"

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

        stop = False
        while not stop:
            try:
                self.run_once()
                t = secrets.choice(
                    [self._min_sleep, self._max_sleep]
                    + [i for i in range(self._min_sleep, self._max_sleep)]
                )
                time.sleep(t)
            except Exception as e:
                logger.warning("error while importing:", e)
                stop = True

        logger.info("Terminating...")

    def run_once(self) -> None:
        """Runs the synchronization process once."""
        start_time: datetime = datetime.utcnow()
        end_time: datetime

        decision_metadata = self.check_new_decisions()
        if not decision_metadata:
            logger.debug("No new decision for any strategy.")
            return

        portfolio_id = decision_metadata[0]
        strategy_id = decision_metadata[1]
        latest_decision_delivery_id = decision_metadata[2]
        latest_decision_datadate = decision_metadata[3]
        
        logger.info(f"Processing new decision. Strategy: {strategy_id}, Delivery: {latest_decision_delivery_id}")

        strategy_records = self.get_latest_decision(strategy_id)

        # ASSET ID TYPE STARTS WITH STOCK
        if strategy_records[0].asset_id_type[:5] == "STOCK":
            self._crypto = False
            self._broker.crypto = self._crypto
            # CHECK IF MARKET IS OPEN
            curr_time = datetime.now(us_market_tz).time()
            market_open = self.check_market_open(US_MARKET_OPEN, US_MARKET_CLOSE, curr_time)
            if not market_open:
                logger.debug("US Stock Market is not open.")
                return

        asset_ids = [s.asset_id for s in strategy_records]
        tradable_asset_ids = self._broker.check_tradable(asset_ids)
        
        # Check if any asset_ids from the strategy are not tradable
        if len(asset_ids) != len(tradable_asset_ids):
            skipped_assets = set(asset_ids) - set(tradable_asset_ids)
            logger.info(
                f"Tradable Check: Total assets in strategy = {len(asset_ids)}. "
                f"Tradable assets = {len(tradable_asset_ids)}. "
                f"Skipped (non-tradable) assets: {skipped_assets if skipped_assets else 'None'}"
            )
            if skipped_assets:
                logger.debug(f"Detailed list of non-tradable assets skipped: {sorted(list(skipped_assets))}")
        strategy_records = [
            s for s in strategy_records if s.asset_id in tradable_asset_ids
        ]

        current_positions = self._broker.get_positions()

        # CLOSE POSITIONS
        closed_symbols = []
        if current_positions:
            current_symbols = list(current_positions.keys())
            strategy_symbols = [s.asset_id for s in strategy_records]
            closed_symbols = [s for s in current_symbols if s not in strategy_symbols]
            
        closed_records = []
        if closed_symbols:
            logger.info(f"Closing {len(closed_symbols)} positions: {closed_symbols}")
            if not self._dry_orders:
                closed_records = self._broker.close_positions(portfolio_id, closed_symbols)

        open_positions = [s for s in strategy_records if s.asset_id not in closed_symbols]

        long_portfolio = [p for p in open_positions if p.decision == 1]
        long_asset_ids = [p.asset_id for p in long_portfolio]

        short_portfolio = [
            p
            for p in open_positions
            if p.decision == -1 and p.asset_id not in long_asset_ids
        ]
        short_asset_ids = list({p.asset_id for p in short_portfolio})
        shortable_ids = self._broker.check_shortable(short_asset_ids)
        short_portfolio = [
            p for p in short_portfolio if p.asset_id in shortable_ids
        ]
        # MAKE TICKERS UNIQUE
        seen_asset_ids = []
        unique_short_portfolio = []
        for p in short_portfolio:
            if p.asset_id not in seen_asset_ids:
                seen_asset_ids.append(p.asset_id)
                unique_short_portfolio.append(p)

        account_capital = self._broker.get_account_capital()
        logger.info(f"Account Capital: {account_capital}")

        ############################################
        ### FOR NOW LONG ONLY IS HARD CODED HERE ###
        ############################################
        long_capital = account_capital
        if self._crypto:
            # CASH CUSHION FOR CRYPTO SLIPPAGE
            long_capital = (1 - self._cash_allocation) * long_capital
        short_capital = Decimal("0")
        ############################################

        long_weighting = Weighting.setup(
            self._broker, long_capital, long_portfolio, current_positions
        ) if long_portfolio and long_capital > 0 else None
        short_weighting = Weighting.setup(
            self._broker, short_capital, unique_short_portfolio, current_positions
        ) if unique_short_portfolio and short_capital > 0 else None

        long_orders = long_weighting.get_orders_params() if long_weighting else {"buy": [], "sell": []}
        short_orders = short_weighting.get_orders_params() if short_weighting else {"buy": [], "sell": []}

        closing_orders = long_orders["sell"] + short_orders["buy"]
        opening_orders = long_orders["buy"] + short_orders["sell"]
        
        total_orders = len(closing_orders) + len(opening_orders)
        if total_orders > 0:
            logger.info(f"Orders generated: {total_orders} (Closing: {len(closing_orders)}, Opening: {len(opening_orders)})")
            for o in closing_orders:
                logger.info(f"  [CLOSE] {o['side'].upper()} {o['symbol']} Qty: {o['quantity']}")
            for o in opening_orders:
                logger.info(f"  [OPEN]  {o['side'].upper()} {o['symbol']} Qty: {o['quantity']}")
        else:
             logger.info("No new orders generated.")

        if not self._dry_orders:
            self._broker.submit_orders(closing_orders)
            self._broker.submit_orders(opening_orders)

        long_records = long_weighting.get_orders_records(portfolio_id) if long_weighting else []
        short_records = short_weighting.get_orders_records(portfolio_id) if short_weighting else []

        orders_records = long_records + short_records + closed_records
        control_records = [(
                portfolio_id,
                latest_decision_delivery_id,
                latest_decision_datadate,
                datetime.utcnow(),
            )]
        config_records = [self.get_config_record(portfolio_id, strategy_id)]

        delivery_id: int = self._target.get_next_delivery_id()
        delivery: Dict = {
            Entity.ORDERS_CONTROL: self.process(
                delivery_id=delivery_id,
                entity=Entity.ORDERS_CONTROL,
                file=control_records,
            ),
            Entity.ORDERS_CONFIG: self.process(
                delivery_id=delivery_id,
                entity=Entity.ORDERS_CONFIG,
                file=config_records,
            ),
        }
        if orders_records:
            logger.info('New orders placed.')
            delivery[Entity.ORDERS] = self.process(
                delivery_id=delivery_id,
                entity=Entity.ORDERS,
                file=orders_records,
            )
            delivery[Entity.ORDERS_LATEST] = self.process(
                delivery_id=delivery_id,
                entity=Entity.ORDERS_LATEST,
                file=orders_records,
            )

        # persist delivery
        if not self._dry_run:
            logger.info(f"Persisting delivery {delivery_id} with {len(orders_records)} orders.")
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

    @staticmethod
    def check_market_open(open_time: dt_time, close_time: dt_time, t: dt_time) -> bool:
        """Check if market is open."""
        if open_time <= close_time:
            return open_time <= t <= close_time
        else:
            return open_time <= t or t <= close_time

    def get_portfolio_id(self, strategy_id: int) -> int:
        """Get portfolio id."""
        portfolio_id = self._target.get_portfolio_id(self._portfolio_name)
        if not portfolio_id:
            portfolio_id = self._target.get_next_portfolio_id()
        return portfolio_id

    def check_new_decisions(self) -> Optional[Record]:
        """Check latest strategy state."""
        strat_metadata = self._source.fetch_one(
            query=StrategyQueries.LOAD_LATEST_DELIVERY_METADATA,
            variable=(self._strategy_id,)
        )
        if not strat_metadata:
            return None

        strategy_id = strat_metadata[0]
        latest_decision_delivery_id = strat_metadata[1]
        latest_decision_datadate = strat_metadata[2]

        portfolio_id = self.get_portfolio_id(strategy_id)

        last_decision_metadata = self._target.get_current_state(
            query=queries.OrdersControlQueries.LOAD_STATE,
            args=[(portfolio_id,)],
        )

        r = (
            portfolio_id,
            strategy_id,
            latest_decision_delivery_id,
            latest_decision_datadate,
        )
        if not last_decision_metadata:
            return r

        last_decision_delivery_id = last_decision_metadata[0][1]

        if last_decision_delivery_id < latest_decision_delivery_id:
            return r

        return None

    def get_latest_decision(self, strategy_id: int) -> List[source_model.Strategy]:
        """Get latest state of strategy."""
        raw_records = self._source.get_file(
            query=source_queries.StrategyQueries.LOAD_LATEST,
            variable=(strategy_id,),
        )
        strategy_records = [source_model.Strategy.from_source(r) for r in raw_records]

        return strategy_records

    def get_config_record(
        self, portfolio_id: int, strategy_id: int
    ) -> Record:
        """Get portfolio configuration records."""
        config_record: Record = (
            portfolio_id,
            strategy_id,
            self._portfolio_name,
            self._account_id,
        )

        return config_record

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
        if entity == Entity.ORDERS_LATEST:
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
            records_count = len(content["records"])
            remove_count = len(content["keys_to_remove"])
            if records_count > 0 or remove_count > 0:
                 logger.info(f"  -> {entity}: Upserting {records_count}, Deleting {remove_count}")

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
        prog="python ./src/paper_engine_orders/__main__.py"
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
        "--dry-orders",
        dest="dry_orders",
        action="store_true",
        required=False,
        help="Disable order placement.",
    )
    parser.add_argument(
        "--no-dry-orders",
        dest="dry_orders",
        action="store_false",
        required=False,
        help="Enable order placement.",
    )
    parser.set_defaults(dry_orders=ast.literal_eval(os.environ.get("DRY_ORDERS", "True")))

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
        "--portfolio_name",
        dest="portfolio_name",
        default=os.getenv("PORTFOLIO_NAME"),
        type=str,
        required=False,
        help="Type of portfolio (dollar neutral, etc.).",
    )

    parser.add_argument(
        "--cash_allocation",
        dest="cash_allocation",
        default=os.getenv("CASH_ALLOCATION"),
        type=str,
        required=False,
        help="Percentage of cash allocation.",
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

    parser.add_argument(
        "--strategy_id",
        dest="strategy_id",
        default=os.getenv("STRATEGY_ID"),
        type=int,
        required=False,
        help="Strategy ID.",
    )

    a = parser.parse_args()

    return a


if __name__ == "__main__":
    parsed_args = parse_args()

    loader = Loader()
    loader.run(parsed_args)
