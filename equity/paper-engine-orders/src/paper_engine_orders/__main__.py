"""Loader execution class."""

import argparse
import ast
from datetime import datetime
from datetime import time as dt_time
from decimal import Decimal
import hashlib
import json
import logging
import os
import secrets
from sys import stdout
import time
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union
)

import pytz


from paper_engine_orders._types import File, Key, Keys, Record
import paper_engine_orders.broker as broker
import paper_engine_orders.model as model
from paper_engine_orders.model.base import EventLog, State
from paper_engine_orders.model.entity import Entity
from paper_engine_orders.model.event_type import EventType
import paper_engine_orders.model.source_model as source_model
from paper_engine_orders.persistance import source, target
import paper_engine_orders.queries as queries
from paper_engine_orders.queries.base import BaseQueries
import paper_engine_orders.queries.source_queries as source_queries
from paper_engine_orders.queries.source_queries import StrategyQueries
import paper_engine_orders.weighting as weighting
from paper_engine_orders.weighting.base import BaseWeight

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=stdout,
)
logger = logging.getLogger(__name__)

Events = Dict[EventType, List[EventLog]]
Delivery = Dict[Entity, Events]
Summary = Dict[Entity, Dict[EventType, int]]

# TODO: IMPLEMENT DYNAMIC CHANGE FROM ET TO EST TIME (?)
US_MARKET_OPEN = dt_time(hour=14, minute=30, second=0, tzinfo=pytz.utc)
US_MARKET_CLOSE = dt_time(hour=21, minute=0, second=0, tzinfo=pytz.utc)


class Loader:
    """Loader main class."""

    _account_id: str
    _source: source.Source
    _target: target.Target
    _broker: broker.Alpaca
    _notifications: bool
    _dry_run: bool
    _min_sleep: int
    _max_sleep: int
    _portfolio_params: Dict[str, Any]
    _portfolio_hash: str

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
    _event_log: Dict[Entity, Type[EventLog]] = {
        Entity.ORDERS: model.OrdersLog,
        Entity.ORDERS_LATEST: model.OrdersLatestLog,
        Entity.ORDERS_CONFIG: model.OrdersConfigLog,
        Entity.ORDERS_CONTROL: model.OrdersControlLog,
    }
    _weights: Dict[str, Type[BaseWeight]] = {"equal": weighting.EqualWeight}

    def setup(self, args: argparse.Namespace) -> None:
        """Prepares loader components.

        Args:
            args: Variables given by user when starting loader process.

        Connects to source, all possible targets and notification components.
        """
        self._notifications = args.notifications
        self._dry_run = args.dry_run
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


        self._portfolio_params = {
            "portfolio_type": args.portfolio_type,
            "rebal_freq": args.rebal_freq,
            "adjust": args.adjust,
            "wgt_method": args.wgt_method,
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

        # CHECK IF MARKET IS OPEN
        curr_time = datetime.utcnow().time().replace(tzinfo=pytz.utc)
        market_open = self.check_market_open(US_MARKET_OPEN, US_MARKET_CLOSE, curr_time)
        if not market_open:
            logger.info("Market is not open.")
            return

        new_decisions_metadata = self.check_new_decisions()
        if not new_decisions_metadata:
            logger.info("No new decision for the strategy.")
            return

        delivery_ids_read = list(set([e[2] for e in new_decisions_metadata]))
        if len(delivery_ids_read) > 1:
            logger.warning("More than 1 delivery read???")
            return
        latest_decision_delivery_id = delivery_ids_read[0]

        orders_records: File = []
        control_records: File = []
        config_records: File = []
        for decision_metadata in new_decisions_metadata:
            portfolio_id = decision_metadata[0]
            strategy_id = decision_metadata[1]
            latest_decision_datadate = decision_metadata[3]

            portfolio_hash = self.get_portfolio_hash(strategy_id)

            strategy_records = self.get_latest_decision(
                strategy_id, latest_decision_delivery_id
            )

            asset_ids = [s.asset_id for s in strategy_records]
            tradable_asset_ids = self._broker.check_tradable(asset_ids)
            strategy_records = [
                s for s in strategy_records if s.asset_id in tradable_asset_ids
            ]

            current_positions = self._broker.get_positions()
            closed_positions = []
            if current_positions:
                closed_positions = [s for s in strategy_records if s.event_type == "REMOVE"]
            open_positions = [s for s in strategy_records if s.event_type != "REMOVE"]

            # CLOSE POSITIONS
            closed_records = []
            if closed_positions:
                closed_tickers = [p.asset_id for p in closed_positions]
                closed_records = self._broker.close_positions(portfolio_id, closed_tickers)

            long_porfolio = [p for p in open_positions if p.decision == 1]
            long_asset_ids = [p.asset_id for p in long_porfolio]

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
            # TODO: FOR NOW LONG ONLY IS HARD CODED HERE
            long_capital = account_capital
            short_capital = Decimal("0")

            wgt_method = self._portfolio_params["wgt_method"].lower()
            long_weighting = self._weights[wgt_method].setup(
                self._broker, long_capital, long_porfolio, current_positions
            ) if long_porfolio else None
            short_weighting = self._weights[wgt_method].setup(
                self._broker, short_capital, unique_short_portfolio, current_positions
            ) if unique_short_portfolio else None

            long_orders = long_weighting.get_orders_params() if long_weighting else {"buy": [], "sell": []}
            short_orders = short_weighting.get_orders_params() if short_weighting else {"buy": [], "sell": []}

            closing_orders = long_orders["sell"] + short_orders["buy"]
            opening_orders = long_orders["buy"] + short_orders["sell"]

            self._broker.submit_orders(closing_orders)
            self._broker.submit_orders(opening_orders)

            long_records = long_weighting.get_orders_records(portfolio_id) if long_weighting else []
            short_records = short_weighting.get_orders_records(portfolio_id) if short_weighting else []
            orders_records.extend(long_records + short_records + closed_records)

            control_records.append(
                (
                    portfolio_id,
                    latest_decision_delivery_id,
                    latest_decision_datadate,
                    datetime.utcnow(),
                )
            )
            config_records.append(
                self.get_config_record(portfolio_id, strategy_id, portfolio_hash)
            )

        delivery_id: int = self._target.get_next_delivery_id()
        delivery: Delivery = {
            Entity.ORDERS: self.process(
                delivery_id=delivery_id, entity=Entity.ORDERS, file=orders_records
            ),
            Entity.ORDERS_LATEST: self.process(
                delivery_id=delivery_id,
                entity=Entity.ORDERS_LATEST,
                file=orders_records,
            ),
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

        # persist delivery
        if not self._dry_run:
            self.persist_delivery(
                delivery_id=delivery_id,
                last_read_delivery=latest_decision_delivery_id,
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
        portfolio_hash = self.get_portfolio_hash(strategy_id)
        portfolio_id = self._target.get_portfolio_id(portfolio_hash)
        if not portfolio_id:
            portfolio_id = self._target.get_next_portfolio_id()
        return portfolio_id

    def check_new_decisions(self) -> Optional[File]:
        """Check latest strategy state."""
        latest_decision_metadata = self._source.get_file(
            query=StrategyQueries.LOAD_LATEST_DELIVERY_METADATA
        )
        if not latest_decision_metadata:
            return None

        res: File = []
        for strat_metadata in latest_decision_metadata:
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
                res.append(r)
                continue

            last_decision_delivery_id = last_decision_metadata[0][1]

            if (
                self._portfolio_params["adjust"]
                or last_decision_delivery_id < latest_decision_delivery_id
            ):
                res.append(r)

        return res

    def get_latest_decision(
        self, strategy_id: int, delivery_id: int
    ) -> List[source_model.Strategy]:
        """Get latest state of strategy."""
        raw_records = self._source.get_file(
            query=source_queries.StrategyQueries.LOAD_LATEST_EVENTS,
            variable=(strategy_id, delivery_id),
        )
        strategy_records = [source_model.Strategy.from_source(r) for r in raw_records]

        return strategy_records

    def get_portfolio_hash(self, strategy_id: int) -> str:
        """Get portfolio hash from portfolio params."""
        portfolio_hash = hashlib.sha256(
            (
                str(strategy_id)
                + self._portfolio_params["portfolio_type"].upper()
                + self._portfolio_params["rebal_freq"].upper()
                + self._portfolio_params["adjust"]
                + self._portfolio_params["wgt_method"].upper()
            ).encode("utf-8")
        ).hexdigest()

        return portfolio_hash

    def get_config_record(
        self, portfolio_id: int, strategy_id: int, portfolio_hash: str
    ) -> Record:
        """Get portfolio configuration records."""
        config_record: Record = (
            portfolio_id,
            strategy_id,
            self._portfolio_params["portfolio_type"].upper(),
            self._portfolio_params["rebal_freq"].upper(),
            self._portfolio_params["adjust"],
            self._portfolio_params["wgt_method"].upper(),
            portfolio_hash,
            self._account_id,
        )

        return config_record

    def process(self, delivery_id: int, entity: Entity, file: File) -> Events:
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
        prev_state: List[Tuple] = self._target.get_current_state(
            query=query.LOAD_STATE, args=keys
        )
        prev_records: Dict[Key, State] = {}
        for record in prev_state:
            state = state_type.from_target(record=record)
            prev_records[state.key] = state

        # current state
        curr_records: List[State] = [
            state_type.from_source(record=record) for record in file
        ]

        del file

        # compute events
        events: Events = self.compute_events(
            delivery_id=delivery_id,
            entity=entity,
            curr=curr_records,
            prev=prev_records,
        )

        return events

    def compute_events(
        self,
        delivery_id: int,
        entity: Entity,
        curr: List[State],
        prev: Dict[Key, State],
    ) -> Events:
        """Computes change events between current state and delivery file records.

        Args:
            delivery_id: Delivery id.
            entity: Entity.
            curr: Entity records in delivery.
            prev: Entity records in system.

        Returns:
            A dictionary with EventType (CREATE, AMEND, REMOVE) as key and a list
            containing the event logs for each event type as value.
        """
        state_type: Type[State] = self._state[entity]
        event_log_type: Type[EventLog] = self._event_log[entity]

        # events
        create: List[EventLog] = []
        amend: List[EventLog] = []
        remove: List[EventLog] = []

        # in memory delivery variables
        # needed to compute events faster
        events: Dict[Key, State] = prev

        it_event_id = self._target.get_next_event_id(n=len(curr))

        for i, item in enumerate(curr):
            if i % 100_000 == 0:
                logger.info(
                    f"Delivery {delivery_id}: {entity} processed {i}/{len(curr)}..."
                )

            # in case multiple actions for same primary key exist in same delivery
            if item.key in events.keys():
                # get latest update to compare state
                prev_item = self.find(needle=item.key, haystack=events)

                # if last update is equal to current version to update just jump
                if prev_item is not None and item.hash == prev_item.hash:
                    continue

                # assign delivery_id to current state
                item.delivery_id = delivery_id
                # assign event_id to current_state
                item.event_id = next(it_event_id)

                event_log = event_log_type.from_states(
                    event_type=EventType.AMEND, curr=item, prev=prev_item
                )
                amend.append(event_log)
                events[item.key] = item
                continue

            if item.key not in events.keys():
                # assign delivery_id to current state
                item.delivery_id = delivery_id
                # assign event_id to current_state
                item.event_id = next(it_event_id)

                event_log = event_log_type.from_states(
                    event_type=EventType.CREATE, curr=item, prev=None
                )
                create.append(event_log)
                events[item.key] = item
                continue

        prev_keys: Set[Key] = set(events.keys()) - set(item.key for item in curr)
        it_event_id = self._target.get_next_event_id(n=len(prev_keys))

        for prev_key in prev_keys:
            # if prev_item.key not in curr_keys:
            item = state_type.removal_instance(
                event_id=next(it_event_id),
                delivery_id=delivery_id,
                key=prev_key,
            )
            prev_item = self.find(needle=prev_key, haystack=events)
            event_log = event_log_type.from_states(
                event_type=EventType.REMOVE, curr=item, prev=prev_item
            )
            remove.append(event_log)

        del it_event_id

        return {
            EventType.CREATE: create,
            EventType.AMEND: amend,
            EventType.REMOVE: remove,
        }

    @staticmethod
    def find(needle: Key, haystack: Dict[Key, State]) -> State:
        """Given a needle searches the haystack and returns the match.

        Args:
            needle: key of entity to look for.
            haystack: list of objects where to look for.

        Returns:
            Matching object if one exists.
        """
        return haystack[needle]

    @staticmethod
    def summarizer(delivery: Delivery) -> Summary:
        """Makes statistical summary of events per entity within a delivery.

        Args:
            delivery: A dictionary with Entity as key and another dictionary
                with Events as value.
                Each Event dictionary is composed by EventType (CREATE, AMEND,
                REMOVE) as key and a list containing the event logs for each
                event type as value.

        Returns:
            A summary, with the same structure as the input but instead of
            having a list structure with events, is the length of the list.
        """
        summary = {}
        for entity, events in delivery.items():
            event_cnt = {}
            for event_type, event_logs in events.items():
                if event_logs:
                    event_cnt[event_type] = len(event_logs)

            if event_cnt:
                summary[entity] = event_cnt

        return summary

    def persist_delivery(
        self,
        delivery_id: int,
        last_read_delivery: int,
        start_time: datetime,
        delivery: Delivery,
    ) -> None:
        """Persists records present in delivery.

        Args:
            delivery_id: Delivery id.
            last_read_delivery: Last read delivery ID.
            start_time: Time delivery started.
            delivery: Delivery to process.
        """
        self._target.begin_transaction()

        for entity, events in delivery.items():
            self.persist_postgres(entity=entity, events=events)

            logger.info(
                f"Delivery {delivery_id}: {entity} ("
                f"create: {len(events[EventType.CREATE])}, "
                f"amend: {len(events[EventType.AMEND])}, "
                f"remove: {len(events[EventType.REMOVE])})."
            )

        summary = self.summarizer(delivery=delivery)
        end_time: datetime = datetime.utcnow()
        self._target.persist_delivery(
            args={
                "delivery_id": delivery_id,
                "last_read_delivery": last_read_delivery,
                "row_creation": datetime.utcnow(),
                "summary": json.dumps(summary),
                "runtime": end_time - start_time,
            }
        )

        self._target.commit_transaction()
        logger.info(f"Delivery {delivery_id}: persisted to postgres.")

        logger.info(f"Delivery {delivery_id}: persisted {summary}.")

    def persist_postgres(self, entity: Entity, events: Events) -> None:
        """Persists records of entity to postgres.

        Args:
            entity: Entity which events are going to be persisted.
            events: Events (CREATE, AMEND, REMOVE) to persist.
        """
        query: BaseQueries = self._queries[entity]

        # if multiple instructions have the same primary key
        # db instructions can't be in batch
        batch_create = len(set(e.curr.key for e in events[EventType.CREATE])) == len(
            events[EventType.CREATE]
        )
        batch_amend = len(set(e.curr.key for e in events[EventType.AMEND])) == len(
            events[EventType.AMEND]
        )

        # CREATE
        self._target.execute(
            instruction=query.APPEND_LOG,
            logs=[e.as_record() for e in events[EventType.CREATE]],
        )
        if batch_create:
            self._target.execute(
                instruction=query.UPSERT,
                logs=[e.curr.as_tuple() for e in events[EventType.CREATE]],
            )
        else:
            for e in events[EventType.CREATE]:
                self._target.execute(
                    instruction=query.UPSERT,
                    logs=[e.curr.as_tuple()],
                )

        # AMEND
        self._target.execute(
            instruction=query.APPEND_LOG,
            logs=[e.as_record() for e in events[EventType.AMEND]],
        )
        if batch_amend:
            self._target.execute(
                instruction=query.UPSERT,
                logs=[e.curr.as_tuple() for e in events[EventType.AMEND]],
            )
        else:
            for e in events[EventType.AMEND]:
                self._target.execute(
                    instruction=query.UPSERT,
                    logs=[e.curr.as_tuple()],
                )

        # REMOVE
        self._target.execute(
            instruction=query.APPEND_LOG,
            logs=[e.as_record() for e in events[EventType.REMOVE]],
        )
        self._target.execute(
            instruction=query.DELETE,
            logs=[e.curr.key for e in events[EventType.REMOVE]],
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
        "--notifications",
        dest="notifications",
        action="store_true",
        required=False,
        help="Enable notifications (default)",
    )
    parser.add_argument(
        "--no-notifications",
        dest="notifications",
        action="store_false",
        required=False,
        help="Disable notifications",
    )
    parser.set_defaults(
        notifications=ast.literal_eval(os.environ.get("NOTIFICATIONS", "False"))
    )

    parser.add_argument(
        "--nats",
        dest="nats",
        default=os.environ.get("NATS"),
        type=str,
        required=False,
        help="NATS connection URL. e.g.: nats://127.0.0.1:4222",
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
        "--portfolio_type",
        dest="portfolio_type",
        default=os.getenv("PORTFOLIO_TYPE"),
        type=str,
        required=False,
        help="Type of portfolio (dollar neutral, etc.).",
    )

    parser.add_argument(
        "--rebal_freq",
        dest="rebal_freq",
        default=os.getenv("REBAL_FREQ"),
        type=str,
        required=False,
        help="Rebalancing frequency of the portfolio.",
    )

    parser.add_argument(
        "--adjust",
        dest="adjust",
        default=os.getenv("ADJUST"),
        type=str,
        required=False,
        help="The portfolio is adjusted even when there are no new decisions.",
    )

    parser.add_argument(
        "--wgt_method",
        dest="wgt_method",
        default=os.getenv("WGT_METHOD"),
        type=str,
        required=False,
        help="Weighting method of the portfolio.",
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
    parsed_args = parse_args()

    loader = Loader()
    loader.run(parsed_args)
