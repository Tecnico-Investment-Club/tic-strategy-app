"""Equal weighting scheme."""

from datetime import datetime
from decimal import Decimal
import logging
from typing import Dict, List

from paper_engine_orders._types import File
from paper_engine_orders.broker.base import Broker
from paper_engine_orders.model.source_model import Strategy
from paper_engine_orders.weighting.base import BaseWeight

logger = logging.getLogger(__name__)


class Weighting(BaseWeight):
    """Equal weighting scheme class."""

    capital: Decimal
    strategy_records: List
    current_positions: Dict

    broker: Broker

    target_weights: Dict
    real_weights: Dict
    quantities: Dict
    notional: Dict
    prices: Dict

    total_value: Decimal

    @classmethod
    def setup(
        cls,
        broker: Broker,
        capital: Decimal,
        strategy_records: List[Strategy],
        current_positions: Dict,
    ) -> "Weighting":
        """Setup weighting scheme for the portfolio."""
        res = cls()
        res.capital = capital
        res.strategy_records = strategy_records
        res.current_positions = current_positions
        res.broker = broker

        res.get_weights()

        return res

    def get_target_weights(self) -> None:
        """Get weighting method target weights."""
        self.target_weights = {s.asset_id: s.weight for s in self.strategy_records}

    def get_weights(self) -> None:
        """Get portfolio weights."""
        strat_tickers = [s.asset_id for s in self.strategy_records]
        latest_asks, latest_bids = self.broker.get_latest_book(strat_tickers)

        self.prices: Dict = {}
        for s in self.strategy_records:
            if s.decision == 1:
                price = latest_asks.get(s.asset_id, 0)
                self.prices[s.asset_id] = price if price is not None else 0
            else:
                price = latest_bids.get(s.asset_id, 0)
                self.prices[s.asset_id] = price if price is not None else 0

        # FILTER OUT STRAT RECORDS WITH TICKERS WITH PRICE 0 OR NONE
        available_strat_records = []
        for s in self.strategy_records:
            price = self.prices[s.asset_id]
            if price is not None and price != 0:
                available_strat_records.append(s)
        self.strategy_records = available_strat_records

        self.get_target_weights()

        used_capital = 0
        notional = {}
        quantities = {}
        for s in self.strategy_records:
            target_notional = self.capital * self.target_weights[s.asset_id]

            price = self.prices[s.asset_id]
            if s.asset_id_type == "STOCK_TICKER":
                quantity = Decimal(target_notional // price)
            else:
                # CRYPTO_TICKER
                quantity = Decimal(target_notional / price)

            # ALWAYS BUY/SELL 1 (?)
            if quantity == Decimal(0):
                quantity = Decimal(1)
            quantities[s.asset_id] = quantity

            real_notional = quantity * price
            notional[s.asset_id] = real_notional

            used_capital += real_notional

        # IMPLEMENT REALOCATION OF NOT ALLOCATED CAPITAL (?)

        self.real_weights = {k: v / used_capital for k, v in notional.items()}
        self.total_value = sum([v for v in notional.values()])
        self.quantities = quantities
        self.notional = notional

    def get_orders_params(self) -> Dict:
        """Get orders porameters."""
        orders_params: Dict = {"buy": [], "sell": []}
        for s in self.strategy_records:
            target_quantity = self.quantities[s.asset_id]
            curr_quantity = (
                self.current_positions[s.asset_id]
                if s.asset_id in self.current_positions.keys()
                else None
            )
            price = self.prices[s.asset_id]
            if s.decision == 1:
                quantity = (
                    target_quantity - curr_quantity
                    if curr_quantity
                    else target_quantity
                )
                notional_change = abs(quantity * price)
                if notional_change > 1:
                    if quantity < 0:
                        order_params = self.broker.sell_params(s.asset_id, quantity)
                        orders_params["sell"].append(order_params)
                    elif quantity > 0:
                        order_params = self.broker.buy_params(s.asset_id, quantity)
                        orders_params["buy"].append(order_params)
            else:
                quantity = (
                    target_quantity + curr_quantity
                    if curr_quantity
                    else target_quantity
                )
                notional_change = abs(quantity * price)
                if notional_change > 1:
                    if quantity > 0:
                        order_params = self.broker.sell_params(s.asset_id, quantity)
                        orders_params["sell"].append(order_params)
                    elif quantity < 0:
                        order_params = self.broker.buy_params(s.asset_id, quantity)
                        orders_params["buy"].append(order_params)

        return orders_params

    def get_orders_records(self, portfolio_id: int) -> File:
        """Get orders records."""
        records: File = []
        for s in self.strategy_records:
            target_quantity = self.quantities[s.asset_id]
            curr_quantity = (
                self.current_positions[s.asset_id]
                if s.asset_id in self.current_positions.keys()
                else None
            )
            quantity = (
                target_quantity - abs(curr_quantity)
                if curr_quantity
                else target_quantity
            )
            notional_change = abs(quantity * self.prices[s.asset_id])
            if quantity and notional_change > 1:
                records.append(
                    (
                        portfolio_id,
                        s.decision,  # side
                        s.asset_id_type,
                        s.asset_id,
                        datetime.utcnow(),  # order timestamp
                        self.target_weights[s.asset_id] * s.decision,
                        self.real_weights[s.asset_id] * s.decision,
                        self.quantities[s.asset_id] * s.decision,  # quantity of the order
                        self.notional[s.asset_id] * s.decision,  # notional value of the order
                    )
                )
        return records
