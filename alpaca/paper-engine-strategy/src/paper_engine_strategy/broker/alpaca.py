"""Alpaca connection."""

from datetime import datetime
from decimal import Decimal
import logging
from typing import Dict, List, Optional

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.trading.client import TradingClient
from alpaca.trading.models import Position, TradeAccount
from alpaca.trading.stream import TradingStream

from paper_engine_strategy.broker.base import Broker

logger = logging.getLogger(__name__)


class Alpaca(Broker):
    """Alpaca connection class."""

    latest_order = None
    positions: List[Position]

    def __init__(self, api_key: str, secret_key: str) -> None:
        self.trading_client = TradingClient(api_key, secret_key, paper=True)
        self.trading_stream = TradingStream(api_key, secret_key, paper=True)

        self.data_client = StockHistoricalDataClient(api_key, secret_key)

    def get_portfolio_value(self, side: str = "ALL") -> Decimal:
        """Get portfolio value."""
        account: TradeAccount = self.trading_client.get_account()
        if side == "LONG":
            return Decimal(account.long_market_value)
        elif side == "SHORT":
            return -Decimal(account.short_market_value)
        else:
            return Decimal(account.equity)

    def get_all_positions(self) -> Optional[Dict]:
        """Get ticker: quantity dict."""
        self.positions = self.trading_client.get_all_positions()
        res = {
            p.symbol: {
                "quantity": p.qty,
                "side": p.side,
                "notional": Decimal(p.market_value),
            }
            for p in self.positions
        }
        return res if res else None

    def get_current_weights(self) -> Optional[List[List]]:
        positions = self.get_all_positions()
        long_value = self.get_portfolio_value("LONG")
        short_value = self.get_portfolio_value("SHORT")

        if not positions:
            return None

        weights = []
        for symbol, position in positions.items():
            if position["side"] == "long":
                wgt = position["notional"] / long_value
            elif position["side"] == "short":
                wgt = position["notional"] / short_value
            else:
                return None
            weights.append([symbol, datetime.utcnow(), float(wgt)])
        return weights
