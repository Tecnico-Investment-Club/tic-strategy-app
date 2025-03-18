"""Alpaca connection."""

from datetime import datetime
from decimal import Decimal
import logging
from typing import Dict, List, Tuple

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest
from alpaca.trading.client import Order, Position, TradingClient
from alpaca.trading.enums import AssetClass, OrderSide, TimeInForce
from alpaca.trading.models import Asset
from alpaca.trading.requests import (
    GetAssetsRequest,
    LimitOrderRequest,
    MarketOrderRequest,
)
from alpaca.trading.stream import TradingStream

from paper_engine_orders.broker.base import Broker

logger = logging.getLogger(__name__)


class Alpaca(Broker):
    """Alpaca conncetion class."""

    latest_order = None
    positions: List[Position]

    def __init__(self, api_key: str, secret_key: str) -> None:
        self.trading_client = TradingClient(api_key, secret_key, paper=True)
        self.trading_stream = TradingStream(api_key, secret_key, paper=True)

        self.data_client = StockHistoricalDataClient(api_key, secret_key)

    def get_account_capital(self) -> Decimal:
        """Get account capital (equity + cash)."""
        account_capital = Decimal(self.trading_client.get_account().equity)
        return account_capital

    def get_all_assets(self) -> List[Asset]:
        """Get all assets from the broker."""
        search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY)
        assets = self.trading_client.get_all_assets(search_params)

        return assets

    def check_tradable(self, symbols) -> List[str]:
        """Get all available tickers from the broker."""
        assets = self.get_all_assets()
        available_asset_ids = [a.symbol for a in assets]
        tradable_asset_ids = [s for s in symbols if s in available_asset_ids]

        return tradable_asset_ids

    def check_shortable(self, symbols: List[str]) -> List[str]:
        """Get shortable tickers."""
        available_assets_info = self.get_all_assets()
        easy_to_borrow_tickers = [
            a.symbol for a in available_assets_info if a.easy_to_borrow
        ]
        shortable = [s for s in symbols if s in easy_to_borrow_tickers]

        return shortable

    def get_latest_book(self, symbols: List[str]) -> Tuple[Dict, Dict]:
        """Get latest bids and asks for the provided tickers."""
        request_params = StockLatestQuoteRequest(symbol_or_symbols=symbols)
        latest_quotes = self.data_client.get_stock_latest_quote(request_params)
        latest_asks = {k: Decimal(v.ask_price) for k, v in latest_quotes.items()}
        latest_bids = {k: Decimal(v.bid_price) for k, v in latest_quotes.items()}

        return latest_asks, latest_bids

    def submit_order(self, order_type: str, config: Dict) -> None:
        """Submit provided order to the broker."""
        if order_type == "MARKET":
            # preparing orders
            order_data = MarketOrderRequest(
                symbol=config["symbol"],
                qty=config["quantity"],
                side=OrderSide(config["side"]),
                time_in_force=TimeInForce.DAY,
            )
        elif order_type == "LIMIT":
            order_data = LimitOrderRequest(
                symbol=config["symbol"],
                qty=config["quantity"],
                limit_price=config["limit_price"],
                side=OrderSide(config["side"]),
                time_in_force=TimeInForce.DAY,
            )
        else:
            logger.info("No valid order type was provided.")
            return None

        self.latest_order = self.trading_client.submit_order(order_data=order_data)

    def submit_orders(self, orders: List[Dict]) -> None:
        """Submit provided orders to the broker."""
        for order in orders:
            try:
                self.submit_order("MARKET", order)
            except Exception as e:
                logger.warning(f"Order: {order} did not go through. {e}")
                continue

    def get_positions(self) -> Dict:
        """Get ticker: quantity dict."""
        self.positions = self.trading_client.get_all_positions()
        res = {p.symbol: int(p.qty) for p in self.positions}
        return res

    def close_positions(self, portfolio_id: int, tickers: List[str]) -> List:
        """Close positions on the provided tickers."""
        # attempt to cancel all open orders
        positions = self.get_positions()
        position_tickers = list(positions.keys())
        orders_records = []
        if len(tickers) > 0:
            for t in tickers:
                if t in position_tickers:
                    closed_position: Order = self.trading_client.close_position(t)
                    side = 1 if closed_position.side == "buy" else -1
                    orders_record = (
                        portfolio_id,
                        side,  # side
                        "TICKER",
                        closed_position.symbol,
                        datetime.utcnow(),  # order timestamp
                        0,
                        0,
                        closed_position.qty,  # quantity of the order
                        closed_position.notional,  # notional value of the order
                    )
                    orders_records.append(orders_record)

        return orders_records

    @staticmethod
    def buy_params(ticker: str, quantity: Decimal) -> Dict:
        """Generate buy order params."""
        order_params = {
            "symbol": ticker,
            "quantity": abs(quantity),
            "side": "buy",
        }
        return order_params

    @staticmethod
    def sell_params(ticker: str, quantity: Decimal) -> Dict:
        """Generate sell order params."""
        order_params = {
            "symbol": ticker,
            "quantity": abs(quantity),
            "side": "sell",
        }
        return order_params
