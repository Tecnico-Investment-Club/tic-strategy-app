"""Alpaca connection."""

from datetime import datetime
from decimal import Decimal
import logging
from typing import Dict, List, Tuple

from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest, CryptoLatestQuoteRequest
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
    """Alpaca connection class."""

    latest_order = None
    positions: List[Position]

    crypto: bool

    def __init__(self, api_key: str, secret_key: str) -> None:
        self.trading_client = TradingClient(api_key, secret_key, paper=True)
        self.trading_stream = TradingStream(api_key, secret_key, paper=True)

        self.stock_data_client = StockHistoricalDataClient(api_key, secret_key)
        self.crypto_data_client = CryptoHistoricalDataClient(api_key, secret_key)
        if self.trading_client and self.trading_stream and self.stock_data_client and self.crypto_data_client:
            logger.warning("Alpaca TradingClient initialized (Paper=True).")
        else:
            logger.warning("Alpaca TradingClient NOT initialized properly. One or more clients is None.")

    def get_account_capital(self) -> Decimal:
        """Get account capital (equity + cash)."""
        account_capital = Decimal(self.trading_client.get_account().equity)
        logger.debug(f"Account Capital: {account_capital}")
        return account_capital

    def get_all_assets(self) -> List[Asset]:
        """Get all assets from the broker."""
        us_stocks_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY)
        crypto_params = GetAssetsRequest(asset_class=AssetClass.CRYPTO)

        us_stocks = self.trading_client.get_all_assets(us_stocks_params)
        crypto = self.trading_client.get_all_assets(crypto_params)

        assets = us_stocks + crypto

        return assets

    def check_tradable(self, symbols) -> List[str]:
        """Get all available tickers from the broker."""
        if self.crypto:
            symbols = [f'{s[:-3]}/{s[-3:]}' for s in symbols]
        assets = self.get_all_assets()
        available_asset_ids = [a.symbol for a in assets]
        tradable_asset_ids = [s for s in symbols if s in available_asset_ids]

        if self.crypto:
            tradable_asset_ids = [f'{s[:-4]}{s[-3:]}' for s in tradable_asset_ids]

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
        if self.crypto:
            symbols = [f'{s[:-3]}/{s[-3:]}' for s in symbols]
            request_params = CryptoLatestQuoteRequest(symbol_or_symbols=symbols)
            latest_quotes = self.crypto_data_client.get_crypto_latest_quote(request_params)
            latest_asks = {f'{k[:-4]}{k[-3:]}': Decimal(v.ask_price) for k, v in latest_quotes.items()}
            latest_bids = {f'{k[:-4]}{k[-3:]}': Decimal(v.bid_price) for k, v in latest_quotes.items()}
        else:
            request_params = StockLatestQuoteRequest(symbol_or_symbols=symbols)
            latest_quotes = self.stock_data_client.get_stock_latest_quote(request_params)
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
                time_in_force=TimeInForce.GTC,
            )
        elif order_type == "LIMIT":
            order_data = LimitOrderRequest(
                symbol=config["symbol"],
                qty=config["quantity"],
                limit_price=config["limit_price"],
                side=OrderSide(config["side"]),
                time_in_force=TimeInForce.GTC,
            )
        else:
            logger.warning("No valid order type was provided.")
            return None

        self.latest_order = self.trading_client.submit_order(order_data=order_data)

    def submit_orders(self, orders: List[Dict]) -> None:
        """Submit provided orders to the broker."""
        for order in orders:
            try:
                logger.warning(f"Submitting {order['side']} order for {order['symbol']} qty={order['quantity']}")
                self.submit_order("MARKET", order)
                logger.warning(f"Order submitted successfully: {order['symbol']}")
            except Exception as e:
                logger.warning(f"Order: {order} did not go through. {e}")
                continue

    def get_positions(self) -> Dict:
        """Get ticker: quantity dict."""
        self.positions = self.trading_client.get_all_positions()
        res = {p.symbol: Decimal(p.qty) for p in self.positions}
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
                    asset_id_type = "CRYPTO_TICKER" if self.crypto else "STOCK_TICKER"
                    orders_record = (
                        portfolio_id,
                        side,  # side
                        asset_id_type,
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
