"""Helper functions."""


def binance_2_alpaca_symbol(symbol: str) -> str:
    # Assuming only USDC or USDT symbols, just removing the final character
    return symbol[:-1]
