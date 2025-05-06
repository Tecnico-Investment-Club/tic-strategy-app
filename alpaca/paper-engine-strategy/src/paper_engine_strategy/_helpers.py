"""Helper functions."""


def binance_2_alpaca_symbol(symbol: str) -> str:
    # Assuming only USDC symbols
    return f'{symbol[:-4]}/{symbol[-4:-1]}'
