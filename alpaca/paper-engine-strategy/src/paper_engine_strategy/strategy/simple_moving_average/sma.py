import pandas as pd

import logging

logger = logging.getLogger(__name__)

class SMA:
    """
    Estratégia simples de SMA (Simple Moving Average).
    Se o preço atual estiver acima da SMA, atribui peso 1. Caso contrário, peso 0.
    Mantém a posição anterior se o sinal não mudou.
    """

    def __init__(
        self,
        symbol: str,
        sma_window: int,
        closes: pd.DataFrame,
        previous_weights=None,
    ):
        self.symbol = symbol
        self.sma_window = sma_window
        self.closes = closes
        self.previous_weights = previous_weights
        self.last_date = closes.index[-1]

    def get_weights(self):
        """Calcula os pesos com base na SMA simples."""
        if self.symbol not in self.closes.columns:
            logger.warning(f"Símbolo {self.symbol} não encontrado nos dados.")
            return []

        prices = self.closes[self.symbol]
        sma = prices.rolling(self.sma_window).mean()

        latest_price = prices.iloc[-1]
        latest_sma = sma.iloc[-1]

        # Sinal simples: 1 se preço > SMA, senão 0
        signal = 1 if latest_price > latest_sma else 0

        # Mantém a posição anterior, se existir
        if self.previous_weights is not None:
            prev_weight = next((w[2] for w in self.previous_weights if w[0] == self.symbol), 0)
            if prev_weight == signal:
                weight = prev_weight
            else:
                weight = signal
        else:
            weight = signal

        logger.debug(
            f"[SMA] {self.symbol} | Price={latest_price:.2f} | SMA={latest_sma:.2f} | Weight={weight}"
        )

        return [[self.symbol, self.last_date, weight]]
