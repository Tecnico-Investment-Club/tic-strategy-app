from typing import Any, Dict, List
import pandas as pd
import logging
import yfinance as yf
from datetime import datetime, timedelta

# Import base
from paper_engine_strategy.strategy.base import BaseStrategy

import paper_engine_strategy.strategy.hrp.config as hrp_config
from paper_engine_strategy.strategy.hrp.functions import _get_weights, _get_sp500_constituents

logger = logging.getLogger(__name__)

class POHRPStrategy(BaseStrategy):
    """
    Estratégia HRP Trimestral.
    Só vai ao Yahoo no início de cada trimestre.
    """
    strategy_id: int
    params: Dict[str, Any]

    @classmethod
    def setup(cls, strategy_config: Dict[str, Any]) -> "BaseStrategy":
        res = cls()
        res.params = {
            "n_clusters": int(strategy_config.get("n_clusters", hrp_config.N_CLUSTERS)),
            "lookback_days": int(strategy_config.get("lookback_days", 300)),
        }
        return res

    def get_weights(self, records: List, prev_weights: List[List]) -> List[List]:
        current_date = datetime.now()
        
        # 1. VERIFICAÇÃO DE TRIMESTRE 
        if not self._needs_rebalancing(prev_weights, current_date):
            logger.info(f"[HRP] Quarter mantém-se. A manter posições antigas.")
            return [[w[0], current_date, w[2]] for w in prev_weights]
        
        logger.info("[HRP] Novo trimestre, a recalcular peso...")

        # 2. Obter Tickers
        try:
            sp500_tickers = _get_sp500_constituents()
            tickers = sp500_tickers + hrp_config.commodities + hrp_config.crypto
        except Exception as e:
            logger.error(f"[HRP] Erro críticos nos tickers: {e}")
            return self._fallback(prev_weights, current_date)

        # 3. Download Yahoo 
        start_date = current_date - timedelta(days=self.params["lookback_days"])
        
        try:
            data_raw = yf.download(
                tickers, 
                start=start_date.strftime('%Y-%m-%d'), 
                end=current_date.strftime('%Y-%m-%d'), 
                progress=False,
                auto_adjust=True
            )
            
            if 'Close' in data_raw.columns:
                data = data_raw['Close']
            elif 'Adj Close' in data_raw.columns:
                data = data_raw['Adj Close']
            else:
                data = data_raw
            
            data.index = pd.to_datetime(data.index)

        except Exception as e:
            logger.error(f"[HRP] Erro no Yahoo Finance: {e}")
            # Se o Yahoo falhar no dia 1 do trimestre, mantemos o antigo e tentamos amanhã
            return self._fallback(prev_weights, current_date)

        # 4. Limpeza e Cálculo
        last_data = data.iloc[-186:] 
        last_data = last_data.dropna(axis=1, how='all')
        last_data = last_data.ffill().iloc[3:].dropna(axis=1, how='any')

        if last_data.empty or last_data.shape[1] < 2:
            logger.warning("[HRP] Dados insuficientes após limpeza.")
            return self._fallback(prev_weights, current_date)

        try:
            raw_weights = _get_weights(last_data, self.params["n_clusters"])
            
            output = []
            iterator = raw_weights.items() if hasattr(raw_weights, 'items') else raw_weights.items()
            
            for ticker, weight in iterator:
                output.append([ticker, current_date, float(weight)])
            
            logger.info(f"[HRP] SUCESSO: Novos pesos gerados para o Quarter.")
            return output

        except Exception as e:
            logger.error(f"[HRP] Erro matemático: {e}")
            return self._fallback(prev_weights, current_date)

    def _needs_rebalancing(self, prev_weights, current_date):
        """
        Retorna True APENAS se mudámos de trimestre.
        """
        # Se não há histórico, temos de calcular (1ª vez)
        if not prev_weights:
            return True

        try:
            # Data da última vez que o sistema registou pesos
            last_rebal_date = prev_weights[0][1]
            if isinstance(last_rebal_date, str):
                last_rebal_date = pd.to_datetime(last_rebal_date)

            # Jan, Fev, Mar = Q1 | Abr, Mai, Jun = Q2 ...
            current_quarter = (current_date.month - 1) // 3 + 1
            last_quarter = (last_rebal_date.month - 1) // 3 + 1
            
            # Se o trimestre for diferente, OU se mudou o ano -> REBALANCEAR
            if current_quarter != last_quarter or current_date.year != last_rebal_date.year:
                return True
            
            # Se for o mesmo trimestre -> NÃO FAZER NADA
            return False

        except Exception:
            return True # Na dúvida, calcula

    def _fallback(self, prev_weights, current_date):
        if not prev_weights: return []
        return [[w[0], current_date, w[2]] for w in prev_weights]