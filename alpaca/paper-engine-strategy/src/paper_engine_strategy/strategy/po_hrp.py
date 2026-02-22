from typing import Any, Dict, List
import pandas as pd
import logging
import os
from datetime import datetime, timedelta

# Imports da Alpaca
from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame

# Import base
from paper_engine_strategy.strategy.base import BaseStrategy

import paper_engine_strategy.strategy.hrp.config as hrp_config
from paper_engine_strategy.strategy.hrp.functions import _get_weights, _get_sp500_constituents

logger = logging.getLogger(__name__)

class POHRPStrategy(BaseStrategy):
    """
    Estratégia HRP Trimestral.
    Só vai à Alpaca no início de cada trimestre.
    """
    strategy_id: int
    params: Dict[str, Any]

    @classmethod
    def setup(cls, strategy_config: Dict[str, Any]) -> "BaseStrategy":
        res = cls()
        
        # Tenta ler do config (se enviado), caso contrário lê direto do ficheiro .env global
        api_key = strategy_config.get("api_key", os.environ.get("API_KEY"))
        secret_key = strategy_config.get("secret_key", os.environ.get("SECRET_KEY"))
        
        res.params = {
            "n_clusters": int(strategy_config.get("n_clusters", hrp_config.N_CLUSTERS)),
            "lookback_days": int(strategy_config.get("lookback_days", 300)),
            "api_key": api_key,
            "secret_key": secret_key
        }
        return res

    def get_weights(self, records: List, prev_weights: List[List]) -> List[List]:
        current_date = datetime.now()
        
        # 1. VERIFICAÇÃO DE TRIMESTRE 
        if not self._needs_rebalancing(prev_weights, current_date):
            logger.info(f"[HRP] Quarter mantém-se. A manter posições antigas.")
            return [[w[0], current_date, w[2]] for w in prev_weights]
        
        logger.info("[HRP] Novo trimestre, a recalcular peso...")

        api_key = self.params.get("api_key")
        secret_key = self.params.get("secret_key")

        if not api_key or not secret_key:
            logger.error("[HRP] Erro: Chaves da Alpaca não configuradas!")
            return self._fallback(prev_weights, current_date)

        # 2. Obter Tickers e Traduzir para formato Alpaca
        try:
            sp500_tickers = _get_sp500_constituents()
            stock_tickers = sp500_tickers + hrp_config.commodities
            
            # A Alpaca precisa de pontos para ações (BRK.B) e barras para crypto (BTC/USD)
            alpaca_stock_tickers = [c.replace('-', '.') for c in stock_tickers]
            alpaca_crypto_tickers = [c.replace('-', '/') for c in hrp_config.crypto]
            
        except Exception as e:
            logger.error(f"[HRP] Erro crítico na organização dos tickers: {e}")
            return self._fallback(prev_weights, current_date)

        # 3. Download Alpaca 
        start_date = current_date - timedelta(days=self.params["lookback_days"])
        
        try:
            stock_client = StockHistoricalDataClient(api_key, secret_key)
            crypto_client = CryptoHistoricalDataClient(api_key, secret_key)

            # A. Download Stocks em lotes
            stock_dfs = []
            for i in range(0, len(alpaca_stock_tickers), 100):
                chunk = alpaca_stock_tickers[i:i+100]
                req = StockBarsRequest(
                    symbol_or_symbols=chunk,
                    timeframe=TimeFrame.Day,
                    start=start_date,
                    end=current_date
                )
                bars = stock_client.get_stock_bars(req)
                
                if not bars.df.empty:
                    df_chunk = bars.df.reset_index().pivot(index='timestamp', columns='symbol', values='close')
                    stock_dfs.append(df_chunk)

            stock_data = pd.concat(stock_dfs, axis=1) if stock_dfs else pd.DataFrame()
            
            # Reverter pontos para traços (BRK.B -> BRK-B)
            if not stock_data.empty:
                stock_data.columns = [c.replace('.', '-') for c in stock_data.columns]

            # B. Download Crypto
            crypto_req = CryptoBarsRequest(
                symbol_or_symbols=alpaca_crypto_tickers,
                timeframe=TimeFrame.Day,
                start=start_date,
                end=current_date
            )
            crypto_bars = crypto_client.get_crypto_bars(crypto_req)
            
            if not crypto_bars.df.empty:
                crypto_data = crypto_bars.df.reset_index().pivot(index='timestamp', columns='symbol', values='close')
                # Reverter barras para traços (BTC/USD -> BTC-USD)
                crypto_data.columns = [c.replace('/', '-') for c in crypto_data.columns]
            else:
                crypto_data = pd.DataFrame()

            # C. Unir tabelas
            data = pd.concat([stock_data, crypto_data], axis=1)
            data.index = pd.to_datetime(data.index).tz_localize(None).normalize()

        except Exception as e:
            logger.error(f"[HRP] Erro no download da Alpaca: {e}")
            return self._fallback(prev_weights, current_date)

        # 4. Limpeza e Cálculo
        try:
            # Remover dias fechados (feriados/fins de semana)
            min_ativos_validos = len(data.columns) * 0.5
            data_limpa = data.dropna(thresh=min_ativos_validos, axis=0)
            
            # Cortar amostra de 186 dias e limpar totalmente
            last_data = data_limpa.iloc[-186:] 
            last_data = last_data.dropna(axis=1, how='all')
            last_data = last_data.ffill().bfill()
            last_data = last_data.dropna(axis=1, how='any')

            if last_data.empty or last_data.shape[1] < 2:
                logger.warning(f"[HRP] Dados insuficientes após limpeza: sobraram {last_data.shape[1]} ativos.")
                return self._fallback(prev_weights, current_date)
        except Exception as e:
            logger.error(f"[HRP] Erro na limpeza de dados: {e}")
            return self._fallback(prev_weights, current_date)

        # 5. Matemática HRP
        try:
            raw_weights = _get_weights(last_data, self.params["n_clusters"])
            
            output = []
            iterator = raw_weights.items() if hasattr(raw_weights, 'items') else raw_weights.items()
            
            for ticker, weight in iterator:
                output.append([ticker, current_date, float(weight)])
            
            logger.info(f"[HRP] SUCESSO: Novos pesos gerados para o Quarter ({len(output)} ativos ativos).")
            return output

        except Exception as e:
            logger.error(f"[HRP] Erro matemático no HRP: {e}")
            return self._fallback(prev_weights, current_date)

    def _needs_rebalancing(self, prev_weights, current_date):
        """
        Retorna True APENAS se mudámos de trimestre.
        """
        if not prev_weights:
            return True

        try:
            last_rebal_date = prev_weights[0][1]
            if isinstance(last_rebal_date, str):
                last_rebal_date = pd.to_datetime(last_rebal_date)

            current_quarter = (current_date.month - 1) // 3 + 1
            last_quarter = (last_rebal_date.month - 1) // 3 + 1
            
            if current_quarter != last_quarter or current_date.year != last_rebal_date.year:
                return True
            
            return False

        except Exception:
            return True 

    def _fallback(self, prev_weights, current_date):
        if not prev_weights: return []
        return [[w[0], current_date, w[2]] for w in prev_weights]