import logging

import pandas as pd

import paper_engine_strategy.strategy.portfolio_optimization.helpers.data_analysis as analysis
import paper_engine_strategy.strategy.portfolio_optimization.helpers.signalling as signalling
import paper_engine_strategy.strategy.portfolio_optimization.helpers.data_models as dm
import paper_engine_strategy.strategy.portfolio_optimization.helpers.portfolio_weights as pw
import paper_engine_strategy.strategy.portfolio_optimization.helpers.tc_optimization as tc

logger = logging.getLogger(__name__)


class PortfolioOptimization:

    def __init__(
        self,
        best_delta: float,
        mom_type: dm.Momentum_Type,
        mean_rev_type: dm.Mean_Rev_Type,
        rebalancing_period: dm.Rebalancing_Period,
        functional_constraints: dm.Functional_Constraints,
        rebalance_constraints: dm.Rebalance_Constraints,
        mom_days: int = 30,
        closes: pd.DataFrame = None,
        previous_weights=None,
    ):
        self.closes = closes
        self.last_date = closes.index[-1]
        self.previous_weights = previous_weights
        self.best_delta = best_delta
        self.momentum_type = mom_type
        self.mean_rev_type = mean_rev_type
        self.rebalancing_period = rebalancing_period
        self.functional_constraints = functional_constraints
        self.rebalance_constraints = rebalance_constraints
        self.momentum_days = mom_days

    def get_weights(self):

        """
        Black-Box function to get the weights of the portfolio.
        It uses the Hurst exponent to filter the assets and then uses the momentum and mean reversion strategies to generate buy signals.
        It then calculates the target weights based on the buy signals and the previous weights.
        It uses the distance method to calculate the rebalanced weights based on the previous weights and the target weights.
        It returns the rebalanced weights.
        Parameters:
            closes (pd.DataFrame): DataFrame with datetime index and adjusted close prices for different assets
            previous_weights: Previous weights of the portfolio before rebalancing (if any)

        Returns:
            list: A list of lists containing the ticker, date and weight of each asset in the portfolio.
        """
        # Gerar sinais
        filtered_data, trendy_assets, mean_reverting_assets = analysis.filter_data(
            analysis.get_data_analysis(
                self.closes,
                self.rebalancing_period,
                self.functional_constraints.hurst_exponents_period,
                self.mean_rev_type,
                self.momentum_type,
                self.functional_constraints,
                self.momentum_days,
                live_analysis=True,
            ),
            hurst_thresholds=self.functional_constraints.hurst_filter,
            mean_rev_type=self.mean_rev_type,
            momentum_type=self.momentum_type,
            live_analysis=True,
        )

        buy_and_sells = signalling.buy_and_sell_signalling(
            filtered_data,
            mean_rev_type=self.mean_rev_type,
            momentum_type=self.momentum_type,
            functional_constraints=self.functional_constraints,
        )

        assets_to_buy, assets_to_sell = analysis.extract_assets(
            buy_and_sells, 0
        )
        logger.info(f"[PO] Assets to buy: {assets_to_buy}")
        logger.info(f"[PO] Assets to sell: {assets_to_sell}")

        if len(assets_to_buy) > 0:
            logger.info(f"[PO] Generating uniform weights for {len(assets_to_buy)} assets to buy")
            buy_array, _, _, _ = pw.calculate_uniform_weights(
                assets_to_buy, [], shorting_value=0
            )
            logger.info(f"[PO] Buy array: {buy_array}")

            target_weights = [
                [ticker, self.last_date, weight]
                for ticker, weight in zip(assets_to_buy, buy_array)
            ]
            logger.info(f"[PO] Target weights: {target_weights}")

            if self.previous_weights is not None:
                prev_assets = [pw[0] for pw in self.previous_weights]
                excluded_assets = [a for a in prev_assets if a not in assets_to_buy]
                logger.info(f"[PO] Previous assets: {prev_assets}")
                logger.info(f"[PO] Assets to exclude (set to 0): {excluded_assets}")
                target_weights.extend([[a, self.last_date, 0] for a in excluded_assets])

                target_weights.sort(key=lambda x: x[0])
                self.previous_weights.sort(key=lambda x: x[0])

                alpha = tc.adjust_alpha(
                    self.rebalance_constraints.distance_method,
                    target_weights,
                    self.previous_weights,
                    self.best_delta,
                    tomas=True,
                )
                logger.info(f"[PO] Rebalancing alpha (controls transition speed): {alpha:.6f}")

                rebalanced_weights = []
                for prev, target in zip(self.previous_weights, target_weights):
                    ticker = prev[0]  # Assuming the ticker (str) is the same
                    date = prev[1]  # Assuming the datetime is the same
                    weight = alpha * prev[2] + (1 - alpha) * target[2]
                    rebalanced_weights.append([ticker, date, weight])
                    logger.info(f"[PO]   {ticker}: prev={prev[2]:.6f}, target={target[2]:.6f} → rebalanced={weight:.6f}")

                logger.info(f"[PO] Rebalanced weights: {rebalanced_weights}")
                return rebalanced_weights

            else:
                logger.info("[PO] No previous weights, using target weights")
                return target_weights

        else:
            logger.warning("[PO] *** NO ASSETS TO BUY - Strategy did not generate any buy signals ***")
            if self.previous_weights:
                logger.info("[PO] Using previous weights (holding current positions).")
                return [
                    [ticker, self.last_date, weight]
                    for ticker, _, weight in self.previous_weights
                ]
            else:
                logger.warning("[PO] No previous weights, returning equal weights across all assets.")
                equal_weights = [
                    [ticker, self.last_date, 1 / len(self.closes.columns)]
                    for ticker in self.closes.columns
                ]
                logger.info(f"[PO] Equal weights: {equal_weights}")
                return equal_weights
