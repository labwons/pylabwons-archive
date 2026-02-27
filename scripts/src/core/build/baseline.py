from scripts.src.schema.dataframe import DataFrameHeir
from scripts.src.schema.key import BASELINE
from scripts.src.core.fetch.aftermarket import AfterMarket
from scripts.src.core.fetch.numbers import Numbers
from scripts.src.core.fetch.wics import Wics
from scripts.env import HOST, PATH
import numpy as np
import pandas as pd
import pylabwons as lw
import time
pd.set_option('display.expand_frame_repr', False)


class Baseline(DataFrameHeir):

    _metadata = ['td', 'sector', 'market', 'number', 'logger']

    def __init__(self):
        self.logger = lw.Logger(console=True)

        self.td     = lw.TradingDate()
        # self.sector = sector = lw.WiseICS(PATH.PARQUET.WICS, logger=logger)
        # self.market = market = lw.AfterMarket(PATH.PARQUET.AFTERMARKET, logger=logger)
        # self.number = number = lw.Fundamentals(PATH.PARQUET.FUNDAMENTALS, logger=logger)
        try:
            super().__init__(pd.read_parquet(PATH.PARQUET.BASELINE, engine='pyarrow'))
            return
        except (ConnectionError, FileNotFoundError, IndexError, Exception):
            super().__init__(
                PATH.PARQUET.WICS,
                PATH.PARQUET.AFTERMARKET,
                PATH.PARQUET.NUMBERS,
                method='join'
            )
            self._post()
        return

    def _post(self):
        for col in self.columns:
            try:
                if col == 'ipo':
                    self[col] = self[col].str.replace('-', '')
                if col == 'numbersDate':
                    self[col] = self[col].fillna('-1').str.replace('/', '')
                if col in ['sharesOutstanding', 'sharesPreferred', 'sharesFloating']:
                    self[col] = self[col].str.replace('nan', '0').fillna('0')
                if 'debt' in col.lower():
                    self[col] = self[col].str.replace('적자전환', np.nan)
                self[col] = self[col].astype(BASELINE[col].data_type)
            except (TypeError, ValueError) as e:
                self.logger(f'{col}: {e}')

        self['fiftyTwoWeekHigh'] = np.fmax(self['close'], self['fiftyTwoWeekHigh'].fillna(self['close']))
        self['fiftyTwoWeekLow'] = np.fmin(self['close'], self['fiftyTwoWeekLow'].fillna(self['close']))
        self['fiftyTwoWeekHighPct'] = round(100 * (self['close'] / self['fiftyTwoWeekHigh'] - 1), 2)
        self['fiftyTwoWeekLowPct'] = round(100 * (self['close'] / self['fiftyTwoWeekLow'] - 1), 2)
        self['targetPricePct'] = round(100 * (self['close'] / self['targetPrice'] - 1), 2)
        self['estimatedPe'] = self['close'] / self['estimatedEps']
        self['forwardPe'] = self['close'] / self['forwardEps']
        self['trailingPe'] = self['close'] / self['trailingEps']
        self['trailingPs'] = (self['marketCap'] / 1e+8) / self['trailingRevenue']

        self.sort_values(by='marketCap', ascending=False, inplace=True)
        super().__init__(self[BASELINE.keys()])
        return

    def is_buildable(self):
        if HOST == 'hkefico':
            return True

        if self.td.closed != self.td.clock("%Y%m%d"):
            return False

        clock = self.td.clock()
        while (clock.hour == 15) and (15 <= clock.minute < 31):
            time.sleep(30)
            clock = self.td.clock()
        return True

    def build(self):
        if not self.is_buildable():
            raise SystemExit

        self.logger(f'[BUILD BASELINE] @{self.td.closed}')
        market = AfterMarket(PATH.PARQUET.AFTERMARKET, logger=self.logger)
        sector = Wics(PATH.PARQUET.WICS, logger=self.logger)
        number = Numbers(PATH.PARQUET.NUMBERS, logger=self.logger)

        if not HOST == 'hkefico':
            if self.td.clock().hour <= 16:
                if market.date != self.td.closed:
                    try:
                        market.fetch()
                        market.to_parquet(PATH.PARQUET.AFTERMARKET, engine='pyarrow')
                    except (ConnectionError, IndexError, KeyError, Exception) as e:
                        self.logger(f'>>> FAILED TO BUILD AFTER MARKET: {e}')

            elif self.td.clock().hour >= 18:
                if sector.date != self.td.closed:
                    try:
                        sector.fetch()
                        sector.to_parquet(PATH.PARQUET.WICS, engine='pyarrow')
                    except (ConnectionError, IndexError, KeyError, Exception) as e:
                        self.logger(f'>>> FAILED TO BUILD SECTOR: {e}')

                base = market[market['marketCap'] >= market['marketCap'].median()]
                try:
                    number.fetch(*base.index)
                    for col in self.number.columns:
                        if number[col].dtype == 'object':
                            number[col] = self.number[col].astype(str)
                    number.to_parquet(PATH.PARQUET.NUMBERS, engine='pyarrow')
                except (ConnectionError, IndexError, KeyError, Exception) as e:
                    self.logger(f'>>> FAILED TO BUILD NUMBERS: {e}')

        super().__init__(sector, market, number, method='join')
        self._post()
        copy = self.copy()
        for col in copy.columns:
            copy[col] = copy[col].astype(str)
        copy.to_parquet(PATH.PARQUET.BASELINE, engine='pyarrow')
        return



if __name__ == "__main__":
    baseline = Baseline()
    # print(baseline)
    # print(baseline.columns)
    # baseline.build()
    # print(baseline)
    # print(baseline.logger)
    # baseline.to_excel(PATH.DOWNLOADS / 'baseline.xlsx')

    print(baseline[baseline['estimatedPayoutRatioGrowth'] == '적자지속'][['fiscalDps', 'estimatedDps']])