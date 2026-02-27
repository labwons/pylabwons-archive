from scripts.env import HOST, PATH
from scripts.src.schema.key import COLUMNS
from pandas import DataFrame
import numpy as np
import pandas as pd
import pylabwons as lw
import time
pd.set_option('display.expand_frame_repr', False)


class BaselineBuilder(DataFrame):

    _metadata = ['td', 'sector', 'market', 'number', 'logger']

    def __init__(self):
        self.logger = logger = lw.Logger(console=True)

        self.td     = lw.TradingDate()
        self.sector = sector = lw.WiseICS(PATH.PARQ.WICS, logger=logger)
        self.market = market = lw.AfterMarket(PATH.PARQ.AFTERMARKET, logger=logger)
        self.number = number = lw.Fundamentals(PATH.PARQ.FUNDAMENTALS, logger=logger)
        try:
            super().__init__(pd.read_parquet(PATH.PARQ.BASELINE, engine='pyarrow'))
        except (ConnectionError, IndexError, KeyError, Exception):
            self._merge(sector, market, number)
        return

    @classmethod
    def _merge(cls, *dataframes) -> DataFrame:
        base = dataframes[0]
        for _df in dataframes[1:]:
            _df = _df[list(set(_df.columns) - set(base.columns))]
            base = base.join(_df, how='left')

        base['fiftyTwoWeekHigh'] = base['fiftyTwoWeekHigh'].fillna(base['close'])
        base['fiftyTwoWeekLow'] = base['fiftyTwoWeekLow'].fillna(base['close'])

        df = cls._type_cast(base)
        df['fiftyTwoWeekHigh'] = np.fmax(df['close'], df['fiftyTwoWeekHigh'])
        df['fiftyTwoWeekLow'] = np.fmin(df['close'], df['fiftyTwoWeekLow'])
        df['fiftyTwoWeekHighPct'] = round(100 * (df['close'] / df['fiftyTwoWeekHigh'] - 1), 2)
        df['fiftyTwoWeekLowPct'] = round(100 * (df['close'] / df['fiftyTwoWeekLow'] - 1), 2)
        df['targetPricePct'] = round(100 * (df['close'] / df['targetPrice'] - 1), 2)
        return df[COLUMNS.keys()]

    @classmethod
    def _type_cast(cls, merged:DataFrame) -> DataFrame:
        for col in merged.columns:
            try:
                if col == 'ipo':
                    merged[col] = merged[col].str.replace('-', '')
                if col == 'numbersDate':
                    merged[col] = merged[col].fillna('-1').str.replace('/', '')
                if col in ['sharesOutstanding', 'sharesPreferred', 'sharesFloating']:
                    merged[col] = merged[col].str.replace('nan', '0').fillna('0')
                merged[col] = merged[col].astype(COLUMNS[col].data_type)
            except (TypeError, ValueError) as e:
                # print(col, ':', e)
                pass
        return merged.copy()

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
        if HOST == 'hkefico':
            self.logger('>>> SKIP FETCHING')
        else:
            if self.td.clock().hour <= 16:
                if self.market.date != self.td.closed:
                    try:
                        self.market.fetch()
                        self.market.to_parquet(PATH.PARQ.AFTERMARKET, engine='pyarrow')
                    except (ConnectionError, IndexError, KeyError, Exception) as e:
                        self.logger(f'>>> FAILED TO BUILD AFTER MARKET: {e}')

            elif self.td.clock().hour >= 17:
                if self.sector.date != self.td.closed:
                    try:
                        self.sector.fetch()
                        self.sector.to_parquet(PATH.PARQ.WICS, engine='pyarrow')
                    except (ConnectionError, IndexError, KeyError, Exception) as e:
                        self.logger(f'>>> FAILED TO BUILD SECTOR: {e}')

                base = self.market[self.market['marketCap'] >= self.market['marketCap'].median()]
                try:
                    self.number.fetch(*base.index)
                    for col in self.number.columns:
                        if self.number[col].dtype == 'object':
                            self.number[col] = self.number[col].astype(str)
                    self.number.to_parquet(PATH.PARQ.FUNDAMENTALS, engine='pyarrow')
                except (ConnectionError, IndexError, KeyError, Exception) as e:
                    self.logger(f'>>> FAILED TO BUILD NUMBERS: {e}')

        super().__init__(self._merge(self.sector, self.market, self.number))
        self.sort_values(by='marketCap', ascending=False, inplace=True)
        for col in self.columns:
            self[col] = self[col].astype(str)
        self.to_parquet(PATH.PARQ.BASELINE, engine='pyarrow')
        return



if __name__ == "__main__":
    baseline = BaselineBuilder()
    # print(baseline)
    # print(baseline.columns)
    # baseline.build()
    # print(baseline)
    # print(baseline.logger)
    # baseline.to_excel(PATH.DOWNLOADS / 'baseline.xlsx')

    # print(baseline.is_buildable())
