from scripts.env import HOST, PATH
from pandas import DataFrame
import pandas as pd
import pylabwons as lw
import time
pd.set_option('display.expand_frame_repr', False)


class BaselineBuilder(DataFrame):

    _metadata = ['td', 'sector', 'market', 'number']

    def __init__(self):
        self.td     = lw.TradingDate()
        self.sector = sector = lw.WiseICS(PATH.PARQ.WICS)
        self.market = market = lw.AfterMarket(PATH.PARQ.AFTERMARKET)
        self.number = number = lw.Fundamentals(PATH.PARQ.FUNDAMENTALS)
        super().__init__(
            sector \
            .join(market.drop(columns=['name'])) \
            .join(number.drop(columns=['close', 'foreignRate']))
        )
        return

    def is_buildable(self):
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

        if (self.td.clock().hour < 20) and (self.sector['date'].unique()[0] != self.td.closed):
            try:
                self.sector.fetch()
                self.sector.to_parquet(PATH.PARQ.WICS, engine='pyarrow')
            except Exception as e:
                pass

        if (self.td.clock().hour < 20) and (self.market['date'].unique()[0] != self.td.closed):
            try:
                self.market.fetch()
                self.market.to_parquet(PATH.PARQ.AFTERMARKET, engine='pyarrow')
            except Exception as e:
                pass

        if self.td.clock().hour >= 20:
            base = self.market[self.market['marketCap'] >= self.market['marketCap'].median()]
            try:
                self.number.fetch(*base.index)
                for col in self.number.columns:
                    if self.number[col].dtype == 'object':
                        self.number[col] = self.number[col].astype(str)
                self.number.to_parquet(PATH.PARQ.FUNDAMENTALS, engine='pyarrow')
            except Exception as e:
                pass

        super().__init__(
            self.sector.drop(columns=['date']) \
                .join(self.market.drop(columns=['name'])) \
                .join(self.number.drop(columns=['date', 'close']))
        )
        self['fiftyTwoWeekHigh'] = self[['close', 'fiftyTwoWeekHigh']].max(axis=1)
        self['fiftyTwoWeekLow'] = self[['close', 'fiftyTwoWeekLow']].min(axis=1)
        self['fiftyTwoWeekHighPct'] = 100 * (self['close'] / self['fiftyTwoWeekHigh'] - 1)
        self['fiftyTwoWeekLowPct'] = 100 * (self['close'] / self['fiftyTwoWeekLow'] - 1)
        self['targetPricePct'] = 100 * (self['close'] / self['targetPrice'] - 1)


        self.to_parquet(PATH.PARQ.BASELINE, engine='pyarrow')
        return



if __name__ == "__main__":
    baseline = BaselineBuilder()
    print(baseline)
    print(baseline.columns)
    baseline.to_excel(PATH.DOWNLOADS / 'baseline.xlsx')

    # print(baseline.is_buildable())