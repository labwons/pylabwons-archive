from pylabwons_stub.schema.dataframe import DataFrameHeir
from pylabwons_stub.schema.key import BASELINE
from pylabwons_stub.core.fetch.aftermarket import AfterMarket
from pylabwons_stub.core.fetch.numbers import Numbers
from pylabwons_stub.core.fetch.wics import Wics
from pylabwons_stub.env import HOST, PATH
from datetime import datetime
from typing import Any, Callable
import numpy as np
import pandas as pd
import pylabwons as lw
import json, time
pd.set_option('display.expand_frame_repr', False)


class Baseline(DataFrameHeir):

    _metadata = ['dates', 'logger', 'td',
                 'market', 'number', 'sector']

    def __init__(self, logger:Callable=print):
        self.logger = logger
        self.td = lw.TradingDate()
        with open(PATH.JSON.BUILD, 'r', encoding='utf-8') as f:
            self.dates = lw.DataDictionary(json.load(f))

        self.market = AfterMarket(src=PATH.PARQUET.AFTERMARKET, logger=logger)
        self.number = Numbers(src=PATH.PARQUET.NUMBERS, logger=logger)
        self.sector = Wics(src=PATH.PARQUET.WICS, logger=logger)

        try:
            super().__init__(pd.read_parquet(PATH.PARQUET.BASELINE, engine='pyarrow'))
            return
        except (ConnectionError, FileNotFoundError, IndexError, Exception):
            self._capture_baseline(
            PATH.PARQUET.WICS,
                PATH.PARQUET.AFTERMARKET,
                PATH.PARQUET.NUMBERS
            )
        return

    def _capture_baseline(self, *args):
        super().__init__(*args, method='join')

        # Data Cleansing and Type casting
        for col in self.columns:
            if BASELINE[col].data_type == str:
                continue
            if col in ['sharesOutstanding', 'sharesPreferred', 'sharesFloating']:
                self[col] = self[col].astype(str).str.replace('nan', '0').fillna('0')
            if col == 'ipo' or 'date' in col.lower():
                self[col] = self._typecast(self[col], datetime)
            else:
                self[col] = self._typecast(self[col])

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

    def _typecast(self, series: pd.Series, dtype: Any = 'numeric') -> pd.Series:
        if dtype == datetime:
            try:
                return pd.to_datetime(series)
            except (TypeError, ValueError) as _e:
                self.logger(f'Unable to cast {series.name}: {series.dtype} -> {dtype}: {_e}')
                return series.astype(str)
        error = ''
        for _dtype in [int, float]:
            try:
                return series.astype(_dtype)
            except (TypeError, ValueError) as _e:
                error = _e
                continue
        self.logger(f'Unable to cast <{series.name}: {series.dtype} -> numeric>, {error}')
        return series.astype(str)

    def is_buildable(self) -> bool:
        if HOST == 'github_action':
            if self.td.closed != self.td.clock("%Y%m%d"):
                return False
            clock = self.td.clock()
            while (clock.hour == 15) and (15 <= clock.minute < 31):
                time.sleep(30)
                clock = self.td.clock()
            return True
        return True

    def build(self, *jobs):
        if not self.is_buildable():
            raise SystemExit

        jobs = list(jobs)
        if not jobs:
            jobs = ['aftermarket', 'wics', 'numbers']
        else:
            jobs = [job.lower() for job in jobs]

        self.logger(f'[BUILD BASELINE] @{self.td.closed}')
        if not HOST == 'hkefico':

            if (not self.market.date == self.td.closed == self.dates.baseline.date) and \
               ('aftermarket' in jobs):
                try:
                    self.market.fetch()
                    self.market.to_parquet(PATH.PARQUET.AFTERMARKET, engine='pyarrow')
                    self.dates.aftermarket.date = str(self.market.date)
                except (ConnectionError, IndexError, KeyError, Exception) as e:
                    self.logger(f'>>> FAILED TO BUILD AFTER MARKET: {e}')

            if (not self.sector.date == self.sector.server_date == self.dates.wics.date) and \
               (not HOST == 'github_action') and \
               ('wics' in jobs):
                try:
                    self.sector.fetch()
                    self.sector.to_parquet(PATH.PARQUET.WICS, engine='pyarrow')
                    self.dates.wics.date = str(self.sector.date)
                except (ConnectionError, IndexError, KeyError, Exception) as e:
                    self.logger(f'>>> FAILED TO BUILD SECTOR: {e}')

            if (not self.number.date == self.number.server_date == self.dates.numbers.date) and \
               ('numbers' in jobs):
                base = self.market[self.market['marketCap'] >= self.market['marketCap'].median()]
                try:
                    self.number.fetch(*base.index)
                    for col in self.number.columns:
                        if col in ['sharesOutstanding', 'sharesPreferred', 'sharesFloating']:
                            self.number[col] = self.number[col].fillna(0).infer_objects(copy=False)

                        if BASELINE[col].data_type == str:
                            self.number[col] = self.number[col].astype(str)
                            continue

                        self.number[col] = self._typecast(self.number[col])
                    self.number.to_parquet(PATH.PARQUET.NUMBERS, engine='pyarrow')
                    self.dates.numbers.date = str(self.number.date)
                except (ConnectionError, IndexError, KeyError, Exception) as e:
                    self.logger(f'>>> FAILED TO BUILD NUMBERS: {e}')

        self._capture_baseline(self.sector, self.market, self.number)
        self.to_parquet(PATH.PARQUET.BASELINE, engine='pyarrow')
        self.to_csv(PATH.CSV.BASELINE, encoding='utf-8', index=True)

        self.dates.baseline.date = self.td.closed
        with open(PATH.JSON.BUILD, 'w', encoding='utf-8') as f:
            json.dump(self.dates, f, ensure_ascii=False, indent=4)
        return



if __name__ == "__main__":
    baseline = Baseline()
    # print(baseline)
    # print(baseline.columns)
    # baseline.build()
    # print(baseline)
    print(baseline.dates)
    # print(baseline.logger)
    print(baseline.number.date)
    # baseline.to_excel(PATH.DOWNLOADS / 'baseline.xlsx')
