from pylabwons_stub.schema.dataframe import DataFrameHeir
from pylabwons_stub.schema.const.baseline import BASELINE
from pylabwons_stub.core.fetch.market import Market
from pylabwons_stub.core.fetch.number import Number
from pylabwons_stub.core.fetch.sector import Sector
from pylabwons_stub.env import HOST, PATH, RUNTIME
from datetime import datetime
from typing import Callable, List
import numpy as np
import pandas as pd
import pylabwons as lw
import json, os, time
pd.set_option('display.expand_frame_repr', False)
pd.set_option('future.no_silent_downcasting', True)


class Baseline(DataFrameHeir):

    _metadata = ['logger', 'log', 'td',
                 'market', 'number', 'sector']

    def __init__(self, logger:Callable=print):
        self.logger = logger
        self.td = lw.TradingDate()
        with open(PATH.JSON.BUILD, 'r', encoding='utf-8') as f:
            self.log = lw.DataDictionary(json.load(f))

        self.market = Market(src=PATH.PARQUET.MARKET, logger=logger, td=self.td)
        self.number = Number(src=PATH.PARQUET.NUMBER, logger=logger)
        self.sector = Sector(src=PATH.PARQUET.SECTOR, logger=logger)

        try:
            super().__init__(pd.read_parquet(PATH.PARQUET.BASELINE, engine='pyarrow'))
            return
        except (ConnectionError, FileNotFoundError, IndexError, Exception):
            self._capture_baseline(
            PATH.PARQUET.SECTOR,
                PATH.PARQUET.MARKET,
                PATH.PARQUET.NUMBER
            )
        return

    def _capture_baseline(self, *args):
        self.logger(f'CAPTURE NEW BASELINE ON {self.log.baseline.date}')
        super().__init__(*args, method='join')

        self._typecast()
        self['fiftyTwoWeekHigh'] = np.fmax(self['close'], self['fiftyTwoWeekHigh'].fillna(self['close']))
        self['fiftyTwoWeekLow'] = np.fmin(self['close'], self['fiftyTwoWeekLow'].fillna(self['close']))
        self['fiftyTwoWeekHighPct'] = round(100 * (self['close'] / self['fiftyTwoWeekHigh'] - 1), 2)
        self['fiftyTwoWeekLowPct'] = round(100 * (self['close'] / self['fiftyTwoWeekLow'] - 1), 2)
        self['targetPricePct'] = round(100 * (self['close'] / self['targetPrice'] - 1), 2)
        self['estimatedPe'] = round(self['close'] / self['estimatedEps'], 2)
        self['forwardPe'] = round(self['close'] / self['forwardEps'], 2)
        self['trailingPe'] = round(self['close'] / self['trailingEps'], 2)
        self['trailingPs'] = round((self['marketCap'] / 1e+8) / self['trailingRevenue'], 2)
        self.sort_values(by='marketCap', ascending=False, inplace=True)

        super().__init__(self[BASELINE.keys()])
        return

    def _typecast(self):
        for col in self.columns:
            meta = BASELINE[col]
            if meta.data_type == str:
                continue

            self[col] = self[col].apply(lambda x: np.nan if str(x) == 'nan' else x)
            if col == 'ipo':
                self[col] = self[col].astype(str).str.replace('-', '')
            try:
                if meta.data_type == int:
                    self[col] = pd.to_numeric(self[col], errors='coerce')
                    self[col] = self[col].astype('Int64')
                else:
                    self[col] = pd.to_numeric(self[col])
            except (ValueError, TypeError, Exception) as e:
                self[col] = self[col].astype(str)
                self.logger(f'>>> Unable to cast <{col}: {self[col].dtype} -> numeric>, {e}')

            if meta.data_type == self[col].dtype == float:
                self[col] = round(self[col], BASELINE[col].round)
        return

    def get_tickets(self, *tickets) -> List[str]:
        if tickets:
            return list(tickets)

        if HOST == 'hkefico':
            return []

        tickets = []
        if self.td.is_open():
            tickets.append("market")
        else:
            log_date = datetime \
                       .strptime(self.log.market.date, "%Y%m%d %H:%M") \
                       .strftime("%Y%m%d")
            if not self.market.date == self.td.closed == log_date:
                tickets.append('market')

        if not self.sector.date == self.sector.server_date == self.log.sector.date:
            tickets.append('sector')

        if not self.number.server_date in ['failed', '']:
            if not self.number.date == self.number.server_date == self.log.number.date:
                tickets.append('number')

        if HOST == 'github_action':
            if self.td.is_open() or int(self.td.clock().hour) < 19:
                if 'sector' in tickets:
                    tickets.remove('sector')
                if 'number' in tickets:
                    tickets.remove('number')
            if not self.td.is_open() and int(self.td.clock().hour) >= 17:
                if 'market' in tickets:
                    tickets.remove('market')
        return tickets

    def build(self, *tickets):
        tickets = self.get_tickets(*tickets)
        self.logger(f'[BUILD BASELINE]')
        self.logger(f'| TRADING DATE(LATEST): {self.td.latest}')
        self.logger(f'| TRADING DATE(CLOSED): {self.td.closed}')
        self.logger(f'| TICKETS: {"NO TICKETS" if not tickets else tickets}')

        if 'market' in tickets:
            try:            
                self.market.fetch()
                self.market.to_parquet(PATH.PARQUET.MARKET, engine='pyarrow')
                self.log.prices.time = self.market.lap
                if not self.td.is_open():
                    self.log.market.date = f"{self.td.closed} 15:30"
                else:
                    self.log.market.date = self.market.lap
            except (ConnectionError, IndexError, KeyError, Exception) as e:
                self.logger(f'>>> FAILED TO BUILD AFTER MARKET: {e}')

        if 'sector' in tickets:
            try:
                self.sector.fetch()
                self.sector.to_parquet(PATH.PARQUET.SECTOR, engine='pyarrow')
                self.log.sector.date = str(self.sector.date)
            except (ConnectionError, IndexError, KeyError, Exception) as e:
                self.logger(f'>>> FAILED TO BUILD SECTOR: {e}')

        if 'number' in tickets:
            base = self.market[self.market['marketCap'] >= self.market['marketCap'].median()]
            try:
                self.number.fetch(*base.index)
                self.number.to_parquet(PATH.PARQUET.NUMBER, engine='pyarrow')
                self.log.number.date = str(self.number.date)
            except (ConnectionError, IndexError, KeyError, Exception) as e:
                self.logger(f'>>> FAILED TO BUILD NUMBERS: {e}')

        if tickets or (HOST == 'github_action' and RUNTIME == 'workflow_dispatch'):
            self._capture_baseline(self.sector, self.market, self.number)
            self.to_parquet(PATH.PARQUET.BASELINE, engine='pyarrow')
            self.to_parquet(PATH.LOG / f'baseline-{self.td.latest}.parquet', engine='pyarrow')
            self.to_csv(PATH.CSV.BASELINE, encoding='utf-8', index=True)

            self.log.baseline.date = self.log.market.date
            if len(os.listdir(PATH.LOG)) > 20:
                logs = os.listdir(PATH.LOG)
                logs.sort()
                dump = logs[:-20]
                self.logger(f'| SHIFT AND CLEAN BASELINE LOG')
                self.logger(f'>>> DROP: {dump}')
                for f in dump:
                    (PATH.LOG / f).unlink(missing_ok=True)
            self.log.baseline.log = sorted(os.listdir(PATH.LOG))
            with open(PATH.JSON.BUILD, 'w', encoding='utf-8') as f:
                json.dump(self.log, f, ensure_ascii=False, indent=4)

        self.logger('| LOG')
        for key, value in self.log.items():
            self.logger(f'>>> {key}: {value}')
        return



if __name__ == "__main__":
    baseline = Baseline()
    print(baseline.market.date)
    # print(baseline)
    # print(baseline.columns)
    # baseline.build()
    # baseline.to_excel(PATH.DOWNLOADS / 'baseline.xlsx')
    # baseline.market.to_excel(PATH.DOWNLOADS / 'market.xlsx')
    # baseline.number.to_excel(PATH.DOWNLOADS / 'number.xlsx')
    # baseline.sector.to_excel(PATH.DOWNLOADS / 'sector.xlsx')
