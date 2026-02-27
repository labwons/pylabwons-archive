from scripts.src.schema.dataframe import DataFrameHeir
from scripts.src.schema import market as SCHEMA
from pylabwons import TradingDate
from datetime import datetime
from pandas import DataFrame, Series
from pykrx import stock
import pandas as pd
import requests, io, time


class AfterMarket(DataFrameHeir):

    _metadata = ['logger']

    def __init__(self, src:str=SCHEMA.AFTERMARKET, **kwargs):
        super().__init__(src, **kwargs)
        self.logger = kwargs.get('logger', print)
        return

    def fetch(self):
        tic = time.perf_counter()
        td = TradingDate()
        self.logger(f'FETCH AFTER MARKET DATA ON {td.closed}')
        try:
            data = pd.concat([
                self._fetch_general(),
                self._fetch_market_cap(date=td.closed),
                self._fetch_foreign_rate(date=td.closed),
                self._fetch_market_cap_type(),
            ], axis=1)

            data = data[data['market'].isin(['kosdaq', 'kospi'])]

            data = data.join(self._fetch_returns(td, data), how='left')
            data['tradingDate'] = str(td.closed)

            super().__init__(data)
            self.logger(f'{"." * 30} {len(self)} STOCKS / RUNTIME: {time.perf_counter() - tic:.2f}s')
        except (KeyError, Exception) as e:
            self.logger(f'FAILED FETCHING: {e} / RUNTIME: {time.perf_counter() - tic:.2f}s')
            raise ConnectionError(e)
        return

    @property
    def date(self) -> str:
        return self['tradingDate'].unique()[0]

    @SCHEMA.marketfetch("MARKET CAP")
    def _fetch_market_cap(self, date:str) -> DataFrame:
        data = stock.get_market_cap_by_ticker(date=date, market='ALL').astype('float64')
        data.rename(columns=SCHEMA.MARKET_CAP, inplace=True)
        return data[SCHEMA.MARKET_CAP.values()]

    @SCHEMA.marketfetch("FOREIGN RATE")
    def _fetch_foreign_rate(self, date:str) -> DataFrame:
        data = stock.get_exhaustion_rates_of_foreign_investment(date=date, market='ALL').astype('float64')
        data.rename(columns=SCHEMA.FOREIGN_RATE, inplace=True)
        return data[SCHEMA.FOREIGN_RATE.values()]

    @SCHEMA.marketfetch("GENERAL INFO")
    def _fetch_general(self) -> DataFrame:
        resp = requests.get(SCHEMA.KRX_GENERAL).text
        data = pd.read_html(io=io.StringIO(resp), encoding='euc-kr')[0].set_index(keys='종목코드')
        data.index = data.index.astype(str).str.zfill(6)
        data.index.name = 'ticker'
        data.rename(columns=SCHEMA.GENERAL, inplace=True)
        data['market'] = data['market'].replace('코스닥', 'kosdaq').replace('유가', 'kospi')
        return data[SCHEMA.GENERAL.values()]

    @SCHEMA.marketfetch("MARKET CAP TYPE")
    def _fetch_market_cap_type(self) -> Series:
        ks200 = Series(index=stock.get_index_portfolio_deposit_file('2203')).fillna('kospi200')
        kq150 = Series(index=stock.get_index_portfolio_deposit_file('1028')).fillna('kosdaq150')
        data = pd.concat([ks200, kq150], axis=0)
        data.name = 'groupByMarketCap'
        return data

    @SCHEMA.marketfetch("RETURNS")
    def _fetch_returns(self, td:TradingDate, base:DataFrame) -> DataFrame:
        base = base[base['volume'] != 0]

        objs = {'D+0': stock.get_market_cap_by_ticker(date=td.closed, market='ALL')}
        for col, n in SCHEMA.YIELD_DAYS.items():
            objs[col] = stock.get_market_cap_by_ticker(date=td - n, market='ALL')
        data = pd.concat(objs, axis=1)
        base = data[data.index.isin(base.index)]

        # 기본 수익률 계산
        returns = pd.concat({dt: base['D+0']['종가'] / base[dt]['종가'] - 1 for dt in objs}, axis=1) \
                  .drop(columns=['D+0'])

        # 상장 주식수 변화 감지: -1Y 대비 1% 이상 변화 종목 대상
        shares_changed = base['D+0']['상장주식수'] / base['returnOn1Year']['상장주식수'] - 1
        diff = shares_changed[shares_changed.abs() >= 0.01].index
        diff_new = diff[diff.isin(base[base[('D+0', '시가총액')] >= base[('D+0', '시가총액')].median()].index)]
        diff_cap = diff[~diff.isin(diff_new)]

        # 상장 주식수 변화 대상 중 주요 종목은 종가 기준 수익률 재계산
        close_objs = {}
        for ticker in diff_new:
            close_objs[ticker] = stock.get_market_ohlcv_by_date(fromdate=td - 365, todate=td.closed, ticker=ticker)['종가']
        close: DataFrame = pd.concat(close_objs, axis=1)
        times = [datetime.strptime(td - n, '%Y%m%d') for n in SCHEMA.YIELD_DAYS.values()]
        close = close.reindex(times + [close.index[-1]], method='ffill')
        return_by_close = ((close.iloc[-1] / close) - 1).iloc[:-1].T
        return_by_close.columns = returns.columns
        returns.update(return_by_close)

        # 상장 주식수 변화 대상 중 비 주류 종목은 시가총액 기준 수익률 재계산 (속도 향상 목적)
        return_by_cap: DataFrame = pd.concat({
            dt: base['D+0']['시가총액'] / base[dt]['시가총액'] - 1 for dt in objs
        }, axis=1)
        return_by_cap.drop(columns=['D+0'], inplace=True)
        return_by_cap = return_by_cap.loc[diff_cap]
        returns.update(return_by_cap)

        return round(100 * returns, 2)



if __name__ == '__main__':
    market = AfterMarket()
    # market.fetch()
    print(market)
    # print(market.tradingDate)
    # print(market.logger)