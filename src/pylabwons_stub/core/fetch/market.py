from pylabwons_stub.schema.dataframe import DataFrameHeir
from pylabwons_stub.schema import market as SCHEMA
from pylabwons import TradingDate
from datetime import datetime
from pandas import DataFrame, Series, MultiIndex
from pykrx import stock
import pandas as pd
import requests, io, time


class Market(DataFrameHeir):
    _metadata = ['logger', 'td']

    def __init__(self, src: str = SCHEMA.MARKET, **kwargs):
        super().__init__(src, **kwargs)
        self.logger = kwargs.get('logger', print)
        self.td = TradingDate()
        return

    def fetch(self, close: DataFrame = DataFrame()):

        def _fetch(_name: str, _objs, _func, **_kwargs):
            try:
                self.logger(f'>>> [{_name}]', end=' ... ')
                _objs.append(_func(**_kwargs))
                self.logger('OK')
            except (KeyError, IndexError, Exception) as _e:
                self.logger(f'NG: {_e}')
                raise ConnectionError(f'FETCH FAILED')

        tic = time.perf_counter()
        self.logger(f'FETCH AFTER MARKET DATA ON {self.td.closed}')

        objs = []
        for name, func, kwargs in [
            ('GENERAL INFO', self.fetch_general, dict()),
            ('MARKET CAP', self.fetch_market_cap, dict(date=self.td.closed)),
            ('FOREIGN RATE', self.fetch_foreign_rate, dict(date=self.td.closed)),
            ('MARKET TYPE', self.fetch_market_cap_type, dict())
        ]:
            _fetch(name, objs, func, **kwargs)

        try:
            data = pd.concat(objs, axis=1)
            data = data[data['market'].isin(['kosdaq', 'kospi'])]
            # data = data.join(self.fetch_returns(td, data), how='left')
            data['tradingDate'] = str(self.td.closed)

            super().__init__(data)
            self.logger(f'{"." * 30} {len(self)} STOCKS / RUNTIME: {time.perf_counter() - tic:.2f}s')
        except (KeyError, Exception) as e:
            raise ConnectionError(e)
        return

    def fetch_close(self, *tickers) -> DataFrame:
        td = TradingDate()
        td.closed = self.td.latest

        # 기본 데이터 설정
        base = self.fetch_market_cap(date=td.closed)
        base['calc'] = 'close'

        objs = {td.closed: base}
        for n in SCHEMA.YIELD_DAYS.values():
            objs[td - n] = self.fetch_market_cap(date=td - n)
        data = pd.concat(objs, axis=1)
        data = data[data.index.isin(base.index) & (data[(td.closed, 'volume')] > 0)]
        if tickers:
            data = data[data.index.isin(tickers)]

        N = list(SCHEMA.YIELD_DAYS.values())[-1]
        shares = data[(td.closed, 'shares')] / data[(td - N, 'shares')] - 1
        shares_diff = shares[shares.abs() >= 0.01].index
        sized_diff = base[base.index.isin(shares_diff) & (base.marketCap < base.marketCap.median())].index
        update_diff = base[base.index.isin(shares_diff) & (base.marketCap >= base.marketCap.median())].index
        data.loc[sized_diff, (td.closed, 'calc')] = 'marketCap'

        times = [datetime.strptime(td - n, '%Y%m%d') for n in SCHEMA.YIELD_DAYS.values()]
        close_objs = {}
        for ticker in update_diff:
            close_objs[ticker] = stock.get_market_ohlcv_by_date(fromdate=td - N, todate=td.closed, ticker=ticker)['종가']
        close: DataFrame = pd.concat(close_objs, axis=1).reindex(times, method='ffill').T
        close.columns = MultiIndex.from_tuples([(td - n, 'close') for n in SCHEMA.YIELD_DAYS.values()])
        data.update(close)
        return data

    @staticmethod
    def fetch_market_cap(date) -> DataFrame:
        data = stock.get_market_cap_by_ticker(date=date, market='ALL').astype('Int64')
        return data.rename(columns=SCHEMA.MARKET_CAP)[SCHEMA.MARKET_CAP.values()]

    @staticmethod
    def fetch_foreign_rate(date: str) -> DataFrame:
        data = stock.get_exhaustion_rates_of_foreign_investment(date=date, market='ALL').astype('float64')
        return data.rename(columns=SCHEMA.FOREIGN_RATE)[SCHEMA.FOREIGN_RATE.values()]

    @staticmethod
    def fetch_general() -> DataFrame:
        resp = requests.get(SCHEMA.KRX_GENERAL).text
        data = pd.read_html(io=io.StringIO(resp), encoding='euc-kr')[0].set_index(keys='종목코드')
        data.index = data.index.astype(str).str.zfill(6)
        data.index.name = 'ticker'
        data.rename(columns=SCHEMA.GENERAL, inplace=True)
        data['market'] = data['market'].replace('코스닥', 'kosdaq').replace('유가', 'kospi')
        return data[SCHEMA.GENERAL.values()]

    @staticmethod
    def fetch_market_cap_type() -> Series:
        ks200 = Series(index=stock.get_index_portfolio_deposit_file('2203')).fillna('kospi200')
        kq150 = Series(index=stock.get_index_portfolio_deposit_file('1028')).fillna('kosdaq150')
        data = pd.concat([ks200, kq150], axis=0)
        data.name = 'groupByMarketCap'
        return data

    @property
    def date(self) -> str:
        return self['tradingDate'].unique()[0]

    @staticmethod
    def fetch_returns(td: TradingDate, base: DataFrame) -> DataFrame:
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
            close_objs[ticker] = stock.get_market_ohlcv_by_date(fromdate=td - 365, todate=td.closed, ticker=ticker)[
                '종가']
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
    market = Market()
    # market.fetch()
    print(market)
    # print(market.tradingDate)
    # print(market.logger)