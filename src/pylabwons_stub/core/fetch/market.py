from pylabwons_stub.env import PATH
from pylabwons_stub.schema.dataframe import DataFrameHeir
from pylabwons_stub.schema import market as SCHEMA
from pylabwons import TradingDate
from datetime import datetime
from pandas import DataFrame, Series, MultiIndex
from pykrx import stock
import pandas as pd
import requests, io, time


class Market(DataFrameHeir):
    _metadata = ['lap', 'logger', 'td']

    def __init__(self, src: str = SCHEMA.MARKET, **kwargs):
        super().__init__(src, **kwargs)
        
        self.logger = kwargs.get('logger', print)
        self.td = kwargs.get('td', TradingDate())
        self.lap = self.td.clock("%Y%m%d %H:%M")
        return

    def __call__(self, _name:str, _func, **_kwargs) -> DataFrame:
        try:
            self.logger(f'>>> [{_name}]', end=' ... ')
            df = _func(**_kwargs)
            self.logger('OK')
            return df
        except (KeyError, IndexError, Exception) as _e:
            self.logger(f'NG: {_e}')
            raise ConnectionError(f'FETCH FAILED')

    def fetch(self):
        tic = time.perf_counter()
        self.lap = self.td.clock('%Y%m%d %H:%M') if self.td.is_open() else f'{self.td.closed} 15:30'
        self.logger(f'FETCH MARKET DATA ON {self.lap}')

        caps = self("MARKET CAP", self.fetch_market_cap, date=self.td.latest)
        objs = [self("GENERAL INFO", self.fetch_general), caps,
                self("FOREIGN RATE", self.fetch_foreign_rate, date=self.td.latest),
                self("MARKET TYPE", self.fetch_market_cap_type)]

        close = self.fetch_close(caps=caps)
        objs.append(self("MARKET RETURN", self.fetch_returns, close=close))

        try:
            data = pd.concat(objs, axis=1)
            data = data[data['market'].isin(['kosdaq', 'kospi'])]
            data['tradingDate'] = str(self.td.latest)
            super().__init__(data)
        except (KeyError, Exception) as e:
            raise ConnectionError(e)

        self.logger(f'{"." * 30} {len(self)} STOCKS / RUNTIME: {time.perf_counter() - tic:.2f}s')
        return

    def fetch_close(self, caps:DataFrame) -> DataFrame:
        self.logger(f'>>> [MARKET PRICE]')
        basis = caps.copy()
        close = pd.read_parquet(PATH.PARQUET.PRICES, engine='pyarrow')

        td = TradingDate()
        td.closed = self.td.latest
        r_columns = [td.closed] + [td - n for n in SCHEMA.YIELD_DAYS.values()]
        c_columns = sorted(close.columns.levels[0].tolist(), reverse=True)
        if r_columns == c_columns:
            self.logger(f'>>> | UPDATE LATEST: {c_columns}')
            basis.columns = MultiIndex.from_tuples([(td.closed, c) for c in basis])
            close.update(basis)
            return close

        # 기본 데이터 설정
        basis['calc'] = 'close'

        objs = {td.closed: basis}
        for dt in r_columns[1:]:
            objs[dt] = self.fetch_market_cap(date=dt)
        data = pd.concat(objs, axis=1)
        data = data[data.index.isin(basis.index) & (data[(td.closed, 'volume')] > 0)]

        n_far = list(SCHEMA.YIELD_DAYS.values())[-1]
        shares = data[(td.closed, 'shares')] / data[(td - n_far, 'shares')] - 1
        shares_diff = shares[shares.abs() >= 0.01].index
        sized_diff = basis[basis.index.isin(shares_diff) & (basis.marketCap < basis.marketCap.median())].index
        update_diff = basis[basis.index.isin(shares_diff) & (basis.marketCap >= basis.marketCap.median())].index
        data.loc[sized_diff, (td.closed, 'calc')] = 'marketCap'

        times = [datetime.strptime(td - n, '%Y%m%d') for n in SCHEMA.YIELD_DAYS.values()]
        close_objs = {}
        for ticker in update_diff:
            close_objs[ticker] = stock.get_market_ohlcv_by_date(fromdate=td - n_far, todate=td.closed, ticker=ticker)['종가']
        close: DataFrame = pd.concat(close_objs, axis=1).reindex(times, method='ffill').T
        close.columns = MultiIndex.from_tuples([(td - n, 'close') for n in SCHEMA.YIELD_DAYS.values()])
        data.update(close)
        data.to_parquet(PATH.PARQUET.PRICES, engine='pyarrow')
        self.logger(f'>>> | UPDATE ALL: {data.columns.levels[0].tolist()}')
        return data

    @staticmethod
    def fetch_market_cap(date) -> DataFrame:
        data = stock.get_market_cap_by_ticker(date=date, market='ALL').astype('Int64')
        return data.rename(columns=SCHEMA.MARKET_CAP)[SCHEMA.MARKET_CAP.values()]

    @staticmethod
    def fetch_market_cap_type() -> Series:
        ks200 = Series(index=stock.get_index_portfolio_deposit_file('2203')).fillna('kospi200')
        kq150 = Series(index=stock.get_index_portfolio_deposit_file('1028')).fillna('kosdaq150')
        data = pd.concat([ks200, kq150], axis=0)
        data.name = 'groupByMarketCap'
        return data

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
    def fetch_returns(close: DataFrame) -> DataFrame:
        dates = close.columns.levels[0].unique().sort_values(ascending=False)
        base = close[dates[0]].copy()

        by_price = pd.concat({date: close[(date, 'close')] for date in dates}, axis=1) \
                   .rename(columns=dict(zip(dates, ['D0'] + list(SCHEMA.YIELD_DAYS.keys()))))
        by_price = by_price[by_price.index.isin(base[base['calc'] == 'close'].index)]
        by_cap = pd.concat({date: close[(date, 'marketCap')] for date in dates}, axis=1) \
                 .rename(columns=dict(zip(dates, ['D0'] + list(SCHEMA.YIELD_DAYS.keys()))))
        by_cap = by_cap[by_cap.index.isin(base[base['calc'] == 'marketCap'].index)]

        by_price = pd.concat({col: (by_price['D0'] - 1) / by_price[col] for col in by_price}, axis=1)
        by_cap = pd.concat({col: (by_cap['D0'] - 1) / by_cap[col] for col in by_cap}, axis=1)

        return round(100 * pd.concat([by_price, by_cap], axis=0).drop(columns=['D0']), 2)

    @property
    def date(self) -> str:
        return self['tradingDate'].unique()[0]



if __name__ == '__main__':
    # market = Market()
    # market.fetch()
    # print(market)
    # print(market.tradingDate)
    # print(market.logger)

    print(pd.read_parquet(PATH.PARQUET.PRICES, engine='pyarrow'))