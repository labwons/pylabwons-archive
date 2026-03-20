from pylabwons_stub.schema.dataframe import DataFrameHeir
from pylabwons_stub.schema import market as SCHEMA
from pylabwons import FnGuide
from tqdm import auto
import numpy as np
import pandas as pd
import time


class Number(DataFrameHeir):

    _metadata = ['logger', 'progress_bar']

    def __init__(self, src:str=SCHEMA.NUMBER, **kwargs):
        super().__init__(src, **kwargs)
        self.logger = kwargs.get('logger', print)
        self.progress_bar:bool = kwargs.get('progress_bar', True)
        try:
            self.server_date = FnGuide('005930').date
        except (ConnectionError, IndexError, ValueError, Exception):
            self.server_date = 'failed'
        return

    def _typecast(self):
        for col in self.columns:
            self[col] = self[col].apply(lambda x: np.nan if str(x) == 'nan' else x)
            try:
                if self[col].dtype == 'object':
                    self[col] = self[col].astype(str)
                if self[col].dtype == int:
                    self[col] = pd.to_numeric(self[col], errors='coerce')
                    self[col] = self[col].astype('Int64')
                else:
                    self[col] = pd.to_numeric(self[col])
            except (ValueError, TypeError, Exception):
                self[col] = self[col].astype(str)
        return

    def fetch(self, *tickers:str):
        tic = time.perf_counter()
        obj = FnGuide(tickers[0])
        self.logger(f'FETCH MARKET NUMBERS ON {obj.date}')

        if self.progress_bar:
            loop = auto.tqdm(enumerate(tickers[1:]))
        else:
            loop = enumerate(tickers[1:])

        objs = [obj.numbers]
        for n, ticker in loop:
            obj = FnGuide(ticker)
            try:
                objs.append(obj.numbers)
            except Exception as e:
                self.logger(f">>> FAILED TO FETCH: {ticker} / {e}")
                continue
            if n and n % 50 == 0:
                time.sleep(3)

        super().__init__(pd.concat(objs, axis=1).T)
        self._typecast()
        self.logger(f'{"." * 30} {len(self)} STOCKS / RUNTIME: {time.perf_counter() - tic:.2f}s')
        return

    @property
    def date(self) -> str:
        return str(int(self['numbersDate'].unique()[0]))


if __name__ == '__main__':
    numbers = Number()
    print(numbers['numbersDate'].unique()[0])
    print(numbers.date)