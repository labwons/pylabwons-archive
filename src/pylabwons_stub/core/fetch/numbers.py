from functools import cached_property
from pylabwons_stub.schema.dataframe import DataFrameHeir
from pylabwons_stub.schema import market as SCHEMA
from pylabwons import FnGuide
from tqdm import auto
import pandas as pd
import time


class Numbers(DataFrameHeir):

    _metadata = ['logger', 'progress_bar']

    def __init__(self, src:str=SCHEMA.NUMBERS, **kwargs):
        super().__init__(src, **kwargs)
        self.logger = kwargs.get('logger', print)
        self.progress_bar:bool = kwargs.get('progress_bar', True)
        return

    def fetch(self, *tickers:str):
        tic = time.perf_counter()
        obj = FnGuide(tickers[0])
        self.logger(f'FETCH MARKET NUMBERS @{obj.date}')

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
                self.logger(f">>> Error while fetching: {ticker} / {e}")
                continue
            if n and n % 50 == 0:
                time.sleep(3)
        super().__init__(pd.concat(objs, axis=1).T)
        self.logger(f'{"." * 30} {len(self)} STOCKS / RUNTIME: {time.perf_counter() - tic:.2f}s')
        return

    @property
    def date(self) -> str:
        return self['numbersDate'].unique()[0]

    @cached_property
    def server_date(self) -> str:
        return FnGuide('005930').date


if __name__ == '__main__':
    numbers = Numbers()
    print(numbers)