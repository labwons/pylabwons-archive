from scripts.src.schema import market as SCHEMA
from scripts.src.schema.dataframe import DataFrameHeir
from pandas import DataFrame
from typing import Callable
import pandas as pd
import re, requests, time


class Wics(DataFrameHeir):
    # WISE INDUSTRY CLASSIFICATION SYSTEM
    _metadata = ['logger']

    def __init__(self, src:str=SCHEMA.WICS, **kwargs):
        super().__init__(src, **kwargs)
        self.logger = kwargs.get('logger', print)
        return

    def fetch(self):
        tic = time.perf_counter()
        try:
            date = self._fetch_date()
            self.logger(f'FETCH WICS ON {date}')

            objs = []
            for n, (code, name) in enumerate(SCHEMA.CODES.items(), start=1):
                self.logger(f'>>> [{n}/{len(SCHEMA.CODES)}]{name}@{code}', end=' ... ')
                obj = self._fetch_group(code, date, logger=self.logger)
                if obj.empty:
                    raise ConnectionError(f'Failed to fetch {code} / {name}')
                objs.append(obj)

            reits = DataFrame(data={'CMP_KOR': SCHEMA.REITS.values(), 'CMP_CD': SCHEMA.REITS.keys()})
            reits[['SEC_CD', 'IDX_CD', 'SEC_NM_KOR', 'IDX_NM_KOR']] = ['G99', 'WI999', '리츠', '리츠']
            objs.append(reits)

            data:DataFrame = pd.concat(objs, axis=0, ignore_index=True)
            data.drop(inplace=True, columns=[key for key in data if not key in SCHEMA.LABELS])
            data.drop(inplace=True, index=data[data['SEC_CD'].isna()].index)
            data.rename(inplace=True, columns=SCHEMA.LABELS)
            data.set_index(inplace=True, keys="ticker")
            data['industryName'] = data['industryName'].str.replace("WI26 ", "")

            sc_mdi = data[(data['industryCode'] == 'WI330') & (data['sectorCode'] == 'G50')].index
            sc_edu = data[(data['industryCode'] == 'WI330') & (data['sectorCode'] == 'G25')].index
            sc_sw = data[(data['industryCode'] == 'WI600') & (data['sectorCode'] == 'G50')].index
            sc_it = data[(data['industryCode'] == 'WI600') & (data['sectorCode'] == 'G45')].index
            data.loc[sc_mdi, 'industryCode'], data.loc[sc_mdi, 'industryName'] = 'WI331', '미디어'
            data.loc[sc_edu, 'industryCode'], data.loc[sc_edu, 'industryName'] = 'WI332', '교육'
            data.loc[sc_sw, 'industryCode'], data.loc[sc_sw, 'industryName'] = 'WI601', '소프트웨어'
            data.loc[sc_it, 'industryCode'], data.loc[sc_it, 'industryName'] = 'WI602', 'IT서비스'

            adder = {}
            for key in SCHEMA.EXCEPTIONS:
                if not key in data.index:
                    adder[key] = SCHEMA.EXCEPTIONS[key]
            exceptions = DataFrame(adder).T
            data = pd.concat(objs=[data, exceptions], axis=0)
            data['wicsDate'] = date
            self.logger(f'{"." * 30} {len(data)} STOCKS / RUNTIME: {time.perf_counter() -  tic:.2f}s')
            super().__init__(data)
        except (ConnectionError, Exception, TimeoutError) as reason:
            self.logger(f'FAILED TO FETCH WICS: {reason} / RUNTIME: {time.perf_counter() -  tic:.2f}s')
            raise ConnectionError(reason)
        return

    @property
    def date(self) -> str:
        return self['wicsDate'].unique()[0]

    @staticmethod
    def _fetch_date() -> str:
        return re.compile(r"var\s+dt\s*=\s*'(\d{8})'") \
            .search(requests.get(SCHEMA.URL.BASE).text) \
            .group(1)

    @staticmethod
    def _fetch_group(code: str, date: str = "", countdown: int = 5, logger: Callable=print) -> DataFrame:
        try:
            resp = requests.get(SCHEMA.URL.SECTOR(date, code))
        except Exception as reason:
            logger(f'NG: {reason}')
            return DataFrame()

        if not resp.status_code == 200:
            if countdown == 0:
                logger(f'NG: TIMEOUT / {resp.status_code}')
                return DataFrame()
            else:
                time.sleep(5)
                return WICS._fetch_group(code, date, countdown - 1)
        if "hmg-corp" in resp.text:
            logger(f'NG: BLOCKED')
            return DataFrame()
        logger(f'OK')
        return DataFrame(resp.json()['list'])


if __name__ == "__main__":
    wics = WICS()
    print(wics)