from pylabwons_stub.core.build.baseline import Baseline
from pylabwons_stub.schema.key import SCHEMA
from pylabwons_stub.utils import tools
from pandas import DataFrame
from pandas.api.types import is_numeric_dtype
from typing import Callable, Hashable, List
import pandas as pd
import plotly.graph_objs as go
import pylabwons as lw


class MarketMap(Baseline):

    def __init__(self, logger:Callable=print):
        super().__init__(logger)

        self._cleanse()
        self._extract()
        self._stack(by='industryName')
        self._stack(by='industryName', exclude_ticker=['005930'])
        self._stack(by='sectorName')
        self._stack(by='sectorName', exclude_ticker=['005930'])
        return

    def _cleanse(self):
        return

    def _extract(self):
        # SELECT ONLY KOSPI200 AND KOSDAQ150, TOTAL 350 TICKERS
        self.drop(inplace=True, index=self[self['groupByMarketCap'].isna()].index)

        # CALC AND PROCESS
        self['name'] = self[['name', 'market']].apply(lambda r: f'{r[0]}*' if r[1] == 'kosdaq' else r[0], axis=1, raw=True)
        self['size'] = (self['marketCap'] / 1e+8).astype(int)
        self['ceil'] = self['industryName']
        self['meta'] = self['name'] + '(' + self.index + ')<br>' \
                     + '시가총액: ' + self['marketCap'].apply(tools.int2krw) + '원<br>' \
                     + '종가: ' + self['close'].apply(lambda x: f'{x:,d}원')
        self.drop(
            inplace=True,
            columns=[c for c, meta in SCHEMA.items() if not meta.market_map] + ['market', 'marketCap']
        )
        return

    def _stack(self, by:str, exclude_ticker:List=None):

        def __repr(_name:Hashable, df:DataFrame) -> lw.DataDict:
            obj = lw.DataDict()
            obj.ticker = df.iloc[0][by.replace("Name", "Code")].rjust(6, pfix)
            obj.name = _name
            obj.size = df['size'].sum()
            obj.ceil = '대형주' if by.startswith('sector') else df.iloc[0]['sectorName']
            obj.meta = f'{_name}<br>시가총액: {tools.int2krw(obj.size * 1e+8)}원'
            if exclude_ticker:
                obj.ceil += f'({self.loc[exclude_ticker[0], "name"]} 제외)'

            '''
            Default Grouping Factors: 
            Weighted Mean
            '''
            w = df['size'] / obj.size
            for col in df.columns:
                if not is_numeric_dtype(df[col]):
                    continue
                try:
                    obj[col] = (w * df[col]).sum()
                except (ValueError, TypeError) as e:
                    self.logger(f'Error calculating weighted: {col}({df[col].dtype})@{name} / {e}')
            '''
            Exception Grouping Factors:
            Arithmetic Mean
            '''
            # Not Defined Yet
            return obj

        objs = []
        pfix = 'N' if exclude_ticker else 'W'
        dup = self[self['industryName'] == self['sectorName']]['sectorName'].unique()
        for name, group in self.groupby(by=by):
            if by == 'industryName' and name in dup:
                continue
            if exclude_ticker:
                group = group[~group.index.isin(exclude_ticker)]
            objs.append(__repr(name, group))

        ceil = DataFrame(data=objs).set_index(keys='ticker')
        if by == 'sectorName':
            ceil['sectorCode'] = 'NS0000' if exclude_ticker else 'WS0000'
            cover = __repr(objs[-1].ceil, ceil)
            cover.ceil = ''
            objs.append(cover)
            ceil = DataFrame(data=objs).set_index(keys='ticker')

        super(Baseline, self).__init__(pd.concat([self, ceil]))
        return

    def test_plot(
        self,
        exclude_ticker: List = None,
        col:str = 'returnOn1Day',
    ):
        if exclude_ticker:
            base = self[~self.index.str.startswith('W')]
        else:
            base = self[~self.index.str.startswith('N')]
        base = base[['name', 'ceil', 'size', 'meta', col]]

        fig = go.Figure()
        fig.add_trace(go.Treemap(
            branchvalues='total',
            labels=base['name'],
            parents=base['ceil'],
            values=base['size'],
            text=base['meta'] + '<br>' + base[col].astype(str) + '%',
            textposition='middle center',
            textfont={
                'color':'#ffffff'
            },
            opacity=0.85,
            pathbar={
                'visible': False,
            }
        ))
        fig.show('browser')
        return


if __name__ == "__main__":
    mmap = MarketMap()
    # print(mmap)
    # mmap.test_plot()