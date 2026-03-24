from pylabwons_stub.env import PATH
from pylabwons_stub.core.build.baseline import Baseline
from pylabwons_stub.schema.const.baseline import BASELINE
from pylabwons_stub.schema.const.bubble import BUBBLE
from pylabwons_stub.utils import tools
from datetime import datetime
from pandas import DataFrame
from jinja2 import Environment, FileSystemLoader
from json import dumps
from typing import Callable, Dict, Hashable, List
import numpy as np
import pandas as pd
import plotly.graph_objs as go
import pylabwons as lw


class MarketBubble(Baseline):

    def __init__(self, logger:Callable=print):
        super().__init__(logger)
        self.logger(f'DEPLOY MARKET MAP ON {self.log.baseline.date}')

        self._extract()
        return

    @classmethod
    def __norm(cls, data):
        return (data - data.min()) / (data.max() - data.min())

    def _extract(self):
        # SELECT ONLY KOSPI200 AND KOSDAQ150, TOTAL 350 TICKERS
        self.drop(inplace=True, index=self[self['groupByMarketCap'].isna()].index)

        # CALC AND PROCESS
        self['name'] = self[['name', 'market']].apply(lambda r: f'{r[0]}*' if r[1] == 'kosdaq' else r[0], axis=1, raw=True)
        self['size'] = np.power(self.__norm(np.log10(self['marketCap'])), 1.5) * 10
        self['meta'] = self['name'] + '(' + self.index + ')<br>' \
                     + '시가총액: ' + self['marketCap'].apply(tools.int2krw) + '원<br>' \
                     + '종가: ' + self['close'].apply(lambda x: f'{x:,d}원')
        keys = list(BUBBLE.keys()) + \
               ['name', 'size', 'ceil', 'meta',
                'industryName', 'industryCode', 'sectorName', 'sectorCode']
        self.drop(
            inplace=True,
            columns=[c for c in self.columns if not c in keys]
        )
        for c in self.columns:
            if c.lower().endswith('pe'):
                self[c] = self[c].apply(lambda x: np.nan if x < 0 else x)
        return


    @property
    def metadata(self) -> Dict:
        def _rgb2hex(r, g, b):
            return f'#{hex(int(r))[2:]}{hex(int(g))[2:]}{hex(int(b))[2:]}'

        meta = {}
        for key, _meta in BUBBLE.items():
            meta[key] = {
                'label': BASELINE[key].kor_name,
                'unit': BASELINE[key].unit,
                'scale': _meta.scale,
                'color': [_rgb2hex(*rgb) for rgb in COLORS[_meta.color]],
            }
        return meta

    def deploy(self):
        date = datetime.strptime(self.log.baseline.date, "%Y%m%d %H:%M")
        with open(file=PATH.HTML.MARKETMAP, mode='w', encoding='utf-8') as file:
            file.write(
                Environment(loader=FileSystemLoader(PATH.HTML.TEMPLATE)) \
                .get_template('marketmap-1.0.0.html') \
                .render({
                    "title": "LAB￦ONS: \uc2dc\uc7a5\uc9c0\ub3c4",
                    "tradingDate": date.strftime("%Y/%m/%d %H:%M"),
                    "statusValue": self.stat.to_dict(),
                    "srcTicker": self.to_json(orient='index'),
                    "srcIndicatorOpt": dumps(self.metadata),
                })
            )
        return

    def test_plot(
        self,
        x:str = 'returnOn1Day',
        y:str = 'returnOn1Month',
    ):
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=self[x],
            y=self[y],
            mode='markers',
            marker={
                'size': self['size'],
            },
            meta=self['meta'],

            # text=base[col].astype(str) + '%',
            # textposition='middle center',
            # textfont={
            #     'color':'#ffffff'
            # },
            # opacity=0.85,
            # pathbar={
            #     'visible': False,
            # },
            # marker={
            #     'colors':base[f'{col}_c']
            # },
            # meta=base['meta'] + '<br>' + base[col].astype(str) + '%',
            hovertemplate='%{meta}<extra></extra>',
        ))
        fig.show('browser')
        return


if __name__ == "__main__":
    bub = MarketBubble()
    bub.test_plot()

