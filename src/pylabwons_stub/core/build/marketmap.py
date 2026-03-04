from pylabwons_stub.core.build.baseline import Baseline
from pylabwons_stub.schema.key import SCHEMA
from pylabwons_stub.utils import tools
from typing import Callable

class MarketMap(Baseline):

    def __init__(self, logger:Callable=print):
        super().__init__(logger)

        self._extract()
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
        self.drop(inplace=True, columns=[c for c, meta in SCHEMA.items() if not meta.market_map])
        return


if __name__ == "__main__":
    mmap = MarketMap()
    print(mmap)
