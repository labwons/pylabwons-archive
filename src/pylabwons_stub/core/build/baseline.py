import xlsxwriter

from pylabwons_stub.schema.dataframe import DataFrameHeir
from pylabwons_stub.schema.const.baseline import BASELINE
from pylabwons_stub.core.fetch.market import Market
from pylabwons_stub.core.fetch.number import Number
from pylabwons_stub.core.fetch.sector import Sector
from pylabwons_stub.env import HOST, PATH, RUNTIME
from datetime import datetime
from pathlib import Path
from typing import Callable, List, Union
import numpy as np
import pandas as pd
import pylabwons as lw
import json, os
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
        self.logger(f'CAPTURE NEW BASELINE ON {self.log.market.date}')
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
        self['yoyRevenue'] = self['yoyRevenue'].fillna('미제공')
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
            if not ((self.market.date == self.td.latest) and
                self.log.market.date.endswith("15:30") and
                self.log.prices.time.endswith("15:30")):
                tickets.append('market')

        if not self.sector.date == self.sector.server_date == self.log.sector.date:
            tickets.append('sector')

        if not self.number.date == self.number.server_date == self.log.number.date:
            tickets.append('number')

        if HOST == 'github_action':
            if 8 <= self.td.clock().hour < 20:
                if 'sector' in tickets:
                    tickets.remove('sector')
                if 'number' in tickets:
                    tickets.remove('number')
        return tickets

    def build(self, *tickets):
        tickets = self.get_tickets(*tickets)
        status = "OPEN" if self.td.is_open() else "CLOSED"
        self.logger(f'[BUILD BASELINE]')
        self.logger(f'| TRADING DATE: {self.td.latest} ({status})')
        self.logger(f'| TICKETS: {"NO TICKETS" if not tickets else tickets}')

        if 'market' in tickets:
            try:            
                self.market.fetch()
                self.market.to_parquet(PATH.PARQUET.MARKET, engine='pyarrow')
                self.log.prices.time = self.log.market.date = self.market.lap
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

    def release(self, path:Union[str, Path]):
        # 데이터 전처리
        drop = ['market', 'shares', 'foreignSharesLimit', 'foreignRateByLimit' ,
                'estimation', 'nOfEstimations',
                'groupByMarketCap', 'wicsDate', 'industryCode', 'sectorCode']

        copy = self.copy()
        copy['name'] = copy[['name', 'market']].apply(lambda r: f'{r[0]}*' if r[1] == 'kosdaq' else r[0], axis=1, raw=True)
        copy.marketCap = (copy.marketCap / 1e+8).astype(int)
        copy.ifrsType = copy.ifrsType.apply(lambda x: "연결" if x == "D" else "별도")
        copy.drop(columns=drop, inplace=True)

        # 열(지표) 이름 처리
        columns = []
        for k, v in BASELINE.items():
            if k in drop:
                continue

            level0 = ''
            level1 = v.kor_name
            if '(' in v.kor_name and ')' in v.kor_name:
                level0 = v.kor_name.split('(')[-1][:-1]
                level1 = level1.replace(f"({level0})", "")

            if level0 == "직전결산연도":
                level0 = "직전 결산 기준"
            if "목표" in level1 or "추정" in level1 or level0 == "12개월선행":
                if level0 == "12개월선행":
                    level1 = f"{level1}(12개월선행)"
                level0 = "추정"
            if level1 in ['KRX업종분류', 'KRX업종PER', '주요제품', '상장일', '베타']:
                level0 = '기타'
            if level0 == "추정":
                level0 = "추정치"

            if v.unit:
                if k == 'marketCap':
                    level1 = f'{level1}\n(억원)'
                else:
                    level1 = f'{level1}\n({v.unit})'
            else:
                level1 = f'{level1}\n'
            if level1 == "추정 기준일":
                level1 = "기준일\n"
            columns.append((level0, level1))

        # 엑셀 파일에 쓰기
        time = datetime.strptime(self.log.baseline.date, "%Y%m%d %H:%M")
        name = time.strftime("%Y년%m월%d일 %H시%M분 기준")

        wb = xlsxwriter.Workbook(path)
        ws = wb.add_worksheet(name)

        # 서식
        style = lw.DataDict(
            head = lw.DataDict(
                basic=wb.add_format({
                    'font_size': 8, 'bold': True,
                    'align': 'center', 'valign':'vcenter', 'text_wrap': True,
                    'bg_color': '#D9D9D9'
                }),
                trailing=wb.add_format({
                    'font_size': 8, 'bold': True,
                    'align': 'center', 'valign':'vcenter', 'text_wrap': True,
                    'bg_color': '#83CCEB',
                    'top': 1, 'top_color': 'black'
                }),
                yoy=wb.add_format({
                    'font_size': 8, 'bold': True,
                    'align': 'center', 'valign': 'vcenter', 'text_wrap': True,
                    'bg_color': '#94DCF8',
                    'top': 1, 'top_color': 'black'
                }),
                fiscal=wb.add_format({
                    'font_size': 8, 'bold': True,
                    'align': 'center', 'valign': 'vcenter', 'text_wrap': True,
                    'bg_color': '#F7C7AC',
                    'top': 1, 'top_color': 'black'
                }),
                estimate=wb.add_format({
                    'font_size': 8, 'bold': True,
                    'align': 'center', 'valign': 'vcenter', 'text_wrap': True,
                    'bg_color': '#B5E6A2',
                    'top': 1, 'top_color': 'black'
                }),
                etc=wb.add_format({
                    'font_size': 8, 'bold': True,
                    'align': 'center', 'valign': 'vcenter', 'text_wrap': True,
                    'bg_color': '#D9D9D9',
                    'top': 1, 'top_color': 'black'
                }),

            ),
            cell=lw.DataDict(
                basic=wb.add_format({
                    'font_size': 8, 'bold': False,
                    'align': 'center', 'valign': 'vcenter'
                }),
                integer=wb.add_format({
                    'font_size': 8, 'bold': False,
                    'align': 'center', 'valign': 'vcenter',
                    'num_format': '#,##0'
                }),
                string=wb.add_format({
                    'font_size': 8, 'bold': False,
                    'align': 'left', 'valign': 'vcenter',
                }),
            )
        )

        switch = {'':'basic', '4분기 합산': 'trailing', '전년 동기 대비': 'yoy',
                  '직전 결산 기준': 'fiscal', '추정치': 'estimate', '기타':'etc'}

        # 헤더(열 이름) 삽입
        l0_base = columns[0][0]
        for n_col, (l0, l1) in enumerate(columns, start=1):
            _style = style.head[switch[l0]]
            if l0 != l0_base:
                ws.write(0, n_col, l0, _style)
                l0_base = l0
            else:
                ws.write(0, n_col, '', _style)
                if n_col == 1:
                    ws.write(0, 0, '', _style)
            ws.write(1, n_col, l1, _style)
            if n_col == 1:
                ws.write(1, 0, '종목코드\n', _style)

        # 인덱스(종목 코드) 삽입
        for n_row, ticker in enumerate(copy.index, start=2):
            ws.write(n_row, 0, ticker, style.cell.basic)

        # 개별 데이터 삽입
        for n_row, row in enumerate(copy.itertuples(index=False), start=2):
            for n_col, col in enumerate(row, start=1):
                _style = style.cell.basic
                if ('int' in str(type(col)).lower()) and (not columns[n_col - 1][1].endswith('일')):
                    _style = style.cell.integer
                if columns[n_col - 1][1].startswith('KRX업종분류') or \
                   columns[n_col - 1][1].startswith('주요제품'):
                    _style = style.cell.string
                if type(col) == str:
                    try:
                        col = float(col)
                    except (ValueError, TypeError, Exception):
                        pass
                try:
                    ws.write(n_row, n_col, col, _style)
                except (ValueError, TypeError, Exception):
                    ws.write(n_row, n_col, '', _style)

        # 열 너비 설정
        for n_col, col in enumerate(copy.columns, start=1):
            name = columns[n_col-1][1]
            name = name[:name.find('\n')]
            if name in ['KRX업종분류', '주요제품']:
                continue
            if name == '업종분류':
                width = 11
            elif name == '섹터분류':
                width = 14
            elif "수익률" in name:
                width = 10
            elif name in ["상장 주식수", "유동 주식수"]:
                width = 12.5
            elif name == "우선주":
                width = 9
            elif name == "매출":
                width = 12
            elif name in ["영업이익성장률", "영업이익률 성장률", "당기순이익성장률", "배당성향증감률"]:
                width = 13.5
            elif name == "KRX업종분류":
                width = 12.5
            else:
                width = max(len(name), copy[col].apply(lambda x: len(str(x))).max()) + 2
            ws.set_column(n_col, n_col, width)

        # 틀 고정
        ws.freeze_panes(2, 2)

        # 필터 추가
        ws.autofilter(1, 0, 1, len(copy.columns) + 1)

        wb.close()
        return


if __name__ == "__main__":
    baseline = Baseline()
    # print(baseline.market.date)
    # print(baseline)
    # print(baseline.columns)
    # baseline.build()
    baseline.release(PATH.DOWNLOADS / f'BASELINE.xlsx')
    # baseline.market.to_excel(PATH.DOWNLOADS / 'market.xlsx')
    # baseline.number.to_excel(PATH.DOWNLOADS / 'number.xlsx')
    # baseline.sector.to_excel(PATH.DOWNLOADS / 'sector.xlsx')
