import xlsxwriter

from pylabwons_stub.schema.dataframe import DataFrameHeir
from pylabwons_stub.schema.const.baseline import BASELINE, COMMENT, STYLE
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

        trailingPe = pd.to_numeric(self.trailingPe, errors='coerce')
        trailingEps = pd.to_numeric(self.trailingEps, errors='coerce')
        yoyEps = pd.to_numeric(self.yoyEps, errors='coerce')
        forwardPe = pd.to_numeric(self.forwardPe, errors='coerce')
        forwardEps = pd.to_numeric(self.forwardEps, errors='coerce')
        self['trailingPeg'] = round(trailingPe / yoyEps, 2)
        self['forwardPeg'] = round(forwardPe / 100 * (forwardEps / trailingEps - 1), 2)
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
        copy = self.copy()
        copy['name'] = copy[['name', 'market']].apply(lambda r: f'{r[0]}*' if r[1] == 'kosdaq' else r[0], axis=1, raw=True)
        copy.marketCap = (copy.marketCap / 1e+8).astype(int)
        copy.ifrsType = copy.ifrsType.apply(lambda x: "연결" if x == "D" else "별도")
        copy.fiscalMonth = copy.fiscalMonth.apply(lambda x: str(x).replace("(P)", ", 잠정"))
        copy.estimatedMonth = copy.estimatedMonth.apply(lambda x: str(x).replace("(E)", ", 추정"))

        copy = copy[[c for c, v in BASELINE.items() if v.rel_name]]

        # 열(지표) 이름 처리
        columns = []
        for k, v in BASELINE.items():
            if not v.rel_name:
                continue

            level0, level1 = v.rel_group, v.rel_name

            unit = v.unit
            if k == 'marketCap':
                unit = '억원'
            level1 = f'{level1}\n({unit})' if unit else f'{level1}\n'

            columns.append((level0, level1))

        # 엑셀 파일에 쓰기
        time = datetime.strptime(self.log.baseline.date, "%Y%m%d %H:%M")
        name = time.strftime("%Y년%m월%d일 %H시%M분 기준")

        wb = xlsxwriter.Workbook(path)
        ws = wb.add_worksheet(name)

        # 서식
        style = STYLE(wb)
        switch = {'':'basic', '4분기 합산': 'trailing', '전년 동기 대비': 'yoy',
                  '직전 결산 기준': 'fiscal', '컨센서스': 'estimate', '기타':'etc'}

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
                if ('int' in str(type(col)).lower()) and (not columns[n_col - 1][1].endswith('일\n')):
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
            meta = BASELINE[col]
            if not meta.rel_name:
                continue
            ws.set_column(n_col, n_col, meta.rel_width)

        # 틀 고정
        ws.freeze_panes(2, 2)

        # 필터 추가
        ws.autofilter(1, 0, 1, len(copy.columns))

        # 2번째 시트 추가(지표 안내)
        ws_2 = wb.add_worksheet("지표 안내")
        ws_2.write(0, 0, COMMENT.WARN, style.head.warn)
        ws_2.write_row(1, 0,  ["분류", "지표 이름", "단위", "정보 출처", "계산식"], style.head.basic)
        n_row = 2
        for c, ind in BASELINE.items():
            if not ind.rel_name:
                continue

            group = '공통' if not ind.rel_group else ind.rel_group
            unit = '억원' if c == 'marketCap' else ind.unit
            ws_2.write_row(n_row, 0, [group, ind.rel_name, unit, ind.ref], style.cell.basic)
            ws_2.write(n_row, 4, ind.calc, style.cell.string)
            n_row += 1

        ws_2.set_column(0, 0, 15)
        ws_2.set_column(1, 1, 18)
        ws_2.set_column(3, 3, 18)
        wb.close()
        return


if __name__ == "__main__":
    baseline = Baseline()
    # print(baseline.market.date)
    # print(baseline)
    # print(baseline.columns)
    # baseline.build('baseline')
    baseline.release(PATH.DOWNLOADS / f'BASELINE.xlsx')
    # baseline.market.to_excel(PATH.DOWNLOADS / 'market.xlsx')
    # baseline.number.to_excel(PATH.DOWNLOADS / 'number.xlsx')
    # baseline.sector.to_excel(PATH.DOWNLOADS / 'sector.xlsx')
