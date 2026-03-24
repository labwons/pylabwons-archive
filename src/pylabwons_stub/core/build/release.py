from pylabwons_stub.core.build.baseline import Baseline
from pylabwons_stub.schema.const.baseline import BASELINE
from pylabwons_stub.schema.const.release import COMMENT, STYLE
from pylabwons_stub.env import PATH
from datetime import datetime
from pandas import DataFrame
from pathlib import Path
from typing import Callable, Union
import xlsxwriter


class Release(Baseline):

    ID = "LAB￦ONS"
    def __init__(self, logger:Callable=print):
        super().__init__(logger)

        copy = self.copy()
        copy['name'] = copy[['name', 'market']] \
                       .apply(lambda r: f'{r[0]}*' if r[1] == 'kosdaq' else r[0], axis=1, raw=True)
        copy.marketCap = (copy.marketCap / 1e+8).astype(int)
        copy.ifrsType = copy.ifrsType.apply(lambda x: "연결" if x == "D" else "별도")
        copy.fiscalMonth = copy.fiscalMonth.apply(lambda x: str(x).replace("(P)", ", 잠정"))
        copy.estimatedMonth = copy.estimatedMonth.apply(lambda x: str(x).replace("(E)", ", 추정"))
        copy = copy[[c for c, v in BASELINE.items() if v.rel_name]]

        super(Baseline, self).__init__(copy)
        return

    @property
    def date(self) -> str:
        return datetime.strptime(self.log.baseline.date, "%Y%m%d %H:%M").strftime("%Y년%m월%d일 %H시%M분 기준")

    @property
    def note(self) -> str:
        return f"""
<h2>기준 일자</h2>
<p>□ 기본 정보 수집일: {self.log.market.date}</p>
<p>□ 재무 정보 수집일: {self.log.number.date}</p>
<p>□ 업종 정보 수집일: {self.log.sector.date}</p>
<br>
<h2>기본 정보</h2>
<p>□ 종목 개수: {len(self)} 종목</p>
<p>&nbsp;&nbsp;&nbsp;- 코스피: {len(self) - len(self[self['name'].str.endswith('*')]):,d} 종목</p>
<p>&nbsp;&nbsp;&nbsp;- 코스닥: {len(self[self['name'].str.endswith('*')]):,d} 종목</p>
<p>* 중위 시가총액(≒ {int(self['marketCap'].median()):,d}억원) 미만 종목은 재무 정보를 수집하지 않습니다.</p>
<br>
<h2>{self.ID} 서비스 바로가기</h2>
<a href="https://labwons.com">시장 지도</a>
<a href="https://labwons.com/bubbles">버블 차트</a>
"""

    @property
    def path(self) -> Path:
        return PATH.DATA / 'src/release/BASELINE.xlsx'


    @property
    def schema(self) -> DataFrame:
        data = []
        # ["분류", "지표 이름", "단위", "정보 출처", "계산식"]
        for c in self.columns:
            meta = BASELINE[c]
            data.append({
                "분류": '공통' if not meta.rel_group else meta.rel_group,
                "분류(엑셀)": '',
                "지표": meta.rel_name,
                "지표(엑셀)": '',
                "단위": "억원" if c == "marketCap" else meta.unit,
                "출처": meta.ref,
                "수식": meta.calc,
                "너비": meta.rel_width
            })
        return DataFrame(data)

    def as_excel(self, path: Union[Path, str]=None):
        if not path:
            path = self.path
        if isinstance(path, str):
            path = Path(path)

        # 엑셀 파일에 쓰기
        wb = xlsxwriter.Workbook(path)
        ws = wb.add_worksheet(self.date)

        # 서식
        style = STYLE(wb)

        schema = self.schema.copy()
        schema['분류(엑셀)'] = schema['분류']
        schema['지표(엑셀)'] = schema[['지표', '단위']] \
                             .apply(lambda r: f'{r[0]}\n({r[1]})' if r[1] else f'{r[0]}\n', axis=1, raw=True)
        schema.loc[schema['분류'].duplicated(keep='first'), '분류(엑셀)'] = ''

        # HEADER 설정(값, 너비)
        for n, row in schema.iterrows():
            _style = style.head[row['분류']]
            ws.write(0, n + (1 if n else 0), row['분류(엑셀)'], _style)
            ws.write(1, n + 1, row['지표(엑셀)'], _style)
            if not n:
                ws.write(0, 1, '', _style)
                ws.write(1, 0, '종목코드\n', _style)
            ws.set_column(n + 1, n + 1, row['너비'])

        # INDEX(종목 코드) 삽입
        for n_row, ticker in enumerate(self.index, start=2):
            ws.write(n_row, 0, ticker, style.cell.basic)

        # 개별 데이터 삽입
        for n_row, row in enumerate(self.itertuples(index=False), start=2):
            for n_col, data in enumerate(row, start=1):
                col = schema.loc[n_col - 1, '지표']
                _style = style.cell.basic
                if 'int' in str(type(data)).lower():
                    _style = style.cell.integer
                if col.endswith('일'):
                    _style = style.cell.basic
                if col.startswith('KRX업종분류') or col.startswith('주요제품'):
                    _style = style.cell.string

                if type(data) == str:
                    try:
                        data = float(data)
                    except (ValueError, TypeError, Exception):
                        pass
                try:
                    ws.write(n_row, n_col, data, _style)
                except (ValueError, TypeError, Exception):
                    ws.write(n_row, n_col, '', _style)

        # 틀 고정
        ws.freeze_panes(2, 2)

        # 필터 추가
        ws.autofilter(1, 0, 1, len(self.columns))

        # SCHEMA SHEET: 2번째 시트 추가(지표 안내)
        ws = wb.add_worksheet("지표 안내")
        ws.write(0, 0, COMMENT.WARN, style.head.warn)
        ws.write_row(1, 0,  [c for c in schema.columns if not "(" in c and c != '너비'], style.head['공통'])
        for n, row in schema.iterrows():
            _style = style.head[row['분류']]
            ws.write(n + 2, 0, row['분류'], style.cell.basic)
            ws.write(n + 2, 1, row['지표'], style.cell.basic)
            ws.write(n + 2, 2, row['단위'], style.cell.basic)
            ws.write(n + 2, 3, row['출처'], style.cell.basic)
            ws.write(n + 2, 4, f"  {row['수식']}", style.cell.string)
        ws.set_column(0, 0, 15)
        ws.set_column(1, 1, 18)
        ws.set_column(3, 3, 18)
        ws.set_column(4, 4, 42)
        ws.freeze_panes(2, 0)

        wb.close()
        return


if __name__ == "__main__":


    release = Release()
    # print(release.schema)
    print(release.note)
    # release.as_excel()