import pylabwons as lw

COMMENT = lw.DataDict(
    WARN="* 모든 투자의 책임은 당사자에게 있습니다. "
         "본 자료는 법적 효력이 없으며 열람 시점에 따라 정보가 불확실할 수 있습니다. "
         "재배포 및 수정은 가능하나, 반드시 출처를 밝히시기 바랍니다."
)

STYLE = lambda wb: lw.DataDict(
    head = lw.DataDict({
        '공통':wb.add_format({
            'font_size': 8, 'bold': True,
            'align': 'center', 'valign':'vcenter', 'text_wrap': True,
            'bg_color': '#D9D9D9'
        }),
        '4분기 합산':wb.add_format({
            'font_size': 8, 'bold': True,
            'align': 'center', 'valign':'vcenter', 'text_wrap': True,
            'bg_color': '#83CCEB',
            'top': 1, 'top_color': 'black'
        }),
        '전년 동기 대비':wb.add_format({
            'font_size': 8, 'bold': True,
            'align': 'center', 'valign': 'vcenter', 'text_wrap': True,
            'bg_color': '#94DCF8',
            'top': 1, 'top_color': 'black'
        }),
        '직전 결산 기준':wb.add_format({
            'font_size': 8, 'bold': True,
            'align': 'center', 'valign': 'vcenter', 'text_wrap': True,
            'bg_color': '#F7C7AC',
            'top': 1, 'top_color': 'black'
        }),
        '컨센서스':wb.add_format({
            'font_size': 8, 'bold': True,
            'align': 'center', 'valign': 'vcenter', 'text_wrap': True,
            'bg_color': '#B5E6A2',
            'top': 1, 'top_color': 'black'
        }),
        '기타':wb.add_format({
            'font_size': 8, 'bold': True,
            'align': 'center', 'valign': 'vcenter', 'text_wrap': True,
            'bg_color': '#D9D9D9',
            'top': 1, 'top_color': 'black'
        }),
        "warn":wb.add_format({
            'font_size': 8, 'font_color': 'red', 'bold': True,
            'align': 'left', 'valign': 'vcenter', 'text_wrap': False,
            'top': 1, 'top_color': 'black'
        })

    }),
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