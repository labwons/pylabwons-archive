from pylabwons.schema.datadict import DataDictionary

URLS = lambda ticker: DataDictionary(
    SNAPSHOT=f"http://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?" \
             f"pGB=1&" \
             f"gicode=A{ticker}&" \
             f"cID=&" \
             f"MenuYn=Y" \
             f"&ReportGB=" \
             f"&NewMenuID=" \
             f"&stkGb=701",
    BANDS=f"http://cdn.fnguide.com/SVO2/json/chart/01_06/chart_A{ticker}_D.json",
    FOREIGNRATE3M=f"http://cdn.fnguide.com/SVO2/json/chart/01_01/chart_A{ticker}_3M.json",
    FOREIGNRATE1Y=f"http://cdn.fnguide.com/SVO2/json/chart/01_01/chart_A{ticker}_1Y.json",
    FOREIGNRATE3Y=f"http://cdn.fnguide.com/SVO2/json/chart/01_01/chart_A{ticker}_3Y.json",
    PRODUCTS=f"http://cdn.fnguide.com/SVO2/json/chart/02/chart_A{ticker}_01_N.json",
    EXPENSES=f"http://cdn.fnguide.com/SVO2/json/chart/02/chart_A{ticker}_D.json",
    XML=f"http://cdn.fnguide.com/SVO2/xml/Snapshot_all/{ticker}.xml"
)

HEADER = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/133.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer": "https://comp.fnguide.com/",
    "Connection": "keep-alive",
}

LABEL_ESTIMATION = {
    "투자의견": "estimation",
    "목표주가": "targetPrice",
    "EPS": "estimatedEps",
    "PER": "estimatedPe",
    "추정기관수": "nOfEstimations"
}

KEY_CHANGE_RATE = {
    "영업이익": "profit",
    "당기순이익": "netProfit",
    "자산총계": "asset",
    "부채총계": "debt",
    "영업이익률": "profitRate",
    "EPS": "eps",
    "DPS": "dps"
}

# IFRS 공시가 없는 종목
NUMBER_EXCEPTION = ['088980', '094800', '415640']