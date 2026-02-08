from scripts.env import HOST, PATH
import pandas as pd
import pylabwons as lw
pd.set_option('display.expand_frame_repr', False)


def run_baseline():
    td = lw.TradingDate()

    wics = lw.WiseICS(PATH.PARQ.WICS)
    try:
        if wics['date'].unique()[0] != td.closed:
            wics.fetch()
            wics.to_parquet(PATH.PARQ.WICS, engine='pyarrow')
    except Exception as e:
        pass
    wics.drop(columns=['date'], inplace=True)

    market = lw.AfterMarket(PATH.PARQ.AFTERMARKET)
    try:
        if market['date'].unique()[0] != td.closed:
            market.fetch()
            market.to_parquet(PATH.PARQ.AFTERMARKET, engine='pyarrow')
    except Exception as e:
        pass
    market.drop(columns=['name'], inplace=True)

    base = wics.join(market)
    # print(base)

    tickers = base.head(5).index
    print(tickers)
    numbers = lw.Numbers(*tickers)
    print(numbers.overview)


if __name__ == "__main__":
    # run_baseline()

    fng = lw.FnGuide('034730')
    print(fng.snapshot)