import pylabwons as lw
import pylabwons_stub as lws
import os


if __name__ == "__main__":
    if lws.HOST == 'github_action':
        lw.login_krx(os.environ['KRX_ID'], os.environ['KRX_PW'])

    logger = lw.Logger(console=True)
    logger(f'RUNS ON {lws.HOST.upper()} / {lws.RUNTIME.upper()} @{os.environ.get("TIMESTAMP", "*")}')
    baseline = lws.Baseline(logger=logger)
    if lws.HOST == 'github_action':
        baseline.number.progress_bar = False
    baseline.build()

    market_map = lws.MarketMap(logger=logger)
    market_map.deploy()

