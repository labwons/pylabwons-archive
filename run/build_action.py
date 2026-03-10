import pylabwons as lw
import pylabwons_stub as lws
import os


if __name__ == "__main__":
    if lws.HOST == 'github_action':
        lw.login_krx(os.environ['KRX_ID'], os.environ['KRX_PW'])

    logger = lw.Logger(console=True)
    baseline = lws.Baseline(logger=logger)
    baseline.number.progress_bar = False
    baseline.build()
    baseline.logger(f'\n{baseline.dates}')

