import pylabwons as lw
import pylabwons_stub as lws
import os


if __name__ == "__main__":
    if not lws.HOST == 'hkefico':
        lw.login_krx(os.environ['KRX_ID'], os.environ['KRX_PW'])

    baseline = lws.Baseline(logger=lw.Logger(console=False))
    baseline.number.progress_bar = False
    baseline.build()
    baseline.logger(f'\n{baseline.dates}')
    print(baseline.logger)
