import pylabwons as lw
import pylabwons_stub as lws
import os


if __name__ == "__main__":
    lw.login_krx(os.environ['KRX_ID'], os.environ['KRX_PW'])

    baseline = lws.Baseline(logger=lw.Logger())
    baseline.number.progress_bar = False
    baseline.build()
    baseline.logger(baseline.dates)

