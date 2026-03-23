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

    if baseline.log.baseline.date.endswith("15:30") and \
       (15 <= baseline.td.clock().hour <= 20):
        if not 'BREVO' in os.environ:
            raise SystemExit
        filepath = lws.PATH.DATA / 'src/release/BASELINE.xlsx'
        baseline.release(filepath)

        mail = lws.Mailing(api=os.environ['BREVO'], logger=logger)
        mail.subject = f'[{mail.ID}] {baseline.log.baseline.date} 시장 정보'
        mail.content = f"""
        <h2>기준 일자</h2>
        <p>- 기본 정보 수집일: {baseline.log.market.date}</p>
        <p>- 재무 정보 수집일: {baseline.log.number.date}</p>
        <p>- 업종 정보 수집일: {baseline.log.sector.date}</p>
        """
        mail.attach(filepath)
        mail.send()

