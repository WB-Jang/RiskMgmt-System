import cx_Oracle, pymysql

def get_oracle_sum(date, metric_code):
    # 오라클 리스크 산출 원천
    # 예: SELECT SUM(lcr) FROM oracle_risk_table WHERE cal_date=:1
    pass

def get_mariadb_sum(date, metric_code):
    # MariaDB 저장 리스크
    # 예: SELECT SUM(metric_value) FROM risk_daily_metric WHERE metric_date=%s AND metric_code=%s
    pass

def main(date, metric_code):
    src_sum = get_oracle_sum(date, metric_code)
    tgt_sum = get_mariadb_sum(date, metric_code)
    if abs(src_sum - tgt_sum) > 1e-2:
        # 문의 알림/Slack 등 연동
        print(f"[오류] 정합성 불일치: {metric_code} {date}, Oracle:{src_sum}, MariaDB:{tgt_sum}")

if __name__=="__main__":
    main('2026-02-13', 'LCR')
