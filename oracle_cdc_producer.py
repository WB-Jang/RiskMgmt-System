import cx_Oracle
from kafka import KafkaProducer
import json
import time

def get_latest_trx(conn, last_id):
    cursor = conn.cursor()
    cursor.execute("SELECT tx_id, account_no, amount, created_at FROM TRX_TABLE WHERE tx_id > :1 ORDER BY tx_id", [last_id])
    return cursor.fetchall()

def main():
    # Oracle DB 연결
    dsn = cx_Oracle.makedsn('oracle_host', 1521, service_name='service')
    conn = cx_Oracle.connect('user', 'password', dsn)

    # Kafka 프로듀서 준비
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v, default=str).encode()
    )

    last_id = 0

    while True:
        rows = get_latest_trx(conn, last_id)
        for row in rows:
            event = {
                "tx_id": row[0],
                "account_no": row[1],
                "amount": float(row[2]),
                "created_at": row[3].isoformat(),
            }
            producer.send("trx_topic", event)
            last_id = row[0]
        time.sleep(5)

if __name__ == "__main__":
    main()
