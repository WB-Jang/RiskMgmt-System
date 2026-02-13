import oracledb
from kafka import KafkaProducer
import json
import time
import os
from dotenv import load_dotenv

# .env 파일에서 환경 변수 로드
load_dotenv()

def get_latest_trx(conn, last_id):
    cursor = conn.cursor()
    cursor.execute("SELECT tx_id, account_no, amount, created_at FROM TRX_TABLE WHERE tx_id > :1 ORDER BY tx_id", [last_id])
    return cursor.fetchall()

def main():
    # 환경 변수에서 Oracle DB 연결 정보 가져오기
    oracle_host = os.getenv('ORACLE_HOST', 'oracle_host')
    oracle_port = int(os.getenv('ORACLE_PORT', '1521'))
    oracle_service = os.getenv('ORACLE_SERVICE_NAME', 'service')
    oracle_user = os.getenv('ORACLE_USER')
    oracle_password = os.getenv('ORACLE_PASSWORD')
    
    # Kafka 연결 정보
    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    kafka_topic = os.getenv('KAFKA_TOPIC', 'trx_topic')
    
    if not oracle_user or not oracle_password:
        raise ValueError("Oracle credentials must be set in environment variables: ORACLE_USER, ORACLE_PASSWORD")
    
    # Oracle DB 연결
    dsn = oracledb.makedsn(oracle_host, oracle_port, service_name=oracle_service)
    conn = oracledb.connect(user=oracle_user, password=oracle_password, dsn=dsn)

    # Kafka 프로듀서 준비
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode()
    )

    last_id = 0

    try:
        while True:
            rows = get_latest_trx(conn, last_id)
            for row in rows:
                event = {
                    "tx_id": row[0],
                    "account_no": row[1],
                    "amount": float(row[2]),
                    "created_at": row[3].isoformat(),
                }
                producer.send(kafka_topic, event)
                last_id = row[0]
            time.sleep(5)
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        conn.close()
        producer.close()

if __name__ == "__main__":
    main()
