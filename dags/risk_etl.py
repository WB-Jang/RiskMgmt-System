from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def extract_from_oracle(**kwargs):
    # 오라클에서 데이터 추출 로직 작성
    # 예시: cx_Oracle 사용
    pass

def transform(**kwargs):
    # 데이터 정제 및 산출식 적용 (ex. LCR, NSFR 산식)
    pass

def load_to_mariadb(**kwargs):
    # MariaDB에 저장
    pass

default_args = {
    'owner': 'risk',
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}

with DAG('risk_batch_etl',
         default_args=default_args,
         schedule_interval='0 2 * * *',  # 매일 2시 실행(EOD 이후)
         catchup=False) as dag:
    extract = PythonOperator(task_id='extract', python_callable=extract_from_oracle)
    transform = PythonOperator(task_id='transform', python_callable=transform)
    load = PythonOperator(task_id='load', python_callable=load_to_mariadb)

    extract >> transform >> load  # 의존성 설정
