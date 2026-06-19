import os
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from airflow.models import Variable

S3_CONN_ID = 'aws_boto3_s3'
S3_BUCKET = 'bronze'
S3_FILE_PATTERN = '*.parquet' 
CH_HOST = 'http://clickhouse-course1:8123'
CH_USER = os.getenv('CH_USER', 'student')            
CH_PASSWORD = os.getenv('CH_PASSWORD')
CH_TABLE = 'test.bronze'  

MINIO_USER = Variable.get("minio_s3_user", default_var="fallback_user")
MINIO_PASSWORD = Variable.get("minio_s3_password", default_var="fallback_password")
ALERT_EMAIL = Variable.get("alert_receiver_email", default_var="admin@example.com")

# ==========================================
# 2. ФУНКЦИИ УВЕДОМЛЕНИЙ (АЛЕРТЫ)
# ==========================================
def send_quality_alert(context):
    """Вызывается, если превышен процент брака данных"""
    send_email(
        to=ALERT_EMAIL,
        subject="DATA QUALITY ALERT: High Error Rate Detected",
        html_content=None,
        template='alert_template.html'
    )

def alert_failed_task(context):
    """Вызывается при падении любого инфраструктурного таска"""
    ti = context.get('task_instance')
    task_id = ti.task_id
    dag_id = ti.dag_id
    execution_date = context.get('execution_date').strftime('%Y-%m-%d %H:%M:%S')
    exception = context.get('exception') 

    subject = f"⚠️ ALERT: Сбой в таске {task_id} | DAG: {dag_id}"
    html_content = f"""
    <div style="font-family: Arial, sans-serif; border: 1px solid #ffcccc; padding: 20px; background-color: #fff5f5;">
        <h2 style="color: #cc0000; margin-top: 0;">💥 Произошла авария в пайплайне данных!</h2>
        <p><b>Идентификатор DAG:</b> <code>{dag_id}</code></p>
        <p><b>Сломался таск:</b> <code>{task_id}</code></p>
        <p><b>Время запуска (UTC):</b> {execution_date}</p>
        <hr style="border: 0; border-top: 1px solid #ffcccc;">
        <p style="color: #660000; font-family: monospace; background: #ffe6e6; padding: 10px; border-left: 4px solid #cc0000;">
            <b>Текст технической ошибки:</b><br>{exception}
        </p>
    </div>
    """
    send_email(to=ALERT_EMAIL, subject=subject, html_content=html_content)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': alert_failed_task, # Ссылка на функцию выше
}

# ==========================================
# 3. ОСНОВНЫЕ РАБОЧИЕ ФУНКЦИИ
# ==========================================
def load_from_s3_to_clickhouse(**kwargs):
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    files = s3_hook.list_keys(bucket_name=S3_BUCKET)
    
    parquet_files = [f for f in files if f.endswith('.parquet')] if files else []
    if not parquet_files:
        raise ValueError("В бакете MinIO не найдено Parquet-файлов для загрузки")
    
    parquet_files.sort()
    target_file = parquet_files[0]
    
    s3_url = f'http://minio:9000/{S3_BUCKET}/{target_file}'
    print(f"Даю команду ClickHouse загрузить файл напрямую из S3: {s3_url}")
    
    query = f"""
    INSERT INTO {CH_TABLE} (
        event_date, event_time, watch_id, user_id, region_id, os, user_agent, 
        is_mobile, mobile_phone, mobile_phone_model, resolution_width, resolution_height, 
        is_robot, trafic_source_id, utm_source, utm_medium, utm_campaign, utm_content, 
        utm_term, referer, referer_domain, search_phrase, url, url_domain, 
        is_not_bounce, goals_reached, param_order_id, param_price
    )
    SELECT 
        EventDate, EventTime, WatchID, UserID, RegionID, OS, UserAgent, 
        IsMobile, MobilePhone, MobilePhoneModel, ResolutionWidth, ResolutionHeight, 
        IsRobot, TraficSourceID, UTMSource, UTMMedium, UTMCampaign, UTMContent, 
        UTMTerm, Referer, RefererDomain, SearchPhrase, URL, URLDomain, 
        IsNotBounce, GoalsReached, ParamOrderID, CAST(ParamPrice AS Decimal(18, 2))
    FROM s3('{s3_url}', '{MINIO_USER}', '{MINIO_PASSWORD}', 'Parquet');
    """
    
    response = requests.post(CH_HOST, params={'query': query, 'user': CH_USER, 'password': CH_PASSWORD})
    if response.status_code != 200:
        raise Exception(f"Ошибка ClickHouse при чтении из S3: {response.text}")
        
    print(f"ClickHouse успешно импортировал файл {target_file}!")
    s3_hook.delete_objects(bucket=S3_BUCKET, keys=target_file)
    print(f"Файл {target_file} удален из бакета {S3_BUCKET}")
    
    kwargs['ti'].xcom_push(key='processed_file_name', value=target_file)


def write_audit_log(**kwargs):
    run_id = kwargs['run_id']
    ti = kwargs['ti']

    try:
        target_file = ti.xcom_pull(task_ids='load_s3_to_clickhouse', key='processed_file_name')
        if not target_file:
            target_file = "file_processed"
    except Exception:
        target_file = "file_processed"

    print(f"Начинаю расчет метрик аудита для запуска: {run_id}")

    query = f"""
    INSERT INTO test.audit_log
    SELECT 
        '{run_id}' AS run_id,
        '{target_file}' AS file_name,
        (SELECT count() FROM test.bronze WHERE _ingestion_time >= now() - INTERVAL 5 MINUTE) AS b_rows,
        (SELECT count() FROM test.silver WHERE _processed_time >= now() - INTERVAL 5 MINUTE) AS s_rows,
        (SELECT count() FROM test.dlq WHERE _ingestion_time >= now() - INTERVAL 5 MINUTE) AS d_rows,
        cast(b_rows - s_rows - d_rows AS Int64) AS dropped_duplicates,
        round(s_rows * 100.0 / nullIf(b_rows, 0), 2) AS valid_percent,
        round(d_rows * 100.0 / nullIf(b_rows, 0), 2) AS invalid_percent,
        now() AS processed_at
    """

    response = requests.post(CH_HOST, params={'query': query, 'user': CH_USER, 'password': CH_PASSWORD})
    if response.status_code != 200:
        raise Exception(f"Ошибка при записи аудит-лога: {response.text}")
    print("Метрики аудита успешно зафиксированы в test.audit_log!")


# ==========================================
# 4. ОБЪЯВЛЕНИЕ DAG И ТАСКОВ
# ==========================================
with DAG(
    dag_id='clickhouse_s3_pipeline',
    default_args=default_args,
    schedule_interval='@continuous', 
    max_active_runs=1,
    catchup=False,
    tags=['minio', 's3', 'clickhouse']
) as dag:

    wait_for_s3_file = S3KeySensor(
        task_id='wait_for_s3_file',
        bucket_name=S3_BUCKET,
        bucket_key=S3_FILE_PATTERN,
        wildcard_match=True,
        aws_conn_id=S3_CONN_ID,
        poke_interval=25,
        timeout=3600,
        mode='reschedule',
    )

    load_data = PythonOperator(
        task_id='load_s3_to_clickhouse',
        python_callable=load_from_s3_to_clickhouse,
    )

    audit_log = PythonOperator(
        task_id='write_audit_log',
        python_callable=write_audit_log,
        provide_context=True,
    )

    check_data_quality = PythonOperator(
        task_id='check_data_quality',
        python_callable=lambda **kwargs: requests.post(
            CH_HOST, 
            params={'user': CH_USER, 'password': CH_PASSWORD},
            data=f"SELECT require(invalid_percent <= 5.0, 'Too many errors!') FROM test.audit_log WHERE run_id = '{kwargs['run_id']}' LIMIT 1"
        ).raise_for_status(),
        on_failure_callback=send_quality_alert
    )

    wait_for_s3_file >> load_data >> audit_log >> check_data_quality