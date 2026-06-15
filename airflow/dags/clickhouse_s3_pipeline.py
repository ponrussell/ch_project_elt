from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

# Настройки MinIO
S3_CONN_ID = 'aws_boto3_s3'
S3_BUCKET = 'bronze'
S3_FILE_PATTERN = '*.parquet' 

# Настройки ClickHouse
CH_HOST = 'http://clickhouse-course1:8123'
CH_USER = 'student'
CH_PASSWORD = 'strongpassword'
CH_TABLE = 'test.bronze'  

# 2. СОЗДАЛИ ФУНКЦИЮ АЛЕРТИНГА
def alert_failed_task(context):
    ti = context.get('task_instance')
    task_id = ti.task_id
    dag_id = ti.dag_id
    execution_date = context.get('execution_date').strftime('%Y-%m-%d %H:%M:%S')
    exception = context.get('exception') 
    
    try:
        files = ti.xcom_pull(task_ids='load_s3_to_clickhouse')
        target_file = files if files else "Не определен"
    except Exception:
        target_file = "Не определен"

    subject = f"⚠️ ALERT: Сбой в таске {task_id} | DAG: {dag_id}"
    
    html_content = f"""
    <div style="font-family: Arial, sans-serif; border: 1px solid #ffcccc; padding: 20px; background-color: #fff5f5;">
        <h2 style="color: #cc0000; margin-top: 0;">💥 Произошла авария в пайплайне данных!</h2>
        <p><b>Идентификатор DAG:</b> <code style="background: #eee; padding: 2px 5px;">{dag_id}</code></p>
        <p><b>Сломался таск:</b> <code style="background: #eee; padding: 2px 5px;">{task_id}</code></p>
        <p><b>Время запуска (UTC):</b> {execution_date}</p>
        <hr style="border: 0; border-top: 1px solid #ffcccc;">
        <p style="color: #660000; font-family: monospace; background: #ffe6e6; padding: 10px; border-left: 4px solid #cc0000;">
            <b>Текст технической ошибки:</b><br>{exception}
        </p>
        <hr style="border: 0; border-top: 1px solid #ffcccc;">
        <p style="font-size: 12px; color: #666;"><i>Уведомление сформировано автоматически платформой Apache Airflow. Файл сохранен в MinIO S3.</i></p>
    </div>
    """
    
    send_email(
        to='xxxponrussellxxx@yandex.ru',  # <--- УКАЖИТЕ ЗДЕСЬ СВОЮ ЛИЧНУЮ ПОЧТУ ЯНДЕКС
        subject=subject,
        html_content=html_content
    )

# 3. ПРИВЯЗАЛИ АЛЕРТ К КОЛБЭКУ DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': alert_failed_task,  # <--- ЭТА СТРОКА ЗАПУСКАЕТ АЛЕРТ ПРИ ПАДЕНИИ
}
def load_from_s3_to_clickhouse(**kwargs):
    """
    Сканирует бакет MinIO, находит файлы Parquet и передает команду в ClickHouse
    с явным маппингом колонок для сохранения нижнего регистра.
    """
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    files = s3_hook.list_keys(bucket_name=S3_BUCKET)
    
    # Отбираем файлы с расширением .parquet (ваша безопасная проверка)
    parquet_files = [f for f in files if f.endswith('.parquet')] if files else []
    
    if not parquet_files:
        raise ValueError("В бакете MinIO не найдено Parquet-файлов для загрузки")
    
    parquet_files.sort()
    target_file = parquet_files[0]
    
    s3_url = f'http://minio:9000/{S3_BUCKET}/{target_file}'
    print(f"Даю команду ClickHouse загрузить файл напрямую из S3: {s3_url}")
    
    # SQL-запрос с маппингом: сопоставляем PascalCase из файла с вашим нижним регистром таблицы
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
        IsNotBounce, GoalsReached, ParamOrderID, 
        CAST(ParamPrice AS Decimal(18, 2))
    FROM s3(
        '{s3_url}',
        'minio_admin',
        'minio_strong_password',
        'Parquet'
    );
    """
    
    response = requests.post(
        CH_HOST,
        params={'query': query, 'user': CH_USER, 'password': CH_PASSWORD}
    )
    
    if response.status_code != 200:
        raise Exception(f"Ошибка ClickHouse при чтении из S3: {response.text}")
        
    print(f"ClickHouse успешно импортировал файл {target_file}!")
    
    # Очищаем бакет после успешной загрузки, чтобы конвейер шел дальше
    s3_hook.delete_objects(bucket=S3_BUCKET, keys=target_file)
    print(f"Файл {target_file} удален из бакета {S3_BUCKET}")


with DAG(
    dag_id='clickhouse_s3_pipeline',
    default_args=default_args,
    schedule_interval='@continuous', 
    max_active_runs=1,
    catchup=False,
    tags=['minio', 's3', 'clickhouse']
) as dag:

    # 1. Сенсор: Ищет Parquet файлы в MinIO
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

    # 2. Оператор загрузки
    load_data = PythonOperator(
        task_id='load_s3_to_clickhouse',
        python_callable=load_from_s3_to_clickhouse,
    )

    wait_for_s3_file >> load_data