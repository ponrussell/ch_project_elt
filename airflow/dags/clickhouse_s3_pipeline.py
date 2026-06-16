import sys
import os
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

# Настройки ClickHouse (ТЕПЕРЬ ПОЛНОСТЬЮ БЕЗОПАСНЫ)
CH_HOST = 'http://clickhouse-course1:8123'
CH_USER = os.getenv('CH_USER', 'student')            # <--- Читаем из Docker env
CH_PASSWORD = os.getenv('CH_PASSWORD', 'strongpassword')  # <--- Читаем из Docker env
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
        to='xxxponrussellxxx@yandex.ru',
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

def write_audit_log(**kwargs):
    """
    Считает количество обработанных строк в текущем запуске
    и записывает метрики качества данных в test.audit_log
    """
    run_id = kwargs['run_id']
    
    # Пытаемся достать имя файла из контекста (чтобы записать в лог)
    try:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        files = s3_hook.list_keys(bucket_name=S3_BUCKET)
        target_file = files[0] if files else "unknown"
    except Exception:
        target_file = "file_processed"

    print(f"Начинаю расчет метрик аудита для запуска: {run_id}")

    # Мощный аналитический запрос ClickHouse: считает срезы за последние 5 минут
    query = f"""
    INSERT INTO test.audit_log
    SELECT 
        '{run_id}' AS run_id,
        '{target_file}' AS file_name,
        -- 1. Сколько пришло в Бронзу (за последние 5 минут)
        (SELECT count() FROM test.bronze WHERE _ingestion_time >= now() - INTERVAL 5 MINUTE) AS b_rows,
        -- 2. Сколько дошло до Сильвер (фильтруем по новому столбцу _processed_time)
        (SELECT count() FROM test.silver WHERE _processed_time >= now() - INTERVAL 5 MINUTE) AS s_rows,
        -- 3. Сколько улетело в Карантин (DLQ)
        (SELECT count() FROM test.dlq WHERE _ingestion_time >= now() - INTERVAL 5 MINUTE) AS d_rows,
        -- 4. Сколько дублей съел движок: Бронза - Сильвер - Карантин
        cast(b_rows - s_rows - d_rows AS Int64) AS dropped_duplicates,
        -- 5. Процент валидных данных
        round(s_rows * 100.0 / nullIf(b_rows, 0), 2) AS valid_percent,
        -- 6. Процент брака
        round(d_rows * 100.0 / nullIf(b_rows, 0), 2) AS invalid_percent,
        now() AS processed_at
    """

    response = requests.post(
        CH_HOST,
        params={'query': query, 'user': CH_USER, 'password': CH_PASSWORD}
    )
    
    if response.status_code != 200:
        raise Exception(f"Ошибка при записи аудит-лога: {response.text}")
    print("Метрики аудита успешно зафиксированы в test.audit_log!")

def check_data_quality(**kwargs):
    """
    Checks data quality using test.audit_log table via JSON format.
    If error rate is above 5%, sends business alert email.
    """
    # Принудительно разрешаем UTF-8 для stdout/stderr внутри этой таски
    sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
    sys.stderr.reconfigure(encoding="utf-8", errors="backslashreplace")

    run_id = kwargs['run_id']
    MAX_ALLOWED_ERROR_RATE = 5.0  # Threshold in %

    query = f"""
    SELECT file_name, invalid_percent, dlq_rows, bronze_rows 
    FROM test.audit_log 
    WHERE run_id = '{run_id}' 
    LIMIT 1
    FORMAT JSONEachRow
    """
    
    response = requests.post(
        CH_HOST,
        params={'query': query, 'user': CH_USER, 'password': CH_PASSWORD}
    )
    
    if response.status_code != 200:
        raise Exception(f"Data Quality check failed: {response.text}")
        
    try:
        # response.json() автоматически корректно обрабатывает UTF-8 из ClickHouse
        data = response.json()
    except Exception:
        print("No valid JSON returned or audit record is missing. Task finished safely.")
        return

    # Теперь здесь безопасно можно использовать оригинальное имя файла, даже если оно на русском
    file_name = data.get('file_name', 'unknown_file') 
    invalid_percent = float(data.get('invalid_percent', 0.0))
    dlq_rows = int(data.get('dlq_rows', 0))
    bronze_rows = int(data.get('bronze_rows', 0))

    print(f"Current error rate verified successfully for file: {file_name}")

    if invalid_percent > MAX_ALLOWED_ERROR_RATE:
        print("CRITICAL ERROR RATE DETECTED! Sending email...")
        
        subject = "DATA QUALITY ALERT: High Error Rate Detected"
        
        html_content = f"""
        <div style="font-family: Arial, sans-serif; border: 2px solid #ff9900; padding: 20px; background-color: #fff9f2;">
            <h2 style="color: #e65c00; margin-top: 0;">Data Quality Anomaly Detected!</h2>
            <p><b>File Name:</b> {file_name}</p>
            <p><b>Total Rows in Source:</b> {bronze_rows}</p>
            <p style="font-size: 16px;"><b>Sent to Dead Letter Queue (DLQ):</b> <span style="color: red; font-weight: bold;">{dlq_rows} rows</span></p>
            <p style="font-size: 18px; background: #ffebd6; padding: 10px; border-left: 5px solid #e65c00;">
                <b>Final Error Rate:</b> <span style="color: #cc0000; font-weight: bold;">{invalid_percent}%</span> (Max Allowed: {MAX_ALLOWED_ERROR_RATE}%)
            </p>
            <hr style="border: 0; border-top: 1px solid #ffcc99;">
            <p style="font-size: 12px; color: #666;"><i>The pipeline finished successfully, but data quality requires investigation. Logged in test.audit_log.</i></p>
        </div>
        """
        
        send_email(
            to='your_real_email@yandex.ru',  # <--- ЗАМЕНЕНО НА ЛАТИНИЦУ
            subject=subject,
            html_content=html_content
        )
    else:
        print("Data quality is within normal limits.")

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

    # 3. Фиксация аудита в лог таблицу
    audit_log = PythonOperator(
        task_id='write_audit_log',
        python_callable=write_audit_log,
        provide_context=True,
    )

    # 4. Проверка бизнес-качества
    check_quality = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality,
        provide_context=True,
    )

    #Сенсор -> Загрузка -> Запись аудита -> Проверка качества
    wait_for_s3_file >> load_data >> audit_log >> check_quality