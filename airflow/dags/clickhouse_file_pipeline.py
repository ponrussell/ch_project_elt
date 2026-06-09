from datetime import datetime, timedelta
import os
import shutil
import requests
from airflow import DAG
from airflow.sensors.base import PokeReturnValue
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator

# Внутренние пути в контейнере Airflow (привязаны к вашей папке for_load)
BASE_DIR = '/opt/airflow/data'
INBOUND_DIR = os.path.join(BASE_DIR, 'inbound')
ARCHIVE_DIR = os.path.join(BASE_DIR, 'archive')

# Настройки ClickHouse внутри сети Docker
CH_HOST = 'http://clickhouse:8123'
CH_USER = 'student'
CH_PASSWORD = 'strongpassword'
# НАСТРОЙКА: Замените на ваше реальное имя таблицы Bronze
CH_TABLE = 'test.hits' 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def check_for_csv_files():
    """Проверяет наличие .csv файлов в папке inbound"""
    # Создаем папку, если её вдруг нет внутри контейнера
    os.makedirs(INBOUND_DIR, exist_ok=True)
    
    files = [f for f in os.listdir(INBOUND_DIR) if f.endswith('.csv')]
    
    if files:
        print(f"Найдены файлы для обработки: {files}")
        # Возвращаем True, чтобы сенсор успешно завершился
        return PokeReturnValue(is_done=True)
    
    print("Файлы .csv в папке inbound не обнаружены...")
    return PokeReturnValue(is_done=False)

def load_to_clickhouse(**kwargs):
    """Берет первый файл и отправляет его в ClickHouse через HTTP POST"""
    files = [f for f in os.listdir(INBOUND_DIR) if f.endswith('.csv')]
    if not files:
        raise ValueError("Файлы для загрузки внезапно исчезли")
    
    # Сортируем, чтобы всегда брать самый старый/первый по алфавиту файл
    files.sort()
    target_file = files[0]
    file_path = os.path.join(INBOUND_DIR, target_file)
    
    # Передаем имя файла в следующий таск через XCom
    kwargs['ti'].xcom_push(key='processed_file', value=target_file)
    
    # Формируем SQL-запрос для ClickHouse
    query = f"INSERT INTO {CH_TABLE} FORMAT CSVWithNames"
    
    print(f"Начинаю загрузку файла {target_file} в ClickHouse...")
    
    with open(file_path, 'rb') as f:
        response = requests.post(
            CH_HOST,
            params={
                'query': query, 
                'user': CH_USER, 
                'password': CH_PASSWORD,
                'format_csv_delimiter': ';'
            },
            data=f
        )
    
    if response.status_code != 200:
        raise Exception(f"Ошибка ClickHouse: {response.text}")
        
    print(f"Файл {target_file} успешно загружен в таблицу {CH_TABLE}")

def archive_file(**kwargs):
    """Переносит отработанный файл в папку архива с временной меткой"""
    target_file = kwargs['ti'].xcom_pull(key='processed_file', task_ids='load_to_ch')
    
    src = os.path.join(INBOUND_DIR, target_file)
    
    # Добавляем таймстемп к имени, чтобы файлы с одинаковым именем не перезаписывались в архиве
    name, ext = os.path.splitext(target_file)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    archived_name = f"{name}_{timestamp}{ext}"
    
    dst = os.path.join(ARCHIVE_DIR, archived_name)
    
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    shutil.move(src, dst)
    print(f"Файл перемещен: {src} -> {dst}")


with DAG(
    dag_id='clickhouse_file_pipeline',
    default_args=default_args,
    schedule_interval='@continuous',  # Запускается заново сразу после окончания предыдущего рана
    max_active_runs=1,                # Строго 1 активный запуск, чтобы файлы не обрабатывались параллельно
    catchup=False,
    tags=['clickhouse', 'bronze']
) as dag:

    # 1. Сенсор: проверяет папку каждые 10 секунд
    wait_for_file = PythonSensor(
        task_id='python_wait_for_csv',
        python_callable=check_for_csv_files,
        poke_interval=25,  # Пауза между проверками в секундах
        timeout=3600,      # Таймаут (1 час), после которого таск упадет, если файлов нет
    )

    # 2. Инжект данных в ClickHouse
    load_to_ch = PythonOperator(
        task_id='load_to_ch',
        python_callable=load_to_clickhouse,
    )

    # 3. Перенос в архив
    archive = PythonOperator(
        task_id='archive_processed_file',
        python_callable=archive_file,
    )

    wait_for_file >> load_to_ch >> archive