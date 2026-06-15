FROM python:3.11-slim

WORKDIR /usr/app

# Добавляем установку git внутри Linux
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    dbt-core~=1.8.0 \
    dbt-clickhouse~=1.8.0 \
    dbt-postgres~=1.8.0

ENTRYPOINT ["dbt"]