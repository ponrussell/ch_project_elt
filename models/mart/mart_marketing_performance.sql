{{ config(
    materialized='table',
    schema='gold',
    engine='MergeTree()',
    order_by='(event_date, utm_source)'
) }}

SELECT
    event_date,
    -- Очищаем текстовые метки (приводим к нижнему регистру)
    trim(lower(utm_source)) as utm_source,
    trim(lower(utm_medium)) as utm_medium,
    trim(lower(utm_campaign)) as utm_campaign,
    
    -- Считаем базовые агрегаты
    count(distinct user_id) as unique_users,
    count(watch_id) as total_views,
    
    -- Считаем финансовые метрики
    sum(param_price) as total_revenue,
    count(distinct param_order_id) as total_orders,
    
    -- Считаем Bounce Rate (Долю отказов в %)
    -- В ClickHouse countIf позволяет изящно считать строки по условию
    round(
        (countIf(is_not_bounce = 0) / count(*)) * 100, 
        2
    ) as bounce_rate

FROM {{ ref('stg__silver_hits') }} -- Читаем из нашего дедуплицированного Серебра!
GROUP BY 
    event_date,
    utm_source,
    utm_medium,
    utm_campaign