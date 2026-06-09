{{ config(materialized='view') }}

WITH ranked_hits AS (
    SELECT
        *,
        -- Нумеруем строки с одинаковым watch_id. 
        -- Берем ту, что прилетела позже по event_time.
        ROW_NUMBER() OVER (PARTITION BY watch_id ORDER BY event_time DESC) as rn
    FROM {{ source('clickhouse_silver', 'hits') }}
)

SELECT
    event_date::Date as event_date,
    event_time::DateTime as event_time,
    watch_id::UInt64 as watch_id,
    user_id::UInt64 as user_id,
    region_id::UInt32 as region_id,
    os::UInt8 as os,
    user_agent::UInt8 as user_agent,
    is_mobile::UInt8 as is_mobile,
    mobile_phone::UInt8 as mobile_phone,
    mobile_phone_model::String as mobile_phone_model,
    resolution_width::UInt16 as resolution_width,
    resolution_height::UInt16 as resolution_height,
    trafic_source_id::Int8 as trafic_source_id,
    utm_source::String as utm_source,
    utm_medium::String as utm_medium,
    utm_campaign::String as utm_campaign,
    utm_content::String as utm_content,
    utm_term::String as utm_term,
    referer::String as referer,
    referer_domain::String as referer_domain,
    search_phrase::String as search_phrase,
    url::String as url,
    url_domain::String as url_domain,
    is_not_bounce::UInt8 as is_not_bounce,
    goals_reached,
    param_order_id::String as param_order_id,
    param_price::Decimal(18, 2) as param_price,
    _processed_time::DateTime as _processed_time,
    _source_system::String as _source_system
FROM ranked_hits
WHERE rn = 1 -- Отсекаем дубликат на лету