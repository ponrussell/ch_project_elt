--Запросы для контроля работы MV
--SELECT count(*) FROM test.bronze;
--SELECT count(*) FROM test.silver;
--SELECT count(*) FROM test.dlq;
--SELECT ((SELECT count(*) FROM test.dlq) / (SELECT count(*) FROM test.bronze)*100.0) AS perc,
--(SELECT count(*) FROM test.silver) / (SELECT count(*) FROM test.bronze)*100.0) AS perc_2;
--TRUNCATE TABLE test.bronze;

-- создаем копию таблицы в тесте
DROP TABLE IF EXISTS test.bronze;

CREATE TABLE test.bronze (
    event_date Date COMMENT 'Дата события по UTC-0.',
    event_time DateTime('UTC') COMMENT 'Дата и время события по UTC-0.',
    watch_id UInt64 COMMENT 'Уникальный идентификатор просмотра страницы.',
    user_id UInt64 COMMENT 'Уникальный идентификатор посетителя сайта.',
    region_id UInt32 COMMENT 'Числовой идентификатор географического региона.',
    os UInt8 COMMENT 'Внутренний числовой идентификатор операционной системы.',
    user_agent UInt8 COMMENT 'Внутренний числовой идентификатор браузера.',
    is_mobile UInt8 COMMENT 'Флаг типа устройства.',
    mobile_phone UInt8 COMMENT 'Числовой идентификатор производителя телефона.',
    mobile_phone_model String COMMENT 'Текстовое название модели устройства.',
    resolution_width UInt16 COMMENT 'Разрешение экрана по ширине.',
    resolution_height UInt16 COMMENT 'Разрешение экрана по высоте.',
    is_robot UInt8 COMMENT 'Флаг антифрод-системы.',
    trafic_source_id Int16 COMMENT 'Внутренний числовой код источника трафика.',
    utm_source String COMMENT 'UTM-метка источника трафика.',
    utm_medium String COMMENT 'UTM-метка типа трафика.',
    utm_campaign String COMMENT 'UTM-метка названия рекламной кампании.',
    utm_content String COMMENT 'UTM-метка содержания объявления.',
    utm_term String COMMENT 'UTM-метка ключевого слова.',
    referer String COMMENT 'Полный URL-адрес страницы перехода.',
    referer_domain String COMMENT 'Домен страницы источника перехода.',
    search_phrase String COMMENT 'Текст поискового запроса.',
    url String COMMENT 'Полный адрес просматриваемой страницы.',
    url_domain String COMMENT 'Домен сайта.',
    is_not_bounce UInt8 COMMENT 'Флаг не-отказа.',
    goals_reached Array(UInt32) COMMENT 'Массив идентификаторов целей.',
    param_order_id String COMMENT 'Номер оформленного заказа.',
    param_price Decimal(18, 2) COMMENT 'Сумма заказа. Превращаем Int64 из Parquet в Decimal автоматом!',
    
    _ingestion_time DateTime DEFAULT now() COMMENT 'Время записи строки в таблицу',
    _source_system LowCardinality(String) DEFAULT 'MinIO S3' COMMENT 'Источник данных'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id);

-- создаем копию таблицы в тесте для сильвер
CREATE TABLE IF NOT EXISTS test.silver (
    event_date Date COMMENT 'Дата события по UTC-0.',
    event_time DateTime ('UTC') COMMENT 'Дата и время события по UTC-0.',
    watch_id UInt64 COMMENT 'Уникальный идентификатор конкретного просмотра страницы (хита).',
    user_id UInt64 COMMENT 'Уникальный идентификатор посетителя сайта.',
    region_id UInt32 COMMENT 'Числовой идентификатор географического региона пользователя.',
    os UInt8 COMMENT 'Внутренний числовой идентификатор операционной системы.',
    user_agent UInt8 COMMENT 'Внутренний числовой идентификатор браузера.',
    is_mobile UInt8 COMMENT 'Флаг типа устройства (1 — мобильное устройство, 0 — десктоп).',
    mobile_phone UInt8 COMMENT 'Числовой идентификатор производителя (бренда) мобильного телефона.',
    mobile_phone_model LowCardinality(String) COMMENT 'Текстовое название модели устройства. Может содержать обрывки парсинга и пустые строки.',
    resolution_width UInt16 COMMENT 'Разрешение экрана устройства в пикселях по ширине.',
    resolution_height UInt16 COMMENT 'Разрешение экрана устройства в пикселях по высоте.',
    trafic_source_id Int8 COMMENT 'Внутренний числовой код типа источника трафика (прямой заход, реклама и т.д.).',
    utm_source LowCardinality(String) COMMENT 'UTM-метка источника трафика. Может содержать пустые строки.',
    utm_medium LowCardinality(String) COMMENT 'UTM-метка типа трафика.',
    utm_campaign LowCardinality(String) COMMENT 'UTM-метка названия рекламной кампании.',
    utm_content LowCardinality(String) COMMENT 'UTM-метка содержания объявления или дополнительной информации.',
    utm_term String COMMENT 'UTM-метка ключевого слова или поисковой фразы из рекламы.',
    referer String COMMENT 'Полный URL-адрес страницы, с которой пользователь перешел на текущую страницу.',
    referer_domain LowCardinality(String) COMMENT 'Домен страницы источника перехода (выделен из Referer).',
    search_phrase String COMMENT 'Текст поискового запроса (если переход был из поисковой системы).',
    url String COMMENT 'Полный адрес просматриваемой страницы сайта.',
    url_domain LowCardinality(String) COMMENT 'Домен сайта, на котором происходит просмотр.',
    is_not_bounce UInt8 COMMENT 'Флаг «не-отказа» (1 — качественный визит, 0 — быстрый уход (отказ)).',
    goals_reached Array(UInt32) COMMENT 'Массив числовых идентификаторов целей, достигнутых в рамках хита. Например, пользователь добавил товар в корзину.',
    param_order_id String COMMENT 'Номер оформленного заказа. Заполняется вместе с суммой при покупке.',
    param_price Decimal(18, 2) COMMENT 'Сумма заказа. Заполняется только в момент совершения покупки на этой странице.',
    _processed_time DateTime DEFAULT now() COMMENT 'Время трансформации и записи в Серебро',
    _source_system LowCardinality(String) COMMENT 'Источник данных'
)
ENGINE = ReplacingMergeTree(_processed_time)
PARTITION BY toYYYYMM(event_date)
ORDER BY (user_id, event_time, url)
TTL _processed_time + INTERVAL 3 YEAR
COMMENT 'Очищенные данные после трансформаций таблицы бронзового слоя';

-- создаем копию таблицы в тесте для карантина
CREATE TABLE IF NOT EXISTS test.dlq (
    event_date Date COMMENT 'Дата события по UTC-0.',
    event_time DateTime('UTC') COMMENT 'Дата и время события по UTC-0.',
    watch_id UInt64 COMMENT 'Уникальный идентификатор конкретного просмотра страницы (хита).',
    user_id UInt64 COMMENT 'Уникальный идентификатор посетителя сайта.',
    region_id UInt32 COMMENT 'Числовой идентификатор географического региона пользователя.',
    os UInt8 COMMENT 'Внутренний числовой идентификатор операционной системы.',
    user_agent UInt8 COMMENT 'Внутренний числовой идентификатор браузера.',
    is_mobile UInt8 COMMENT 'Флаг типа устройства (1 — мобильное устройство, 0 — десктоп).',
    mobile_phone UInt8 COMMENT 'Числовой идентификатор производителя (бренда) мобильного телефона.',
    mobile_phone_model LowCardinality(String) COMMENT 'Текстовое название модели устройства. Может содержать обрывки парсинга и пустые строки.',
    resolution_width UInt16 COMMENT 'Разрешение экрана устройства в пикселях по ширине.',
    resolution_height UInt16 COMMENT 'Разрешение экрана устройства в пикселях по высоте.',
    is_robot UInt8 COMMENT 'Флаг антифрод-системы от 0 до 4 (1-4 — визит совершен роботом, 0 — живым человеком).',
    trafic_source_id Int8 COMMENT 'Внутренний числовой код типа источника трафика (прямой заход, реклама и т.д.).',
    utm_source LowCardinality(String) COMMENT 'UTM-метка источника трафика. Может содержать пустые строки.',
    utm_medium LowCardinality(String) COMMENT 'UTM-метка типа трафика.',
    utm_campaign LowCardinality(String) COMMENT 'UTM-метка названия рекламной кампании.',
    utm_content LowCardinality(String) COMMENT 'UTM-метка содержания объявления или дополнительной информации.',
    utm_term String COMMENT 'UTM-метка ключевого слова или поисковой фразы из рекламы.',
    referer String COMMENT 'Полный URL-адрес страницы, с которой пользователь перешел на текущую страницу.',
    referer_domain LowCardinality(String) COMMENT 'Домен страницы источника перехода (выделен из Referer).',
    search_phrase String COMMENT 'Текст поискового запроса (если переход был из поисковой системы).',
    url String COMMENT 'Полный адрес просматриваемой страницы сайта.',
    url_domain LowCardinality(String) COMMENT 'Домен сайта, на котором происходит просмотр.',
    is_not_bounce UInt8 COMMENT 'Флаг «не-отказа» (1 — качественный визит, 0 — быстрый уход (отказ)).',
    goals_reached Array(UInt32) COMMENT 'Массив числовых идентификаторов целей, достигнутых в рамках хита. Например, пользователь добавил товар в корзину.',
    param_order_id String COMMENT 'Номер оформленного заказа. Заполняется вместе с суммой при покупке.',
    param_price Decimal(18, 2) COMMENT 'Сумма заказа. Заполняется только в момент совершения покупки на этой странице.',
    _ingestion_time DateTime COMMENT 'Время записи строки в таблицу',
    _source_system LowCardinality(String) COMMENT 'Источник данных'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id)
TTL _ingestion_time + INTERVAL 3 YEAR
COMMENT 'невалидные данные, карантин (bronze->silver)';


-- создаем MV

CREATE MATERIALIZED VIEW IF NOT EXISTS test.mv_bronze_to_silver_hits
TO test.silver
AS
SELECT
    * EXCEPT (is_robot, _ingestion_time)
    REPLACE (
        toDate(event_time) AS event_date,
        domain(url) AS url_domain,
        domain(referer) AS referer_domain,
        trim(lower(utm_source)) AS utm_source,
        trim(lower(utm_medium)) AS utm_medium,
        trim(lower(utm_campaign)) AS utm_campaign,
        trim(lower(utm_content)) AS utm_content,
        trim(lower(utm_term)) AS utm_term,
        trim(lower(mobile_phone_model)) AS mobile_phone_model,
        trim(lower(search_phrase)) AS search_phrase
    )
FROM test.bronze
WHERE is_robot = 0 
  AND region_id != 0
  AND user_id != 0 AND watch_id != 0
  AND toDate(event_time) > '1970-01-01'
  AND url != ''
  AND trafic_source_id BETWEEN -1 AND 10
  AND is_not_bounce IN (0, 1)
  AND param_price >= 0
  AND (NOT (param_price > 0 AND (param_order_id = '' OR param_order_id = '0')))
  AND (
      (is_mobile = 1 AND mobile_phone != 0 AND mobile_phone_model != '') 
      OR is_mobile = 0
  )
  AND resolution_width > 10 AND resolution_height > 10;


CREATE MATERIALIZED VIEW IF NOT EXISTS test.mv_bronze_to_dlq_hits
TO test.dlq
AS
SELECT *
FROM test.bronze
WHERE is_robot > 0 
   OR region_id = 0
   OR user_id = 0 
   OR watch_id = 0
   OR toDate(event_time) <= '1970-01-01'
   OR url = ''
   OR trafic_source_id < -1 OR trafic_source_id > 10
   OR is_not_bounce NOT IN (0, 1)
   OR param_price < 0
   OR (param_price > 0 AND (param_order_id = '' OR param_order_id = '0'))
   OR (is_mobile = 1 AND (mobile_phone = 0 OR mobile_phone_model = ''))
   OR resolution_width <= 10 OR resolution_height <= 10;