SELECT 
    silver.watch_id
FROM {{ ref('stg__silver_hits') }} AS silver
INNER JOIN {{ ref('stg__bronze_hits') }} AS bronze 
    ON silver.watch_id = bronze.watch_id
WHERE bronze.is_robot != 0