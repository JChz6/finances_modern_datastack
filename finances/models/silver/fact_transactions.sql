{{
    config(
        materialized='incremental',
        incremental_strategy= 'delete+insert',
        unique_key= 'month_id'
    )
}}

WITH CTE AS(
    SELECT
        *,
        SPLIT_PART(comentario, ' ', 1) AS part_1,
        SPLIT_PART(comentario, ' ', 2) AS part_2
    FROM {{ref('stg_transactions')}}
    WHERE cuenta NOT IN ('Kilometraje', 'Personal')

    {% if is_incremental() %}
        WHERE month_id::INTEGER >= (select month_id from {{ this }})
    {% endif %}

)

SELECT
    txn_id,
    month_id,
    txn_time,
    cuenta,
    categoria,
    subcategoria,
    concepto,
    importe_moneda_principal,
    ingreso_gasto,
    comentario,
    importe_nativo,
    moneda,
    importe_txn,
    ROUND(importe_moneda_principal / importe_txn, 3) AS tipo_cambio,
    CASE
        WHEN TRIM(REPLACE(LOWER(comentario), 'í', 'i')) LIKE '%dias trabajados'
            THEN part_1
        WHEN TRIM(REPLACE(LOWER(comentario), 'í', 'i')) LIKE '%dia trabajado'
            THEN part_1
        ELSE NULL
    END AS dias_trabajados,

    CASE
        WHEN part_1 IN (SELECT DISTINCT clave FROM {{ref('claves')}})
            THEN part_1
        ELSE NULL
    END AS clave,

    CASE
        WHEN clave = 'C/' THEN part_2
        WHEN clave IS NOT NULL AND clave != 'C/' THEN TRIM(SPLIT_PART(comentario, '/', 2))
        ELSE NULL
    END AS valor,
    fecha_carga
FROM CTE
ORDER BY VALOR