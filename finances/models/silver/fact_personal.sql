{{
    config(
        materialized='incremental',
        incremental_strategy= 'delete+insert',
        unique_key= 'month_id'
    )
}}

WITH CTE AS(
    SELECT
        txn_id,
        month_id,
        txn_time,
        cuenta,
        categoria,
        subcategoria,
        concepto,
        CASE 
            WHEN LOWER(ingreso_gasto) LIKE '%ingr%' THEN 'Positivo'
            WHEN LOWER(ingreso_gasto) LIKE '%gast%' THEN 'Negativo'
            ELSE NULL
        END AS sentimiento,
        comentario,
        importe_nativo,
        moneda,
        importe_txn,
        SPLIT_PART(comentario, ' ', 1) AS part_1,
        SPLIT_PART(comentario, ' ', 2) AS part_2,
        fecha_carga
    FROM {{ref('stg_transactions')}}
    WHERE cuenta IN ('Personal')

    {% if is_incremental() %}
        AND 
        (
            month_id::INTEGER > (SELECT MAX(month_id) from {{ this }})
            OR month_id::INTEGER IN (SELECT DISTINCT month_id from {{ this }})
        )
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
    sentimiento,
    comentario,
    importe_nativo,
    moneda,
    importe_txn,
    
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