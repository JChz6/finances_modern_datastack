{{
    config(
        materialized='incremental',
        incremental_strategy= 'delete+insert',
        unique_key= 'month_id'
    )
}}

WITH CTE AS(
    SELECT
        *
    FROM {{ref('fact_transactions')}}
    WHERE clave = 'Inm/'
    AND categoria NOT IN ('Comida', 'Regalos', 'Transporte')
    
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
    importe_moneda_principal,
    ingreso_gasto,
    comentario,
    importe_nativo,
    moneda,
    importe_txn,
    tipo_cambio,
    clave,
    valor,
    fecha_carga
FROM CTE
ORDER BY txn_time DESC