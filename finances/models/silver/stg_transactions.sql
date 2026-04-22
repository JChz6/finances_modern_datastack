{{
    config(
        materialized='incremental',
        incremental_strategy= 'delete+insert',
        unique_key= 'month_id'
    )
}}


WITH CTE AS (
    SELECT
        *
    FROM {{source('finances_raw', 'raw_transactions')}}

    {% if is_incremental() %}
        WHERE TO_VARCHAR(segun_un_periodo::DATE, 'YYYYMM')::INTEGER >= (SELECT DISTINCT month_id from {{ this }})
    {% endif %}

)
SELECT 
    MD5(
        CONCAT(
            segun_un_periodo,
            cuentas,
            pen,
            COALESCE(nota, 'transfer'),
            COALESCE(descripcion, 'null'),
            categoria,
            COALESCE(subcategorias, 'null')
        )
    ) AS txn_id,
    TO_VARCHAR(segun_un_periodo::DATE, 'YYYYMM')::INTEGER AS month_id,
    segun_un_periodo :: DATETIME AS txn_time,
    cuentas :: STRING AS cuenta,
    categoria :: STRING AS categoria,
    subcategorias :: STRING AS subcategoria,
    nota :: STRING AS concepto,
    pen :: FLOAT AS importe_moneda_principal,
    ingreso_gasto :: STRING AS ingreso_gasto,
    descripcion :: STRING AS comentario,
    importe :: FLOAT AS importe_nativo,
    moneda :: STRING AS moneda,
    cuentas_1 :: FLOAT AS importe_txn,
    archivo as src_file,
    CURRENT_TIMESTAMP AS fecha_carga 
FROM CTE