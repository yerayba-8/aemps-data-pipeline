{{ config(materialized='view') }}

with datos_en_bruto as (
    select * from {{ source('aemps_raw_source', 'raw_medicamentos') }}
),

-- Usamos un CROSS JOIN LATERAL al llamar a la función en el select para duplicar las filas por cada principio activo que tenga el medicamento.

lista_desplegada as (
    select
        id as medicamento_id,
        jsonb_array_elements(data->'principios_activos') as pa_json
    from datos_en_bruto
)

select
    -- Generamos una clave compuesta única para esta tabla de staging.
    md5(concat(medicamento_id::text, '-', pa_json ->>'id')) as principio_activo_pk,
    medicamento_id,
    (pa_json ->>'id')::int as principio_activo_id,
    pa_json ->>'nombre' as principio_activo_nombre,
    pa_json ->>'cantidad' as cantidad,
    pa_json ->>'unidad' as unidad
from lista_desplegada
