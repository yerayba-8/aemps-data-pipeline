{{ config(materialized='view') }}

with datos_en_bruto as (
    select * from {{ source('aemps_raw_source', 'raw_medicamentos') }}
),

extraer_vtm as (
    select
        id as medicamento_id,
        data->'vtm'->>'nombre' as vtm_nombre
    from datos_en_bruto
    where data->'vtm' is not null
)

select
    medicamento_id,
    md5(medicamento_id::text || trim(principio)) as principio_activo_id,
    trim(principio) as principio_activo_nombre
from extraer_vtm,
regexp_split_to_table(vtm_nombre, '\+') as principio
where vtm_nombre is not null