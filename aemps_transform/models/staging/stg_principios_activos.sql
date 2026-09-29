{{ config(materialized='view') }}

with datos_en_bruto as (
    select id, data
    from {{ source('aemps_raw_source', 'raw_medicamentos_detalle') }}
),

principios as (
    select
        d.id as medicamento_id,
        p.elemento->>'id'     as sustancia_id,
        p.elemento->>'nombre' as principio_activo_nombre,
        p.elemento->>'orden'  as orden
    from datos_en_bruto d
    cross join lateral jsonb_array_elements(
        case when jsonb_typeof(d.data->'principiosActivos') = 'array'
             then d.data->'principiosActivos'
             else '[]'::jsonb end
    ) as p(elemento)
)

select
    medicamento_id,
    md5(medicamento_id::text || '|' || coalesce(orden, '') || '|'
        || trim(principio_activo_nombre)) as principio_activo_id,
    sustancia_id,
    trim(principio_activo_nombre) as principio_activo_nombre
from principios
where principio_activo_nombre is not null