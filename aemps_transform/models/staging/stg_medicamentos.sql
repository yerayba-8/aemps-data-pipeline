{{ config(materialized='view') }}

with datos_en_bruto as (
    select * from {{ source('aemps_raw_source', 'raw_medicamentos_detalle') }}
),

-- Solo los nregistro que el catálogo actual considera vigentes. Un
-- medicamento dado de baja sigue en raw_medicamentos_detalle (es
-- upsert-only), pero desaparece de aquí, y por tanto de este modelo.
activos as (
    select nregistro from {{ source('aemps_raw_source', 'raw_catalogo_activo') }}
)

select
    d.id as medicamento_id,
    d.nregistro as numero_registro,
    d.data->>'nombre' as nombre_medicamento,
    d.data->>'labtitular' as laboratorio_titular,
    (d.data->>'cpresc')::text as condicion_prescripcion,
    d.ingested_at as fecha_ingesta
from datos_en_bruto d
inner join activos a on d.nregistro = a.nregistro