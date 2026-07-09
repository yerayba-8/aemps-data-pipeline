{{ config(materialized='view') }}

with datos_en_bruto as (
    -- Usamos la función source() apuntando a lo que configuramos en el yaml
    select * from {{ source('aemps_raw_source', 'raw_medicamentos') }}
)

select
    id as medicamento_id,
    data->>'nregistro' as numero_registro,
    data ->>'nombre' as nombre_medicamento,
    data ->>'labtitular' as laboratorio_titular,
    (data ->>'cpresc')::text as condicion_prescripcion,
    ingested_at as fecha_ingesta
from datos_en_bruto