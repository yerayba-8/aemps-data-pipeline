{{ config(materialized='table') }}

with snapshot_vigente as (
    -- La versión más reciente de cada medicamento: si sigue activo, es la
    -- fila con dbt_valid_to nulo; si causó baja, es la última fila que
    -- tuvo antes de que dbt la invalidara. Por eso dim_medicamentos incluye
    -- TANTO los activos como los dados de baja, cada uno una sola vez.
    select distinct on (numero_registro) *
    from {{ ref('snapshot_medicamentos') }}
    order by numero_registro, dbt_valid_from desc
),

principios_activos as (
    select * from {{ ref('stg_principios_activos') }}
),

principios_agregados as (
    select
        medicamento_id,
        count(principio_activo_id) as numero_principios_activos,
        string_agg(principio_activo_nombre, ', ' order by principio_activo_nombre) as lista_principios_activos
    from principios_activos
    group by medicamento_id
)

select
    m.medicamento_id,
    m.numero_registro,
    m.nombre_medicamento,
    m.laboratorio_titular,
    m.condicion_prescripcion,
    coalesce(p.numero_principios_activos, 0) as numero_principios_activos,
    p.lista_principios_activos,
    m.fecha_ingesta,
    (m.dbt_valid_to is null) as activo,
    m.dbt_valid_to as fecha_baja
from snapshot_vigente m
left join principios_agregados p on m.medicamento_id = p.medicamento_id