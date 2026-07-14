{{ config(materialized='table') }}

with medicamentos as (
    select * from {{ ref('stg_medicamentos') }}
),

principios_activos as (
    select * from {{ ref('stg_principios_activos') }}
),

--Agrupamos los principios activos  por medicamentos antes de hacer el join principal
principios_agregados as (
    select
        medicamento_id,
        count(principio_activo_id) as  numero_principios_activos,
        -- Concatenamos los nombres  ordenados alfabeticamente separados por comas
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
    -- Si el medicamento no tiene principios  activos en el JSON, asignamos 0 y un texto descriptivo
    coalesce(p.numero_principios_activos,0) as numero_principios_activos,
    coalesce(p.lista_principios_activos, 'Sin principio activo especificado') as lista_principios_activos,
    m.fecha_ingesta
    from medicamentos m
    left join principios_agregados p on m.medicamento_id = p.medicamento_id