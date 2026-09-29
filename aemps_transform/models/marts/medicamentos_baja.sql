{{ config(materialized='table') }}

select *
from {{ ref('dim_medicamentos') }}
where activo = false