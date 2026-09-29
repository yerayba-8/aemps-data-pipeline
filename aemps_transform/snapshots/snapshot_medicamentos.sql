{% snapshot snapshot_medicamentos %}

{{
    config(
        target_schema='snapshots',
        unique_key='numero_registro',
        strategy='check',
        check_cols=['nombre_medicamento', 'laboratorio_titular', 'condicion_prescripcion'],
        invalidate_hard_deletes=True,
    )
}}

select * from {{ ref('stg_medicamentos') }}

{% endsnapshot %}