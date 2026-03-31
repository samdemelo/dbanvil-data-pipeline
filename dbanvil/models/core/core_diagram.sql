{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='diagram_id',
    on_schema_change='sync_all_columns',
    post_hook=[
        "
        delete from {{ this }} as target
        where not exists (
            select 1
            from {{ ref('stg_diagram') }} as source
            where source.diagram_id = target.diagram_id
        )
        "
    ]
) }}

select
    diagram_id,
    owner_id,
    diagram_name,
    db_system,
    legend_enabled,
    updated_at,
    load_timestamp
from {{ ref('stg_diagram') }}