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

with source_data as (
    select
        diagram_id,
        owner_id,
        diagram_name,
        db_system,
        legend_enabled,
        updated_at,
        load_timestamp
    from {{ ref('stg_diagram') }}
)

{% if is_incremental() %}

, existing_data as (
    select
        diagram_id,
        db_system,
        legend_enabled
    from {{ this }}
)

select
    source_data.diagram_id,
    source_data.owner_id,
    source_data.diagram_name,
    coalesce(source_data.db_system, existing_data.db_system) as db_system,
    coalesce(source_data.legend_enabled, existing_data.legend_enabled) as legend_enabled,
    source_data.updated_at,
    source_data.load_timestamp
from source_data
left join existing_data
    on source_data.diagram_id = existing_data.diagram_id

{% else %}

select
    diagram_id,
    owner_id,
    diagram_name,
    db_system,
    legend_enabled,
    updated_at,
    load_timestamp
from source_data

{% endif %}