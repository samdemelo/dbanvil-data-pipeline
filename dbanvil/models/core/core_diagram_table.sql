{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['diagram_id', 'table_id'],
    on_schema_change='sync_all_columns',
    post_hook=[
        "
        delete from {{ this }} as target
        where not exists (
            select 1
            from {{ ref('stg_diagram') }} as source
            where source.diagram_id = target.diagram_id
        )
        ",
        "
        delete from {{ this }} as target
        where exists (
            select 1
            from {{ ref('stg_diagram_table') }} as source_scope
            where source_scope.diagram_id = target.diagram_id
        )
        and not exists (
            select 1
            from {{ ref('stg_diagram_table') }} as source_match
            where source_match.diagram_id = target.diagram_id
              and source_match.table_id = target.table_id
        )
        "
    ]
) }}

select
    diagram_id,
    table_id,
    table_name,
    table_schema,
    referencing_table_count,
    referenced_table_count,
    load_timestamp
from {{ ref('stg_diagram_table') }}