{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['diagram_id', 'index_id'],
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
            from {{ ref('stg_diagram_index') }} as source_scope
            where source_scope.diagram_id = target.diagram_id
        )
        and not exists (
            select 1
            from {{ ref('stg_diagram_index') }} as source_match
            where source_match.diagram_id = target.diagram_id
              and source_match.index_id = target.index_id
        )
        ",
        "
        delete from {{ this }} as target
        where not exists (
            select 1
            from {{ ref('core_diagram_table') }} as parent_table
            where parent_table.diagram_id = target.diagram_id
              and parent_table.table_id = target.parent_table_id
        )
        "
    ]
) }}

select
    diagram_id,
    parent_table_id,
    index_id,
    index_name,
    uniqueness,
    index_type,
    column_count,
    load_timestamp
from {{ ref('stg_diagram_index') }}