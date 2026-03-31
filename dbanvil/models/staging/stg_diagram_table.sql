select
    cast(diagram_id as varchar) as diagram_id,
    cast(table_id as varchar) as table_id,
    trim(table_name) as table_name,
    trim(table_schema) as table_schema,
    referencing_table_count,
    referenced_table_count,
    load_timestamp
from {{ source('raw', 'diagram_table') }}