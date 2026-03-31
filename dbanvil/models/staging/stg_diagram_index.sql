select
    cast(diagram_id as varchar) as diagram_id,
    cast(parent_table_id as varchar) as parent_table_id,
    cast(index_id as varchar) as index_id,
    trim(index_name) as index_name,
    lower(trim(uniqueness)) as uniqueness,
    lower(trim(index_type)) as index_type,
    column_count,
    load_timestamp
from {{ source('raw', 'diagram_index') }}