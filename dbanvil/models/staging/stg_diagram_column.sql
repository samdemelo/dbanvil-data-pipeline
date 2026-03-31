select
    cast(diagram_id as varchar) as diagram_id,
    cast(parent_table_id as varchar) as parent_table_id,
    cast(column_id as varchar) as column_id,
    trim(column_name) as column_name,
    lower(trim(data_type_name)) as data_type_name,
    has_identity_constraint,
    is_computed
from {{ source('raw', 'diagram_column') }}