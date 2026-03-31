select
    cast(id as varchar) as diagram_id,
    cast(owner_id as varchar) as owner_id,
    trim(diagram_name) as diagram_name,
    lower(trim(db_system)) as db_system,
    legend_enabled,
    updated_at
from {{ source('raw', 'diagram') }}