from datetime import datetime, timezone
import pandas as pd

from connection.data_sources import get_dbanvil_supabase_engine, get_snowflake_engine
from extract.data_extraction import get_users, get_diagram_summary, get_diagram_data
from transform.tabular_data_transform import transform_users
from transform.diagram_data_transform import (
    get_diagram_dataset,
    get_table_dataset,
    get_index_dataset,
    get_column_dataset,
)
from load.data_load import (
    load_users,
    load_diagram,
    load_diagram_table,
    load_diagram_index,
    load_diagram_column,
)
from load.run_metadata import get_last_pipeline_run_timestamp

def main():
    source_engine = get_dbanvil_supabase_engine()
    target_engine = get_snowflake_engine()

    load_timestamp = pd.Timestamp.utcnow()

    users_df = get_users(source_engine)
    users_df = transform_users(users_df)

    diagrams_df = get_diagram_summary(source_engine)

    # Get the datetime of the last run so we only pull the delta of diagram details
    last_modified_date = get_last_pipeline_run_timestamp(
        target_engine,
        fallback=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )

    diagram_data = get_diagram_data(source_engine, last_modified_date)

    diagrams_from_json_df = get_diagram_dataset(diagram_data)
    table_df = get_table_dataset(diagram_data)
    index_df = get_index_dataset(diagram_data)
    column_df = get_column_dataset(diagram_data)

    load_users(users_df, target_engine, load_timestamp)
    load_diagram(diagrams_df, diagrams_from_json_df, target_engine, load_timestamp)
    load_diagram_table(table_df, target_engine, load_timestamp)
    load_diagram_index(index_df, target_engine, load_timestamp)
    load_diagram_column(column_df, target_engine, load_timestamp)

#Python pads the name of the file with double underscores when the file is run directly via python.exe, otherwise __name__ (a built-in variable) = "main", e.g. if it's imported.
if __name__ == "__main__":
    main()