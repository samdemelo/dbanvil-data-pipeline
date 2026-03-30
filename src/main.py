from connection.data_sources import get_dbanvil_supabase_engine
from extract.data_extraction import get_users, get_diagram_summary, get_diagram_data
from transform.tabular_data_transform import transform_users
from datetime import datetime, timezone
from transform.diagram_data_transform import (
    get_diagram_dataset,
    get_table_dataset,
    get_index_dataset,
    get_column_dataset,
)

def main():
    engine = get_dbanvil_supabase_engine()

    users_df = get_users(engine)
    users_df = transform_users(users_df)

    diagrams_df = get_diagram_summary(engine)

    last_modified_date = datetime(2025, 1, 1, tzinfo=timezone.utc)
    diagram_data = get_diagram_data(engine, last_modified_date)
    diagrams_from_json_df = get_diagram_dataset(diagram_data)
    table_df = get_table_dataset(diagram_data)
    index_df = get_index_dataset(diagram_data)
    column_df = get_column_dataset(diagram_data)

 #   print(users_df.head())
 #   print(diagrams_df.head())
    print(diagrams_from_json_df.head())
    print(table_df.head())
    print(index_df.head())
    print(column_df.head())

#Python pads the name of the file with double underscores when the file is run directly via python.exe, otherwise __name__ (a built-in variable) = "main", e.g. if it's imported.
if __name__ == "__main__":
    main()