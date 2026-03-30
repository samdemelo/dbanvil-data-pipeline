from connection.data_sources import get_dbanvil_supabase_engine
from extract.data_extraction import get_users, get_diagram_summary
from transform.tabular_data_transform import transform_users

def main():
    engine = get_dbanvil_supabase_engine()

    users_df = get_users(engine)
    users_df = transform_users(users_df)

    diagrams_df = get_diagram_summary(engine)

    print(users_df.head())
    print(diagrams_df.head())

#Python pads the name of the file with double underscores when the file is run directly via python.exe, otherwise __name__ (a built-in variable) = "main", e.g. if it's imported.
if __name__ == "__main__":
    main()