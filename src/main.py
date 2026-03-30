from connection.data_sources import get_connection_dbanvil_supabase
from extract.data_extraction import get_users, get_diagram_summary
from transform.tabular_data_transform import transform_users

def main():
    conn = get_connection_dbanvil_supabase()

    users_df = get_users(conn)
    users_df = transform_users(users_df)

    diagrams_df = get_diagram_summary(conn)

    print(users_df.head())
    print(diagrams_df.head())

    conn.close()

#Python pads the name of the file with double underscores when the file is run directly via python.exe, otherwise __name__ (a built-in variable) = "main", e.g. if it's imported.
if __name__ == "__main__":
    main()