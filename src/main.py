from extract.supabase import get_connection

def main():
    print("Pipeline starting...")

    conn = get_connection()
    print("Supabase connection successful.")

    conn.close()
    print("Connection closed.")

#Python pads the name of the file with double underscores when the file is run directly via python.exe, otherwise __name__ (a built-in variable) = "main", e.g. if it's imported.
if __name__ == "__main__":
    main()