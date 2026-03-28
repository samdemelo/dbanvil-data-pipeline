from extract.supabase import get_connection

def main():
    print("Pipeline starting...")

    conn = get_connection()
    print("Supabase connection successful.")

    conn.close()
    print("Connection closed.")

if __name__ == "__main__":
    main()