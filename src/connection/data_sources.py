import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_connection_dbanvil_supabase():
    return psycopg2.connect(os.getenv("DBANVIL_DATABASE_URL"))