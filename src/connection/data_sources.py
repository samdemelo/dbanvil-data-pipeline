import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

def get_dbanvil_supabase_engine():
    # Fetch variables
    USER = os.getenv("DBANVIL_USER")
    PASSWORD = os.getenv("DBANVIL_PASSWORD")
    HOST = os.getenv("DBANVIL_HOST")
    PORT = os.getenv("DBANVIL_PORT")
    DBNAME = os.getenv("DBANVIL_DBNAME")

    # Construct the SQLAlchemy connection string
    DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

    return create_engine(DATABASE_URL)