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

def get_snowflake_engine():
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    role = os.getenv("SNOWFLAKE_ROLE")

    if not all([user, password, account, warehouse, database, schema]):
        raise ValueError("One or more Snowflake environment variables are missing.")

    connection_string = (
        f"snowflake://{user}:{password}@{account}/"
        f"{database}/{schema}?warehouse={warehouse}"
    )

    if role:
        connection_string += f"&role={role}"

    return create_engine(connection_string)