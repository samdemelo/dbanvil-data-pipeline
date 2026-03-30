import pandas as pd
from sqlalchemy import text

def get_users(engine):
    query = """
    SELECT 
        u.id,
        u.confirmation_sent_at,
        u.email_confirmed_at,
        u.created_at,
        u.last_sign_in_at,
        u.email,
        p.marketing_opt_in
    FROM auth.users u
    INNER JOIN profiles p ON p.id = u.id
    ORDER BY last_sign_in_at DESC;
    """

    df = pd.read_sql_query(query, engine)
    return df

def get_diagram_summary(engine):
    query = """
        SELECT id, owner_id, name, updated_at FROM diagrams d
        ORDER BY updated_at DESC;
    """

    df = pd.read_sql_query(query, engine)
    return df

def get_diagram_data(engine, last_modified_date):
    query = text("""
        SELECT id, data
        FROM diagrams d
        WHERE updated_at > :last_modified_date
        ORDER BY updated_at;
    """)

    df = pd.read_sql_query(
        query,
        engine,
        params={"last_modified_date": last_modified_date}
    )
    return df