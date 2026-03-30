import pandas as pd

def get_users(connection):
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
    INNER JOIN profiles p ON p.id = u.id;
    """

    df = pd.read_sql_query(query, connection)
    return df

def get_diagram_summary(connection):
    query = """
        SELECT id, owner_id, name, updated_at FROM diagrams d
        ORDER BY id;
    """

    df = pd.read_sql_query(query, connection)
    return df

def get_diagram_data(connection, last_modified_date):
    query = """
        SELECT id, data
        FROM diagrams d
        WHERE updated_at > %s 
        ORDER BY updated_at;
    """

    df = pd.read_sql_query(query, connection, params=(last_modified_date,))
    return df

def get_diagram_data2(connection, last_modified_date):
    query = """
        SELECT id, data
        FROM diagrams d
        WHERE updated_at > %(last_modified_date)s 
        ORDER BY updated_at;
    """

    df = pd.read_sql_query(query, connection, params = {"last_modified_date": last_modified_date})
    return df