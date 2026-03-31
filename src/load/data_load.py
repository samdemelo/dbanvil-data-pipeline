import pandas as pd
from sqlalchemy import text


RAW_SCHEMA = "RAW"


def _truncate_table(engine, table_name: str, schema: str = RAW_SCHEMA) -> None:
    with engine.begin() as connection:
        connection.execute(text(f"TRUNCATE TABLE {schema}.{table_name}"))


def _append_dataframe(df: pd.DataFrame, engine, table_name: str, schema: str = RAW_SCHEMA) -> None:
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        method="multi",
    )

def _cast_columns_to_string(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = df.copy()

    for col in columns:
        if col in result.columns:
            result[col] = result[col].apply(lambda x: str(x) if x is not None else None)

    return result

def load_users(users_df: pd.DataFrame, engine) -> None:
    """
    Destination: RAW.USERS

    Expected input columns:
    - id
    - confirmation_sent_at
    - email_confirmed_at
    - created_at
    - last_sign_in_at
    - marketing_opt_in
    - email_domain
    """
    target_df = users_df[
        [
            "id",
            "confirmation_sent_at",
            "email_confirmed_at",
            "created_at",
            "last_sign_in_at",
            "marketing_opt_in",
            "email_domain",
        ]
    ].copy()

    target_df = _cast_columns_to_string(target_df, ["id"])

    _truncate_table(engine, "USERS")
    _append_dataframe(target_df, engine, "USERS")


def load_diagram(diagrams_df: pd.DataFrame, diagrams_from_json_df: pd.DataFrame, engine) -> None:
    """
    Destination: RAW.DIAGRAM

    diagrams_df columns:
    - id
    - owner_id
    - name
    - updated_at

    diagrams_from_json_df columns:
    - id
    - db_system
    - legend_enabled
    """
    target_df = diagrams_df.merge(
        diagrams_from_json_df,
        how="left",
        on="id",
        validate="one_to_one",
    ).copy()

    target_df = target_df.rename(
        columns={
            "name": "diagram_name",
        }
    )

    target_df = target_df[
        [
            "id",
            "owner_id",
            "diagram_name",
            "db_system",
            "legend_enabled",
            "updated_at",
        ]
    ]

    target_df = _cast_columns_to_string(target_df, ["id", "owner_id"])

    _truncate_table(engine, "DIAGRAM")
    _append_dataframe(target_df, engine, "DIAGRAM")


def load_diagram_table(table_df: pd.DataFrame, engine) -> None:
    """
    Destination: RAW.DIAGRAM_TABLE

    Expected input columns:
    - diagram_id
    - table_id
    - table_name
    - table_schema
    - referencing_table_count
    - referenced_table_count
    """
    target_df = table_df[
        [
            "diagram_id",
            "table_id",
            "table_name",
            "table_schema",
            "referencing_table_count",
            "referenced_table_count",
        ]
    ].copy()

    target_df = _cast_columns_to_string(target_df, ["diagram_id", "table_id"])

    _truncate_table(engine, "DIAGRAM_TABLE")
    _append_dataframe(target_df, engine, "DIAGRAM_TABLE")


def load_diagram_index(index_df: pd.DataFrame, engine) -> None:
    """
    Destination: RAW.DIAGRAM_INDEX

    Expected input columns:
    - diagram_id
    - parent_table_id
    - index_id
    - index_name
    - uniqueness
    - index_type
    - number_of_columns
    """
    target_df = index_df.rename(
        columns={
            "number_of_columns": "column_count",
        }
    ).copy()

    target_df = target_df[
        [
            "diagram_id",
            "parent_table_id",
            "index_id",
            "index_name",
            "uniqueness",
            "index_type",
            "column_count",
        ]
    ]

    target_df = _cast_columns_to_string(target_df, ["diagram_id", "parent_table_id", "index_id"])

    _truncate_table(engine, "DIAGRAM_INDEX")
    _append_dataframe(target_df, engine, "DIAGRAM_INDEX")


def load_diagram_column(column_df: pd.DataFrame, engine) -> None:
    """
    Destination: RAW.DIAGRAM_COLUMN

    Expected input columns:
    - diagram_id
    - parent_table_id
    - column_id
    - column_name
    - data_type_name
    - has_identity_constraint
    - is_computed
    """
    target_df = column_df[
        [
            "diagram_id",
            "parent_table_id",
            "column_id",
            "column_name",
            "data_type_name",
            "has_identity_constraint",
            "is_computed",
        ]
    ].copy()

    target_df = _cast_columns_to_string(target_df, ["diagram_id", "parent_table_id", "column_id"])

    _truncate_table(engine, "DIAGRAM_COLUMN")
    _append_dataframe(target_df, engine, "DIAGRAM_COLUMN")