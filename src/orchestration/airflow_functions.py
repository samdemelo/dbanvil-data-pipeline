from datetime import datetime, timezone
import pandas as pd

from connection.data_sources import get_dbanvil_supabase_engine, get_snowflake_engine
from extract.data_extraction import get_users, get_diagram_summary, get_diagram_data
from transform.tabular_data_transform import transform_users
from transform.diagram_data_transform import (
    get_diagram_dataset,
    get_table_dataset,
    get_index_dataset,
    get_column_dataset,
)
from load.data_load import (
    load_users,
    load_diagram,
    load_diagram_table,
    load_diagram_index,
    load_diagram_column,
)
from load.run_metadata import get_last_pipeline_run_timestamp


FALLBACK_LAST_MODIFIED_DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)


# Added by ChatGPT
def _parse_load_timestamp(load_timestamp: str) -> pd.Timestamp:
    return pd.Timestamp(load_timestamp)


# Added by ChatGPT
def _get_last_modified_date(target_engine) -> datetime:
    return get_last_pipeline_run_timestamp(
        target_engine,
        fallback=FALLBACK_LAST_MODIFIED_DATE,
    )


def run_users_raw_load(load_timestamp: str) -> None:
    source_engine = get_dbanvil_supabase_engine()
    target_engine = get_snowflake_engine()

    parsed_load_timestamp = _parse_load_timestamp(load_timestamp)

    users_df = get_users(source_engine)
    users_df = transform_users(users_df)

    load_users(users_df, target_engine, parsed_load_timestamp)


def run_diagram_raw_load(load_timestamp: str) -> None:
    source_engine = get_dbanvil_supabase_engine()
    target_engine = get_snowflake_engine()

    parsed_load_timestamp = _parse_load_timestamp(load_timestamp)
    last_modified_date = _get_last_modified_date(target_engine)

    diagrams_df = get_diagram_summary(source_engine)
    diagram_data = get_diagram_data(source_engine, last_modified_date)
    diagrams_from_json_df = get_diagram_dataset(diagram_data)

    load_diagram(diagrams_df, diagrams_from_json_df, target_engine, parsed_load_timestamp)


def run_diagram_table_raw_load(load_timestamp: str) -> None:
    source_engine = get_dbanvil_supabase_engine()
    target_engine = get_snowflake_engine()

    parsed_load_timestamp = _parse_load_timestamp(load_timestamp)
    last_modified_date = _get_last_modified_date(target_engine)

    diagram_data = get_diagram_data(source_engine, last_modified_date)
    table_df = get_table_dataset(diagram_data)

    load_diagram_table(table_df, target_engine, parsed_load_timestamp)


def run_diagram_index_raw_load(load_timestamp: str) -> None:
    source_engine = get_dbanvil_supabase_engine()
    target_engine = get_snowflake_engine()

    parsed_load_timestamp = _parse_load_timestamp(load_timestamp)
    last_modified_date = _get_last_modified_date(target_engine)

    diagram_data = get_diagram_data(source_engine, last_modified_date)
    index_df = get_index_dataset(diagram_data)

    load_diagram_index(index_df, target_engine, parsed_load_timestamp)


def run_diagram_column_raw_load(load_timestamp: str) -> None:
    source_engine = get_dbanvil_supabase_engine()
    target_engine = get_snowflake_engine()

    parsed_load_timestamp = _parse_load_timestamp(load_timestamp)
    last_modified_date = _get_last_modified_date(target_engine)

    diagram_data = get_diagram_data(source_engine, last_modified_date)
    column_df = get_column_dataset(diagram_data)

    load_diagram_column(column_df, target_engine, parsed_load_timestamp)