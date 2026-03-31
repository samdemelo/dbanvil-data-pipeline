from datetime import datetime, timezone
from sqlalchemy import text


def get_last_pipeline_run_timestamp(
    engine,
    fallback: datetime | None = None,
) -> datetime:
    if fallback is None:
        fallback = datetime(2025, 1, 1, tzinfo=timezone.utc)

    query = text("""
        select max(load_timestamp) as last_run_timestamp
        from RAW.DIAGRAM
    """)

    with engine.connect() as connection:
        result = connection.execute(query).scalar()

    if result is None:
        return fallback

    if result.tzinfo is None:
        return result.replace(tzinfo=timezone.utc)

    return result