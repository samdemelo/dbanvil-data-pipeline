## Analytics Dashboard (Power BI)

This repository includes a sample analytics dashboard built in Power BI using data from the DBAnvil data modeling platform.

The dashboard is backed by a full data pipeline:

- Source: Supabase/Postgres (OLTP)
- Extraction: Python ELT
- Storage: Snowflake (data warehouse)
- Transformation: dbt
- Orchestration: Airflow
- Visualization: Power BI

A static export of the dashboard is included below:

![Power BI Dashboard sample](/dbanvil_analytics.pdf)
