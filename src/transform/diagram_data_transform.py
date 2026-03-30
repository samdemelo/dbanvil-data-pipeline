import json
import pandas as pd


def _coerce_payload(payload):
    """
    Accepts either:
    - a Python dict (already parsed JSON)
    - a JSON string

    Returns a Python dict.
    """
    if isinstance(payload, dict):
        return payload

    if isinstance(payload, str):
        return json.loads(payload)

    raise TypeError(f"Unsupported payload type: {type(payload)}")


def _iter_diagram_rows(diagram_data: pd.DataFrame):
    """
    Expects diagram_data to contain:
    - id   : diagram id from the source table
    - data : JSON payload (dict or JSON string)

    Yields:
    - source_diagram_id
    - payload (dict)
    """
    required_columns = {"id", "data"}
    missing = required_columns - set(diagram_data.columns)

    if missing:
        raise ValueError(f"diagram_data is missing required columns: {missing}")

    for _, row in diagram_data.iterrows():
        source_diagram_id = row["id"]
        payload = _coerce_payload(row["data"])
        yield source_diagram_id, payload


def _get_project(payload: dict) -> dict:
    project = payload.get("project")
    if not isinstance(project, dict):
        raise ValueError("Payload does not contain a valid 'project' object.")
    return project


def _get_diagram_tables(payload: dict) -> list:
    diagram = payload.get("diagram")
    if not isinstance(diagram, dict):
        raise ValueError("Payload does not contain a valid 'diagram' object.")

    diagram_tables = diagram.get("diagramTables")
    if not isinstance(diagram_tables, list):
        raise ValueError("Payload does not contain a valid 'diagram.diagramTables' list.")

    return diagram_tables


def _extract_index_object(index_entry):
    """
    In the sample file, indexes are stored as:
        [index_id, { ...index object... }]

    This helper extracts the dict safely.
    """
    if isinstance(index_entry, list) and len(index_entry) == 2 and isinstance(index_entry[1], dict):
        return index_entry[1]

    raise ValueError(f"Unexpected index entry shape: {index_entry}")


def _build_fk_counts(payload: dict):
    """
    Returns two dicts:
    - inbound_counts[parent_table_id] = number of DISTINCT child tables referencing this table
    - outbound_counts[child_table_id] = number of DISTINCT parent tables this table references

    Based on the sample schema, foreign keys live inside each index object under:
        index.foreignKeys[]

    Each FK object contains:
        childTableId
    The parent table is the table currently being iterated.
    """
    inbound_sets = {}
    outbound_sets = {}

    diagram_tables = _get_diagram_tables(payload)

    for diagram_table in diagram_tables:
        table = diagram_table.get("table", {})
        parent_table_id = table.get("id")

        if parent_table_id is None:
            continue

        inbound_sets.setdefault(parent_table_id, set())

        for index_entry in table.get("indexes", []):
            index_obj = _extract_index_object(index_entry)

            for fk in index_obj.get("foreignKeys", []):
                child_table_id = fk.get("childTableId")

                if child_table_id is None:
                    continue

                inbound_sets.setdefault(parent_table_id, set()).add(child_table_id)
                outbound_sets.setdefault(child_table_id, set()).add(parent_table_id)

    inbound_counts = {table_id: len(child_tables) for table_id, child_tables in inbound_sets.items()}
    outbound_counts = {table_id: len(parent_tables) for table_id, parent_tables in outbound_sets.items()}

    return inbound_counts, outbound_counts


def get_diagram_dataset(diagram_data: pd.DataFrame) -> pd.DataFrame:
    """
    One row per diagram.
    Required output:
    - id
    - db_system
    - legend_enabled
    """
    rows = []

    for source_diagram_id, payload in _iter_diagram_rows(diagram_data):
        project = _get_project(payload)
        legend = project.get("legend", {})

        rows.append(
            {
                "id": source_diagram_id,
                "db_system": project.get("vendor"),
                "legend_enabled": legend.get("visible"),
            }
        )

    return pd.DataFrame(rows)


def get_table_dataset(diagram_data: pd.DataFrame) -> pd.DataFrame:
    """
    One row per table.
    Required output:
    - table_id
    - table_name
    - table_schema
    - referencing_table_count
    - referenced_table_count
    """
    rows = []

    for source_diagram_id, payload in _iter_diagram_rows(diagram_data):
        inbound_counts, outbound_counts = _build_fk_counts(payload)

        for diagram_table in _get_diagram_tables(payload):
            table = diagram_table.get("table", {})
            table_id = table.get("id")

            rows.append(
                {
                    "diagram_id": source_diagram_id,
                    "table_id": table_id,
                    "table_name": table.get("name"),
                    "table_schema": table.get("schema"),
                    "referencing_table_count": inbound_counts.get(table_id, 0),
                    "referenced_table_count": outbound_counts.get(table_id, 0),
                }
            )

    return pd.DataFrame(rows)


def get_index_dataset(diagram_data: pd.DataFrame) -> pd.DataFrame:
    """
    One row per index.
    Required output:
    - index_id
    - parent_table_id
    - index_name
    - uniqueness
    - index_type
    - number_of_columns
    """
    rows = []

    for source_diagram_id, payload in _iter_diagram_rows(diagram_data):
        for diagram_table in _get_diagram_tables(payload):
            table = diagram_table.get("table", {})
            parent_table_id = table.get("id")

            for index_entry in table.get("indexes", []):
                index_obj = _extract_index_object(index_entry)

                if index_obj.get("isPrimary"):
                    uniqueness = "PK"
                elif index_obj.get("isUnique"):
                    uniqueness = "UNIQUE"
                else:
                    uniqueness = "NON-UNIQUE"

                index_type_obj = index_obj.get("type")
                if isinstance(index_type_obj, dict):
                    index_type = index_type_obj.get("type")
                else:
                    index_type = index_type_obj

                rows.append(
                    {
                        "diagram_id": source_diagram_id,
                        "index_id": index_obj.get("id"),
                        "parent_table_id": parent_table_id,
                        "index_name": index_obj.get("name"),
                        "uniqueness": uniqueness,
                        "index_type": index_type,
                        "number_of_columns": len(index_obj.get("keyColumns", [])),
                    }
                )

    return pd.DataFrame(rows)


def get_column_dataset(diagram_data: pd.DataFrame) -> pd.DataFrame:
    """
    One row per column.
    Required output:
    - column_id
    - column_name
    - parent_table_id
    - data_type_name
    - has_identity_constraint
    - is_computed
    """
    rows = []

    for source_diagram_id, payload in _iter_diagram_rows(diagram_data):
        for diagram_table in _get_diagram_tables(payload):
            table = diagram_table.get("table", {})
            parent_table_id = table.get("id")

            for column in table.get("columns", []):
                data_type = column.get("dataType")

                data_type_name = None
                if isinstance(data_type, dict):
                    data_type_name = data_type.get("name")

                rows.append(
                    {
                        "diagram_id": source_diagram_id,
                        "column_id": column.get("id"),
                        "column_name": column.get("name"),
                        "parent_table_id": parent_table_id,
                        "data_type_name": data_type_name,
                        "has_identity_constraint": column.get("identity") is not None,
                        "is_computed": column.get("computed") is not None,
                    }
                )

    return pd.DataFrame(rows)