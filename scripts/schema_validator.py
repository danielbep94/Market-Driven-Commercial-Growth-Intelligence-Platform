#!/usr/bin/env python3
"""Validate DataFrames against the semantic registry and detect Snowflake schema drift.

This script operates in two complementary modes:

**Mode 1 — DataFrame Validation** (``--validate`` / ``--validate-all``):
    Load ``configs/semantic_registry.yaml`` and verify that a PySpark or Pandas
    DataFrame matches the expected column names and types for a given source.
    Useful both as a Python API (``from scripts.schema_validator import
    validate_schema``) and as a CI gate (``--validate-all``).

**Mode 2 — Snowflake Drift Detection** (``--drift-check``):
    Connect to Snowflake and query live ``INFORMATION_SCHEMA.COLUMNS`` for every
    source table registered in the semantic registry.  Compare the live schema
    against the pinned snapshot (``configs/column_types_snapshot.yaml``) and
    report any additions, removals, type changes, or nullability changes.
    Exits with code 1 when drift is found so the check can be used in CI/CD
    pipelines.

Supported connection modes (same as ``extract_column_types.py``):
    1. **Environment variables** — SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
       SNOWFLAKE_ROLE (optional), SNOWFLAKE_ACCOUNT (defaults to
       ``danonenam.east-us-2.azure``).
    2. **Azure Key Vault** — set ``AZURE_KEY_VAULT_URL`` and the script uses
       ``azure-identity`` DefaultAzureCredential.

Usage
-----
    # Validate a single source:
    python -m scripts.schema_validator --validate SELL_IN

    # Validate every source in the registry (CI):
    python -m scripts.schema_validator --validate-all

    # Snowflake drift detection:
    python -m scripts.schema_validator --drift-check

    # Custom paths:
    python -m scripts.schema_validator --drift-check \\
        --registry configs/semantic_registry.yaml \\
        --snapshot configs/column_types_snapshot.yaml

Public API
----------
    >>> from scripts.schema_validator import validate_schema
    >>> df = spark.table('bronze.sell_in')
    >>> validate_schema(df, source='SELL_IN')  # raises SchemaValidationError on mismatch
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Union

try:
    import yaml
except ImportError:
    sys.exit("ERROR: PyYAML is required.  Install it with:  pip install pyyaml")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger("schema_validator")

# ---------------------------------------------------------------------------
# Constants — Snowflake connection (mirrors extract_column_types.py)
# ---------------------------------------------------------------------------

SNOWFLAKE_HOST: str = "danonenam.east-us-2.azure.snowflakecomputing.com"
SNOWFLAKE_ACCOUNT: str = "danonenam.east-us-2.azure"
WAREHOUSE: str = "PRD_MDP_ANL_WH"

DEFAULT_REGISTRY_PATH: Path = Path("configs/semantic_registry.yaml")
DEFAULT_SNAPSHOT_PATH: Path = Path("configs/column_types_snapshot.yaml")

# ---------------------------------------------------------------------------
# Snowflake → Spark type mapping
# ---------------------------------------------------------------------------

#: Maps Snowflake type families to the set of compatible PySpark type names.
#: A Spark column is considered valid when its ``typeName()`` (lower-cased)
#: appears in the set for the expected Snowflake type family.
SNOWFLAKE_TO_SPARK: dict[str, set[str]] = {
    # Exact-numeric types
    "NUMBER":   {"decimal", "long", "integer", "int", "bigint", "short", "double"},
    "DECIMAL":  {"decimal", "long", "integer", "int", "bigint", "short", "double"},
    "NUMERIC":  {"decimal", "long", "integer", "int", "bigint", "short", "double"},
    "BIGINT":   {"long", "bigint"},
    "INT":      {"integer", "int"},
    "INTEGER":  {"integer", "int"},
    "SMALLINT": {"integer", "int", "short"},
    "TINYINT":  {"integer", "int", "short", "byte"},
    # Floating-point types
    "FLOAT":    {"double", "float"},
    "DOUBLE":   {"double"},
    "REAL":     {"double", "float"},
    # String types
    "VARCHAR":  {"string"},
    "TEXT":     {"string"},
    "STRING":   {"string"},
    "CHAR":     {"string"},
    # Date / Time
    "DATE":          {"date"},
    "TIMESTAMP":     {"timestamp"},
    "TIMESTAMP_NTZ": {"timestamp"},
    "TIMESTAMP_LTZ": {"timestamp"},
    "TIMESTAMP_TZ":  {"timestamp"},
    # Boolean
    "BOOLEAN":  {"boolean"},
    # Binary
    "BINARY":   {"binary"},
    "VARBINARY": {"binary"},
    # Variant / semi-structured (Snowflake-specific)
    "VARIANT":  {"string", "map", "struct", "array"},
    "OBJECT":   {"string", "map", "struct"},
    "ARRAY":    {"string", "array"},
}

#: Reverse mapping: Spark ``typeName()`` → primary Snowflake type name.
#: Used when generating human-readable error messages.
SPARK_TO_SNOWFLAKE: dict[str, str] = {
    "decimal":   "NUMBER",
    "long":      "BIGINT",
    "integer":   "INTEGER",
    "int":       "INTEGER",
    "short":     "SMALLINT",
    "byte":      "TINYINT",
    "double":    "DOUBLE",
    "float":     "FLOAT",
    "string":    "VARCHAR",
    "date":      "DATE",
    "timestamp": "TIMESTAMP",
    "boolean":   "BOOLEAN",
    "binary":    "BINARY",
}


# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------

class SchemaValidationError(Exception):
    """Raised when a DataFrame does not match the expected schema.

    Attributes
    ----------
    source : str
        Name of the source that was validated (e.g. ``SELL_IN``).
    missing_columns : list[str]
        Columns expected by the registry but absent in the DataFrame.
    type_mismatches : list[dict[str, str]]
        Per-column type mismatches with ``column``, ``expected``, ``actual``.
    unexpected_columns : list[str]
        Columns present in the DataFrame but not declared in the registry.
    """

    def __init__(
        self,
        source: str,
        missing_columns: list[str],
        type_mismatches: list[dict[str, str]],
        unexpected_columns: list[str],
    ) -> None:
        self.source = source
        self.missing_columns = missing_columns
        self.type_mismatches = type_mismatches
        self.unexpected_columns = unexpected_columns

        parts: list[str] = [f"Schema validation failed for source '{source}':"]
        if missing_columns:
            parts.append(f"  Missing columns ({len(missing_columns)}): "
                         + ", ".join(missing_columns))
        if type_mismatches:
            parts.append(f"  Type mismatches ({len(type_mismatches)}):")
            for m in type_mismatches:
                parts.append(
                    f"    {m['column']}: expected {m['expected']}, "
                    f"got {m['actual']}"
                )
        if unexpected_columns:
            parts.append(f"  Unexpected columns ({len(unexpected_columns)}): "
                         + ", ".join(unexpected_columns))
        super().__init__("\n".join(parts))


# ---------------------------------------------------------------------------
# Drift report dataclass
# ---------------------------------------------------------------------------

@dataclass
class ColumnDrift:
    """A single column-level drift observation."""

    table: str
    column: str
    kind: str  # "added" | "dropped" | "type_changed" | "nullability_changed"
    detail: str


@dataclass
class DriftReport:
    """Aggregated drift report across all tables."""

    drifts: list[ColumnDrift] = field(default_factory=list)

    @property
    def has_drift(self) -> bool:
        """Return ``True`` when any drift was detected."""
        return len(self.drifts) > 0

    def summary(self) -> str:
        """Return a human-readable drift report."""
        if not self.has_drift:
            return "✓ No schema drift detected."

        lines: list[str] = []
        # Group by table
        tables: dict[str, list[ColumnDrift]] = {}
        for d in self.drifts:
            tables.setdefault(d.table, []).append(d)

        for table, items in sorted(tables.items()):
            lines.append(f"\nDRIFT DETECTED in {table}:")
            for item in items:
                lines.append(f"  COLUMN '{item.column}': {item.detail}")

        lines.append(
            "\n→ Run: python scripts/extract_column_types.py "
            "&& python scripts/build_registry.py"
        )
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# YAML loaders
# ---------------------------------------------------------------------------

def _load_yaml(path: Path) -> dict[str, Any]:
    """Load a YAML file and return its contents as a dict.

    Parameters
    ----------
    path:
        Absolute or relative path to the YAML file.

    Returns
    -------
    dict
        Parsed YAML content.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    """
    resolved = path.resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"YAML file not found: {resolved}")
    with resolved.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"Expected a YAML mapping at top level, got {type(data).__name__}")
    logger.debug("Loaded YAML: %s (%d top-level keys)", resolved, len(data))
    return data


def load_registry(path: Path = DEFAULT_REGISTRY_PATH) -> dict[str, Any]:
    """Load the semantic registry.

    Parameters
    ----------
    path:
        Path to ``semantic_registry.yaml``.

    Returns
    -------
    dict
        Full registry content.  Expected to have a ``sources`` key whose
        values are dicts with a ``columns`` mapping.
    """
    return _load_yaml(path)


def load_snapshot(path: Path = DEFAULT_SNAPSHOT_PATH) -> dict[str, Any]:
    """Load the column-types snapshot.

    Parameters
    ----------
    path:
        Path to ``column_types_snapshot.yaml``.

    Returns
    -------
    dict
        Snapshot content.  Expected to have a ``tables`` key.
    """
    return _load_yaml(path)


# ---------------------------------------------------------------------------
# Helpers — Spark type extraction
# ---------------------------------------------------------------------------

def _get_spark_type_name(spark_type: Any) -> str:
    """Extract a lower-cased type name string from a PySpark DataType.

    Handles both ``pyspark.sql.types.DataType`` instances and plain strings
    so that the function also works with Pandas dtype names.

    Parameters
    ----------
    spark_type:
        A PySpark ``DataType`` object, a ``StructField.dataType``, or a
        string representation.

    Returns
    -------
    str
        Lower-cased type name (e.g. ``"string"``, ``"decimal"``, ``"long"``).
    """
    if isinstance(spark_type, str):
        # Strip parenthesised precision, e.g. "decimal(38,2)" → "decimal"
        base = spark_type.split("(")[0].strip().lower()
        return base

    # PySpark DataType objects
    type_name: str = getattr(spark_type, "typeName", lambda: str(spark_type))()
    return type_name.lower()


def _get_pandas_dtype_name(dtype: Any) -> str:
    """Map a Pandas / NumPy dtype to a Spark-equivalent type name.

    Parameters
    ----------
    dtype:
        A ``pandas.Series.dtype`` value.

    Returns
    -------
    str
        Spark-equivalent type name string.
    """
    name = str(dtype).lower()

    if "int64" in name or "int64" in name:
        return "long"
    if "int32" in name:
        return "integer"
    if "int16" in name or "int8" in name:
        return "short"
    if "float64" in name or "float" in name:
        return "double"
    if "bool" in name:
        return "boolean"
    if "datetime" in name:
        return "timestamp"
    if "object" in name or "string" in name or "str" in name:
        return "string"
    # Fallback
    return name


def _normalize_snowflake_type(raw_type: str) -> str:
    """Normalise a Snowflake type string to a family key.

    Examples::

        "NUMBER(38,2)"   → "NUMBER"
        "VARCHAR(16777216)" → "VARCHAR"
        "TIMESTAMP_NTZ(9)" → "TIMESTAMP_NTZ"

    Parameters
    ----------
    raw_type:
        Raw type string from the registry or snapshot.

    Returns
    -------
    str
        Upper-cased type family key.
    """
    return raw_type.split("(")[0].strip().upper()


# ---------------------------------------------------------------------------
# Mode 1 — DataFrame validation
# ---------------------------------------------------------------------------

def _extract_columns_from_registry(
    registry: dict[str, Any],
    source: str,
) -> dict[str, str]:
    """Extract the expected column → type mapping for *source* from the registry.

    The function looks for the source under ``registry["sources"][source]``
    and expects each entry in its ``columns`` list/dict to carry a ``name``
    and a ``snowflake_type`` (or ``data_type``, ``type``).

    Parameters
    ----------
    registry:
        Loaded semantic registry dict.
    source:
        Source name (e.g. ``SELL_IN``).

    Returns
    -------
    dict[str, str]
        ``{COLUMN_NAME: SNOWFLAKE_TYPE, ...}``

    Raises
    ------
    KeyError
        If the source is not found in the registry.
    """
    sources = registry.get("sources", registry)

    # Case-insensitive lookup
    source_upper = source.upper()
    source_data: dict[str, Any] | None = None
    for key, val in sources.items():
        if key.upper() == source_upper:
            source_data = val
            break

    if source_data is None:
        available = sorted(
            k for k in sources if k != "_meta"
        )
        raise KeyError(
            f"Source '{source}' not found in registry.  "
            f"Available sources: {', '.join(available)}"
        )

    # Determine where columns live
    columns_raw = source_data.get("columns", {})

    result: dict[str, str] = {}

    if isinstance(columns_raw, dict):
        # {COL_NAME: {data_type: ..., ...}, ...}
        for col_name, col_meta in columns_raw.items():
            if isinstance(col_meta, dict):
                col_type = (
                    col_meta.get("snowflake_type")
                    or col_meta.get("data_type")
                    or col_meta.get("type", "VARCHAR")
                )
            else:
                col_type = str(col_meta)
            result[col_name.upper()] = col_type
    elif isinstance(columns_raw, list):
        # [{name: COL, type: ...}, ...]
        for entry in columns_raw:
            if isinstance(entry, dict):
                col_name = entry.get("name", entry.get("column", ""))
                col_type = (
                    entry.get("snowflake_type")
                    or entry.get("data_type")
                    or entry.get("type", "VARCHAR")
                )
                result[col_name.upper()] = col_type
            elif isinstance(entry, str):
                result[entry.upper()] = "VARCHAR"

    return result


def _is_type_compatible(snowflake_type: str, spark_type_name: str) -> bool:
    """Check if a Spark type is compatible with a Snowflake type.

    Parameters
    ----------
    snowflake_type:
        Raw Snowflake type string (e.g. ``"NUMBER(38,2)"``).
    spark_type_name:
        Lower-cased Spark type name (e.g. ``"decimal"``).

    Returns
    -------
    bool
    """
    family = _normalize_snowflake_type(snowflake_type)
    compatible = SNOWFLAKE_TO_SPARK.get(family)
    if compatible is None:
        logger.warning(
            "Unknown Snowflake type family '%s' (from '%s'); "
            "skipping type check.",
            family,
            snowflake_type,
        )
        return True  # Don't fail on unknown types
    return spark_type_name in compatible


def validate_schema(
    df: Any,
    source: str,
    registry_path: Path = DEFAULT_REGISTRY_PATH,
    *,
    strict: bool = True,
    warn_unexpected: bool = True,
) -> None:
    """Validate a DataFrame against the semantic registry.

    This is the main public API for programmatic validation (e.g. inside
    a Databricks notebook or a pytest fixture).

    Parameters
    ----------
    df:
        A PySpark ``DataFrame`` or a Pandas ``DataFrame``.
    source:
        Source name in the registry (e.g. ``"SELL_IN"``).
    registry_path:
        Path to the semantic-registry YAML file.
    strict:
        If ``True`` (default), raise :class:`SchemaValidationError` on any
        column-presence or type mismatch.  If ``False``, log warnings only.
    warn_unexpected:
        If ``True`` (default), include unexpected columns in the error /
        warning output.  Set to ``False`` to ignore extra columns.

    Raises
    ------
    SchemaValidationError
        When ``strict=True`` and mismatches are detected.
    FileNotFoundError
        When the registry file does not exist.
    KeyError
        When the source is not found in the registry.
    """
    registry = load_registry(registry_path)
    expected_columns = _extract_columns_from_registry(registry, source)

    if not expected_columns:
        logger.warning(
            "No columns defined for source '%s' in registry; "
            "skipping validation.",
            source,
        )
        return

    # Detect framework: PySpark vs Pandas
    is_pyspark = hasattr(df, "schema") and hasattr(df.schema, "fields")

    if is_pyspark:
        actual_columns: dict[str, str] = {
            f.name.upper(): _get_spark_type_name(f.dataType)
            for f in df.schema.fields
        }
    else:
        # Pandas
        actual_columns = {
            col.upper(): _get_pandas_dtype_name(df[col].dtype)
            for col in df.columns
        }

    actual_col_names = set(actual_columns.keys())
    expected_col_names = set(expected_columns.keys())

    # --- Check 1: missing columns ----
    missing = sorted(expected_col_names - actual_col_names)

    # --- Check 2: type compatibility ---
    type_mismatches: list[dict[str, str]] = []
    for col_name in sorted(expected_col_names & actual_col_names):
        sf_type = expected_columns[col_name]
        spark_name = actual_columns[col_name]
        if not _is_type_compatible(sf_type, spark_name):
            type_mismatches.append(
                {
                    "column": col_name,
                    "expected": f"{sf_type} → "
                                f"{', '.join(sorted(SNOWFLAKE_TO_SPARK.get(_normalize_snowflake_type(sf_type), set())))}",
                    "actual": spark_name,
                }
            )

    # --- Check 3: unexpected columns ---
    unexpected = sorted(actual_col_names - expected_col_names) if warn_unexpected else []

    has_issues = bool(missing or type_mismatches)
    if has_issues or unexpected:
        if has_issues and strict:
            raise SchemaValidationError(
                source=source,
                missing_columns=missing,
                type_mismatches=type_mismatches,
                unexpected_columns=unexpected,
            )
        # Non-strict: log warnings
        if missing:
            logger.warning(
                "Source '%s': missing columns: %s", source, ", ".join(missing)
            )
        for m in type_mismatches:
            logger.warning(
                "Source '%s': type mismatch on %s — expected %s, got %s",
                source, m["column"], m["expected"], m["actual"],
            )
        if unexpected:
            logger.info(
                "Source '%s': unexpected columns (not in registry): %s",
                source, ", ".join(unexpected),
            )
    else:
        logger.info("✓ Source '%s': schema valid (%d columns).", source, len(expected_columns))


# ---------------------------------------------------------------------------
# Mode 2 — Snowflake drift detection
# ---------------------------------------------------------------------------

def _get_snowflake_credentials() -> dict[str, str]:
    """Resolve Snowflake credentials from env vars or Azure Key Vault.

    Returns
    -------
    dict
        Keys: ``user``, ``password``, and optionally ``role``.

    Raises
    ------
    SystemExit
        If required credentials are missing.
    """
    vault_url = os.environ.get("AZURE_KEY_VAULT_URL")

    if vault_url:
        try:
            from azure.identity import DefaultAzureCredential  # type: ignore[import-untyped]
            from azure.keyvault.secrets import SecretClient  # type: ignore[import-untyped]

            credential = DefaultAzureCredential()
            client = SecretClient(vault_url=vault_url, credential=credential)
            return {
                "user": client.get_secret("snowflake-user").value,
                "password": client.get_secret("snowflake-password").value,
                "role": client.get_secret("snowflake-role").value or "",
            }
        except ImportError:
            sys.exit(
                "ERROR: azure-identity and azure-keyvault-secrets are required "
                "when AZURE_KEY_VAULT_URL is set.  Install them with:\n"
                "  pip install azure-identity azure-keyvault-secrets"
            )
        except Exception as exc:
            sys.exit(f"ERROR: Failed to retrieve credentials from Key Vault: {exc}")

    # Fall back to environment variables
    user = os.environ.get("SNOWFLAKE_USER")
    password = os.environ.get("SNOWFLAKE_PASSWORD")
    if not user or not password:
        sys.exit(
            "ERROR: Set SNOWFLAKE_USER and SNOWFLAKE_PASSWORD environment "
            "variables, or set AZURE_KEY_VAULT_URL for Key Vault auth."
        )
    return {
        "user": user,
        "password": password,
        "role": os.environ.get("SNOWFLAKE_ROLE", ""),
    }


def _connect_snowflake() -> Any:
    """Create and return a Snowflake connection.

    Returns
    -------
    snowflake.connector.SnowflakeConnection
    """
    try:
        import snowflake.connector  # type: ignore[import-untyped]
    except ImportError:
        sys.exit(
            "ERROR: snowflake-connector-python is required.  Install it with:\n"
            "  pip install snowflake-connector-python"
        )

    creds = _get_snowflake_credentials()
    connect_kwargs: dict[str, Any] = {
        "account": os.environ.get("SNOWFLAKE_ACCOUNT", SNOWFLAKE_ACCOUNT),
        "host": SNOWFLAKE_HOST,
        "user": creds["user"],
        "password": creds["password"],
        "warehouse": WAREHOUSE,
    }
    if creds.get("role"):
        connect_kwargs["role"] = creds["role"]

    try:
        conn = snowflake.connector.connect(**connect_kwargs)
        logger.info("✓ Connected to Snowflake (%s)", SNOWFLAKE_HOST)
        return conn
    except Exception as exc:
        sys.exit(f"ERROR: Snowflake connection failed: {exc}")


def _extract_tables_from_snapshot(
    snapshot: dict[str, Any],
) -> dict[str, dict[str, dict[str, Any]]]:
    """Extract the ``tables`` section from a column-types snapshot.

    Parameters
    ----------
    snapshot:
        Loaded snapshot dict.

    Returns
    -------
    dict
        ``{FQ_TABLE: {COL: {data_type, is_nullable, ...}}}``
    """
    tables = snapshot.get("tables", {})
    if not tables:
        raise ValueError("Snapshot contains no 'tables' key or it is empty.")
    return tables


def _query_live_columns(
    conn: Any,
    database: str,
    schema: str,
    table_names: list[str],
) -> dict[str, dict[str, dict[str, Any]]]:
    """Query live INFORMATION_SCHEMA.COLUMNS for a set of tables.

    Parameters
    ----------
    conn:
        Active Snowflake connection.
    database:
        Snowflake database name.
    schema:
        Snowflake schema name.
    table_names:
        List of table/view names.

    Returns
    -------
    dict
        ``{FQ_TABLE: {COL: {data_type, is_nullable, ...}}}``
    """
    fields = (
        "TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, "
        "DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, "
        "NUMERIC_PRECISION, NUMERIC_SCALE"
    )
    in_list = ", ".join(f"'{t}'" for t in sorted(table_names))
    sql = (
        f"SELECT {fields}\n"
        f"  FROM {database}.INFORMATION_SCHEMA.COLUMNS\n"
        f" WHERE TABLE_SCHEMA = '{schema}'\n"
        f"   AND TABLE_NAME IN ({in_list})\n"
        f" ORDER BY TABLE_NAME, COLUMN_NAME;"
    )

    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()

    result: dict[str, dict[str, dict[str, Any]]] = {}
    for row in rows:
        fq_table = f"{row[0]}.{row[1]}.{row[2]}"
        col_name = row[3]
        col_meta: dict[str, Any] = {
            "data_type": row[4],
            "is_nullable": row[5],
        }
        if row[6] is not None:
            col_meta["character_maximum_length"] = row[6]
        if row[7] is not None:
            col_meta["numeric_precision"] = row[7]
        if row[8] is not None:
            col_meta["numeric_scale"] = row[8]

        if fq_table not in result:
            result[fq_table] = {}
        result[fq_table][col_name] = col_meta

    return result


def _format_type_with_params(col_meta: dict[str, Any]) -> str:
    """Build a human-readable type string from column metadata.

    Examples: ``NUMBER(38,2)``, ``VARCHAR(100)``, ``DATE``.

    Parameters
    ----------
    col_meta:
        Column metadata dict with ``data_type`` and optional precision fields.

    Returns
    -------
    str
        Formatted type string.
    """
    dt = col_meta.get("data_type", "UNKNOWN")
    precision = col_meta.get("numeric_precision")
    scale = col_meta.get("numeric_scale")
    max_len = col_meta.get("character_maximum_length")

    if precision is not None and scale is not None:
        return f"{dt}({precision},{scale})"
    if precision is not None:
        return f"{dt}({precision})"
    if max_len is not None:
        return f"{dt}({max_len})"
    return dt


def _diff_table(
    table_name: str,
    snapshot_cols: dict[str, dict[str, Any]],
    live_cols: dict[str, dict[str, Any]],
) -> list[ColumnDrift]:
    """Compare snapshot columns against live columns for a single table.

    Parameters
    ----------
    table_name:
        Fully-qualified table name.
    snapshot_cols:
        Column metadata from the snapshot.
    live_cols:
        Column metadata from live INFORMATION_SCHEMA.

    Returns
    -------
    list[ColumnDrift]
        Drift observations (may be empty).
    """
    drifts: list[ColumnDrift] = []
    snapshot_names = set(snapshot_cols.keys())
    live_names = set(live_cols.keys())

    # Added columns (in live, not in snapshot)
    for col in sorted(live_names - snapshot_names):
        drifts.append(
            ColumnDrift(
                table=table_name,
                column=col,
                kind="added",
                detail="added (not in snapshot)",
            )
        )

    # Dropped columns (in snapshot, not in live)
    for col in sorted(snapshot_names - live_names):
        drifts.append(
            ColumnDrift(
                table=table_name,
                column=col,
                kind="dropped",
                detail="dropped (no longer in live schema)",
            )
        )

    # Columns in both — check type and nullability changes
    for col in sorted(snapshot_names & live_names):
        snap = snapshot_cols[col]
        live = live_cols[col]

        snap_type = _format_type_with_params(snap)
        live_type = _format_type_with_params(live)

        if snap_type != live_type:
            drifts.append(
                ColumnDrift(
                    table=table_name,
                    column=col,
                    kind="type_changed",
                    detail=f"type changed {snap_type} → {live_type}",
                )
            )

        snap_nullable = snap.get("is_nullable", "YES")
        live_nullable = live.get("is_nullable", "YES")
        if snap_nullable != live_nullable:
            drifts.append(
                ColumnDrift(
                    table=table_name,
                    column=col,
                    kind="nullability_changed",
                    detail=f"nullability changed {snap_nullable} → {live_nullable}",
                )
            )

    return drifts


def detect_drift(
    snapshot_path: Path = DEFAULT_SNAPSHOT_PATH,
) -> DriftReport:
    """Connect to Snowflake and detect schema drift vs. the pinned snapshot.

    Parameters
    ----------
    snapshot_path:
        Path to ``column_types_snapshot.yaml``.

    Returns
    -------
    DriftReport
        Report containing all detected drifts.
    """
    snapshot = load_snapshot(snapshot_path)
    snapshot_tables = _extract_tables_from_snapshot(snapshot)

    # Group tables by database.schema for efficient querying
    groups: dict[str, list[str]] = {}
    for fq_table in snapshot_tables:
        parts = fq_table.split(".")
        if len(parts) != 3:
            logger.warning("Skipping malformed table key: %s", fq_table)
            continue
        db_schema = f"{parts[0]}.{parts[1]}"
        table_name = parts[2]
        groups.setdefault(db_schema, []).append(table_name)

    conn = _connect_snowflake()
    report = DriftReport()

    try:
        for db_schema, table_names in sorted(groups.items()):
            database, schema = db_schema.split(".")
            logger.info(
                "Querying live schema for %s (%d tables)…",
                db_schema,
                len(table_names),
            )
            live_tables = _query_live_columns(conn, database, schema, table_names)

            for fq_table in sorted(
                fq for fq in snapshot_tables if fq.startswith(db_schema + ".")
            ):
                snap_cols = snapshot_tables[fq_table].get("columns", {})
                # If snapshot entry stores columns at top level (no "columns" key)
                if not snap_cols and isinstance(snapshot_tables[fq_table], dict):
                    # Check if keys look like column names (not metadata keys)
                    non_meta = {
                        k: v
                        for k, v in snapshot_tables[fq_table].items()
                        if isinstance(v, dict)
                    }
                    if non_meta:
                        snap_cols = non_meta

                live_cols = live_tables.get(fq_table, {})

                if not live_cols:
                    logger.warning(
                        "Table %s not found in live INFORMATION_SCHEMA (may "
                        "have been dropped or access denied).",
                        fq_table,
                    )
                    # Mark every snapshot column as dropped
                    for col in sorted(snap_cols.keys()):
                        report.drifts.append(
                            ColumnDrift(
                                table=fq_table,
                                column=col,
                                kind="dropped",
                                detail="entire table missing from live schema",
                            )
                        )
                    continue

                report.drifts.extend(_diff_table(fq_table, snap_cols, live_cols))
    finally:
        conn.close()
        logger.info("✓ Snowflake connection closed.")

    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _configure_logging(verbosity: int = 0) -> None:
    """Set up logging for the CLI.

    Parameters
    ----------
    verbosity:
        0 = INFO, 1+ = DEBUG.
    """
    level = logging.DEBUG if verbosity else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Parameters
    ----------
    argv:
        Argument list (defaults to ``sys.argv[1:]``).

    Returns
    -------
    argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        prog="schema_validator",
        description=(
            "Validate DataFrames against the semantic registry and/or detect "
            "Snowflake schema drift."
        ),
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--validate",
        metavar="SOURCE_NAME",
        help="Validate a specific source against the semantic registry.",
    )
    mode.add_argument(
        "--validate-all",
        action="store_true",
        default=False,
        help="Validate all sources defined in the semantic registry (for CI).",
    )
    mode.add_argument(
        "--drift-check",
        action="store_true",
        default=False,
        help="Detect Snowflake schema drift vs. the column-types snapshot.",
    )

    parser.add_argument(
        "--registry",
        type=Path,
        default=DEFAULT_REGISTRY_PATH,
        help=(
            "Path to semantic_registry.yaml "
            f"(default: {DEFAULT_REGISTRY_PATH})."
        ),
    )
    parser.add_argument(
        "--snapshot",
        type=Path,
        default=DEFAULT_SNAPSHOT_PATH,
        help=(
            "Path to column_types_snapshot.yaml "
            f"(default: {DEFAULT_SNAPSHOT_PATH})."
        ),
    )
    parser.add_argument(
        "-v", "--verbose",
        action="count",
        default=0,
        help="Increase output verbosity (can be repeated).",
    )

    return parser.parse_args(argv)


def _cli_validate_source(source: str, registry_path: Path) -> bool:
    """Validate a single source via CLI (no live DataFrame — schema-only check).

    In CLI mode we cannot load a live DataFrame, so we perform a structural
    validation of the registry entry: verify that columns are defined and
    types are recognised.

    Parameters
    ----------
    source:
        Source name.
    registry_path:
        Path to the semantic registry.

    Returns
    -------
    bool
        ``True`` if valid, ``False`` if issues found.
    """
    registry = load_registry(registry_path)
    try:
        expected = _extract_columns_from_registry(registry, source)
    except KeyError as exc:
        logger.error(str(exc))
        return False

    if not expected:
        logger.warning("Source '%s' has no columns defined.", source)
        return False

    issues: list[str] = []
    for col_name, sf_type in expected.items():
        family = _normalize_snowflake_type(sf_type)
        if family not in SNOWFLAKE_TO_SPARK:
            issues.append(
                f"  Column '{col_name}': unrecognised Snowflake type '{sf_type}'"
            )

    if issues:
        logger.error(
            "Source '%s' has %d type issue(s):\n%s",
            source,
            len(issues),
            "\n".join(issues),
        )
        return False

    print(f"✓ Source '{source}': {len(expected)} column(s), all types valid.")
    return True


def main(argv: list[str] | None = None) -> None:
    """CLI entry point for schema validation and drift detection.

    Parameters
    ----------
    argv:
        Optional argument list for testing; defaults to ``sys.argv[1:]``.
    """
    args = parse_args(argv)
    _configure_logging(args.verbose)

    # --- Mode 1a: Validate a single source ---
    if args.validate:
        ok = _cli_validate_source(args.validate, args.registry)
        sys.exit(0 if ok else 1)

    # --- Mode 1b: Validate all sources ---
    if args.validate_all:
        registry = load_registry(args.registry)
        sources_section = registry.get("sources", registry)
        source_names = [
            k for k in sources_section if k != "_meta" and isinstance(sources_section[k], dict)
        ]
        if not source_names:
            logger.error("No sources found in registry: %s", args.registry)
            sys.exit(1)

        all_ok = True
        for source_name in sorted(source_names):
            ok = _cli_validate_source(source_name, args.registry)
            if not ok:
                all_ok = False

        print(
            f"\n{'✓' if all_ok else '✗'} Validated {len(source_names)} source(s). "
            f"{'All passed.' if all_ok else 'Some failed — see above.'}"
        )
        sys.exit(0 if all_ok else 1)

    # --- Mode 2: Drift detection ---
    if args.drift_check:
        print(f"Snowflake host : {SNOWFLAKE_HOST}")
        print(f"Warehouse      : {WAREHOUSE}")
        print(f"Snapshot       : {args.snapshot}")
        print()

        report = detect_drift(snapshot_path=args.snapshot)
        print(report.summary())
        sys.exit(1 if report.has_drift else 0)


if __name__ == "__main__":
    main()
