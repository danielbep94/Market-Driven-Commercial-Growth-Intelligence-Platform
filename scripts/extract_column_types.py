#!/usr/bin/env python3
"""Extract column-type metadata from Snowflake INFORMATION_SCHEMA.COLUMNS.

This script connects to Snowflake and queries INFORMATION_SCHEMA.COLUMNS for
every source table consumed by the Market-Growth-Intelligence semantic layer.
The result is a deterministic YAML snapshot that downstream tools (e.g.
homologation validators, semantic-layout generators, CI drift-detection)
can reference to understand the current schema of each source view/table.

Supported connection modes
--------------------------
1. **Environment variables** (default):
   SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ROLE (optional),
   SNOWFLAKE_ACCOUNT (defaults to danonenam.east-us-2.azure).
2. **Azure Key Vault** (future-ready):
   Set AZURE_KEY_VAULT_URL and the script will attempt to fetch credentials
   from Key Vault using ``azure-identity`` DefaultAzureCredential.

Usage
-----
    # Dry-run — prints the SQL statements without connecting:
    python -m scripts.extract_column_types --dry-run

    # Full extraction with custom output path:
    python -m scripts.extract_column_types --output configs/column_types_snapshot.yaml

    # Use a specific Snowflake role:
    SNOWFLAKE_ROLE=PRD_MDP_ANL_RL python -m scripts.extract_column_types

Output
------
A YAML file with the following top-level keys:

* ``_meta`` – snapshot timestamp, content SHA-256 hash, Snowflake host &
  warehouse used for extraction.
* ``tables`` – mapping of ``DATABASE.SCHEMA.TABLE_NAME`` → column definitions,
  each containing ``data_type``, ``is_nullable``, ``character_maximum_length``,
  ``numeric_precision``, and ``numeric_scale`` (only present when non-null).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:
    sys.exit(
        "ERROR: PyYAML is required.  Install it with:  pip install pyyaml"
    )

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SNOWFLAKE_HOST = "danonenam.east-us-2.azure.snowflakecomputing.com"
SNOWFLAKE_ACCOUNT = "danonenam.east-us-2.azure"
WAREHOUSE = "PRD_MDP_ANL_WH"

# Source tables grouped by DATABASE.SCHEMA
SOURCE_TABLES: dict[str, list[str]] = {
    "PRD_MEX.MEX_DSP_DPH_MKT": [
        "VW_IR_YOG_GEL_MT_NLSN_AGG_DATA_PVT",
        "VW_IR_YOG_GEL_MT_NLSN_MKT_DIM",
        "VW_IR_YOG_GEL_MT_NLSN_PER_DIM",
        "VW_IR_YOG_GEL_MT_NLSN_FACT_REF",
        "VW_IR_YOG_GEL_MT_NLSN_PROD_DIM",
        "VW_IND_AGUA_BNF_RT_NLSN_AGG_DATA_PVT",
        "VW_IND_AGUA_BNF_RT_NLSN_PROD_DIM",
        "VW_IND_AGUA_BNF_RT_NLSN_MKT_DIM",
        "VW_IND_AGUA_BNF_RT_NLSN_PER_DIM",
        "VW_IND_AGUA_BNF_RT_NLSN_FACT_REF",
        "VW_IND_AGUA_BNF_ST_NLSN_AGG_DATA_PVT",
        "VW_IND_AGUA_BNF_ST_NLSN_MKT_DIM",
        "VW_IND_AGUA_BNF_ST_NLSN_PER_DIM",
        "VW_IND_AGUA_BNF_ST_NLSN_FACT_REF",
        "VW_IND_AGUA_BNF_ST_NLSN_PROD_DIM",
        "VW_SUST_LECHE_ST_NLSN_AGG_DATA_PVT",
        "VW_SUST_LECHE_ST_NLSN_PROD_DIM",
        "VW_SUST_LECHE_ST_NLSN_MKT_DIM",
        "VW_SUST_LECHE_ST_NLSN_PER_DIM",
        "VW_SUST_LECHE_ST_NLSN_FACT_REF",
    ],
    "PRD_MEX.MEX_DSP_OTC": [
        "VW_FACT_RNV",
        "V_D_CLIENT",
        "V_D_PERIOD",
        "V_D_ITEM",
        "VW_D_CUSTOMER_DICTONARY",
    ],
    "PRD_MDP.MDP_DSP": [
        "VW_FACT_DANONE_IBP",
        "VW_MKT_ECOMM",
        "VW_FACT_SELL_OUT",
        "V_D_PERIOD",
        "VW_D_STORE_RM",
        "VW_D_PRODUCT_RM",
    ],
    "PRD_MDP.MDP_STG": [
        "FACT_MEDIA_OFF",
        "VW_WASTE",
    ],
}

# Columns we pull from INFORMATION_SCHEMA.COLUMNS, in order.
INFO_SCHEMA_FIELDS = [
    "TABLE_CATALOG",
    "TABLE_SCHEMA",
    "TABLE_NAME",
    "COLUMN_NAME",
    "ORDINAL_POSITION",
    "DATA_TYPE",
    "IS_NULLABLE",
    "CHARACTER_MAXIMUM_LENGTH",
    "NUMERIC_PRECISION",
    "NUMERIC_SCALE",
]


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

def _build_query(database: str, schema: str, table_names: list[str]) -> str:
    """Build a single SQL query against INFORMATION_SCHEMA.COLUMNS.

    Parameters
    ----------
    database:
        Snowflake database name (e.g. ``PRD_MEX``).
    schema:
        Snowflake schema name (e.g. ``MEX_DSP_OTC``).
    table_names:
        List of table/view names to query metadata for.

    Returns
    -------
    str
        A ready-to-execute SQL statement.
    """
    fields = ", ".join(INFO_SCHEMA_FIELDS)
    in_list = ", ".join(f"'{t}'" for t in sorted(table_names))
    return (
        f"SELECT {fields}\n"
        f"  FROM {database}.INFORMATION_SCHEMA.COLUMNS\n"
        f" WHERE TABLE_SCHEMA = '{schema}'\n"
        f"   AND TABLE_NAME IN ({in_list})\n"
        f" ORDER BY TABLE_NAME, ORDINAL_POSITION;"
    )


def build_all_queries() -> list[tuple[str, str, str, list[str], str]]:
    """Build SQL queries for every source-table group.

    Returns
    -------
    list[tuple]
        Each element is ``(database, schema, db_schema_key, table_names, sql)``.
    """
    queries: list[tuple[str, str, str, list[str], str]] = []
    for db_schema, tables in SOURCE_TABLES.items():
        database, schema = db_schema.split(".")
        sql = _build_query(database, schema, tables)
        queries.append((database, schema, db_schema, tables, sql))
    return queries


# ---------------------------------------------------------------------------
# Snowflake connection
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
            from azure.identity import DefaultAzureCredential
            from azure.keyvault.secrets import SecretClient

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
        print(f"✓ Connected to Snowflake ({SNOWFLAKE_HOST})")
        return conn
    except Exception as exc:
        sys.exit(f"ERROR: Snowflake connection failed: {exc}")


# ---------------------------------------------------------------------------
# Extraction logic
# ---------------------------------------------------------------------------

def _row_to_column_dict(row: tuple) -> dict[str, Any]:
    """Convert a single INFORMATION_SCHEMA row to a column-metadata dict.

    Only includes keys whose values are not None.
    """
    col_meta: dict[str, Any] = {
        "data_type": row[5],
        "is_nullable": row[6],
    }
    if row[7] is not None:
        col_meta["character_maximum_length"] = row[7]
    if row[8] is not None:
        col_meta["numeric_precision"] = row[8]
    if row[9] is not None:
        col_meta["numeric_scale"] = row[9]
    return col_meta


def extract_column_types(dry_run: bool = False) -> dict[str, Any]:
    """Run all queries and assemble the tables dict.

    Parameters
    ----------
    dry_run:
        If ``True``, print queries to stdout and return an empty dict.

    Returns
    -------
    dict
        Mapping of ``DATABASE.SCHEMA.TABLE`` → column metadata.
    """
    queries = build_all_queries()

    if dry_run:
        print("=" * 72)
        print("DRY-RUN MODE – queries that would be executed:")
        print("=" * 72)
        for database, schema, db_schema, table_names, sql in queries:
            print(f"\n-- {db_schema} ({len(table_names)} tables)")
            print(sql)
        print("\n" + "=" * 72)
        return {}

    conn = _connect_snowflake()
    tables: dict[str, dict[str, Any]] = {}
    total_columns = 0

    try:
        cur = conn.cursor()
        for database, schema, db_schema, table_names, sql in queries:
            print(f"  Querying {db_schema} ({len(table_names)} tables)…")
            cur.execute(sql)
            rows = cur.fetchall()

            for row in rows:
                fq_table = f"{row[0]}.{row[1]}.{row[2]}"  # CATALOG.SCHEMA.TABLE
                col_name = row[3]

                if fq_table not in tables:
                    tables[fq_table] = {"columns": {}}
                tables[fq_table]["columns"][col_name] = _row_to_column_dict(row)
                total_columns += 1

            # Warn about tables that returned no rows
            found_tables = {
                f"{row[0]}.{row[1]}.{row[2]}" for row in rows
            }
            for t in table_names:
                expected_fq = f"{database}.{schema}.{t}"
                if expected_fq not in found_tables:
                    print(f"  ⚠ WARNING: No columns found for {expected_fq}")
    finally:
        conn.close()
        print(f"✓ Connection closed. Extracted {total_columns} column(s) "
              f"across {len(tables)} table(s).")

    return dict(sorted(tables.items()))


# ---------------------------------------------------------------------------
# Hashing & YAML output
# ---------------------------------------------------------------------------

def _compute_content_hash(tables: dict[str, Any]) -> str:
    """Compute a deterministic SHA-256 hash of the tables payload.

    The hash is computed over a JSON serialisation of the sorted tables
    dict so that the result is stable across Python runs.

    Parameters
    ----------
    tables:
        The ``tables`` section of the output YAML.

    Returns
    -------
    str
        ``sha256:<hex digest>``
    """
    canonical = json.dumps(tables, sort_keys=True, default=str)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def build_snapshot(tables: dict[str, Any]) -> dict[str, Any]:
    """Build the full YAML document dict including ``_meta``.

    Parameters
    ----------
    tables:
        Column-type mapping produced by :func:`extract_column_types`.

    Returns
    -------
    dict
        Ready to serialise with ``yaml.safe_dump``.
    """
    return {
        "_meta": {
            "snapshot_timestamp": datetime.now(timezone.utc)
                                        .strftime("%Y-%m-%dT%H:%M:%SZ"),
            "content_hash": _compute_content_hash(tables),
            "snowflake_host": SNOWFLAKE_HOST,
            "warehouse": WAREHOUSE,
        },
        "tables": tables,
    }


def write_yaml(data: dict[str, Any], output_path: Path) -> None:
    """Write the snapshot dict to a YAML file.

    Parameters
    ----------
    data:
        Full document dict (``_meta`` + ``tables``).
    output_path:
        Destination file path; parent directories are created automatically.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(
            data,
            fh,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
            width=120,
        )
    print(f"✓ Snapshot written to {output_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

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
        prog="extract_column_types",
        description=(
            "Extract column-type metadata from Snowflake "
            "INFORMATION_SCHEMA.COLUMNS for all semantic-layer source tables."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Print the SQL queries without executing them.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("configs/column_types_snapshot.yaml"),
        help=(
            "Path for the output YAML file "
            "(default: configs/column_types_snapshot.yaml)."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Entry point for the column-type extraction script.

    Parameters
    ----------
    argv:
        Optional argument list for testing; defaults to ``sys.argv[1:]``.
    """
    args = parse_args(argv)

    print(f"Snowflake host : {SNOWFLAKE_HOST}")
    print(f"Warehouse      : {WAREHOUSE}")
    print(f"Output file    : {args.output}")
    print(f"Dry-run        : {args.dry_run}")
    print()

    tables = extract_column_types(dry_run=args.dry_run)

    if args.dry_run:
        print("\nDry-run complete — no output file written.")
        return

    snapshot = build_snapshot(tables)
    write_yaml(snapshot, args.output)

    # Summary
    total_cols = sum(
        len(tbl["columns"]) for tbl in tables.values()
    )
    print(
        f"\nSummary: {len(tables)} table(s), {total_cols} column(s), "
        f"hash={snapshot['_meta']['content_hash']}"
    )


if __name__ == "__main__":
    main()
