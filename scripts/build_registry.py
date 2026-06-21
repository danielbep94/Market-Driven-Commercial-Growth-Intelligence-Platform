#!/usr/bin/env python3
"""Build the semantic registry from SEMANTIC_LAYOUTS/ source files.

This script is the single source-of-truth generator for
``configs/semantic_registry.yaml``.  It parses the 10 layout ``.txt`` files
(Python-dict syntax), enriches each column with type information from the
optional ``column_types_snapshot.yaml``, resolves canonical names via
``column_name_mapping.yaml``, and attaches glossary references from
``business_glossary_seed.yaml``.

CLI modes
---------
``python scripts/build_registry.py``
    Generate (or regenerate) the registry YAML.

``python scripts/build_registry.py --check``
    Content-hash staleness check.  Exit 0 if fresh, exit 1 if stale
    (prints which input files changed).

``python scripts/build_registry.py --coverage``
    Report glossary coverage percentage.  Exit 0 if 100 %, exit 1 otherwise.

``--output PATH``
    Override the default output path (``configs/semantic_registry.yaml``).

Design notes
------------
* Layout files use **Python** syntax (``True`` / ``False``, triple-quoted
  strings) — parsed via :func:`ast.literal_eval`.
* Some files are wrapped in ``{...}``, others start bare as
  ``"KEY": { ... }``; the parser normalises both forms.
* Trailing markdown fences (`` `` ``) are stripped before parsing.
* SHA-256 hashing is mtime-independent (works after ``git clone``).
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent

LAYOUTS_DIR: Path = PROJECT_ROOT / "SEMANTIC_LAYOUTS"
COLUMN_TYPES_PATH: Path = PROJECT_ROOT / "configs" / "column_types_snapshot.yaml"
COLUMN_MAPPING_PATH: Path = PROJECT_ROOT / "configs" / "column_name_mapping.yaml"
GLOSSARY_PATH: Path = PROJECT_ROOT / "configs" / "business_glossary_seed.yaml"
DEFAULT_OUTPUT: Path = PROJECT_ROOT / "configs" / "semantic_registry.yaml"

# Fields we extract from each layout dict (order preserved).
EXTRACT_KEYS: list[str] = [
    "db",
    "schema",
    "notes",
    "grain_cols",
    "metric_cols",
    "date_col",
    "date_format",
    "date_requires_cast",
    "business_keys",
    "source_tables",
    "source_coverage",
    "category_hint",
    "grain_hint",
]

PENDING_TYPE: str = "PENDING_SNAPSHOT"

# ---------------------------------------------------------------------------
# Derived-column type overrides — for columns created by SQL CTEs/aliases
# that don't exist in INFORMATION_SCHEMA.  Types are traced from the actual
# source columns via configs/derived_column_crosswalk.yaml.
#
# Lineage: configs/derived_column_crosswalk.yaml documents the full
#          source_table → source_column → transformation → output_type chain.
# ---------------------------------------------------------------------------
DERIVED_COLUMN_TYPES: dict[str, str] = {
    # ── Nielsen pivoted metrics ──────────────────────────────────────────────
    # All come from SUM(CASE WHEN ... THEN FACT_VALUE), where FACT_VALUE is
    # NUMBER(38,9) in all *_AGG_DATA_PVT tables.
    "VTAS_KGS": "NUMBER(38,9)", "VTAS_UNDS": "NUMBER(38,9)",
    "VTAS_VALOR": "NUMBER(38,9)", "VTAS_LITROS": "NUMBER(38,9)",
    "VTAS_LITRES": "NUMBER(38,9)",
    "VTAS_UNDS_CUALQUIER_PROMO": "NUMBER(38,9)",
    "VTAS_VALOR_CUALQUIER_PROMO": "NUMBER(38,9)",
    "VTAS_LITRES_CUALQUIER_PROMO": "NUMBER(38,9)",
    "PRECIO_KGS_PROM": "NUMBER(38,9)", "PRECIO_UNDS_PROM": "NUMBER(38,9)",
    "PRECIO_LITROS_PROM": "NUMBER(38,9)",
    "RATIO_VTAS_KGS": "NUMBER(38,9)", "RATIO_VTAS_UNDS": "NUMBER(38,9)",
    "RATIO_VTAS_VALOR": "NUMBER(38,9)",
    "DIST_NUM": "NUMBER(38,9)", "DIST_NUM_TDP": "NUMBER(38,9)",
    "DIST_NUM_CUALQUIER_PROMO": "NUMBER(38,9)",
    "DIST_NUM_TIENDAS_VENDEDORAS": "NUMBER(38,9)",
    "DIST_POND": "NUMBER(38,9)", "DIST_POND_REACH": "NUMBER(38,9)",
    "DIST_POND_SIN_VTAS": "NUMBER(38,9)",
    "DIST_POND_TDP_REACH": "NUMBER(38,9)",
    "DIST_POND_TDP": "NUMBER(38,9)",
    "DIST_POND_CUALQUIER_PROMO": "NUMBER(38,9)",
    # ── Nielsen dimension aliases ────────────────────────────────────────────
    # From PROD_DIM CSTM_*/INP_* columns → all TEXT(250)
    "MKT_SHORT_DSC": "TEXT(250)",   # ← MRKT_DSC_SHRT in MKT_DIM
    "ITM_UNIF_BRAND": "TEXT(250)",  # ← CSTM_310589 in PROD_DIM
    "ITM_UNIF_BRND": "TEXT(250)",   # ← INP_56985 in EDP PROD_DIM
    "ITM_UNIF_MANUF": "TEXT(250)",  # ← CSTM_321331 / INP_56982
    "ITM_UNIF_MANUF_DAN": "TEXT(250)",  # CASE expr over CSTM_321331
    "ITM_UNIF_BRAND_DAN": "TEXT(250)",  # CASE expr over CSTM_310589
    "ITM_UNIF_BRND_DAN": "TEXT(250)",   # CASE expr over CSTM_310589
    "ITM_UNIF_SUBBRND": "TEXT(250)",    # ← CSTM_972397
    "ITM_UNIF_SUBBRND_DAN": "TEXT(250)",  # CASE expr over CSTM_972397
    "ITM_UNIF_SUBSEG_DAN": "TEXT(250)",   # ← INP_57006
    "ITM_UNIF_SUBSEG_DAN_1": "TEXT(250)",  # constant 'TOTAL WATERS'
    "ITM_UNIF_SUBSEG_DAN_2": "TEXT(250)",  # CASE over CSTM_421268
    "ITM_UNIF_SUBSEG_DAN_3": "TEXT(250)",  # CASE over CSTM_421268+940056
    "ITM_UNIF_SUBSEG_DAN_4": "TEXT(250)",  # CASE over CTE columns
    "ITM_UNIF_SUBSEG_DAN_5": "TEXT(250)",  # CASE over CTE columns
    "ITM_SUBSEG_UNIF": "TEXT(250)",   # ← INP_57063 / CSTM_310594
    "ITM_SEGMENT_2": "TEXT(250)",     # ← CSTM_421268
    "ITEM_UNIF_SUBBRND": "TEXT(250)", # ← INP_56991
    "SEGMENTACION_2025": "TEXT(250)",  # CASE expr multi-column
    "SEGMENTO_LOCAL": "TEXT(250)",     # CASE over CSTM_421268
    "SUBSEGMENTO_LOCAL": "TEXT(250)",  # CASE over CTE columns
    "PRESENTACION_LOCAL": "TEXT(250)", # CASE over multiple CTE columns
    "TIPO": "TEXT(250)",              # CASE over CSTM_421268
    "SUBCATEGORIA": "TEXT(250)",      # constant 'SUSTITUTOS DE LECHE'
    "PER_DATE": "DATE",               # TO_DATE(period_ending_datetime)
    # ── SELL_IN derived ──────────────────────────────────────────────────────
    "VOLUMEN": "NUMBER(31,15)",   # SUM(LITER) where LITER is NUMBER(31,15)
    "CEDIS_DSC": "TEXT(250)",     # ← CUS_SAL_PLT_DSC
    "CLIENTE": "TEXT(200)",       # ← CUS_NAM_DSC
    "CLIENTE_ID": "TEXT(30)",     # CONCAT over SHP_CUS_IDT / NEW_CUS_IDT
    "ID_CEDIS": "TEXT(200)",      # ← CUS_SAL_PLT_COD
    # ── SELL_OUT derived ─────────────────────────────────────────────────────
    "FORMATO_CADENA": "TEXT(46)",  # ← FORMAT in VW_D_STORE_RM
    # ── WASTE derived ────────────────────────────────────────────────────────
    "WASTE_AMOUNT": "FLOAT",      # ← "Waste ($)" in VW_WASTE
    # ── Other ────────────────────────────────────────────────────────────────
    "SOURCE": "TEXT(250)",        # constant literal in SQL
}


# ---------------------------------------------------------------------------
# Helpers — file discovery & hashing
# ---------------------------------------------------------------------------


def discover_layout_files() -> list[Path]:
    """Return sorted list of ``.txt`` files under ``SEMANTIC_LAYOUTS/``.

    Returns
    -------
    list[Path]
        Absolute paths sorted lexicographically by their path relative to
        the project root.
    """
    files = sorted(LAYOUTS_DIR.rglob("*.txt"), key=lambda p: p.relative_to(PROJECT_ROOT))
    if not files:
        print(
            f"ERROR: No .txt files found under {LAYOUTS_DIR.relative_to(PROJECT_ROOT)}/",
            file=sys.stderr,
        )
        sys.exit(2)
    return files


def sha256_file(path: Path) -> str:
    """Return ``sha256:<hex>`` digest of *path*'s contents."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            h.update(chunk)
    return f"sha256:{h.hexdigest()}"


def compute_input_hashes(
    layout_files: list[Path],
) -> tuple[dict[str, str], str]:
    """Compute per-file and combined input hashes.

    Parameters
    ----------
    layout_files:
        Sorted list of layout ``.txt`` paths.

    Returns
    -------
    tuple[dict[str, str], str]
        ``(per_file_hashes, combined_hash)`` where keys in *per_file_hashes*
        are project-root-relative POSIX paths and *combined_hash* is the
        SHA-256 of all per-file hash hex values concatenated.
    """
    per_file: dict[str, str] = {}

    # Layout files
    for path in layout_files:
        rel = str(path.relative_to(PROJECT_ROOT))
        per_file[rel] = sha256_file(path)

    # Config files (if they exist)
    for cfg_path in (COLUMN_TYPES_PATH, COLUMN_MAPPING_PATH):
        if cfg_path.is_file():
            rel = str(cfg_path.relative_to(PROJECT_ROOT))
            per_file[rel] = sha256_file(cfg_path)

    # Combined hash = SHA-256 of concatenated hex digests (sorted by key)
    combined = hashlib.sha256()
    for key in sorted(per_file):
        # Strip the "sha256:" prefix to get raw hex
        hex_val = per_file[key].split(":", 1)[1]
        combined.update(hex_val.encode())

    return per_file, f"sha256:{combined.hexdigest()}"


# ---------------------------------------------------------------------------
# Helpers — parsing layout files
# ---------------------------------------------------------------------------

# Pattern to strip trailing markdown fences (e.g. trailing `` `` ``)
_TRAILING_FENCE_RE = re.compile(r"\s*`{1,4}\s*$")


def _clean_layout_text(raw: str) -> str:
    """Normalise raw layout text so :func:`ast.literal_eval` can parse it.

    Handles three quirks found in the project's ``.txt`` files:

    1. **Trailing markdown fences** — some files end with `` `` ``.
    2. **Missing outer braces** — some files start with ``"KEY": {``
       instead of ``{ "KEY": { ... } }``.
    3. **Non-triple-quoted SQL strings** — some files use ``"sql": "...``
       with embedded newlines which ``ast.literal_eval`` cannot parse;
       these are converted to triple-quoted strings.
    """
    # Strip trailing markdown fences (line by line from the end)
    lines = raw.rstrip().split("\n")
    while lines and _TRAILING_FENCE_RE.fullmatch(lines[-1]):
        lines.pop()
    text = "\n".join(lines).strip()

    # Ensure the text is wrapped in outer braces
    if not text.startswith("{"):
        text = "{" + text + "}"

    # Convert non-triple-quoted "sql" values to triple-quoted.
    # Matches: "sql": "  ...multiline...  "  (followed by comma or closing)
    # but NOT: "sql": """  (already triple-quoted)
    text = _fix_sql_quoting(text)

    return text


# Pattern for "sql": "<non-triple-quoted multiline content>"
_SQL_KEY_RE = re.compile(
    r'("sql"\s*:\s*)"'       # capture group 1: the key + opening quote
    r'(?!"")'                # negative lookahead: not already triple-quoted
)


def _fix_sql_quoting(text: str) -> str:
    """Convert ``"sql": "..."`` to ``"sql": \"\"\"...\"\"\"`` for multi-line SQL.

    The SQL body may contain embedded double-quotes (Snowflake identifiers
    like ``L."period_ending_datetime"``), so we cannot simply look for the
    next ``"`` to find the closing boundary.  Instead, we search for the
    *next metadata key* pattern (e.g. ``"grain_hint"``, ``"business_keys"``)
    and work backwards to find the closing ``",`` of the SQL value.
    """
    match = _SQL_KEY_RE.search(text)
    if not match:
        return text

    sql_value_start = match.end()  # position right after the opening "

    # Find the next key that follows the SQL block.
    # Layout files always have keys like "grain_hint", "business_keys",
    # "grain_cols", etc. after "sql".
    next_key_re = re.compile(
        r'\n\s*"(?:grain_hint|business_keys|grain_cols|date_col|'
        r'category_hint|notes|metric_cols|source_tables|'
        r'source_coverage|nielsen_study_type|date_format|'
        r'date_requires_cast|derived_cols)"'
    )
    next_match = next_key_re.search(text, sql_value_start)
    if not next_match:
        return text  # can't find boundary — return unchanged

    # The SQL value ends just before the next key.  The pattern is:
    #   ...SQL CONTENT...",\n    "next_key":
    # We need to find the closing ", which is the last " before the
    # next key's line.
    boundary = next_match.start()
    # Walk backwards from the boundary to find the closing quote + comma
    search_zone = text[sql_value_start:boundary]
    # The closing pattern is: <sql content>",  (with optional whitespace)
    close_idx = search_zone.rfind('",')
    if close_idx == -1:
        close_idx = search_zone.rfind('"')
    if close_idx == -1:
        return text  # can't find closing quote

    sql_body = search_zone[:close_idx]
    if "\n" not in sql_body:
        return text  # single-line SQL, no fix needed

    # Replace the non-triple-quoted SQL with triple-quoted version
    abs_close = sql_value_start + close_idx
    new_text = (
        text[:match.end() - 1]   # everything before the opening "
        + '"""'
        + sql_body
        + '"""'
        + text[abs_close + 1:]   # skip the old closing "
    )
    return new_text


def parse_layout_file(path: Path) -> dict[str, dict[str, Any]]:
    """Parse a single ``.txt`` layout file and return its dict.

    Parameters
    ----------
    path:
        Absolute path to the ``.txt`` file.

    Returns
    -------
    dict[str, dict[str, Any]]
        Top-level dict mapping source key → metadata dict.

    Raises
    ------
    SystemExit
        If the file cannot be parsed.
    """
    raw = path.read_text(encoding="utf-8")
    cleaned = _clean_layout_text(raw)
    try:
        data = ast.literal_eval(cleaned)
    except (SyntaxError, ValueError) as exc:
        rel = path.relative_to(PROJECT_ROOT)
        print(f"ERROR: Failed to parse {rel}: {exc}", file=sys.stderr)
        sys.exit(2)

    if not isinstance(data, dict):
        rel = path.relative_to(PROJECT_ROOT)
        print(
            f"ERROR: Expected dict at top level of {rel}, got {type(data).__name__}",
            file=sys.stderr,
        )
        sys.exit(2)

    return data  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Helpers — loading auxiliary configs
# ---------------------------------------------------------------------------


def load_column_types() -> dict[str, str]:
    """Load column type mapping from the snapshot YAML.

    The snapshot has the structure::

        tables:
          PRD_MDP.MDP_DSP.VW_D_PRODUCT_RM:
            columns:
              CBU_ID:
                data_type: NUMBER
                numeric_precision: 1
                numeric_scale: 0

    We flatten this into ``{COLUMN_NAME: TYPE_STRING}`` for easy lookup.
    For columns with precision/scale, the type string is formatted as
    ``NUMBER(38,6)``; for text with max length, as ``TEXT(16777216)``.

    Returns
    -------
    dict[str, str]
        ``{COLUMN_NAME: TYPE_STRING}`` or empty dict if the file is missing.
    """
    if not COLUMN_TYPES_PATH.is_file():
        return {}

    with open(COLUMN_TYPES_PATH, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    if not data or not isinstance(data, dict):
        return {}

    tables = data.get("tables", {})
    if not tables:
        return {}

    type_map: dict[str, str] = {}
    for _table_fqn, table_info in tables.items():
        if not isinstance(table_info, dict):
            continue
        columns = table_info.get("columns", {})
        if not isinstance(columns, dict):
            continue
        for col_name, col_meta in columns.items():
            if not isinstance(col_meta, dict):
                continue
            raw_type = col_meta.get("data_type", "")
            # Build a precise type string with precision/scale or length
            type_str = _format_type_string(raw_type, col_meta)
            type_map[str(col_name).upper()] = type_str

    return type_map


def _format_type_string(raw_type: str, col_meta: dict) -> str:
    """Format a Snowflake type into a readable string.

    Examples: ``NUMBER(38,6)``, ``TEXT(100)``, ``DATE``, ``BOOLEAN``.
    """
    raw_type = str(raw_type).upper()
    precision = col_meta.get("numeric_precision")
    scale = col_meta.get("numeric_scale")
    max_len = col_meta.get("character_maximum_length")

    if precision is not None and raw_type in ("NUMBER", "DECIMAL", "NUMERIC"):
        if scale is not None and int(scale) > 0:
            return f"{raw_type}({precision},{scale})"
        return f"{raw_type}({precision},0)"
    if max_len is not None and raw_type in ("TEXT", "VARCHAR", "STRING", "CHAR"):
        return f"{raw_type}({max_len})"
    return raw_type


def load_glossary() -> set[str]:
    """Load glossary column names as a lookup set.

    Returns
    -------
    set[str]
        Upper-cased column names present in the business glossary.
    """
    if not GLOSSARY_PATH.is_file():
        return set()

    with open(GLOSSARY_PATH, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    if not data:
        return set()

    columns = data.get("columns", data) if isinstance(data, dict) else data
    glossary: set[str] = set()
    if isinstance(columns, list):
        for entry in columns:
            if isinstance(entry, dict):
                name = entry.get("column_name") or entry.get("name")
                if name:
                    glossary.add(str(name).upper())
    return glossary


def load_canonical_names() -> dict[str, str]:
    """Build a reverse-lookup from alias → canonical name.

    Returns
    -------
    dict[str, str]
        ``{ALIAS_UPPER: CANONICAL_UPPER}`` including the canonical name
        itself as a key mapping to itself.
    """
    if not COLUMN_MAPPING_PATH.is_file():
        return {}

    with open(COLUMN_MAPPING_PATH, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    if not data:
        return {}

    mappings = data.get("column_mappings", data) if isinstance(data, dict) else data
    lookup: dict[str, str] = {}

    if isinstance(mappings, list):
        for entry in mappings:
            if not isinstance(entry, dict):
                continue
            canonical = str(entry.get("canonical", "")).upper()
            if not canonical:
                continue
            lookup[canonical] = canonical
            for alias in entry.get("aliases", []) or []:
                if isinstance(alias, dict):
                    alias_name = str(alias.get("name", "")).upper()
                    # Only treat as alias if is_alias is not explicitly False
                    if alias.get("is_alias") is False:
                        continue
                    if alias_name:
                        lookup[alias_name] = canonical
                elif isinstance(alias, str):
                    lookup[alias.upper()] = canonical

    return lookup


# ---------------------------------------------------------------------------
# Registry builder
# ---------------------------------------------------------------------------


def _build_column_entry(
    col_name: str,
    role: str,
    type_map: dict[str, str],
    glossary: set[str],
) -> dict[str, Any]:
    """Create a single column entry dict for the registry.

    Type resolution order:
    1. ``type_map`` (from Snowflake INFORMATION_SCHEMA snapshot)
    2. ``DERIVED_COLUMN_TYPES`` (SQL-computed columns not in source tables)
    3. ``PENDING_SNAPSHOT`` (fallback — unknown type)
    """
    col_upper = col_name.upper()
    col_type = type_map.get(
        col_upper,
        DERIVED_COLUMN_TYPES.get(col_upper, PENDING_TYPE),
    )
    return {
        "name": col_name,
        "type": col_type,
        "role": role,
        "glossary_ref": col_name if col_upper in glossary else None,
    }


def build_source_entry(
    key: str,
    meta: dict[str, Any],
    source_file: str,
    type_map: dict[str, str],
    glossary: set[str],
) -> dict[str, Any]:
    """Transform a parsed layout entry into a registry source entry.

    Parameters
    ----------
    key:
        Source key (e.g. ``"SELL_IN"``).
    meta:
        Parsed metadata dict from the layout file.
    source_file:
        Project-root-relative path of the originating ``.txt`` file.
    type_map:
        Column name → type string mapping.
    glossary:
        Set of upper-cased column names present in the glossary.

    Returns
    -------
    dict[str, Any]
        Registry-ready source entry.
    """
    date_col = meta.get("date_col", "")
    date_col_upper = date_col.upper() if date_col else ""

    grain_cols = meta.get("grain_cols", []) or []
    metric_cols = meta.get("metric_cols", []) or []

    entry: dict[str, Any] = {
        "key": key,
        "db": meta.get("db", ""),
        "schema": meta.get("schema", ""),
        "category": meta.get("category_hint", ""),
        "notes": meta.get("notes", ""),
        "sql_ref": source_file,
        "date_column": {
            "name": date_col,
            "type": type_map.get(date_col_upper, DERIVED_COLUMN_TYPES.get(date_col_upper, PENDING_TYPE)) if date_col else PENDING_TYPE,
            "format": meta.get("date_format", ""),
            "requires_cast": bool(meta.get("date_requires_cast", False)),
        },
        "grain_columns": [
            _build_column_entry(c, "grain", type_map, glossary) for c in grain_cols
        ],
        "metric_columns": [
            _build_column_entry(c, "metric", type_map, glossary) for c in metric_cols
        ],
        "business_keys": meta.get("business_keys", []) or [],
        "source_tables": meta.get("source_tables", []) or [],
        "source_coverage": meta.get("source_coverage", []) or [],
    }

    # Include optional extra fields if present
    grain_hint = meta.get("grain_hint")
    if grain_hint:
        entry["grain_hint"] = grain_hint

    return entry


def build_registry(
    output_path: Path,
) -> dict[str, Any]:
    """Parse all layout files, merge with configs, and write the registry.

    Parameters
    ----------
    output_path:
        Where to write the generated YAML.

    Returns
    -------
    dict[str, Any]
        The complete registry dict (also written to *output_path*).
    """
    layout_files = discover_layout_files()
    type_map = load_column_types()
    glossary = load_glossary()
    # canonical_names loaded for potential future use / downstream consumers
    _canonical_names = load_canonical_names()  # noqa: F841

    per_file_hashes, combined_hash = compute_input_hashes(layout_files)

    sources: dict[str, Any] = {}
    parse_errors: list[str] = []

    for path in layout_files:
        rel_path = str(path.relative_to(PROJECT_ROOT))
        try:
            data = parse_layout_file(path)
        except SystemExit:
            parse_errors.append(rel_path)
            continue

        for key, meta in data.items():
            if not isinstance(meta, dict):
                print(
                    f"WARNING: Skipping non-dict entry '{key}' in {rel_path}",
                    file=sys.stderr,
                )
                continue

            sources[key] = build_source_entry(
                key=key,
                meta=meta,
                source_file=rel_path,
                type_map=type_map,
                glossary=glossary,
            )

    if parse_errors:
        print(
            f"WARNING: {len(parse_errors)} file(s) had parse errors and were "
            f"skipped: {parse_errors}",
            file=sys.stderr,
        )

    # Assemble the full registry
    registry: dict[str, Any] = {
        "_meta": {
            "generated_by": "scripts/build_registry.py",
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "input_hash": combined_hash,
            "input_file_hashes": per_file_hashes,
            "column_types_snapshot": str(COLUMN_TYPES_PATH.relative_to(PROJECT_ROOT)),
            "canonical_names": str(COLUMN_MAPPING_PATH.relative_to(PROJECT_ROOT)),
        },
        "sources": sources,
    }

    # Write YAML
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write(
            "# ===========================================================================\n"
            "# Semantic Registry — AUTO-GENERATED by scripts/build_registry.py\n"
            "# DO NOT EDIT MANUALLY.  Re-run the build script to regenerate.\n"
            "# ===========================================================================\n\n"
        )
        yaml.safe_dump(
            registry,
            fh,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
            width=120,
        )

    n_sources = len(sources)
    n_cols = sum(
        len(s.get("grain_columns", [])) + len(s.get("metric_columns", []))
        for s in sources.values()
    )
    print(f"✓ Registry written to {output_path.relative_to(PROJECT_ROOT)}")
    print(f"  {n_sources} sources, {n_cols} columns, hash={combined_hash[:24]}…")

    return registry


# ---------------------------------------------------------------------------
# CLI — --check
# ---------------------------------------------------------------------------


def check_staleness(output_path: Path) -> None:
    """Compare committed registry hash against current input hashes.

    Exits with code 0 if the registry is fresh, 1 if stale.
    """
    if not output_path.is_file():
        print(f"STALE: Registry file does not exist: {output_path.relative_to(PROJECT_ROOT)}")
        sys.exit(1)

    with open(output_path, encoding="utf-8") as fh:
        registry = yaml.safe_load(fh)

    if not registry or "_meta" not in registry:
        print("STALE: Registry file exists but has no _meta section.")
        sys.exit(1)

    committed_hash: str = registry["_meta"].get("input_hash", "")
    committed_file_hashes: dict[str, str] = registry["_meta"].get("input_file_hashes", {})

    # Compute current hashes
    layout_files = discover_layout_files()
    current_file_hashes, current_combined = compute_input_hashes(layout_files)

    if current_combined == committed_hash:
        print(f"FRESH: Registry is up-to-date (hash={committed_hash[:24]}…)")
        sys.exit(0)

    # Identify which files changed
    print("STALE: Registry is out-of-date.  Changed inputs:")
    all_keys = sorted(set(list(committed_file_hashes.keys()) + list(current_file_hashes.keys())))
    for key in all_keys:
        old = committed_file_hashes.get(key)
        new = current_file_hashes.get(key)
        if old != new:
            if old is None:
                print(f"  + {key}  (new file)")
            elif new is None:
                print(f"  - {key}  (removed)")
            else:
                print(f"  ~ {key}  (content changed)")

    sys.exit(1)


# ---------------------------------------------------------------------------
# CLI — --coverage
# ---------------------------------------------------------------------------


def report_coverage(output_path: Path) -> None:
    """Report glossary coverage across all registry columns.

    Exits with code 0 if 100 %, 1 otherwise.
    """
    if not output_path.is_file():
        print(
            f"ERROR: Registry file not found: {output_path.relative_to(PROJECT_ROOT)}\n"
            "       Run `python scripts/build_registry.py` first.",
            file=sys.stderr,
        )
        sys.exit(2)

    with open(output_path, encoding="utf-8") as fh:
        registry = yaml.safe_load(fh)

    if not registry or "sources" not in registry:
        print("ERROR: Registry file has no 'sources' section.", file=sys.stderr)
        sys.exit(2)

    total = 0
    covered = 0

    for _source_key, source in registry["sources"].items():
        for col_list_key in ("grain_columns", "metric_columns"):
            for col in source.get(col_list_key, []):
                total += 1
                if col.get("glossary_ref") is not None:
                    covered += 1

    if total == 0:
        print("glossary_coverage: N/A (0 columns found)")
        sys.exit(0)

    pct = (covered / total) * 100
    print(f"glossary_coverage: {pct:.1f}% ({covered}/{total} columns)")

    if covered < total:
        # List uncovered columns
        uncovered: list[str] = []
        for source_key, source in registry["sources"].items():
            for col_list_key in ("grain_columns", "metric_columns"):
                for col in source.get(col_list_key, []):
                    if col.get("glossary_ref") is None:
                        uncovered.append(f"  {source_key}.{col['name']}")

        if uncovered:
            print(f"\nUncovered columns ({len(uncovered)}):")
            for line in sorted(set(uncovered)):
                print(line)

        sys.exit(1)

    sys.exit(0)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Build or verify configs/semantic_registry.yaml",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python scripts/build_registry.py              # generate registry\n"
            "  python scripts/build_registry.py --check      # staleness check\n"
            "  python scripts/build_registry.py --coverage   # glossary coverage\n"
        ),
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Content-hash staleness check (exit 0=fresh, 1=stale).",
    )
    parser.add_argument(
        "--coverage",
        action="store_true",
        help="Report glossary coverage percentage.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output path (default: {DEFAULT_OUTPUT.relative_to(PROJECT_ROOT)}).",
    )
    return parser.parse_args()


def main() -> None:
    """Entry point for the build_registry CLI."""
    args = parse_args()

    # Resolve output relative to project root if not absolute
    output_path: Path = args.output
    if not output_path.is_absolute():
        output_path = PROJECT_ROOT / output_path

    if args.check:
        check_staleness(output_path)
        return  # never reached (check_staleness calls sys.exit)

    if args.coverage:
        report_coverage(output_path)
        return  # never reached (report_coverage calls sys.exit)

    # Default: build the registry
    build_registry(output_path)


if __name__ == "__main__":
    main()
