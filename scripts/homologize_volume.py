#!/usr/bin/env python3
"""
homologize_volume.py — Cross-CBU Volume Homologation Utility
=============================================================

**Purpose**:
    The SELL_IN data source contains a ``VOLUMEN`` column whose *unit of
    measure differs across CBUs* (Commercial Business Units):

    ┌──────────┬────────────────────────────┬────────────┐
    │   CBU    │ Raw Formula                │ Raw Unit   │
    ├──────────┼────────────────────────────┼────────────┤
    │ WATERS   │ SUM(LITER)                 │ liters     │
    │ EDP      │ SUM(BIL_NET_KGR / 1000)   │ metric ton │
    └──────────┴────────────────────────────┴────────────┘

    This means that **raw VOLUMEN values CANNOT be summed across CBUs**
    without first converting them to a common unit.

**Design Rules**:
    1. The original ``VOLUMEN`` column is **NEVER** modified or dropped.
    2. A new ``VOLUMEN_HOMOLOGATED_KG`` column is added alongside it.
    3. Conversion factors are explicit, auditable, and logged.

**When to use which column**:
    - **Single-CBU analysis** → use raw ``VOLUMEN`` (it is already
      internally consistent within each CBU).
    - **Cross-CBU summation / comparison** → use ``VOLUMEN_HOMOLOGATED_KG``
      (all values expressed in **kilograms**).

**Supported DataFrame types**: PySpark ``DataFrame`` and Pandas ``DataFrame``.

Usage (library)::

    from scripts.homologize_volume import homologize_volume
    df_out = homologize_volume(df, cbu_column="CBU", volume_column="VOLUMEN")

Usage (CLI — parquet round-trip for testing)::

    python -m scripts.homologize_volume \\
        --input  data/sell_in.parquet \\
        --output data/sell_in_homologated.parquet \\
        --cbu-column CBU \\
        --volume-column VOLUMEN
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Union

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level conversion factors (explicit & auditable)
# ---------------------------------------------------------------------------

CONVERSION_FACTORS: Dict[str, Dict[str, Any]] = {
    "WATERS": {
        "source_unit": "liters",
        "target_unit": "kg",
        "factor": 1.0,
        "rationale": (
            "Water density ≈ 1.0 kg/L at standard conditions. "
            "Adjust this factor if the product mix includes flavoured "
            "beverages with materially different densities."
        ),
    },
    "EDP": {
        "source_unit": "metric_tons",
        "target_unit": "kg",
        "factor": 1000.0,
        "rationale": (
            "Raw VOLUMEN for EDP = SUM(BIL_NET_KGR / 1000), i.e. metric "
            "tons.  Multiply by 1 000 to recover kilograms."
        ),
    },
}
"""
Registry of conversion factors per CBU.

Each entry documents:
    - ``source_unit``: the unit of the raw ``VOLUMEN`` column for this CBU.
    - ``target_unit``: always ``"kg"`` (the homologated target).
    - ``factor``: the multiplicative factor applied to raw ``VOLUMEN``
      to obtain kilograms.
    - ``rationale``: human-readable explanation for audit purposes.

**Extending**: To on-board a new CBU, add an entry here and the
``homologize_volume`` function will pick it up automatically.  Unknown
CBUs will produce a ``VOLUMEN_HOMOLOGATED_KG`` of ``NULL`` and emit a
warning.
"""


# ---------------------------------------------------------------------------
# Core homologation — PySpark path
# ---------------------------------------------------------------------------

def _homologize_pyspark(
    df: Any,
    cbu_column: str,
    volume_column: str,
) -> Any:
    """Apply volume homologation on a PySpark DataFrame.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input DataFrame containing *at least* ``cbu_column`` and
        ``volume_column``.
    cbu_column : str
        Name of the column holding CBU identifiers (e.g. ``"WATERS"``,
        ``"EDP"``).
    volume_column : str
        Name of the raw volume column to convert.

    Returns
    -------
    pyspark.sql.DataFrame
        The original DataFrame with an additional
        ``VOLUMEN_HOMOLOGATED_KG`` column.
    """
    from pyspark.sql import functions as F  # type: ignore[import-untyped]

    # Build a CASE WHEN expression from the conversion factors dict.
    # Unknown CBUs → NULL (explicit; forces investigation rather than
    # silent mis-aggregation).
    expr = F.lit(None).cast("double")  # default branch

    for cbu, meta in CONVERSION_FACTORS.items():
        factor = float(meta["factor"])
        logger.info(
            "PySpark homologation — CBU=%s: %s * %.6f → %s",
            cbu,
            meta["source_unit"],
            factor,
            meta["target_unit"],
        )
        expr = (
            F.when(F.upper(F.col(cbu_column)) == cbu, F.col(volume_column) * factor)
            .otherwise(expr)
        )

    return df.withColumn("VOLUMEN_HOMOLOGATED_KG", expr)


# ---------------------------------------------------------------------------
# Core homologation — Pandas path
# ---------------------------------------------------------------------------

def _homologize_pandas(
    df: Any,
    cbu_column: str,
    volume_column: str,
) -> Any:
    """Apply volume homologation on a Pandas DataFrame.

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame containing *at least* ``cbu_column`` and
        ``volume_column``.
    cbu_column : str
        Name of the column holding CBU identifiers.
    volume_column : str
        Name of the raw volume column to convert.

    Returns
    -------
    pandas.DataFrame
        A **copy** of the input with an additional
        ``VOLUMEN_HOMOLOGATED_KG`` column.  The original DataFrame is
        not mutated.
    """
    import numpy as np  # type: ignore[import-untyped]

    df = df.copy()
    # Initialise to NaN (unknown CBU → NaN, same semantics as PySpark NULL)
    df["VOLUMEN_HOMOLOGATED_KG"] = np.nan

    cbu_upper = df[cbu_column].astype(str).str.upper()

    for cbu, meta in CONVERSION_FACTORS.items():
        factor = float(meta["factor"])
        mask = cbu_upper == cbu
        matched = int(mask.sum())
        logger.info(
            "Pandas homologation — CBU=%s: %d rows × %.6f (%s → %s)",
            cbu,
            matched,
            factor,
            meta["source_unit"],
            meta["target_unit"],
        )
        df.loc[mask, "VOLUMEN_HOMOLOGATED_KG"] = (
            df.loc[mask, volume_column] * factor
        )

    # Warn about rows with unrecognised CBUs (these stay as NaN).
    known_cbus = set(CONVERSION_FACTORS.keys())
    unknown_mask = ~cbu_upper.isin(known_cbus)
    n_unknown = int(unknown_mask.sum())
    if n_unknown > 0:
        unknown_values = df.loc[unknown_mask, cbu_column].unique().tolist()
        logger.warning(
            "Pandas homologation — %d rows have unknown CBU values and "
            "will have NULL VOLUMEN_HOMOLOGATED_KG: %s",
            n_unknown,
            unknown_values,
        )

    return df


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def homologize_volume(
    df: Any,
    cbu_column: str = "CBU",
    volume_column: str = "VOLUMEN",
) -> Any:
    """Add a ``VOLUMEN_HOMOLOGATED_KG`` column to *df*.

    The function auto-detects whether *df* is a PySpark or Pandas
    DataFrame and dispatches to the appropriate implementation.

    .. important::
        The original ``VOLUMEN`` column is **never** modified or dropped.
        Use ``VOLUMEN_HOMOLOGATED_KG`` for cross-CBU aggregations and
        raw ``VOLUMEN`` for single-CBU analytics.

    Parameters
    ----------
    df : pyspark.sql.DataFrame | pandas.DataFrame
        Input DataFrame.
    cbu_column : str, default ``"CBU"``
        Column containing the CBU identifier string.
    volume_column : str, default ``"VOLUMEN"``
        Column containing the raw (non-homologated) volume value.

    Returns
    -------
    pyspark.sql.DataFrame | pandas.DataFrame
        DataFrame with the added ``VOLUMEN_HOMOLOGATED_KG`` column.

    Raises
    ------
    ValueError
        If *cbu_column* or *volume_column* is missing from *df*.
    TypeError
        If *df* is neither a PySpark nor a Pandas DataFrame.

    Examples
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame({
    ...     "CBU": ["WATERS", "EDP", "WATERS"],
    ...     "VOLUMEN": [500.0, 2.5, 1000.0],
    ... })
    >>> result = homologize_volume(df)
    >>> result["VOLUMEN_HOMOLOGATED_KG"].tolist()
    [500.0, 2500.0, 1000.0]
    """
    # --- Column presence checks -------------------------------------------
    _validate_columns_exist(df, cbu_column, volume_column)

    # --- Dispatch ---------------------------------------------------------
    class_name = type(df).__module__ + "." + type(df).__qualname__

    if _is_pyspark_dataframe(df):
        logger.info("Detected PySpark DataFrame (%s)", class_name)
        return _homologize_pyspark(df, cbu_column, volume_column)

    if _is_pandas_dataframe(df):
        logger.info("Detected Pandas DataFrame (%s)", class_name)
        return _homologize_pandas(df, cbu_column, volume_column)

    raise TypeError(
        f"Unsupported DataFrame type: {class_name}. "
        "Expected pyspark.sql.DataFrame or pandas.DataFrame."
    )


# ---------------------------------------------------------------------------
# Validation helper
# ---------------------------------------------------------------------------

def validate_homologation(
    df: Any,
    volume_column: str = "VOLUMEN",
    homologated_column: str = "VOLUMEN_HOMOLOGATED_KG",
) -> bool:
    """Validate that homologation was applied correctly.

    Checks
    ------
    1. ``homologated_column`` exists in *df*.
    2. Every row where ``volume_column`` is non-null also has a non-null
       ``homologated_column`` value.  (A null in ``homologated_column``
       when ``volume_column`` is non-null means the CBU was not
       recognised — this is flagged as a validation failure.)

    Parameters
    ----------
    df : pyspark.sql.DataFrame | pandas.DataFrame
        The DataFrame to validate (should already have been passed
        through :func:`homologize_volume`).
    volume_column : str, default ``"VOLUMEN"``
        Name of the raw volume column.
    homologated_column : str, default ``"VOLUMEN_HOMOLOGATED_KG"``
        Name of the homologated column that should exist after
        calling :func:`homologize_volume`.

    Returns
    -------
    bool
        ``True`` if all checks pass, ``False`` otherwise.

    Notes
    -----
    This function logs ``WARNING`` messages for each failed check so
    the caller can inspect details without catching exceptions.
    """
    passed = True

    # --- Check 1: column existence ----------------------------------------
    columns = _get_column_names(df)
    if homologated_column not in columns:
        logger.warning(
            "Validation FAILED — column '%s' not found in DataFrame. "
            "Available columns: %s",
            homologated_column,
            columns,
        )
        return False

    # --- Check 2: no unexpected nulls -------------------------------------
    if _is_pyspark_dataframe(df):
        from pyspark.sql import functions as F  # type: ignore[import-untyped]

        bad_count = (
            df.filter(
                F.col(volume_column).isNotNull()
                & F.col(homologated_column).isNull()
            )
            .count()
        )
    elif _is_pandas_dataframe(df):
        non_null_vol = df[volume_column].notna()
        null_homol = df[homologated_column].isna()
        bad_count = int((non_null_vol & null_homol).sum())
    else:
        logger.warning(
            "Validation SKIPPED — unsupported DataFrame type: %s",
            type(df).__qualname__,
        )
        return False

    if bad_count > 0:
        logger.warning(
            "Validation FAILED — %d row(s) have non-null %s but null %s. "
            "This typically means an unknown CBU value was encountered. "
            "Check CONVERSION_FACTORS and the data.",
            bad_count,
            volume_column,
            homologated_column,
        )
        passed = False
    else:
        logger.info(
            "Validation PASSED — all non-null %s rows have a "
            "corresponding %s value.",
            volume_column,
            homologated_column,
        )

    return passed


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _is_pyspark_dataframe(df: Any) -> bool:
    """Return ``True`` if *df* is a PySpark DataFrame."""
    try:
        from pyspark.sql import DataFrame as SparkDF  # type: ignore[import-untyped]
        return isinstance(df, SparkDF)
    except ImportError:
        return False


def _is_pandas_dataframe(df: Any) -> bool:
    """Return ``True`` if *df* is a Pandas DataFrame."""
    try:
        import pandas as pd
        return isinstance(df, pd.DataFrame)
    except ImportError:
        return False


def _get_column_names(df: Any) -> list[str]:
    """Return a list of column names from a PySpark or Pandas DataFrame."""
    if _is_pyspark_dataframe(df):
        return df.columns  # type: ignore[return-value]
    if _is_pandas_dataframe(df):
        return list(df.columns)
    return []


def _validate_columns_exist(
    df: Any,
    cbu_column: str,
    volume_column: str,
) -> None:
    """Raise ``ValueError`` if required columns are missing."""
    columns = _get_column_names(df)
    missing = [c for c in (cbu_column, volume_column) if c not in columns]
    if missing:
        raise ValueError(
            f"Missing required column(s) {missing} in DataFrame. "
            f"Available columns: {columns}"
        )


# ---------------------------------------------------------------------------
# CLI entry-point (parquet round-trip for testing)
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Volume Homologation CLI — reads a Parquet file, applies "
            "cross-CBU volume homologation, writes the result back to "
            "Parquet.  Intended for local testing and validation."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Example:\n"
            "  python -m scripts.homologize_volume \\\n"
            "      --input  data/sell_in.parquet \\\n"
            "      --output data/sell_in_homologated.parquet\n"
            "\n"
            "The output Parquet will contain all original columns PLUS\n"
            "VOLUMEN_HOMOLOGATED_KG.  The raw VOLUMEN column is never\n"
            "modified or dropped.\n"
            "\n"
            "IMPORTANT:\n"
            "  • For cross-CBU summation → use VOLUMEN_HOMOLOGATED_KG\n"
            "  • For single-CBU analysis → use raw VOLUMEN\n"
        ),
    )
    parser.add_argument(
        "--input", "-i",
        type=Path,
        required=True,
        help="Path to the input Parquet file.",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        required=True,
        help="Path for the output (homologated) Parquet file.",
    )
    parser.add_argument(
        "--cbu-column",
        type=str,
        default="CBU",
        help="Name of the CBU column (default: CBU).",
    )
    parser.add_argument(
        "--volume-column",
        type=str,
        default="VOLUMEN",
        help="Name of the raw volume column (default: VOLUMEN).",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity (default: INFO).",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    """CLI entry-point for parquet-based volume homologation.

    Parameters
    ----------
    argv : list[str] | None
        Command-line arguments.  Defaults to ``sys.argv[1:]``.
    """
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    input_path: Path = args.input.resolve()
    output_path: Path = args.output.resolve()

    if not input_path.exists():
        logger.error("Input file not found: %s", input_path)
        sys.exit(1)

    # --- Read ---------------------------------------------------------------
    try:
        import pandas as pd

        logger.info("Reading Parquet: %s", input_path)
        df = pd.read_parquet(input_path)
        logger.info(
            "Loaded %d rows × %d columns", len(df), len(df.columns)
        )
    except ImportError:
        logger.error(
            "pandas is required for CLI mode.  "
            "Install it with: pip install pandas pyarrow"
        )
        sys.exit(1)
    except Exception as exc:
        logger.error("Failed to read Parquet file: %s", exc)
        sys.exit(1)

    # --- Homologize ---------------------------------------------------------
    df_out = homologize_volume(
        df,
        cbu_column=args.cbu_column,
        volume_column=args.volume_column,
    )

    # --- Validate -----------------------------------------------------------
    is_valid = validate_homologation(
        df_out,
        volume_column=args.volume_column,
    )
    if not is_valid:
        logger.warning(
            "Homologation validation reported issues — review warnings above."
        )

    # --- Write --------------------------------------------------------------
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_parquet(output_path, index=False)
    logger.info("Wrote homologated Parquet: %s", output_path)

    # --- Summary ------------------------------------------------------------
    logger.info(
        "Summary — columns in output: %s", list(df_out.columns)
    )
    logger.info(
        "Reminder: use VOLUMEN_HOMOLOGATED_KG for cross-CBU aggregations, "
        "raw %s for single-CBU analysis.",
        args.volume_column,
    )


if __name__ == "__main__":
    main()
