"""
pcp_and_realm_csv_transfer.py

Transfers member data between Planning Center People (PCP) and Realm.
Reads an exported CSV, applies a column mapping spreadsheet, and writes
a reformatted CSV ready for import into the destination system.
"""
# run gitupdater to make sure bekutils and bekgoogle utility libraries are updated
import os
import re
import sys
from datetime import datetime

sys.path.append(os.path.expanduser("~/Dropbox/Postcard Files/"))
if True:
    import gitupdater
from pathlib import Path

import time

import dtale
import pandas as pd

# ── uvbekutils ────────────────────────────────────────────────────────────────
_UTILS_ROOT = Path("/Users/Denise/Library/CloudStorage/Dropbox/PythonPrograms/uvbekutils")
if str(_UTILS_ROOT) not in sys.path:
    sys.path.insert(0, str(_UTILS_ROOT))

from uvbekutils.bek_funcs import exit_yes, exit_yes_no
from uvbekutils.pyautobek import alert_with_file_link, confirm
from uvbekutils.select_file import select_file
from uvbekutils.standardize_columns import ColSpec, standardize_columns

# ── Constants ─────────────────────────────────────────────────────────────────
MAP_START_DIR = str(Path(__file__).parent)

PCP_REQUIRED_COLS = ["First Name", "Last Name", "Home Email", "Work Email"]
REALM_REQUIRED_COLS = ["First Name", "Last Name", "Primary Email", "Alternate Email"]
MAP_REQUIRED_COLS = ["pcp_column_name", "pcp_keep", "realm_column_name", "realm_keep"]

PCP_FILE_PATTERN = "fourth-universalist-society-export*.csv"
REALM_FILE_PATTERN = "*realm*.csv"
MAP_FILE_PATTERN = "*map*.xlsx"


# ── Helpers ───────────────────────────────────────────────────────────────────

def prompt_direction() -> str:
    """Show a popup to choose transfer direction. Returns 'pcp_to_realm' or 'realm_to_pcp'."""
    choice = confirm(
        "Select transfer direction:",
        title="Transfer Direction",
        buttons=["PCP → Realm", "Realm → PCP"],
    )
    return "pcp_to_realm" if choice == "pcp → realm" else "realm_to_pcp"


def strip_screen_name_prefixes(df: pd.DataFrame) -> pd.DataFrame:
    """Strip 'screenname::' style prefixes from column headers (no-op if none present)."""
    df.columns = [re.sub(r"^[^:]+::", "", col) for col in df.columns]
    return df


def validate_columns(df: pd.DataFrame, required: list[str], label: str) -> None:
    """Exit with an error if any required column is missing from df."""
    try:
        standardize_columns(df, [ColSpec(c) for c in required], col_check="subset")
    except ValueError as exc:
        exit_yes(f"{label} is missing required columns:\n{exc}")


def build_renames(
    map_df: pd.DataFrame,
    keep_col: str,
    origin_col: str,
    dest_col: str,
) -> dict[str, str]:
    """Return {origin_col_name: output_col_name} for all kept fields.

    When keep_col is 'x', the output name is taken from dest_col (the
    destination system's column name). An explicit value in keep_col overrides
    dest_col and is used as the output name directly.
    """
    renames: dict[str, str] = {}
    for _, row in map_df.iterrows():
        keep_val = str(row[keep_col]).strip() if pd.notna(row[keep_col]) else ""
        orig_name = str(row[origin_col]).strip()
        if not keep_val:
            continue
        if keep_val.lower() == "x":
            dest_name = str(row[dest_col]).strip() if pd.notna(row[dest_col]) else orig_name
            renames[orig_name] = dest_name if dest_name else orig_name
        else:
            renames[orig_name] = keep_val
    return renames


def show_mapping_popup(renames: dict[str, str]) -> None:
    """Show origin→output field mapping and ask user to continue or exit."""
    col_w = max((len(k) for k in renames), default=20) + 2
    lines = [f"  {'Origin field':<{col_w}}  Output field", "  " + "-" * (col_w + 20)]
    for orig, out in renames.items():
        lines.append(f"  {orig:<{col_w}}  {out}")
    exit_yes_no(f"{len(renames)} fields to be transferred:\n\n" + "\n".join(lines) + "\n\nContinue?")


def build_output_df(
    origin_df: pd.DataFrame,
    renames: dict[str, str],
) -> pd.DataFrame:
    """Apply renames to origin_df; exits if any kept column is missing."""
    missing = [c for c in renames if c not in origin_df.columns]
    if missing:
        exit_yes(
            "The following columns are marked as keep but not found in the input file:\n"
            + "\n".join(missing)
        )
    return origin_df[list(renames)].rename(columns=renames)


def browse(df: pd.DataFrame) -> None:
    """Open dtale in the browser; continues when the dtale instance is shut down."""
    d = dtale.show(df, open_browser=True)
    print("Review the data in the browser. Use dtale's Shutdown button when done.")
    while d.is_up():
        time.sleep(1)


def write_coverage_log(
    log_path: Path,
    origin_df: pd.DataFrame,
    renames: dict[str, str],
) -> None:
    """Write a field-coverage report to log_path.

    Reports two categories:
    - Fields in the origin that are NOT being kept but contain at least one
      non-empty value (data being silently discarded).
    - Fields that ARE being kept but are entirely empty in the origin
      (will produce blank columns in the output).
    """
    kept_cols = set(renames.keys())
    all_cols = set(origin_df.columns)

    def has_data(col: str) -> bool:
        return origin_df[col].replace("", pd.NA).notna().any()

    # Sort by ascending distinct-value count, then descending non-empty row count
    # within ties — low-cardinality/most-populated fields first, high-cardinality last.
    def not_kept_sort_key(c: str) -> tuple:
        s = origin_df[c].replace("", pd.NA).dropna()
        return (s.nunique(), -len(s))

    not_kept_with_data = sorted(
        (c for c in all_cols - kept_cols if has_data(c)),
        key=not_kept_sort_key,
    )
    kept_without_data = sorted(c for c in kept_cols & all_cols if not has_data(c))

    with open(log_path, "w") as f:
        f.write("Transfer coverage log\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        f.write(f"Fields KEPT but entirely empty ({len(kept_without_data)}):\n")
        for c in kept_without_data:
            f.write(f"  {c}\n")
        if not kept_without_data:
            f.write("  (none)\n")

        f.write(f"\n\n\nFields NOT kept but contain data ({len(not_kept_with_data)}):\n")
        if not not_kept_with_data:
            f.write("  (none)\n")
        for col in not_kept_with_data:
            counts = origin_df[col].replace("", pd.NA).dropna().value_counts()
            f.write(f"\n  {col} ({counts.sum()} values, {len(counts)} distinct):\n")
            for val, n in counts.items():
                f.write(f"    {val!r:<40}  {n}\n")

    print(f"Coverage log written to:\n  {log_path}")
    alert_with_file_link("Coverage log written.", log_path, title="Transfer Coverage Log")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    direction = prompt_direction()

    if direction == "pcp_to_realm":
        keep_col = "pcp_keep"
        origin_col = "pcp_column_name"
        dest_col = "realm_column_name"
        required = PCP_REQUIRED_COLS
        file_pattern = PCP_FILE_PATTERN
        dest_label = "realm"
    else:
        keep_col = "realm_keep"
        origin_col = "realm_column_name"
        dest_col = "pcp_column_name"
        required = REALM_REQUIRED_COLS
        file_pattern = REALM_FILE_PATTERN
        dest_label = "pcp"

    # Select and load origin file
    origin_path = select_file(
        title="Select origin data file",
        start_dir=MAP_START_DIR,
        files_like=file_pattern,
    )
    if not origin_path:
        exit_yes("No origin file selected.")

    origin_df = pd.read_csv(origin_path)
    origin_df = strip_screen_name_prefixes(origin_df)
    validate_columns(origin_df, required, "Origin file")

    # Select and load map file
    map_path = select_file(
        title="Select column map file",
        start_dir=str(Path(origin_path).parent),
        files_like=MAP_FILE_PATTERN,
    )
    if not map_path:
        exit_yes("No map file selected.")

    try:
        map_df = pd.read_excel(map_path, sheet_name="columns")
    except ValueError:
        exit_yes("Map file must have a sheet named 'columns'.")

    validate_columns(map_df, MAP_REQUIRED_COLS, "Map file")

    renames = build_renames(map_df, keep_col, origin_col, dest_col)
    show_mapping_popup(renames)

    # Build, browse, review log, confirm, write
    output_df = build_output_df(origin_df, renames)

    browse(output_df)

    datestamp = datetime.now().strftime("%Y%m%d")
    output_path = Path(origin_path).parent / f"xfer_{dest_label}_{datestamp}.csv"
    log_path = output_path.with_suffix(".log")
    write_coverage_log(log_path, origin_df, renames)

    exit_yes_no("Ready to write the output file. Continue?")

    output_df.fillna("").to_csv(output_path, index=False)
    print(f"\nOutput written to:\n  {output_path}")


if __name__ == "__main__":
    main()
