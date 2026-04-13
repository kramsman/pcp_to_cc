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
import unicodedata
from datetime import datetime

import subprocess

import requests
from dotenv import load_dotenv
from google.cloud import secretmanager

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
from uvbekutils.pyautobek import confirm, confirm_with_file_link
from uvbekutils.select_file import select_file
from uvbekutils.standardize_columns import ColSpec, standardize_columns

# ── Constants ─────────────────────────────────────────────────────────────────
MAP_START_DIR = str(Path(__file__).parent)

PCP_API_BASE = "https://api.planningcenteronline.com/people/v2"

PCP_REQUIRED_COLS = ["First Name", "Last Name", "Home Email", "Work Email"]
REALM_REQUIRED_COLS = ["First Name", "Last Name", "Primary Email", ]
MAP_REQUIRED_COLS = [
    "pcp_column_name", "pcp_keep", "pcp_skip_tab",
    "realm_column_name", "realm_keep", "realm_skip_tab",
]

PCP_FILE_PATTERN = "fourth-universalist-society-export*.csv"
PCP_TO_PCP_FILE_PATTERN = "pcp_*.csv"
REALM_FILE_PATTERN = "*.csv"
MAP_FILE_PATTERN = "*map*.xlsx"


# ── Helpers ───────────────────────────────────────────────────────────────────

def clean_col(s: str) -> str:
    """Normalize a column name: NFC unicode, strip non-printable chars, collapse whitespace."""
    s = unicodedata.normalize("NFC", str(s))
    s = "".join(c for c in s if c.isprintable())
    return " ".join(s.split())


def clean_map_col(s: str) -> str:
    """Strip 'screenname::' prefix then normalize — mirrors how origin CSV headers are cleaned."""
    return clean_col(re.sub(r"^.+::\s*", "", str(s)))


def _excel_col_letter(n: int) -> str:
    """Convert 0-based column index to Excel column letter (A, B, …, Z, AA, AB, …)."""
    result = ""
    n += 1
    while n:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result


def check_duplicate_cols(df: pd.DataFrame) -> str:
    """Return a warning message if any columns share the same name after tab-prefix stripping.

    Groups columns by their stripped name and reports any group with more than one member,
    showing the original column name (with prefix) and the Excel column letter for each.
    Returns an empty string if there are no duplicates.
    """
    from collections import defaultdict
    stripped_to_originals: dict = defaultdict(list)
    for i, col in enumerate(df.columns):
        stripped = clean_map_col(col)
        stripped_to_originals[stripped].append((i, col))

    duplicates = {k: v for k, v in stripped_to_originals.items() if len(v) > 1}
    if not duplicates:
        return ""

    lines = []
    for stripped_name, occurrences in sorted(duplicates.items()):
        lines.append(f'  "{stripped_name}"')
        for col_idx, orig_name in occurrences:
            lines.append(f'    Column {_excel_col_letter(col_idx)}:  "{orig_name}"')
    return "Duplicate column names found after stripping tab prefixes:\n\n" + "\n".join(lines)


def col_as_series(df: pd.DataFrame, col: str) -> pd.Series:
    """Return df[col] as a flat Series, concatenating all instances if col is duplicated."""
    data = df[col]
    if isinstance(data, pd.DataFrame):
        return pd.concat([data.iloc[:, i] for i in range(data.shape[1])], ignore_index=True)
    return data


def prompt_direction() -> str:
    """Show a popup to choose transfer direction. Returns 'pcp_to_realm', 'realm_to_pcp', or 'pcp_to_pcp'."""
    choice = confirm(
        "Select transfer direction:",
        title="Transfer Direction",
        buttons=["PCP → Realm", "Realm → PCP", "PCP → PCP", "Cancel"],
    )
    if choice == "pcp → realm":
        return "pcp_to_realm"
    if choice == "realm → pcp":
        return "realm_to_pcp"
    if choice == "pcp → pcp":
        return "pcp_to_pcp"
    sys.exit(0)


def strip_screen_name_prefixes(df: pd.DataFrame) -> pd.DataFrame:
    """Strip 'screenname::' style prefixes from column headers and normalize names."""
    df.columns = [clean_col(re.sub(r"^.+::\s*", "", col)) for col in df.columns]
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
) -> tuple[dict[str, str], list[str]]:
    """Return ({origin_col_name: output_col_name}, warnings) for all kept fields.

    When keep_col is 'x', the output name is taken from dest_col (the
    destination system's column name). An explicit value in keep_col overrides
    dest_col and is used as the output name directly.
    warnings lists map rows where keep is set but the origin column name is blank.
    Row numbers are Excel row numbers (1-based, counting the header as row 1).
    """
    renames: dict[str, str] = {}
    warnings: list[str] = []
    for idx, row in map_df.iterrows():
        keep_val = str(row[keep_col]).strip() if pd.notna(row[keep_col]) else ""
        if not keep_val:
            continue
        orig_name = clean_map_col(row[origin_col]) if pd.notna(row[origin_col]) else ""
        if not orig_name:
            excel_row = idx + 2  # +1 for 0-index, +1 for header row
            warnings.append(f"  map row {excel_row}: keep='{keep_val}' but {origin_col} is blank")
            continue
        if keep_val.lower() == "x":
            raw_dest = str(row[dest_col]).strip() if pd.notna(row[dest_col]) else ""
            dest_name = clean_map_col(raw_dest) if raw_dest else orig_name
            renames[orig_name] = dest_name if dest_name else orig_name
        else:
            renames[orig_name] = keep_val
    return renames, warnings


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
    try:
        while d.is_up():
            time.sleep(1)
    finally:
        d.kill()


def write_coverage_log(
    log_path: Path,
    origin_df: pd.DataFrame,
    renames: dict[str, str],
    map_df: pd.DataFrame,
    origin_col: str,
    skip_tab_col: str,
    direction: str,
    append: bool = False,
) -> None:
    """Write a field-coverage report to log_path.

    Reports three categories:
    1. Kept but entirely empty — will produce blank columns in the output.
    2. Not kept but has data — data being silently discarded.
    3. Not in map at all but has data — completely unknown fields.
    Fields listed in skip_tab_col are included but tabulation is suppressed.
    """
    all_cols = set(origin_df.columns)

    # Column names in origin_df and renames keys are already clean_col()-normalized.
    # Map values are normalized here so all comparisons are plain equality.
    kept_cols = set(renames.keys())
    all_map_cols = set(map_df[origin_col].dropna().map(clean_map_col))
    skip_tab_fields = set(
        map_df.loc[map_df[skip_tab_col].replace("", pd.NA).notna(), origin_col]
        .dropna().map(clean_map_col)
    )

    def has_data(col: str) -> bool:
        return bool(col_as_series(origin_df, col).replace("", pd.NA).notna().any())

    # Sort by ascending distinct-value count, then descending non-empty row count
    # within ties — low-cardinality/most-populated fields first, high-cardinality last.
    def tabulation_sort_key(c: str) -> tuple:
        s = col_as_series(origin_df, c).replace("", pd.NA).dropna()
        return (c in skip_tab_fields, s.nunique(), -len(s))

    # In the map but not being kept, and has data
    not_kept_with_data = sorted(
        (c for c in all_cols if c in all_map_cols and c not in kept_cols and has_data(c)),
        key=tabulation_sort_key,
    )

    # Not in the map at all, and has data
    not_in_map = sorted(
        (c for c in all_cols if c not in all_map_cols and has_data(c)),
        key=tabulation_sort_key,
    )

    # Kept but entirely empty
    kept_without_data = sorted(
        c for c in kept_cols & all_cols if not has_data(c)
    )

    def write_tabulation(f, col: str) -> None:
        if col in skip_tab_fields:
            f.write(f"\n  {col}:\n    tabulate skipped\n")
        else:
            counts = col_as_series(origin_df, col).replace("", pd.NA).dropna().value_counts()
            f.write(f"\n  {col} ({counts.sum()} values, {len(counts)} distinct):\n")
            for val, n in counts.items():
                f.write(f"    {val!r:<40}  {n}\n")

    direction_labels = {"pcp_to_realm": "PCP → Realm", "realm_to_pcp": "Realm → PCP", "pcp_to_pcp": "PCP → PCP"}
    direction_label = direction_labels.get(direction, direction)

    with open(log_path, "a" if append else "w") as f:
        if append:
            f.write("\n\n" + "=" * 79 + "\n\n")
        f.write("Transfer coverage log\n")
        f.write(f"Direction: {direction_label}\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        f.write(f"Fields KEPT but entirely empty ({len(kept_without_data)}):\n")
        for c in kept_without_data:
            f.write(f"  {c}\n")
        if not kept_without_data:
            f.write("  (none)\n")

        f.write(f"\n\n\n\nFields NOT kept but contain data ({len(not_kept_with_data)}):\n")
        if not not_kept_with_data:
            f.write("  (none)\n")
        for col in not_kept_with_data:
            write_tabulation(f, col)

        f.write(f"\n\n\n\nFields NOT IN MAP but contain data ({len(not_in_map)}):\n")
        if not not_in_map:
            f.write("  (none)\n")
        for col in not_in_map:
            write_tabulation(f, col)

        f.flush()
        os.fsync(f.fileno())

    print(f"Coverage log written to:\n  {log_path}")
    choice = confirm_with_file_link(
        "Coverage log written. Review and continue, or exit.",
        log_path,
        title="Transfer Coverage Log",
        buttons=["Continue", "Exit"],
    )
    if choice == "exit":
        sys.exit(0)


# ── PCP field reformatting ────────────────────────────────────────────────────

def _reformat_checkboxes(val: str) -> str:
    """Convert PCP export comma-separated checkbox values to pipe-separated for reimport."""
    return "|".join(p.strip() for p in val.split(";") if p.strip())


# Map PCP field data_type → reformatter function. Add entries here for future types.
FIELD_TYPE_REFORMATTERS = {
    "checkboxes": _reformat_checkboxes,
}


def reformat_pcp_fields(
    df: pd.DataFrame,
    renames: dict[str, str],
    schema: dict[str, dict],
) -> None:
    """Reformat field values in df in-place for types listed in FIELD_TYPE_REFORMATTERS.

    Only modifies columns that (a) appear in renames, (b) are found in the PCP schema,
    and (c) have a matching entry in FIELD_TYPE_REFORMATTERS.
    """
    for origin_col, pcp_col in renames.items():
        if origin_col not in df.columns:
            continue
        norm = clean_map_col(pcp_col)
        if norm not in schema:
            continue
        dtype = schema[norm]["type"]
        reformatter = FIELD_TYPE_REFORMATTERS.get(dtype)
        if reformatter is None:
            continue
        col_data = df[origin_col]
        if isinstance(col_data, pd.DataFrame):
            print(f"  Skipping [{dtype}]: {origin_col} (duplicate column name in input file)")
            continue
        mask = col_data.replace("", pd.NA).notna()
        df.loc[mask, origin_col] = col_data[mask].astype(str).map(reformatter)
        print(f"  Reformatted [{dtype}]: {origin_col}")


# ── PCP validation ────────────────────────────────────────────────────────────

def _fetch_pcp_schema() -> dict[str, dict]:
    """Return combined PCP field schema: standard built-in fields + live custom field definitions.

    Returns {clean_col(name): {"type": str, "options": list[str] | None}}.
    Standard fields have type="standard". Custom fields have their data_type value
    (text, paragraph, date, boolean, number, dropdown, checkboxes).
    Options are populated only for dropdown and checkboxes fields.
    """
    load_dotenv()
    project_id = os.environ.get("CLOUD_PROJECT_ID", "")
    if not project_id:
        raise RuntimeError("CLOUD_PROJECT_ID not set in .env")

    sm_client = secretmanager.SecretManagerServiceClient()

    def _secret(name: str) -> str:
        path = f"projects/{project_id}/secrets/{name}/versions/latest"
        return sm_client.access_secret_version(request={"name": path}).payload.data.decode("UTF-8")

    auth = (_secret("PCP_APP_ID"), _secret("PCP_SECRET"))
    hdrs = {"User-Agent": "pcp_to_cc (office2@4thu.org)"}

    schema: dict[str, dict] = {}

    # Fetch custom field definitions (paginated)
    custom_fields: list = []
    url: str | None = f"{PCP_API_BASE}/field_definitions"
    while url:
        resp = requests.get(url, auth=auth, headers=hdrs, timeout=10)
        resp.raise_for_status()
        body = resp.json()
        custom_fields.extend(body.get("data", []))
        url = body.get("links", {}).get("next")

    # Fetch options for dropdown/checkbox fields
    for field in custom_fields:
        fid = field["id"]
        attrs = field["attributes"]
        dtype = attrs.get("data_type", "text")
        entry: dict = {"type": dtype, "options": None}

        if dtype in ("select", "checkboxes"):
            options: list[str] = []
            opts_url: str | None = f"{PCP_API_BASE}/field_definitions/{fid}/field_options"
            while opts_url:
                resp = requests.get(opts_url, auth=auth, headers=hdrs, timeout=10)
                resp.raise_for_status()
                odata = resp.json()
                options.extend(opt["attributes"]["value"] for opt in odata.get("data", []))
                opts_url = odata.get("links", {}).get("next")
            entry["options"] = options

        schema[clean_col(attrs["name"])] = entry

    return schema


def validate_pcp_data(
    origin_df: pd.DataFrame,
    renames: dict[str, str],
    log_path: Path,
    direction: str,
    schema: dict[str, dict] | None = None,
    skip_tab_fields: set | None = None,
) -> None:
    """Validate origin data against the live PCP field schema.

    renames maps origin_col_name → pcp_col_name (output of build_renames()).
    Checks every mapped PCP column: unknown fields are flagged, and dropdown/
    checkbox fields have their values checked against valid options.
    Writes a validation log and shows a popup. Exits if user chooses not to continue.

    Pass a pre-fetched schema to avoid a second API call (e.g. when reformat_pcp_fields
    already fetched it). If schema is None, it will be fetched here.
    """
    if schema is None:
        print("Fetching PCP field schema …")
        try:
            schema = _fetch_pcp_schema()
        except Exception as exc:
            if "reauthentication" in str(exc).lower() or "application-default login" in str(exc).lower():
                print("GCP credentials expired — opening browser to reauthenticate …")
                result = subprocess.run(
                    ["gcloud", "auth", "application-default", "login"],
                    check=False,
                )
                if result.returncode != 0:
                    exit_yes("GCP reauthentication failed. Please run:\n  gcloud auth application-default login\nthen try again.")
                    return
                print("Reauthenticated. Retrying PCP schema fetch …")
                try:
                    schema = _fetch_pcp_schema()
                except Exception as exc2:
                    exit_yes(f"Could not fetch PCP field schema after reauthentication:\n{exc2}")
                    return
            else:
                exit_yes(f"Could not fetch PCP field schema:\n{exc}")
                return  # unreachable

    missing = [c for c in renames if c not in origin_df.columns]
    if missing:
        exit_yes(
            "The following columns are marked as keep but not found in the input file:\n"
            + "\n".join(missing)
        )

    clean_fields: list[tuple[str, str]] = []   # (pcp_col, type)
    invalid_fields: list[dict] = []             # fields with bad option values
    unknown_fields: list[str] = []              # pcp_col not found in schema at all

    for origin_col, pcp_col in renames.items():
        norm = clean_map_col(pcp_col)
        if norm not in schema:
            unknown_fields.append(pcp_col)
            continue

        dtype = schema[norm]["type"]

        if dtype not in ("select", "checkboxes"):
            clean_fields.append((pcp_col, dtype))
            continue

        valid_opts = set(schema[norm]["options"] or [])
        invalid_vals: dict[str, int] = {}
        affected_rows = 0

        series = col_as_series(origin_df, origin_col).replace("", pd.NA).dropna().astype(str)
        for val in series:
            if dtype == "checkboxes":
                bad = [p.strip() for p in val.split("|") if p.strip() and p.strip() not in valid_opts]
            else:
                bad = [val] if val not in valid_opts else []
            if bad:
                affected_rows += 1
                for b in bad:
                    invalid_vals[b] = invalid_vals.get(b, 0) + 1

        if invalid_vals:
            invalid_fields.append({
                "pcp_col": pcp_col,
                "type": dtype,
                "valid_options": sorted(valid_opts),
                "invalid_values": invalid_vals,
                "affected_rows": affected_rows,
            })
        else:
            clean_fields.append((pcp_col, dtype))

    direction_labels = {"pcp_to_realm": "PCP → Realm", "realm_to_pcp": "Realm → PCP", "pcp_to_pcp": "PCP → PCP"}
    direction_label = direction_labels.get(direction, direction)

    with open(log_path, "w") as f:
        f.write("PCP Data Validation Report\n")
        f.write(f"Direction: {direction_label}\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Custom field definitions fetched from PCP: {len(schema)}\n\n")

        col_w = max((len(k) for k in renames), default=20) + 2
        f.write(f"Field mapping ({len(renames)} fields):\n")
        f.write(f"  {'Origin field':<{col_w}}  Destination field\n")
        f.write("  " + "-" * (col_w + 20) + "\n")
        for orig, dest in renames.items():
            f.write(f"  {orig:<{col_w}}  {dest}\n")

        f.write(f"\n\nFields CLEAN — will import correctly ({len(clean_fields)}):\n")
        for pcp_col, dtype in clean_fields:
            f.write(f"  {pcp_col}  [{dtype}]\n")
        if not clean_fields:
            f.write("  (none)\n")

        f.write(f"\n\nFields with INVALID VALUES — values will be skipped on import ({len(invalid_fields)}):\n")
        for entry in invalid_fields:
            f.write(f"\n  {entry['pcp_col']}  [{entry['type']}]  ({entry['affected_rows']} row(s) affected)\n")
            f.write(f"    Valid options: {', '.join(entry['valid_options'])}\n")
            f.write(f"    Invalid values seen:\n")
            for val, count in sorted(entry["invalid_values"].items(), key=lambda x: -x[1]):
                f.write(f"      {val!r:<40}  {count}\n")
        if not invalid_fields:
            f.write("  (none)\n")

        f.write(f"\n\nColumns not in PCP custom field definitions — standard PCP fields or unknown; values not validated ({len(unknown_fields)}):\n")
        for col in unknown_fields:
            f.write(f"  {col}\n")
        if not unknown_fields:
            f.write("  (none)\n")

        f.write("\n\n" + "=" * 79 + "\n\n")
        f.write(f"Full PCP field schema ({len(schema)} custom fields):\n")
        for field_name, field_info in sorted(schema.items()):
            dtype = field_info["type"]
            opts = field_info["options"]
            if opts is not None:
                f.write(f"  {field_name}  [{dtype}]\n")
                for opt in opts:
                    f.write(f"    - {opt}\n")
            else:
                f.write(f"  {field_name}  [{dtype}]\n")

        _skip = skip_tab_fields or set()
        f.write(f"\n\n\n\nActual values in input data — all mapped fields ({len(renames)}):\n")
        f.write(f"  (skip_tab_fields: {sorted(_skip)})\n")
        for orig_col, pcp_col in renames.items():
            if orig_col in _skip:
                f.write(f"\n  {pcp_col}  (tabulate skipped)\n")
                continue
            series = col_as_series(origin_df, orig_col).replace("", pd.NA).dropna()
            if series.empty:
                f.write(f"\n  {pcp_col}  (no data)\n")
            else:
                counts = series.value_counts()
                f.write(f"\n  {pcp_col}  ({counts.sum()} values, {len(counts)} distinct):\n")
                for val, n in counts.items():
                    f.write(f"    {val!r:<40}  {n}\n")

        f.flush()
        os.fsync(f.fileno())

    print(f"PCP validation log written to:\n  {log_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    direction = prompt_direction()

    if direction == "pcp_to_realm":
        keep_col = "pcp_keep"
        origin_col = "pcp_column_name"
        dest_col = "realm_column_name"
        skip_tab_col = "pcp_skip_tab"
        required = PCP_REQUIRED_COLS
        file_pattern = PCP_FILE_PATTERN
        dest_label = "realm"
    elif direction == "pcp_to_pcp":
        keep_col = "pcp_keep"
        origin_col = "pcp_column_name"
        dest_col = "pcp_column_name"
        skip_tab_col = "pcp_skip_tab"
        required = PCP_REQUIRED_COLS
        file_pattern = PCP_TO_PCP_FILE_PATTERN
        dest_label = "pcp"
    else:
        keep_col = "realm_keep"
        origin_col = "realm_column_name"
        dest_col = "pcp_column_name"
        skip_tab_col = "realm_skip_tab"
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
    dup_msg = check_duplicate_cols(origin_df)
    if dup_msg:
        exit_yes_no(dup_msg + "\n\nReformatting will be skipped for duplicate fields. Continue?")
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

    renames, map_warnings = build_renames(map_df, keep_col, origin_col, dest_col)
    if map_warnings:
        exit_yes_no(
            "Map rows with keep set but blank origin column name (will be skipped):\n\n"
            + "\n".join(map_warnings)
            + "\n\nContinue?"
        )

    datestamp = datetime.now().strftime("%Y%m%d")
    output_path = Path(origin_path).parent / f"xfer_{dest_label}_{datestamp}.csv"
    log_path = output_path.with_suffix(".log")

    skip_tab_fields = set(
        map_df.loc[map_df[skip_tab_col].replace("", pd.NA).notna(), origin_col]
        .dropna().map(clean_map_col)
    )

    if direction in ("realm_to_pcp", "pcp_to_pcp"):
        if direction == "pcp_to_pcp":
            print("Fetching PCP schema for reformatting …")
            pcp_schema = _fetch_pcp_schema()
            print("Reformatting fields …")
            reformat_pcp_fields(origin_df, renames, pcp_schema)
            validate_pcp_data(origin_df, renames, log_path, direction, schema=pcp_schema,
                              skip_tab_fields=skip_tab_fields)
        else:
            validate_pcp_data(origin_df, renames, log_path, direction,
                              skip_tab_fields=skip_tab_fields)

    # Build, review log, browse, confirm, write
    output_df = build_output_df(origin_df, renames)

    write_coverage_log(log_path, origin_df, renames, map_df, origin_col, skip_tab_col, direction,
                       append=direction in ("realm_to_pcp", "pcp_to_pcp"))

    browse(output_df)

    exit_yes_no("Ready to write the output file. Continue?")

    output_df.fillna("").to_csv(output_path, index=False)
    print(f"\nOutput written to:\n  {output_path}")


if __name__ == "__main__":
    main()
