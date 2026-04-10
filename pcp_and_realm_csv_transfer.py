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
REALM_FILE_PATTERN = "*.csv"
MAP_FILE_PATTERN = "*map*.xlsx"


# ── Helpers ───────────────────────────────────────────────────────────────────

def clean_col(s: str) -> str:
    """Normalize a column name: NFC unicode, strip non-printable chars, collapse whitespace."""
    s = unicodedata.normalize("NFC", str(s))
    s = "".join(c for c in s if c.isprintable())
    return " ".join(s.split())


def prompt_direction() -> str:
    """Show a popup to choose transfer direction. Returns 'pcp_to_realm' or 'realm_to_pcp'."""
    choice = confirm(
        "Select transfer direction:",
        title="Transfer Direction",
        buttons=["PCP → Realm", "Realm → PCP"],
    )
    return "pcp_to_realm" if choice == "pcp → realm" else "realm_to_pcp"


def strip_screen_name_prefixes(df: pd.DataFrame) -> pd.DataFrame:
    """Strip 'screenname::' style prefixes from column headers and normalize names."""
    df.columns = [clean_col(re.sub(r"^\S+::", "", col)) for col in df.columns]
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
        orig_name = clean_col(row[origin_col])
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
    map_df: pd.DataFrame,
    origin_col: str,
    skip_tab_col: str,
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
    all_map_cols = set(map_df[origin_col].dropna().map(clean_col))
    skip_tab_fields = set(
        map_df.loc[map_df[skip_tab_col].replace("", pd.NA).notna(), origin_col]
        .dropna().map(clean_col)
    )

    def has_data(col: str) -> bool:
        return origin_df[col].replace("", pd.NA).notna().any()

    # Sort by ascending distinct-value count, then descending non-empty row count
    # within ties — low-cardinality/most-populated fields first, high-cardinality last.
    def tabulation_sort_key(c: str) -> tuple:
        s = origin_df[c].replace("", pd.NA).dropna()
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
            counts = origin_df[col].replace("", pd.NA).dropna().value_counts()
            f.write(f"\n  {col} ({counts.sum()} values, {len(counts)} distinct):\n")
            for val, n in counts.items():
                f.write(f"    {val!r:<40}  {n}\n")

    with open(log_path, "w") as f:
        f.write("Transfer coverage log\n")
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
) -> None:
    """Validate origin data against the live PCP field schema.

    renames maps origin_col_name → pcp_col_name (output of build_renames()).
    Checks every mapped PCP column: unknown fields are flagged, and dropdown/
    checkbox fields have their values checked against valid options.
    Writes a validation log and shows a popup. Exits if user chooses not to continue.
    """
    print("Fetching PCP field schema …")
    try:
        schema = _fetch_pcp_schema()
    except Exception as exc:
        exit_yes(f"Could not fetch PCP field schema:\n{exc}")
        return  # unreachable

    clean_fields: list[tuple[str, str]] = []   # (pcp_col, type)
    invalid_fields: list[dict] = []             # fields with bad option values
    unknown_fields: list[str] = []              # pcp_col not found in schema at all

    for origin_col, pcp_col in renames.items():
        norm = clean_col(pcp_col)
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

        series = origin_df[origin_col].replace("", pd.NA).dropna().astype(str)
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

    with open(log_path, "w") as f:
        f.write("PCP Data Validation Report\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        f.write(f"Fields CLEAN — will import correctly ({len(clean_fields)}):\n")
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

        f.flush()
        os.fsync(f.fileno())

    print(f"PCP validation log written to:\n  {log_path}")
    choice = confirm_with_file_link(
        "PCP validation complete. Review results, then continue or exit.",
        log_path,
        title="PCP Data Validation",
        buttons=["Continue", "Exit"],
    )
    if choice == "exit":
        sys.exit(0)


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

    datestamp = datetime.now().strftime("%Y%m%d")
    output_path = Path(origin_path).parent / f"xfer_{dest_label}_{datestamp}.csv"

    if direction == "realm_to_pcp":
        pcp_val_log = output_path.with_name(output_path.stem + "_pcp_validation.log")
        validate_pcp_data(origin_df, renames, pcp_val_log)

    # Build, review log, browse, confirm, write
    output_df = build_output_df(origin_df, renames)

    log_path = output_path.with_suffix(".log")
    write_coverage_log(log_path, origin_df, renames, map_df, origin_col, skip_tab_col)

    browse(output_df)

    exit_yes_no("Ready to write the output file. Continue?")

    output_df.fillna("").to_csv(output_path, index=False)
    print(f"\nOutput written to:\n  {output_path}")


if __name__ == "__main__":
    main()
