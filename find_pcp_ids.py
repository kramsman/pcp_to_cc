"""
Helper script: list every ID used in rules.json, from both Planning Center People
and Constant Contact, in one report.

Output order: Workflows → Forms → Field Tabs → Custom Fields
              → Constant Contact Lists
Custom fields carry their tab as a column, and dropdown (select) fields are
marked, since only those can be required as an anytime item.

Results are printed to the terminal and saved to find_pcp_ids.html, which has a
filter box for narrowing every table at once.

Prerequisites:
    1. .env has CLOUD_PROJECT_ID set.
    2. PCP_APP_ID and PCP_SECRET are stored in GCP Secret Manager.
    3. gcloud auth application-default login has been run.

Usage:
    python find_pcp_ids.py
"""

import html
import os
import sys

os.environ.setdefault("GRPC_VERBOSITY", "ERROR")  # suppress gRPC noise before grpc loads

from datetime import datetime  # noqa: E402

import requests  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from google.api_core import retry as api_retry  # noqa: E402
from google.cloud import secretmanager  # noqa: E402
from bekgoogle import ensure_adc_auth  # noqa: E402

load_dotenv()

PCP_API_BASE = "https://api.planningcenteronline.com/people/v2"
_project_id = os.environ.get("CLOUD_PROJECT_ID", "")
_client = None
_cache: dict[str, str] = {}


def _get_secret(secret_id: str) -> str:
    global _client
    if secret_id not in _cache:
        if _client is None:
            _client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{_project_id}/secrets/{secret_id}/versions/latest"
        try:
            resp = _client.access_secret_version(
                request={"name": name},
                retry=api_retry.Retry(deadline=5.0),
            )
        except Exception as e:
            if "Reauthentication is needed" in str(e):
                ensure_adc_auth()
                _client = secretmanager.SecretManagerServiceClient()
                resp = _client.access_secret_version(request={"name": name})
            else:
                raise
        _cache[secret_id] = resp.payload.data.decode("UTF-8")
    return _cache[secret_id]


def _emit(lines: list[str], text: str) -> None:
    lines.append(text)
    print(text)


def _fetch_all(endpoint: str, auth: tuple, lines: list[str]) -> list[dict]:
    """Fetch all pages from a PCP API endpoint, returning the combined data list."""
    items = []
    next_url = f"{PCP_API_BASE}/{endpoint}"
    while next_url:
        try:
            resp = requests.get(next_url, auth=auth, timeout=10,
                                headers={"User-Agent": "pco_webhook (office2@4thu.org)"})
            resp.raise_for_status()
        except requests.RequestException as e:
            _emit(lines, f"ERROR fetching {endpoint}: {e}")
            break
        data = resp.json()
        items.extend(data.get("data", []))
        next_url = data.get("links", {}).get("next")
    return items


def fetch_pcp_ids() -> dict:
    """Return {pcp_workflow, pcp_form, pcp_field} as lists of {id, name} dicts."""
    print("Fetching PCP credentials...")
    app_id = _get_secret("PCP_APP_ID")
    secret = _get_secret("PCP_SECRET")
    auth = (app_id, secret)
    dummy: list[str] = []
    print("Fetching PCP workflows...")
    workflows = _fetch_all("workflows", auth, dummy)
    print(f"  {len(workflows)} workflows")
    print("Fetching PCP forms...")
    forms = _fetch_all("forms", auth, dummy)
    print(f"  {len(forms)} forms")
    print("Fetching PCP field definitions...")
    fields = _fetch_all("field_definitions", auth, dummy)
    print(f"  {len(fields)} field definitions")
    print("Fetching PCP field tabs...")
    tabs = _fetch_all("tabs", auth, dummy)
    print(f"  {len(tabs)} tabs")
    return {
        "pcp_workflow": [{"id": w["id"], "name": w["attributes"].get("name", "")} for w in workflows],
        "pcp_form":     [{"id": f["id"], "name": f["attributes"].get("name", "")} for f in forms],
        # data_type and tab_id let the editor offer only fields that can legally
        # fill a given slot — e.g. a durable prerequisite must be a dropdown, and
        # must not live on the workflow's own items tab.
        "pcp_field":    [{"id": f["id"], "name": f["attributes"].get("name", ""),
                          "data_type": f["attributes"].get("data_type", ""),
                          "tab_id": str(f["attributes"].get("tab_id", "") or "")}
                         for f in fields if not f["attributes"].get("deleted_at")],
        "pcp_tab":      [{"id": t["id"], "name": t["attributes"].get("name", "")} for t in tabs],
    }


def collect_sections(auth: tuple, lines: list[str]) -> list[dict]:
    """Gather every ID list as {title, columns, rows, note}.

    One structure feeds both the terminal output and the HTML page, so the two
    can never drift apart.
    """
    sections = []

    workflows = _fetch_all("workflows", auth, lines)
    sections.append({
        "title": "Workflows", "columns": ["ID", "Name", "Campus"],
        "rows": [[w["id"], w["attributes"].get("name", ""),
                  w["attributes"].get("campus_name", "") or ""] for w in workflows],
    })

    forms = _fetch_all("forms", auth, lines)
    sections.append({
        "title": "Forms", "columns": ["ID", "Name", "Active"],
        "rows": [[f["id"], f["attributes"].get("name", ""),
                  "yes" if f["attributes"].get("active") else "no"] for f in forms],
    })

    # Tabs group custom fields on a person's profile, and are how anytime-item
    # workflows find their items: the config names a tab, and the dropdown fields
    # on it are the items. Nothing in PCP links a tab to a workflow — that link
    # lives only in rules.json.
    tabs = _fetch_all("tabs", auth, lines)
    tab_names = {t["id"]: t["attributes"].get("name", "") for t in tabs}
    sections.append({
        "title": "Field Tabs", "columns": ["ID", "Name", "Order"],
        "rows": [[t["id"], t["attributes"].get("name", ""),
                  str(t["attributes"].get("sequence", ""))]
                 for t in sorted(tabs, key=lambda x: x["attributes"].get("sequence", 0))],
        "note": "A tab is named in a rule to say which fields are that workflow's "
                "anytime items.",
    })

    # Carries its tab as a column rather than splitting into a table per tab, so
    # filtering on a tab name gathers its fields in one go.
    fields = [f for f in _fetch_all("field_definitions", auth, lines)
              if not f["attributes"].get("deleted_at")]
    field_rows = []
    for f in sorted(fields, key=lambda x: (tab_names.get(str(x["attributes"].get("tab_id", "") or ""), "zz"),
                                           x["attributes"].get("name", ""))):
        attrs = f["attributes"]
        tab_id = str(attrs.get("tab_id", "") or "")
        field_rows.append([
            f["id"], attrs.get("name", ""), attrs.get("data_type", ""),
            tab_names.get(tab_id, "(no tab)"),
            "yes" if attrs.get("data_type") == "select" else "",
        ])
    sections.append({
        "title": "Custom Fields", "columns": ["ID", "Name", "Type", "Tab", "Can be required"],
        "rows": field_rows,
        "note": "Only dropdown (select) fields can be required as an anytime item.",
    })

    # Folded in here so one report answers "what is the ID of X", whichever
    # system X lives in. A CC auth failure must not cost you the PCP report,
    # so it degrades to a note rather than an exit.
    try:
        from find_cc_ids import fetch_cc_lists_full
        cc = fetch_cc_lists_full()
        sections.append({
            "title": "Constant Contact Lists", "columns": ["UUID", "Name", "Members"],
            "rows": [[l.get("list_id", ""), l.get("name", ""),
                      str(l.get("membership_count", ""))] for l in cc],
        })
    except BaseException as e:
        sections.append({
            "title": "Constant Contact Lists", "columns": ["UUID", "Name", "Members"],
            "rows": [],
            "note": f"Could not fetch: {e}. Planning Center results above are "
                    f"unaffected — check CC_ACCESS_TOKEN in Secret Manager.",
        })
    return sections


def print_sections(sections: list[dict], lines: list[str]) -> None:
    """Echo the same data to the terminal as aligned columns."""
    for s in sections:
        _emit(lines, f"\n\n=== {s['title']} ===\n")
        if s.get("note"):
            _emit(lines, f"{s['note']}\n")
        if not s["rows"]:
            _emit(lines, "None found.")
            continue
        widths = [max(len(str(r[i])) for r in s["rows"] + [s["columns"]])
                  for i in range(len(s["columns"]))]
        _emit(lines, "  ".join(c.ljust(w) for c, w in zip(s["columns"], widths)))
        _emit(lines, "-" * (sum(widths) + 2 * len(widths)))
        for r in s["rows"]:
            _emit(lines, "  ".join(str(v).ljust(w) for v, w in zip(r, widths)))
        _emit(lines, f"\nTotal: {len(s['rows'])}")


def render_html(sections: list[dict]) -> str:
    """One page, one filter box across every table."""
    esc = html.escape
    blocks = []
    for n, s in enumerate(sections):
        head = "".join(f"<th>{esc(c)}</th>" for c in s["columns"])
        rows = "".join(
            "<tr>" + "".join(
                f'<td class="{"id" if i == 0 else "yes" if v == "yes" and c == "Can be required" else ""}">'
                f"{esc(str(v))}</td>"
                for i, (v, c) in enumerate(zip(r, s["columns"]))
            ) + "</tr>"
            for r in s["rows"]
        )
        note = f'<p class="note">{esc(s["note"])}</p>' if s.get("note") else ""
        empty = "" if s["rows"] else '<p class="note">None found.</p>'
        blocks.append(
            f'<section data-sec="{n}"><h2>{esc(s["title"])} '
            f'<span class="count" data-total="{len(s["rows"])}">{len(s["rows"])}</span></h2>'
            f'{note}{empty}'
            f'<div class="wrap"><table><thead><tr>{head}</tr></thead>'
            f"<tbody>{rows}</tbody></table></div></section>"
        )

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Planning Center and Constant Contact IDs</title>
<style>
 :root {{ color-scheme: light dark; }}
 body {{ font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; margin:0; padding:1.5rem; }}
 h1 {{ font-size:1.2rem; margin:0 0 .25rem; }}
 h2 {{ font-size:1rem; margin:1.75rem 0 .4rem; }}
 .sub {{ opacity:.7; font-size:.85rem; margin-bottom:1rem; }}
 .note {{ opacity:.7; font-size:.85rem; margin:.2rem 0 .5rem; }}
 .count {{ font-weight:400; opacity:.6; font-size:.85rem; }}
 #q {{ width:100%; max-width:32rem; padding:.5rem .7rem; font-size:1rem;
       border:1px solid rgba(128,128,128,.5); border-radius:.4rem;
       background:transparent; color:inherit; position:sticky; top:0; }}
 .wrap {{ overflow-x:auto; }}
 table {{ border-collapse:collapse; min-width:100%; }}
 th,td {{ padding:.35rem .6rem; text-align:left; white-space:nowrap;
          border-bottom:1px solid rgba(128,128,128,.25); }}
 th {{ font-size:.75rem; text-transform:uppercase; letter-spacing:.04em; opacity:.7; }}
 td.id {{ font-family:ui-monospace,SFMono-Regular,Menlo,monospace; }}
 td.yes {{ color:#1b6b3a; font-weight:600; }}
 section.hide, tr.hide {{ display:none; }}
 @media (prefers-color-scheme: dark) {{ td.yes {{ color:#7ddc9a; }} }}
</style></head><body>
<h1>Planning Center and Constant Contact IDs</h1>
<div class="sub">Run {datetime.now().strftime('%d %b %Y, %-I:%M %p')} ·
 type below to narrow every table at once</div>
<input id="q" type="search" placeholder="Filter by name, ID, type or tab…" autofocus>
{"".join(blocks)}
<script>
const q = document.getElementById('q');
q.addEventListener('input', () => {{
  const t = q.value.trim().toLowerCase();
  document.querySelectorAll('section').forEach(sec => {{
    let shown = 0;
    sec.querySelectorAll('tbody tr').forEach(tr => {{
      const hit = !t || tr.textContent.toLowerCase().includes(t);
      tr.classList.toggle('hide', !hit);
      if (hit) shown++;
    }});
    const c = sec.querySelector('.count');
    c.textContent = t ? shown + ' of ' + c.dataset.total : c.dataset.total;
    sec.classList.toggle('hide', t && shown === 0);
  }});
}});
</script>
</body></html>"""


def main():
    if not _project_id:
        print("ERROR: CLOUD_PROJECT_ID not set in .env")
        sys.exit(1)

    auth = (_get_secret("PCP_APP_ID"), _get_secret("PCP_SECRET"))
    lines: list[str] = []

    sections = collect_sections(auth, lines)
    print_sections(sections, lines)

    out_path = os.path.splitext(os.path.abspath(__file__))[0] + ".html"
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(render_html(sections))
    print(f"\nSaved to: {out_path}")

    # Imported here, not at module level: this pulls in PySide6, and other
    # scripts import this module purely for its API helpers.
    from uvbekutils.pyautobek import confirm_with_file_link
    confirm_with_file_link(
        "Workflows, forms, tabs, field definitions, and CC lists written.\n"
        "The page has a filter box — type to narrow every table at once.",
        out_path,
        title="Rpt 'PCO and CC Field Ids'",
        buttons=["OK"],
    )


if __name__ == "__main__":
    main()
