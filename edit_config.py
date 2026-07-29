"""GUI editor for PCP → CC automation rules. Reads/writes rules.json."""

# todo: add cc lists to find id report as are shown in edit_config cc dropdown
# todo: prefix custom variable names with tab (screen name) to pinpoint and differentiate same names

import html
import json
import sys
import webbrowser
from datetime import date
from pathlib import Path

_UTILS_ROOT = Path("/Users/Denise/Library/CloudStorage/Dropbox/PythonPrograms/uvbekutils")
if str(_UTILS_ROOT) not in sys.path:
    sys.path.insert(0, str(_UTILS_ROOT))

from PySide6.QtCore import Qt, QProcess
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QTabWidget,
    QTableWidget, QTableWidgetItem, QPushButton, QDialog, QFormLayout,
    QLineEdit, QComboBox, QMessageBox, QDialogButtonBox, QLabel,
    QListWidget, QListWidgetItem,
)

RULES_FILE = Path(__file__).parent / "rules.json"
ANYTIME_KEY = "anytime_item_workflows"
REPORT_FILE = Path(__file__).parent / "rules_report.html"
SET_ENV_VARS_SCRIPT = Path(__file__).parent / "set-env-vars.sh"

DROPDOWN_FIELDS = {
    "workflow_id":           "pcp_workflow",
    "add_to_workflow_id":    "pcp_workflow",
    "complete_workflow_id":  "pcp_workflow",
    "remove_workflow_id":    "pcp_workflow",
    "trigger_workflow_id":   "pcp_workflow",
    "displaces_workflow_id": "pcp_workflow",
    "field_id":              "pcp_field",
    "pcp_field_id":          "pcp_field",
    "form_id":               "pcp_form",
    "cc_list_id":            "cc_list",
    "field_tab_id":          "pcp_tab",
    "notes_field_id":        "pcp_field",
}

TABS = [
    {
        "title":         "Assign WF by Form Submission",
        "description":   "Adds profile to a workflow and/or completes a workflow when they submit a particular "
                         "form.  Example: when someone fills out the 'Tell Us About Yourself' form, complete the "
                         "'Visitor' workflow and add to 'Explorer'. 'Add to WF' can be used instead of a PCO "
                         "automation so all form-triggered workflow functions can be done here and not split with PCO.",
        "key":           "form_completion_rules",
        "cols":          ["description", "form_id", "add_to_workflow_id", "complete_workflow_id"],
        "labels":        {
            "description":          "Description",
            "form_id":              "Trigger Form ID",
            "add_to_workflow_id":   "Add to WF ID",
            "complete_workflow_id": "Complete WF ID",
        },
        "widths":        [350, 150, 175, 175],
        "trigger_field": None,
        "optional_cols": ["complete_workflow_id", "add_to_workflow_id"],
    },
    {
        "title":         "WF From Field",
        "description":   "Assigns a person to a workflow when one of their profile fields contains a particular string.  "
                         "Example: when the 'Relationship to "
                         "4th U' field contains 'membership' (which shows an interest) assign them to the "
                         "'Explorer' workflow. "
                         "The check runs whenever the field's value is set or changed. "
                         "'Trigger when entering WF ID' alters what starts the "
                         "check: instead of checking with every profile edit, it only runs when they enter "
                         "the workflow (it's an AND condition). Use this when the field "
                         "gets set automatically on workflow entry, since PCO's change notification can "
                         "arrive before the value is saved. "
                         "'Match' decides how 'Field Value' is compared: 'contains' finds it anywhere, so "
                         "'membership' matches a long free-text answer; 'whole word' only matches whole "
                         "words, so 'No' will not match 'Not attending'; 'exact' means the value and "
                         "nothing else. "
                         "Optionally, 'Remove from' pulls them from an earlier workflow — and 'Assign to' "
                         "may be left as (none) to only remove, e.g. an RSVP of 'No'.",
        "key":           "pcp_workflow_rules",
        "cols":          ["description", "trigger_workflow_id", "pcp_field_id", "pcp_value",
                          "match", "workflow_id", "displaces_workflow_id"],
        "labels":        {
            "description":           "Description",
            "pcp_field_id":          "Field ID",
            "pcp_value":             "Field Value",
            "match":                 "Match",
            "workflow_id":           "Assign to WF ID (optional)",
            "trigger_workflow_id":   "Trigger when entering WF ID (optional)",
            "displaces_workflow_id": "Remove from WF ID (optional)",
        },
        "widths":        [280, 110, 110, 140, 110, 165, 150],
        "trigger_field": None,
        "choice_cols":   {"match": ["contains", "whole word", "exact"]},
        # workflow_id is optional so a rule can only REMOVE — e.g. an RSVP of
        # "No" takes someone off a workflow without pretending they completed it.
        "optional_cols": ["trigger_workflow_id", "workflow_id", "displaces_workflow_id"],
    },
    {
        "title":         "Chain WFs",
        "description":   "Automatically adds a profile to a second workflow or removes them from a workflow when they "
                         "enter or complete a "
                         "first one, linking workflows into a sequence. Example: when someone completes "
                         "the 'Explorer' workflow, add them to the 'Member in Process workflow.'",
        "key":           "workflow_chain_rules",
        "cols":          ["description", "trigger", "workflow_id",
                          "add_to_workflow_id", "remove_workflow_id"],
        "labels":        {
            "description":        "Description",
            "workflow_id":        "Triggered WF ID",
            "trigger":            "Trigger",
            "add_to_workflow_id": "Add to WF ID",
            "remove_workflow_id": "Remove from WF ID",
        },
        "widths":        [375, 150, 110, 175, 175],
        "trigger_field": "trigger",
        "optional_cols": ["add_to_workflow_id", "remove_workflow_id"],
    },
    {
        "title":         "Set Fields by WF",
        "description":   "Sets a profile field to a particular value when a person enters or completes a "
                         "workflow. Example: when someone enters the 'Visitor' workflow, set their "
                         "Member Stage field to 'Visitor'.",
        "key":           "workflow_field_rules",
        "cols":          ["description", "trigger", "workflow_id", "field_id", "value"],
        "labels":        {
            "description": "Description",
            "workflow_id": "Triggered WF ID",
            "field_id":    "Field ID",
            "trigger":     "Trigger",
            "value":       "Value to set",
        },
        "widths":        [375, 110, 110, 110, 160],
        "trigger_field": "trigger",
    },
    {
        "title":         "Anytime WF Items",
        "description":   "Lets a workflow require items that can be done at ANY time, which PCO cannot do on its "
                         "own because its steps are strictly sequential. Example: a retreat runs forms → payment → "
                         "arrange a ride, but 'buy a shirt' can happen whenever. Add a 'Outstanding items' step near "
                         "the end of the workflow and name it below as the Holding Step; the card is held there "
                         "until every item is done, then released automatically. "
                         "The items are the DROPDOWN fields on that tab whose name starts with the prefix below, "
                         "e.g. '>>_RSVP'. Add such a dropdown in PCO and it becomes a required item with no change "
                         "here and no redeploy — and the prefix shows staff, on a person's profile, which fields are "
                         "required. Leave the prefix blank to treat EVERY dropdown on the tab as required. "
                         "Text and paragraph fields are always ignored, so notes and bios can live there "
                         "safely. 'Satisfying values' are the dropdown choices that count as finished (default "
                         "Done, Not needed, Waived) — anything else, such as 'Promised', leaves the item outstanding. "
                         "Fields on the tab are cleared when someone re-enters the workflow so next year starts "
                         "clean; put anything that should carry over year to year on a DIFFERENT tab and list its "
                         "IDs under 'Durable field IDs'. "
                         "See who still owes what at /readiness/<workflow id>.",
        "key":           "anytime_item_workflows",
        "cols":          ["description", "workflow_id", "field_tab_id", "gate_step_id",
                          "requires_person_fields", "notes_field_id"],
        "labels":        {
            "description":            "Description",
            "workflow_id":            "Workflow ID",
            "field_tab_id":           "Items Tab (dropdowns on it are the items)",
            "gate_step_id":           "Holding Step ID",
            "requires_person_fields": "Durable field IDs, never cleared (comma separated, optional)",
            "notes_field_id":         "Notes field for the report (optional)",
        },
        "widths":        [260, 150, 200, 200, 200, 200, 160],
        "trigger_field": None,
        "optional_cols": ["requires_person_fields", "notes_field_id"],
        # item_prefix is free text, not an ID lookup — no dropdown for it.
        # Choices that only exist once another field is chosen: a workflow's
        # steps, and the dropdown values used by the items on a tab.
        "dependent_cols": {
            "gate_step_id":      {"source": "workflow_id",  "fetch": "workflow_steps"},
            "satisfying_values": {"source": "field_tab_id", "fetch": "tab_option_values",
                                  "multi": True},
            # Offers only dropdowns that live off the items tab, so a text field
            # or an items-tab field can no longer be chosen here — both would
            # park every card forever.
            "requires_person_fields": {"source": "field_tab_id", "fetch": "durable_fields",
                                       "multi": True},
        },
    },
    {
        "title":         "CC Lists from Fields",
        "description":   "Adds a person to a Constant Contact email list based on the value of one of their "
                         "profile fields. Example: when a person's 'Member Stage' field contains "
                         "'Member', add them to the 'Members' email list in Constant Contact.",
        "key":           "cc_list_rules",
        "cols":          ["description", "pcp_field_id", "pcp_value", "match", "cc_list_id"],
        "labels":        {
            "description":  "Description",
            "pcp_field_id": "PCP Field ID",
            "pcp_value":    "Field Value",
            "match":        "Match",
            "cc_list_id":   "CC List UUID",
        },
        "widths":        [330, 120, 120, 110, 330],
        "trigger_field": None,
        "choice_cols":   {"match": ["contains", "whole word", "exact"]},
    },
]

# Fetched once on first dialog open; shared across all TabWidget instances.
_api_cache: dict = {}
_api_fetched: bool = False


def _ensure_api_data() -> None:
    global _api_cache, _api_fetched
    if _api_fetched:
        return
    _api_fetched = True  # mark before fetch so a failure doesn't retry every click
    print("Fetching API data for dropdowns...")

    # Load .env using absolute path so it works regardless of cwd (e.g. when
    # launched as a detached subprocess from the launcher).
    _here = Path(__file__).parent
    from dotenv import load_dotenv
    load_dotenv(_here / ".env")

    sys.path.insert(0, str(_here))
    try:
        from find_pcp_ids import fetch_pcp_ids
        _api_cache.update(fetch_pcp_ids())
    except BaseException as e:
        print(f"Warning: PCP fetch failed ({e})")
    try:
        from find_cc_ids import fetch_cc_lists
        _api_cache["cc_list"] = fetch_cc_lists()
    except BaseException as e:
        print(f"Warning: CC fetch failed ({e})")
    print(f"API data: { {k: len(v) for k, v in _api_cache.items()} }")


# ── Dependent dropdown data ───────────────────────────────────────────────────
# Some choices only make sense once another has been made: a workflow's steps
# depend on the workflow, and the values that count as "done" depend on which
# tab holds the items. These are fetched on demand and cached per parent id.

_dependent_cache: dict[tuple, list] = {}


def _pcp_auth() -> tuple:
    from find_pcp_ids import _get_secret
    return (_get_secret("PCP_APP_ID"), _get_secret("PCP_SECRET"))


def fetch_workflow_steps(workflow_id: str) -> list[dict]:
    """Return [{id, name}] for a workflow's steps, ordered as they run."""
    if not workflow_id:
        return []
    key = ("steps", workflow_id)
    if key in _dependent_cache:
        return _dependent_cache[key]
    try:
        import requests
        from find_pcp_ids import PCP_API_BASE
        r = requests.get(f"{PCP_API_BASE}/workflows/{workflow_id}/steps",
                         params={"per_page": 100}, auth=_pcp_auth(), timeout=10,
                         headers={"User-Agent": "pco_webhook (office2@4thu.org)"})
        r.raise_for_status()
        steps = sorted(r.json().get("data", []),
                       key=lambda s: s.get("attributes", {}).get("sequence", 0))
        result = [{"id": s["id"],
                   "name": f"{s['attributes'].get('sequence', '?')}. {s['attributes'].get('name', '')}"}
                  for s in steps]
    except BaseException as e:
        print(f"Warning: could not fetch steps for workflow {workflow_id} ({e})")
        result = []
    _dependent_cache[key] = result
    return result


def fetch_tab_option_values(tab_id: str) -> list[dict]:
    """Return the distinct dropdown option values across a tab's `select` fields.

    Only `select` fields can gate a workflow, so only their options can sensibly
    be marked as satisfying. The three defaults are always offered so a tab whose
    dropdowns are not built yet still produces a usable rule.
    """
    defaults = ["Done", "Not needed", "Waived"]
    if not tab_id:
        return [{"id": v, "name": v} for v in defaults]
    key = ("options", tab_id)
    if key in _dependent_cache:
        return _dependent_cache[key]
    values: list[str] = []
    try:
        import requests
        from find_pcp_ids import PCP_API_BASE
        r = requests.get(f"{PCP_API_BASE}/tabs/{tab_id}/field_definitions",
                         params={"include": "field_options", "per_page": 100},
                         auth=_pcp_auth(), timeout=10,
                         headers={"User-Agent": "pco_webhook (office2@4thu.org)"})
        r.raise_for_status()
        body = r.json()
        by_id = {i["id"]: i["attributes"]["value"]
                 for i in body.get("included", []) if i.get("type") == "FieldOption"}
        for f in body.get("data", []):
            if f["attributes"].get("data_type") != "select":
                continue
            for rel in (f.get("relationships", {}).get("field_options", {}).get("data") or []):
                v = by_id.get(rel["id"])
                if v and v not in values:
                    values.append(v)
    except BaseException as e:
        print(f"Warning: could not fetch options for tab {tab_id} ({e})")
    for d in defaults:
        if d not in values:
            values.append(d)
    result = [{"id": v, "name": v} for v in values]
    _dependent_cache[key] = result
    return result


def fetch_durable_field_choices(tab_id: str) -> list[dict]:
    """Fields eligible as durable prerequisites for a workflow.

    A durable prerequisite is evaluated exactly like an item, so it must be a
    dropdown — a text field's prose can never equal "Done" and would park every
    card forever. It must also live off the items tab, since fields on that tab
    are cleared on re-enrolment, which is the opposite of durable.
    """
    return [
        f for f in _api_cache.get("pcp_field", [])
        if f.get("data_type") == "select" and str(f.get("tab_id", "")) != str(tab_id)
    ]


DEPENDENT_FETCHERS = {
    "workflow_steps":    fetch_workflow_steps,
    "tab_option_values": fetch_tab_option_values,
    "durable_fields":    fetch_durable_field_choices,
}


def _display(item: dict) -> str:
    """Label for a picker or table cell: "Name · Tab (id)".

    Field names are not unique across tabs — this org already has two 'Edited Bio'
    and two 'Invited' — so the tab is what tells them apart.
    """
    label = item.get("name", "")
    tab_id = str(item.get("tab_id", "") or "")
    if tab_id:
        tab = next((t["name"] for t in _api_cache.get("pcp_tab", [])
                    if str(t["id"]) == tab_id), "")
        if tab:
            label = f"{label} · {tab}"
    return f"{label} ({item['id']})"


def cell_text(col: str, value: str) -> str:
    """Render a stored value for display: an ID becomes "Name · Tab (id)"."""
    api_key = DROPDOWN_FIELDS.get(col)
    if not api_key or not value:
        return value
    item = next((i for i in _api_cache.get(api_key, []) if i["id"] == value), None)
    return _display(item) if item else value


def resolve_name(value: str) -> str:
    """Render an id as "1089420: 'Edited Bio'" when the name is known.

    A bare id in a warning tells nobody which field is at fault, and these
    warnings exist precisely to be acted on.
    """
    for items in _api_cache.values():
        for item in items:
            if str(item.get("id")) == str(value):
                return _display(item)
    return str(value)


class MultiSelectList(QListWidget):
    """Checkbox list whose value is a comma-separated string.

    Used where a rule takes several ids or values at once. Anything already in
    the rule stays checked even if it is no longer offered by the API, so opening
    and saving a rule can never silently drop a value.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMaximumHeight(160)
        self.setAlternatingRowColors(True)
        # An item view with input methods enabled swallows keystrokes meant for
        # other widgets on macOS, which leaves the text fields in this dialog
        # looking editable but ignoring the keyboard. Same workaround the rules
        # table already needs — see TabWidget below.
        self.setAttribute(Qt.WA_InputMethodEnabled, False)
        self.viewport().setAttribute(Qt.WA_InputMethodEnabled, False)

    def set_items(self, items: list[dict], selected_csv: str) -> None:
        """Load choices, checked ones first.

        Ordering matters: these lists can run to every field definition in the
        org, and a checked row scrolled out of sight is one nobody can find to
        uncheck. Anything already selected sorts to the top.
        """
        selected = [s.strip() for s in (selected_csv or "").split(",") if s.strip()]
        self.clear()
        offered = {item["id"] for item in items}

        for missing in selected:            # selected but no longer offered
            if missing not in offered:
                self._add(f"⚠ {resolve_name(missing)} — not a valid choice, "
                          f"uncheck to remove", missing, True)
        for item in items:                  # selected and still offered
            if item["id"] in selected:
                self._add(self._label(item), item["id"], True)
        for item in items:                  # everything else
            if item["id"] not in selected:
                self._add(self._label(item), item["id"], False)

    @staticmethod
    def _label(item: dict) -> str:
        # satisfying values are plain strings, where id == name
        return item["name"] if item["name"] == item["id"] else _display(item)

    def _add(self, label: str, value: str, checked: bool) -> None:
        row = QListWidgetItem(label)
        row.setFlags(row.flags() | Qt.ItemIsUserCheckable)
        row.setCheckState(Qt.Checked if checked else Qt.Unchecked)
        row.setData(Qt.UserRole, value)
        self.addItem(row)

    def values_csv(self) -> str:
        return ", ".join(
            self.item(i).data(Qt.UserRole)
            for i in range(self.count())
            if self.item(i).checkState() == Qt.Checked
        )


class RuleDialog(QDialog):
    def __init__(self, tab: dict, initial: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Edit Rule" if initial else "Add Rule")
        self.setMinimumWidth(950)
        layout = QFormLayout(self)
        layout.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        self._entries = {}
        self._id_dropdown_cols: set = set()
        self._multi_cols: set = set()
        optional_cols = set(tab.get("optional_cols", []))
        multi_cols = tab.get("multi_cols", {})        # col -> api_key, checkbox list
        dependent_cols = tab.get("dependent_cols", {})  # col -> {source, fetch, multi}

        for col in tab["cols"]:
            api_key = DROPDOWN_FIELDS.get(col)
            items = _api_cache.get(api_key, []) if api_key else []

            if col in dependent_cols:
                # Populated from another field's current value — filled in below,
                # once every widget exists and the source can be read.
                if dependent_cols[col].get("multi"):
                    widget = MultiSelectList()
                    self._multi_cols.add(col)
                else:
                    widget = QComboBox()
                    widget.setEditable(True)  # so an ID can still be typed if the fetch fails
                    self._id_dropdown_cols.add(col)
            elif col in multi_cols:
                widget = MultiSelectList()
                widget.set_items(_api_cache.get(multi_cols[col], []), initial.get(col, ""))
                self._multi_cols.add(col)
            elif items:
                widget = QComboBox()
                if col in optional_cols:
                    widget.addItem("(none)", "")
                for item in items:
                    widget.addItem(_display(item), item["id"])
                existing_id = initial.get(col, "")
                idx = next((i for i in range(widget.count()) if widget.itemData(i) == existing_id), -1)
                if idx >= 0:
                    widget.setCurrentIndex(idx)
                elif existing_id:
                    widget.insertItem(0, f"⚠ Unknown: {resolve_name(existing_id)}", existing_id)
                    widget.setCurrentIndex(0)
                self._id_dropdown_cols.add(col)
            elif col in tab.get("choice_cols", {}):
                # Fixed set of values, not an API lookup.
                choices = tab["choice_cols"][col]
                widget = QComboBox()
                widget.addItems(choices)
                widget.setCurrentText(initial.get(col) or choices[0])
            elif col == tab.get("trigger_field"):
                widget = QComboBox()
                widget.addItems(["entered", "completed"])
                widget.setCurrentText(initial.get(col, "entered"))
            else:
                widget = QLineEdit(initial.get(col, ""))
            self._entries[col] = widget
            layout.addRow(tab["labels"][col] + ":", widget)

        # Wire each dependent field to its source, then populate once so an
        # existing rule opens with its current selection already showing.
        for col, spec in dependent_cols.items():
            source = self._entries.get(spec["source"])
            target = self._entries[col]
            fetch = DEPENDENT_FETCHERS[spec["fetch"]]

            def repopulate(_=None, col=col, source=source, target=target, fetch=fetch):
                parent_id = source.currentData() or source.currentText().strip()
                self._fill_dependent(target, fetch(parent_id), initial.get(col, ""))

            if isinstance(source, QComboBox):
                source.currentIndexChanged.connect(repopulate)
            repopulate()

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addRow(buttons)

        # Land the cursor in the first text field rather than wherever the last
        # widget built happened to leave it.
        first_text = next((self._entries[c] for c in tab["cols"]
                           if isinstance(self._entries[c], QLineEdit)), None)
        if first_text is not None:
            first_text.setFocus()

    @staticmethod
    def _fill_dependent(widget, items: list[dict], existing: str) -> None:
        """Load a dependent widget's choices, keeping any value already saved."""
        if isinstance(widget, MultiSelectList):
            widget.set_items(items, existing)
            return
        widget.clear()
        widget.addItem("(none)", "")
        for item in items:
            widget.addItem(_display(item), item["id"])
        idx = next((i for i in range(widget.count()) if widget.itemData(i) == existing), -1)
        if idx >= 0:
            widget.setCurrentIndex(idx)
        elif existing:
            widget.insertItem(1, f"⚠ Unknown: {resolve_name(existing)}", existing)
            widget.setCurrentIndex(1)

    def values(self) -> dict:
        result = {}
        for col, w in self._entries.items():
            if col in self._multi_cols:
                result[col] = w.values_csv()
            elif col in self._id_dropdown_cols:
                # Editable combos return no data for typed text, so fall back to
                # the raw text — a hand-entered ID must still save.
                result[col] = w.currentData() or w.currentText().strip()
                if result[col].startswith("(none)"):
                    result[col] = ""
            elif isinstance(w, QComboBox):
                result[col] = w.currentText()
            else:
                result[col] = w.text().strip()
        return result


class TabWidget(QWidget):
    def __init__(self, tab: dict, rules: list, parent=None):
        super().__init__(parent)
        self.tab = tab
        self.rules = rules
        layout = QVBoxLayout(self)
        if tab.get("description"):
            desc = QLabel(tab["description"])
            desc.setWordWrap(True)
            desc.setStyleSheet("color: #555; padding: 4px 2px;")
            layout.addWidget(desc)
        self.table = QTableWidget(0, len(tab["cols"]))
        self.table.setHorizontalHeaderLabels([tab["labels"][c] for c in tab["cols"]])
        for i, w in enumerate(tab["widths"]):
            self.table.setColumnWidth(i, w)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setAttribute(Qt.WA_InputMethodEnabled, False)
        self.table.viewport().setAttribute(Qt.WA_InputMethodEnabled, False)
        self.table.doubleClicked.connect(self._edit)
        layout.addWidget(self.table)
        btn_row = QHBoxLayout()
        for label, fn in [("Add", self._add), ("Edit", self._edit), ("Delete", self._delete),
                          ("▲ Up", self._move_up), ("▼ Down", self._move_down)]:
            btn = QPushButton(label)
            btn.clicked.connect(lambda checked=False, f=fn: f())
            btn_row.addWidget(btn)
        btn_row.addStretch()
        layout.addLayout(btn_row)
        self._refresh()

    def _refresh(self):
        self.table.setRowCount(0)
        for rule in self.rules:
            row = self.table.rowCount()
            self.table.insertRow(row)
            for ci, col in enumerate(self.tab["cols"]):
                self.table.setItem(row, ci,
                                   QTableWidgetItem(cell_text(col, rule.get(col, ""))))

    def _add(self):
        dlg = RuleDialog(self.tab, {}, self)
        if dlg.exec():
            self.rules.append(dlg.values())
            self._refresh()

    def _edit(self):
        print(f"DEBUG _edit called, currentRow={self.table.currentRow()}")
        row = self.table.currentRow()
        if row < 0:
            print("DEBUG _edit: no row selected, showing message")
            QMessageBox.information(self, "No row selected", "Click a row in the table to select it, then click Edit.")
            return
        print(f"DEBUG _edit: opening dialog for row {row}")
        dlg = RuleDialog(self.tab, self.rules[row], self)
        if dlg.exec():
            self.rules[row] = dlg.values()
            self._refresh()

    def _delete(self):
        row = self.table.currentRow()
        if row < 0:
            QMessageBox.information(self, "No row selected", "Click a row in the table to select it, then click Delete.")
            return
        desc = self.rules[row].get("description", "")
        if QMessageBox.question(self, "Delete", f"Delete rule:\n{desc}?") == QMessageBox.Yes:
            self.rules.pop(row)
            self._refresh()

    def _move_up(self):
        row = self.table.currentRow()
        if row <= 0:
            return
        self.rules[row - 1], self.rules[row] = self.rules[row], self.rules[row - 1]
        self._refresh()
        self.table.selectRow(row - 1)

    def _move_down(self):
        row = self.table.currentRow()
        if row < 0 or row >= len(self.rules) - 1:
            return
        self.rules[row + 1], self.rules[row] = self.rules[row], self.rules[row + 1]
        self._refresh()
        self.table.selectRow(row + 1)


class RuleEditor(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Rule Editor")
        self.setMinimumWidth(950)
        with open(RULES_FILE) as f:
            data = json.load(f)
        self.rules = {tab["key"]: list(data[tab["key"]]) for tab in TABS}
        # Snapshot so save can tell whether the anytime rules actually changed —
        # validating hits the PCP API several times and is only worth it then.
        self._anytime_on_open = json.dumps(data.get(ANYTIME_KEY, []), sort_keys=True)
        layout = QVBoxLayout(self)
        nb = QTabWidget()
        for tab in TABS:
            nb.addTab(TabWidget(tab, self.rules[tab["key"]]), tab["title"])
        layout.addWidget(nb)
        btn_row = QHBoxLayout()
        self.status_label = QLabel("")
        btn_row.addWidget(self.status_label)
        btn_row.addStretch()
        print_btn = QPushButton("Print Rules")
        print_btn.clicked.connect(self._print_rules)
        self.save_btn = QPushButton("Save")
        self.save_btn.clicked.connect(self._save)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.close)
        btn_row.addWidget(print_btn)
        btn_row.addWidget(self.save_btn)
        btn_row.addWidget(close_btn)
        layout.addLayout(btn_row)
        self._push_process = None

    def _save(self):
        with open(RULES_FILE) as f:
            data = json.load(f)
        for key, rules in self.rules.items():
            data[key] = rules
        with open(RULES_FILE, "w") as f:
            json.dump(data, f, indent=2)

        self.save_btn.setEnabled(False)
        self.status_label.setText("Pushing rules to Cloud Run (wait for 'Done' popup)....")
        self._push_process = QProcess(self)
        self._push_process.setWorkingDirectory(str(Path(__file__).parent))
        self._push_process.setProcessChannelMode(QProcess.MergedChannels)
        self._push_process.finished.connect(self._on_env_push_finished)
        self._push_process.start("bash", [str(SET_ENV_VARS_SCRIPT)])

    def _validate_anytime_rules(self) -> None:
        """After saving a changed anytime rule, check it against live PCP.

        Runs only when the anytime section actually changed, and only reports
        problems — a clean run says nothing, so this never adds friction to the
        common case. It cannot block or undo the save: the rules are already
        written, and a mid-edit state the user means to come back to is their
        business. Failure to reach PCP is silent for the same reason.
        """
        current = json.dumps(self.rules.get(ANYTIME_KEY, []), sort_keys=True)
        if current == self._anytime_on_open:
            return
        self._anytime_on_open = current
        try:
            sys.path.insert(0, str(Path(__file__).parent))
            from wf_anytimeitems_validate import problems_only, validate_rules
            problems, lines = validate_rules()
        except BaseException as e:
            print(f"Warning: could not validate anytime rules ({e})")
            return
        findings = problems_only(lines)
        if not problems:
            # Non-blocking notes go to the console, never a dialog. A deliberate
            # choice like a spelling variant in satisfying_values produces a NOTE
            # on every save, and a popup you learn to dismiss is worse than none.
            for line in findings:
                print(f"anytime rules: {line}")
            return
        box = QMessageBox(self)
        box.setIcon(QMessageBox.Warning)
        box.setWindowTitle("Anytime rules — setup problems found")
        box.setText(
            f"Your rules were saved, but checking them against Planning Center "
            f"found {problems} setup problem{'s' if problems != 1 else ''}.\n\n"
            "Fix these or cards will never leave the holding step, with nothing "
            "in the logs to explain why."
        )
        box.setInformativeText("\n".join(findings))
        box.setDetailedText("\n".join(lines))
        box.exec()

    def _on_env_push_finished(self, exit_code: int, _exit_status) -> None:
        output = bytes(self._push_process.readAllStandardOutput()).decode(errors="replace")
        self.save_btn.setEnabled(True)
        self.status_label.setText("")
        if exit_code == 0:
            QMessageBox.information(self, "Saved",
                "Rules saved to rules.json and pushed live to Cloud Run.\n"
                "(Code changes still require running deploy.sh separately.)")
            self._validate_anytime_rules()
        else:
            QMessageBox.warning(self, "Push failed",
                "Rules were saved to rules.json, but pushing them to Cloud Run failed:\n\n"
                f"{output}\n\nClick Save to retry, or run ./set-env-vars.sh manually.")

    def _cell_html(self, col: str, value: str) -> str:
        """Format one cell. ID columns are shown as 'Name (id)' when the name is known."""
        return html.escape(cell_text(col, value))

    def _print_rules(self):
        """Write all rule tables to rules_report.html and open it in the browser."""
        parts = [
            "<!DOCTYPE html><html><head><meta charset='utf-8'>",
            "<title>PCP → CC Automation Rules</title>",
            "<style>",
            "body{font-family:-apple-system,Helvetica,Arial,sans-serif;margin:32px;color:#222;}",
            "h1{font-size:20px;} h2{font-size:15px;margin-top:28px;}",
            "p.generated{color:#777;font-size:12px;}",
            "table{border-collapse:collapse;width:100%;margin-top:6px;font-size:12px;}",
            "th,td{border:1px solid #bbb;padding:4px 8px;text-align:left;vertical-align:top;}",
            "th{background:#eee;} tr:nth-child(even) td{background:#f7f7f7;}",
            "p.empty{color:#999;font-style:italic;}",
            "@media print{h2{page-break-after:avoid;} tr{page-break-inside:avoid;}}",
            "</style></head><body>",
            "<h1>PCP → CC Automation Rules</h1>",
            f"<p class='generated'>Created by {html.escape(Path(__file__).name)}</p>",
            f"<p class='generated'>Generated {html.escape(date.today().isoformat())}</p>",
        ]
        for tab in TABS:
            rules = self.rules[tab["key"]]
            parts.append(f"<h2>{html.escape(tab['title'])}</h2>")
            if not rules:
                parts.append("<p class='empty'>No rules.</p>")
                continue
            parts.append("<table><tr>")
            for col in tab["cols"]:
                parts.append(f"<th>{html.escape(tab['labels'][col])}</th>")
            parts.append("</tr>")
            for rule in rules:
                parts.append("<tr>")
                for col in tab["cols"]:
                    parts.append(f"<td>{self._cell_html(col, rule.get(col, ''))}</td>")
                parts.append("</tr>")
            parts.append("</table>")
        parts.append("</body></html>")
        REPORT_FILE.write_text("\n".join(parts), encoding="utf-8")
        webbrowser.open(REPORT_FILE.as_uri())
        QMessageBox.information(self, "Rules printed",
            f"Rules written to {REPORT_FILE.name} and opened in your browser.\n"
            "Use the browser's Print (⌘P) to print or save as PDF.")


def main():
    # Line-buffered stdout so the user sees loading messages even when
    # launched detached (block-buffered if stdout isn't detected as a TTY).
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    # Fetch BEFORE creating the QApplication. When this script is launched
    # as a subprocess of another Qt app (pco_utils.py), having two Qt
    # apps alive while gRPC/network calls run on the child hangs or
    # silently empties the dropdowns on macOS. Doing the fetch first means
    # there is no Qt state in this process during the network work, and
    # (with detach=True in the launcher) the parent has already exited by
    # the time we open the window.
    print("Loading PCP and Constant Contact ID lists — please wait ~30 seconds…")
    _ensure_api_data()
    print("Done. Opening editor window.")

    app = QApplication.instance() or QApplication(sys.argv)
    window = RuleEditor()
    window.show()
    window.activateWindow()
    window.raise_()
    _macos_activate()
    app.exec()


def _macos_activate() -> None:
    """Bring this process to the macOS foreground (needed when launched as a subprocess)."""
    try:
        from AppKit import NSApp  # type: ignore[import]
        NSApp.activateIgnoringOtherApps_(True)
        return
    except ImportError:
        pass
    try:
        import os, subprocess as _sp
        _sp.Popen(
            ["osascript", "-e",
             f"tell application \"System Events\" to set frontmost of first process"
             f" whose unix id is {os.getpid()} to true"],
            stdout=_sp.DEVNULL, stderr=_sp.DEVNULL,
        )
    except Exception:
        pass


if __name__ == "__main__":
    main()
