"""GUI editor for PCP → CC automation rules. Reads/writes rules.json."""

import json
import sys
from pathlib import Path

_UTILS_ROOT = Path("/Users/Denise/Library/CloudStorage/Dropbox/PythonPrograms/uvbekutils")
if str(_UTILS_ROOT) not in sys.path:
    sys.path.insert(0, str(_UTILS_ROOT))

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QTabWidget,
    QTableWidget, QTableWidgetItem, QPushButton, QDialog, QFormLayout,
    QLineEdit, QComboBox, QMessageBox, QDialogButtonBox,
)

RULES_FILE = Path(__file__).parent / "rules.json"

DROPDOWN_FIELDS = {
    "workflow_id":          "pcp_workflow",
    "add_to_workflow_id":   "pcp_workflow",
    "complete_workflow_id": "pcp_workflow",
    "field_id":             "pcp_field",
    "pcp_field_id":         "pcp_field",
    "form_id":              "pcp_form",
    "cc_list_id":           "cc_list",
}

TABS = [
    {
        "title":         "Set PCP Field Values",
        "key":           "workflow_field_rules",
        "cols":          ["description", "workflow_id", "field_id", "trigger", "value"],
        "labels":        {
            "description": "Description",
            "workflow_id": "PCP Workflow ID",
            "field_id":    "PCP Field ID",
            "trigger":     "Trigger",
            "value":       "Value to set",
        },
        "widths":        [375, 110, 110, 110, 160],
        "trigger_field": "trigger",
    },
    {
        "title":         "Chain PCP Workflows",
        "key":           "workflow_chain_rules",
        "cols":          ["description", "workflow_id", "trigger", "add_to_workflow_id"],
        "labels":        {
            "description":        "Description",
            "workflow_id":        "Start Workflow ID",
            "trigger":            "Trigger",
            "add_to_workflow_id": "Add to PC Workflow ID",
        },
        "widths":        [375, 150, 110, 175],
        "trigger_field": "trigger",
    },
    {
        "title":         "Delete from Workflow on Form",
        "key":           "form_completion_rules",
        "cols":          ["description", "form_id", "complete_workflow_id"],
        "labels":        {
            "description":          "Description",
            "form_id":              "PCP trigger Form ID",
            "complete_workflow_id": "Workflow to Complete ID",
        },
        "widths":        [375, 150, 175],
        "trigger_field": None,
    },
    {
        "title":         "Assign to CC Lists",
        "key":           "cc_list_rules",
        "cols":          ["description", "pcp_field_id", "pcp_value", "cc_list_id"],
        "labels":        {
            "description":  "Description",
            "pcp_field_id": "PCP Field ID",
            "pcp_value":    "PCP Field Check Value",
            "cc_list_id":   "CC List UUID",
        },
        "widths":        [350, 125, 125, 375],
        "trigger_field": None,
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


class RuleDialog(QDialog):
    def __init__(self, tab: dict, initial: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Edit Rule" if initial else "Add Rule")
        self.setMinimumWidth(950)
        layout = QFormLayout(self)
        layout.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        self._entries = {}
        self._id_dropdown_cols: set = set()
        for col in tab["cols"]:
            api_key = DROPDOWN_FIELDS.get(col)
            items = _api_cache.get(api_key, []) if api_key else []
            if items:
                widget = QComboBox()
                for item in items:
                    widget.addItem(f"{item['name']} ({item['id']})", item["id"])
                existing_id = initial.get(col, "")
                idx = next((i for i in range(widget.count()) if widget.itemData(i) == existing_id), -1)
                if idx >= 0:
                    widget.setCurrentIndex(idx)
                elif existing_id:
                    widget.insertItem(0, f"⚠ Unknown: {existing_id}", existing_id)
                    widget.setCurrentIndex(0)
                self._id_dropdown_cols.add(col)
            elif col == tab.get("trigger_field"):
                widget = QComboBox()
                widget.addItems(["entered", "completed"])
                widget.setCurrentText(initial.get(col, "entered"))
            else:
                widget = QLineEdit(initial.get(col, ""))
            self._entries[col] = widget
            layout.addRow(tab["labels"][col] + ":", widget)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addRow(buttons)

    def values(self) -> dict:
        result = {}
        for col, w in self._entries.items():
            if col in self._id_dropdown_cols:
                result[col] = w.currentData() or ""
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
                self.table.setItem(row, ci, QTableWidgetItem(rule.get(col, "")))

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
        layout = QVBoxLayout(self)
        nb = QTabWidget()
        for tab in TABS:
            nb.addTab(TabWidget(tab, self.rules[tab["key"]]), tab["title"])
        layout.addWidget(nb)
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        save_btn = QPushButton("Save")
        save_btn.clicked.connect(self._save)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.close)
        btn_row.addWidget(save_btn)
        btn_row.addWidget(close_btn)
        layout.addLayout(btn_row)

    def _save(self):
        with open(RULES_FILE) as f:
            data = json.load(f)
        for key, rules in self.rules.items():
            data[key] = rules
        with open(RULES_FILE, "w") as f:
            json.dump(data, f, indent=2)
        QMessageBox.information(self, "Saved",
            "Rules saved to rules.json.\nRun deploy.sh to apply changes to Cloud Run.")


def main():
    # Line-buffered stdout so the user sees loading messages even when
    # launched detached (block-buffered if stdout isn't detected as a TTY).
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    # Fetch BEFORE creating the QApplication. When this script is launched
    # as a subprocess of another Qt app (pcp_launcher.py), having two Qt
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
