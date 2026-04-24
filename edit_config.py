"""GUI editor for PCP → CC automation rules. Reads/writes rules.json."""

import json
import sys
from pathlib import Path

_UTILS_ROOT = Path("/Users/Denise/Library/CloudStorage/Dropbox/PythonPrograms/uvbekutils")
if str(_UTILS_ROOT) not in sys.path:
    sys.path.insert(0, str(_UTILS_ROOT))

from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QTabWidget,
    QTableWidget, QTableWidgetItem, QPushButton, QDialog, QFormLayout,
    QLineEdit, QComboBox, QMessageBox, QDialogButtonBox,
)

RULES_FILE = Path(__file__).parent / "rules.json"

TABS = [
    {
        "title":         "Workflow Field Rules",
        "key":           "workflow_field_rules",
        "cols":          ["description", "workflow_id", "field_id", "trigger", "value"],
        "labels":        {
            "description": "Description",
            "workflow_id": "Workflow ID",
            "field_id":    "Field ID",
            "trigger":     "Trigger",
            "value":       "Value to set",
        },
        "widths":        [300, 90, 90, 90, 130],
        "trigger_field": "trigger",
    },
    {
        "title":         "Chain Rules",
        "key":           "workflow_chain_rules",
        "cols":          ["description", "workflow_id", "trigger", "add_to_workflow_id"],
        "labels":        {
            "description":        "Description",
            "workflow_id":        "Source Workflow ID",
            "trigger":            "Trigger",
            "add_to_workflow_id": "Add to Workflow ID",
        },
        "widths":        [300, 120, 90, 140],
        "trigger_field": "trigger",
    },
    {
        "title":         "CC List Rules",
        "key":           "cc_list_rules",
        "cols":          ["description", "pcp_field_id", "pcp_value", "cc_list_id"],
        "labels":        {
            "description":  "Description",
            "pcp_field_id": "PCP Field ID",
            "pcp_value":    "PCP Field Value",
            "cc_list_id":   "CC List UUID",
        },
        "widths":        [280, 100, 100, 300],
        "trigger_field": None,
    },
]


class RuleDialog(QDialog):
    def __init__(self, tab: dict, initial: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Edit Rule" if initial else "Add Rule")
        self.setMinimumWidth(760)
        layout = QFormLayout(self)
        layout.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        self._entries = {}
        for col in tab["cols"]:
            if col == tab.get("trigger_field"):
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
            result[col] = w.currentText() if isinstance(w, QComboBox) else w.text().strip()
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
        self.table.doubleClicked.connect(self._edit)
        layout.addWidget(self.table)
        btn_row = QHBoxLayout()
        for label, fn in [("Add", self._add), ("Edit", self._edit), ("Delete", self._delete)]:
            btn = QPushButton(label)
            btn.clicked.connect(fn)
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
        row = self.table.currentRow()
        if row < 0:
            return
        dlg = RuleDialog(self.tab, self.rules[row], self)
        if dlg.exec():
            self.rules[row] = dlg.values()
            self._refresh()

    def _delete(self):
        row = self.table.currentRow()
        if row < 0:
            return
        desc = self.rules[row].get("description", "")
        if QMessageBox.question(self, "Delete", f"Delete rule:\n{desc}?") == QMessageBox.Yes:
            self.rules.pop(row)
            self._refresh()


class RuleEditor(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("PCP → CC Rule Editor")
        self.setMinimumWidth(760)
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
    app = QApplication.instance() or QApplication(sys.argv)
    window = RuleEditor()
    window.show()
    app.exec()


if __name__ == "__main__":
    main()
