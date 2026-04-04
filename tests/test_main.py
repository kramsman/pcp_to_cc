"""
Tests for pcp_to_cc/main.py

Run all:         pytest tests/ -s -v
Run one class:   pytest tests/test_main.py::TestParsePerson -s -v
Integration:     pytest tests/ -s -v -m integration
"""

import json
from unittest.mock import MagicMock, patch

import pytest


# ── TestParsePerson ────────────────────────────────────────────────────────────

class TestParsePerson:
    """Tests for parse_person() — PCP API response → flat dict."""

    def test_happy_path(self, pcp_person_with_opt_in):
        from pcp_to_cc.main import parse_person
        person = parse_person(pcp_person_with_opt_in)

        print(f"\nperson: {json.dumps(person, indent=2)}")
        assert person["person_id"]  == "12345678"
        assert person["first_name"] == "Jane"
        assert person["last_name"]  == "Smith"
        assert person["email"]      == "jane.smith@example.com"
        assert person["custom_fields"]["999"] == "Yes"

    def test_no_email(self, pcp_person_no_email):
        from pcp_to_cc.main import parse_person
        person = parse_person(pcp_person_no_email)

        print(f"\nperson: {json.dumps(person, indent=2)}")
        assert person["email"] == ""
        assert person["first_name"] == "Child"

    def test_name_title_cased(self, pcp_person_with_opt_in):
        from pcp_to_cc.main import parse_person
        pcp_person_with_opt_in["data"]["attributes"]["first_name"] = "JANE"
        pcp_person_with_opt_in["data"]["attributes"]["last_name"]  = "smith"
        person = parse_person(pcp_person_with_opt_in)

        assert person["first_name"] == "Jane"
        assert person["last_name"]  == "Smith"

    def test_email_lowercased(self, pcp_person_with_opt_in):
        from pcp_to_cc.main import parse_person
        pcp_person_with_opt_in["included"][0]["attributes"]["address"] = "Jane.Smith@EXAMPLE.COM"
        person = parse_person(pcp_person_with_opt_in)

        assert person["email"] == "jane.smith@example.com"

    def test_empty_response(self):
        from pcp_to_cc.main import parse_person
        person = parse_person({})

        assert person["person_id"]   == ""
        assert person["first_name"]  == ""
        assert person["email"]       == ""
        assert person["custom_fields"] == {}


# ── TestApplyRules ─────────────────────────────────────────────────────────────

class TestApplyRules:
    """Tests for apply_rules() — person dict → list of CC list UUIDs."""

    def test_rule_matches(self, pcp_person_with_opt_in):
        from pcp_to_cc.main import apply_rules, parse_person
        person   = parse_person(pcp_person_with_opt_in)
        list_ids = apply_rules(person)

        print(f"\nmatched list_ids: {list_ids}")
        assert "cc-list-uuid-001" in list_ids

    def test_rule_not_matched(self, pcp_person_no_opt_in):
        from pcp_to_cc.main import apply_rules, parse_person
        person   = parse_person(pcp_person_no_opt_in)
        list_ids = apply_rules(person)

        print(f"\nmatched list_ids: {list_ids}")
        assert list_ids == []

    def test_no_email_no_match(self, pcp_person_no_email):
        from pcp_to_cc.main import apply_rules, parse_person
        person   = parse_person(pcp_person_no_email)
        # Note: apply_rules doesn't check email — that's the webhook route's job.
        # But we confirm the rule match still works for the opt-in field.
        list_ids = apply_rules(person)

        print(f"\nmatched list_ids: {list_ids}")
        assert "cc-list-uuid-001" in list_ids


# ── TestWebhookRoute ───────────────────────────────────────────────────────────

class TestWebhookRoute:
    """Tests for POST /webhook Flask route."""

    def test_health(self, flask_client):
        resp = flask_client.get("/health")
        print(f"\nstatus={resp.status_code}  body={resp.get_json()}")
        assert resp.status_code == 200
        assert resp.get_json()["status"] == "ok"

    def test_bad_payload_not_dict(self, flask_client):
        resp = flask_client.post(
            "/webhook", data="not json", content_type="application/json"
        )
        print(f"\nstatus={resp.status_code}  body={resp.get_json()}")
        assert resp.status_code == 400

    def test_ignored_event(self, flask_client, webhook_payload):
        webhook_payload["name"] = "person.updated"
        resp = flask_client.post("/webhook", json=webhook_payload)
        print(f"\nstatus={resp.status_code}  body={resp.get_json()}")
        assert resp.status_code == 200
        assert resp.get_json()["status"] == "ignored"

    def test_missing_person_id(self, flask_client):
        payload = {"name": "person.created", "payload": {}}
        resp = flask_client.post("/webhook", json=payload)
        print(f"\nstatus={resp.status_code}  body={resp.get_json()}")
        assert resp.status_code == 400

    def test_skipped_no_email(self, flask_client, webhook_payload, pcp_person_no_email):
        with patch("pcp_to_cc.main.fetch_person_from_pcp", return_value=pcp_person_no_email):
            resp = flask_client.post("/webhook", json=webhook_payload)
        print(f"\nstatus={resp.status_code}  body={resp.get_json()}")
        assert resp.status_code == 200
        assert resp.get_json()["status"] == "skipped"
        assert resp.get_json()["reason"] == "no email"

    def test_skipped_no_rules_matched(self, flask_client, webhook_payload, pcp_person_no_opt_in):
        with patch("pcp_to_cc.main.fetch_person_from_pcp", return_value=pcp_person_no_opt_in):
            resp = flask_client.post("/webhook", json=webhook_payload)
        print(f"\nstatus={resp.status_code}  body={resp.get_json()}")
        assert resp.status_code == 200
        assert resp.get_json()["status"] == "skipped"
        assert resp.get_json()["reason"] == "no rules matched"

    def test_test_mode_logs_instead_of_sending(self, flask_client, webhook_payload, pcp_person_with_opt_in):
        """TEST_MODE=true should not call CC API — just log what would happen."""
        with patch("pcp_to_cc.main.fetch_person_from_pcp", return_value=pcp_person_with_opt_in), \
             patch("pcp_to_cc.main.add_to_cc") as mock_add:
            resp = flask_client.post("/webhook", json=webhook_payload)

        print(f"\nstatus={resp.status_code}  body={resp.get_json()}")
        assert resp.status_code == 200
        assert resp.get_json()["status"] == "test_mode"
        assert "cc-list-uuid-001" in resp.get_json()["would_add_to_lists"]
        mock_add.assert_not_called()

    def test_pcp_fetch_failure_returns_502(self, flask_client, webhook_payload):
        with patch("pcp_to_cc.main.fetch_person_from_pcp", return_value=None):
            resp = flask_client.post("/webhook", json=webhook_payload)
        print(f"\nstatus={resp.status_code}  body={resp.get_json()}")
        assert resp.status_code == 502

    def test_settings_route(self, flask_client):
        resp = flask_client.get("/settings")
        print(f"\nstatus={resp.status_code}  body={resp.get_json()}")
        assert resp.status_code == 200
        body = resp.get_json()
        assert "CC_LIST_RULES" in body
        assert "TEST_MODE" in body
