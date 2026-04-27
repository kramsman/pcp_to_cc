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
        assert person["custom_fields"]["1039700"] == ["true"]

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
        assert "a8a7f3ea-1298-11ed-a555-fa163ec0164a" in list_ids

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
        assert "a8a7f3ea-1298-11ed-a555-fa163ec0164a" in list_ids

    def test_what_brings_you_social_justice(self, pcp_person_with_social_justice):
        from pcp_to_cc.main import apply_rules, parse_person
        person   = parse_person(pcp_person_with_social_justice)
        list_ids = apply_rules(person)

        print(f"\nmatched list_ids: {list_ids}")
        assert "3701fc00-8ca9-11ed-946d-fa163e57b7cb" in list_ids

    def test_what_brings_you_no_match(self, pcp_person_with_social_justice):
        from pcp_to_cc.main import apply_rules, parse_person
        pcp_person_with_social_justice["included"][1]["attributes"]["value"] = "Community"
        person   = parse_person(pcp_person_with_social_justice)
        list_ids = apply_rules(person)

        print(f"\nmatched list_ids: {list_ids}")
        assert "3701fc00-8ca9-11ed-946d-fa163e57b7cb" not in list_ids


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


# ── TestCCTokenRefresh ─────────────────────────────────────────────────────────

class TestCCTokenRefresh:
    """Tests for CC access token refresh on 401."""

    def test_refresh_success(self):
        """_refresh_cc_token() updates cache and Secret Manager on success."""
        from unittest.mock import MagicMock, patch

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"access_token": "new-token-abc"}
        mock_resp.raise_for_status.return_value = None

        with patch("pcp_to_cc.main.requests.post", return_value=mock_resp), \
             patch("pcp_to_cc.main.update_secret") as mock_update:
            from pcp_to_cc.main import _refresh_cc_token
            result = _refresh_cc_token()

        print(f"\nresult={result}")
        assert result is True
        mock_update.assert_called_once_with("CC_ACCESS_TOKEN", "new-token-abc")

    def test_refresh_bad_response(self):
        """_refresh_cc_token() returns False when response has no access_token."""
        from unittest.mock import MagicMock, patch

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {}       # missing access_token
        mock_resp.raise_for_status.return_value = None

        with patch("pcp_to_cc.main.requests.post", return_value=mock_resp), \
             patch("pcp_to_cc.main.update_secret") as mock_update:
            from pcp_to_cc.main import _refresh_cc_token
            result = _refresh_cc_token()

        print(f"\nresult={result}")
        assert result is False
        mock_update.assert_not_called()

    def test_add_to_cc_retries_on_401(self, pcp_person_with_opt_in):
        """add_to_cc() refreshes token and retries once on 401."""
        from unittest.mock import MagicMock, call, patch
        from pcp_to_cc.main import add_to_cc, parse_person

        person = parse_person(pcp_person_with_opt_in)

        resp_401 = MagicMock()
        resp_401.status_code = 401

        resp_200 = MagicMock()
        resp_200.status_code = 200
        resp_200.raise_for_status.return_value = None

        with patch("pcp_to_cc.main.requests.post", side_effect=[resp_401, resp_200]) as mock_post, \
             patch("pcp_to_cc.main._refresh_cc_token", return_value=True) as mock_refresh:
            result = add_to_cc(person, ["cc-list-uuid-001"])

        print(f"\nresult={result}  post_calls={mock_post.call_count}  refresh_calls={mock_refresh.call_count}")
        assert result is True
        assert mock_post.call_count == 2
        mock_refresh.assert_called_once()

    def test_add_to_cc_body_matches_payload_file(self, pcp_person_with_opt_in, cc_add_contact_payload):
        """Body sent to CC POST /contacts must match tests/payloads/cc_add_contact.json exactly."""
        from unittest.mock import MagicMock, patch
        from pcp_to_cc.main import add_to_cc, parse_person

        person = parse_person(pcp_person_with_opt_in)

        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_resp.raise_for_status.return_value = None

        with patch("pcp_to_cc.main.requests.post", return_value=mock_resp) as mock_post:
            add_to_cc(person, ["cc-list-uuid-001"])

        actual_body = mock_post.call_args.kwargs["json"]
        print(f"\nactual body:   {actual_body}")
        print(f"expected body: {cc_add_contact_payload}")
        assert actual_body == cc_add_contact_payload

    def test_add_to_cc_updates_on_409_conflict(self, pcp_person_with_opt_in):
        """On 409 conflict, extract contact_id and PUT to update existing contact."""
        from unittest.mock import MagicMock, patch
        from pcp_to_cc.main import add_to_cc, parse_person

        person = parse_person(pcp_person_with_opt_in)

        resp_409 = MagicMock()
        resp_409.status_code = 409
        resp_409.json.return_value = [{
            "error_key": "contacts.api.conflict",
            "error_message": "Email already exists for contact existing-contact-uuid-001"
        }]

        resp_200 = MagicMock()
        resp_200.status_code = 200
        resp_200.raise_for_status.return_value = None

        with patch("pcp_to_cc.main.requests.post", return_value=resp_409), \
             patch("pcp_to_cc.main.requests.put", return_value=resp_200) as mock_put:
            result = add_to_cc(person, ["cc-list-uuid-001"])

        print(f"\nresult={result}")
        print(f"PUT called with URL: {mock_put.call_args}")
        assert result is True
        assert "existing-contact-uuid-001" in str(mock_put.call_args)

    def test_add_to_cc_409_no_contact_id_returns_false(self, pcp_person_with_opt_in):
        """On 409 conflict with unparseable error, return False."""
        from unittest.mock import MagicMock, patch
        from pcp_to_cc.main import add_to_cc, parse_person

        person = parse_person(pcp_person_with_opt_in)

        resp_409 = MagicMock()
        resp_409.status_code = 409
        resp_409.json.return_value = [{"error_key": "contacts.api.conflict", "error_message": "unknown"}]
        resp_409.text = "unknown conflict"

        with patch("pcp_to_cc.main.requests.post", return_value=resp_409):
            result = add_to_cc(person, ["cc-list-uuid-001"])

        print(f"\nresult={result}")
        assert result is False

    def test_add_to_cc_fails_if_refresh_fails(self, pcp_person_with_opt_in):
        """add_to_cc() returns False when token refresh itself fails."""
        from unittest.mock import MagicMock, patch
        from pcp_to_cc.main import add_to_cc, parse_person

        person = parse_person(pcp_person_with_opt_in)

        resp_401 = MagicMock()
        resp_401.status_code = 401

        with patch("pcp_to_cc.main.requests.post", return_value=resp_401), \
             patch("pcp_to_cc.main._refresh_cc_token", return_value=False):
            result = add_to_cc(person, ["cc-list-uuid-001"])

        print(f"\nresult={result}")
        assert result is False
