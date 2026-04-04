"""
Shared fixtures for pcp-to-cc tests.

All external calls (GCP Secret Manager, PCP API, CC API) are mocked so tests
run without credentials. Use @pytest.mark.integration for tests that need live services.
"""

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ── Path helpers ───────────────────────────────────────────────────────────────

PAYLOADS_DIR = Path(__file__).parent / "payloads"


def load_payload(filename: str) -> dict:
    with open(PAYLOADS_DIR / filename) as f:
        return json.load(f)


# ── Environment setup ──────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def set_env_vars(monkeypatch):
    """
    Set required env vars before each test so config.py imports without error.
    PCP_NEWSLETTER_FIELD_ID=999 matches the field_definition id in the mock PCP API response.
    """
    monkeypatch.setenv("CLOUD_PROJECT_ID",       "test-project")
    monkeypatch.setenv("TEST_MODE",              "true")
    monkeypatch.setenv("LOG_PAYLOADS",           "false")
    monkeypatch.setenv("PCP_NEWSLETTER_FIELD_ID", "999")
    monkeypatch.setenv("CC_NEWSLETTER_LIST_ID",   "cc-list-uuid-001")


# ── Secret Manager mock ────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def mock_secret_manager():
    """Prevent any real GCP Secret Manager calls during tests."""
    fake_secrets = {
        "PCP_APP_ID":        "fake-pcp-app-id",
        "PCP_SECRET":        "fake-pcp-secret",
        "CC_ACCESS_TOKEN":   "fake-cc-access-token",
        "CC_REFRESH_TOKEN":  "fake-cc-refresh-token",
        "CC_API_KEY":        "fake-cc-api-key",
        "CC_API_SECRET":     "fake-cc-api-secret",
    }

    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.payload.data.decode.side_effect = lambda enc: fake_secrets.get(
        # extract secret_id from the name string "projects/.../secrets/<id>/versions/latest"
        "UNKNOWN", ""
    )

    def fake_access(request):
        name = request["name"]
        secret_id = name.split("/secrets/")[1].split("/")[0]
        mock_response.payload.data.decode.return_value = fake_secrets.get(secret_id, "")
        return mock_response

    mock_client.access_secret_version.side_effect = fake_access

    with patch("google.cloud.secretmanager.SecretManagerServiceClient", return_value=mock_client):
        yield mock_client


# ── PCP API fixtures ───────────────────────────────────────────────────────────

@pytest.fixture
def pcp_person_with_opt_in():
    """PCP API response for a person who opted in to the newsletter."""
    return {
        "data": {
            "type": "Person",
            "id": "12345678",
            "attributes": {
                "first_name": "Jane",
                "last_name": "Smith",
            },
        },
        "included": [
            {
                "type": "Email",
                "id": "111",
                "attributes": {
                    "address": "jane.smith@example.com",
                    "primary": True,
                },
            },
            {
                "type": "FieldDatum",
                "id": "222",
                "attributes": {"value": "Yes"},
                "relationships": {
                    "field_definition": {
                        "data": {"type": "FieldDefinition", "id": "999"}
                    }
                },
            },
        ],
    }


@pytest.fixture
def pcp_person_no_opt_in():
    """PCP API response for a person who did NOT opt in."""
    return {
        "data": {
            "type": "Person",
            "id": "12345679",
            "attributes": {"first_name": "Bob", "last_name": "Jones"},
        },
        "included": [
            {
                "type": "Email",
                "id": "333",
                "attributes": {"address": "bob@example.com", "primary": True},
            },
            {
                "type": "FieldDatum",
                "id": "444",
                "attributes": {"value": "No"},
                "relationships": {
                    "field_definition": {
                        "data": {"type": "FieldDefinition", "id": "999"}
                    }
                },
            },
        ],
    }


@pytest.fixture
def pcp_person_no_email():
    """PCP API response for a person with no email (e.g. a child)."""
    return {
        "data": {
            "type": "Person",
            "id": "12345680",
            "attributes": {"first_name": "Child", "last_name": "Smith"},
        },
        "included": [
            {
                "type": "FieldDatum",
                "id": "555",
                "attributes": {"value": "Yes"},
                "relationships": {
                    "field_definition": {
                        "data": {"type": "FieldDefinition", "id": "999"}
                    }
                },
            },
        ],
    }


# ── CC payload fixture ─────────────────────────────────────────────────────────

@pytest.fixture
def cc_add_contact_payload():
    """Expected JSON body sent to CC POST /v3/contacts to add a contact to a list."""
    return load_payload("cc_add_contact.json")


# ── Webhook payload fixtures ───────────────────────────────────────────────────

@pytest.fixture
def webhook_payload():
    """Valid PCP person.created webhook payload."""
    return load_payload("person_created_webhook.json")


@pytest.fixture
def flask_client(mock_secret_manager):
    """Flask test client with Secret Manager mocked."""
    # Import app after env vars are set by set_env_vars fixture
    import importlib
    import sys

    # Reload config and main to pick up monkeypatched env vars
    for mod in ["config", "pcp_to_cc.config", "pcp_to_cc.main"]:
        if mod in sys.modules:
            del sys.modules[mod]

    from pcp_to_cc.main import app
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client
