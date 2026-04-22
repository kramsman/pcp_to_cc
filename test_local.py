"""
Local test script for pcp-to-cc webhook.

Usage:
    1. Start the server:       python pcp_to_cc/main.py
    2. In another terminal:    python test_local.py

Edit the payloads below to test different scenarios.
Set LOG_PAYLOADS=true and TEST_MODE=true in .env to see full output without touching CC.
"""

import json

import requests

SERVER_URL = "http://localhost:8080"

# ── Sample PCP person.created webhook payload ──────────────────────────────────
# This is the webhook PCP sends to our endpoint when a new person is created.
# The person_id here (12345678) is a placeholder — for a real test, use a real PCP person ID.
#
# NOTE: The actual payload format from PCP may differ slightly.
# Set LOG_PAYLOADS=true and trigger a real PCP webhook to capture the real format.

WEBHOOK_PAYLOAD = {
    "data": [
        {
            "id": "fc40af07-f707-46cb-bc97-08f080e74ac7",
            "type": "EventDelivery",
            "attributes": {
                "name": "people.v2.events.person.created",
                "attempt": 1,
                "payload": '{"data":{"type":"Person","id":"12345678","attributes":{"first_name":"Test","last_name":"Person","created_at":"2026-04-03T12:00:00Z"}}}',
                # ^^^ replace 12345678 with a real PCP person ID for live testing
            },
            "relationships": {
                "organization": {"data": {"type": "Organization", "id": "526881"}}
            },
        }
    ]
}

# ── Sample workflow_card.created payload ──────────────────────────────────────
# Replace person_id (190947666) and workflow_id (730471) with real values for live testing.
# The inner payload is a JSON string — same pattern as person.created.

WORKFLOW_CARD_CREATED_PAYLOAD = {
    "event": "people.v2.events.workflow_card.created",
    "payload": {
        "data": [
            {
                "id": "d6748dcd-e3b2-48a0-96f8-4850c616f8d6",
                "type": "EventDelivery",
                "attributes": {
                    "name": "people.v2.events.workflow_card.created",
                    "attempt": 1,
                    "payload": '{"data":{"type":"WorkflowCard","id":"48257343","attributes":{"stage":"ready"},"relationships":{"person":{"data":{"type":"Person","id":"190947666"}},"workflow":{"data":{"type":"Workflow","id":"730471"}},"assignee":{"data":{"type":"Assignee","id":"190519711"}},"current_step":{"data":{"type":"WorkflowStep","id":"1994220"}}}}}',
                },
                "relationships": {
                    "organization": {"data": {"type": "Organization", "id": "526881"}}
                },
            }
        ]
    },
}

IGNORED_EVENT_PAYLOAD = {
    "data": [
        {
            "type": "EventDelivery",
            "attributes": {
                "name": "people.v2.events.person.updated",
                "payload": '{"data":{"type":"Person","id":"12345678"}}',
            },
        }
    ]
}


def post(path: str, payload: dict, label: str):
    print(f"\n{'='*60}")
    print(f"{label}")
    print(f"POST {SERVER_URL}{path}")
    print(f"Payload: {json.dumps(payload, indent=2)}")
    print("="*60)
    try:
        resp = requests.post(
            f"{SERVER_URL}{path}",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        print(f"Status:   {resp.status_code}")
        print(f"Response: {json.dumps(resp.json(), indent=2)}")
    except requests.exceptions.ConnectionError:
        print("ERROR: Could not connect. Is the server running?")
        print("  Run:  python pcp_to_cc/main.py")


def test_health():
    print(f"\n{'='*60}")
    print(f"GET {SERVER_URL}/health")
    print("="*60)
    try:
        resp = requests.get(f"{SERVER_URL}/health", timeout=5)
        print(f"Status:   {resp.status_code}")
        print(f"Response: {json.dumps(resp.json(), indent=2)}")
    except requests.exceptions.ConnectionError:
        print("ERROR: Could not connect. Is the server running?")


def test_settings():
    print(f"\n{'='*60}")
    print(f"GET {SERVER_URL}/settings")
    print("="*60)
    try:
        resp = requests.get(f"{SERVER_URL}/settings", timeout=5)
        print(f"Status:   {resp.status_code}")
        print(f"Response: {json.dumps(resp.json(), indent=2)}")
    except requests.exceptions.ConnectionError:
        print("ERROR: Could not connect.")


def test_bad_payload():
    post("/webhook", {"bad": "not a webhook"}, "Test: bad payload (should return 400)")
    post("/webhook", IGNORED_EVENT_PAYLOAD, "Test: ignored event (should return 200 ignored)")


if __name__ == "__main__":
    test_health()
    test_settings()
    post("/webhook", WEBHOOK_PAYLOAD, "Test: person.created webhook")
    post("/webhook", WORKFLOW_CARD_CREATED_PAYLOAD, "Test: workflow_card.created (should apply workflow field rules)")
    test_bad_payload()
