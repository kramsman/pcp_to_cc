"""
PCP → Constant Contact webhook receiver.

Receives person.created webhooks from Planning Center People,
fetches the full person record from the PCP API (including email
and custom field data), then adds matching profiles to Constant
Contact lists based on CC_LIST_RULES in config.py.

Usage (local dev):
    python pcp_to_cc/main.py          # start Flask dev server
    python test_local.py              # send a test webhook in another terminal
"""

import os

import requests
from flask import Flask, jsonify, request
from google.cloud import secretmanager
from loguru import logger

import config

app = Flask(__name__)

# ─── GCP Secret Manager ───────────────────────────────────────────────────────

_secret_client: secretmanager.SecretManagerServiceClient | None = None
_secrets: dict[str, str] = {}


def _get_secret_client() -> secretmanager.SecretManagerServiceClient:
    global _secret_client
    if _secret_client is None:
        _secret_client = secretmanager.SecretManagerServiceClient()
    return _secret_client


def get_secret(secret_id: str) -> str:
    """
    Fetch a secret from GCP Secret Manager (cached after first fetch).
    Works locally via: gcloud auth application-default login
    """
    if secret_id not in _secrets:
        name = f"projects/{config.CLOUD_PROJECT_ID}/secrets/{secret_id}/versions/latest"
        response = _get_secret_client().access_secret_version(request={"name": name})
        _secrets[secret_id] = response.payload.data.decode("UTF-8")
    return _secrets[secret_id]


# ─── PCP API ──────────────────────────────────────────────────────────────────

def fetch_person_from_pcp(person_id: str) -> dict | None:
    """
    Fetch full person record from PCP API including emails and custom field data.

    Uses HTTP Basic Auth with PCP Personal Access Token credentials.
    Returns the raw PCP API response dict, or None on error.

    NOTE: If the webhook payload format from PCP changes, update _extract_person_id()
    below. The PCP API response format here is stable (JSON:API standard).
    """
    url    = f"{config.PCP_API_BASE}/people/{person_id}"
    params = {"include": "emails,field_data"}
    auth   = (get_secret("PCP_APP_ID"), get_secret("PCP_SECRET"))

    try:
        resp = requests.get(url, params=params, auth=auth, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if config.LOG_PAYLOADS:
            logger.debug(f"PCP API response for person_id={person_id}: {data}")
        return data
    except requests.RequestException as e:
        logger.error(f"PCP API fetch failed for person_id={person_id}: {e}")
        if hasattr(e, "response") and e.response is not None:
            logger.error(f"PCP API error body: {e.response.text}")
        return None


def _extract_person_id(webhook_payload: dict) -> str:
    """
    Extract the person ID from a PCP webhook payload.

    Expected PCP webhook format:
        {
          "name": "person.created",
          "payload": {
            "data": {
              "type": "Person",
              "id": "12345678",
              ...
            }
          }
        }

    NOTE: If PCP sends a different structure, update this function.
    Set LOG_PAYLOADS=true to log the raw webhook and inspect it.
    """
    return (
        webhook_payload
        .get("payload", {})
        .get("data", {})
        .get("id", "")
    )


def parse_person(pcp_api_response: dict) -> dict:
    """
    Extract relevant fields from a PCP API person response.
    The response must have been fetched with ?include=emails,field_data.

    Returns a flat dict:
        person_id    (str)
        first_name   (str, title-cased)
        last_name    (str, title-cased)
        email        (str, lowercase, empty string if none)
        custom_fields (dict mapping field_definition_id → value)
    """
    data     = pcp_api_response.get("data", {})
    attrs    = data.get("attributes", {})
    included = pcp_api_response.get("included", [])

    person_id  = data.get("id", "")
    first_name = attrs.get("first_name", "") or ""
    last_name  = attrs.get("last_name",  "") or ""

    # Find primary email from included Email resources
    email = ""
    for item in included:
        if item.get("type") != "Email":
            continue
        item_attrs = item.get("attributes", {})
        addr = item_attrs.get("address", "").strip()
        if not addr:
            continue
        # Accept first found; prefer primary
        if not email or item_attrs.get("primary"):
            email = addr
        if item_attrs.get("primary"):
            break

    # Build custom_fields dict: field_definition_id → value
    # Keys are strings (PCP numeric IDs as strings)
    custom_fields: dict[str, str] = {}
    for item in included:
        if item.get("type") != "FieldDatum":
            continue
        field_def_id = (
            item.get("relationships", {})
                .get("field_definition", {})
                .get("data", {})
                .get("id", "")
        )
        value = item.get("attributes", {}).get("value", "") or ""
        if field_def_id:
            custom_fields[str(field_def_id)] = value

    return {
        "person_id":     person_id,
        "first_name":    first_name.strip().title(),
        "last_name":     last_name.strip().title(),
        "email":         email.lower(),
        "custom_fields": custom_fields,
    }


# ─── Rule matching ────────────────────────────────────────────────────────────

def apply_rules(person: dict) -> list[str]:
    """
    Walk CC_LIST_RULES (from config.py) and return the deduplicated union of
    all CC list UUIDs where this person should be added.

    A rule matches when:
      - config.PCP_FIELD_IDS has an ID for the rule's pcp_field, AND
      - person["custom_fields"][that_id] == rule["pcp_value"]
    """
    matched: set[str] = set()
    custom_fields = person.get("custom_fields", {})

    for rule in config.CC_LIST_RULES:
        field_name = rule["pcp_field"]
        field_id   = config.PCP_FIELD_IDS.get(field_name, "")

        if not field_id:
            logger.warning(f"Rule '{rule['description']}': PCP_FIELD_IDS['{field_name}'] not set — skipping")
            continue

        actual_value = custom_fields.get(str(field_id), "")
        if actual_value == rule["pcp_value"]:
            valid_list_ids = [lid for lid in rule["cc_lists"] if lid]
            matched.update(valid_list_ids)
            logger.info(f"Rule matched: '{rule['description']}' → {valid_list_ids}")
        else:
            logger.debug(f"Rule not matched: '{rule['description']}' (field_id={field_id}, got '{actual_value}', want '{rule['pcp_value']}')")

    return list(matched)


# ─── Constant Contact API ─────────────────────────────────────────────────────

def add_to_cc(person: dict, list_ids: list[str]) -> bool:
    """
    Create or update a contact in Constant Contact and add them to list_ids.

    Uses the CC v3 /contacts upsert endpoint — safe to call repeatedly,
    no duplicate contacts are created.
    Returns True on success, False on error.
    """
    url     = f"{config.CC_API_BASE}/contacts"
    headers = {
        "Authorization": f"Bearer {get_secret('CC_ACCESS_TOKEN')}",
        "Content-Type":  "application/json",
    }
    body = {
        "email_address": {
            "address":            person["email"],
            "permission_to_send": "implicit",
        },
        "first_name":       person["first_name"],
        "last_name":        person["last_name"],
        "list_memberships": list_ids,
    }

    try:
        resp = requests.post(url, json=body, headers=headers, timeout=10)
        resp.raise_for_status()
        logger.info(f"CC contact added/updated: email={person['email']}  lists={list_ids}")
        return True
    except requests.RequestException as e:
        logger.error(f"CC API call failed for email={person['email']}: {e}")
        if hasattr(e, "response") and e.response is not None:
            logger.error(f"CC API error body: {e.response.text}")
        return False


# ─── Flask routes ─────────────────────────────────────────────────────────────

@app.route("/webhook", methods=["POST"])
def webhook():
    payload = request.get_json(silent=True)

    if config.LOG_PAYLOADS:
        logger.debug(f"Incoming webhook payload: {payload}")

    # ── Validate ─────────────────────────────────────────────────────────────
    if not isinstance(payload, dict):
        logger.warning("Rejected: payload is not a JSON object")
        return jsonify({"error": "payload must be a JSON object"}), 400

    event_name = payload.get("name", "")
    if event_name != "person.created":
        logger.info(f"Ignored event: {event_name!r}")
        return jsonify({"status": "ignored", "event": event_name}), 200

    person_id = _extract_person_id(payload)
    if not person_id:
        logger.warning("Rejected: could not extract person_id from payload")
        return jsonify({"error": "missing person id in payload"}), 400

    logger.info(f"Processing person.created  person_id={person_id}")

    # ── Fetch full person from PCP ────────────────────────────────────────────
    pcp_data = fetch_person_from_pcp(person_id)
    if pcp_data is None:
        return jsonify({"error": "failed to fetch person from PCP API"}), 502

    person = parse_person(pcp_data)
    name_display = f"{person['first_name']} {person['last_name']}".strip() or f"person_id={person_id}"
    logger.info(f"Parsed: {name_display}  email={'(none)' if not person['email'] else '(set)'}")

    # ── Skip if no email ──────────────────────────────────────────────────────
    if not person["email"]:
        logger.info(f"Skipped {name_display}: no email address")
        return jsonify({"status": "skipped", "reason": "no email"}), 200

    # ── Apply rules ───────────────────────────────────────────────────────────
    list_ids = apply_rules(person)
    if not list_ids:
        logger.info(f"Skipped {name_display}: no rules matched")
        return jsonify({"status": "skipped", "reason": "no rules matched"}), 200

    # ── Add to Constant Contact ───────────────────────────────────────────────
    if config.TEST_MODE:
        logger.info(f"TEST_MODE=true — would add {person['email']} to CC lists {list_ids}")
        return jsonify({"status": "test_mode", "would_add_to_lists": list_ids}), 200

    success = add_to_cc(person, list_ids)
    if not success:
        return jsonify({"error": "failed to add contact to Constant Contact"}), 502

    return jsonify({"status": "ok", "email": person["email"], "lists": list_ids}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/settings", methods=["GET"])
def settings():
    """Echo current configuration — useful for diagnosing Cloud Run env var issues."""
    rules_summary = [
        {
            "description": r["description"],
            "pcp_field":   r["pcp_field"],
            "pcp_value":   r["pcp_value"],
            "cc_lists":    r["cc_lists"],
            "field_id":    config.PCP_FIELD_IDS.get(r["pcp_field"], "(not set)"),
        }
        for r in config.CC_LIST_RULES
    ]
    return jsonify({
        "TEST_MODE":        config.TEST_MODE,
        "LOG_PAYLOADS":     config.LOG_PAYLOADS,
        "CLOUD_PROJECT_ID": config.CLOUD_PROJECT_ID,
        "PCP_API_BASE":     config.PCP_API_BASE,
        "CC_API_BASE":      config.CC_API_BASE,
        "CC_LIST_RULES":    rules_summary,
    }), 200


# ─── Dev server ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=config.PORT, debug=True)
