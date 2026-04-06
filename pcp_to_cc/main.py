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

import json
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


def update_secret(secret_id: str, value: str) -> None:
    """
    Write a new version of a secret to GCP Secret Manager and update the local cache.
    Used to store refreshed CC access tokens so they survive across Cloud Run instances.
    """
    parent  = f"projects/{config.CLOUD_PROJECT_ID}/secrets/{secret_id}"
    payload = secretmanager.SecretPayload(data=value.encode("UTF-8"))
    _get_secret_client().add_secret_version(request={"parent": parent, "payload": payload})
    _secrets[secret_id] = value  # keep cache in sync
    logger.debug(f"Secret '{secret_id}' updated in Secret Manager")


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
        resp = requests.get(
            url, params=params, auth=auth, timeout=10,
            headers={"User-Agent": "pcp_to_cc (office2@4thu.org)"},
        )
        resp.raise_for_status()
        data = resp.json()
        if config.LOG_PAYLOADS:
            logger.info(f"PCP API response for person_id={person_id}: {data}")
        return data
    except requests.RequestException as e:
        logger.error(f"PCP API fetch failed for person_id={person_id}: {e}")
        if hasattr(e, "response") and e.response is not None:
            logger.error(f"PCP API error body: {e.response.text}")
        return None


def _extract_person_id(webhook_payload: dict) -> str:
    """
    Extract the person ID from a PCP webhook payload.

    Actual PCP webhook format:
        {
          "data": [{
            "type": "EventDelivery",
            "attributes": {
              "name": "people.v2.events.person.created",
              "payload": "{\"data\":{\"type\":\"Person\",\"id\":\"12345678\",...}}"
            }
          }]
        }

    The inner payload is a JSON string that must be parsed separately.
    """
    try:
        inner_str = (
            webhook_payload
            .get("data", [{}])[0]
            .get("attributes", {})
            .get("payload", "{}")
        )
        return json.loads(inner_str).get("data", {}).get("id", "")
    except (json.JSONDecodeError, IndexError):
        return ""


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
            logger.info(f"Rule not matched: '{rule['description']}' (field_id={field_id}, got '{actual_value}', want '{rule['pcp_value']}')")

    return list(matched)


# ─── Constant Contact API ─────────────────────────────────────────────────────

_CC_TOKEN_URL = "https://authz.constantcontact.com/oauth2/default/v1/token"


def _refresh_cc_token() -> bool:
    """
    Exchange CC_REFRESH_TOKEN for a new CC_ACCESS_TOKEN.

    CC access tokens expire (~24 hrs). This is called automatically by add_to_cc()
    on a 401 response. The new access token is written back to GCP Secret Manager
    so it persists across Cloud Run instances.

    Requires secrets: CC_API_KEY, CC_REFRESH_TOKEN (in Secret Manager).
    CC_API_SECRET is optional — CC does not always issue one.
    Long Lived Refresh Tokens are used so CC_REFRESH_TOKEN never needs updating.

    Returns True on success, False on error.
    """
    try:
        # CC_API_SECRET is optional — use empty string if not present
        try:
            cc_api_secret = get_secret("CC_API_SECRET")
        except Exception:
            cc_api_secret = ""

        resp = requests.post(
            _CC_TOKEN_URL,
            auth=(get_secret("CC_API_KEY"), cc_api_secret),
            data={"grant_type": "refresh_token", "refresh_token": get_secret("CC_REFRESH_TOKEN")},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10,
        )
        resp.raise_for_status()
        new_token = resp.json().get("access_token", "")
        if not new_token:
            logger.error("CC token refresh: response missing access_token")
            return False
        update_secret("CC_ACCESS_TOKEN", new_token)
        logger.info("CC access token refreshed and stored in Secret Manager")
        return True
    except requests.RequestException as e:
        logger.error(f"CC token refresh failed: {e}")
        if hasattr(e, "response") and e.response is not None:
            logger.error(f"CC token refresh response: {e.response.text}")
        return False


def _cc_headers() -> dict:
    return {
        "Authorization": f"Bearer {get_secret('CC_ACCESS_TOKEN')}",
        "Content-Type":  "application/json",
    }


def _cc_create(person: dict, list_ids: list[str]) -> requests.Response:
    """POST /v3/contacts — creates a new contact. Returns the response."""
    body = {
        "email_address": {
            "address":            person["email"],
            "permission_to_send": "implicit",
        },
        "first_name":       person["first_name"],
        "last_name":        person["last_name"],
        "create_source":    "Account",
        "list_memberships": list_ids,
    }
    return requests.post(
        f"{config.CC_API_BASE}/contacts",
        json=body, headers=_cc_headers(), timeout=10,
    )


def _cc_update(contact_id: str, person: dict, list_ids: list[str]) -> requests.Response:
    """PUT /v3/contacts/{id} — updates an existing contact. Returns the response."""
    body = {
        "email_address": {
            "address":            person["email"],
            "permission_to_send": "implicit",
        },
        "first_name":       person["first_name"],
        "last_name":        person["last_name"],
        "update_source":    "Account",
        "list_memberships": list_ids,
    }
    return requests.put(
        f"{config.CC_API_BASE}/contacts/{contact_id}",
        json=body, headers=_cc_headers(), timeout=10,
    )


def _extract_contact_id_from_conflict(resp: requests.Response) -> str:
    """
    CC returns 409 when a contact already exists, with the existing contact_id
    embedded in the error_message string. Extract and return it, or "" if not found.

    Example error_message:
        "Email already exists for contact 5cf018e4-302e-11f1-84b4-0242841d1f0f"
    """
    try:
        errors = resp.json()
        for error in errors:
            msg = error.get("error_message", "")
            if "already exists for contact" in msg:
                return msg.split("already exists for contact")[-1].strip()
    except Exception:
        pass
    return ""


def add_to_cc(person: dict, list_ids: list[str]) -> bool:
    """
    Create or update a contact in Constant Contact and add them to list_ids.

    Flow:
      1. POST /v3/contacts to create the contact.
      2. If 409 (already exists), extract contact_id from error and PUT to update.
      3. On 401 (expired token), refresh once and retry from step 1.

    Returns True on success, False on error.
    """
    for attempt in range(2):
        try:
            resp = _cc_create(person, list_ids)

            # ── Token expired — refresh and retry ─────────────────────────────
            if resp.status_code == 401 and attempt == 0:
                logger.warning("CC API returned 401 — access token expired, refreshing")
                if not _refresh_cc_token():
                    return False
                continue

            # ── Contact already exists — update instead ───────────────────────
            if resp.status_code == 409:
                contact_id = _extract_contact_id_from_conflict(resp)
                if not contact_id:
                    logger.error(f"CC 409 conflict but could not extract contact_id: {resp.text}")
                    return False
                logger.info(f"CC contact exists ({contact_id}) — updating")
                resp = _cc_update(contact_id, person, list_ids)

            resp.raise_for_status()
            logger.info(f"CC contact added/updated: email={person['email']}  lists={list_ids}")
            return True

        except requests.RequestException as e:
            logger.error(f"CC API call failed for email={person['email']}: {e}")
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"CC API error body: {e.response.text}")
            return False

    return False


# ─── Flask routes ─────────────────────────────────────────────────────────────

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    # PCP sends a GET to verify the endpoint is reachable when you first subscribe
    if request.method == "GET":
        logger.info("Webhook GET verification request received")
        return jsonify({"status": "ok"}), 200

    payload = request.get_json(silent=True)

    if config.LOG_PAYLOADS:
        logger.info(f"Incoming webhook payload: {payload}")

    # ── Validate ─────────────────────────────────────────────────────────────
    if not isinstance(payload, dict):
        logger.warning("Rejected: payload is not a JSON object")
        return jsonify({"error": "payload must be a JSON object"}), 400

    try:
        event_name = payload["data"][0]["attributes"]["name"]
    except (KeyError, IndexError, TypeError):
        event_name = ""
    if not event_name:
        logger.warning("Rejected: payload missing event name")
        return jsonify({"error": "payload missing event name"}), 400

    if event_name != "people.v2.events.person.created":
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
