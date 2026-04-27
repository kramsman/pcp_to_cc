"""
PCP → Constant Contact webhook receiver.

Receives person.created and person.updated webhooks from Planning Center People,
fetches the full person record from the PCP API (including email
and custom field data), then adds matching profiles to Constant
Contact lists based on CC_LIST_RULES in config.py.

Usage (local dev):
    python pcp_to_cc/main.py          # start Flask dev server
    python test_local.py              # send a test webhook in another terminal
"""

import json
import os
from datetime import datetime
from typing import Annotated, Any, Literal, Optional, Union

import requests
from flask import Flask, jsonify, request
from google.cloud import secretmanager
from loguru import logger
from pydantic import BaseModel, Field, ValidationError, model_validator

import config

# ═══════════════════════════════════════════════════════════════════════════
# PYDANTIC MODELS
# ─────────────────────────────────────────────────────────────────────────
# Two independent model trees:
#   1. Incoming webhook  → parse_webhook_payload()  replaces _extract_person_id()
#   2. PCP API response  → PcpPersonResponse        replaces parse_person()
# ═══════════════════════════════════════════════════════════════════════════

# ── Shared ─────────────────────────────────────────────────────────────────

class TypedRef(BaseModel):
    type: str
    id: str

class RelRef(BaseModel):
    data: Optional[TypedRef] = None


# ── 1. Incoming webhook (LegacyWebhookEvent) ──────────────────────────────
# PCP sends:  { "data": [ { "type": "EventDelivery",
#                           "attributes": { "name": "...", "payload": "<json str>" } } ] }
# The inner "payload" field is a raw JSON string — model_validator decodes it.

class PersonAttrs(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    created_at: Optional[datetime] = None

class Person(BaseModel):
    type: Literal["Person"]
    id: str
    attributes: PersonAttrs

class WorkflowCardRels(BaseModel):
    person:   RelRef = RelRef()
    workflow: RelRef = RelRef()

class WorkflowCardAttrs(BaseModel):
    stage: Optional[str] = None

class WorkflowCard(BaseModel):
    type: Literal["WorkflowCard"]
    id: str
    attributes: WorkflowCardAttrs = WorkflowCardAttrs()
    relationships: WorkflowCardRels = WorkflowCardRels()

class WorkflowCardActivityAttrs(BaseModel):
    comment:     Optional[str] = None
    type:        Optional[str] = None
    person_name: Optional[str] = None

class WorkflowCardActivityRels(BaseModel):
    workflow_card: RelRef = RelRef()
    workflow_step: RelRef = RelRef()

class WorkflowCardActivity(BaseModel):
    type: Literal["WorkflowCardActivity"]
    id: str
    attributes:    WorkflowCardActivityAttrs = WorkflowCardActivityAttrs()
    relationships: WorkflowCardActivityRels  = WorkflowCardActivityRels()

class FormSubmissionAttrs(BaseModel):
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    requires_verification: bool = False
    verified: bool = False

class FormSubmissionRels(BaseModel):
    person: RelRef = RelRef()
    form:   RelRef = RelRef()

class FormSubmission(BaseModel):
    type: Literal["FormSubmission"]
    id: str
    attributes:    FormSubmissionAttrs = FormSubmissionAttrs()
    relationships: FormSubmissionRels  = FormSubmissionRels()

class Unknown(BaseModel):
    model_config = {"extra": "allow"}
    type: str
    id: str = ""

# No discriminator — falls back to Unknown for any type PCP adds in future
InnerData = Union[Person, WorkflowCard, WorkflowCardActivity, FormSubmission, Unknown]

class InnerPayload(BaseModel):
    data: InnerData

class WebhookDeliveryAttrs(BaseModel):
    name: str
    attempt: int = 1
    payload: InnerPayload

    @model_validator(mode="before")
    @classmethod
    def _decode_payload_string(cls, v):
        if isinstance(v.get("payload"), str):
            v["payload"] = json.loads(v["payload"])
        return v

class WebhookDelivery(BaseModel):
    type: Literal["EventDelivery"]
    id: str = ""
    attributes: WebhookDeliveryAttrs

# Format: { "data": [EventDelivery] }  — person.created, legacy format
class LegacyWebhookEvent(BaseModel):
    data: list[WebhookDelivery]

    @property
    def delivery(self) -> WebhookDelivery:
        return self.data[0]

    @property
    def event_name(self) -> str:
        return self.delivery.attributes.name

    @property
    def person_id(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, WorkflowCard):
            return inner.relationships.person.data.id if inner.relationships.person.data else ""
        if isinstance(inner, FormSubmission):
            return inner.relationships.person.data.id if inner.relationships.person.data else ""
        return inner.id

    @property
    def submission_id(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, FormSubmission):
            return inner.id
        return ""

    @property
    def form_id(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, FormSubmission):
            return inner.relationships.form.data.id if inner.relationships.form.data else ""
        return ""

    @property
    def workflow_id(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, WorkflowCard):
            return inner.relationships.workflow.data.id if inner.relationships.workflow.data else ""
        return ""

    @property
    def stage(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, WorkflowCard):
            return inner.attributes.stage or ""
        return ""

    @property
    def comment(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, WorkflowCardActivity):
            return inner.attributes.comment or ""
        return ""

    @property
    def activity_type(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, WorkflowCardActivity):
            return inner.attributes.type or ""
        return ""

    @property
    def person_name(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, WorkflowCardActivity):
            return inner.attributes.person_name or ""
        return ""

    @property
    def card_id(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, WorkflowCardActivity):
            return inner.relationships.workflow_card.data.id if inner.relationships.workflow_card.data else ""
        return ""

# Format: { "event": "...", "payload": { "data": [EventDelivery] } }  — workflow events
class PcpWebhookPayload(BaseModel):
    data: list[WebhookDelivery]

class PcpWebhookEvent(BaseModel):
    event: str
    payload: PcpWebhookPayload

    @property
    def delivery(self) -> WebhookDelivery:
        return self.payload.data[0]

    @property
    def event_name(self) -> str:
        return self.event

    @property
    def person_id(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, WorkflowCard):
            return inner.relationships.person.data.id if inner.relationships.person.data else ""
        if isinstance(inner, FormSubmission):
            return inner.relationships.person.data.id if inner.relationships.person.data else ""
        return inner.id

    @property
    def submission_id(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, FormSubmission):
            return inner.id
        return ""

    @property
    def form_id(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, FormSubmission):
            return inner.relationships.form.data.id if inner.relationships.form.data else ""
        return ""

    @property
    def workflow_id(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, WorkflowCard):
            return inner.relationships.workflow.data.id if inner.relationships.workflow.data else ""
        return ""

    @property
    def stage(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, WorkflowCard):
            return inner.attributes.stage or ""
        return ""

    @property
    def comment(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, WorkflowCardActivity):
            return inner.attributes.comment or ""
        return ""

    @property
    def activity_type(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, WorkflowCardActivity):
            return inner.attributes.type or ""
        return ""

    @property
    def person_name(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, WorkflowCardActivity):
            return inner.attributes.person_name or ""
        return ""

    @property
    def card_id(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, WorkflowCardActivity):
            return inner.relationships.workflow_card.data.id if inner.relationships.workflow_card.data else ""
        return ""


def parse_webhook_payload(raw: dict) -> LegacyWebhookEvent | PcpWebhookEvent:
    """Parse incoming PCP webhook dict. Raises ValidationError if malformed."""
    if "event" in raw:
        return PcpWebhookEvent.model_validate(raw)
    return LegacyWebhookEvent.model_validate(raw)


# ── 3. Direct REST poll response (workflow_complete) ──────────────────────
# Returned by GET /people/v2/workflows/{id}/cards/{id}
# Structure: { "data": {WorkflowCard}, "included": [], "meta": {} }

class ApiWorkflowCardAttrs(BaseModel):
    stage: str
    completed_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    overdue: bool
    removed_at: Optional[datetime] = None
    snooze_until: Optional[datetime] = None

class ApiWorkflowCardRels(BaseModel):
    assignee: RelRef
    person: RelRef
    workflow: RelRef
    current_step: RelRef

class ApiWorkflowCard(BaseModel):
    type: Literal["WorkflowCard"]
    id: str
    attributes: ApiWorkflowCardAttrs
    relationships: ApiWorkflowCardRels

class WorkflowCompleteResponse(BaseModel):
    data: ApiWorkflowCard
    included: list[Any] = []


# ── FormSubmissionValues REST poll response ────────────────────────────────
# Returned by GET /forms/{form_id}/form_submissions/{submission_id}/form_submission_values

class FormSubmissionValueAttrs(BaseModel):
    display_value: Optional[str] = None
    attachments:   list[Any]     = []

class FormSubmissionValueRels(BaseModel):
    form_field:       RelRef = RelRef()
    form_field_option: RelRef = RelRef()
    form_submission:  RelRef = RelRef()

class FormSubmissionValue(BaseModel):
    type:          Literal["FormSubmissionValue"]
    id:            str
    attributes:    FormSubmissionValueAttrs = FormSubmissionValueAttrs()
    relationships: FormSubmissionValueRels  = FormSubmissionValueRels()

class FormSubmissionValuesResponse(BaseModel):
    data: list[FormSubmissionValue] = []

    def to_field_map(self) -> dict[str, str]:
        """Returns {form_field_id: display_value} for all submission values."""
        return {
            fsv.relationships.form_field.data.id: (fsv.attributes.display_value or "")
            for fsv in self.data
            if fsv.relationships.form_field.data
        }


def parse_any_pcp_payload(raw: dict) -> tuple[str, BaseModel]:
    """Try all known PCP payload formats. Returns (format_name, parsed_model)."""
    if isinstance(raw.get("data"), list):
        return "LegacyWebhookEvent", LegacyWebhookEvent.model_validate(raw)
    if raw.get("data", {}).get("type") == "WorkflowCard":
        return "WorkflowCompleteResponse", WorkflowCompleteResponse.model_validate(raw)
    raise ValidationError.from_exception_data(
        title="parse_any_pcp_payload",
        input_type="python",
        line_errors=[],
    )


# ── 2. PCP API person response ─────────────────────────────────────────────
# Fetched via GET /people/v2/people/{id}?include=emails,field_data
# Structure: { "data": {Person}, "included": [Email, ..., FieldDatum, ...] }

class PcpPersonAttrs(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None

class PcpPersonData(BaseModel):
    type: Literal["Person"]
    id: str
    attributes: PcpPersonAttrs

class PcpEmailAttrs(BaseModel):
    address: Optional[str] = None
    primary: bool = False

class PcpEmail(BaseModel):
    type: Literal["Email"]
    attributes: PcpEmailAttrs

class PcpFieldDatumAttrs(BaseModel):
    value: Optional[str] = None

class PcpFieldDatumRels(BaseModel):
    field_definition: RelRef = RelRef()

class PcpFieldDatum(BaseModel):
    type: Literal["FieldDatum"]
    attributes: PcpFieldDatumAttrs
    relationships: PcpFieldDatumRels = PcpFieldDatumRels()

class PcpUnknownIncluded(BaseModel):
    type: str
    model_config = {"extra": "allow"}

PcpIncluded = Annotated[
    Union[PcpEmail, PcpFieldDatum, PcpUnknownIncluded],
    Field(discriminator="type"),
]

class PcpPersonResponse(BaseModel):
    data: PcpPersonData
    included: list[Any] = []

    def to_person_dict(self) -> dict:
        attrs = self.data.attributes
        first_name = (attrs.first_name or "").strip().title()
        last_name  = (attrs.last_name  or "").strip().title()

        email = ""
        custom_fields: dict[str, list[str]] = {}

        for item in self.included:
            item_type = item.get("type") if isinstance(item, dict) else getattr(item, "type", "")

            if item_type == "Email":
                item_attrs = item.get("attributes", {}) if isinstance(item, dict) else {}
                addr = (item_attrs.get("address") or "").strip()
                if addr:
                    if not email or item_attrs.get("primary"):
                        email = addr
                    if item_attrs.get("primary"):
                        continue

            elif item_type == "FieldDatum":
                item_attrs = item.get("attributes", {}) if isinstance(item, dict) else {}
                rels       = item.get("relationships", {}) if isinstance(item, dict) else {}
                field_id   = (
                    rels.get("field_definition", {})
                        .get("data", {})
                        .get("id", "")
                )
                value = (item_attrs.get("value") or "")
                if field_id:
                    custom_fields.setdefault(str(field_id), []).append(value)

        return {
            "person_id":     self.data.id,
            "first_name":    first_name,
            "last_name":     last_name,
            "email":         email.lower(),
            "custom_fields": custom_fields,
        }

app = Flask(__name__)

_payloads: list[dict] = []
_MAX_PAYLOADS = 20


def _log_json(severity: str, message: str, **fields) -> None:
    """Write a single-line structured JSON entry to stdout for Cloud Logging."""
    import sys
    entry = {"severity": severity, "message": message, **fields}
    print(json.dumps(entry, default=str), flush=True, file=sys.stdout)

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
        try:
            response = _get_secret_client().access_secret_version(request={"name": name})
        except Exception as e:
            if not any(k in str(e).lower() for k in ("reauth", "expired", "unavailable", "credentials")):
                raise
            _ensure_adc_auth()
            global _secret_client
            _secret_client = None
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
            _log_json("INFO", "PCP API person response", person_id=person_id, data=data)
        return data
    except requests.RequestException as e:
        logger.error(f"PCP API fetch failed for person_id={person_id}: {e}")
        if hasattr(e, "response") and e.response is not None:
            logger.error(f"PCP API error body: {e.response.text}")
        return None


def parse_person(pcp_api_response: dict) -> dict:
    """
    Extract relevant fields from a PCP API person response using Pydantic.
    The response must have been fetched with ?include=emails,field_data.

    Returns a flat dict:
        person_id, first_name, last_name, email, custom_fields
    """
    try:
        return PcpPersonResponse.model_validate(pcp_api_response).to_person_dict()
    except ValidationError:
        return {"person_id": "", "first_name": "", "last_name": "", "email": "", "custom_fields": {}}


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
        field_id      = rule["pcp_field_id"]
        actual_values = custom_fields.get(str(field_id), [])
        pcp_value     = rule["pcp_value"]
        if pcp_value and any(pcp_value in v for v in actual_values):
            valid_list_ids = [lid for lid in rule["cc_lists"] if lid]
            matched.update(valid_list_ids)
            logger.info(f"Rule matched: '{rule['description']}' → {valid_list_ids}")
        else:
            logger.info(f"Rule not matched: '{rule['description']}' (field_id={field_id}, got {actual_values}, want '{pcp_value}')")

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


# ─── PCP custom field writer ──────────────────────────────────────────────────

def set_custom_field(person_id: str, field_def_id: str, value: str) -> bool:
    """
    Write a custom field value to a PCP person record (POST if new, PATCH if exists).
    field_def_id is the numeric PCP field definition ID.
    Returns True on success, False on error.
    """
    if not field_def_id:
        logger.warning(f"set_custom_field: field_def_id is empty — skipping")
        return False

    if config.TEST_MODE:
        logger.info(f"TEST_MODE — would set field id={field_def_id} = '{value}' on person {person_id}")
        return True

    auth = (get_secret("PCP_APP_ID"), get_secret("PCP_SECRET"))
    base = config.PCP_API_BASE

    try:
        # Check for existing FieldDatum for this person + field
        r = requests.get(f"{base}/people/{person_id}/field_data", auth=auth, timeout=10)
        r.raise_for_status()
        existing = next(
            (fd for fd in r.json().get("data", [])
             if fd.get("relationships", {}).get("field_definition", {}).get("data", {}).get("id") == str(field_def_id)),
            None,
        )

        payload = {"data": {
            "type": "FieldDatum",
            "attributes": {"value": value},
            "relationships": {"field_definition": {"data": {"type": "FieldDefinition", "id": str(field_def_id)}}},
        }}

        if existing:
            logger.info(f"set_custom_field: PATCH existing FieldDatum {existing['id']} → '{value}'")
            r = requests.patch(f"{base}/field_data/{existing['id']}", json=payload, auth=auth, timeout=10)
        else:
            logger.info(f"set_custom_field: POST new FieldDatum for person {person_id} field_def {field_def_id} → '{value}'")
            r = requests.post(f"{base}/people/{person_id}/field_data", json=payload, auth=auth, timeout=10)

        r.raise_for_status()
        logger.info(f"set_custom_field: success — field_id={field_def_id} = '{value}' on person {person_id}  HTTP {r.status_code}")
        return True

    except requests.RequestException as e:
        logger.error(f"set_custom_field failed for person {person_id}: {e}")
        if hasattr(e, "response") and e.response is not None:
            logger.error(f"PCP API error body: {e.response.text}")
        return False


def add_to_workflow(person_id: str, workflow_id: str) -> bool:
    if config.TEST_MODE:
        logger.info(f"TEST_MODE — would add person {person_id} to workflow {workflow_id}")
        return True
    try:
        auth = (get_secret("PCP_APP_ID"), get_secret("PCP_SECRET"))
        url  = f"{config.PCP_API_BASE}/workflows/{workflow_id}/cards"
        body = {"data": {"type": "WorkflowCard",
                         "relationships": {"person": {"data": {"type": "Person", "id": person_id}}}}}
        r = requests.post(url, json=body, auth=auth, timeout=10)
        r.raise_for_status()
        logger.info(f"add_to_workflow: added person {person_id} to workflow {workflow_id}  HTTP {r.status_code}")
        return True
    except requests.RequestException as e:
        logger.error(f"add_to_workflow failed person {person_id} workflow {workflow_id}: {e}")
        if hasattr(e, "response") and e.response is not None:
            logger.error(f"PCP API error body: {e.response.text}")
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
        _log_json("INFO", "webhook_payload", body=json.dumps(payload, default=str))

    # ── Validate & parse with Pydantic ───────────────────────────────────────
    if not isinstance(payload, dict):
        raw = request.get_data(as_text=True)
        logger.warning(f"Rejected: payload is not a JSON object\nRaw body:\n{raw}")
        return jsonify({"error": "payload must be a JSON object"}), 400

    try:
        event = parse_webhook_payload(payload)
    except ValidationError as exc:
        logger.warning(f"Rejected: payload failed Pydantic validation\n{exc}")
        return jsonify({"error": "payload missing event name"}), 400

    event_name = event.event_name
    global _payloads
    _payloads = ([{"event": event_name, "payload": payload}] + _payloads)[:_MAX_PAYLOADS]

    # ── Workflow card events — set custom fields ──────────────────────────────
    _WORKFLOW_CARD_EVENTS = {
        "people.v2.events.workflow_card.created",
        "people.v2.events.workflow_card.updated",
    }
    if event_name in _WORKFLOW_CARD_EVENTS:
        trigger     = "completed" if event.stage == "completed" else "entered"
        person_id   = event.person_id
        workflow_id = event.workflow_id
        if not person_id:
            logger.warning(f"Rejected: {event_name} missing person_id")
            return jsonify({"error": "missing person_id in workflow payload"}), 400

        logger.info(f"Processing {event_name}  trigger={trigger}  person_id={person_id}  workflow_id={workflow_id}")
        matched = False
        for rule in config.WORKFLOW_FIELD_RULES:
            if rule["workflow_id"] and rule["workflow_id"] != workflow_id:
                continue
            if rule["trigger"] != trigger:
                continue
            matched = True
            set_custom_field(person_id, rule["field_id"], rule["value"])
            logger.info(f"Workflow field rule applied: '{rule['description']}'")

        for rule in config.WORKFLOW_CHAIN_RULES:
            if rule["workflow_id"] and rule["workflow_id"] != workflow_id:
                continue
            if rule["trigger"] != trigger:
                continue
            matched = True
            add_to_workflow(person_id, rule["add_to_workflow_id"])
            logger.info(f"Workflow chain rule applied: '{rule['description']}'")

        if not matched:
            logger.info(f"No workflow rules matched workflow_id={workflow_id} trigger={trigger}")
        return jsonify({"status": "ok", "event": event_name, "person_id": person_id}), 200

    _PERSON_EVENTS = {
        "people.v2.events.person.created",
        "people.v2.events.person.updated",
    }
    if event_name not in _PERSON_EVENTS:
        extras = {k: v for k, v in {
            "activity_type": event.activity_type,
            "person_name":   event.person_name,
            "card_id":       event.card_id,
            "comment":       event.comment,
            "workflow_id":   event.workflow_id,
        }.items() if v}
        _log_json("INFO", f"Ignored event: {event_name}", event=event_name, **extras)
        return jsonify({"status": "ignored", "event": event_name}), 200

    person_id = event.person_id
    if not person_id:
        logger.warning("Rejected: could not extract person_id from payload")
        return jsonify({"error": "missing person id in payload"}), 400

    logger.info(f"Processing {event_name}  person_id={person_id}")

    # ── Fetch full person from PCP ────────────────────────────────────────────
    pcp_data = fetch_person_from_pcp(person_id)
    if pcp_data is None:
        return jsonify({"error": "failed to fetch person from PCP API"}), 502
    person = parse_person(pcp_data)
    if config.TEST_MODE:
        logger.info(f"TEST_MODE — fetched real PCP data, CC update will be skipped")
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
            "description":  r["description"],
            "pcp_field_id": r["pcp_field_id"],
            "pcp_value":    r["pcp_value"],
            "cc_lists":     r["cc_lists"],
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


@app.route("/payload", methods=["GET"])
def last_payload():
    """Return captured unrecognized webhook payloads (newest first, max 20)."""
    if not _payloads:
        return jsonify({"status": "none", "count": 0, "payloads": []}), 200
    return jsonify({"count": len(_payloads), "payloads": _payloads}), 200


@app.route("/payload/<int:index>", methods=["GET"])
def get_payload(index: int):
    """Return raw webhook body at position index (0=newest). Ready to paste into Postman."""
    if index >= len(_payloads):
        return jsonify({"error": "index out of range", "count": len(_payloads)}), 404
    return jsonify(_payloads[index]["payload"]), 200


@app.route("/payload/clear", methods=["POST"])
def clear_payloads():
    """Clear captured payloads."""
    global _payloads
    _payloads = []
    return jsonify({"status": "cleared"}), 200


@app.route("/parse", methods=["POST"])
def parse_debug():
    """TEST_MODE only — parse any known PCP payload and return the Pydantic model_dump.
    Use from Postman to verify payload parsing without triggering the full webhook flow."""
    if not config.TEST_MODE:
        return jsonify({"error": "only available in TEST_MODE"}), 403

    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"error": "payload must be a JSON object"}), 400

    try:
        fmt, parsed = parse_any_pcp_payload(payload)
        result = {"status": "ok", "format": fmt, "parsed": parsed.model_dump()}
        if isinstance(parsed, LegacyWebhookEvent):
            result["event_name"] = parsed.event_name
            result["person_id"] = parsed.person_id
        elif isinstance(parsed, WorkflowCompleteResponse):
            result["card_id"] = parsed.data.id
            result["stage"] = parsed.data.attributes.stage
            result["person_id"] = parsed.data.relationships.person.data.id if parsed.data.relationships.person.data else None
        return jsonify(result), 200
    except (ValidationError, Exception) as exc:
        errors = exc.errors() if isinstance(exc, ValidationError) else [{"msg": str(exc)}]
        return jsonify({"status": "invalid", "errors": errors}), 422


# ─── Dev server ───────────────────────────────────────────────────────────────

def _ensure_adc_auth() -> None:
    """Check ADC credentials locally and trigger browser re-auth if expired."""
    import subprocess
    import google.auth
    import google.auth.transport.requests
    try:
        creds, _ = google.auth.default()
        creds.refresh(google.auth.transport.requests.Request())
    except Exception as e:
        if not any(k in str(e).lower() for k in ("reauth", "expired", "invalid_grant", "could not be found", "credentials")):
            return
        print("\nGoogle credentials have expired. A browser window will open — sign in to continue.\n")
        subprocess.run(["gcloud", "auth", "application-default", "login"], check=True)


if __name__ == "__main__":
    if not config.TEST_MODE:
        _ensure_adc_auth()
    app.run(host="0.0.0.0", port=config.PORT, debug=True)
