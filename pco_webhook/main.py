"""
PCP → Constant Contact webhook receiver.

Receives person.created and person.updated webhooks from Planning Center People,
fetches the full person record from the PCP API (including email
and custom field data), then adds matching profiles to Constant
Contact lists based on CC_LIST_RULES in config.py.

Usage (local dev):
    python pco_webhook/main.py          # start Flask dev server
    python test_local.py              # send a test webhook in another terminal

If you get an error "Gitupdater not found:
  1. go into terminal
  2. copy and enter: source .venv/bin/activate
  3. then enter: uv pip install git+https://github.com/kramsman/gitupdater.git
"""

import hmac
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from html import escape as html_escape
from typing import Annotated, Any, Literal, Optional, Union

import requests
from flask import Flask, Response, jsonify, request
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
    person:       RelRef = RelRef()
    workflow:     RelRef = RelRef()
    current_step: RelRef = RelRef()  # null once the card is completed

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

class FieldDatumAttrs(BaseModel):
    value: Optional[str] = None

class FieldDatumRels(BaseModel):
    field_definition: RelRef = RelRef()
    customizable:     RelRef = RelRef()  # the Person the datum belongs to

class FieldDatum(BaseModel):
    type: Literal["FieldDatum"]
    id: str
    attributes:    FieldDatumAttrs = FieldDatumAttrs()
    relationships: FieldDatumRels  = FieldDatumRels()

class Unknown(BaseModel):
    model_config = {"extra": "allow"}
    type: str
    id: str = ""

# No discriminator — falls back to Unknown for any type PCP adds in future
InnerData = Union[Person, WorkflowCard, WorkflowCardActivity, FormSubmission, FieldDatum, Unknown]

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
        if isinstance(inner, FieldDatum):
            return inner.relationships.customizable.data.id if inner.relationships.customizable.data else ""
        return inner.id

    @property
    def field_definition_id(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, FieldDatum):
            return inner.relationships.field_definition.data.id if inner.relationships.field_definition.data else ""
        return ""

    @property
    def field_value(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, FieldDatum):
            return inner.attributes.value or ""
        return ""

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
        if isinstance(inner, WorkflowCard):
            return inner.id
        return ""

    @property
    def current_step_id(self) -> str:
        """Step the card now sits on. Empty once the card is completed or removed."""
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, WorkflowCard):
            return inner.relationships.current_step.data.id if inner.relationships.current_step.data else ""
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
        if isinstance(inner, FieldDatum):
            return inner.relationships.customizable.data.id if inner.relationships.customizable.data else ""
        return inner.id

    @property
    def field_definition_id(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, FieldDatum):
            return inner.relationships.field_definition.data.id if inner.relationships.field_definition.data else ""
        return ""

    @property
    def field_value(self) -> str:
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, FieldDatum):
            return inner.attributes.value or ""
        return ""

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
        if isinstance(inner, WorkflowCard):
            return inner.id
        return ""

    @property
    def current_step_id(self) -> str:
        """Step the card now sits on. Empty once the card is completed or removed."""
        inner = self.delivery.attributes.payload.data
        if isinstance(inner, WorkflowCard):
            return inner.relationships.current_step.data.id if inner.relationships.current_step.data else ""
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
    try:
        _get_secret_client().add_secret_version(request={"parent": parent, "payload": payload})
        logger.debug(f"Secret '{secret_id}' updated in Secret Manager")
    except Exception as e:
        # Gracefully degrade: log the IAM/API error but still update in-memory cache
        # so the refreshed token works for this Cloud Run instance's lifetime.
        # Fix: grant roles/secretmanager.secretVersionAdder to pcp-to-cc-sa on this secret.
        logger.error(f"update_secret: could not write '{secret_id}' to Secret Manager ({e}) — token cached in memory only")
    _secrets[secret_id] = value  # always update cache, even if Secret Manager write failed


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
            headers={"User-Agent": "pco_webhook (office2@4thu.org)"},
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
        if pcp_value and any(pcp_value.lower() in v.lower() for v in actual_values):
            valid_list_ids = [lid for lid in rule["cc_lists"] if lid]
            matched.update(valid_list_ids)
            logger.info(f"Rule matched: '{rule['description']}' → {valid_list_ids}")
        else:
            logger.info(f"Rule not matched: '{rule['description']}' (field_id={field_id}, got {actual_values}, want '{pcp_value}')")

    return list(matched)


def apply_workflow_rules(person: dict, trigger_workflow_id: str) -> list[dict]:
    """
    Walk PCP_WORKFLOW_RULES and return matching rule dicts (so the caller has
    access to displaces_workflow_id etc), scoped to rules whose trigger_workflow_id
    matches the given workflow — i.e. override-on-entry rules fired from that
    workflow's card.created event. Rules with no trigger_workflow_id are handled
    separately, by the field_datum event handler.
    """
    matched: list[dict] = []
    custom_fields = person.get("custom_fields", {})

    for rule in config.PCP_WORKFLOW_RULES:
        if rule.get("trigger_workflow_id", "") != trigger_workflow_id:
            continue
        field_id      = rule["pcp_field_id"]
        actual_values = custom_fields.get(str(field_id), [])
        pcp_value     = rule["pcp_value"]
        if pcp_value and any(pcp_value.lower() in v.lower() for v in actual_values):
            if rule.get("workflow_id"):
                matched.append(rule)
                logger.info(f"PCP workflow rule matched: '{rule['description']}' → workflow {rule['workflow_id']}")
        else:
            logger.info(f"PCP workflow rule not matched: '{rule['description']}' (field_id={field_id}, got {actual_values}, want '{pcp_value}')")

    return matched


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


def clear_custom_field(person_id: str, field_def_id: str) -> bool:
    """Delete a person's FieldDatum so the field reads as blank.

    Deletes rather than writing "" — a `select` field rejects a value that is not
    one of its options, so PATCHing an empty string into a dropdown fails.
    Returns True if the field is now blank, including when it already was.
    """
    if not field_def_id:
        return False
    if config.TEST_MODE:
        logger.info(f"TEST_MODE — would clear field id={field_def_id} on person {person_id}")
        return True
    auth = (get_secret("PCP_APP_ID"), get_secret("PCP_SECRET"))
    base = config.PCP_API_BASE
    try:
        r = requests.get(f"{base}/people/{person_id}/field_data", auth=auth, timeout=10)
        r.raise_for_status()
        existing = [
            fd for fd in r.json().get("data", [])
            if (fd.get("relationships", {}).get("field_definition", {}).get("data") or {}).get("id") == str(field_def_id)
        ]
        if not existing:
            return True
        for fd in existing:
            d = requests.delete(f"{base}/field_data/{fd['id']}", auth=auth, timeout=10)
            d.raise_for_status()
            logger.info(f"clear_custom_field: deleted FieldDatum {fd['id']} "
                        f"(field_def {field_def_id}) for person {person_id}")
        return True
    except requests.RequestException as e:
        logger.error(f"clear_custom_field failed for person {person_id} field {field_def_id}: {e}")
        if hasattr(e, "response") and e.response is not None:
            logger.error(f"PCP API error body: {e.response.text}")
        return False


def _active_cards_in_workflow(person_id: str, workflow_id: str, auth) -> list[dict]:
    """Return the person's active (non-removed, non-completed) cards in workflow_id.

    Queries the PERSON's own cards and filters by workflow client-side. The workflow
    cards endpoint (/workflows/{id}/cards) does NOT honor where[person_id] — person is
    a relationship, not a filterable attribute — so it returns every card in the
    workflow. Filtering there would match other people's cards (e.g. removing the
    wrong person, or wrongly thinking this person is already enrolled).
    """
    r = requests.get(
        f"{config.PCP_API_BASE}/people/{person_id}/workflow_cards",
        params={"include": "workflow", "per_page": 100},
        auth=auth, timeout=10,
    )
    r.raise_for_status()
    return [
        c for c in r.json().get("data", [])
        if (c.get("relationships", {}).get("workflow", {}).get("data") or {}).get("id") == workflow_id
        and c.get("attributes", {}).get("removed_at") is None
        and c.get("attributes", {}).get("stage") != "completed"
    ]


def add_to_workflow(person_id: str, workflow_id: str, reason: str = "") -> bool:
    """Add person to workflow, optionally adding a note explaining why. Skips if
    person already has an active (non-removed, non-completed) card in the workflow."""
    if config.TEST_MODE:
        logger.info(f"TEST_MODE — would add person {person_id} to workflow {workflow_id}  reason='{reason}'")
        return True
    try:
        auth = (get_secret("PCP_APP_ID"), get_secret("PCP_SECRET"))
        active = _active_cards_in_workflow(person_id, workflow_id, auth)
        if active:
            logger.info(f"add_to_workflow: person {person_id} already active in workflow {workflow_id} — skipping")
            return True
        url  = f"{config.PCP_API_BASE}/workflows/{workflow_id}/cards"
        body = {"data": {"type": "WorkflowCard",
                         "relationships": {"person": {"data": {"type": "Person", "id": person_id}}}}}
        r = requests.post(url, json=body, auth=auth, timeout=10)
        r.raise_for_status()
        logger.info(f"add_to_workflow: added person {person_id} to workflow {workflow_id}  HTTP {r.status_code}")
        if reason:
            card_id = r.json()["data"]["id"]
            try:
                requests.post(
                    f"{url}/{card_id}/notes",
                    json={"data": {"type": "WorkflowCardNote", "attributes": {"note": reason}}},
                    auth=auth, timeout=10,
                ).raise_for_status()
            except requests.RequestException as note_err:
                logger.warning(f"add_to_workflow: could not add note to card {card_id}: {note_err}")
        return True
    except requests.RequestException as e:
        logger.error(f"add_to_workflow failed person {person_id} workflow {workflow_id}: {e}")
        if hasattr(e, "response") and e.response is not None:
            logger.error(f"PCP API error body: {e.response.text}")
        return False


def complete_workflow_for_person(person_id: str, workflow_id: str, reason: str = "Removed via automation") -> bool:
    """Find the person's active card in workflow_id, add a note, then remove it. Silent no-op if not enrolled."""
    if config.TEST_MODE:
        logger.info(f"TEST_MODE — would remove person {person_id} from workflow {workflow_id}  reason='{reason}'")
        return True
    try:
        auth = (get_secret("PCP_APP_ID"), get_secret("PCP_SECRET"))
        active = _active_cards_in_workflow(person_id, workflow_id, auth)
        if not active:
            logger.info(f"complete_workflow_for_person: person {person_id} not active in workflow {workflow_id} — skipping")
            return True
        card_id = active[0]["id"]
        base = f"{config.PCP_API_BASE}/workflows/{workflow_id}/cards/{card_id}"
        try:
            requests.post(
                f"{base}/notes",
                json={"data": {"type": "WorkflowCardNote", "attributes": {"note": reason}}},
                auth=auth, timeout=10,
            ).raise_for_status()
        except requests.RequestException as note_err:
            logger.warning(f"complete_workflow_for_person: could not add note to card {card_id}: {note_err}")
        r2 = requests.post(f"{base}/remove", auth=auth, timeout=10)
        r2.raise_for_status()
        logger.info(f"complete_workflow_for_person: removed card {card_id} from workflow {workflow_id} for person {person_id}  HTTP {r2.status_code}")
        return True
    except requests.RequestException as e:
        logger.error(f"complete_workflow_for_person failed person {person_id} workflow {workflow_id}: {e}")
        if hasattr(e, "response") and e.response is not None:
            logger.error(f"PCP API error body: {e.response.text}")
        return False


# ─── Anytime items (gate steps) ───────────────────────────────────────────────
# PCP workflows are strictly sequential — a card sits on exactly one step and
# there is no optional or parallel step. An "anytime" item is modelled as a gate
# step near the end that this code promotes the moment every item is satisfied.
# Satisfy an item early and the card blows straight through the gate on arrival;
# leave one outstanding and the card parks there, so the workflow cannot complete.

_tab_fields_cache: dict[str, list[dict]] = {}
_steps_cache: dict[str, dict[str, int]] = {}


def _tab_field_definitions(tab_id: str, auth) -> list[dict]:
    """Return the tab's field definitions as [{id, name, data_type, options}].

    Cached per process — field definitions change when a human edits PCP, not
    per request. Options are sideloaded, following the pattern already used in
    pcp_and_realm_csv_transfer.py.
    """
    if tab_id in _tab_fields_cache:
        return _tab_fields_cache[tab_id]
    r = requests.get(
        f"{config.PCP_API_BASE}/tabs/{tab_id}/field_definitions",
        params={"include": "field_options", "per_page": 100},
        auth=auth, timeout=10,
    )
    r.raise_for_status()
    body = r.json()
    option_values = {
        item["id"]: item["attributes"]["value"]
        for item in body.get("included", [])
        if item.get("type") == "FieldOption"
    }
    fields = []
    for f in body.get("data", []):
        attrs = f["attributes"]
        if attrs.get("deleted_at"):
            continue
        related = (f.get("relationships", {}).get("field_options", {}).get("data") or [])
        fields.append({
            "id":        f["id"],
            "name":      attrs.get("name", ""),
            "data_type": attrs.get("data_type", "text"),
            "options":   [option_values[r_["id"]] for r_ in related if r_["id"] in option_values],
        })
    _tab_fields_cache[tab_id] = fields
    return fields


def _workflow_steps(workflow_id: str, auth) -> dict[str, int]:
    """Return {step_id: sequence} for a workflow, cached per process."""
    if workflow_id in _steps_cache:
        return _steps_cache[workflow_id]
    r = requests.get(
        f"{config.PCP_API_BASE}/workflows/{workflow_id}/steps",
        params={"per_page": 100}, auth=auth, timeout=10,
    )
    r.raise_for_status()
    steps = {s["id"]: s["attributes"].get("sequence", 0) for s in r.json().get("data", [])}
    _steps_cache[workflow_id] = steps
    return steps


def _gate_items(cfg: dict, auth) -> list[dict]:
    """Return the fields that actually gate the workflow.

    Only `select` (dropdown) fields on the tab are items. text/paragraph/boolean
    fields on the same tab are data — a tab like "Membership Ceremony 5-17-26"
    holds both status dropdowns and content fields such as "Bio Text Edited",
    and gating on someone's bio prose would park every card forever.
    Durable prerequisites named by requires_person_fields are appended.
    """
    items = [f for f in _tab_field_definitions(cfg["field_tab_id"], auth)
             if f["data_type"] == "select"]
    durable = set(str(x) for x in cfg.get("requires_person_fields", []))
    if durable:
        seen = {f["id"] for f in items}
        for tab in {str(w["field_tab_id"]) for w in config.ANYTIME_ITEM_WORKFLOWS}:
            for f in _tab_field_definitions(tab, auth):
                if f["id"] in durable and f["id"] not in seen:
                    items.append(f)
                    seen.add(f["id"])
        for fid in durable - {f["id"] for f in items}:
            items.append({"id": fid, "name": f"field {fid}", "data_type": "select", "options": []})
    return items


def outstanding_items(person: dict, cfg: dict, auth) -> tuple[list[dict], list[dict]]:
    """Split a workflow's anytime items into (satisfied, outstanding) for a person.

    An item is satisfied when its stored value appears in `satisfying_values`.
    Anything else — "Promised", blank, an unrecognised value — leaves it
    outstanding, which is the conservative direction: the card parks rather than
    completing on a value nobody intended to mean "done".
    """
    satisfying = set(cfg.get("satisfying_values", ["Done", "Not needed", "Waived"]))
    custom_fields = person.get("custom_fields", {})
    satisfied, outstanding = [], []
    for item in _gate_items(cfg, auth):
        values = custom_fields.get(str(item["id"]), [])
        # A renamed dropdown option silently blocks every card, so say so loudly.
        for v in values:
            if v and item["options"] and v not in item["options"]:
                logger.warning(
                    f"anytime item '{item['name']}' ({item['id']}) holds {v!r}, which is not "
                    f"one of its options {item['options']} — was the dropdown edited in PCP?"
                )
        if any(v in satisfying for v in values):
            satisfied.append(item)
        else:
            outstanding.append(item)
    return satisfied, outstanding


def workflows_for_field(field_definition_id: str, auth) -> list[dict]:
    """Return anytime configs whose gate depends on the given field.

    A field_datum event carries only a field id, so this maps a field change back
    to the gates it could unblock — matching either a `select` field on a
    configured tab, or a durable field named by requires_person_fields.
    """
    fid = str(field_definition_id)
    hits = []
    for cfg in config.ANYTIME_ITEM_WORKFLOWS:
        if fid in {str(x) for x in cfg.get("requires_person_fields", [])}:
            hits.append(cfg)
            continue
        try:
            if any(f["id"] == fid for f in _tab_field_definitions(cfg["field_tab_id"], auth)):
                hits.append(cfg)
        except requests.RequestException as e:
            logger.warning(f"workflows_for_field: could not read tab {cfg.get('field_tab_id')}: {e}")
    return hits


def reevaluate_gates_for_field(person_id: str, field_definition_id: str) -> list[str]:
    """Re-run the gate for every workflow this person is in that the field affects.

    This is what makes an item genuinely "anytime": satisfying it while the card
    is still on an early sequential step records the value and does nothing, and
    satisfying it while the card waits on the gate releases the card immediately.
    """
    if not config.ANYTIME_ITEM_WORKFLOWS:
        return []
    auth = (get_secret("PCP_APP_ID"), get_secret("PCP_SECRET"))
    results = []
    for cfg in workflows_for_field(field_definition_id, auth):
        workflow_id = cfg["workflow_id"]
        try:
            cards = _active_cards_in_workflow(person_id, workflow_id, auth)
        except requests.RequestException as e:
            logger.warning(f"reevaluate_gates_for_field: card lookup failed for {person_id}: {e}")
            continue
        for card in cards:
            step = (card.get("relationships", {}).get("current_step", {}).get("data") or {}).get("id", "")
            results.append(evaluate_gate(person_id, workflow_id, card["id"], step))
    return results


def evaluate_gate(person_id: str, workflow_id: str, card_id: str, current_step_id: str) -> str:
    """Promote a card sitting on its gate step once every anytime item is satisfied.

    Returns a short status string for logging/response. No-op for workflows with
    no anytime config, and for cards not currently on the gate step.
    """
    cfg = config.anytime_workflow(workflow_id)
    if not cfg:
        return "no-gate-config"
    if current_step_id != str(cfg.get("gate_step_id", "")):
        return "not-on-gate"

    pcp_data = fetch_person_from_pcp(person_id)
    if not pcp_data:
        logger.warning(f"evaluate_gate: could not fetch person {person_id}")
        return "person-fetch-failed"
    person = parse_person(pcp_data)

    auth = (get_secret("PCP_APP_ID"), get_secret("PCP_SECRET"))
    satisfied, outstanding = outstanding_items(person, cfg, auth)
    names = [i["name"] for i in outstanding]
    logger.info(
        f"evaluate_gate: person={person_id} workflow={workflow_id} card={card_id} "
        f"satisfied={[i['name'] for i in satisfied]} outstanding={names}"
    )

    if outstanding:
        note = "Outstanding before this workflow can complete: " + ", ".join(names)
        _add_card_note(workflow_id, card_id, note, auth)
        return f"parked ({len(outstanding)} outstanding)"

    if config.TEST_MODE:
        logger.info(f"TEST_MODE — would promote card {card_id} past gate in workflow {workflow_id}")
        return "would-promote"
    try:
        r = requests.post(
            f"{config.PCP_API_BASE}/workflows/{workflow_id}/cards/{card_id}/promote",
            auth=auth, timeout=10,
        )
        r.raise_for_status()
        logger.info(f"evaluate_gate: promoted card {card_id} past gate  HTTP {r.status_code}")
        return "promoted"
    except requests.RequestException as e:
        logger.error(f"evaluate_gate: promote failed for card {card_id}: {e}")
        if hasattr(e, "response") and e.response is not None:
            logger.error(f"PCP API error body: {e.response.text}")
        return "promote-failed"


def _add_card_note(workflow_id: str, card_id: str, note: str, auth) -> None:
    """Best-effort note on a workflow card. Never raises — notes are not the point."""
    if config.TEST_MODE:
        logger.info(f"TEST_MODE — would note card {card_id}: {note}")
        return
    try:
        requests.post(
            f"{config.PCP_API_BASE}/workflows/{workflow_id}/cards/{card_id}/notes",
            json={"data": {"type": "WorkflowCardNote", "attributes": {"note": note}}},
            auth=auth, timeout=10,
        ).raise_for_status()
    except requests.RequestException as e:
        logger.warning(f"could not add note to card {card_id}: {e}")


def clear_consumable_items(person_id: str, workflow_id: str, card_id: str) -> None:
    """On enrolment, reset this workflow's anytime items so the run starts clean.

    Prior values are written to the new card's notes first, so "bought a shirt in
    2026" survives on the card even though the live field resets for 2027.
    Durable prerequisites (requires_person_fields) are never cleared — that is
    the whole point of keeping them on a separate tab.
    """
    cfg = config.anytime_workflow(workflow_id)
    if not cfg:
        return
    pcp_data = fetch_person_from_pcp(person_id)
    if not pcp_data:
        return
    person = parse_person(pcp_data)
    custom_fields = person.get("custom_fields", {})
    auth = (get_secret("PCP_APP_ID"), get_secret("PCP_SECRET"))
    durable = {str(x) for x in cfg.get("requires_person_fields", [])}

    carried = []
    for item in _gate_items(cfg, auth):
        if item["id"] in durable:
            continue
        values = [v for v in custom_fields.get(str(item["id"]), []) if v]
        if values:
            carried.append(f"{item['name']}: {', '.join(values)}")
            clear_custom_field(person_id, item["id"])
    if carried:
        _add_card_note(workflow_id, card_id,
                       "Carried over from the previous enrolment (now reset): "
                       + "; ".join(carried), auth)
        logger.info(f"clear_consumable_items: reset {len(carried)} item(s) for person {person_id}")


# ─── Readiness report ─────────────────────────────────────────────────────────
# The operational question is not "is Bob's card correct" but "who among these 40
# people still owes what, and how do I reach them before Saturday". A card parks
# on one step, so the PCP workflow UI cannot answer that — this can.

def _person_for_report(person_id: str, auth) -> dict:
    """Fetch one person with the fields the report needs (email, phone, customs)."""
    r = requests.get(
        f"{config.PCP_API_BASE}/people/{person_id}",
        params={"include": "emails,phone_numbers,field_data"},
        auth=auth, timeout=10,
    )
    r.raise_for_status()
    body = r.json()
    person = parse_person(body)
    person["phone"] = next(
        (i.get("attributes", {}).get("number", "")
         for i in body.get("included", [])
         if i.get("type") == "PhoneNumber"),
        "",
    )
    return person


def build_readiness(workflow_id: str) -> dict:
    """Assemble the person x item matrix for one workflow.

    Returns {config, items, notes_field, rows, totals}. Rows are sorted with the
    most outstanding first, so the chase list is the top of the page.
    """
    cfg = config.anytime_workflow(workflow_id)
    if not cfg:
        raise ValueError(f"workflow {workflow_id} has no anytime_item_workflows config")

    auth = (get_secret("PCP_APP_ID"), get_secret("PCP_SECRET"))
    items = _gate_items(cfg, auth)
    notes_field = str(cfg.get("notes_field_id", "")) or None

    # Names for the header — an ID on its own tells the reader nothing.
    def _name_of(path: str) -> str:
        try:
            r = requests.get(f"{config.PCP_API_BASE}/{path}", auth=auth, timeout=10)
            r.raise_for_status()
            return r.json().get("data", {}).get("attributes", {}).get("name", "")
        except requests.RequestException:
            return ""

    workflow_name = _name_of(f"workflows/{workflow_id}")
    tab_name = _name_of(f"tabs/{cfg['field_tab_id']}")

    cards = requests.get(
        f"{config.PCP_API_BASE}/workflows/{workflow_id}/cards",
        params={"include": "person", "per_page": 100}, auth=auth, timeout=15,
    )
    cards.raise_for_status()
    active = [
        c for c in cards.json().get("data", [])
        if c["attributes"].get("removed_at") is None
        and c["attributes"].get("stage") != "completed"
    ]
    person_ids = [
        (c.get("relationships", {}).get("person", {}).get("data") or {}).get("id")
        for c in active
    ]
    person_ids = [p for p in person_ids if p]

    # Sequential fetches would make a 40-person page take ~10s; a small pool keeps
    # it responsive without hammering the PCP rate limit.
    people: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_person_for_report, pid, auth): pid for pid in person_ids}
        for fut in as_completed(futures):
            pid = futures[fut]
            try:
                people[pid] = fut.result()
            except requests.RequestException as e:
                logger.warning(f"build_readiness: could not fetch person {pid}: {e}")

    satisfying = set(cfg.get("satisfying_values", ["Done", "Not needed", "Waived"]))
    exempt = {"Not needed", "Waived"}
    rows, per_item = [], {i["id"]: {"outstanding": 0, "exempt": 0} for i in items}

    for pid, person in people.items():
        custom = person.get("custom_fields", {})
        cells, missing = [], 0
        for item in items:
            values = [v for v in custom.get(str(item["id"]), []) if v]
            value = values[0] if values else ""
            done = any(v in satisfying for v in values)
            if not done:
                missing += 1
                per_item[item["id"]]["outstanding"] += 1
            elif value in exempt:
                per_item[item["id"]]["exempt"] += 1
            cells.append({"value": value, "done": done,
                          "exempt": done and value in exempt})
        rows.append({
            "name":    f"{person.get('first_name','')} {person.get('last_name','')}".strip(),
            "email":   person.get("email", ""),
            "phone":   person.get("phone", ""),
            "cells":   cells,
            "missing": missing,
            "notes":   (custom.get(notes_field, [""])[0] if notes_field else ""),
        })

    rows.sort(key=lambda r: (-r["missing"], r["name"]))
    return {
        "config": cfg, "items": items, "rows": rows,
        "workflow_name": workflow_name, "tab_name": tab_name,
        "totals": {
            "enrolled":    len(rows),
            "ready":       sum(1 for r in rows if r["missing"] == 0),
            "outstanding": sum(1 for r in rows if r["missing"] > 0),
            "per_item":    per_item,
        },
    }


def render_readiness_html(data: dict) -> str:
    """Render the matrix as a standalone page — no external assets, no CDN."""
    cfg, items, rows, totals = data["config"], data["items"], data["rows"], data["totals"]
    esc = html_escape

    # Prefer the workflow's real PCP name in the heading; the rule's description
    # is the automation's label for itself and means little to a reader.
    wf_name, tab_name = data.get("workflow_name", ""), data.get("tab_name", "")
    wf_label = wf_name or cfg.get("description", "Workflow")
    wf_suffix = f" ({wf_name})" if wf_name else ""
    tab_suffix = f" ({tab_name})" if tab_name else ""

    head = "".join(f"<th>{esc(i['name'])}</th>" for i in items)
    body = []
    for r in rows:
        cells = []
        for c in r["cells"]:
            if c["exempt"]:
                cls, text = "exempt", c["value"]
            elif c["done"]:
                cls, text = "done", c["value"] or "Done"
            else:
                cls, text = "todo", c["value"] or "—"
            cells.append(f'<td class="{cls}">{esc(text)}</td>')
        klass = "ready" if r["missing"] == 0 else ""
        body.append(
            f'<tr class="{klass}"><td class="nm">{esc(r["name"])}</td>'
            f'<td class="qt">{esc(r["phone"])}</td>'
            f'<td class="qt">{esc(r["email"])}</td>'
            + "".join(cells)
            + f'<td class="miss">{r["missing"] or ""}</td>'
            f'<td class="qt">{esc(r["notes"])}</td></tr>'
        )

    summary = "  ".join(
        f'<span class="pill">{esc(i["name"])}: '
        f'{totals["per_item"][i["id"]]["outstanding"]} outstanding'
        + (f' ({totals["per_item"][i["id"]]["exempt"]} not needed)'
           if totals["per_item"][i["id"]]["exempt"] else "")
        + "</span>"
        for i in items
    )

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{esc(cfg.get('description','Readiness'))} — Readiness</title>
<style>
 :root {{ color-scheme: light dark; }}
 body {{ font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
        margin:0; padding:1.5rem; }}
 h1 {{ font-size:1.25rem; margin:0 0 .25rem; }}
 .sub {{ opacity:.7; font-size:.85rem; margin-bottom:1rem; }}
 .totals {{ margin:0 0 1rem; font-size:.9rem; }}
 .pill {{ display:inline-block; padding:.15rem .5rem; margin:.15rem .3rem .15rem 0;
          border:1px solid currentColor; border-radius:1rem; opacity:.85; }}
 .wrap {{ overflow-x:auto; }}
 table {{ border-collapse:collapse; min-width:100%; }}
 th,td {{ padding:.4rem .6rem; text-align:left; white-space:nowrap;
          border-bottom:1px solid rgba(128,128,128,.25); }}
 th {{ font-size:.78rem; text-transform:uppercase; letter-spacing:.04em; opacity:.7; }}
 .nm {{ font-weight:600; }}
 .qt {{ opacity:.75; font-size:.9em; }}
 .todo {{ color:#b3261e; font-weight:600; }}
 .done {{ color:#1b6b3a; }}
 .exempt {{ opacity:.55; font-style:italic; }}
 .miss {{ font-weight:700; text-align:center; }}
 tr.ready {{ opacity:.5; }}
 @media (prefers-color-scheme: dark) {{
   .todo {{ color:#ff8a80; }} .done {{ color:#7ddc9a; }}
 }}
</style></head><body>
<h1>{esc(wf_label)} — anytime items still needed</h1>
<div class="sub">Workflow ID {esc(str(cfg.get('workflow_id')))}{esc(wf_suffix)} ·
 Items from tab ID {esc(str(cfg.get('field_tab_id')))}{esc(tab_suffix)} ·
 Rule: {esc(cfg.get('description', ''))} ·
 Run {datetime.now().strftime('%d %b %Y, %-I:%M %p')}</div>
<div class="totals"><strong>{totals['enrolled']} enrolled · {totals['ready']} ready ·
 {totals['outstanding']} outstanding</strong><br>{summary}</div>
<div class="wrap"><table>
<thead><tr><th>Name</th><th>Phone</th><th>Email</th>{head}<th>Miss</th><th>Notes</th></tr></thead>
<tbody>{''.join(body)}</tbody></table></div>
</body></html>"""


# ─── Brevo chase email ────────────────────────────────────────────────────────
# One message per person naming only their own outstanding items. A PCP dynamic
# List per item is the zero-code alternative, but someone missing three things
# gets three generic blasts from it; this sends them one useful email.

BREVO_SEND_URL = "https://api.brevo.com/v3/smtp/email"


def _chase_email_html(row: dict, cfg: dict) -> str:
    """Body for one person's chase email — their outstanding items, nothing else."""
    esc = html_escape
    missing = "".join(f"<li>{esc(c['name'])}</li>" for c in row["outstanding_names"])
    event = esc(cfg.get("description", "this event"))
    return (
        f"<p>Hi {esc(row['name'].split(' ')[0] or 'there')},</p>"
        f"<p>Before {event}, we still need:</p>"
        f"<ul>{missing}</ul>"
        f"<p>Everything else on your list is complete — thank you!</p>"
    )


def send_chase_emails(workflow_id: str, dry_run: bool = True) -> dict:
    """Email everyone in the workflow who still has outstanding items.

    Defaults to dry_run: sending mail to a cohort is not something to trigger by
    accident, so the caller has to ask for it explicitly.
    """
    data = build_readiness(workflow_id)
    cfg, items = data["config"], data["items"]
    sender = cfg.get("sender", {})

    targets = []
    for row in data["rows"]:
        if row["missing"] == 0 or not row["email"]:
            continue
        row = {**row, "outstanding_names": [
            items[idx] for idx, cell in enumerate(row["cells"]) if not cell["done"]
        ]}
        targets.append(row)

    if dry_run or config.TEST_MODE:
        for t in targets:
            logger.info(f"DRY RUN — would email {t['email']}: "
                        f"{[i['name'] for i in t['outstanding_names']]}")
        return {"sent": 0, "would_send": len(targets), "dry_run": True,
                "recipients": [t["email"] for t in targets]}

    api_key = get_secret("BREVO_API_KEY")
    headers = {"api-key": api_key, "content-type": "application/json",
               "accept": "application/json"}
    sent, failed = 0, []
    for t in targets:
        payload = {
            "sender": {"name":  sender.get("name", "4th Universalist"),
                       "email": sender.get("email", "office2@4thu.org")},
            "to": [{"email": t["email"], "name": t["name"]}],
            "subject": f"{cfg.get('description', 'Upcoming event')} — "
                       f"{t['missing']} item{'s' if t['missing'] != 1 else ''} still needed",
            "htmlContent": _chase_email_html(t, cfg),
        }
        try:
            r = requests.post(BREVO_SEND_URL, json=payload, headers=headers, timeout=15)
            r.raise_for_status()
            sent += 1
        except requests.RequestException as e:
            logger.error(f"Brevo send failed for {t['email']}: {e}")
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"Brevo error body: {e.response.text}")
            failed.append(t["email"])
    logger.info(f"send_chase_emails: workflow {workflow_id} — sent {sent}, failed {len(failed)}")
    return {"sent": sent, "failed": failed, "dry_run": False}


# ─── Flask routes ─────────────────────────────────────────────────────────────

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    # PCP sends a GET to verify the endpoint is reachable when you first subscribe
    if request.method == "GET":
        logger.info("Webhook GET verification request received")
        return jsonify({"status": "ok"}), 200

    try:
        return _handle_webhook_post()
    except Exception as exc:
        logger.exception(f"Unhandled exception in webhook handler: {exc}")
        return jsonify({"error": "internal error"}), 500


def _handle_webhook_post():
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

    # ── Early-exit for events that never require action ───────────────────────
    _IGNORE_EVENTS = {
        "people.v2.events.person.destroyed",
        "people.v2.events.workflow_step.updated",
    }
    if event_name in _IGNORE_EVENTS:
        logger.info(f"Ignored event (no action needed): {event_name}")
        return jsonify({"status": "ignored", "event": event_name}), 200

    # ── Workflow card events — set custom fields ──────────────────────────────
    _WORKFLOW_CARD_EVENTS = {
        "people.v2.events.workflow_card.created",
        "people.v2.events.workflow_card.updated",
    }
    if event_name in _WORKFLOW_CARD_EVENTS:
        # "entered" comes only from card.created. "completed" comes from a card.updated
        # whose stage is "completed". Any other card.updated (a step move, a removal,
        # snooze, etc.) is NOT an entry or completion — ignore it, otherwise removing a
        # card or moving a step would re-fire the workflow's "entered" rules.
        person_id   = event.person_id
        workflow_id = event.workflow_id

        # Anytime-item gate. A step move is exactly the event the rest of this
        # handler discards, and exactly the one the gate needs: it carries the
        # step the card just landed on. Scoped to workflows with a gate config so
        # every other workflow keeps its existing ignore-the-step-move behaviour.
        if (event_name == "people.v2.events.workflow_card.updated"
                and event.stage != "completed"
                and person_id
                and config.anytime_workflow(workflow_id)):
            result = evaluate_gate(person_id, workflow_id, event.card_id, event.current_step_id)
            if result != "not-on-gate":
                return jsonify({"status": "ok", "event": event_name,
                                "gate": result, "person_id": person_id}), 200

        if event_name == "people.v2.events.workflow_card.created":
            trigger = "entered"
        elif event.stage == "completed":
            trigger = "completed"
        else:
            logger.info(f"Ignored {event_name} (stage={event.stage}) — not an entry or completion  "
                        f"person_id={person_id}  workflow_id={workflow_id}")
            return jsonify({"status": "ignored", "event": event_name, "stage": event.stage}), 200
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
            if rule.get("add_to_workflow_id"):
                add_to_workflow(person_id, rule["add_to_workflow_id"],
                                 reason=f"Added by chain rule: {rule['description']}")
                matched = True
            if rule.get("remove_workflow_id"):
                complete_workflow_for_person(
                    person_id, rule["remove_workflow_id"],
                    reason=f"Removed by chain rule: {rule['description']}",
                )
                matched = True
            if rule.get("add_to_workflow_id") or rule.get("remove_workflow_id"):
                logger.info(f"Workflow chain rule applied: '{rule['description']}'")

        # A fresh enrolment starts with a clean slate: reset this workflow's
        # consumable anytime items so last year's shirt does not satisfy this
        # year's gate. Durable prerequisites are deliberately left alone.
        if (event_name == "people.v2.events.workflow_card.created"
                and config.anytime_workflow(workflow_id)):
            clear_consumable_items(person_id, workflow_id, event.card_id)
            matched = True

        # PCP workflow override: on entry to a workflow, if a field-based
        # override rule matches, redirect to the rule's target workflow.
        # Field values are guaranteed settled by the time card.created fires,
        # which avoids the form-submission eventual-consistency race.
        if event_name == "people.v2.events.workflow_card.created":
            pcp_data = fetch_person_from_pcp(person_id)
            if pcp_data:
                person = parse_person(pcp_data)
                for rule in apply_workflow_rules(person, trigger_workflow_id=workflow_id):
                    matched = True
                    add_to_workflow(person_id, rule["workflow_id"],
                                     reason=f"Added by automation rule: {rule['description']}")
                    if rule.get("displaces_workflow_id"):
                        complete_workflow_for_person(
                            person_id, rule["displaces_workflow_id"],
                            reason=f"Replaced by automation rule: {rule['description']}",
                        )
                    logger.info(f"PCP workflow override applied: '{rule['description']}'")

        if not matched:
            logger.info(f"No workflow rules matched workflow_id={workflow_id} trigger={trigger}")
        return jsonify({"status": "ok", "event": event_name, "person_id": person_id}), 200

    # ── Field datum events — field-driven workflow rules ──────────────────────
    # A custom field was written or changed. The event carries the field id,
    # person id, and value inline, so PCP_WORKFLOW_RULES can match without a PCP
    # re-fetch — avoiding the eventual-consistency race where a person/form event
    # fires before the form's field data has settled. This is the sole path for
    # rules with no trigger_workflow_id; rules WITH one fire on workflow entry
    # instead (see the workflow_card.created block above).
    _FIELD_DATUM_EVENTS = {
        "people.v2.events.field_datum.created",
        "people.v2.events.field_datum.updated",
    }
    if event_name in _FIELD_DATUM_EVENTS:
        field_id  = event.field_definition_id
        person_id = event.person_id
        value     = event.field_value
        if not (field_id and person_id):
            logger.info(f"Ignored {event_name}: missing field_definition_id or person_id")
            return jsonify({"status": "ignored", "event": event_name}), 200

        matched = False
        for rule in config.PCP_WORKFLOW_RULES:
            if rule.get("trigger_workflow_id", ""):
                continue  # workflow-entry rules fire from workflow_card.created instead
            if rule["pcp_field_id"] != field_id:
                continue
            pcp_value = rule["pcp_value"]
            if pcp_value and pcp_value.lower() in value.lower():
                matched = True
                add_to_workflow(person_id, rule["workflow_id"],
                                 reason=f"Added by automation rule: {rule['description']}")
                if rule.get("displaces_workflow_id"):
                    complete_workflow_for_person(
                        person_id, rule["displaces_workflow_id"],
                        reason=f"Replaced by automation rule: {rule['description']}",
                    )
                logger.info(f"Field datum rule applied: '{rule['description']}'  "
                            f"person {person_id} → workflow {rule['workflow_id']}")
            else:
                logger.info(f"Field datum rule not matched: '{rule['description']}' "
                            f"(field_id={field_id}, value={value!r}, want '{pcp_value}')")

        # An anytime item may have just been satisfied. If the person's card is
        # already waiting on that workflow's gate, this releases it now; if the
        # card is still on an earlier sequential step, this is a no-op and the
        # value simply waits there until the card arrives.
        gate_results = reevaluate_gates_for_field(person_id, field_id)
        if gate_results:
            matched = True
            logger.info(f"Anytime gate re-evaluated for person {person_id} "
                        f"field {field_id}: {gate_results}")

        if not matched:
            logger.info(f"No field datum rules matched field_id={field_id} person_id={person_id}")
        return jsonify({"status": "ok", "event": event_name, "person_id": person_id}), 200

    # ── Form completion rules — workflow actions on form submit ────────────────
    if event_name == "people.v2.events.form_submission.created":
        form_id   = event.form_id
        person_id = event.person_id
        if form_id and person_id:
            matched = False
            for rule in config.FORM_COMPLETION_RULES:
                if rule["form_id"] != form_id:
                    continue
                rule_matched = False
                if rule.get("complete_workflow_id"):
                    rule_matched = True
                    complete_workflow_for_person(
                        person_id, rule["complete_workflow_id"],
                        reason=f"Removed via automation — form {form_id} submitted",
                    )
                if rule.get("add_to_workflow_id"):
                    rule_matched = True
                    add_to_workflow(person_id, rule["add_to_workflow_id"],
                                     reason=f"Added via automation — form {form_id} submitted")
                if rule_matched:
                    matched = True
                    logger.info(f"Form completion rule applied: '{rule['description']}'")
            if not matched:
                logger.info(f"No form rules matched form_id={form_id} person_id={person_id}")

    _PERSON_EVENTS = {
        "people.v2.events.person.created",
        "people.v2.events.person.updated",
        "people.v2.events.form_submission.created",
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

    # ── Apply CC list rules ───────────────────────────────────────────────────
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


@app.route("/readiness/<workflow_id>", methods=["GET"])
def readiness(workflow_id: str):
    """Who in this workflow still owes what. Queries PCP on load, so never stale.

    Guarded by a shared secret because this service is publicly reachable and the
    page carries names, emails and phone numbers. Set READINESS_TOKEN in Secret
    Manager; without it the route stays disabled rather than open.
    """
    try:
        expected = get_secret("READINESS_TOKEN")
    except Exception:
        logger.warning("readiness: READINESS_TOKEN secret missing — route disabled")
        return jsonify({"error": "readiness report not configured"}), 503
    if not expected or not hmac.compare_digest(request.args.get("key", ""), expected):
        return jsonify({"error": "unauthorized"}), 401

    try:
        data = build_readiness(workflow_id)
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except requests.RequestException as e:
        logger.error(f"readiness: PCP fetch failed for workflow {workflow_id}: {e}")
        return jsonify({"error": "could not reach Planning Center"}), 502

    if request.args.get("format") == "json":
        return jsonify(data), 200
    return Response(render_readiness_html(data), mimetype="text/html")


@app.route("/readiness/<workflow_id>/chase", methods=["POST"])
def readiness_chase(workflow_id: str):
    """Send each person with outstanding items one email listing just theirs.

    POST, not GET, because this reaches real people — a link someone clicks twice
    must not mail the cohort twice. Dry run unless ?send=true is passed, so the
    default outcome of getting this wrong is a log line, not an inbox.
    """
    try:
        expected = get_secret("READINESS_TOKEN")
    except Exception:
        return jsonify({"error": "readiness report not configured"}), 503
    if not expected or not hmac.compare_digest(request.args.get("key", ""), expected):
        return jsonify({"error": "unauthorized"}), 401

    dry_run = request.args.get("send") != "true"
    try:
        return jsonify(send_chase_emails(workflow_id, dry_run=dry_run)), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except requests.RequestException as e:
        logger.error(f"readiness_chase: PCP fetch failed for {workflow_id}: {e}")
        return jsonify({"error": "could not reach Planning Center"}), 502


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
    import os
    rules_source = "env_var" if os.environ.get("RULES_JSON") else "file"
    return jsonify({
        "TEST_MODE":              config.TEST_MODE,
        "LOG_PAYLOADS":           config.LOG_PAYLOADS,
        "CLOUD_PROJECT_ID":       config.CLOUD_PROJECT_ID,
        "PCP_API_BASE":           config.PCP_API_BASE,
        "CC_API_BASE":            config.CC_API_BASE,
        "CC_LIST_RULES":          rules_summary,
        "RULES_SOURCE":           rules_source,
        "WORKFLOW_FIELD_RULES":   config.WORKFLOW_FIELD_RULES,
        "WORKFLOW_CHAIN_RULES":   config.WORKFLOW_CHAIN_RULES,
        "FORM_COMPLETION_RULES":  config.FORM_COMPLETION_RULES,
        "PCP_WORKFLOW_RULES":     config.PCP_WORKFLOW_RULES,
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
