"""
Run this directly in PyCharm (F5) to see how each payload file is parsed.
No server or request needed — loads JSON files from tests/payloads/.
"""

import json
import os
from datetime import datetime
from typing import Annotated, Any, Literal, Optional, Union

from pydantic import BaseModel, Field, ValidationError, model_validator

# ── Paths ──────────────────────────────────────────────────────────────────

_PAYLOADS = os.path.join(os.path.dirname(__file__), "payloads")

PAYLOAD_FILES = {
    "workflow_complete  (direct REST response)": os.path.join(_PAYLOADS, "PCP", "workflow_complete.json"),
    "create_flow_card   (webhook with event field)": os.path.join(_PAYLOADS, "PCP", "create_flow_card.json"),
    "person_created     (legacy webhook, no event field)": os.path.join(_PAYLOADS, "person_created_webhook.json"),
}


# ── Shared primitives ──────────────────────────────────────────────────────

class TypedRef(BaseModel):
    type: str
    id: str


class RelRef(BaseModel):
    data: Optional[TypedRef] = None


# ── WorkflowCard ───────────────────────────────────────────────────────────

class WorkflowCardAttrs(BaseModel):
    stage: str
    completed_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    overdue: bool
    removed_at: Optional[datetime] = None
    snooze_until: Optional[datetime] = None


class WorkflowCardRels(BaseModel):
    assignee: RelRef
    person: RelRef
    workflow: RelRef
    current_step: RelRef


class WorkflowCardData(BaseModel):
    type: Literal["WorkflowCard"]
    id: str
    attributes: WorkflowCardAttrs
    relationships: WorkflowCardRels


# ── Person ─────────────────────────────────────────────────────────────────

class PersonAttrs(BaseModel):
    first_name: str
    last_name: str
    created_at: datetime
    updated_at: datetime


class PersonData(BaseModel):
    type: Literal["Person"]
    id: str
    attributes: PersonAttrs


# ── Inner webhook payload (the JSON string inside EventDelivery) ───────────
# The "payload" field in EventDelivery.attributes arrives as a raw JSON string.
# model_validator decodes it once; the discriminated union routes by "type".

InnerData = Annotated[
    Union[WorkflowCardData, PersonData],
    Field(discriminator="type")
]


class InnerPayload(BaseModel):
    data: InnerData


class EventDeliveryAttrs(BaseModel):
    name: str
    attempt: int
    payload: InnerPayload

    @model_validator(mode="before")
    @classmethod
    def _parse_payload_string(cls, v):
        if isinstance(v.get("payload"), str):
            v["payload"] = json.loads(v["payload"])
        return v


class EventDelivery(BaseModel):
    type: Literal["EventDelivery"]
    id: str
    attributes: EventDeliveryAttrs


# ── Format A: Direct REST response (workflow_complete.json) ────────────────

class WorkflowCompleteResponse(BaseModel):
    data: WorkflowCardData
    included: list[Any] = []


# ── Format B: Webhook with "event" field (create_flow_card.json) ──────────

class PcpWebhookPayload(BaseModel):
    data: list[EventDelivery]


class PcpWebhookEvent(BaseModel):
    event: str
    payload: PcpWebhookPayload

    @property
    def delivery(self) -> EventDelivery:
        return self.payload.data[0]


# ── Format C: Legacy webhook, no "event" field (person_created_webhook.json)

class LegacyWebhookEvent(BaseModel):
    data: list[EventDelivery]

    @property
    def delivery(self) -> EventDelivery:
        return self.data[0]


# ── Entry point ────────────────────────────────────────────────────────────

def parse_pcp_payload(
    raw: dict,
) -> WorkflowCompleteResponse | PcpWebhookEvent | LegacyWebhookEvent:
    if "event" in raw:
        return PcpWebhookEvent.model_validate(raw)
    if isinstance(raw.get("data"), list):
        return LegacyWebhookEvent.model_validate(raw)
    return WorkflowCompleteResponse.model_validate(raw)


# ── Debug runner ───────────────────────────────────────────────────────────

def _debug_parsed(label: str, parsed: object) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {label}")
    print(f"  → type: {type(parsed).__name__}")

    if isinstance(parsed, WorkflowCompleteResponse):
        card = parsed.data
        print(f"  card id      : {card.id}")
        print(f"  stage        : {card.attributes.stage}")
        print(f"  completed_at : {card.attributes.completed_at}")
        print(f"  person id    : {card.relationships.person.data.id if card.relationships.person.data else None}")
        print(f"  workflow id  : {card.relationships.workflow.data.id if card.relationships.workflow.data else None}")

    elif isinstance(parsed, (PcpWebhookEvent, LegacyWebhookEvent)):
        d = parsed.delivery
        print(f"  delivery id  : {d.id}")
        print(f"  event name   : {d.attributes.name}")
        print(f"  attempt      : {d.attributes.attempt}")
        inner = d.attributes.payload.data
        print(f"  inner type   : {inner.type}")
        print(f"  inner id     : {inner.id}")
        if isinstance(inner, WorkflowCardData):
            print(f"  stage        : {inner.attributes.stage}")
            print(f"  person id    : {inner.relationships.person.data.id if inner.relationships.person.data else None}")
        elif isinstance(inner, PersonData):
            print(f"  name         : {inner.attributes.first_name} {inner.attributes.last_name}")

    print(f"\n  full model_dump:")
    import pprint
    pprint.pprint(parsed.model_dump(), indent=4)


def main() -> None:
    for label, path in PAYLOAD_FILES.items():
        print(f"\n{'═' * 60}")
        print(f"FILE: {os.path.basename(path)}")
        if not os.path.exists(path):
            print(f"  *** FILE NOT FOUND: {path}")
            continue
        with open(path, encoding="utf-8") as fh:
            raw = json.load(fh)
        try:
            parsed = parse_pcp_payload(raw)
            _debug_parsed(label, parsed)
        except ValidationError as exc:
            print(f"  *** VALIDATION ERROR:\n{exc}")


if __name__ == "__main__":
    main()
