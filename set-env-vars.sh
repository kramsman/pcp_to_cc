#!/bin/bash
# Update Cloud Run environment variables without redeploying.
# Run this from the project root: ./set-env-vars.sh
# Also called automatically by deploy.sh after every deploy.
#
# Each variable is one array entry so inline comments work correctly.
# The ^|^ prefix tells gcloud to use | as the separator instead of the
# default comma — necessary if any values contain commas.

ENV_VARS=(

  # ── Feature flags ──────────────────────────────────────────────────────────
  # TEST_MODE=true skips the CC API call and logs what would happen instead.
  # Set to false only when ready to go live.
  "TEST_MODE=true"

  # LOG_PAYLOADS=true logs raw webhook payloads and PCP API responses.
  # Useful for discovering PCP field definition IDs. Contains PII — disable when stable.
  "LOG_PAYLOADS=true"

  # ── PCP field definition IDs ───────────────────────────────────────────────
  # Find by setting LOG_PAYLOADS=true and submitting a test person.
  # Look in logs for type=FieldDatum → relationships.field_definition.data.id
  "PCP_NEWSLETTER_FIELD_ID=your-pcp-field-definition-id"

  # ── Constant Contact list IDs ──────────────────────────────────────────────
  # Find in CC: Contacts → Lists → click a list → UUID is in the URL.
  "CC_NEWSLETTER_LIST_ID=your-cc-list-uuid"

  # ── GCP ────────────────────────────────────────────────────────────────────
  "CLOUD_PROJECT_ID=YOUR_PROJECT_ID"

)

# Join array with | separator and pass to gcloud
IFS='|'
gcloud run services update pcp-to-cc \
  --region us-east1 \
  --update-env-vars "^|^${ENV_VARS[*]}" \
  --project YOUR_PROJECT_ID
