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
  # Find by running: python find_pcp_custom_field_ids.py  (requires PCP credentials in Secret Manager)
  "PCP_NEWSLETTER_TRIGGER_FIELD_ID=1039700"

  # ── Constant Contact list IDs ──────────────────────────────────────────────
  # Find in CC: Contacts → Lists → click a list → UUID is in the URL.
  "CC_NEWSLETTER_LIST_ID=dd8406e2-129f-11ed-a1a4-fa163eaee913"

  # ── GCP ────────────────────────────────────────────────────────────────────
  "CLOUD_PROJECT_ID=pcp-to-cc"

)

# Join array with | separator and pass to gcloud
IFS='|'
gcloud run services update pcp-to-cc \
  --region us-east1 \
  --update-env-vars "^|^${ENV_VARS[*]}" \
  --project pcp-to-cc
