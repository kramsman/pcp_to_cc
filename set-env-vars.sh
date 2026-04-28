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
  "TEST_MODE=false"

  # LOG_PAYLOADS=true logs raw webhook payloads and PCP API responses.
  # Useful for discovering PCP field definition IDs. Contains PII — disable when stable.
  "LOG_PAYLOADS=true"

  # ── GCP ────────────────────────────────────────────────────────────────────
  "CLOUD_PROJECT_ID=pcp-to-cc"

)

# ── Rules (rules.json → RULES_JSON env var) ────────────────────────────────
# Minify rules.json and inject as an env var so rule changes take effect
# by running this script alone — no full redeploy needed.
RULES_JSON=$(python3 -c "import json,base64; print(base64.b64encode(json.dumps(json.load(open('rules.json'))).encode()).decode())")
ENV_VARS+=("RULES_JSON=${RULES_JSON}")

# Join array with | separator and pass to gcloud
IFS='|'
gcloud run services update pcp-to-cc \
  --region us-east1 \
  --update-env-vars "^|^${ENV_VARS[*]}" \
  --project pcp-to-cc
