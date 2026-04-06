#!/bin/bash
# Deploy pcp-to-cc to Google Cloud Run.
# Run this from the project root: ./deploy.sh
# After deploying, automatically runs set-env-vars.sh to update all env vars.
#
# Prerequisites:
#   gcloud auth login
#   gcloud config set project YOUR_PROJECT_ID

# Refresh credentials via browser if needed (avoids terminal password prompt)
gcloud auth print-access-token --account=office2@4thu.org > /dev/null 2>&1 || \
  gcloud auth login --account=office2@4thu.org

gcloud run deploy pcp-to-cc \
  --source . \
  --region us-east1 \
  --platform managed \
  --allow-unauthenticated \
  --clear-base-image \
  --service-account "pcp-to-cc-sa@pcp-to-cc.iam.gserviceaccount.com" \
  --project pcp-to-cc


echo ""
echo "Deploy complete. Updating environment variables..."
./set-env-vars.sh
