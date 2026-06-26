#!/bin/bash
# Deploy weekly report to Cloud Run.
# Run this from the project root: ./deploy_weekly.sh
# Authentication done through office2@4thu.org.  You'll need the password.

# Refresh credentials via browser if needed (avoids terminal password prompt)
gcloud auth print-access-token --account=office2@4thu.org > /dev/null 2>&1 || \
  gcloud auth login --account=office2@4thu.org

gcloud run jobs deploy pcp-workflow-report \
  --source . --region us-east1 --project pcp-to-cc \
  --service-account pcp-to-cc-sa@pcp-to-cc.iam.gserviceaccount.com \
  --set-env-vars CLOUD_PROJECT_ID=pcp-to-cc,TZ=America/New_York \
  --command uv --args run,python,pcp_workflow_report.py

echo ""
echo "Deploy complete. To run the report now: ./run_weekly_report.sh"
