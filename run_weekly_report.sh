#!/bin/bash
# Execute the weekly workflow report on Cloud Run without redeploying.
# Run this from the project root: ./run_report.sh
# Only run deploy_weekly.sh when pcp_workflow_report.py code has changed.

# Refresh credentials via browser if needed (avoids terminal password prompt)
gcloud auth print-access-token --account=office2@4thu.org > /dev/null 2>&1 || \
  gcloud auth login --account=office2@4thu.org

gcloud run jobs execute pcp-workflow-report \
  --region us-east1 --project pcp-to-cc --wait
