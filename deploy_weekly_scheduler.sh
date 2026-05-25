#!/bin/bash
# Create (or update) the Cloud Scheduler job that triggers the weekly report.
# Run once from the project root: ./deploy_weekly_scheduler.sh
#
# Schedule: Mondays at 6:00 AM Eastern.
# To change the day/time, edit SCHEDULE below (one place, used for both create and update).
#   cron format: "minute hour day-of-month month day-of-week"
#   Examples:
#     "0 6 * * 1"   → Monday    6:00 AM
#     "0 6 * * 5"   → Friday    6:00 AM
#     "0 8 * * 1"   → Monday    8:00 AM
#
# Prerequisites:
#   - pcp-workflow-report Cloud Run Job must already be deployed (deploy_weekly.sh)
#   - pcp-to-cc-sa must have roles/run.invoker on the job (one-time grant below)

SCHEDULE="0 6 * * 1"
#SCHEDULE="30 13 25 5 *"


# Refresh credentials via browser if needed (avoids terminal password prompt)
gcloud auth print-access-token --account=office2@4thu.org > /dev/null 2>&1 || \
  gcloud auth login --account=office2@4thu.org

# Grant the service account permission to invoke the Cloud Run Job (safe to re-run)
echo "Granting roles/run.invoker to pcp-to-cc-sa on the job..."
gcloud run jobs add-iam-policy-binding pcp-workflow-report \
  --region us-east1 --project pcp-to-cc \
  --member "serviceAccount:pcp-to-cc-sa@pcp-to-cc.iam.gserviceaccount.com" \
  --role "roles/run.invoker"

# Create the scheduler job (use 'update' if it already exists)
echo ""
echo "Creating Cloud Scheduler job..."
gcloud scheduler jobs create http pcp-workflow-report-weekly \
  --location us-east1 \
  --schedule "$SCHEDULE" \
  --time-zone "America/New_York" \
  --uri "https://us-east1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/pcp-to-cc/jobs/pcp-workflow-report:run" \
  --http-method POST \
  --oauth-service-account-email pcp-to-cc-sa@pcp-to-cc.iam.gserviceaccount.com \
  --project pcp-to-cc \
  || \
gcloud scheduler jobs update http pcp-workflow-report-weekly \
  --location us-east1 \
  --schedule "$SCHEDULE" \
  --time-zone "America/New_York" \
  --uri "https://us-east1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/pcp-to-cc/jobs/pcp-workflow-report:run" \
  --http-method POST \
  --oauth-service-account-email pcp-to-cc-sa@pcp-to-cc.iam.gserviceaccount.com \
  --project pcp-to-cc

echo ""
echo "Scheduler set up. Schedule: $SCHEDULE (America/New_York)"
echo "To trigger it manually right now:"
echo "  gcloud scheduler jobs run pcp-workflow-report-weekly --location us-east1 --project pcp-to-cc"
