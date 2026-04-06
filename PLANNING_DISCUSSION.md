# PCP → Constant Contact: Planning Discussion & Decisions
Date: 2026-04-03
Status: Code complete — waiting on credentials to deploy

When you come back with credentials, just say "check your memory and continue" 
and I'll pick up exactly at the next steps.

---

## Goal

Automatically add new Planning Center People (PCP) profiles to a newsletter list in
Constant Contact (CC) whenever someone fills out a PCP form with `newsletter_opt_in = Yes`.

---

## Feasibility: Yes

Both PCP and Constant Contact expose REST APIs. This is a straightforward webhook integration.

| Capability | PCP | Constant Contact |
|---|---|---|
| REST API | Yes (api.planningcenteronline.com/people/v2) | Yes (api.cc.email/v3) |
| Auth | Personal Access Token (HTTP Basic Auth) | Bearer token (OAuth) |
| Detect new people | Webhooks on person.created | — |
| Add contact to list | — | POST /v3/contacts with list_memberships |

---

## Key Decisions

### Trigger: PCP Webhooks (not polling)
- Polling on a schedule was considered — requires laptop to be on and awake
- Webhooks are event-driven, no laptop required, near real-time
- PCP fires `person.created` automatically when a form creates a new person

### Filter: Form opt-in field
- A field called `newsletter_opt_in` is on the PCP form
- When the person checks "Yes", the webhook fires and our code adds them to CC
- No staff workflow steps needed — the person opts in themselves
- Profiles without email (e.g. children) are skipped gracefully

### Multiple lists: Supported via CC_LIST_RULES
- `config.py` contains a `CC_LIST_RULES` list of dicts
- Each rule maps a PCP field value to one or more CC list UUIDs
- To add a new list: add a new dict to the list — no logic changes needed

### Where it runs: New Google Cloud Run service
- Separate from existing `cfcg-an-webhook` (different organization)
- Serverless — only runs when PCP sends a webhook; free tier covers church volume
- No laptop required

### Code template: cfcg-an-webhook
- Existing webhook at `/Users/Denise/Library/CloudStorage/Dropbox/Postcard Files/ROVPrograms/cfcg-an-webhook`
- New project mirrors it exactly: Flask, config.py, deploy.sh + set-env-vars.sh, GCP Secret Manager, loguru, uv

---

## Runtime Flow

```
Person fills out PCP form with newsletter_opt_in = Yes
  ↓
PCP fires person.created webhook to Cloud Run URL
  ↓
Flask /webhook receives payload, extracts person_id
  ↓
Calls PCP API: GET /people/v2/people/{id}?include=emails,field_data
  ↓
Parses: first_name, last_name, email, custom field values
  ↓
No email? → skip and log
  ↓
apply_rules(): checks if newsletter_opt_in field == "Yes"
  ↓
No match? → skip and log
  ↓
TEST_MODE=true? → log what would happen, skip CC call
  ↓
add_to_cc(): POST https://api.cc.email/v3/contacts
             with list_memberships = [CC_NEWSLETTER_LIST_ID]
  ↓
Return 200 OK
```

---

## Configuration (CC_LIST_RULES in config.py)

```python
CC_LIST_RULES = [
    {
        "description": "Newsletter opt-in → newsletter list",
        "pcp_field":   "newsletter_opt_in",
        "pcp_value":   "Yes",
        "cc_lists":    [os.environ.get("CC_NEWSLETTER_LIST_ID", "")],
    },
    # Add more rules here for additional lists — no other code changes needed
    # Example:
    # {
    #     "description": "Events opt-in → events list",
    #     "pcp_field":   "events_opt_in",
    #     "pcp_value":   "Yes",
    #     "cc_lists":    [os.environ.get("CC_EVENTS_LIST_ID", "")],
    # },
]
```

---

## Files Built

| File | Purpose |
|---|---|
| `pcp_to_cc/config.py` | All env vars, CC_LIST_RULES, PCP_FIELD_IDS |
| `pcp_to_cc/main.py` | Flask app — /webhook, /health, /settings |
| `pyproject.toml` | Dependencies (flask, gunicorn, requests, etc.) |
| `.env.example` | Template — copy to .env and fill in |
| `Dockerfile` | Cloud Run container |
| `deploy.sh` | Full deploy (fill in YOUR_PROJECT_ID) |
| `set-env-vars.sh` | Update config without redeploying |
| `tests/conftest.py` | Fixtures, Secret Manager mock |
| `tests/test_main.py` | 17 tests — all passing |
| `tests/payloads/person_created_webhook.json` | Sample PCP webhook payload |
| `test_local.py` | Manual local testing script |

**Test results: 17/17 passing** (2026-04-03)

---

## Credentials Needed (not yet obtained)

| Credential | Where to get it | How to store |
|---|---|---|
| PCP Application ID | PCP Developer Settings → Personal Access Tokens | GCP Secret Manager as `PCP_APP_ID` |
| PCP Secret | Same as above | GCP Secret Manager as `PCP_SECRET` |
| CC API Key | developer.constantcontact.com → your app | GCP Secret Manager as `CC_API_KEY` |
| CC API Secret | Same app page | GCP Secret Manager as `CC_API_SECRET` |
| CC Access Token | Generated during initial OAuth flow | GCP Secret Manager as `CC_ACCESS_TOKEN` (auto-refreshed) |
| CC Refresh Token | Generated during initial OAuth flow | GCP Secret Manager as `CC_REFRESH_TOKEN` (never changes) |
| PCP field definition ID | Set LOG_PAYLOADS=true, submit test person, read logs | `.env` as `PCP_NEWSLETTER_TRIGGER_FIELD_ID`, then `set-env-vars.sh` |
| CC list UUID | CC account → Contacts → Lists → click list → UUID in URL | `.env` as `CC_NEWSLETTER_LIST_ID`, then `set-env-vars.sh` |

---

## Deployment Checklist

### One-time GCP setup
- [ ] Create GCP project (or reuse existing)
- [ ] Create service account `pcp-to-cc-sa` with `roles/secretmanager.secretAccessor`
- [ ] Store the 3 secrets above in GCP Secret Manager
- [ ] Fill in `YOUR_PROJECT_ID` in `deploy.sh` and `set-env-vars.sh`

### Local testing
- [ ] `cp .env.example .env` → fill in `CLOUD_PROJECT_ID`
- [ ] `gcloud auth application-default login`
- [ ] `uv sync`
- [ ] `python pcp_to_cc/main.py` (start server)
- [ ] `python test_local.py` (send test webhook in another terminal)
- [ ] With `LOG_PAYLOADS=true`: submit real test person in PCP, read logs to find field definition ID

### Deploy
- [ ] Fill in `PCP_NEWSLETTER_TRIGGER_FIELD_ID` and `CC_NEWSLETTER_LIST_ID` in `set-env-vars.sh`
- [ ] Confirm `TEST_MODE=true` in `set-env-vars.sh` for first deploy
- [ ] Run `./deploy.sh`
- [ ] Register Cloud Run URL in PCP as webhook endpoint for `person.created`
- [ ] Submit test person in PCP → confirm they appear in CC list
- [ ] Set `TEST_MODE=false` in `set-env-vars.sh` → run `./set-env-vars.sh` to go live

---

## Important Notes

- **Duplicate safety:** CC's `/v3/contacts` upsert endpoint never creates duplicates — safe to run repeatedly
- **CC token refresh:** CC access tokens expire (~24 hrs). The app automatically refreshes on a 401, stores the new token in Secret Manager, and retries. Requires `CC_REFRESH_TOKEN`, `CC_API_KEY`, `CC_API_SECRET` in Secret Manager. Choose **Long Lived Refresh Tokens** in CC OAuth settings so `CC_REFRESH_TOKEN` never needs updating.
- **PCP webhook payload format:** Assumed based on PCP API docs; update `_extract_person_id()` in `main.py` if the real format differs
- **Adding more lists:** Edit only `CC_LIST_RULES` in `config.py` — no logic changes needed
- **Test mode:** `TEST_MODE=true` skips the CC API call entirely and logs what would happen — use this for all initial testing
- **Test results:** 21/21 passing (2026-04-04)
