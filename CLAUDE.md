# Project: pcp_to_cc_root

## Shared utility libraries — CHECK THESE BEFORE WRITING NEW CODE

Two local libraries are installed in the venv and should be checked for existing implementations before writing new utility functions:

### bekgoogle — `/Users/Denise/Library/CloudStorage/Dropbox/PythonPrograms/bekgoogle`
Google API helpers. Covers:
- OAuth2 credentials (`get_creds`, `get_serviceaccount_creds`)
- Secret Manager CRUD (`google_secrets.py`)
- ADC re-auth (`ensure_adc_auth` — checks/refreshes gcloud ADC, opens browser if expired)
- Google Drive: upload, subfolder creation, file/folder ID lookup, permissions, trash
- Google Sheets: read ranges, append rows, upload Excel as Sheet
- Updated from GitHub via `gitupdater`

### uvbekutils — `/Users/Denise/Library/CloudStorage/Dropbox/PythonPrograms/uvbekutils`
General Python utilities. Covers:
- GUI dialogs (`pyautobek`: alert, confirm, confirm_with_file_link, etc.)
- Check here for any UI popup or user-interaction helpers before implementing your own
- Updated from GitHub via `gitupdater`

**When in doubt: read the source. Don't catalog every function here — that goes stale.**
