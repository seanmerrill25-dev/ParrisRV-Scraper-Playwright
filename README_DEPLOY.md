# Streamlit Cloud Deploy (Playwright-only) — v2

## Files at repo root
- app.py
- requirements.txt
- .python-version        # force Python 3.11.9
- runtime.txt            # backup hint for Python version
- packages.txt           # apt packages (curl, wget)
- postSetup.sh           # installs Chromium
- README_DEPLOY.md

## Why this version?
Your build logs showed Python 3.13.6. Pandas wheels for 3.13 often trigger slow source builds.
This bundle forces Python 3.11.9 to ensure wheels and Playwright compatibility.

## Deploy flow
1) push files to repo root
2) Streamlit Cloud installs requirements
3) Cloud executes postSetup.sh (installs Chromium)
