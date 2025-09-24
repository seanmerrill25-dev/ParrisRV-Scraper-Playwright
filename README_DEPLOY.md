# Streamlit Cloud Deploy (Playwright-only)

## Files required at repo root
- app.py
- requirements.txt
- runtime.txt           # pin Python (3.11 is safe with Playwright)
- packages.txt          # apt packages
- postSetup.sh          # installs Chromium for Playwright
- README_DEPLOY.md

## Build behavior
Streamlit Cloud now runs without a UI "post install" box. If a `postSetup.sh` file is present
at the repo root, Streamlit executes it after Python deps are installed. We use it to install
Playwright's Chromium with system deps.

## Commands run by Cloud
- pip install -r requirements.txt
- bash postSetup.sh

## Common gotchas
- If you see "No version of playwright==X.Y.Z", relax the pin in requirements or use a compatible Python.
  The included `runtime.txt` sets Python 3.11 which is broadly supported.
- If scraping stalls due to site defenses, reduce page count or add small random waits between loads.
