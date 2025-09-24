# Deploying to Streamlit Community Cloud

1) Push `app.py` and `requirements.txt` to GitHub.
2) In Streamlit Cloud, set **Post-install** command to:
```
python -m playwright install --with-deps chromium
```
3) Point the app to `app.py`. Done.

## Notes
- This app uses Playwright only (no Selenium). Chromium is installed during the build step via the post-install command above.
- Outputs match the local Selenium version: columns = `title, tagline, list_price, payments_from, payments_disclaimer, image_url`.
