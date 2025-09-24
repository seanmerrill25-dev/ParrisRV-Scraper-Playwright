#!/bin/bash
set -euxo pipefail
echo "[postSetup] Python: $(python --version || true)"
echo "[postSetup] Pip list before playwright:"
pip list || true
echo "[postSetup] Installing Playwright browsers (chromium)"
python -m playwright install --with-deps chromium
echo "[postSetup] Done installing Playwright browsers"
