#!/bin/bash
set -e
echo "Installing Playwright browsers..."
python -m playwright install --with-deps chromium
