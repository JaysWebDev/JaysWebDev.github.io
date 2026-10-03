#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Regenerate resume.pdf from resume/index.html — the single source of truth.
# The HTML already carries print-tuned @media print / @page CSS; this just
# renders it headless so the PDF can never drift from the web version.
#
# Usage:  ./build-pdf.sh      (run from anywhere)
# Requires: google-chrome / chromium (headless). No network needed — the page
# is fully self-contained (inline <style>, no external assets).
# ---------------------------------------------------------------------------
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="$DIR/index.html"
OUT="$DIR/resume.pdf"

CHROME="$(command -v google-chrome || command -v google-chrome-stable || command -v chromium || command -v chromium-browser || true)"
if [ -z "$CHROME" ]; then
  echo "error: no Chrome/Chromium found on PATH" >&2
  exit 1
fi

"$CHROME" --headless=new --disable-gpu --no-sandbox \
  --no-pdf-header-footer \
  --print-to-pdf="$OUT" "file://$SRC" 2>/dev/null

echo "Wrote $OUT  ($(du -h "$OUT" | cut -f1)) from $SRC"
