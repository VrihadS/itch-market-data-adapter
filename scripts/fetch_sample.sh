#!/usr/bin/env bash
# Download a sample ITCH 5.0 day into data/ (not committed).
# Browse the directory first to see which files exist:
#   https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/
# Usage: scripts/fetch_sample.sh 01302019.NASDAQ_ITCH50.gz
set -euo pipefail
f="${1:?usage: $0 <filename.gz>}"
cd "$(dirname "$0")/../data"
curl -fL -O "https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/${f}"
echo "downloaded data/${f}"
