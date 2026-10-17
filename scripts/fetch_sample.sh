#!/usr/bin/env bash
# Download a Nasdaq ITCH 5.0 sample day into data/ (git-ignored), resuming and retrying as needed, then verify it.
#
#   scripts/fetch_sample.sh                          # 12302019.NASDAQ_ITCH50.gz (2019-12-30, ~3.5 GB, ~269M messages)
#   scripts/fetch_sample.sh 01302019.NASDAQ_ITCH50.gz
#
# The server drops long transfers, so the download is resumed (curl -C -) until the size matches what the server
# reports, then checked with `gzip -t`. Browse https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/ for the other days.
set -euo pipefail

file="${1:-12302019.NASDAQ_ITCH50.gz}"
url="https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/${file}"
dir="$(cd "$(dirname "$0")/.." && pwd)/data"
mkdir -p "$dir"
cd "$dir"

size_of() { stat -f%z "$1" 2>/dev/null || stat -c%s "$1" 2>/dev/null || echo 0; }

hdr=$(curl -sIL --max-time 20 "$url" || true)
status=$(printf '%s\n' "$hdr" | awk '/^HTTP/ {s=$2} END {print s}')
if [ -z "$status" ]; then echo "no answer from emi.nasdaq.com within 20 s (network down, or the file name does not exist): check https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/"; exit 1; fi
if [ "$status" != "200" ]; then echo "the server answered HTTP ${status} for ${file}: check the file name at https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/"; exit 1; fi
want=$(printf '%s\n' "$hdr" | awk 'tolower($1)=="content-length:" {gsub(/\r/,"",$2); n=$2} END {print n+0}')
if [ "${want:-0}" -le 0 ]; then echo "the server did not report a size for ${file}"; exit 1; fi
echo "downloading ${file}: ${want} bytes"

for attempt in $(seq 1 200); do
  have=$(size_of "$file")
  [ "$have" -ge "$want" ] && break
  curl -fL -C - -sS --connect-timeout 30 --speed-limit 20000 --speed-time 60 -o "$file" "$url" || sleep 5
done

have=$(size_of "$file")
if [ "$have" -ne "$want" ]; then echo "incomplete: ${have}/${want} bytes; run the script again to resume"; exit 1; fi
gzip -t "$file" && echo "ok: data/${file} (${have} bytes, gzip integrity verified)"
