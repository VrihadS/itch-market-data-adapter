#!/usr/bin/env bash
# Resume a partial sample download until complete. Usage: scripts/resume_download.sh 12302019.NASDAQ_ITCH50.gz <expected_bytes>
set -u
f="${1:?file}"; want="${2:?expected bytes}"
cd "$(dirname "$0")/../data"
for i in $(seq 1 200); do
  have=$(stat -f%z "$f" 2>/dev/null || stat -c%s "$f" 2>/dev/null || echo 0)
  [ "$have" -ge "$want" ] && break
  curl -fL -C - -sS --connect-timeout 30 --speed-limit 20000 --speed-time 60 \
       -o "$f" "https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/$f" || sleep 5
done
have=$(stat -f%z "$f" 2>/dev/null || stat -c%s "$f")
echo "size=$have expected=$want"
gzip -t "$f" && echo "gzip integrity OK"
