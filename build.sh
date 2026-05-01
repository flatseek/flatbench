#!/usr/bin/env bash
# Build static site for Vercel: copies report_viewer.html as index.html,
# collects all benchmark JSON/MD files, and emits an index.json manifest
# so the viewer can list reports without relying on directory listings.
set -euo pipefail

PUB="public"
rm -rf "$PUB"
mkdir -p "$PUB/output"

cp report_viewer.html "$PUB/index.html"
[ -f favicon.svg ] && cp favicon.svg "$PUB/" || true

shopt -s nullglob
json_files=(output/*.json)
md_files=(output/*.md)
shopt -u nullglob

if (( ${#json_files[@]} )); then
  cp "${json_files[@]}" "$PUB/output/"
fi
if (( ${#md_files[@]} )); then
  cp "${md_files[@]}" "$PUB/output/"
fi

python3 - <<'PY'
import json, os
out = "public/output"
files = sorted(
    [f for f in os.listdir(out) if f.endswith(".json") and f != "index.json"],
    reverse=True,
)
with open(os.path.join(out, "index.json"), "w") as fh:
    json.dump({"files": files, "count": len(files)}, fh)
print(f"Manifest: {len(files)} reports")
PY

echo "Built $(ls -1 "$PUB"/output/*.json | wc -l | tr -d ' ') JSON files into $PUB/"
