#!/usr/bin/env bash
# Build report.md from head.md, findings.md, and the tables of results.jsonl.
set -eu
cd "$(dirname "$0")"
status=${1:-"Status: complete."}
python3 -I - "$status" <<'EOF'
import pathlib, sys
head = pathlib.Path("head.md").read_text()
head = head.replace("STATUS_LINE", sys.argv[1])
head = head.replace("FINDINGS", pathlib.Path("findings.md").read_text().strip())
pathlib.Path("report.md").write_text(head)
EOF
python3 -I ../../../scripts/bench-k8s-aggregate.py results.jsonl >> report.md
