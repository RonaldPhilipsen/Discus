#!/usr/bin/env bash
# Regenerate the Rust code for the FlatBuffers wire contract (spec section 13.2).
#
# usage: scripts/gen-wire.sh [--check]
#
# The generated code is checked in, so a build needs no flatc. With --check, the script generates
# into a temporary directory and fails when the checked-in code differs. `mise run lint` runs the
# check, and mise supplies the pinned flatc.
set -euo pipefail
cd "$(dirname "$0")/.."

schema=crates/discus-wire/schema/discus.fbs
out=crates/discus-wire/src/generated

if ! command -v flatc >/dev/null; then
  echo "flatc not found. Run this script through mise (mise run gen-wire) to get the pinned flatc."
  exit 1
fi

if [ "${1:-}" = "--check" ]; then
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT
  flatc --rust --gen-all -o "$tmp" "$schema"
  if diff -u "$out/discus_generated.rs" "$tmp/discus_generated.rs" >/dev/null; then
    echo "wire code is up to date"
  else
    echo "wire code is out of date: run mise run gen-wire"
    exit 1
  fi
else
  flatc --rust --gen-all -o "$out" "$schema"
  echo "generated $out/discus_generated.rs"
fi
