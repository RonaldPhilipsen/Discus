#!/usr/bin/env bash
# Check every supported build feature combination, and make sure that the rejected ones fail.
# Spec §15.5. Run it with `mise run build-matrix`.
set -euo pipefail
cd "$(dirname "$0")/.."

base="tls-aws-lc,roots-system"
failures=0

check() {
  local features="$1"
  if cargo check -q -p discus --all-targets --no-default-features --features "$features" 2>/dev/null; then
    echo "ok      $features"
  else
    echo "FAILED  $features (expected the build to pass)"
    failures=$((failures + 1))
  fi
}

reject() {
  local features="$1" message="$2" out
  if out=$(cargo check -q -p discus --no-default-features --features "$features" 2>&1); then
    echo "FAILED  $features (expected the build to fail)"
    failures=$((failures + 1))
  elif grep -qF "$message" <<<"$out"; then
    echo "ok      $features (rejected)"
  else
    echo "FAILED  $features (rejected without the message: $message)"
    failures=$((failures + 1))
  fi
}

if cargo check -q -p discus --all-targets 2>/dev/null; then
  echo "ok      default features"
else
  echo "FAILED  default features"
  failures=$((failures + 1))
fi

# Every optional subsystem off, then each one off alone.
check "$base"
check "$base,oauth2,k8s"
check "$base,otel,k8s"
check "$base,otel,oauth2"
check "$base,otel,oauth2-introspection,k8s"

# Each TLS backend and each trust root feature. The base case above covers tls-aws-lc.
check "tls-ring,roots-system"
check "tls-openssl,roots-system"
check "tls-aws-lc,roots-webpki"

# Rejected combinations.
reject "roots-system" "enable exactly one TLS backend feature"
reject "tls-aws-lc,tls-ring,roots-system" "enable only one TLS backend feature"
reject "tls-aws-lc" "enable exactly one trust root feature"
reject "tls-aws-lc,roots-system,roots-webpki" "enable only one trust root feature"

if [ "$failures" -ne 0 ]; then
  echo "$failures build matrix case(s) failed"
  exit 1
fi
echo "build matrix passed"
