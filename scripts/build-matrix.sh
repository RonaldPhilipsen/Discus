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
# tls-openssl links the system OpenSSL, so it needs the development headers.
if pkg-config --exists openssl 2>/dev/null; then
  check "tls-openssl,roots-system"
else
  echo "skipped tls-openssl,roots-system (OpenSSL development headers not found)"
fi
check "tls-aws-lc,roots-webpki"

# A binary without a subsystem refuses a configuration that needs it, and names the feature.
if cargo test -q -p discus --no-default-features --features "$base" --test security_config \
  >/dev/null 2>&1; then
  echo "ok      $base refuses configurations that need a missing feature"
else
  echo "FAILED  $base refuses configurations that need a missing feature"
  failures=$((failures + 1))
fi

# Introspection is off by default, so `mise run test` does not run its tests.
if cargo test -q -p discus --features oauth2-introspection --test introspection \
  >/dev/null 2>&1; then
  echo "ok      oauth2-introspection tests"
else
  echo "FAILED  oauth2-introspection tests"
  failures=$((failures + 1))
fi

# Telemetry with the other TLS backends and trust roots (TLS to the OTLP collector).
check "tls-ring,roots-webpki,otel"
if pkg-config --exists openssl 2>/dev/null; then
  check "tls-openssl,roots-system,otel"
else
  echo "skipped tls-openssl,roots-system,otel (OpenSSL development headers not found)"
fi

# Without otel, no OpenTelemetry crate is compiled into the binary (spec §15.5).
if cargo tree -q -p discus -e normal --no-default-features --features "$base" | grep -q opentelemetry; then
  echo "FAILED  $base pulls in an OpenTelemetry crate without otel"
  failures=$((failures + 1))
else
  echo "ok      $base has no OpenTelemetry crate"
fi

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
