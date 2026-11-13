#!/usr/bin/env bash
# Make a test CA and a node certificate for localhost and 127.0.0.1 in a directory.
#
# usage: scripts/dev-cert.sh DIR
#
# Writes DIR/ca.pem, DIR/cert.pem, and DIR/key.pem. Every client uses TLS, so a local node needs
# a certificate. Clients trust DIR/ca.pem, for example `redis-cli --tls --cacert DIR/ca.pem`.
set -eu
dir=$1
mkdir -p "$dir"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 \
  -subj "/CN=discus dev CA" -keyout "$dir/ca-key.pem" -out "$dir/ca.pem" 2>/dev/null
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=localhost" \
  -keyout "$dir/key.pem" -out "$dir/node.csr" 2>/dev/null
printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' >"$dir/ext"
openssl x509 -req -in "$dir/node.csr" -CA "$dir/ca.pem" -CAkey "$dir/ca-key.pem" \
  -CAcreateserial -days 2 -extfile "$dir/ext" -out "$dir/cert.pem" 2>/dev/null
