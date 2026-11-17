#!/usr/bin/env bash
# Test certificates for the TLS and mTLS suite. Regenerate with:
#   bash tests/tls/generate.sh
#
# Not checked in — a repository with a private key in it teaches the wrong habit, and
# these expire. The TLS tests skip themselves if this has not been run.
set -euo pipefail
cd "$(dirname "$0")"

openssl req -x509 -newkey rsa:2048 -keyout ca.key -out ca.crt -days 3650 -nodes \
    -subj "/CN=kafgres-test-ca" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null

openssl req -newkey rsa:2048 -keyout server.key -out server.csr -nodes \
    -subj "/CN=localhost" 2>/dev/null
# The SAN is what the client actually checks; a CN-only certificate is rejected by
# every modern TLS stack including rustls.
# Python 3.13 and later verify strictly: the CA needs keyUsage, a leaf needs keyIdentifiers.
printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid\n' > server.ext
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out server.crt -days 3650 -extfile server.ext 2>/dev/null

openssl req -newkey rsa:2048 -keyout client.key -out client.csr -nodes \
    -subj "/CN=alice/O=kafgres" 2>/dev/null
printf 'extendedKeyUsage=clientAuth\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid\n' > client.ext
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out client.crt -days 3650 -extfile client.ext 2>/dev/null

# A client signed by a *different* CA, for the rejection test.
openssl req -x509 -newkey rsa:2048 -keyout rogue-ca.key -out rogue-ca.crt -days 3650 \
    -nodes -subj "/CN=kafgres-rogue-ca" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
openssl req -newkey rsa:2048 -keyout rogue.key -out rogue.csr -nodes \
    -subj "/CN=mallory/O=elsewhere" 2>/dev/null
openssl x509 -req -in rogue.csr -CA rogue-ca.crt -CAkey rogue-ca.key -CAcreateserial \
    -out rogue.crt -days 3650 -extfile client.ext 2>/dev/null

chmod 644 ./*.crt ./*.key
echo "wrote $(ls ./*.crt ./*.key | wc -l) files to $(pwd)"
