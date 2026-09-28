#!/bin/sh
# Builds the server's backend as LLVM bitcode at -O0 -g, from upstream source, into
# /opt/ffi-bitcode/postgres. -O0 keeps struct field accesses typed (packaged bitcode is
# -O2, mostly byte offsets), so pointers resolve by field; -g adds file and line per call.
#
#   build-bitcode.sh <llvm major> <pg_config>
set -eu
llvm="$1"
pgc="$2"
ver="$("$pgc" --version | awk '{print $2}')"
out=/opt/ffi-bitcode

apt-get update
apt-get install -y --no-install-recommends "clang-$llvm" "llvm-$llvm-dev" make perl bison flex \
    libc6-dev curl ca-certificates bzip2 pkg-config
cd /tmp
curl -fsSL "https://ftp.postgresql.org/pub/source/v$ver/postgresql-$ver.tar.bz2" | tar xj
cd "postgresql-$ver"
./configure --with-llvm LLVM_CONFIG="llvm-config-$llvm" CLANG="clang-$llvm" CC="clang-$llvm" \
    CFLAGS="-O0" --without-icu --without-readline --without-zlib --prefix=/tmp/unused >/tmp/configure.log 2>&1 \
    || { tail -40 /tmp/configure.log; exit 1; }
# Linking may fail on this minimal toolchain; only the bitcode matters, checked below.
make -C src/backend -k -j"$(nproc)" BITCODE_CFLAGS="-O0 -g" >/tmp/make.log 2>&1 || true
mkdir -p "$out/postgres"
(cd src/backend && find . -name '*.bc' | tar cf - -T -) | tar xf - -C "$out/postgres"
n="$(find "$out/postgres" -name '*.bc' | wc -l)"
if [ "$n" -lt 300 ]; then
    grep -m 20 -i error /tmp/make.log || true
    echo "only $n bitcode files built" >&2
    exit 1
fi
echo "PostgreSQL $ver, backend bitcode at -O0 -g from upstream source, $n modules" > "$out/VERSION"

cd /
rm -rf "/tmp/postgresql-$ver" /tmp/*.log
apt-get purge -y "clang-$llvm" "llvm-$llvm-dev" bison flex libc6-dev pkg-config
apt-get autoremove -y
rm -rf /var/lib/apt/lists/*
