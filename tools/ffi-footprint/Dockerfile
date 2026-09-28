# Analysis layer over an extension image: adds llvm-dis matching the server's JIT LLVM,
# nm and python3; BITCODE=source also builds the backend bitcode at -O0 -g (build-bitcode.sh).
ARG BASE
FROM ${BASE}
ARG BITCODE=package
USER root
COPY build-bitcode.sh /usr/local/lib/ffi-footprint/build-bitcode.sh
RUN set -eu; \
    v="$(dpkg-query -W -f='${Package}\n' 'libllvm*' 2>/dev/null \
         | sed -n 's/^libllvm\([0-9][0-9]*\).*/\1/p' | sort -n | tail -1)"; \
    if [ -z "$v" ]; then echo "no libllvm in the base image: server built without JIT, no bitcode" >&2; exit 1; fi; \
    apt-get update; \
    apt-get install -y --no-install-recommends "llvm-$v" binutils python3; \
    ln -sf "/usr/bin/llvm-dis-$v" /usr/local/bin/llvm-dis; \
    if [ "$BITCODE" = source ]; then \
        pgc="$(ls /usr/lib/postgresql/*/bin/pg_config | sort -V | tail -1)"; \
        sh /usr/local/lib/ffi-footprint/build-bitcode.sh "$v" "$pgc"; \
    fi; \
    rm -rf /var/lib/apt/lists/*
