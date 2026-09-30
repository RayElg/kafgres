# Development image: builds and runs kafgres in one image.
#
# `docker compose build` is the only way to verify the extension compiles. `cargo check`
# skips the pgrx/Postgres link step and passes on code that cannot load, which is why
# this gate exists.

# PostgreSQL major to build for and run, 13 through 18. Default 16 is the release version.
ARG PG_MAJOR=16

# Pinned for reproducible builds, like `cargo-pgrx` below.
FROM rust:1.98.0-bookworm AS builder
ARG PG_MAJOR

RUN apt-get update && apt-get install -y \
    lsb-release \
    wget \
    gnupg \
    && sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' \
    && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && apt-get update \
    && apt-get install -y \
    clang \
    gcc \
    libclang-dev \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Pinned to the version in extension/Cargo.toml. Installed before the PG headers so
# this layer is shared across every PG_MAJOR.
RUN cargo install --locked cargo-pgrx --version 0.16.1

RUN apt-get update && apt-get install -y postgresql-server-dev-${PG_MAJOR} \
    && rm -rf /var/lib/apt/lists/*

# Use the packaged Postgres rather than compiling one from source.
RUN cargo pgrx init --pg${PG_MAJOR} /usr/lib/postgresql/${PG_MAJOR}/bin/pg_config

WORKDIR /usr/src/kafgres

# The codec crate is a path dependency of the extension, so it has to come along.
# Schemas are copied too: lib.rs embeds codec/KAFKA_VERSION via include_str!, so the
# running extension can report the wire version it was built against.
COPY codec/Cargo.toml       ./codec/Cargo.toml
COPY codec/src              ./codec/src
COPY codec/KAFKA_VERSION    ./codec/KAFKA_VERSION

COPY extension/Cargo.toml    ./extension/Cargo.toml
COPY extension/Cargo.lock    ./extension/Cargo.lock
COPY extension/kafgres.control ./extension/kafgres.control
COPY extension/src           ./extension/src
# Version upgrade scripts (kafgres--X--Y.sql); `cargo pgrx package` ships them alongside
# the generated schema.
COPY extension/sql           ./extension/sql

WORKDIR /usr/src/kafgres/extension
# Compile-check the `#[cfg(test)]` modules, which nothing else does.
#
# `check --tests` rather than `test --no-run`: a pgrx extension's test binary links against
# the Postgres backend's symbols (`errstart`, `CurrentMemoryContext`), which exist only
# inside a running backend, so anything that reaches the linker fails. Compilation is the
# part that can be gated here, and it is the part that rots.
#
# Test-only code can rot invisibly because nothing links it: two such errors sat in
# `#[cfg(test)]` code for a long stretch of this project's history. The gate therefore
# goes in the build the project mandates rather than in a CI job someone has to
# remember to add.
RUN cargo check --tests --no-default-features --features pg${PG_MAJOR} && \
    cargo pgrx package --no-default-features --features pg${PG_MAJOR} \
        --pg-config /usr/lib/postgresql/${PG_MAJOR}/bin/pg_config && \
    mkdir -p /app/dist && \
    cp -r target/release/kafgres-pg${PG_MAJOR}/* /app/dist/ && \
    # Only /app/dist is copied onward; Dockerfile.pgrx-test derives from this image, so
    # without this every test run re-extracts the whole build tree from the layer.
    # The tests' own artifacts live in the cargo cache volume, not here.
    rm -rf /usr/src/kafgres/extension/target

FROM postgres:${PG_MAJOR}-bookworm
ARG PG_MAJOR

COPY --from=builder /app/dist/usr/share/postgresql/${PG_MAJOR}/extension /usr/share/postgresql/${PG_MAJOR}/extension
COPY --from=builder /app/dist/usr/lib/postgresql/${PG_MAJOR}/lib /usr/lib/postgresql/${PG_MAJOR}/lib

# kafgres must be preloaded: background workers can only be registered from _PG_init,
# which runs at postmaster start.
RUN echo "shared_preload_libraries = 'kafgres'" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "kafgres.advertised_host = '127.0.0.1'" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "log_min_messages = warning" >> /usr/share/postgresql/postgresql.conf.sample && \
    # The CDC source is a logical replication slot, which `replica` cannot serve.
    # `logical` is a superset of `replica`, so physical streaming replication is
    # unaffected. Written to the versioned sample as well, for the reason given below
    # the pg_hba lines.
    echo "wal_level = logical" >> /usr/share/postgresql/postgresql.conf.sample && \
    for f in /usr/share/postgresql/*/postgresql.conf.sample; do \
        echo "wal_level = logical" >> "$f"; \
    done && \
    # An output plugin is loaded on behalf of any REPLICATION user, so since 16.15 the
    # library must be allowlisted — otherwise creating the slot fails with "may not be
    # used as an output plugin". The default is `pgoutput, test_decoding`, which is worth
    # noting: the shortcut this project rejected needs no configuration at all.
    # PG 13 has no such setting; 14 through 18 do. An unknown setting refuses to start
    # the server, so write it only where this server reports it.
    settings="$(gosu postgres postgres --describe-config)" && \
    if printf '%s\n' "$settings" | grep -q '^output_plugin_libraries'; then \
        echo "output_plugin_libraries = 'pgoutput, test_decoding, kafgres'" \
            >> /usr/share/postgresql/postgresql.conf.sample; \
        for f in /usr/share/postgresql/*/postgresql.conf.sample; do \
            echo "output_plugin_libraries = 'pgoutput, test_decoding, kafgres'" >> "$f"; \
        done; \
    fi && \
    echo "local   all             all                                     trust" >> /usr/share/postgresql/pg_hba.conf.sample && \
    echo "host    all             all             all                     trust" >> /usr/share/postgresql/pg_hba.conf.sample && \
    # The failover rig takes a base backup and streams from here. `all` does not cover
    # the `replication` pseudo-database, so this is a separate line.
    echo "host    replication     all             all                     trust" >> /usr/share/postgresql/pg_hba.conf.sample && \
    # And the version-scoped path, which is the one PG16 actually reads. Writing only to
    # the unversioned sample silently did nothing: the standby's pg_basebackup failed with
    # "no pg_hba.conf entry for replication connection", and the primary's live pg_hba had
    # only the default localhost replication lines.
    for f in /usr/share/postgresql/*/pg_hba.conf.sample; do \
        echo "local   all             all                                     trust" >> "$f"; \
        echo "host    all             all             all                     trust" >> "$f"; \
        echo "host    replication     all             all                     trust" >> "$f"; \
    done

COPY docker/standby-entrypoint.sh /usr/local/bin/standby-entrypoint.sh
RUN chmod +x /usr/local/bin/standby-entrypoint.sh

RUN echo '#!/bin/bash' > /docker-entrypoint-initdb.d/01-create-extension.sh && \
    echo 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CREATE EXTENSION IF NOT EXISTS kafgres;"' >> /docker-entrypoint-initdb.d/01-create-extension.sh && \
    chmod +x /docker-entrypoint-initdb.d/01-create-extension.sh

# 5432 PostgreSQL | 9092 Kafka
EXPOSE 5432 9092
