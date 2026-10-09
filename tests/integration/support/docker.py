"""Reach the compose stack from a test: the broker's Postgres container, and the repo root
that `docker compose` must run from."""

import pathlib
import subprocess

REPO = str(pathlib.Path(__file__).resolve().parents[3])

# The test-clients image and the broker's address on the host.
CLIENTS = "kafgres-clients"
BROKER = "127.0.0.1:9092"


def compose(*args, timeout=180, check=False):
    """`docker compose <args>` from the repo root. With `check`, a failure raises."""
    out = subprocess.run(
        ["docker", "compose", *args],
        capture_output=True, text=True, timeout=timeout, cwd=REPO,
    )
    if check and out.returncode != 0:
        raise RuntimeError(f"docker compose {args}: {out.stderr.strip()}")
    return out


def psql(query, timeout=60):
    """Run `query` in the broker's database; the whole CompletedProcess, errors included."""
    return compose("exec", "-T", "postgres", "psql", "-U", "postgres", "-d", "postgres",
                   "-tAc", query, timeout=timeout)


def query(sql_text, timeout=60):
    """`psql`, reduced to stdout. A failed statement reads as empty output, not an error."""
    return psql(sql_text, timeout).stdout.strip()


def sh(command, timeout=60):
    """Run a shell command inside the broker container."""
    return compose("exec", "-T", "postgres", "sh", "-c", command, timeout=timeout)
