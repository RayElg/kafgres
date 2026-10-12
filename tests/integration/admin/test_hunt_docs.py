"""docs/configuration.md against pg_settings: every setting documented, nothing documented
that does not exist, and the documented context and default are the server's."""

import pathlib
import re

from conftest import sql
from support.docker import REPO

DOC = pathlib.Path(REPO) / "docs" / "configuration.md"
CONTEXT = {"sighup": "reload", "postmaster": "restart"}
UNITS = {"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30}


def documented():
    rows = {}
    for line in DOC.read_text().splitlines():
        m = re.match(r"^\| `(kafgres\.[a-z_]+)` \| (\w+) \| ([^|]*) \|", line)
        if m:
            rows[m[1]] = (m[2], m[3].strip())
    return rows


def normalise(default):
    """The documented default as pg_settings spells its boot value; "empty" is ""."""
    value = re.sub(r"\s*\(.*\)$", "", default).strip("`")
    if value == "empty":
        return ""
    m = re.fullmatch(r"(\d+) (KiB|MiB|GiB)", value)
    if m:
        return str(int(m[1]) * UNITS[m[2]])
    return value


def test_the_configuration_reference_matches_pg_settings():
    server = {}
    for row in sql("SELECT name || '|' || context || '|' || coalesce(boot_val, '') "
                   "FROM pg_settings WHERE name LIKE 'kafgres.%' ORDER BY 1").splitlines():
        name, context, boot = row.split("|", 2)
        server[name] = (context, boot)
    docs = documented()

    assert sorted(set(server) - set(docs)) == [], "settings missing from configuration.md"
    assert sorted(set(docs) - set(server)) == [], "documented settings that do not exist"
    for name, (context, boot) in server.items():
        doc_context, doc_default = docs[name]
        assert CONTEXT.get(context, context) == doc_context, (
            f"{name}: documented {doc_context}, the server's context is {context}")
        # An empty boot value is filled in by the code; its documented default is not checkable here.
        if boot != "":
            assert normalise(doc_default) == boot, (
                f"{name}: documented default {doc_default}, the server's is {boot!r}")
