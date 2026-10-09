"""Objects of the caller's that share a name with the extension's must not stand in for
them inside kafgres_produce(): a `kafgres_markers` the caller owns would pass the INSERT
check that decides whether it may produce at all."""

import pytest

from conftest import sql
from test_sql_produce_permissions import (  # noqa: F401 (fixture)
    ROLE, TOPIC, as_role, log_end, producer_role,
)

COLS = "(topic_id oid, partition int, base_offset bigint, last_offset bigint, bytes int)"

SHADOWS = {
    "schema-first-on-path": (
        f"CREATE SCHEMA IF NOT EXISTS {ROLE}_shadow; "
        f"CREATE TABLE {ROLE}_shadow.kafgres_markers {COLS}; "
        f"SET search_path = {ROLE}_shadow, public; "
    ),
    "temp-table": f"CREATE TEMP TABLE kafgres_markers {COLS}; ",
    "temp-first-on-path": (
        f"CREATE TEMP TABLE kafgres_markers {COLS}; "
        "SET search_path = pg_temp, public; "
    ),
}


@pytest.mark.parametrize("shadow", SHADOWS)
def test_a_shadowing_markers_table_does_not_grant_produce(producer_role, shadow):
    sql(f"GRANT CREATE ON DATABASE postgres TO {ROLE}")
    try:
        before = log_end()
        ok, out = as_role(SHADOWS[shadow] + f"SELECT kafgres_produce('{TOPIC}', 'k', 'x')")
        assert not ok and "permission denied to produce" in out, (
            f"{shadow}: the caller's own kafgres_markers let it produce: {out}")
        assert log_end() == before, f"{shadow}: the refused record reached the log"
    finally:
        sql(f"DROP SCHEMA IF EXISTS {ROLE}_shadow CASCADE")
        sql(f"REVOKE CREATE ON DATABASE postgres FROM {ROLE}")
