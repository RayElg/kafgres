"""The advertised API surface, compared with the reference broker's.

franz-go and the Java client pick request versions from it.
"""

import subprocess

import pytest

from harness import KAFGRES, KAFKA_IMAGE, REFERENCE, has_reference


@pytest.mark.skipif(
    not has_reference(),
    reason="reference broker not running: docker compose --profile conformance up -d kafka",
)
def test_the_advertised_api_surface_is_a_subset_of_kafkas():
    """Every API we advertise, Kafka advertises with a range that contains ours."""
    def surface(broker):
        out = subprocess.run(
            ["docker", "run", "--rm", "--network", "host", KAFKA_IMAGE,
             "/opt/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", broker],
            capture_output=True, text=True, timeout=180,
        )
        assert out.returncode == 0, out.stderr
        found = {}
        for line in out.stdout.splitlines():
            line = line.strip().rstrip(",")
            if "(" not in line or "):" not in line:
                continue
            _name, rest = line.split("(", 1)
            key, rest = rest.split("):", 1)
            if "UNSUPPORTED" in rest:
                continue
            span = rest.split("[")[0].strip()
            if " to " in span:
                lo, hi = span.split(" to ")
            else:
                lo = hi = span
            found[int(key)] = (int(lo), int(hi.strip()))
        return found

    ours, theirs = surface(KAFGRES), surface(REFERENCE)
    assert len(ours) >= 26, f"parsed {len(ours)} APIs from kafgres; the parser is broken"
    assert len(theirs) >= 70, f"parsed {len(theirs)} APIs from kafka; the parser is broken"
    wider = {
        key: (rng, theirs.get(key))
        for key, rng in ours.items()
        if key not in theirs or rng[1] > theirs[key][1] or rng[0] < theirs[key][0]
    }
    assert not wider, (
        "we advertise versions the reference broker does not:\n"
        + "\n".join(f"  api {k}: kafgres {v[0]}, kafka {v[1]}" for k, v in sorted(wider.items()))
    )
