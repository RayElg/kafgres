# Vendored Kafka message schemas

`codec/KAFKA_VERSION` holds the pinned tag and nothing else, so build tooling can read it
without parsing prose. Everything else about the pin lives here.

| | |
|---|---|
| Upstream | https://github.com/apache/kafka |
| Tag | `4.3.1` |
| Commit | `26b251a451ce941d3d7a55e6487bcb7f16b5ad48` |
| Source path | `clients/src/main/resources/common/message/` |
| Vendored | 2026-07-31 |
| Contents | 198 `.json` files: 93 request/response pairs, 2 header schemas, 10 internal record schemas |

`schemas/UPSTREAM-README.md` is Kafka's own README from that directory, kept because it
documents the schema language the generator has to implement.

## Why 4.3.1

Latest stable release at pin time. 4.x is KRaft-only and dropped message formats v0 and
v1, which narrows the wire surface the codec has to cover. Pinning older would
reintroduce schema versions the broker does not implement.

## Re-vendoring

Deliberately, never incidentally: a bump that widens a version range makes clients
negotiate a version the broker does not implement, and that surfaces as a hang rather
than an error. Run `./vendor-schemas.sh <tag>`, then diff `schemas/` and read every
`validVersions` / `flexibleVersions` change before accepting it. The generator's advertised
ranges are derived from these files, so a bump moves the wire surface.

## The schemas are JSON with `//` comments

Not valid JSON. Comments carry load-bearing information: `RequestHeader.json` explains
the `ClientId` encoding exception in a comment, not a field. Strip comments to parse, but read
them.
