"""Send a legacy `AlterConfigs` (api key 33) and print the outcome.

No Java tool sends this any more (`kafka-configs.sh` moved to IncrementalAlterConfigs);
kafka-python still does, which is the point. Run inside the `kafgres-clients` image.

    python3 legacy_alter_configs.py <broker> <topic> [k=v,k=v] [--validate-only]
"""

import sys

from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType

broker, topic = sys.argv[1], sys.argv[2]
pairs = sys.argv[3] if len(sys.argv) > 3 else ""
configs = dict(p.split("=", 1) for p in pairs.split(",") if p)

admin = KafkaAdminClient(bootstrap_servers=broker)
resource = ConfigResource(ConfigResourceType.TOPIC, topic, configs=configs)
try:
    res = admin.alter_configs([resource])
    for entry in res.resources:
        code, message = entry[0], entry[1]
        print(f"code={code} message={message}")
except Exception as e:  # noqa: BLE001 - report whatever came back
    print(f"raised={type(e).__name__}: {e}")
