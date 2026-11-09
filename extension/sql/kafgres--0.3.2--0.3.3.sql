-- kafgres 0.3.2 -> 0.3.3.
--
-- kafgres_follower_lag() reports how far the follower is behind the leader. It stays
-- executable by PUBLIC, like the other read-only reports.
CREATE FUNCTION @extschema@."kafgres_follower_lag"() RETURNS TABLE (
	"lag_records" bigint,
	"observed_at_ms" bigint
)
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'kafgres_follower_lag_wrapper';

-- kafgres_slot_ready() waits for the CDC slot; like the other administrative functions it
-- is not executable by PUBLIC.
CREATE FUNCTION @extschema@."kafgres_slot_ready"(
	"timeout_ms" INT DEFAULT 120000
) RETURNS text
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'kafgres_slot_ready_wrapper';
REVOKE EXECUTE ON FUNCTION @extschema@."kafgres_slot_ready"(int) FROM PUBLIC;
