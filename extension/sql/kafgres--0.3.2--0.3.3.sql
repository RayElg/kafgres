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
