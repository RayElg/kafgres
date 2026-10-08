-- kafgres 0.3.0 -> 0.4.0: adds kafgres_read() as a fresh install defines it, not PUBLIC.

CREATE FUNCTION @extschema@.kafgres_read(
	"topic" TEXT,
	"partition" INT DEFAULT NULL,
	"from_offset" bigint DEFAULT NULL,
	"to_offset" bigint DEFAULT NULL,
	"from_time" timestamp with time zone DEFAULT NULL,
	"to_time" timestamp with time zone DEFAULT NULL,
	"isolation" TEXT DEFAULT 'read_committed',
	"max_records" bigint DEFAULT NULL
) RETURNS TABLE (
	"partition" INT,
	"record_offset" bigint,
	"timestamp" timestamp with time zone,
	"timestamp_type" TEXT,
	"key" bytea,
	"value" bytea,
	"header_keys" TEXT[],
	"header_values" bytea[],
	"leader_epoch" INT,
	"producer_id" bigint,
	"producer_epoch" smallint,
	"sequence" INT,
	"is_transactional" bool
)
SET search_path TO pg_catalog, @extschema@, pg_temp
LANGUAGE c
AS 'MODULE_PATHNAME', 'kafgres_read_wrapper';

REVOKE EXECUTE ON FUNCTION @extschema@.kafgres_read(text, int, bigint, bigint, timestamptz,
    timestamptz, text, bigint) FROM PUBLIC;
