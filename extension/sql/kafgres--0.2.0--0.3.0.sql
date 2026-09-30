-- kafgres 0.2.0 -> 0.3.0.
--
-- No function was added, removed or re-signed; only who may call them changes, and
-- kafgres_produce()'s search path.

-- kafgres_produce() reads the ACL rules as a superuser: pin its search path, pg_temp last.
ALTER FUNCTION @extschema@.kafgres_produce(text, text, text)
    SET search_path = pg_catalog, @extschema@, pg_temp;

-- Revoke EXECUTE from PUBLIC on everything but kafgres_produce(), which checks its own
-- permissions, and the read-only reports, which need table grants anyway.
DO $$
DECLARE
    f regprocedure;
BEGIN
    FOR f IN
        SELECT p.oid::regprocedure
          FROM pg_depend d
          JOIN pg_proc p ON p.oid = d.objid
         WHERE d.classid = 'pg_proc'::regclass
           AND d.refclassid = 'pg_extension'::regclass
           AND d.refobjid = (SELECT oid FROM pg_extension WHERE extname = 'kafgres')
           AND d.deptype = 'e'
           AND p.proname NOT IN ('kafgres_produce', 'kafgres_version', 'kafgres_kafka_version',
                                 'kafgres_partition_offsets', 'kafgres_archive_status',
                                 'kafgres_cdc_status', 'kafgres_cdc_snapshots',
                                 'kafgres_share_state')
    LOOP
        EXECUTE format('REVOKE EXECUTE ON FUNCTION %s FROM PUBLIC', f);
    END LOOP;
END
$$;
