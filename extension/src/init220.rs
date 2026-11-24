//! Record the owning role of each CDC mapping so the CDC worker renders mappings under
//! the owner's privileges rather than as the bootstrap superuser.

use crate::ddl::run_ddl;

pub fn init_220() {
    run_ddl(
        "ALTER TABLE kafgres_cdc_mappings
            ADD COLUMN IF NOT EXISTS owner text NOT NULL DEFAULT current_user",
        "add cdc mapping owner column",
    );

    // Defence in depth: even if an operator grants direct INSERT/UPDATE on
    // kafgres_cdc_mappings, a non-superuser cannot claim another role's ownership or
    // modify another role's mapping.
    run_ddl(
        "CREATE OR REPLACE FUNCTION kafgres_cdc_mappings_enforce_owner()
         RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NOT (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user) THEN
                 IF TG_OP = 'INSERT' THEN
                     IF NEW.owner IS DISTINCT FROM current_user THEN
                         RAISE EXCEPTION 'permission denied: cannot create CDC mapping owned by %', NEW.owner
                             USING ERRCODE = 'insufficient_privilege';
                     END IF;
                 ELSIF TG_OP = 'UPDATE' THEN
                     IF OLD.owner IS DISTINCT FROM current_user OR NEW.owner IS DISTINCT FROM current_user THEN
                         RAISE EXCEPTION 'permission denied: cannot modify CDC mapping owned by %', OLD.owner
                             USING ERRCODE = 'insufficient_privilege';
                     END IF;
                 END IF;
             END IF;
             RETURN NEW;
         END;
         $$",
        "create cdc mapping owner enforcement function",
    );

    run_ddl(
        "DROP TRIGGER IF EXISTS kafgres_cdc_mappings_owner_guard ON kafgres_cdc_mappings",
        "drop old cdc mapping owner guard trigger",
    );
    run_ddl(
        "CREATE TRIGGER kafgres_cdc_mappings_owner_guard
             BEFORE INSERT OR UPDATE ON kafgres_cdc_mappings
             FOR EACH ROW EXECUTE FUNCTION kafgres_cdc_mappings_enforce_owner()",
        "create cdc mapping owner guard trigger",
    );
}
