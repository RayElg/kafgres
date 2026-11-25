//! Record the owning role of each CDC mapping so the CDC worker renders mappings under
//! the owner's privileges rather than as the bootstrap superuser.

use crate::ddl::run_ddl;

pub fn init_220() {
    // No default while the column is added: this runs as a superuser, and a default would
    // make every mapping that predates the column the superuser's. Those stay NULL, and
    // render nothing until a superuser sets their owner.
    run_ddl(
        "ALTER TABLE kafgres_cdc_mappings ADD COLUMN IF NOT EXISTS owner text",
        "add cdc mapping owner column",
    );
    // A build that added the column NOT NULL DEFAULT current_user has already stamped its
    // rows; those cannot be told apart, but new rows still need no NOT NULL.
    run_ddl(
        "ALTER TABLE kafgres_cdc_mappings ALTER COLUMN owner DROP NOT NULL",
        "allow an unset cdc mapping owner",
    );
    // Only new rows: SET DEFAULT does not touch existing ones.
    run_ddl(
        "ALTER TABLE kafgres_cdc_mappings ALTER COLUMN owner SET DEFAULT current_user",
        "default new cdc mappings to their creator",
    );

    // Defence in depth: even if an operator grants direct DML on kafgres_cdc_mappings, a
    // non-superuser cannot claim another role's ownership, nor change or delete another
    // role's mapping or one with no owner.
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
                 ELSIF TG_OP = 'DELETE' THEN
                     IF OLD.owner IS DISTINCT FROM current_user THEN
                         RAISE EXCEPTION 'permission denied: cannot delete CDC mapping owned by %', OLD.owner
                             USING ERRCODE = 'insufficient_privilege';
                     END IF;
                 END IF;
             END IF;
             IF TG_OP = 'DELETE' THEN
                 RETURN OLD;
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
             BEFORE INSERT OR UPDATE OR DELETE ON kafgres_cdc_mappings
             FOR EACH ROW EXECUTE FUNCTION kafgres_cdc_mappings_enforce_owner()",
        "create cdc mapping owner guard trigger",
    );
}
