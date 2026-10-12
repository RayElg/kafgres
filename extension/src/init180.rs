//! KIP-848 regex subscriptions. `regex_matched` holds the pattern's matches under the
//! member's ACLs at its last heartbeat, for recomputing the assignment without its request.

use crate::ddl::run_ddl;

pub fn init_180() {
    run_ddl(
        "ALTER TABLE kafgres_consumer_group_members
            ADD COLUMN IF NOT EXISTS subscribed_regex text,
            ADD COLUMN IF NOT EXISTS regex_matched text[] NOT NULL DEFAULT '{}'",
        "add the regex subscription columns",
    );
}
