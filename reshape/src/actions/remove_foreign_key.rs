use std::fmt;

use serde::{Deserialize, Serialize};
use anyhow::{anyhow, Context};

use crate::{
    db::Connection,
    schema::Schema,
    actions::{Action, MigrationContext},
};

#[derive(Serialize, Deserialize, Debug)]
pub struct RemoveForeignKey {
    table: String,
    foreign_key: String,
}

impl fmt::Display for RemoveForeignKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,
            "Removing foreign key \"{}\" from table \"{}\"",
            self.foreign_key,
            self.table
        )
    }
}

#[typetag::serde(name = "remove_foreign_key")]
#[async_trait::async_trait]
impl Action for RemoveForeignKey {
    async fn begin(
        &self,
        _ctx: &MigrationContext,
        db: &mut dyn Connection,
        schema: &Schema,
    ) -> anyhow::Result<()> {
        // The foreign key is only removed once the migration is completed.
        // Removing it earlier would be hard/undesirable for several reasons:
        // - Postgres doesn't have an easy way to temporarily disable a foreign key check.
        //   If it did, we could disable the FK for the new schema.
        // - Even if we could, it probably wouldn't be a good idea as it would cause temporary
        //   inconsistencies for the old schema which still expects the FK to hold.
        // - For the same reason, we can't remove the FK when the migration is first applied.
        //   If the migration was to be aborted, then the FK would have to be recreated with
        //   the risk that it would no longer be valid.

        // Ensure foreign key exists
        let table = schema.get_table(db, &self.table).await?;
        let fk_exists = !db
            .query(&format!(
                r#"
                SELECT constraint_name
                FROM information_schema.table_constraints
                WHERE
                    constraint_type = 'FOREIGN KEY' AND
                    table_name = '{table_name}' AND
                    constraint_name = '{foreign_key}'
                "#,
                table_name = table.real_name,
                foreign_key = self.foreign_key,
            )).await
            .context("failed to check for foreign key")?
            .is_empty();

        if !fk_exists {
            return Err(anyhow!(
                "no foreign key \"{}\" exists on table \"{}\"",
                self.foreign_key,
                self.table
            ));
        }

        Ok(())
    }

    async fn complete(
        &self,
        _ctx: &MigrationContext,
        db: &mut dyn Connection,
    ) -> anyhow::Result<()> {
        db.run(&format!(
            r#"
            ALTER TABLE {table}
            DROP CONSTRAINT IF EXISTS {foreign_key}
            "#,
            table = self.table,
            foreign_key = self.foreign_key,
        )).await
        .context("failed to remove foreign key")
    }

    fn update_schema(&self, _ctx: &MigrationContext, _schema: &mut Schema) {}

    async fn abort(&self, _ctx: &MigrationContext, _db: &mut dyn Connection) -> anyhow::Result<()> {
        Ok(())
    }
}
