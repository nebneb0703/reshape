use std::fmt;

use serde::{Deserialize, Serialize};
use anyhow::Context;

use crate::{
    db::{Connection, Transaction},
    schema::Schema,
    actions::{Action, MigrationContext},
};

#[derive(Serialize, Deserialize, Debug)]
pub struct RenameTable {
    pub table: String,
    pub new_name: String,
}

impl fmt::Display for RenameTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,
            "Renaming table \"{}\" to \"{}\"",
            self.table,
            self.new_name
        )
    }
}

#[typetag::serde(name = "rename_table")]
#[async_trait::async_trait]
impl Action for RenameTable {
    async fn run(
        &self,
        _ctx: &MigrationContext,
        _db: &mut dyn Connection,
        _schema: &Schema,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn complete<'a>(
        &self,
        _ctx: &MigrationContext,
        db: &'a mut dyn Connection,
    ) -> anyhow::Result<Option<Transaction<'a>>> {
        // Rename table
        let query = format!(
            r#"
            ALTER TABLE IF EXISTS "{table}"
            RENAME TO "{new_name}"
            "#,
            table = self.table,
            new_name = self.new_name,
        );
        db.run(&query).await.context("failed to rename table")?;

        Ok(None)
    }

    fn update_schema(&self, _ctx: &MigrationContext, schema: &mut Schema) {
        schema.change_table(&self.table, |table_changes| {
            table_changes.set_name(&self.new_name);
        });
    }

    async fn abort(&self, _ctx: &MigrationContext, _db: &mut dyn Connection) -> anyhow::Result<()> {
        Ok(())
    }
}
