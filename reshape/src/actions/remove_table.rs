use std::fmt;

use serde::{Deserialize, Serialize};
use anyhow::Context;

use crate::{
    db::{Connection, Transaction},
    schema::Schema,
    actions::{Action, MigrationContext},
};

#[derive(Serialize, Deserialize, Debug)]
pub struct RemoveTable {
    pub table: String,
}

impl fmt::Display for RemoveTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,
            "Removing table \"{}\"",
            self.table
        )
    }
}

#[typetag::serde(name = "remove_table")]
#[async_trait::async_trait]
impl Action for RemoveTable {
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
        // Remove table
        let query = format!(
            r#"
            DROP TABLE IF EXISTS "{table}";
            "#,
            table = self.table,
        );
        db.run(&query).await.context("failed to drop table")?;

        Ok(None)
    }

    fn update_schema(&self, _ctx: &MigrationContext, schema: &mut Schema) {
        schema.change_table(&self.table, |table_changes| {
            table_changes.set_removed();
        });
    }

    async fn abort(&self, _ctx: &MigrationContext, _db: &mut dyn Connection) -> anyhow::Result<()> {
        Ok(())
    }
}
