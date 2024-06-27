use std::fmt;

use serde::{Deserialize, Serialize};
use anyhow::Context;

use crate::{
    db::{Connection, Transaction},
    schema::Schema,
    actions::{Action, MigrationContext},
};

#[derive(Serialize, Deserialize, Debug)]
pub struct RemoveIndex {
    pub index: String,
}

impl fmt::Display for RemoveIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,
            "Removing index \"{}\"",
            self.index
        )
    }
}

#[typetag::serde(name = "remove_index")]
#[async_trait::async_trait]
impl Action for RemoveIndex {
    async fn run(
        &self,
        _ctx: &MigrationContext,
        _db: &mut dyn Connection,
        _schema: &Schema,
    ) -> anyhow::Result<()> {
        // Do nothing, the index isn't removed until completion
        Ok(())
    }

    async fn complete<'a>(
        &self,
        _ctx: &MigrationContext,
        db: &'a mut dyn Connection,
    ) -> anyhow::Result<Option<Transaction<'a>>> {
        db.run(&format!(
            r#"
            DROP INDEX CONCURRENTLY IF EXISTS "{name}"
            "#,
            name = self.index
        )).await
        .context("failed to drop index")?;

        Ok(None)
    }

    fn update_schema(&self, _ctx: &MigrationContext, _schema: &mut Schema) {}

    async fn abort(&self, _ctx: &MigrationContext, _db: &mut dyn Connection) -> anyhow::Result<()> {
        Ok(())
    }
}
