use std::fmt;

use serde::{Deserialize, Serialize};
use anyhow::Context;

use crate::{
    db::Connection,
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
    async fn begin(
        &self,
        _ctx: &MigrationContext,
        _db: &mut dyn Connection,
        _schema: &Schema,
    ) -> anyhow::Result<()> {
        // Do nothing, the index isn't removed until completion
        Ok(())
    }

    async fn complete(
        &self,
        _ctx: &MigrationContext,
        db: &mut dyn Connection,
    ) -> anyhow::Result<()> {
        db.run(&format!(
            r#"
            DROP INDEX CONCURRENTLY IF EXISTS "{name}"
            "#,
            name = self.index
        )).await
        .context("failed to drop index")
    }

    fn update_schema(&self, _ctx: &MigrationContext, _schema: &mut Schema) {}

    async fn abort(&self, _ctx: &MigrationContext, _db: &mut dyn Connection) -> anyhow::Result<()> {
        Ok(())
    }
}
