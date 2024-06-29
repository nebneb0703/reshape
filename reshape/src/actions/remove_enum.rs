use std::fmt;

use serde::{Deserialize, Serialize};
use anyhow::Context;

use crate::{
    db::Connection,
    schema::Schema,
    actions::{Action, MigrationContext},
};

#[derive(Serialize, Deserialize, Debug)]
pub struct RemoveEnum {
    #[serde(rename = "enum")]
    pub enum_name: String,
}

impl fmt::Display for RemoveEnum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,
            "Removing enum \"{}\"",
            self.enum_name
        )
    }
}

#[typetag::serde(name = "remove_enum")]
#[async_trait::async_trait]
impl Action for RemoveEnum {
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
    ) -> anyhow::Result<()> {
        db.run(&format!(
            r#"
            DROP TYPE IF EXISTS {name}
            "#,
            name = self.enum_name,
        )).await
        .context("failed to drop enum")
    }

    fn update_schema(&self, _ctx: &MigrationContext, _schema: &mut Schema) {}

    async fn abort(&self, _ctx: &MigrationContext, _db: &mut dyn Connection) -> anyhow::Result<()> {
        Ok(())
    }
}
