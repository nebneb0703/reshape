use std::fmt;

use serde::{Deserialize, Serialize};
use anyhow::Context;

use crate::{
    db::Connection,
    schema::Schema,
    actions::{Action, MigrationContext},
};

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateEnum {
    pub name: String,
    pub values: Vec<String>,
}

impl fmt::Display for CreateEnum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,
            "Creating enum \"{}\"",
            self.name
        )
    }
}

#[typetag::serde(name = "create_enum")]
#[async_trait::async_trait]
impl Action for CreateEnum {
    async fn begin(
        &self,
        _ctx: &MigrationContext,
        db: &mut dyn Connection,
        _schema: &Schema,
    ) -> anyhow::Result<()> {
        // Check if enum already exists. CREATE TYPE doesn't have
        // a IF NOT EXISTS option so we have to do it manually.
        let enum_exists = !db
            .query(&format!(
                "
                SELECT typname
                FROM pg_catalog.pg_type
                WHERE typcategory = 'E'
                AND typname = '{name}'
                ",
                name = self.name,
            )).await?
            .is_empty();
        if enum_exists {
            return Ok(());
        }

        let values_def: Vec<String> = self
            .values
            .iter()
            .map(|value| format!("'{}'", value))
            .collect();

        db.run(&format!(
            r#"
            CREATE TYPE "{name}" AS ENUM ({values})
            "#,
            name = self.name,
            values = values_def.join(", "),
        )).await
        .context("failed to create enum")?;

        Ok(())
    }

    async fn complete(
        &self,
        _ctx: &MigrationContext,
        _db: &mut dyn Connection,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn update_schema(&self, _ctx: &MigrationContext, _schema: &mut Schema) {}

    async fn abort(&self, _ctx: &MigrationContext, db: &mut dyn Connection) -> anyhow::Result<()> {
        db.run(&format!(
            r#"
            DROP TYPE IF EXISTS {name}
            "#,
            name = self.name,
        )).await
        .context("failed to drop enum")?;

        Ok(())
    }
}
