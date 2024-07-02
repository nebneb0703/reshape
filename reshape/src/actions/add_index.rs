use std::fmt;

use serde::{Deserialize, Serialize};
use anyhow::Context;

use crate::{
    db::Connection,
    schema::Schema,
    actions::{Action, MigrationContext},
};

#[derive(Serialize, Deserialize, Debug)]
pub struct AddIndex {
    pub table: String,
    pub index: Index,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Index {
    pub name: String,
    pub columns: Vec<String>,
    #[serde(default)]
    pub unique: bool,
    #[serde(rename = "type")]
    pub index_type: Option<String>,
}

impl fmt::Display for AddIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,
            "Adding index \"{}\" to table \"{}\"",
            self.index.name,
            self.table
        )
    }
}

#[typetag::serde(name = "add_index")]
#[async_trait::async_trait]
impl Action for AddIndex {
    async fn begin(
        &self,
        _ctx: &MigrationContext,
        db: &mut dyn Connection,
        schema: &Schema,
    ) -> anyhow::Result<()> {
        let table = schema.get_table(db, &self.table).await?;

        let column_real_names: Vec<String> = table
            .columns
            .iter()
            .filter(|column| self.index.columns.contains(&column.name))
            .map(|column| format!("\"{}\"", column.real_name))
            .collect();

        let unique = if self.index.unique { "UNIQUE" } else { "" };
        let index_type_def = if let Some(index_type) = &self.index.index_type {
            format!("USING {index_type}")
        } else {
            "".to_owned()
        };

        db.run(&format!(
            r#"
			CREATE {unique} INDEX CONCURRENTLY IF NOT EXISTS "{name}" ON "{table}" {index_type_def} ({columns})
			"#,
            name = self.index.name,
            table = self.table,
            columns = column_real_names.join(", "),
        )).await.context("failed to create index")?;

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
			DROP INDEX CONCURRENTLY IF EXISTS "{name}"
			"#,
            name = self.index.name,
        )).await.context("failed to drop index")?;

        Ok(())
    }
}
