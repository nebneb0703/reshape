use std::fmt;

use serde::{Deserialize, Serialize};
use anyhow::Context;

use crate::{
    db::Connection,
    schema::Schema,
    actions::{Action, common::ForeignKey, MigrationContext},
};

#[derive(Serialize, Deserialize, Debug)]
pub struct AddForeignKey {
    pub table: String,
    foreign_key: ForeignKey,
}

impl fmt::Display for AddForeignKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,
            "Adding foreign key from table \"{}\" to \"{}\"",
            self.table,
            self.foreign_key.referenced_table
        )
    }
}

#[typetag::serde(name = "add_foreign_key")]
#[async_trait::async_trait]
impl Action for AddForeignKey {
    async fn run(
        &self,
        ctx: &MigrationContext,
        db: &mut dyn Connection,
        schema: &Schema,
    ) -> anyhow::Result<()> {
        let table = schema.get_table(db, &self.table).await?;
        let referenced_table = schema.get_table(db, &self.foreign_key.referenced_table).await?;

        // Add quotes around all column names
        let columns: Vec<String> = table
            .real_column_names(&self.foreign_key.columns)
            .map(|col| format!("\"{}\"", col))
            .collect();
        let referenced_columns: Vec<String> = referenced_table
            .real_column_names(&self.foreign_key.referenced_columns)
            .map(|col| format!("\"{}\"", col))
            .collect();

        // Create foreign key but set is as NOT VALID.
        // This means the foreign key will be enforced for inserts and updates
        // but the existing data won't be checked, that would cause a long-lived lock.
        db.run(&format!(
            r#"
            ALTER TABLE "{table}"
            ADD CONSTRAINT {constraint_name}
            FOREIGN KEY ({columns})
            REFERENCES "{referenced_table}" ({referenced_columns})
            NOT VALID
            "#,
            table = table.real_name,
            constraint_name = self.temp_constraint_name(ctx),
            columns = columns.join(", "),
            referenced_table = referenced_table.real_name,
            referenced_columns = referenced_columns.join(", "),
        )).await
        .context("failed to create foreign key")?;

        db.run(&format!(
            r#"
            ALTER TABLE "{table}"
            VALIDATE CONSTRAINT "{constraint_name}"
            "#,
            table = table.real_name,
            constraint_name = self.temp_constraint_name(ctx),
        )).await
        .context("failed to validate foreign key")?;

        Ok(())
    }

    async fn complete<'a>(
        &self,
        ctx: &MigrationContext,
        db: &'a mut dyn Connection,
    ) -> anyhow::Result<()> {
        db.run(&format!(
            r#"
            ALTER TABLE {table}
            RENAME CONSTRAINT {temp_constraint_name} TO {constraint_name}
            "#,
            table = self.table,
            temp_constraint_name = self.temp_constraint_name(ctx),
            constraint_name = self.final_constraint_name(),
        )).await
        .context("failed to rename temporary constraint")
    }

    fn update_schema(&self, _ctx: &MigrationContext, _schema: &mut Schema) {}

    async fn abort(&self, ctx: &MigrationContext, db: &mut dyn Connection) -> anyhow::Result<()> {
        db.run(&format!(
            r#"
            ALTER TABLE "{table}"
            DROP CONSTRAINT IF EXISTS "{constraint_name}"
            "#,
            table = self.table,
            constraint_name = self.temp_constraint_name(ctx),
        )).await
        .context("failed to validate foreign key")?;

        Ok(())
    }
}

impl AddForeignKey {
    fn temp_constraint_name(&self, ctx: &MigrationContext) -> String {
        format!("{}_temp_fkey", ctx.prefix())
    }

    fn final_constraint_name(&self) -> String {
        format!(
            "{table}_{columns}_fkey",
            table = self.table,
            columns = self.foreign_key.columns.join("_")
        )
    }
}
