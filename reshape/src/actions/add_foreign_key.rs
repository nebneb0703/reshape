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
    async fn begin(
        &self,
        _ctx: &MigrationContext,
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
            DO $$
            BEGIN
                ALTER TABLE public."{table}"
                ADD CONSTRAINT "{constraint_name}"
                FOREIGN KEY ({columns})
                REFERENCES public."{referenced_table}" ({referenced_columns})
                NOT VALID;
            EXCEPTION
                -- Ignore duplicate constraint. This is necessary as
                -- postgres does not support "IF NOT EXISTS" here.
                WHEN duplicate_object THEN
            END;
            $$ language 'plpgsql';
            "#,
            table = table.real_name,
            constraint_name = self.constraint_name(),
            columns = columns.join(", "),
            referenced_table = referenced_table.real_name,
            referenced_columns = referenced_columns.join(", "),
        )).await.context("failed to create foreign key")?;

        db.run(&format!(
            r#"
            ALTER TABLE public."{table}"
            VALIDATE CONSTRAINT "{constraint_name}"
            "#,
            table = table.real_name,
            constraint_name = self.constraint_name(),
        )).await.context("failed to validate foreign key")?;

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
            ALTER TABLE "{table}"
            DROP CONSTRAINT IF EXISTS "{constraint_name}"
            "#,
            table = self.table,
            constraint_name = self.constraint_name(),
        )).await.context("failed to validate foreign key")?;

        Ok(())
    }
}

impl AddForeignKey {
    fn constraint_name(&self) -> String {
        format!(
            "{table}_{columns}_fkey",
            table = self.table,
            columns = self.foreign_key.columns.join("_")
        )
    }
}
