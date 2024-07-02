use std::fmt;

use serde::{Deserialize, Serialize};
use anyhow::{bail, Context};

use crate::{
    db::Connection,
    schema::Schema,
    actions::{Action, MigrationContext, common, Column},
};

#[derive(Serialize, Deserialize, Debug)]
pub struct AddColumn {
    pub table: String,
    pub column: Column,
    pub up: Option<Transformation>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum Transformation {
    Simple(String),
    Update {
        table: String,
        value: String,
        r#where: String,
    },
}

impl fmt::Display for AddColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,
            "Adding column \"{}\" to \"{}\"",
            self.column.name,
            self.table
        )
    }
}

#[typetag::serde(name = "add_column")]
#[async_trait::async_trait]
impl Action for AddColumn {
    async fn begin(
        &self,
        ctx: &MigrationContext,
        db: &mut dyn Connection,
        schema: &Schema,
    ) -> anyhow::Result<()> {
        let table = schema.get_table(db, &self.table).await?;

        let quoted_column_name = format!("\"{}\"", self.column.name);

        let mut definition_parts = vec![
            quoted_column_name.as_str(),
            &self.column.data_type,
        ];

        if let Some(default) = &self.column.default {
            definition_parts.push("DEFAULT");
            definition_parts.push(default);
        }

        if let Some(generated) = &self.column.generated {
            definition_parts.push("GENERATED");
            definition_parts.push(generated);
        }

        // Add column as nullable at this stage regardless of nullability
        db.run(&format!(
            r#"
			ALTER TABLE public."{table}"
            ADD COLUMN IF NOT EXISTS {definition};
			"#,
            table = table.real_name,
            definition = definition_parts.join(" "),
        )).await.context("failed to add column")?;

        match &self.up {
            Some(Transformation::Simple(up)) => {
                // Declare variables so the up query has the expected view into the table
                let declarations: Vec<String> = table
                    .columns
                    .iter()
                    .map(|column| {
                        format!(
                            r#"
                            "{alias}" public."{table}"."{real_name}"%TYPE := NEW."{real_name}";
                            "#,
                            alias = column.name,
                            table = table.real_name,
                            real_name = column.real_name,
                        )
                    })
                    .collect();

                db.run(&format!(
                    r#"
                    CREATE OR REPLACE FUNCTION "{trigger_name}"()
                    RETURNS TRIGGER AS $$
                    #variable_conflict use_variable
                    BEGIN
                        IF NOT reshape.is_new_schema() THEN
                            DECLARE
                                {declarations}
                            BEGIN
                                NEW."{column}" = {up};
                            END;
                        END IF;
                        RETURN NEW;
                    END;
                    $$ language 'plpgsql';

                    DROP TRIGGER IF EXISTS "{trigger_name}" ON public."{table}";
                    CREATE TRIGGER "{trigger_name}" BEFORE UPDATE OR INSERT ON public."{table}" FOR EACH ROW EXECUTE PROCEDURE "{trigger_name}"();
                    "#,
                    trigger_name = self.trigger_name(ctx),
                    table = table.real_name,
                    column = self.column.name,
                    declarations = declarations.join("\n"),
                )).await.context("failed to create up trigger")?;

                // Backfill values in batches
                common::batch_touch_rows(db, &table.real_name, Some(&self.column.name))
                    .await.context("failed to batch update existing rows")?;
            },
            Some(Transformation::Update { table: from_table, value, r#where }) => {
                let from_table = schema.get_table(db, from_table).await?;

                // Declare and assign variables so the query has the expected view into the table
                let from_table_assignments: Vec<String> = from_table
                    .columns
                    .iter()
                    .map(|column| format!(
                        r#"
                        "{table}"."{alias}" = NEW."{real_name}";
                        "#,
                        table = from_table.name,
                        alias = column.name,
                        real_name = column.real_name,
                    )).collect();

                db.run(&format!(
                    r#"
                    CREATE OR REPLACE FUNCTION "{trigger_name}"()
                    RETURNS TRIGGER AS $$
                    #variable_conflict use_variable
                    BEGIN
                        IF NOT reshape.is_new_schema() THEN
                            DECLARE
                                "{from_table_alias}" public."{from_table_real}"%ROWTYPE;
                            BEGIN
                                {assignments}

                                -- Don't trigger reverse trigger when making this update
                                perform set_config('reshape.disable_triggers', 'TRUE', TRUE);

                                UPDATE public."{changed_table_real}"
                                SET "{column}" = {value}
                                WHERE {where};

                                perform set_config('reshape.disable_triggers', '', TRUE);
                            END;
                        END IF;
                        RETURN NEW;
                    END;
                    $$ language 'plpgsql';

                    DROP TRIGGER IF EXISTS "{trigger_name}" ON public."{from_table_real}";
                    CREATE TRIGGER "{trigger_name}" BEFORE UPDATE OR INSERT ON public."{from_table_real}" FOR EACH ROW EXECUTE PROCEDURE "{trigger_name}"();
                    "#,
                    assignments = from_table_assignments.join("\n"),
                    from_table_alias = from_table.name,
                    from_table_real = from_table.real_name,
                    changed_table_real = table.real_name,
                    column = self.column.name,
                    trigger_name = self.trigger_name(ctx),
                )).await.context("failed to create up trigger")?;

                let from_table_columns = from_table
                    .columns
                    .iter()
                    .map(|column| format!(
                        r#"
                        "{}" AS "{}"
                        "#, column.real_name, column.name))
                    .collect::<Vec<String>>()
                    .join(", ");

                let changed_table_assignments: Vec<String> = table
                    .columns
                    .iter()
                    .map(|column| {
                        format!(
                            r#"
                            "{table}"."{alias}" := NEW."{real_name}";
                            "#,
                            table = table.name,
                            alias = column.name,
                            real_name = column.real_name,
                        )
                    })
                    .collect();

                db.run(&format!(
                    r#"
                    CREATE OR REPLACE FUNCTION "{reverse_trigger_name}"()
                    RETURNS TRIGGER AS $$
                    #variable_conflict use_variable
                    BEGIN
                        IF NOT reshape.is_new_schema() AND NOT current_setting('reshape.disable_triggers', TRUE) = 'TRUE' THEN
                            DECLARE
                                "{changed_table_alias}" public."{changed_table_real}"%ROWTYPE;
                                __temp_row public."{from_table_real}"%ROWTYPE;
                            BEGIN
                                {changed_table_assignments}

                                SELECT {from_table_columns}
                                INTO "__temp_row"
                                FROM public."{from_table_real}"
                                WHERE {where};

                                DECLARE
                                    "{from_table_alias}" public."{from_table_real}"%ROWTYPE;
                                BEGIN
                                    "{from_table_alias}" = __temp_row;
                                    NEW."{column}" = {value};
                                END;
                            END;
                        END IF;
                        RETURN NEW;
                    END;
                    $$ language 'plpgsql';

                    DROP TRIGGER IF EXISTS "{reverse_trigger_name}" ON public."{changed_table_real}";
                    CREATE TRIGGER "{reverse_trigger_name}" BEFORE UPDATE OR INSERT ON public."{changed_table_real}" FOR EACH ROW EXECUTE PROCEDURE "{reverse_trigger_name}"();
                    "#,
                    changed_table_assignments = changed_table_assignments.join("\n"),
                    changed_table_alias = table.name,
                    changed_table_real = table.real_name,
                    from_table_alias = from_table.name,
                    from_table_real = from_table.real_name,
                    column = self.column.name,
                    reverse_trigger_name = self.reverse_trigger_name(ctx),
                )).await.context("failed to create reverse up trigger")?;

                // Backfill values in batches by touching the from table
                common::batch_touch_rows(db, &from_table.real_name, None)
                    .await.context("failed to batch update existing rows")?;
            },
            _ => {}
        }

        // Add a temporary NOT NULL constraint if the column shouldn't be nullable.
        // This constraint is set as NOT VALID so it doesn't apply to existing rows and
        // the existing rows don't need to be scanned under an exclusive lock.
        // Thanks to this, we can set the full column as NOT NULL later with minimal locking.
        if !self.column.nullable {
            db.run(&format!(
                r#"
                DO $$
                BEGIN
                    ALTER TABLE public."{table}"
                    ADD CONSTRAINT "{constraint_name}"
                    CHECK ("{column}" IS NOT NULL) NOT VALID;
                EXCEPTION
                    -- Ignore duplicate constraint. This is necessary as
                    -- postgres does not support "IF NOT EXISTS" here.
                    WHEN duplicate_object THEN
                END;
                $$ language 'plpgsql';
                "#,
                table = table.real_name,
                constraint_name = self.not_null_constraint_name(ctx),
                column = self.column.name,
            )).await.context("failed to add NOT NULL constraint")?;
        }

        Ok(())
    }

    async fn complete(
        &self,
        ctx: &MigrationContext,
        db: &mut dyn Connection,
    ) -> anyhow::Result<()> {
        db.run(&format!(
            r#"
            DROP FUNCTION IF EXISTS "{trigger_name}" CASCADE;
            DROP FUNCTION IF EXISTS "{reverse_trigger_name}" CASCADE;
            "#,
            trigger_name = self.trigger_name(ctx),
            reverse_trigger_name = self.reverse_trigger_name(ctx),
        )).await.context("failed to drop up trigger")?;

        // Update column to be NOT NULL if necessary
        if !self.column.nullable {
            // Validate the temporary constraint (should always be valid).
            // This performs a sequential scan but does not take an exclusive lock.
            db.run(&format!(
                r#"
                DO $$
                BEGIN
                    ALTER TABLE "{table}"
                    VALIDATE CONSTRAINT "{constraint_name}";
                EXCEPTION
                    -- Ignore if constraint does not exist. This is necessary as
                    -- postgres does not support "IF EXISTS" here.
                    WHEN undefined_object THEN
                END;
                $$ language 'plpgsql';
                "#,
                table = self.table,
                constraint_name = self.not_null_constraint_name(ctx),
            )).await.context("failed to validate NOT NULL constraint")?;

            // Update the column to be NOT NULL.
            // This requires an exclusive lock but since PG 12 it can check
            // the existing constraint for correctness which makes the lock short-lived.
            // Source: https://dba.stackexchange.com/a/268128
            db.run(&format!(
                r#"
                DO $$
                BEGIN
                    ALTER TABLE "{table}"
                    ALTER COLUMN "{column}" SET NOT NULL;
                EXCEPTION
                    -- Ignore if column does not exist. This is necessary as
                    -- postgres does not support "IF EXISTS" here.
                    WHEN undefined_column THEN
                END;
                $$ language 'plpgsql';
                "#,
                table = self.table,
                column = self.column.name,
            )).await.context("failed to set column as NOT NULL")?;

            // Drop the temporary constraint
            db.run(&format!(
                r#"
                ALTER TABLE "{table}"
                DROP CONSTRAINT IF EXISTS "{constraint_name}"
                "#,
                table = self.table,
                constraint_name = self.not_null_constraint_name(ctx),
            )).await.context("failed to drop NOT NULL constraint")?;
        }

        Ok(())
    }

    fn update_schema(&self, _ctx: &MigrationContext, schema: &mut Schema) {
        schema.change_table(&self.table, |table_changes| {
            table_changes.change_column(&self.column.name, |_| {})
        });
    }

    async fn abort(&self, ctx: &MigrationContext, db: &mut dyn Connection) -> anyhow::Result<()> {
        // Remove column
        let query = format!(
            r#"
            ALTER TABLE "{table}"
            DROP COLUMN IF EXISTS "{column}"
            "#, // todo: cascade?
            table = self.table,
            column = self.column.name,
        );
        db.run(&query).await.context("failed to drop column")?;

        // Remove triggers and procedures
        let query = format!(
            r#"
            DROP FUNCTION IF EXISTS "{trigger_name}" CASCADE;
            DROP FUNCTION IF EXISTS "{reverse_trigger_name}" CASCADE;
            "#,
            trigger_name = self.trigger_name(ctx),
            reverse_trigger_name = self.reverse_trigger_name(ctx),
        );
        db.run(&query).await.context("failed to drop up trigger")?;

        Ok(())
    }
}

impl AddColumn {
    fn trigger_name(&self, ctx: &MigrationContext) -> String {
        format!(
            "{}_add_column_{}_{}",
            ctx.prefix(),
            self.table,
            self.column.name
        )
    }

    fn reverse_trigger_name(&self, ctx: &MigrationContext) -> String {
        format!(
            "{}_add_column_{}_{}_rev",
            ctx.prefix(),
            self.table,
            self.column.name
        )
    }

    fn not_null_constraint_name(&self, ctx: &MigrationContext) -> String {
        format!(
            "{}_add_column_not_null_{}_{}",
            ctx.prefix(),
            self.table,
            self.column.name
        )
    }
}
