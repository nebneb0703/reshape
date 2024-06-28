use std::collections::{HashMap, HashSet};
use anyhow::Context;
use crate::db::Connection;

// Schema tracks changes made to tables and columns during a migration.
// These changes are not applied until the migration is completed but
// need to be taken into consideration when creating views for a migration
// and when a user references a table or column in a migration.
//
// The changes to a table are tracked by a `TableChanges` struct. The possible
// changes are:
//   - Changing the name which updates `current_name`.
//   - Removing which sets the `removed` flag.
//
// Changes to a column are tracked by a `ColumnChanges` struct which reside in
// the corresponding `TableChanges`. The possible changes are:
//   - Changing the name which updates `current_name`.
//   - Changing the backing column which will add the new column to the end of
//     `intermediate_columns`. This is used when temporary columns are
//     introduced which will eventually replace the current column.
//   - Removing which sets the `removed` flag.
//
// Schema provides some schema introspection methods, `get_tables` and `get_table`,
// which will retrieve the current schema from the database and apply the changes.
#[derive(Debug)]
pub struct Schema {
    table_changes: Vec<TableChanges>,
}

impl Schema {
    pub fn new() -> Schema {
        Schema {
            table_changes: Vec::new(),
        }
    }

    pub fn change_table<F>(&mut self, current_name: &str, f: F)
    where
        F: FnOnce(&mut TableChanges),
    {
        let table_change_index = self
            .table_changes
            .iter()
            .position(|table| table.current_name == current_name)
            .unwrap_or_else(|| {
                let new_changes = TableChanges::new(current_name.to_string());
                self.table_changes.push(new_changes);
                self.table_changes.len() - 1
            });

        let table_changes = &mut self.table_changes[table_change_index];
        f(table_changes)
    }

    pub async fn create_for_migration(
        &self,
        db: &mut impl Connection,
        migration_name: &str
    ) -> anyhow::Result<()> {
        // Create schema for migration
        let schema_name = crate::schema_name_for_migration(migration_name);
        db.run(&format!("CREATE SCHEMA IF NOT EXISTS {}", schema_name)).await
            .with_context(|| {
                format!(
                    "failed to create schema {} for migration {}",
                    schema_name, migration_name
                )
            })?;

        // Create views inside schema
        for table in self.get_tables(db).await? {
            table.create_view(db, &schema_name).await?;
        }

        Ok(())
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct TableChanges {
    current_name: String,
    real_name: String,
    column_changes: Vec<ColumnChanges>,
    removed: bool,
}

impl TableChanges {
    fn new(name: String) -> Self {
        Self {
            current_name: name.to_string(),
            real_name: name,
            column_changes: Vec::new(),
            removed: false,
        }
    }

    pub fn set_name(&mut self, name: &str) {
        self.current_name = name.to_string();
    }

    pub fn change_column<F>(&mut self, current_name: &str, f: F)
    where
        F: FnOnce(&mut ColumnChanges),
    {
        let column_change_index = self
            .column_changes
            .iter()
            .position(|column| column.current_name == current_name)
            .unwrap_or_else(|| {
                let new_changes = ColumnChanges::new(current_name.to_string());
                self.column_changes.push(new_changes);
                self.column_changes.len() - 1
            });

        let column_changes = &mut self.column_changes[column_change_index];
        f(column_changes)
    }

    pub fn set_removed(&mut self) {
        self.removed = true;
    }
}

#[derive(Debug)]
pub struct ColumnChanges {
    current_name: String,
    backing_columns: Vec<String>,
    removed: bool,
}

impl ColumnChanges {
    fn new(name: String) -> Self {
        Self {
            current_name: name.to_string(),
            backing_columns: vec![name],
            removed: false,
        }
    }

    pub fn set_name(&mut self, name: &str) {
        self.current_name = name.to_string();
    }

    pub fn set_column(&mut self, column_name: &str) {
        self.backing_columns.push(column_name.to_string())
    }

    pub fn set_removed(&mut self) {
        self.removed = true;
    }

    fn real_name(&self) -> &str {
        self.backing_columns
            .last()
            .expect("backing_columns should never be empty")
    }
}

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub real_name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub real_name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default: Option<String>,
}

impl Schema {
    pub async fn get_tables(&self, db: &mut dyn Connection) -> anyhow::Result<Vec<Table>> {
        let rows = db.query(
            "
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ",
        ).await?;

        let names = rows
            .iter()
            .map(|row| row.get::<'_, _, String>("table_name"))
            .filter_map(|real_name| {
                let table_changes = self
                    .table_changes
                    .iter()
                    .find(|changes| changes.real_name == real_name);

                // Skip table if it has been removed
                if let Some(changes) = table_changes {
                    if changes.removed {
                        return None;
                    }
                }

                Some(real_name)
            });

        let mut tables = Vec::new();
        for real_name in names {
            tables.push(self.get_table_by_real_name(db, &real_name).await?);
        }

        Ok(tables)
    }

    pub async fn get_table(&self, db: &mut dyn Connection, table_name: &str) -> anyhow::Result<Table> {
        let table_changes = self
            .table_changes
            .iter()
            .find(|changes| changes.current_name == table_name);

        let real_table_name = table_changes
            .map(|changes| changes.real_name.to_string())
            .unwrap_or_else(|| table_name.to_string());

        self.get_table_by_real_name(db, &real_table_name).await
    }

    async fn get_table_by_real_name(
        &self,
        db: &mut dyn Connection,
        real_table_name: &str,
    ) -> anyhow::Result<Table> {
        let table_changes = self
            .table_changes
            .iter()
            .find(|changes| changes.real_name == real_table_name);

        let real_columns: Vec<(String, String, bool, Option<String>)> = db
            .query(&format!(
                "
                SELECT column_name, CASE WHEN data_type = 'USER-DEFINED' THEN udt_name ELSE data_type END, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = '{table}' AND table_schema = 'public'
                ORDER BY ordinal_position
                ",
                table = real_table_name,
            )).await?
            .iter()
            .map(|row| {
                (
                    row.get("column_name"),
                    row.get("data_type"),
                    row.get::<'_, _, String>("is_nullable") == "YES",
                    row.get("column_default"),
                )
            })
            .collect();

        let mut ignore_columns: HashSet<String> = HashSet::new();
        let mut aliases: HashMap<String, &str> = HashMap::new();

        if let Some(changes) = table_changes {
            for column_changes in &changes.column_changes {
                if column_changes.removed {
                    ignore_columns.insert(column_changes.real_name().to_string());
                } else {
                    aliases.insert(
                        column_changes.real_name().to_string(),
                        &column_changes.current_name,
                    );
                }

                let (_, rest) = column_changes
                    .backing_columns
                    .split_last()
                    .expect("backing_columns should never be empty");

                for column in rest {
                    ignore_columns.insert(column.to_string());
                }
            }
        }

        let mut columns: Vec<Column> = Vec::new();

        for (real_name, data_type, nullable, default) in real_columns {
            if ignore_columns.contains(&*real_name) {
                continue;
            }

            let name = aliases
                .get(&real_name)
                .map(|alias| alias.to_string())
                .unwrap_or_else(|| real_name.to_string());

            columns.push(Column {
                name,
                real_name,
                data_type,
                nullable,
                default,
            });
        }

        let current_table_name = table_changes
            .map(|changes| changes.current_name.as_ref())
            .unwrap_or_else(|| real_table_name);

        let table = Table {
            name: current_table_name.to_string(),
            real_name: real_table_name.to_string(),
            columns,
        };

        Ok(table)
    }
}

impl Table {
    pub fn real_column_names<'a>(
        &'a self,
        columns: &'a [String],
    ) -> impl Iterator<Item = &'a String> {
        columns.iter().map(|name| {
            self.get_column(name)
                .map(|col| &col.real_name)
                .unwrap_or(name)
        })
    }

    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|column| column.name == name)
    }

    pub async fn create_view(
        &self,
        db: &mut impl Connection,
        schema: &str
    ) -> anyhow::Result<()> {
        let select_columns: Vec<String> = self
            .columns
            .iter()
            .map(|column| {
                format!(
                    r#"
                        "{real_name}" AS "{alias}"
                        "#,
                    real_name = column.real_name,
                    alias = column.name,
                )
            })
            .collect();

        db.run(&format!(
            r#"
            CREATE OR REPLACE VIEW {schema}."{view_name}" AS
                SELECT {columns}
                FROM "{table_name}"
            "#,
            schema = schema,
            table_name = self.real_name,
            view_name = self.name,
            columns = select_columns.join(","),
        )).await
        .with_context(|| format!("failed to create view for table {}", self.name))?;

        Ok(())
    }
}

pub async fn create_new_schema_func(db: &mut dyn Connection, target_migration: &str) -> anyhow::Result<()> {
    let query = format!(
        "
			CREATE OR REPLACE FUNCTION reshape.is_new_schema()
			RETURNS BOOLEAN AS $$
            DECLARE
                setting TEXT := current_setting('reshape.is_new_schema', TRUE);
                setting_bool BOOLEAN := setting IS NOT NULL AND setting = 'YES';
			BEGIN
				RETURN current_setting('search_path') = 'migration_{}' OR setting_bool;
			END
			$$ language 'plpgsql';
        ",
        target_migration,
    );
    db.query(&query).await.context("failed creating helper function reshape.is_new_schema()")?;

    Ok(())
}

pub async fn drop_new_schema_func(db: &mut dyn Connection) -> anyhow::Result<()> {
    db.query("DROP FUNCTION IF EXISTS reshape.is_new_schema;").await?;
    Ok(())
}
