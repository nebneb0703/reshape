#[macro_use] extern crate tracing;

pub mod db;
pub mod migration;
pub mod actions;
pub mod schema;
pub mod state;

use tokio_postgres::Config;
use anyhow::bail;

use crate::{
    db::{Lock, Connection},
    migration::Migration,
};

pub struct Reshape {
    pub db: Lock,
}

impl Reshape {
    pub async fn new(connection_string: &str) -> anyhow::Result<Reshape> {
        let config: Config = connection_string.parse()?;
        Self::new_with_config(&config).await
    }

    pub async fn new_with_options(
        host: &str,
        port: u16,
        database: &str,
        username: &str,
        password: &str,
    ) -> anyhow::Result<Reshape> {
        let mut config = Config::new();
        config
            .host(host)
            .port(port)
            .user(username)
            .dbname(database)
            .password(password);

        Self::new_with_config(&config).await
    }

    pub async fn new_with_config(config: &Config) -> anyhow::Result<Reshape> {
        let db = Lock::connect(config).await?;
        Ok(Reshape { db })
    }
}

pub fn schema_name_for_migration(migration_name: &str) -> String {
    format!("migration_{}", migration_name)
}

pub async fn current_migration(db: &mut impl Connection) -> anyhow::Result<Option<String>> {
    let name: Option<String> = db
        .query(
            "
            SELECT name
            FROM reshape.migrations
            ORDER BY index DESC
            LIMIT 1
            ",
        ).await?
        .first()
        .map(|row| row.get("name"));
    Ok(name)
}

pub async fn remaining_migrations(
    db: &mut impl Connection,
    new_migrations: impl IntoIterator<Item = Migration>,
) -> anyhow::Result<Vec<Migration>> {
    let mut new_iter = new_migrations.into_iter();

    // Ensure the new migrations match up with the existing ones
    let mut highest_index: Option<i32> = None;
    loop {
        let migrations = get_migrations(db, highest_index).await?;
        if migrations.is_empty() {
            break;
        }

        for (index, existing) in migrations {
            highest_index = Some(index);

            let new = match new_iter.next() {
                Some(migration) => migration,
                None => {
                    bail!(
                        "existing migration {} doesn't exist in local migrations",
                        existing
                    );
                }
            };

            if existing != new.name {
                bail!(
                    "existing migration {} does not match new migration {}",
                    existing,
                    new.name
                );
            }
        }
    }

    // Return the remaining migrations
    let items: Vec<Migration> = new_iter.collect();
    Ok(items)
}

async fn get_migrations(
    db: &mut impl Connection,
    index_larger_than: Option<i32>,
) -> anyhow::Result<Vec<(i32, String)>> {
    let rows = if let Some(index_larger_than) = index_larger_than {
        db.query_with_params(
            "
            SELECT index, name
            FROM reshape.migrations
            WHERE index > $1
            ORDER BY index ASC
            LIMIT 100
            ",
            &[&index_larger_than],
        ).await?
    } else {
        db.query(
            "
            SELECT index, name
            FROM reshape.migrations
            LIMIT 100
            ",
        ).await?
    };

    Ok(rows
        .iter()
        .map(|row| (row.get("index"), row.get("name")))
        .collect()
    )
}

pub async fn save_migrations(db: &mut impl Connection, migrations: &[Migration]) -> anyhow::Result<()> {
    for migration in migrations {
        let encoded_actions = serde_json::to_value(&migration.actions)?;
        db.query_with_params(
            "INSERT INTO reshape.migrations(name, description, actions) VALUES ($1, $2, $3)",
            &[&migration.name, &migration.description, &encoded_actions],
        ).await?;
    }

    Ok(())
}

pub fn schema_query_for_migration(migration_name: &str) -> String {
    let schema_name = schema_name_for_migration(migration_name);
    format!("SET search_path TO {}", schema_name)
}
