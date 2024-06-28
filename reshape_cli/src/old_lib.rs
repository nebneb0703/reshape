use crate::{
    migrations::{Migration, MigrationContext},
    schema::Schema,
};

use anyhow::{anyhow, Context};
use colored::*;
use db::{Conn, Postgres, Lock};
use postgres::Config;
use schema::Table;

mod db;
mod helpers;
pub mod migration;
pub mod actions;
mod schema;
mod state;

pub use crate::state::State;

pub struct Reshape {
    db: Lock,
}

impl Reshape {
    pub fn new(connection_string: &str) -> anyhow::Result<Reshape> {
        let config: Config = connection_string.parse()?;
        Self::new_with_config(&config)
    }

    pub fn new_with_options(
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

        Self::new_with_config(&config)
    }

    fn new_with_config(config: &Config) -> anyhow::Result<Reshape> {
        let db = Lock::connect(config)?;
        Ok(Reshape { db })
    }

    pub fn status(
        &mut self,
        migrations: impl IntoIterator<Item = Migration>,
    ) -> anyhow::Result<()> {
        self.db.lock(|db| {
            let state = State::load(db)?;
            status(db, &state, migrations)
        })
    }

    pub fn migrate(
        &mut self,
        migrations: impl IntoIterator<Item = Migration>,
        range: Range,
    ) -> anyhow::Result<()> {
        self.db.lock(|db| {
            let mut state = State::load(db)?;
            migrate(db, &mut state, migrations, range)
        })
    }

    pub fn complete(&mut self) -> anyhow::Result<()> {
        self.db.lock(|db| {
            let mut state = State::load(db)?;
            complete(db, &mut state)
        })
    }

    pub fn abort(&mut self, range: Range) -> anyhow::Result<()> {
        self.db.lock(|db| {
            let mut state = State::load(db)?;
            abort(db, &mut state, range)
        })
    }

    pub fn remove(&mut self) -> anyhow::Result<()> {
        self.db.lock(|db| {
            let mut state = State::load(db)?;

            // Remove migration schemas and views
            if let Some(current_migration) = &state::current_migration(db)? {
                db.run(&format!(
                    "DROP SCHEMA IF EXISTS {} CASCADE",
                    schema_name_for_migration(current_migration)
                ))?;
            }

            if let State::InProgress { migrations } = &state {
                let target_migration = migrations.last().unwrap().name.to_string();
                db.run(&format!(
                    "DROP SCHEMA IF EXISTS {} CASCADE",
                    schema_name_for_migration(&target_migration)
                ))?;
            }

            // Remove all tables
            let schema = Schema::new();
            for table in schema.get_tables(db)? {
                db.run(&format!(
                    r#"
                    DROP TABLE IF EXISTS "{}" CASCADE
                    "#,
                    table.real_name
                ))?;
            }

            // Remove all enums
            let enums: Vec<String> = db
                .query("SELECT typname FROM pg_type WHERE typcategory = 'E'")?
                .iter()
                .map(|row| row.get("typname"))
                .collect();
            for enum_type in enums {
                db.run(&format!("DROP TYPE {}", enum_type))?;
            }

            // Reset state
            state.clear(db)?;

            println!("Reshape and all data has been removed");

            Ok(())
        })
    }
}

pub enum Range {
    All,
    Number(usize),
    UpTo(String),
}

pub fn latest_schema_from_migrations(migrations: &[Migration]) -> Option<String> {
    migrations
        .last()
        .map(|migration| schema_name_for_migration(&migration.name))
}
