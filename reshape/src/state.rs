use crate::{db::Connection, migration::Migration};

use serde::{Deserialize, Serialize};
use version::version;

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(tag = "state")]
pub enum State {
    #[serde(rename = "idle")]
    #[default]
    Idle,

    #[serde(rename = "applying")]
    Applying { migrations: Vec<Migration> },

    #[serde(rename = "in_progress")]
    InProgress { migrations: Vec<Migration> },

    #[serde(rename = "completing")]
    Completing {
        migrations: Vec<Migration>,
        current_migration_index: usize,
        current_action_index: usize,
    },

    #[serde(rename = "aborting")]
    Aborting {
        migrations: Vec<Migration>,
        last_migration_index: usize,
        last_action_index: usize,
    },
}

impl State {
    pub async fn load(db: &mut impl Connection) -> anyhow::Result<State> {
        Self::ensure_schema_and_table(db).await?;

        let results = db.query("SELECT value FROM reshape.data WHERE key = 'state'").await?;

        let state = match results.first() {
            Some(row) => {
                let json: serde_json::Value = row.get(0);
                serde_json::from_value(json)?
            }
            None => Default::default(),
        };
        Ok(state)
    }

    pub async fn save(&self, db: &mut impl Connection) -> anyhow::Result<()> {
        Self::ensure_schema_and_table(db).await?;

        let json = serde_json::to_value(self)?;
        db.query_with_params(
            "INSERT INTO reshape.data (key, value) VALUES ('state', $1) ON CONFLICT (key) DO UPDATE SET value = $1",
            &[&json]
        ).await?;
        Ok(())
    }

    pub async fn clear(&mut self, db: &mut impl Connection) -> anyhow::Result<()> {
        db.run("DROP SCHEMA reshape CASCADE").await?;

        *self = Self::default();

        Ok(())
    }

    pub fn applying(&mut self, new_migrations: Vec<Migration>) {
        *self = Self::Applying {
            migrations: new_migrations,
        };
    }

    pub fn in_progress(&mut self, new_migrations: Vec<Migration>) {
        *self = Self::InProgress {
            migrations: new_migrations,
        };
    }

    pub fn completing(
        &mut self,
        migrations: Vec<Migration>,
        current_migration_index: usize,
        current_action_index: usize,
    ) {
        *self = Self::Completing {
            migrations,
            current_migration_index,
            current_action_index,
        }
    }

    pub fn aborting(
        &mut self,
        migrations: Vec<Migration>,
        last_migration_index: usize,
        last_action_index: usize,
    ) {
        *self = Self::Aborting {
            migrations,
            last_migration_index,
            last_action_index,
        }
    }

    async fn ensure_schema_and_table(db: &mut impl Connection) -> anyhow::Result<()> {
        db.run("CREATE SCHEMA IF NOT EXISTS reshape").await?;

        // Create data table which will be a key-value table containing
        // the version and current state.
        db.run("CREATE TABLE IF NOT EXISTS reshape.data (key TEXT PRIMARY KEY, value JSONB)").await?;

        // Create migrations table which will store all completed migrations
        db.run(
            "
            CREATE TABLE IF NOT EXISTS reshape.migrations (
                index INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                actions JSONB NOT NULL,
                completed_at TIMESTAMP DEFAULT NOW()
            )
            ",
        ).await?;

        // Update the current version
        let encoded_version = serde_json::to_value(version!().to_owned())?;
        db.query_with_params(
            "
            INSERT INTO reshape.data (key, value)
            VALUES ('version', $1)
            ON CONFLICT (key) DO UPDATE SET value = $1
            ",
            &[&encoded_version],
        ).await?;

        Ok(())
    }
}
