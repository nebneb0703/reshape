use std::{fs, path::Path};

use clap::Args;
use anyhow::Context;

use reshape::migration::Migration;

#[derive(Args)]
pub struct Options {
    #[clap(long, default_value = "migrations.plan")]
    plan: String
}

impl Options {
    pub fn find_migrations(&self) -> anyhow::Result<Vec<Migration>> {
        let plan_file = fs::read_to_string(&self.plan)?;

        let planned_migrations = plan_file.lines()
            .filter(|line| !line.trim().is_empty())
            .filter(|line| !line.trim().starts_with('#'));

        let mut migrations = Vec::with_capacity(plan_file.lines().count());

        for planned_migration in planned_migrations {
            let path = Path::new(planned_migration);

            let migration = Migration::from_file(path, None).with_context(|| {
                format!("failed to parse migration file {}", path.display())
            })?;

            migrations.push(migration)
        }

        Ok(migrations)
    }
}
