use clap::Args;

#[derive(Args)]
pub struct Options {
    #[clap(long, default_value = "migrations.plan")]
    plan: String
}

impl Options {
    fn find_migrations(&self) -> anyhow::Result<Vec<Migration>> {
        let plan_file = fs::read_to_string(&opts.plan)?;

        let planned_migrations = plan_file.lines()
            .filter(|line| !line.trim().is_empty())
            .filter(|line| !line.trim().starts_with('#'));

        let mut migrations = Vec::with_capacity(plan_file.lines().count());

        for planned_migration in planned_migrations {
            let path = Path::new(planned_migration);

            let data = fs::read_to_string(path)?;

            let Some(extension) = path.extension().and_then(|ext| ext.to_str()) else {
                return Err(anyhow!(
                    "migration {} has no file extension",
                    path.to_string_lossy()
                ));
            };

            let file_migration = decode_migration_file(&data, extension).with_context(|| {
                format!("failed to parse migration file {}", path.display())
            })?;

            let file_name = path.file_stem().and_then(|name| name.to_str()).unwrap();
            migrations.push(Migration {
                name: file_migration.name.unwrap_or_else(|| file_name.to_string()),
                description: file_migration.description,
                actions: file_migration.actions,
            })
        }

        Ok(migrations)
    }
}
