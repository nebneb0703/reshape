use std::{
    fs::{self, File},
    io::Read,
    path::Path,
};

use anyhow::{Context, anyhow};
use clap::{Args, Parser, ArgAction};
use reshape::{
    migrations::{Action, Migration},
    Reshape, Range
};
use serde::{Deserialize, Serialize};

#[derive(Parser)]
#[clap(name = "Reshape", version, about)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Parser)]
#[clap(about)]
enum Command {
    #[clap(subcommand)]
    Migration(MigrationCommand),

    #[clap(
        about = "Output the query your application should use to select the right schema",
        display_order = 2
    )]
    SchemaQuery(FindMigrationsOptions),

    #[clap(
        about = "Deprecated. Use `reshape schema-query` instead",
        display_order = 3
    )]
    GenerateSchemaQuery(FindMigrationsOptions),

    #[clap(
        about = "Deprecated. Use `reshape migration start` instead",
        display_order = 4
    )]
    Migrate(MigrateOptions),
    #[clap(
        about = "Deprecated. Use `reshape migration complete` instead",
        display_order = 5
    )]
    Complete(ConnectionOptions),
    #[clap(
        about = "Deprecated. Use `reshape migration abort` instead",
        display_order = 6
    )]
    Abort(ConnectionOptions),
}

#[derive(Parser)]
#[clap(about = "Commands for managing migrations", display_order = 1)]
enum MigrationCommand {
    #[clap(
        about = "Starts a new migration, applying any migrations which haven't yet been applied",
        display_order = 1
    )]
    Start(MigrateOptions),

    #[clap(display_order = 2)] // todo: add about
    Status(StatusOptions),

    #[clap(about = "Completes an in-progress migration", display_order = 3)]
    Complete(ConnectionOptions),

    #[clap(
        about = "Aborts an in-progress migration without losing any data",
        display_order = 4
    )]
    Abort(AbortOptions),
}

#[derive(Args)]
struct MigrateOptions {
    // Some comment
    #[clap(long, short)]
    complete: bool,
    #[clap(flatten)]
    connection_options: ConnectionOptions,
    #[clap(flatten)]
    find_migrations_options: FindMigrationsOptions,

    #[clap(flatten)]
    range: RangeOptions,
}

#[derive(Args)]
struct StatusOptions {
    #[clap(flatten)]
    connection_options: ConnectionOptions,
    #[clap(flatten)]
    find_migrations_options: FindMigrationsOptions,
}

#[derive(Parser)]
struct ConnectionOptions {
    #[clap(long)]
    url: Option<String>,
    #[clap(long, default_value = "localhost")]
    host: String,
    #[clap(long, default_value = "5432")]
    port: u16,
    #[clap(long, short, default_value = "postgres")]
    database: String,
    #[clap(long, short, default_value = "postgres")]
    username: String,
    #[clap(long, short, default_value = "postgres")]
    password: String,
}


#[derive(Args)]
struct AbortOptions {
    #[clap(flatten)]
    range: RangeOptions,

    #[clap(flatten)]
    connection: ConnectionOptions,
}

#[derive(Args)]
#[group(
    multiple = false,
    required = true,
)]
struct RangeOptions {
    #[clap(short, long, action = ArgAction::SetTrue)]
    all: bool,

    #[clap(short, long)]
    number: Option<usize>,

    migration: Option<String>,
}

impl From<RangeOptions> for Range {
    fn from(value: RangeOptions) -> Self {
        match value {
            RangeOptions { all: true, number: None, migration: None } => {
                Range::All
            },
            RangeOptions { all: false, number: Some(number), migration: None } => {
                Range::Number(number)
            },
            RangeOptions { all: false, number: None, migration: Some(migration) } => {
                Range::UpTo(migration)
            },
            _ => unreachable!("invalid abort options"),
        }
    }
}

#[derive(Parser)]
struct FindMigrationsOptions {
    #[clap(long, default_value = "migrations.plan")]
    plan: String
}

fn main() -> anyhow::Result<()> {
    let opts: Opts = Opts::parse();
    run(opts)
}

fn run(opts: Opts) -> anyhow::Result<()> {
    match opts.cmd {
        Command::Migration(MigrationCommand::Start(opts)) | Command::Migrate(opts) => {
            let mut reshape = reshape_from_connection_options(&opts.connection_options)?;
            let migrations = find_migrations(&opts.find_migrations_options)?;
            reshape.migrate(migrations, opts.range.into())?;

            // Automatically complete migration if --complete flag is set
            if opts.complete {
                reshape.complete()?;
            }

            Ok(())
        },
        Command::Migration(MigrationCommand::Status(opts)) => {
            let migrations = find_migrations(&opts.find_migrations_options)?;

            let mut reshape = reshape_from_connection_options(&opts.connection_options)?;

            reshape.status(migrations)
        }
        Command::Migration(MigrationCommand::Complete(opts)) | Command::Complete(opts) => {
            let mut reshape = reshape_from_connection_options(&opts)?;
            reshape.complete()
        },
        Command::Migration(MigrationCommand::Abort(opts)) => {
            let mut reshape = reshape_from_connection_options(&opts.connection)?;

            reshape.abort(opts.range.into())
        },
        Command::Abort(opts) => {
            let mut reshape = reshape_from_connection_options(&opts)?;

            reshape.abort(Range::All)
        },
        Command::SchemaQuery(opts) | Command::GenerateSchemaQuery(opts) => {
            let migrations = find_migrations(&opts)?;
            let query = migrations
                .last()
                .map(|migration| reshape::schema_query_for_migration(&migration.name));
            println!("{}", query.unwrap_or_else(|| "".to_string()));

            Ok(())
        },
    }
}

fn reshape_from_connection_options(opts: &ConnectionOptions) -> anyhow::Result<Reshape> {
    // Load environment variables from .env file if it exists
    dotenv::dotenv().ok();

    let url_env = std::env::var("DB_URL").ok();
    let url = url_env.as_ref().or_else(|| opts.url.as_ref());

    // Use the connection URL if it has been set
    if let Some(url) = url {
        return Reshape::new(url);
    }

    let host_env = std::env::var("DB_HOST").ok();
    let host = host_env.as_ref().unwrap_or_else(|| &opts.host);

    let port = std::env::var("DB_PORT")
        .ok()
        .and_then(|port| port.parse::<u16>().ok())
        .unwrap_or(opts.port);

    let username_env = std::env::var("DB_USERNAME").ok();
    let username = username_env.as_ref().unwrap_or_else(|| &opts.username);

    let password_env = std::env::var("DB_PASSWORD").ok();
    let password = password_env.as_ref().unwrap_or_else(|| &opts.password);

    let database_env = std::env::var("DB_NAME").ok();
    let database = database_env.as_ref().unwrap_or_else(|| &opts.database);

    Reshape::new_with_options(host, port, database, username, password)
}

fn find_migrations(opts: &FindMigrationsOptions) -> anyhow::Result<Vec<Migration>> {
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

fn decode_migration_file(data: &str, extension: &str) -> anyhow::Result<FileMigration> {
    let migration: FileMigration = match extension {
        "json" => serde_json::from_str(data)?,
        "toml" => toml::from_str(data)?,
        extension => {
            return Err(anyhow::anyhow!(
                "unrecognized file extension '{}'",
                extension
            ))
        }
    };

    Ok(migration)
}

#[derive(Serialize, Deserialize)]
struct FileMigration {
    name: Option<String>,
    description: Option<String>,
    actions: Vec<Box<dyn Action>>,
}
