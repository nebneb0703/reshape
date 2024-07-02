mod migration;
mod connection;
mod config;
mod range;

use clap::Parser;

#[derive(Parser)]
#[clap(name = "Reshape", version, about)]
struct Args {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Parser)]
#[clap(about)]
enum Command {
    #[clap(subcommand, display_order = 1)]
    Migration(migration::Command),

    #[clap(
        about = "Output the query your application should use to select the right schema",
        display_order = 2
    )]
    SchemaQuery(config::Options),
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let args: Args = Args::parse();

    match args.cmd {
        Command::Migration(cmd) => migration::command(cmd).await,
        Command::SchemaQuery(opts) => {
            todo!();
            // let migrations = find_migrations(&opts)?;
            // let query = migrations
            //     .last()
            //     .map(|migration| reshape::schema_query_for_migration(&migration.name));
            // println!("{}", query.unwrap_or(""));

            // Ok(())
        },
    }
}
