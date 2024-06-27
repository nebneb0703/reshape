use clap::Args;

use crate::{ connection, config };

#[derive(Args)]
pub struct Options {
    #[clap(flatten)]
    connection: connection::Options,

    #[clap(flatten)]
    config: config::Options,
}

pub fn command(opts: Options) -> anyhow::Result<()> {
    let migrations = find_migrations(&opts.find_migrations_options)?;

    let mut reshape = reshape_from_connection_options(&opts.connection_options)?;

    reshape.status(migrations)
}
