use clap::Args;

use crate::{ connection, range, config };

#[derive(Args)]
pub struct Options {
    #[clap(long, short)]
    complete: bool,

    #[clap(flatten)]
    range: range::Options,

    #[clap(flatten)]
    connection: connection::Options,

    #[clap(flatten)]
    config: config::Options,
}

pub fn command(opts: Options) -> anyhow::Result<()> {
    let mut reshape = reshape_from_connection_options(&opts.connection_options)?;
    let migrations = find_migrations(&opts.find_migrations_options)?;
    reshape.migrate(migrations, opts.range.into())?;

    // Automatically complete migration if --complete flag is set
    if opts.complete {
        reshape.complete()?;
    }

    Ok(())
}
