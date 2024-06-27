use clap::Args;

use crate::{ connection, range };

#[derive(Args)]
pub struct Options {
    #[clap(flatten)]
    range: range::Options,

    #[clap(flatten)]
    connection: connection::Options,
}

pub fn command(opts: Options) -> anyhow::Result<()> {
    let mut reshape = reshape_from_connection_options(&opts.connection)?;

    reshape.abort(opts.range.into())
}
