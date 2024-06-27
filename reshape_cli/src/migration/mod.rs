mod start;
mod status;
mod complete;
mod abort;

use clap::Parser;

use crate::connection;

#[derive(Parser)]
#[clap(about = "Commands for managing migrations")]
pub enum Command {
    #[clap(
        about = "Starts a new migration, applying any migrations which haven't yet been applied",
        display_order = 1
    )]
    Start(start::Options),

    #[clap(
        display_order = 2
    )]
    Status(status::Options),

    #[clap(
        about = "Completes an in-progress migration",
        display_order = 3
    )]
    Complete(connection::Options),

    #[clap(
        about = "Aborts an in-progress migration without losing any data",
        display_order = 4
    )]
    Abort(abort::Options),
}

pub fn command(cmd: Command) -> anyhow::Result<()> {
    match cmd {
        Command::Start(opts) => start::command(opts),
        Command::Status(opts) => status::command(opts),
        Command::Complete(opts) => complete::command(opts),
        Command::Abort(opts) => abort::command(opts),
    }
}
