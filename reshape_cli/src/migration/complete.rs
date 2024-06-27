use crate::connection::Options;

pub fn command(opts: Options) -> anyhow::Result<()> {
    let mut reshape = reshape_from_connection_options(&opts)?;
    reshape.complete()
}
