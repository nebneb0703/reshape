use clap::{ Args, ArgAction };

use reshape::Range;

#[derive(Args)]
#[group(
    multiple = false,
    required = true,
)]
pub struct Options {
    #[clap(short, long, action = ArgAction::SetTrue)]
    all: bool,

    #[clap(short, long)]
    number: Option<usize>,

    migration: Option<String>,
}

impl From<Options> for Range {
    fn from(value: Options) -> Self {
        match value {
            Options { all: true, number: None, migration: None } => {
                Range::All
            },
            Options { all: false, number: Some(number), migration: None } => {
                Range::Number(number)
            },
            Options { all: false, number: None, migration: Some(migration) } => {
                Range::UpTo(migration)
            },
            _ => unreachable!("invalid abort options"),
        }
    }
}
