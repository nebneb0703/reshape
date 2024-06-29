use clap::Args;
use reshape::{
    db::Connection, state::State,
    migration::Migration,
    current_migration,
    remaining_migrations,
};

use crate::{
    connection,
    config,
};

#[derive(Args)]
pub struct Options {
    #[clap(flatten)]
    connection: connection::Options,

    #[clap(flatten)]
    config: config::Options,
}

pub async fn command(opts: Options) -> anyhow::Result<()> {
    let mut reshape = opts.connection.to_reshape_from_env().await?;
    let migrations = opts.config.find_migrations()?;

    let db = reshape.db.acquire_lock().await?;

    let state = State::load(db).await?;
    status(db, &state, migrations).await?;

    reshape.db.release_lock().await
}

pub async fn status(
    db: &mut impl Connection,
    state: &State,
    migrations: impl IntoIterator<Item = Migration>,
) -> anyhow::Result<()> {
    let remaining_migrations = remaining_migrations(db, migrations).await?;
    let current_migration = current_migration(db).await?;

    let current_migration = |space| if let Some(current_migration) = current_migration {
        println!("...");
        println!("[x]{}{}", "".repeat(space), current_migration);
    };

    match state {
        State::Idle => {
            println!("Status: Idle.");
            println!();

            current_migration(1);

            for migration in remaining_migrations {
                println!("[ ] {}", migration.name);
            }
        },
        State::Applying { migrations } | State::InProgress { migrations } => {
            let status = match state {
                State::Applying { .. } => "Applying",
                State::InProgress { .. } => "In Progress",
                _ => unreachable!(),
            };

            println!("Status: {}", status);
            println!();

            let mut valid_count = 0;

            for i in 0..remaining_migrations.len().max(migrations.len()) {
                valid_count = i + 1;

                if migrations.get(i).ne(&remaining_migrations.get(i)) {
                    valid_count -= 1;
                    break;
                }
            }

            let diverging = valid_count != migrations.len();

            if diverging {
                current_migration(4);

                for valid_migration in migrations[0..valid_count].iter() {
                    println!("[~]    {}", valid_migration.name);
                }

                println!(" +     Diverging...");
                println!(" |\\");
                println!(" | \\");
                println!(" +  +");

                let mut end = false;

                for i in valid_count..remaining_migrations.len().max(migrations.len()) {
                    if let Some(applied_migration) = migrations.get(i) {
                        // println!(" |  |  ");
                        println!(
                            "[~] {}  {}",
                            if end { ' ' } else { '|' },
                            applied_migration.name
                        );

                        if migrations.len() == i + 1 { end = true; }
                    }

                    if let Some(new_migration) = remaining_migrations.get(i) {
                        // println!(" |  |  ");
                        println!(
                            " {} [ ] {}",
                            if end { ' ' } else { '|' },
                            new_migration.name
                        );

                        if remaining_migrations.len() == i + 1 { end = true; }
                    }
                }
            } else {
                current_migration(1);

                for valid_migration in migrations {
                    println!("[~] {}", valid_migration.name);
                }

                for migration in remaining_migrations.get(valid_count..).into_iter().flatten() {
                    println!("[ ] {}", migration.name);
                }
            }
        },
        State::Completing { migrations, current_migration_index, .. } => {
            println!("Status: Completing");
            println!();

            let mut valid_count = 0;

            for i in 0..remaining_migrations.len().max(migrations.len()) {
                valid_count = i + 1;

                if migrations.get(i).ne(&remaining_migrations.get(i)) {
                    valid_count -= 1;
                    break;
                }
            }

            let diverging = valid_count != migrations.len();

            if diverging {
                current_migration(4);

                for valid_migration in migrations[0..valid_count].iter() {
                    println!("[x]    {}", valid_migration.name);
                }

                println!(" +     Diverging...");
                println!(" |\\");
                println!(" | \\");
                println!(" +  +");

                let mut end = false;

                for i in valid_count..remaining_migrations.len().max(migrations.len()) {
                    if let Some(applied_migration) = migrations.get(i) {
                        // println!(" |  |  ");
                        println!(
                            "[{}] {}  {}",
                            if i >= *current_migration_index { 'x' } else { '~' },
                            if end { ' ' } else { '|' },
                            applied_migration.name
                        );

                        if migrations.len() == i + 1 { end = true; }
                    }

                    if let Some(new_migration) = remaining_migrations.get(i) {
                        // println!(" |  |  ");
                        println!(
                            " {} [ ] {}",
                            if end { ' ' } else { '|' },
                            new_migration.name
                        );

                        if remaining_migrations.len() == i + 1 { end = true; }
                    }
                }
            } else {
                current_migration(1);

                for (i, valid_migration) in migrations.iter().enumerate() {
                    println!(
                        "[{}] {}",
                        if i >= *current_migration_index { 'x' } else { '~' },
                        valid_migration.name
                    );
                }

                for migration in remaining_migrations.get(valid_count..).into_iter().flatten() {
                    println!("[ ] {}", migration.name);
                }
            }
        },
        State::Aborting { migrations, last_migration_index, .. } => {
            println!("Status: Aborting");
            println!();

            let mut valid_count = 0;

            for i in 0..remaining_migrations.len().max(migrations.len()) {
                valid_count = i + 1;

                if migrations.get(i).ne(&remaining_migrations.get(i)) {
                    valid_count -= 1;
                    break;
                }
            }

            let diverging = valid_count != migrations.len();

            if diverging {
                current_migration(4);

                for valid_migration in migrations[0..valid_count].iter() {
                    println!("[~]    {}", valid_migration.name);
                }

                println!(" +     Diverging...");
                println!(" |\\");
                println!(" | \\");
                println!(" +  +");

                let mut end = false;

                for i in valid_count..remaining_migrations.len().max(migrations.len()) {
                    if let Some(applied_migration) = migrations.get(i) {
                        // println!(" |  |  ");
                        println!(
                            "[{}] {}  {}",
                            if i <= *last_migration_index { '~' } else { '@' },
                            if end { ' ' } else { '|' },
                            applied_migration.name
                        );

                        if migrations.len() == i + 1 { end = true; }
                    }

                    if let Some(new_migration) = remaining_migrations.get(i) {
                        // println!(" |  |  ");
                        println!(
                            " {} [ ] {}",
                            if end { ' ' } else { '|' },
                            new_migration.name
                        );

                        if remaining_migrations.len() == i + 1 { end = true; }
                    }
                }
            } else {
                current_migration(1);

                for (i, valid_migration) in migrations.iter().enumerate() {
                    println!(
                        "[{}] {}",
                        if i <= *last_migration_index { '~' } else { '@' },
                        valid_migration.name
                    );
                }

                for migration in remaining_migrations.get(valid_count..).into_iter().flatten() {
                    println!("[ ] {}", migration.name);
                }
            }
        },
    }

    Ok(())
}
