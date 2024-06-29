use clap::Args;
use anyhow::{anyhow, bail, Context};
use colored::Colorize;
use reshape::{
    db::Connection, state::State,
    actions::MigrationContext,
    schema::drop_new_schema_func,
    schema_name_for_migration,
    current_migration,
};

use crate::{
    range::{self, Range},
    connection,
    migration::migrate,
};

#[derive(Args)]
pub struct Options {
    #[clap(flatten)]
    range: range::Options,

    #[clap(flatten)]
    connection: connection::Options,
}

pub async fn command(opts: Options) -> anyhow::Result<()> {
    let mut reshape = opts.connection.to_reshape_from_env().await?;

    let db = reshape.db.acquire_lock().await?;

    let mut state = State::load(db).await?;
    abort(db, &mut state, opts.range.into()).await?;

    reshape.db.release_lock().await
}

pub async fn abort(
    db: &mut impl Connection,
    state: &mut State,
    range: Range
) -> anyhow::Result<()> {
    let (remaining_migrations, last_migration_index, last_action_index) = match state.clone() {
        State::InProgress { migrations } | State::Applying { migrations } => {
            // Set to the Aborting state. Once this is done, the migration has to
            // be fully aborted and can't be completed.
            state.aborting(migrations.clone(), usize::MAX, usize::MAX);
            state.save(db).await?;

            (migrations, usize::MAX, usize::MAX)
        }
        State::Aborting {
            migrations,
            last_migration_index,
            last_action_index,
        } => {
            (migrations, last_migration_index, last_action_index)
        },
        State::Completing { .. } => {
            bail!("migration completion has already been started. Please run `reshape migration complete` again to finish it.");
        }
        State::Idle => {
            println!("No migration is in progress");
            return Ok(());
        }
    };

    let migrations_up_to_index = match range {
        Range::All => 0,
        Range::Number(number) => remaining_migrations.len() - number,
        Range::UpTo(migration) => remaining_migrations.iter()
            .position(|m| m.name == migration)
            .ok_or(anyhow!(
                "migration {} not in progress",
                migration
            ))?,
    };

    // Remove new migration's schema
    let target_migration = remaining_migrations.last().unwrap().name.to_string();
    let schema_name = schema_name_for_migration(&target_migration);
    db.run(&format!("DROP SCHEMA IF EXISTS {} CASCADE", schema_name,))
        .await.with_context(|| format!("failed to drop schema {}", schema_name))?;

    let mut ctx = MigrationContext::new(last_migration_index, last_action_index, current_migration(db).await?);

    // Abort all pending migrations
    // Abort all migrations in reverse order
    for (migration_index, migration) in remaining_migrations.iter().enumerate().rev() {
        // Skip migrations which shouldn't be aborted
        // The reason can be that they have already been aborted or that
        // the migration was never applied in the first place.
        if migration_index >= last_migration_index {
            continue;
        }

        if migration_index < migrations_up_to_index {
            break;
        }

        ctx.migration_index = migration_index;

        print!("Aborting '{}' ", migration.name);

        // todo: verify that this leads to correct state saving
        let result = migration.abort(db, &mut ctx).await
            .with_context(|| format!("failed to abort migration {}", migration.name));

        // Update state with which migrations and actions have been aborted.
        // We don't need to run this in a transaction as aborts are idempotent.
        state.aborting(remaining_migrations.to_vec(), ctx.migration_index, ctx.action_index);
        state.save(db).await.context("failed to save state")?;

        result?;

        println!("{}", "done".green());
    }

    drop_new_schema_func(db).await.context("failed to tear down helpers")?;

    *state = State::Idle;

    // todo: better condition
    if migrations_up_to_index != 0 {
        // Running migrations again is fine as they are idempotent.
        return Box::pin(migrate(db, state, remaining_migrations, Range::Number(migrations_up_to_index))).await; // todo: fix this
    }

    state.save(db).await.context("failed to save state")?;

    Ok(())
}
