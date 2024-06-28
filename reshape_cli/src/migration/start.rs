use clap::Args;
use anyhow::{anyhow, bail, Context};
use colored::Colorize;
use reshape::{
    db::Connection, state::State,
    actions::MigrationContext,
    migration::Migration,
    schema::{Schema, create_new_schema_func},
    schema_name_for_migration,
    schema_query_for_migration,
    current_migration,
    remaining_migrations,
};

use crate::{
    range::{self, Range},
    migration::{abort, complete},
    connection,
    config,
};

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

pub async fn command(opts: Options) -> anyhow::Result<()> {
    let mut reshape = opts.connection.to_reshape_from_env().await?;
    let migrations = opts.config.find_migrations()?;

    let db = reshape.db.acquire_lock().await?;

    let mut state = State::load(db).await?;
    migrate(db, &mut state, migrations, opts.range.into()).await?;

    // Automatically complete migration if --complete flag is set
    if opts.complete {
        complete(db, &mut state).await?;
    }

    reshape.db.release_lock().await
}

pub async fn migrate(
    db: &mut impl Connection,
    state: &mut State,
    migrations: impl IntoIterator<Item = Migration>,
    range: Range,
) -> anyhow::Result<()> {
    // Make sure no migration is in progress
    if let State::Completing { .. } = &state {
        println!(
            "Migration already in progress and has started completion, please finish using 'reshape migration complete'"
        );
        return Ok(());
    }

    if let State::Aborting { .. } = &state {
        bail!(
            "Migration has begun aborting, please finish using `reshape migration abort`"
        );
    }

    // Determine which migrations need to be applied by comparing the provided migrations
    // with the already applied ones stored in the state. This will throw an error if the
    // two sets of migrations don't agree, for example if a new migration has been added
    // in between two existing ones.
    let mut remaining_migrations = remaining_migrations(db, migrations).await?;

    if let Range::UpTo(migration) = &range {
        let index = remaining_migrations.iter()
            .position(|m| &m.name == migration)
            .ok_or(anyhow!(
                "migration {} not found",
                migration
            ))?;

        remaining_migrations.resize_with(index + 1, || unreachable!());
    };

    if let State::InProgress { migrations: existing_migrations } = state.clone() {
        // If we have already started applying some migrations we need to ensure that
        // they are the same ones we want to apply now
        if Some(existing_migrations.as_slice()) != remaining_migrations.get(0..existing_migrations.len()) {
            return Err(anyhow!(
                "a previous migration is already in progress, and diverges from new migrations. Please run `reshape migration abort` and then run migrate again."
            ))
        }

        if existing_migrations.len() == remaining_migrations.len() {
            println!("Migration already in progress, please complete using 'reshape migration complete'");

            return Ok(());
        }

        if let Range::Number(n) = &range {
            remaining_migrations.resize_with(remaining_migrations.len().min(n + existing_migrations.len()), || unreachable!());
        };

        if remaining_migrations.is_empty() {
            println!("No migrations left to apply");
            return Ok(());
        }

        state.in_progress(remaining_migrations.clone());

        // "Abort" the current schema, and continue with a new one.
        // This will drop the existing schema, abort the new, still unapplied migrations
        // (which is safe because they are idempotent), and then rerun the migration,
        // now in the "Applying" state.

        let target_migration = &existing_migrations.last().unwrap().name;

        // Drop the existing schema here, as the migrations list changes and won't be
        // correct in the function.
        let schema_name = schema_name_for_migration(target_migration);
        db.run(&format!("DROP SCHEMA IF EXISTS {} CASCADE", schema_name,))
            .await.with_context(|| format!("failed to drop schema {}", schema_name))?;

        return abort(db, state, Range::Number(0)).await;
    }

    if let State::Applying { migrations: existing_migrations } = &state {
        if existing_migrations != &remaining_migrations[0..existing_migrations.len()] {
            return Err(anyhow!(
                "a previous migration seems to have failed without cleaning up. Please run `reshape migration abort` and then run migrate again."
            ));
        }

        if let Range::Number(n) = &range {
            remaining_migrations.resize_with(remaining_migrations.len().min(n + existing_migrations.len()), || unreachable!());
        };

        if remaining_migrations.is_empty() {
            println!("No migrations left to apply");
            return Ok(());
        }
    }

    // Move to the "Applying" state which is necessary as we can't run the migrations
    // and state update as a single transaction. If a migration unexpectedly fails without
    // automatically aborting, this state saves us from dangling migrations. It forces the user
    // to either run migrate again (which works as all migrations are idempotent) or abort.
    state.applying(remaining_migrations.clone());
    state.save(db).await?;

    println!("Applying {} migrations\n", remaining_migrations.len());

    let target_migration = remaining_migrations.last().unwrap().name.to_string();
    create_new_schema_func(db, &target_migration).await.context("failed to set up helpers")?;

    let mut new_schema = Schema::new();
    let mut last_migration_index = usize::MAX;
    let mut last_action_index = usize::MAX;
    let mut result: anyhow::Result<()> = Ok(());

    'outer: for (migration_index, migration) in remaining_migrations.iter().enumerate() {
        println!("Migrating '{}':", migration.name);
        last_migration_index = migration_index;

        for (action_index, action) in migration.actions.iter().enumerate() {
            last_action_index = action_index;

            print!("  + {} ", action);

            let ctx = MigrationContext::new(migration_index, action_index, current_migration(db).await?);

            result = action.run(&ctx, db, &new_schema).await.with_context(|| format!("failed to {}", action));

            if result.is_ok() {
                action.update_schema(&ctx, &mut new_schema);
                println!("{}", "done".green());
            } else {
                println!("{}", "failed".red());
                break 'outer;
            }
        }

        println!();
    }

    // If a migration failed, we abort all the migrations that were applied
    if let Err(err) = result {
        println!("Migration failed, aborting.");

        println!("ERROR: {err:#?}");

        // Set to the Aborting state. This is to ensure that the failed
        // migration is fully aborted and nothing is left dangling.
        // If the abort is interrupted for any reason, the user can try again
        // by running `reshape migration abort`.
        state.aborting(
            remaining_migrations.clone(),
            last_migration_index + 1,
            last_action_index + 1,
        );

        abort(db, state, Range::Number(remaining_migrations.len() - last_migration_index + 1)).await?;

        return Err(err);
    }

    // Create schema and views for migration
    new_schema.create_for_migration(db, &target_migration)
        .await.with_context(|| format!("failed to create schema for migration {}", target_migration))?;

    // Update state once migrations have been performed
    state.in_progress(remaining_migrations);
    state.save(db).await.context("failed to save in-progress state")?;

    println!("Migrations have been applied and the new schema is ready for use:");
    println!(
        "  - Run '{}' from your application to use the latest schema",
        schema_query_for_migration(&target_migration)
    );
    println!(
        "  - Run 'reshape migration complete' once your application has been updated and the previous schema is no longer in use"
    );
    Ok(())
}
