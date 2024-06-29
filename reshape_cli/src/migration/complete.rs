use anyhow::{anyhow, Context};
use colored::Colorize;
use reshape::{
    db::Connection, state::State,
    actions::MigrationContext,
    migration::Migration,
    schema::drop_new_schema_func,
    schema_name_for_migration,
    current_migration,
    save_migrations,
};

use crate::connection::Options;

pub async fn command(opts: Options) -> anyhow::Result<()> {
    let mut reshape = opts.to_reshape_from_env().await?;

    let db = reshape.db.acquire_lock().await?;

    let mut state = State::load(db).await?;
    complete(db, &mut state).await?;

    reshape.db.release_lock().await
}

pub async fn complete(
    db: &mut impl Connection,
    state: &mut State
) -> anyhow::Result<()> {
    // Make sure a migration is in progress
    let (remaining_migrations, starting_migration_index, starting_action_index) = match state.clone() {
        State::InProgress { migrations } => {
            // Move into the Completing state. Once in this state,
            // the migration can't be aborted and must be completed.
            state.completing(migrations.clone(), 0, 0);
            state.save(db).await.context("failed to save state")?;

            (migrations, 0, 0)
        },
        State::Completing {
            migrations,
            current_migration_index,
            current_action_index
        } => (migrations, current_migration_index, current_action_index),
        State::Aborting { .. } => {
            return Err(anyhow!("migration been aborted and can't be completed. Please finish using `reshape migration abort`."))
        }
        State::Applying { .. } => {
            return Err(anyhow!("a previous migration unexpectedly failed. Please run `reshape migrate` to try applying the migration again."))
        }
        State::Idle => {
            println!("No migration in progress");
            return Ok(());
        }
    };

    // todo: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA

    // Remove previous migration's schema
    if let Some(current_migration) = &current_migration(db).await? {
        db.run(&format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            schema_name_for_migration(current_migration)
        )).await
        .context("failed to remove previous migration's schema")?;
    }

    for (migration_index, migration) in remaining_migrations.iter().enumerate() {
        // Skip all the migrations which have already been completed
        if migration_index < starting_migration_index {
            continue;
        }

        println!("Completing '{}':", migration.name);

        for (action_index, action) in migration.actions.iter().enumerate() {
            // Skip all actions which have already been completed
            if migration_index == starting_migration_index && action_index < starting_action_index {
                continue;
            }

            print!("  + {} ", action);

            let ctx = MigrationContext::new(migration_index, action_index, current_migration(db).await?);

            // Update state to indicate that this action has been completed.
            // We won't save this new state until after the action has completed.
            state.completing(
                remaining_migrations.clone(),
                migration_index + 1,
                action_index + 1,
            );


            let result = action
                .complete(&ctx, db).await
                .with_context(|| format!("failed to complete migration {}", migration.name))
                .with_context(|| format!("failed to complete action: {}", action));

            if let Err(e) = result {
                println!("{}", "failed".red());
                    return Err(e);
            }

            println!("{}", "done".green());

            state.save(db).await.context("failed to save state after completing action")?;
        }

        println!();
    }

    // Remove helpers which are no longer in use
    drop_new_schema_func(db).await.context("failed to tear down helpers")?;

    save_migrations(db, &remaining_migrations).await?;
    State::Idle.save(db).await?;

    *state = State::Idle;

    Ok(())
}
