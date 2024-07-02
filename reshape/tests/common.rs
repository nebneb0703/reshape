use tokio_postgres::{Client, NoTls, connect};
use reshape::{
    migration::Migration,
    actions::MigrationContext,
    db::Connection,
    schema::{Schema, create_new_schema_func, drop_new_schema_func},
    Reshape, schema_query_for_migration, schema_name_for_migration,
};

pub struct Test {
    pub reshape: Reshape,
    pub old_db: Client,
    pub new_db: Client,
}

impl Test {
    pub async fn connect() -> Test {
        let connection_string = std::env::var("TEST_DB_URL")
            .unwrap_or("postgres://postgres:postgres@localhost/reshape_test".to_owned());

        let (old_db, conn1) = connect(&connection_string, NoTls).await.unwrap();
        let (new_db, conn2) = connect(&connection_string, NoTls).await.unwrap();

        let reshape = Reshape::new(&connection_string).await.unwrap();

        tokio::spawn(async move {
            conn1.await.unwrap();
        });

        tokio::spawn(async move {
            conn2.await.unwrap();
        });

        Test {
            reshape,
            old_db,
            new_db,
        }
    }
}

pub async fn setup_db(
    reshape: &mut Reshape,
    old_db: &mut Client,
    first_migration: &Migration,
) {
    let db = reshape.db.acquire_lock().await.unwrap();

    let migrations = db.query("SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'migration%'").await
        .unwrap()
        .into_iter()
        .map(| row| {
            let migration: String = row.get("schema_name");
            format!(r#"DROP SCHEMA IF EXISTS "{migration}" CASCADE;"#)
        });

    db.query(r#"DROP SCHEMA IF EXISTS "public" CASCADE;"#).await.unwrap();

    drop_new_schema_func(db).await.unwrap();

    for query in migrations {
        db.query(&query).await.unwrap();
    }

    db.query(r#"CREATE SCHEMA "public";"#).await.unwrap();
    db.query(r#"CREATE SCHEMA IF NOT EXISTS "reshape";"#).await.unwrap();

    let mut schema = Schema::new();
    let ctx = MigrationContext::new(0, 0, None);

    first_migration.migrate(db, &mut ctx.clone(), &mut schema).await.unwrap();
    schema.create_for_migration(db, &first_migration.name).await.unwrap();
    first_migration.complete(db, &mut ctx.clone()).await.unwrap();

    create_new_schema_func(db, &first_migration.name).await.unwrap();

    old_db.simple_query(&schema_query_for_migration(&first_migration.name)).await.unwrap();

    reshape.db.release_lock().await.unwrap();
}

pub async fn migrate(
    reshape: &mut Reshape,
    new_db: &mut Client,
    first_migration: &Migration,
    second_migration: &Migration,
) -> anyhow::Result<()> {
    let db = reshape.db.acquire_lock().await.unwrap();

    let mut schema = Schema::new();
    let ctx = MigrationContext::new(0, 0, Some(first_migration.name.clone()));

    second_migration.migrate(db, &mut ctx.clone(), &mut schema).await?;

    schema.create_for_migration(db, &second_migration.name).await.unwrap();

    new_db.simple_query(&schema_query_for_migration(&second_migration.name)).await.unwrap();

    create_new_schema_func(db, &second_migration.name).await.unwrap();

    reshape.db.release_lock().await.unwrap();

    Ok(())
}

pub async fn complete(
    reshape: &mut Reshape,
    first_migration: &Migration,
    second_migration: &Migration,
) {
    let db = reshape.db.acquire_lock().await.unwrap();

    db.query(&format!(
        r#"
        DROP SCHEMA IF EXISTS {} CASCADE;
        "#,
        schema_name_for_migration(&first_migration.name),
    )).await.unwrap();

    let ctx = MigrationContext::new(0, 0, Some(first_migration.name.clone()));

    second_migration.complete(db, &mut ctx.clone()).await.unwrap();

    reshape.db.release_lock().await.unwrap();
}

pub async fn abort(
    reshape: &mut Reshape,
    first_migration: &Migration,
    second_migration: &Migration,
) {
    let db = reshape.db.acquire_lock().await.unwrap();

    db.query(&format!(
        r#"
        DROP SCHEMA IF EXISTS {} CASCADE;
        "#,
        schema_name_for_migration(&second_migration.name),
    )).await.unwrap();

    let ctx = MigrationContext::new(0, usize::MAX, Some(first_migration.name.clone()));

    second_migration.abort(db, &mut ctx.clone()).await.unwrap();

    drop_new_schema_func(db).await.unwrap();

    reshape.db.release_lock().await.unwrap();
}

pub enum Task {
    Complete,
    Abort,
}

pub async fn assert_cleaned_up(db: &mut Client) {
    // Make sure no temporary columns remain
    let temp_columns: Vec<String> = db
        .query(
            "
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND column_name LIKE '__reshape%'
            ",
            &[],
        ).await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();

    assert!(
        temp_columns.is_empty(),
        "expected no temporary columns to exist, found: {}",
        temp_columns.join(", ")
    );

    // Make sure no triggers remain
    let triggers: Vec<String> = db
        .query(
            "
            SELECT trigger_name
            FROM information_schema.triggers
            WHERE trigger_schema = 'public'
            AND trigger_name LIKE '__reshape%'
            ",
            &[],
        ).await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();

    assert!(
        triggers.is_empty(),
        "expected no triggers to exist, found: {}",
        triggers.join(", ")
    );

    // Make sure no functions remain
    let functions: Vec<String> = db
        .query(
            "
            SELECT routine_name
            FROM information_schema.routines
            WHERE routine_schema = 'public'
            AND routine_name LIKE '__reshape%'
            ",
            &[],
        ).await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();

    assert!(
        functions.is_empty(),
        "expected no functions to exist, found: {}",
        functions.join(", ")
    );
}
