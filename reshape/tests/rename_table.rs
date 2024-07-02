mod common; use common::{
    Test, Task, setup_db,
    migrate, complete, abort
};

use reshape::{
    migration::{Migration, Format},
    schema::{Schema, create_new_schema_func, drop_new_schema_func},
    actions::MigrationContext,
    db::Connection,
    schema_query_for_migration, schema_name_for_migration,
};

#[tokio::test]
async fn rename_table() {
    let Test { mut reshape, mut old_db, mut new_db } = Test::connect().await;

    let first_migration = Migration::from_text(
        r#"
        name = "create_users_table"

        [[actions]]
        type = "create_table"
        name = "users"
        primary_key = ["id"]

            [[actions.columns]]
            name = "id"
            type = "INTEGER"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
        name = "rename_users_table_to_customers"

        [[actions]]
        type = "rename_table"
        table = "users"
        new_name = "customers"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();
    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

    // Make sure inserts work using both the old and new name
    old_db.simple_query("INSERT INTO users(id) VALUES (1)").await.unwrap();
    new_db.simple_query("INSERT INTO customers(id) VALUES (2)").await.unwrap();

    // Ensure the table can be queried using both the old and new name
    let expected: Vec<i32> = vec![1, 2];
    assert_eq!(
        expected,
        old_db
            .query("SELECT id FROM users ORDER BY id", &[])
            .await
            .unwrap()
            .iter()
            .map(|row| row.get::<_, i32>("id"))
            .collect::<Vec<i32>>()
    );
    assert_eq!(
        expected,
        new_db
            .query("SELECT id FROM customers ORDER BY id", &[])
            .await
            .unwrap()
            .iter()
            .map(|row| row.get::<_, i32>("id"))
            .collect::<Vec<i32>>()
    );

    // Ensure the table can't be queried using the wrong name for the schema
    assert!(old_db.simple_query("SELECT id FROM customers").await.is_err());
    assert!(new_db.simple_query("SELECT id FROM users").await.is_err());
}

#[tokio::test]
async fn rename_abort() {
    let Test { mut reshape, mut old_db, mut new_db } = Test::connect().await;

    let first_migration = Migration::from_text(
        r#"
        name = "create_users_table"

        [[actions]]
        type = "create_table"
        name = "users"
        primary_key = ["id"]

            [[actions.columns]]
            name = "id"
            type = "INTEGER"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
        name = "rename_users_table_to_customers"

        [[actions]]
        type = "rename_table"
        table = "users"
        new_name = "customers"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    let third_migration = Migration::from_text(
        r#"
        name = "add_name_column_to_customers"

        [[actions]]
        type = "add_column"
        table = "customers"

        up = "'User ' || id"

                [actions.column]
                name = "name"
                type = "TEXT"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    // Insert test data
    old_db.simple_query("INSERT INTO users(id) VALUES (1)").await.unwrap();

    {
        let db = reshape.db.acquire_lock().await.unwrap();

        let mut schema = Schema::new();
        let mut ctx = MigrationContext::new(0, 0, Some(first_migration.name.clone()));

        second_migration.migrate(db, &mut ctx, &mut schema.clone()).await.unwrap();
        ctx.migration_index -= 1;
        second_migration.migrate(db, &mut ctx, &mut schema).await.unwrap();

        third_migration.migrate(db, &mut ctx, &mut schema.clone()).await.unwrap();
        ctx.migration_index -= 1;
        third_migration.migrate(db, &mut ctx, &mut schema).await.unwrap();

        schema.create_for_migration(db, &third_migration.name).await.unwrap();

        new_db.simple_query(&schema_query_for_migration(&third_migration.name)).await.unwrap();

        create_new_schema_func(db, &third_migration.name).await.unwrap();

        reshape.db.release_lock().await.unwrap();
    }

    // Make sure inserts work using both the old and new name
    old_db.simple_query("INSERT INTO users(id) VALUES (2)").await.unwrap();
    new_db.simple_query("INSERT INTO customers(id, name) VALUES (3, 'John')").await.unwrap();

    // Ensure the table can be queried using both the old and new name
    assert_eq!(
        [1, 2, 3].as_slice(),
        old_db
            .query("SELECT id FROM users ORDER BY id", &[])
            .await
            .unwrap()
            .iter()
            .map(|row| row.get::<_, i32>("id"))
            .collect::<Vec<i32>>().as_slice()
    );
    assert_eq!(
        [
            (1, "User 1".to_owned()),
            (2, "User 2".to_owned()),
            (3, "John".to_owned()),
        ].as_slice(),
        new_db
            .query("SELECT id, name FROM customers ORDER BY id", &[])
            .await
            .unwrap()
            .iter()
            .map(|row| (row.get::<_, i32>("id"), row.get::<_, String>("name")))
            .collect::<Vec<(i32, String)>>().as_slice()
    );

    // Ensure the table can't be queried using the wrong name for the schema
    assert!(old_db.simple_query("SELECT id FROM customers").await.is_err());
    assert!(new_db.simple_query("SELECT id FROM users").await.is_err());

    {
        let db = reshape.db.acquire_lock().await.unwrap();

        let mut ctx = MigrationContext::new(1, usize::MAX, Some(first_migration.name.clone()));

        third_migration.abort(db, &mut ctx).await.unwrap();
        ctx.migration_index += 1;
        third_migration.abort(db, &mut ctx).await.unwrap();

        second_migration.abort(db, &mut ctx).await.unwrap();
        ctx.migration_index += 1;
        second_migration.abort(db, &mut ctx).await.unwrap();

        drop_new_schema_func(db).await.unwrap();

        db.query(&format!(
            r#"
            DROP SCHEMA IF EXISTS {} CASCADE;
            "#,
            schema_name_for_migration(&third_migration.name),
        )).await.unwrap();

        new_db.simple_query(&schema_query_for_migration(&first_migration.name)).await.unwrap();

        create_new_schema_func(db, &first_migration.name).await.unwrap();

        reshape.db.release_lock().await.unwrap();
    }

    assert_eq!(
        [1, 2, 3].as_slice(),
        old_db
            .query("SELECT id FROM users ORDER BY id", &[])
            .await
            .unwrap()
            .iter()
            .map(|row| row.get::<_, i32>("id"))
            .collect::<Vec<i32>>().as_slice()
    );

    assert!(new_db.query("SELECT id FROM customers", &[]).await.is_err());
}
