mod common; use common::{
    Test, Task, setup_db,
    migrate, complete, abort
};

use reshape::migration::{Migration, Format};

#[tokio::test]
async fn remove_column() {
    let Test { mut reshape, mut old_db, mut new_db } = Test::connect().await;

    let first_migration = Migration::from_text(
        r#"
        name = "create_user_table"

        [[actions]]
        type = "create_table"
        name = "users"
        primary_key = ["id"]

            [[actions.columns]]
            name = "id"
            type = "INTEGER"

            [[actions.columns]]
            name = "name"
            type = "TEXT"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
        name = "remove_name_column"

        [[actions]]
        type = "remove_column"
        table = "users"
        column = "name"
        down = "'TEST_DOWN_VALUE'"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

    // Insert using old schema and ensure it can be retrieved through new schema
    old_db
        .simple_query("INSERT INTO users(id, name) VALUES (1, 'John Doe')")
        .await
        .unwrap();
    let results = new_db
        .query("SELECT id FROM users WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(1, results.len());
    assert_eq!(1, results[0].get::<_, i32>("id"));

    // Ensure the name column is not accesible through the new schema
    assert!(new_db.query("SELECT id, name FROM users", &[]).await.is_err());

    // Insert using new schema and ensure the down function is correctly applied
    new_db
        .simple_query("INSERT INTO users(id) VALUES (2)")
        .await
        .unwrap();
    let result = old_db
        .query_opt("SELECT name FROM users WHERE id = 2", &[])
        .await
        .unwrap();
    assert_eq!(
        Some("TEST_DOWN_VALUE"),
        result.as_ref().map(|row| row.get("name"))
    );
}

#[tokio::test]
async fn remove_column_with_index() {
    let Test { mut reshape, mut old_db, mut new_db } = Test::connect().await;

    let first_migration = Migration::from_text(
        r#"
        name = "create_user_table"

        [[actions]]
        type = "create_table"
        name = "users"
        primary_key = ["id"]

            [[actions.columns]]
            name = "id"
            type = "INTEGER"

            [[actions.columns]]
            name = "name"
            type = "TEXT"

        [[actions]]
        type = "add_index"
        table = "users"

            [actions.index]
            name = "name_idx"
            columns = ["name"]
        "#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
        name = "remove_name_column"

        [[actions]]
        type = "remove_column"
        table = "users"
        column = "name"
        down = "'TEST_DOWN_VALUE'"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();
    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

    complete(&mut reshape, &first_migration, &second_migration).await;
    complete(&mut reshape, &first_migration, &second_migration).await;

    // Ensure index has been removed after the migration is complete
    let count: i64 = new_db
        .query(
            "
            SELECT COUNT(*)
            FROM pg_catalog.pg_index
            JOIN pg_catalog.pg_class ON pg_index.indexrelid = pg_class.oid
            WHERE pg_class.relname = 'name_idx'
            ",
            &[],
        ).await
        .unwrap()
        .first()
        .map(|row| row.get(0))
        .unwrap();

    assert_eq!(0, count, "expected index to not exist");
}

#[tokio::test]
async fn remove_column_with_complex_down() {
    let Test { mut reshape, mut old_db, mut new_db } = Test::connect().await;

    let first_migration = Migration::from_text(
        r#"
        name = "create_tables"

        [[actions]]
        type = "create_table"
        name = "users"
        primary_key = ["id"]

            [[actions.columns]]
            name = "id"
            type = "INTEGER"

            [[actions.columns]]
            name = "email"
            type = "TEXT"

        [[actions]]
        type = "create_table"
        name = "profiles"
        primary_key = ["user_id"]

            [[actions.columns]]
            name = "user_id"
            type = "INTEGER"

            [[actions.columns]]
            name = "email"
            type = "TEXT"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
        name = "remove_users_email_column"

        [[actions]]
        type = "remove_column"
        table = "users"
        column = "email"

            [actions.down]
            table = "profiles"
            value = "profiles.email"
            where = "users.id = profiles.user_id"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    old_db.simple_query("INSERT INTO users (id, email) VALUES (1, 'test@example.com')").await.unwrap();
    old_db.simple_query("INSERT INTO profiles (user_id, email) VALUES (1, 'test@example.com')").await.unwrap();

    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();
    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

    new_db.simple_query("UPDATE profiles SET email = 'test2@example.com' WHERE user_id = 1").await.unwrap();

    // Ensure new email was propagated to users table in old schema
    let email: String = old_db
        .query(
            "
            SELECT email
            FROM users
            WHERE id = 1
            ",
            &[],
        ).await
        .unwrap()
        .first()
        .map(|row| row.get("email"))
        .unwrap();
    assert_eq!("test2@example.com", email);
}
