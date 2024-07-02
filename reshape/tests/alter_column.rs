mod common; use common::{
    Test, Task, setup_db,
    migrate, complete, abort
};

use reshape::migration::{Migration, Format};

#[tokio::test]
async fn alter_column_data() {
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
        name = "uppercase_name"

        [[actions]]
        type = "alter_column"
        table = "users"
        column = "name"
        up = "UPPER(name)"
        down = "LOWER(name)"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    // Insert some test users
    old_db.simple_query(
        "
        INSERT INTO users (id, name) VALUES
            (1, 'john Doe'),
            (2, 'jane Doe');
        ",
    ).await
    .unwrap();

    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();
    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

    // Check that the existing users has the altered data
    let expected = vec!["JOHN DOE", "JANE DOE"];
    assert!(new_db
        .query("SELECT name FROM users ORDER BY id", &[],)
        .await
        .unwrap()
        .iter()
        .map(|row| row.get::<_, String>("name"))
        .eq(expected));

    // Insert data using old schema and make sure the new schema gets correct values
    old_db
        .simple_query("INSERT INTO users (id, name) VALUES (3, 'test testsson')")
        .await
        .unwrap();
    let result = new_db
        .query_one("SELECT name from users WHERE id = 3", &[])
        .await
        .unwrap();
    assert_eq!("TEST TESTSSON", result.get::<_, &str>("name"));

    // Insert data using new schema and make sure the old schema gets correct values
    new_db
        .simple_query("INSERT INTO users (id, name) VALUES (4, 'TEST TESTSSON')")
        .await
        .unwrap();
    let result = old_db
        .query_one("SELECT name from users WHERE id = 4", &[])
        .await
        .unwrap();
    assert_eq!("test testsson", result.get::<_, &str>("name"));
}

#[tokio::test]
async fn alter_column_set_not_null() {
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
        name = "set_name_not_null"

        [[actions]]
        type = "alter_column"
        table = "users"
        column = "name"
        up = "COALESCE(name, 'TEST_DEFAULT_VALUE')"

            [actions.changes]
            nullable = false
        "#,
        None,
        Format::Toml,
    ).unwrap();

    for task in [Task::Complete, Task::Abort] {
        setup_db(&mut reshape, &mut old_db, &first_migration).await;

        // Insert some test users
        old_db.simple_query(
            "
            INSERT INTO users (id, name) VALUES
                (1, 'John Doe'),
                (2, NULL);
            ",
        ).await
        .unwrap();

        migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();
        migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

        // Check that existing users got the correct values
        let expected = vec!["John Doe", "TEST_DEFAULT_VALUE"];
        assert!(new_db
            .query("SELECT name FROM users ORDER BY id", &[],)
            .await
            .unwrap()
            .iter()
            .map(|row| row.get::<_, String>("name"))
            .eq(expected));

        // Insert data using old schema and make sure the new schema gets correct values
        old_db
            .simple_query("INSERT INTO users (id, name) VALUES (3, NULL)")
            .await
            .unwrap();
        let result = new_db
            .query_one("SELECT name from users WHERE id = 3", &[])
            .await
            .unwrap();
        assert_eq!("TEST_DEFAULT_VALUE", result.get::<_, &str>("name"));

        // Insert data using new schema and make sure the old schema gets correct values
        new_db
            .simple_query("INSERT INTO users (id, name) VALUES (4, 'Jane Doe')")
            .await
            .unwrap();
        let result = old_db
            .query_one("SELECT name from users WHERE id = 4", &[])
            .await
            .unwrap();
        assert_eq!("Jane Doe", result.get::<_, &str>("name"));

        // Ensure NULL can't be inserted using the new schema
        let result = new_db.simple_query("INSERT INTO users (id, name) VALUES (5, NULL)").await;
        assert!(result.is_err(), "expected insert to fail");


        match task {
            Task::Complete => {
                complete(&mut reshape, &first_migration, &second_migration).await;
                complete(&mut reshape, &first_migration, &second_migration).await;

                // Ensure NULL can't be inserted
                let result = new_db.simple_query("INSERT INTO users (id, name) VALUES (5, NULL)").await;
                assert!(result.is_err(), "expected insert to fail");
            },
            Task::Abort => {
                abort(&mut reshape, &first_migration, &second_migration).await;
                abort(&mut reshape, &first_migration, &second_migration).await;

                // Ensure NULL can be inserted
                let result = old_db.simple_query("INSERT INTO users (id, name) VALUES (5, NULL)").await;
                assert!(result.is_ok(), "expected insert to succeed");
            },
        }
    }
}

#[tokio::test]
async fn alter_column_set_nullable() {
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
            nullable = false
        "#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
        name = "set_name_nullable"

        [[actions]]
        type = "alter_column"
        table = "users"
        column = "name"
        down = "COALESCE(name, 'TEST_DEFAULT_VALUE')"

            [actions.changes]
            nullable = true
        "#,
        None,
        Format::Toml,
    ).unwrap();

    for task in [Task::Complete, Task::Abort] {
        setup_db(&mut reshape, &mut old_db, &first_migration).await;

        // Insert a test user
        old_db.simple_query(
            "
            INSERT INTO users (id, name) VALUES
                (1, 'John Doe')
            ",
        ).await
        .unwrap();

        migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();
        migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

        // Insert data using new schema and make sure the old schema gets correct values
        new_db
            .simple_query("INSERT INTO users (id, name) VALUES (2, NULL)")
            .await
            .unwrap();
        let result = old_db
            .query_one("SELECT name from users WHERE id = 2", &[])
            .await
            .unwrap();
        assert_eq!("TEST_DEFAULT_VALUE", result.get::<_, &str>("name"));

        // Ensure NULL can't be inserted using the old schema
        let result = old_db.simple_query("INSERT INTO users (id, name) VALUES (3, NULL)").await;
        assert!(result.is_err(), "expected insert to fail");

        // Ensure NULL can be inserted using the new schema
        let result = new_db.simple_query("INSERT INTO users (id, name) VALUES (4, NULL)").await;
        assert!(result.is_ok(), "expected insert to succeed");

        match task {
            Task::Complete => {
                complete(&mut reshape, &first_migration, &second_migration).await;
                complete(&mut reshape, &first_migration, &second_migration).await;

                // Ensure NULL can be inserted
                let result = new_db.simple_query("INSERT INTO users (id, name) VALUES (5, NULL)").await;
                assert!(result.is_ok(), "expected insert to succeed");
            },
            Task::Abort => {
                abort(&mut reshape, &first_migration, &second_migration).await;
                abort(&mut reshape, &first_migration, &second_migration).await;

                // Ensure NULL can't be inserted
                let result = old_db.simple_query("INSERT INTO users (id, name) VALUES (5, NULL)").await;
                assert!(result.is_err(), "expected insert to fail");
            },
        }
    }
}

#[tokio::test]
async fn alter_column_rename() {
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
        name = "set_name_not_null"

        [[actions]]
        type = "alter_column"
        table = "users"
        column = "name"

            [actions.changes]
            name = "full_name"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    // Insert some test data
    old_db.simple_query(
        "
        INSERT INTO users (id, name) VALUES
            (1, 'John Doe'),
            (2, 'Jane Doe');
        ",
    ).await
    .unwrap();

    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();
    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

    // Check that existing values can be queried using new column name
    let expected = vec!["John Doe", "Jane Doe"];
    assert!(new_db
        .query("SELECT full_name FROM users ORDER BY id", &[],)
        .await
        .unwrap()
        .iter()
        .map(|row| row.get::<_, String>("full_name"))
        .eq(expected));
}

#[tokio::test]
async fn alter_column_multiple() {
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
            name = "counter"
            type = "INTEGER"
            nullable = false
        "#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
        name = "increment_counter_twice"

        [[actions]]
        type = "alter_column"
        table = "users"
        column = "counter"
        up = "counter + 1"
        down = "counter - 1"

        [[actions]]
        type = "alter_column"
        table = "users"
        column = "counter"
        up = "counter + 1"
        down = "counter - 1"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    // Insert some test data
    old_db.simple_query(
        "
        INSERT INTO users (id, counter) VALUES
            (1, 0),
            (2, 100);
        ",
    ).await
    .unwrap();

    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();
    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

    // Check that the existing data has been updated
    let expected = vec![2, 102];
    let results: Vec<i32> = new_db
        .query("SELECT counter FROM users ORDER BY id", &[])
        .await
        .unwrap()
        .iter()
        .map(|row| row.get::<_, i32>("counter"))
        .collect();
    assert_eq!(expected, results);

    // Update data using old schema and make sure it was updated for the new schema
    old_db
        .query("UPDATE users SET counter = 50 WHERE id = 1", &[])
        .await
        .unwrap();
    let result: i32 = new_db
        .query("SELECT counter FROM users WHERE id = 1", &[])
        .await
        .unwrap()
        .iter()
        .map(|row| row.get("counter"))
        .nth(0)
        .unwrap();
    assert_eq!(52, result);

    // Update data using new schema and make sure it was updated for the old schema
    new_db
        .query("UPDATE users SET counter = 50 WHERE id = 1", &[])
        .await
        .unwrap();
    let result: i32 = old_db
        .query("SELECT counter FROM users WHERE id = 1", &[])
        .await
        .unwrap()
        .iter()
        .map(|row| row.get("counter"))
        .nth(0)
        .unwrap();
    assert_eq!(48, result);
}

#[tokio::test]
async fn alter_column_default() {
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
            nullable = false
            default = "'DEFAULT'"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
        name = "change_name_default"

        [[actions]]
        type = "alter_column"
        table = "users"
        column = "name"

            [actions.changes]
            default = "'NEW DEFAULT'"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    // Insert a test user
    old_db.simple_query(
        "
        INSERT INTO users (id) VALUES (1)
        ",
    ).await
    .unwrap();

    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();
    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

    // Check that the existing users has the old default value
    let expected = vec!["DEFAULT"];
    assert!(new_db
        .query("SELECT name FROM users", &[],)
        .await
        .unwrap()
        .iter()
        .map(|row| row.get::<_, String>("name"))
        .eq(expected));

    // Insert data using old schema and make those get the old default value
    old_db
        .simple_query("INSERT INTO users (id) VALUES (2)")
        .await
        .unwrap();
    let result = new_db
        .query_one("SELECT name from users WHERE id = 2", &[])
        .await
        .unwrap();
    assert_eq!("DEFAULT", result.get::<_, &str>("name"));

    // Insert data using new schema and make sure it gets the new default value
    new_db
        .simple_query("INSERT INTO users (id) VALUES (3)")
        .await
        .unwrap();
    let result = old_db
        .query_one("SELECT name from users WHERE id = 3", &[])
        .await
        .unwrap();
    assert_eq!("NEW DEFAULT", result.get::<_, &str>("name"));
}

#[tokio::test]
async fn alter_column_with_index() {
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
            name = "first_name"
            type = "TEXT"

            [[actions.columns]]
            name = "last_name"
            type = "TEXT"

        [[actions]]
        type = "add_index"
        table = "users"

            [actions.index]
            name = "users_name_idx"
            columns = ["first_name", "last_name"]
        "#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
        name = "uppercase_last_name"

        [[actions]]
        type = "alter_column"
        table = "users"
        column = "last_name"
        up = "UPPER(last_name)"
        down = "LOWER(last_name)"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();
    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

    complete(&mut reshape, &first_migration, &second_migration).await;
    complete(&mut reshape, &first_migration, &second_migration).await;

    // Make sure index still exists
    let result: i64 = new_db
        .query(
            "
        SELECT COUNT(*)
        FROM pg_catalog.pg_index
        JOIN pg_catalog.pg_class ON pg_index.indexrelid = pg_class.oid
        WHERE pg_class.relname = 'users_name_idx'
        ",
            &[],
        ).await
        .unwrap()
        .first()
        .map(|row| row.get(0))
        .unwrap();
    assert_eq!(1, result, "expected index to still exist");
}

#[tokio::test]
async fn alter_column_with_unique_index() {
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
            unique = true
        "#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
        name = "uppercase_name"

        [[actions]]
        type = "alter_column"
        table = "users"
        column = "name"
        up = "UPPER(name)"
        down = "LOWER(name)"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    old_db.simple_query("INSERT INTO users (id, name) VALUES (1, 'Test')").await.unwrap();

    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();
    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

    // Try inserting a value which duplicates the uppercase value of an existing row
    let result = new_db.simple_query("INSERT INTO users (id, name) VALUES (2, 'TEST')").await;
    assert!(
        result.is_err(),
        "expected duplicate insert to new schema to fail"
    );

    // Try inserting a value which duplicates the lowercase value of an existing row
    new_db
        .simple_query("INSERT INTO users (id, name) VALUES (2, 'JOHN')")
        .await
        .unwrap();
    let result = old_db.simple_query("INSERT INTO users (id, name) VALUES (3, 'john')").await;
    assert!(
        result.is_err(),
        "expected duplicate insert to old schema to fail"
    );

    complete(&mut reshape, &first_migration, &second_migration).await;
    complete(&mut reshape, &first_migration, &second_migration).await;

    // Make sure index still exists
    let is_unique: bool = new_db
        .query(
            "
            SELECT pg_index.indisunique
            FROM pg_catalog.pg_index
            JOIN pg_catalog.pg_class ON pg_index.indexrelid = pg_class.oid
            WHERE pg_class.relname = 'name_idx'
            ",
            &[],
        ).await
        .unwrap()
        .first()
        .map(|row| row.get("indisunique"))
        .unwrap();
    assert!(is_unique, "expected index to still be unique");
}

#[tokio::test]
async fn alter_column_with_hash_index() {
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
            type = "hash"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
        name = "uppercase_name"

        [[actions]]
        type = "alter_column"
        table = "users"
        column = "name"
        up = "UPPER(name)"
        down = "LOWER(name)"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();
    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

    complete(&mut reshape, &first_migration, &second_migration).await;
    complete(&mut reshape, &first_migration, &second_migration).await;

    // Make sure index still has type GIN
    let index_type: String = new_db
        .query(
            "
            SELECT pg_am.amname
            FROM pg_catalog.pg_index
            JOIN pg_catalog.pg_class ON pg_index.indexrelid = pg_class.oid
            JOIN pg_catalog.pg_am ON pg_class.relam = pg_am.oid
            WHERE pg_class.relname = 'name_idx'
            ",
            &[],
        ).await
        .unwrap()
        .first()
        .map(|row| row.get("amname"))
        .unwrap();
    assert_eq!("hash", index_type);
}
