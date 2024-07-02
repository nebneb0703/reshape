mod common; use common::{
    Test, Task, setup_db,
    migrate, complete, abort
};

use reshape::migration::{Migration, Format};

#[tokio::test]
async fn move_column_between_tables() {
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
        primary_key = ["id"]

            [[actions.columns]]
            name = "id"
            type = "INTEGER"

            [[actions.columns]]
            name = "user_id"
            type = "INTEGER"
            nullable = false
        "#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
        name = "move_email_column"

        [[actions]]
        type = "add_column"
        table = "profiles"

            [actions.column]
            name = "email"
            type = "TEXT"
            nullable = false

            # When `users` is updated in the old schema, we write the email value to `profiles`
            # When `profiles` is updated in the old schema, the equivalent `users.email` will also be updated
            [actions.up]
            table = "users"
            value = "users.email"
            where = "profiles.user_id = users.id"

        [[actions]]
        type = "remove_column"
        table = "users"
        column = "email"

            # When `profiles` is changed in the new schema, we write the email address back to the removed column
            [actions.down]
            table = "profiles"
            value = "profiles.email"
            where = "users.id = profiles.user_id"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    old_db.simple_query("INSERT INTO users (id, email) VALUES (1, 'test1@test.com')").await.unwrap();
    old_db.simple_query("INSERT INTO users (id, email) VALUES (2, 'test2@test.com')").await.unwrap();

    old_db.simple_query("INSERT INTO profiles (id, user_id) VALUES (1, 1)").await.unwrap();
    old_db.simple_query("INSERT INTO profiles (id, user_id) VALUES (2, 2)").await.unwrap();

    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();
    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

    // Ensure emails were backfilled into profiles
    let profiles_emails: Vec<String> = new_db
        .query(
            r#"
            SELECT email
            FROM profiles
            ORDER BY id
            "#,
            &[],
        ).await
        .unwrap()
        .iter()
        .map(|row| row.get("email"))
        .collect();
    assert_eq!(vec!("test1@test.com", "test2@test.com"), profiles_emails);

    // Ensure insert in old schema updates new
    old_db.simple_query("INSERT INTO users (id, email) VALUES (3, 'test3@test.com')").await.unwrap();
    old_db.simple_query("INSERT INTO profiles (id, user_id) VALUES (3, 3)").await.unwrap();
    let new_email: String = new_db
        .query("SELECT email FROM profiles WHERE id = 3", &[])
        .await
        .unwrap()
        .first()
        .map(|row| row.get("email"))
        .unwrap();
    assert_eq!("test3@test.com", new_email);

    // Ensure updates in old schema updates new
    old_db.simple_query("UPDATE users SET email = 'test3+updated@test.com' WHERE id = 3").await.unwrap();
    let new_email: String = new_db
        .query("SELECT email FROM profiles WHERE id = 3", &[])
        .await
        .unwrap()
        .first()
        .map(|row| row.get("email"))
        .unwrap();
    assert_eq!("test3+updated@test.com", new_email);

    // Ensure insert in new schema updates old
    new_db
        .simple_query(
            "INSERT INTO profiles (id, user_id, email) VALUES (4, 4, 'test4@test.com')",
        ).await
        .unwrap();
    new_db.simple_query("INSERT INTO users (id) VALUES (4)").await.unwrap();
    let old_email: String = old_db
        .query("SELECT email FROM users WHERE id = 4", &[])
        .await
        .unwrap()
        .first()
        .map(|row| row.get("email"))
        .unwrap();
    assert_eq!("test4@test.com", old_email);

    // Ensure update in new schema updates old
    new_db
        .simple_query("UPDATE profiles SET email = 'test4+updated@test.com' WHERE id = 4").await
        .unwrap();
    let old_email: String = old_db
        .query("SELECT email FROM users WHERE id = 4", &[])
        .await
        .unwrap()
        .first()
        .map(|row| row.get("email"))
        .unwrap();
    assert_eq!("test4+updated@test.com", old_email);
}

#[tokio::test]
async fn extract_relation_into_new_table() {
    let Test { mut reshape, mut old_db, mut new_db } = Test::connect().await;

    let first_migration = Migration::from_text(
        r#"
        name = "create_tables"

        [[actions]]
        type = "create_table"
        name = "accounts"
        primary_key = ["id"]

            [[actions.columns]]
            name = "id"
            type = "INTEGER"

        [[actions]]
        type = "create_table"
        name = "users"
        primary_key = ["id"]

            [[actions.columns]]
            name = "id"
            type = "INTEGER"

            [[actions.columns]]
            name = "account_id"
            type = "INTEGER"
            nullable = false

            [[actions.columns]]
            name = "account_role"
            type = "TEXT"
            nullable = false
        "#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
        name = "add_account_user_connection"

        [[actions]]
        type = "create_table"
        name = "user_account_connections"
        primary_key = ["account_id", "user_id"]

            [[actions.columns]]
            name = "account_id"
            type = "INTEGER"

            [[actions.columns]]
            name = "user_id"
            type = "INTEGER"

            [[actions.columns]]
            name = "role"
            type = "TEXT"
            nullable = false

            [actions.up]
            table = "users"
            values = { user_id = "id", account_id = "account_id", role = "UPPER(account_role)" }
            where = "user_account_connections.user_id = users.id"

        [[actions]]
        type = "remove_column"
        table = "users"
        column = "account_id"

            [actions.down]
            table = "user_account_connections"
            value = "user_account_connections.account_id"
            where = "users.id = user_account_connections.user_id"

        [[actions]]
        type = "remove_column"
        table = "users"
        column = "account_role"

            [actions.down]
            table = "user_account_connections"
            value = "LOWER(user_account_connections.role)"
            where = "users.id = user_account_connections.user_id"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    old_db.simple_query("INSERT INTO accounts (id) VALUES (1)").await.unwrap();
    old_db.simple_query("INSERT INTO users (id, account_id, account_role) VALUES (1, 1, 'admin')").await.unwrap();

    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();
    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

    // Ensure connections was backfilled
    let rows: Vec<(i32, i32, String)> = new_db
        .query(
            "
            SELECT account_id, user_id, role
            FROM user_account_connections
            ",
            &[],
        ).await
        .unwrap()
        .iter()
        .map(|row| (row.get("account_id"), row.get("user_id"), row.get("role")))
        .collect();
    assert_eq!(1, rows.len());

    let row = rows.first().unwrap();
    assert_eq!(1, row.0);
    assert_eq!(1, row.1);
    assert_eq!("ADMIN", row.2);

    // Ensure inserted user in old schema creates a new connection
    old_db
        .simple_query(
            "INSERT INTO users (id, account_id, account_role) VALUES (2, 1, 'developer')",
        ).await
        .unwrap();
    assert!(
        new_db
            .query(
                "
                SELECT account_id, user_id, role
                FROM user_account_connections
                WHERE account_id = 1 AND user_id = 2 AND role = 'DEVELOPER'
                ",
                &[],
            ).await
            .unwrap()
            .len()
            == 1
    );

    // Ensure NOT NULL constraint still applies to old schema
    let result = old_db
        .simple_query(
            "INSERT INTO users (id, account_id, account_role) VALUES (2, NULL, 'developer')",
        ).await;
        assert!(result.is_err());

    // Ensure updated user role in old schema updates connection in new schema
    old_db
        .simple_query("UPDATE users SET account_role = 'admin' WHERE id = 2")
        .await
        .unwrap();
    assert!(
        new_db
            .query(
                "
                SELECT account_id, user_id, role
                FROM user_account_connections
                WHERE account_id = 1 AND user_id = 2 AND role = 'ADMIN'
                ",
                &[],
            ).await
            .unwrap()
            .len()
            == 1
    );

    // Ensure updated connection in new schema updates old schema user
    new_db
        .simple_query(
            "UPDATE user_account_connections SET role = 'DEVELOPER' WHERE account_id = 1 AND user_id = 2",
        ).await
        .unwrap();
    assert!(
        old_db
            .query(
                "
                SELECT id
                FROM users
                WHERE id = 2 AND account_id = 1 AND account_role = 'developer'
                ",
                &[],
            ).await
            .unwrap()
            .len()
            == 1
    );

    // Ensure insert of user with connection through new schema updates user in old schema
    new_db
        .simple_query(
            r#"
            BEGIN;
            INSERT INTO users (id) VALUES (3);
            INSERT INTO user_account_connections (user_id, account_id, role) VALUES (3, 1, 'DEVELOPER');
            COMMIT;
            "#,
        ).await
        .unwrap();
    new_db
        .simple_query(
            "",
        ).await
        .unwrap();
    assert!(
        old_db
            .query(
                "
                SELECT id
                FROM users
                WHERE id = 3 AND account_id = 1 AND account_role = 'developer'
                ",
                &[],
            ).await
            .unwrap()
            .len()
            == 1
    );
}
