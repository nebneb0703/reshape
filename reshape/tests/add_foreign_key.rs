mod common; use common::{
    Test, Task, setup_db,
    migrate, complete, abort
};

use reshape::migration::{Migration, Format};

#[tokio::test]
async fn add_foreign_key() {
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

        [[actions]]
        type = "create_table"
        name = "items"
        primary_key = ["id"]

            [[actions.columns]]
            name = "id"
            type = "INTEGER"

            [[actions.columns]]
            name = "user_id"
            type = "INTEGER"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
        name = "add_foreign_key"

        [[actions]]
        type = "add_foreign_key"
        table = "items"

            [actions.foreign_key]
            columns = ["user_id"]
            referenced_table = "users"
            referenced_columns = ["id"]
        "#,
        None,
        Format::Toml,
    ).unwrap();

    for task in [Task::Complete, Task::Abort] {
        setup_db(&mut reshape, &mut old_db, &first_migration).await;

        // Insert some test users
        old_db.simple_query("INSERT INTO users (id) VALUES (1), (2)").await.unwrap();

        migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

        // Ensure items can be inserted if they reference valid users
        old_db.simple_query("INSERT INTO items (id, user_id) VALUES (1, 1), (2, 2)").await.unwrap();

        // Ensure items can't be inserted if they don't reference valid users
        let result = old_db.simple_query("INSERT INTO items (id, user_id) VALUES (3, 3)").await;
        assert!(result.is_err(), "expected insert to fail");

        match task {
            Task::Complete => {
                complete(&mut reshape, &first_migration, &second_migration).await;

                // Ensure items can be inserted if they reference valid users
                new_db.simple_query("INSERT INTO items (id, user_id) VALUES (3, 1), (4, 2)").await.unwrap();

                // Ensure items can't be inserted if they don't reference valid users
                let result = new_db.simple_query("INSERT INTO items (id, user_id) VALUES (5, 3)").await;
                assert!(result.is_err(), "expected insert to fail");

                // Ensure foreign key exists with the right name
                let foreign_key_name: Option<String> = new_db
                .query(
                    "
                    SELECT tc.constraint_name
                    FROM information_schema.table_constraints AS tc
                    WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='items';
                    ",
                    &[],
                ).await
                .unwrap()
                .first()
                .map(|row| row.get(0));
                assert_eq!(Some("items_user_id_fkey".to_string()), foreign_key_name);
            },
            Task::Abort => {
                abort(&mut reshape, &first_migration, &second_migration).await;

                // Ensure foreign key doesn't exist
                let fk_does_not_exist = old_db
                .query(
                    "
                    SELECT tc.constraint_name
                    FROM information_schema.table_constraints AS tc
                    WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='items';
                    ",
                    &[],
                ).await
                .unwrap()
                .is_empty();
                assert!(fk_does_not_exist);
            },
        }
    }
}

#[tokio::test]
async fn add_invalid_foreign_key() {
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

        [[actions]]
        type = "create_table"
        name = "items"
        primary_key = ["id"]

            [[actions.columns]]
            name = "id"
            type = "INTEGER"

            [[actions.columns]]
            name = "user_id"
            type = "INTEGER"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
        name = "add_foreign_key"

        [[actions]]
        type = "add_foreign_key"
        table = "items"

            [actions.foreign_key]
            columns = ["user_id"]
            referenced_table = "users"
            referenced_columns = ["id"]
        "#,
        None,
        Format::Toml,
    ).unwrap();

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    // Insert some items which don't reference a valid user
    new_db.simple_query("INSERT INTO items (id, user_id) VALUES (1, 1), (2, 2)").await.unwrap();

    assert!(migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.is_err());
}
