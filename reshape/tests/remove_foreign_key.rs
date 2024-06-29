mod common; use common::{
    Test, Task, setup_db,
    migrate, complete, abort
};

use reshape::migration::{Migration, Format};

#[tokio::test]
async fn remove_foreign_key() {
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

            [[actions.foreign_keys]]
            columns = ["user_id"]
            referenced_table = "users"
            referenced_columns = ["id"]
        "#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
        name = "remove_foreign_key"

        [[actions]]
        type = "remove_foreign_key"
        table = "items"
        foreign_key = "items_user_id_fkey"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    for task in [Task::Complete, Task::Abort] {
        setup_db(&mut reshape, &mut old_db, &first_migration).await;

        // Insert some test users
        old_db.simple_query("INSERT INTO users (id) VALUES (1), (2)").await.unwrap();

        migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

        // Ensure items can't be inserted if they don't reference valid users
        // The foreign key is only removed when the migration is completed so
        // it should still be enforced for the new and old schema.
        let result = old_db.simple_query("INSERT INTO items (id, user_id) VALUES (3, 3)").await;
        assert!(
            result.is_err(),
            "expected insert against old schema to fail"
        );

        let result = new_db.simple_query("INSERT INTO items (id, user_id) VALUES (3, 3)").await;
        assert!(
            result.is_err(),
            "expected insert against new schema to fail"
        );

        match task {
            Task::Complete => {
                complete(&mut reshape, &first_migration, &second_migration).await;

                // Ensure items can be inserted even if they don't reference valid users
                new_db.simple_query("INSERT INTO items (id, user_id) VALUES (5, 3)").await.unwrap();

                // Ensure foreign key doesn't exist
                let foreign_keys = new_db
                    .query(
                        "
                        SELECT tc.constraint_name
                        FROM information_schema.table_constraints AS tc
                        WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='items';
                        ",
                        &[],
                    ).await
                    .unwrap();
                assert!(
                    foreign_keys.is_empty(),
                    "expected no foreign keys to exist on items table"
                );
            },
            Task::Abort => {
                abort(&mut reshape, &first_migration, &second_migration).await;

                // Ensure items can't be inserted if they don't reference valid users
                let result = old_db.simple_query("INSERT INTO items (id, user_id) VALUES (3, 3)").await;
                assert!(result.is_err(), "expected insert to fail");

                // Ensure foreign key still exists
                let fk_exists = !old_db
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
                assert!(fk_exists);
            },
        }
    }
}
