mod common; use common::{
    Test, Task, setup_db,
    migrate, complete, abort
};

use reshape::migration::{Migration, Format};

#[tokio::test]
async fn remove_table() {
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
        name = "remove_users_table"

        [[actions]]
        type = "remove_table"
        table = "users"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

    // Make sure inserts work against the old schema
    old_db.simple_query("INSERT INTO users(id) VALUES (1)").await.unwrap();

    // Ensure the table is not accessible through the new schema
    assert!(new_db.query("SELECT id FROM users", &[]).await.is_err());
}
