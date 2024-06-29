mod common; use common::{
    Test, Task, setup_db,
    migrate, complete, abort
};

use reshape::migration::{Migration, Format};

#[tokio::test]
async fn invalid_migration() {
    let Test { mut reshape, mut old_db, mut new_db } = Test::connect().await;

    let first_migration = Migration::from_text(
        r#"
        name = "invalid_migration"

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
        name = "add_invalid_column"

        [[actions]]
        type = "add_column"
        table = "users"

        up = "INVALID SQL"

            [actions.column]
            name = "first"
            type = "TEXT"
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

    assert!(migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.is_err());
}
