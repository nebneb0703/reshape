mod common; use common::{
    Test, Task, setup_db,
    migrate, complete, abort
};

use reshape::migration::{Migration, Format};

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
