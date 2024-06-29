mod common; use common::{
    Test, Task, setup_db,
    migrate, complete, abort
};

use reshape::migration::{Migration, Format};

#[tokio::test]
async fn remove_index() {
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
        name = "remove_name_index"

        [[actions]]
        type = "remove_index"
        index = "name_idx"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

    // Ensure index is still valid and ready during the migration
    let result: Vec<(bool, bool)> = old_db
        .query(
            "
            SELECT pg_index.indisready, pg_index.indisvalid
            FROM pg_catalog.pg_index
            JOIN pg_catalog.pg_class ON pg_index.indexrelid = pg_class.oid
            WHERE pg_class.relname = 'name_idx'
            ",
            &[],
        ).await
        .unwrap()
        .iter()
        .map(|row| (row.get("indisready"), row.get("indisvalid")))
        .collect();

    assert_eq!([(true, true)].as_slice(), result.as_slice());

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
