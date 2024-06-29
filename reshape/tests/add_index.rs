mod common; use common::{
    Test, Task, setup_db,
    migrate, complete, abort
};

use reshape::migration::{Migration, Format};

#[tokio::test]
async fn add_index() {
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
        "#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
        name = "add_users_name_index"

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

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

    // Ensure index is valid and ready
    let (is_ready, is_valid): (bool, bool) = old_db
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
        .first()
        .map(|row| (row.get("indisready"), row.get("indisvalid")))
        .unwrap();

    assert!(is_ready, "expected index to be ready");
    assert!(is_valid, "expected index to be valid");

    complete(&mut reshape, &first_migration, &second_migration).await;

    // Ensure index is valid and ready
    let (is_ready, is_valid): (bool, bool) = new_db
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
        .first()
        .map(|row| (row.get("indisready"), row.get("indisvalid")))
        .unwrap();

    assert!(is_ready, "expected index to be ready");
    assert!(is_valid, "expected index to be valid");
}

#[tokio::test]
async fn add_index_unique() {
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
        "#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
        name = "add_name_index"

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

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

    // Ensure index is valid, ready and unique
    let (is_ready, is_valid, is_unique): (bool, bool, bool) = old_db
        .query(
            "
            SELECT pg_index.indisready, pg_index.indisvalid, pg_index.indisunique
            FROM pg_catalog.pg_index
            JOIN pg_catalog.pg_class ON pg_index.indexrelid = pg_class.oid
            WHERE pg_class.relname = 'name_idx'
            ",
            &[],
        ).await
        .unwrap()
        .first()
        .map(|row| {
            (
                row.get("indisready"),
                row.get("indisvalid"),
                row.get("indisunique"),
            )
        })
        .unwrap();

    assert!(is_ready, "expected index to be ready");
    assert!(is_valid, "expected index to be valid");
    assert!(is_unique, "expected index to be unique");
}

#[tokio::test]
async fn add_index_with_type() {
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
            name = "data"
            type = "JSONB"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
        name = "add_data_index"

        [[actions]]
        type = "add_index"
        table = "users"

            [actions.index]
            name = "data_idx"
            columns = ["data"]
            type = "gin"
        "#,
        None,
        Format::Toml,
    ).unwrap();

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

    // Ensure index is valid, ready and has the right type
    let (is_ready, is_valid, index_type): (bool, bool, String) = old_db
        .query(
            "
            SELECT pg_index.indisready, pg_index.indisvalid, pg_am.amname
            FROM pg_catalog.pg_index
            JOIN pg_catalog.pg_class ON pg_index.indexrelid = pg_class.oid
            JOIN pg_catalog.pg_am ON pg_class.relam = pg_am.oid
            WHERE pg_class.relname = 'data_idx'
            ",
            &[],
        ).await
        .unwrap()
        .first()
        .map(|row| {
            (
                row.get("indisready"),
                row.get("indisvalid"),
                row.get("amname"),
            )
        })
        .unwrap();

    assert!(is_ready, "expected index to be ready");
    assert!(is_valid, "expected index to be valid");
    assert_eq!("gin", index_type, "expected index type to be GIN");
}
