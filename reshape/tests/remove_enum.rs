mod common; use common::{
    Test, Task, setup_db,
    migrate, complete, abort
};

use reshape::migration::{Migration, Format};

#[tokio::test]
async fn remove_enum() {
    let Test { mut reshape, mut old_db, mut new_db } = Test::connect().await;

    let first_migration = Migration::from_text(
        r#"
		name = "create_enum"

		[[actions]]
		type = "create_enum"
		name = "mood"
		values = ["happy", "ok", "sad"]
		"#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
		name = "remove_enum"

		[[actions]]
		type = "remove_enum"
		enum = "mood"
		"#,
        None,
        Format::Toml,
    ).unwrap();

    setup_db(&mut reshape, &mut old_db, &first_migration).await;

    // Ensure enum was created
    let enum_exists = !old_db
        .query(
            "
            SELECT typname
            FROM pg_catalog.pg_type
            WHERE typcategory = 'E'
            AND typname = 'mood'
            ",
            &[],
        ).await
        .unwrap()
        .is_empty();

    assert!(enum_exists, "expected mood enum to have been created");

    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();
    migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

    complete(&mut reshape, &first_migration, &second_migration).await;
    complete(&mut reshape, &first_migration, &second_migration).await;

    // Ensure enum was removed after completion
    let enum_does_not_exist = new_db
        .query(
            "
            SELECT typname
            FROM pg_catalog.pg_type
            WHERE typcategory = 'E'
            AND typname = 'mood'
            ",
            &[],
        ).await
        .unwrap()
        .is_empty();

    assert!(
        enum_does_not_exist,
        "expected mood enum to have been removed"
    );
}
