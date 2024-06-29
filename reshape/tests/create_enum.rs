mod common; use common::{
    Test, Task, setup_db,
    migrate, complete, abort
};

use reshape::migration::{Migration, Format};

#[tokio::test]
async fn create_enum() {
    let Test { mut reshape, mut old_db, mut new_db } = Test::connect().await;

    let first_migration = Migration::from_text(
        r#"
		name = "create_enum_and_table"

		[[actions]]
		type = "create_enum"
		name = "mood"
		values = ["happy", "ok", "sad"]

		[[actions]]
		type = "create_table"
		name = "updates"
		primary_key = ["id"]

			[[actions.columns]]
			name = "id"
			type = "INTEGER"

			[[actions.columns]]
			name = "status"
			type = "mood"
		"#,
		None,
		Format::Toml,
    ).unwrap();

	setup_db(&mut reshape, &mut old_db, &first_migration).await;

	// Valid enum values should succeed
	old_db.simple_query(
		"INSERT INTO updates (id, status) VALUES (1, 'happy'), (2, 'ok'), (3, 'sad')",
	).await
	.unwrap();
}
