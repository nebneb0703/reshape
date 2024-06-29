mod common; use common::{
    Test, Task, setup_db,
    migrate, complete, abort
};

use reshape::migration::{Migration, Format};

#[tokio::test]
async fn custom_enable_extension() {
    let Test { mut reshape, mut old_db, mut new_db } = Test::connect().await;

    let first_migration = Migration::from_text(
        r#"
		name = "empty_migration"

		[[actions]]
		type = "custom"
		"#,
        None,
        Format::Toml,
    ).unwrap();

    let second_migration = Migration::from_text(
        r#"
		name = "enable_extensions"

		[[actions]]
		type = "custom"

		start = """
			CREATE EXTENSION IF NOT EXISTS bloom;
			CREATE EXTENSION IF NOT EXISTS btree_gin;
		"""

        complete = "CREATE EXTENSION IF NOT EXISTS btree_gist"

		abort = """
			DROP EXTENSION IF EXISTS bloom;
			DROP EXTENSION IF EXISTS btree_gin;
		"""
		"#,
        None,
        Format::Toml,
    ).unwrap();

    for task in [Task::Complete, Task::Abort] {
        old_db.simple_query(
            "
            DROP EXTENSION IF EXISTS bloom;
            DROP EXTENSION IF EXISTS btree_gin;
            DROP EXTENSION IF EXISTS btree_gist;
            ",
        ).await
        .unwrap();

        setup_db(&mut reshape, &mut old_db, &first_migration).await;

        migrate(&mut reshape, &mut new_db, &first_migration, &second_migration).await.unwrap();

        let bloom_activated = !old_db
            .query("SELECT * FROM pg_extension WHERE extname = 'bloom'", &[])
            .await
            .unwrap()
            .is_empty();
        assert!(bloom_activated);

        let btree_gin_activated = !old_db
            .query(
                "SELECT * FROM pg_extension WHERE extname = 'btree_gin'",
                &[],
            ).await
            .unwrap()
            .is_empty();
        assert!(btree_gin_activated);

        let btree_gist_activated = !old_db
            .query(
                "SELECT * FROM pg_extension WHERE extname = 'btree_gist'",
                &[],
            ).await
            .unwrap()
            .is_empty();
        assert!(!btree_gist_activated);

        match task {
            Task::Complete => {
                complete(&mut reshape, &first_migration, &second_migration).await;

                let bloom_activated = !new_db
                    .query("SELECT * FROM pg_extension WHERE extname = 'bloom'", &[])
                    .await
                    .unwrap()
                    .is_empty();
                assert!(bloom_activated);

                let btree_gin_activated = !new_db
                    .query(
                        "SELECT * FROM pg_extension WHERE extname = 'btree_gin'",
                        &[],
                    ).await
                    .unwrap()
                    .is_empty();
                assert!(btree_gin_activated);

                let btree_gist_activated = !new_db
                    .query(
                        "SELECT * FROM pg_extension WHERE extname = 'btree_gist'",
                        &[],
                    ).await
                    .unwrap()
                    .is_empty();
                assert!(btree_gist_activated);
            },
            Task::Abort => {
                abort(&mut reshape, &first_migration, &second_migration).await;

                let bloom_activated = !old_db
                    .query("SELECT * FROM pg_extension WHERE extname = 'bloom'", &[])
                    .await
                    .unwrap()
                    .is_empty();
                assert!(!bloom_activated);

                let btree_gin_activated = !old_db
                    .query(
                        "SELECT * FROM pg_extension WHERE extname = 'btree_gin'",
                        &[],
                    ).await
                    .unwrap()
                    .is_empty();
                assert!(!btree_gin_activated);

                let btree_gist_activated = !old_db
                    .query(
                        "SELECT * FROM pg_extension WHERE extname = 'btree_gist'",
                        &[],
                    ).await
                    .unwrap()
                    .is_empty();
                assert!(!btree_gist_activated);
            },
        }
    }
}
