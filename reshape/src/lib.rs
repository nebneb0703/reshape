mod db;
pub mod migration;
pub mod actions;
mod schema;
mod state;

use tokio_postgres::Config;

pub struct Reshape {
    db: db::Lock,
}

impl Reshape {
    pub async fn new(connection_string: &str) -> anyhow::Result<Reshape> {
        let config: Config = connection_string.parse()?;
        Self::new_with_config(&config).await
    }

    pub async fn new_with_options(
        host: &str,
        port: u16,
        database: &str,
        username: &str,
        password: &str,
    ) -> anyhow::Result<Reshape> {
        let mut config = Config::new();
        config
            .host(host)
            .port(port)
            .user(username)
            .dbname(database)
            .password(password);

        Self::new_with_config(&config).await
    }

    pub async fn new_with_config(config: &Config) -> anyhow::Result<Reshape> {
        let db = db::Lock::connect(config).await?;
        Ok(Reshape { db })
    }
}
