use clap::Args;

use reshape::Reshape;

#[derive(Args)]
pub struct Options {
    #[clap(long)]
    url: Option<String>,
    #[clap(long, default_value = "localhost")]
    host: String,
    #[clap(long, default_value = "5432")]
    port: u16,
    #[clap(long, short)]
    database: Option<String>,
    #[clap(long, short, default_value = "postgres")]
    username: String,
    #[clap(long, short)]
    password: Option<String>,
}

impl Options {
    pub async fn to_reshape_from_env(&self) -> anyhow::Result<Reshape> {
        // Load environment variables from .env file if it exists
        dotenvy::dotenv().ok();

        let url_env = std::env::var("DB_URL").ok();
        let url = url_env.as_ref().or(self.url.as_ref());

        // Use the connection URL if it has been set
        if let Some(url) = url {
            return Reshape::new(url).await;
        }

        let host_env = std::env::var("DB_HOST").ok();
        let host = host_env.as_ref().unwrap_or(&self.host);

        let port = std::env::var("DB_PORT")
            .ok()
            .and_then(|port| port.parse::<u16>().ok())
            .unwrap_or(self.port);

        let username_env = std::env::var("DB_USERNAME").ok();
        let username = username_env.as_ref().unwrap_or(&self.username);

        let password_env = std::env::var("DB_PASSWORD").ok();
        let password = password_env.as_ref().or(self.password.as_ref()).unwrap();

        let database_env = std::env::var("DB_NAME").ok();
        let database = database_env.as_ref().or(self.database.as_ref()).unwrap();

        Reshape::new_with_options(host, port, database, username, password).await
    }
}
