use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{
    db::{Connection, Transaction},
    schema::Schema,
    actions::{Action, MigrationContext},
};

#[derive(Serialize, Deserialize, Debug)]
pub struct Custom {
    #[serde(default)]
    pub start: Option<String>,

    #[serde(default)]
    pub complete: Option<String>,

    #[serde(default)]
    pub abort: Option<String>,
}

impl fmt::Display for Custom {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Running custom migration")
    }
}

#[typetag::serde(name = "custom")]
#[async_trait::async_trait]
impl Action for Custom {
    async fn run(
        &self,
        _ctx: &MigrationContext,
        db: &mut dyn Connection,
        _schema: &Schema,
    ) -> anyhow::Result<()> {
        if let Some(start_query) = &self.start {
            println!("Running query: {}", start_query);
            db.run(start_query).await?;
        }

        Ok(())
    }

    async fn complete<'a>(
        &self,
        _ctx: &MigrationContext,
        db: &'a mut dyn Connection,
    ) -> anyhow::Result<Option<Transaction<'a>>> {
        if let Some(complete_query) = &self.complete {
            db.run(complete_query).await?;
        }

        Ok(None)
    }

    fn update_schema(&self, _ctx: &MigrationContext, _schema: &mut Schema) {}

    async fn abort(&self, _ctx: &MigrationContext, db: &mut dyn Connection) -> anyhow::Result<()> {
        if let Some(abort_query) = &self.abort {
            db.run(abort_query).await?;
        }

        Ok(())
    }
}
