mod common; pub use common::Column;
mod create_table; pub use create_table::CreateTable;
mod alter_column; pub use alter_column::{AlterColumn, ColumnChanges};
mod add_column; pub use add_column::AddColumn;
mod remove_column; pub use remove_column::RemoveColumn;
mod add_index; pub use add_index::{AddIndex, Index};
mod remove_index; pub use remove_index::RemoveIndex;
mod remove_table; pub use remove_table::RemoveTable;
mod rename_table; pub use rename_table::RenameTable;
mod create_enum; pub use create_enum::CreateEnum;
mod remove_enum; pub use remove_enum::RemoveEnum;
mod custom; pub use custom::Custom;
mod add_foreign_key; pub use add_foreign_key::AddForeignKey;
mod remove_foreign_key; pub use remove_foreign_key::RemoveForeignKey;

use std::fmt::{Debug, Display};

use crate::{
    db::{Connection, Transaction},
    schema::Schema,
};

#[typetag::serde(tag = "type")]
#[async_trait::async_trait]
pub trait Action: Debug + Display {
    async fn run(
        &self,
        ctx: &MigrationContext,
        db: &mut dyn Connection,
        schema: &Schema
    ) -> anyhow::Result<()>;

    async fn complete<'a>(
        &self,
        ctx: &MigrationContext,
        db: &'a mut dyn Connection,
    ) -> anyhow::Result<Option<Transaction<'a>>>;

    fn update_schema(&self, ctx: &MigrationContext, schema: &mut Schema);

    async fn abort(
        &self,
        ctx: &MigrationContext,
        db: &mut dyn Connection
    ) -> anyhow::Result<()>;
}

pub struct MigrationContext {
    migration_index: usize,
    action_index: usize,
    existing_schema_name: Option<String>,
}

impl MigrationContext {
    pub fn new(
        migration_index: usize,
        action_index: usize,
        existing_schema_name: Option<String>,
    ) -> Self {
        MigrationContext {
            migration_index,
            action_index,
            existing_schema_name,
        }
    }

    fn prefix(&self) -> String {
        format!(
            "__reshape_{:0>4}_{:0>4}",
            self.migration_index, self.action_index
        )
    }

    fn prefix_inverse(&self) -> String {
        format!(
            "__reshape_{:0>4}_{:0>4}",
            1000 - self.migration_index,
            1000 - self.action_index
        )
    }
}
