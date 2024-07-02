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
    db::Connection,
    schema::Schema,
};

// todo: some kind of type state to enforce this behaviour at compile time.

/// A migration action.
///
/// Actions are begun and completed in order. Actions are aborted in reverse order.
#[typetag::serde(tag = "type")]
#[async_trait::async_trait]
pub trait Action: Debug + Display {
    async fn begin(
        &self,
        ctx: &MigrationContext,
        db: &mut dyn Connection,
        schema: &Schema,
    ) -> anyhow::Result<()>;

    async fn complete(
        &self,
        ctx: &MigrationContext,
        db: &mut dyn Connection,
    ) -> anyhow::Result<()>;

    fn update_schema(&self, ctx: &MigrationContext, schema: &mut Schema);

    async fn abort(
        &self,
        ctx: &MigrationContext,
        db: &mut dyn Connection,
    ) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct MigrationContext {
    pub migration_index: usize,
    pub action_index: usize,
    pub existing_schema_name: Option<String>,
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
