use crate::{db::Conn, schema::Schema};
use core::fmt::Debug;
use serde::{Deserialize, Serialize};

// Re-export migration types
mod common;
pub use common::Column;

mod create_table;
pub use create_table::CreateTable;

mod alter_column;
pub use alter_column::{AlterColumn, ColumnChanges};

mod add_column;
pub use add_column::AddColumn;

mod remove_column;
pub use remove_column::RemoveColumn;

#[derive(Serialize, Deserialize, Debug)]
pub struct Migration {
    pub name: String,
    pub description: Option<String>,
    pub actions: Vec<Box<dyn Action>>,
}

impl Migration {
    pub fn new(name: impl Into<String>, description: Option<String>) -> Migration {
        Migration {
            name: name.into(),
            description,
            actions: vec![],
        }
    }

    pub fn with_action(mut self, action: impl Action + 'static) -> Self {
        self.actions.push(Box::new(action));
        self
    }
}

impl PartialEq for Migration {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Migration {}

impl Clone for Migration {
    fn clone(&self) -> Self {
        let serialized = serde_json::to_string(self).unwrap();
        serde_json::from_str(&serialized).unwrap()
    }
}

#[typetag::serde(tag = "type")]
pub trait Action: Debug {
    fn describe(&self) -> String;
    fn run(&self, db: &mut dyn Conn, schema: &Schema) -> anyhow::Result<()>;
    fn complete(&self, db: &mut dyn Conn, schema: &Schema) -> anyhow::Result<()>;
    fn update_schema(&self, schema: &mut Schema) -> anyhow::Result<()>;
    fn abort(&self, db: &mut dyn Conn) -> anyhow::Result<()>;
}
