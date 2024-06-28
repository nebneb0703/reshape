use std::{
    str::FromStr,
    path::Path,
    fmt::Debug,
    fs,
};

use serde::{Deserialize, Serialize};

use crate::actions::Action;

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

    pub fn from_file(path: impl AsRef<Path>, hint: Option<Format>) -> anyhow::Result<Self> {
        let path = path.as_ref();

        let format = path.extension().and_then(|ext| ext.to_str())
            .and_then(|ext| Format::from_str(ext).ok()).or(hint)
            .ok_or(anyhow::anyhow!(
                "migration {} has no file extension",
                path.to_string_lossy()
            ))?;

        let data = fs::read_to_string(path)?;
        let name = path.file_stem().and_then(|name| name.to_str()).map(ToOwned::to_owned);

        Self::from_text(&data, name, format)
    }

    pub fn from_text(data: &str, name: Option<String>, format: Format) -> anyhow::Result<Self> {
        #[derive(Serialize, Deserialize)]
        struct File {
            name: Option<String>,
            description: Option<String>,
            actions: Vec<Box<dyn Action>>,
        }

        let file: File = match format {
            Format::Toml => toml::from_str(data)?,
            Format::Json => serde_json::from_str(data)?,
        };

        let name = file.name.or(name).ok_or(anyhow::anyhow!(
            "missing migration name"
        ))?;

        Ok(Migration {
            name,
            description: file.description,
            actions: file.actions,
        })
    }
}

impl PartialEq for Migration {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name &&
        // lol lmao
        self.actions.len() == other.actions.len() &&
        self.actions.iter().map(|a| serde_json::to_string(a).unwrap())
            .zip(other.actions.iter().map(|a| serde_json::to_string(a).unwrap()))
            .all(|(a, b)| a == b)
    }
}

impl Eq for Migration {}

impl Clone for Migration {
    fn clone(&self) -> Self {
        let serialized = serde_json::to_string(self).unwrap();
        serde_json::from_str(&serialized).unwrap()
    }
}

pub enum Format {
    Toml,
    Json
}

pub struct InvalidExtension;

impl FromStr for Format {
    type Err = InvalidExtension;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "toml" => Ok(Format::Toml),
            "json" => Ok(Format::Json),
            _ => Err(InvalidExtension)
        }
    }
}
