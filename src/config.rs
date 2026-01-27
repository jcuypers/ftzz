use std::{fs, num::NonZeroU64, path::{Path, PathBuf}};

use error_stack::{Result, ResultExt};
use serde::Deserialize;

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub files: Option<NonZeroU64>,
    pub files_exact: Option<bool>,
    pub total_bytes: Option<u64>,
    pub fill_byte: Option<u8>,
    pub bytes_exact: Option<bool>,
    pub exact: Option<bool>,
    pub max_depth: Option<u32>,
    pub ftd_ratio: Option<NonZeroU64>,
    pub audit_output: Option<PathBuf>,
    pub seed: Option<u64>,
    pub duplicate_percentage: Option<f64>,
    pub max_duplicates_per_file: Option<std::num::NonZeroUsize>,
}

#[derive(thiserror::Error, Debug)]
pub enum ConfigError {
    #[error("Failed to read configuration file")]
    Read,
    #[error("Failed to parse configuration file")]
    Parse,
}

impl Config {
    pub fn from_file(path: &Path) -> Result<Self, ConfigError> {
        let content = fs::read_to_string(path).change_context(ConfigError::Read)?;
        toml::from_str(&content).change_context(ConfigError::Parse)
    }
}
