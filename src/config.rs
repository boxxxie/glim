use std::path::PathBuf;

use directories::{BaseDirs, ProjectDirs};

use crate::{
    glim_app::GlimConfig,
    result::{GlimError, Result},
};

pub fn default_config_path() -> PathBuf {
    if let Some(dirs) = BaseDirs::new() {
        dirs.config_dir().join("glim.toml")
    } else {
        PathBuf::from("glim.toml")
    }
}

pub fn default_data_dir() -> PathBuf {
    if let Some( dirs) = BaseDirs::new() {
        dirs.data_local_dir().join("glim")
    } else if let Some(proj_dirs) = ProjectDirs::from("", "", "glim") {
        proj_dirs.data_local_dir().to_path_buf()
    } else {
        PathBuf::from(".glim-data")
    }
}

pub fn default_db_path() -> PathBuf {
    default_data_dir().join("glim.duckdb")
}

pub fn default_quack_token_path() -> PathBuf {
    default_data_dir().join("quack.token")
}

pub fn save_config(config_file: &PathBuf, config: GlimConfig) -> Result<()> {
    confy::store_path(config_file, &config)
        .map_err(|e| GlimError::config_save_error(config_file.clone(), e))?;

    Ok(())
}
