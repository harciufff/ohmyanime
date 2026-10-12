// ohMyAnime config: source site + scrapper endpoint, under the OS config dir.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{fs, path::PathBuf, time::UNIX_EPOCH};

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Source {
    pub(crate) base_url: String,
    pub(crate) search_path: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Scrapper {
    pub(crate) obscura_endpoint: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Config {
    pub(crate) source: Source,
    pub(crate) scrapper: Scrapper,
}

impl Default for Config {
    fn default() -> Self {
        let source = Source { base_url: "https://www.animeworld.ac".into(), search_path: "/search?keyword=".into() };
        Self { source, scrapper: Scrapper { obscura_endpoint: "ws://127.0.0.1:9222".into() } }
    }
}

pub(crate) fn app_dir() -> Option<PathBuf> { dirs::config_dir().map(|d| d.join("ohMyAnime")) }

pub(crate) fn load_config() -> Result<Config> {
    let dir = app_dir().ok_or_else(|| anyhow::anyhow!("cannot resolve the user config directory"))?;
    let path = dir.join("config.toml");
    if !path.exists() {
        fs::create_dir_all(&dir).context("creating config dir")?;
        fs::write(&path, toml::to_string_pretty(&Config::default())?)?;
        eprintln!("Seeded default config at {}", path.display());
    }
    Ok(toml::from_str(&fs::read_to_string(&path)?)?)
}

/// Wall-clock seconds, for catalogue stamps and menu.log lines.
pub(crate) fn now_secs() -> u64 {
    std::time::SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}
