// ohmyanime config: source site + scrapper endpoint, under the OS config dir.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{fs, path::{Path, PathBuf}, time::UNIX_EPOCH};

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Source {
    pub(crate) base_url: String,
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
        let source = Source { base_url: "https://www.animeworld.ac".into() };
        Self { source, scrapper: Scrapper { obscura_endpoint: "ws://127.0.0.1:9222".into() } }
    }
}

pub(crate) fn app_dir() -> Option<PathBuf> { dirs::config_dir().map(|d| d.join("ohmyanime")) }

/// Legacy directory name (mixed case). Kept only for the one-way migration.
fn legacy_dir(new_dir: &Path) -> Option<PathBuf> {
    new_dir.parent().map(|p| p.join("ohMyAnime"))
}

/// One-way rename `ohMyAnime` -> `ohmyanime`: moves the whole directory so
/// catalog/episode caches and config survive the upgrade. Runs before any
/// file use; a no-op when the new dir exists or the legacy one is absent.
/// Returns true when something was moved.
fn move_legacy_dir(new_dir: &Path, legacy: &Path) -> bool {
    if new_dir.exists() || !legacy.exists() { return false; }
    match fs::rename(legacy, new_dir) {
        Ok(()) => {
            eprintln!("moved {} to {}", legacy.display(), new_dir.display());
            true
        }
        Err(e) => {
            eprintln!("could not move {} to {} ({e}); starting fresh", legacy.display(), new_dir.display());
            false
        }
    }
}

pub(crate) fn load_config() -> Result<Config> {
    let dir = app_dir().ok_or_else(|| anyhow::anyhow!("cannot resolve the user config directory"))?;
    if let Some(legacy) = legacy_dir(&dir) { move_legacy_dir(&dir, &legacy); }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_root(tag: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("ohmyanime-test-{}-{tag}", std::process::id()));
        let _ = fs::remove_dir_all(&p);
        p
    }

    #[test]
    fn app_dir_is_lowercase() {
        let d = app_dir().expect("config dir");
        assert_eq!(d.file_name().and_then(|s| s.to_str()), Some("ohmyanime"));
    }

    #[test]
    fn legacy_dir_moves_when_new_is_absent() {
        let root = tmp_root("move");
        let legacy = root.join("ohMyAnime");
        let new = root.join("ohmyanime");
        fs::create_dir_all(&legacy).unwrap();
        fs::write(legacy.join("catalog.json"), "{}").unwrap();
        assert!(move_legacy_dir(&new, &legacy));
        assert!(new.join("catalog.json").exists());
        assert!(!legacy.exists());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn legacy_dir_kept_when_new_exists() {
        let root = tmp_root("keep");
        let legacy = root.join("ohMyAnime");
        let new = root.join("ohmyanime");
        fs::create_dir_all(&legacy).unwrap();
        fs::create_dir_all(&new).unwrap();
        assert!(!move_legacy_dir(&new, &legacy));
        assert!(legacy.exists() && new.exists());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn no_legacy_no_move() {
        let root = tmp_root("absent");
        assert!(!move_legacy_dir(&root.join("ohmyanime"), &root.join("ohMyAnime")));
    }
}
