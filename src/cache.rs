// Per-series episode + season indexes, cached next to the catalogue
// (episodes.json). Two maps keyed by the series /play/ URL: episodes are
// fetched once per season page and remembered; seasons once per picked
// title. Every visit after the first is instant and offline, exactly like
// the title catalogue. `--update` keeps the cache (titles are stable),
// episodes/seasons are re-checked only via `--update --episodes`.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, fs, path::PathBuf};

use crate::config::app_dir;

const EPISODES_FILE: &str = "episodes.json";

#[derive(Serialize, Deserialize, Clone, Default)]
pub(crate) struct Episode {
    pub(crate) num: u32,
    pub(crate) url: String,
}

/// One related `/play/...` entry from the "Correlati" widget: on AnimeWorld
/// every season/sequel/prequel/movie is its own page, so the season menu is
/// just this list plus the current page on top (marked `cur`).
#[derive(Serialize, Deserialize, Clone, Default)]
pub(crate) struct Season {
    pub(crate) title: String,
    pub(crate) url: String,
    pub(crate) cur: bool,
}

#[derive(Serialize, Deserialize, Default)]
pub(crate) struct EpisodeCache {
    #[serde(default)]
    pub(crate) episodes: HashMap<String, Vec<Episode>>,
    #[serde(default)]
    pub(crate) seasons: HashMap<String, Vec<Season>>,
}

pub(crate) fn episodes_path() -> Option<PathBuf> { app_dir().map(|d| d.join(EPISODES_FILE)) }

pub(crate) fn load_episode_cache() -> EpisodeCache {
    let raw = match episodes_path().and_then(|p| fs::read_to_string(p).ok()) {
        Some(r) => r,
        None => return EpisodeCache::default(),
    };
    let v: Value = match serde_json::from_str(&raw) {
        Ok(v) => v,
        Err(_) => return EpisodeCache::default(),
    };
    // New shape has explicit maps; old shape was a bare `{url: [episodes]}` map
    // (which would parse as an *empty* EpisodeCache since serde ignores
    // unknown fields — hence the explicit shape check, or old caches silently
    // vanish).
    if v.get("episodes").is_some() || v.get("seasons").is_some() {
        serde_json::from_value(v).unwrap_or_default()
    } else {
        serde_json::from_value::<HashMap<String, Vec<Episode>>>(v)
            .map(|episodes| EpisodeCache { episodes, seasons: HashMap::new() })
            .unwrap_or_default()
    }
}

pub(crate) fn save_episode_cache(c: &EpisodeCache) {
    if let Some(dir) = app_dir() {
        let _ = fs::create_dir_all(&dir);
        if let Some(p) = episodes_path() {
            let _ = fs::write(&p, serde_json::to_vec(c).unwrap_or_default());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn old_episode_cache_still_loads() {
        // The old episodes.json was a bare map; the new loader must route it
        // into .episodes (a naive #[derive] parse yields an EMPTY cache since
        // serde ignores unknown fields — this test guards the shape check).
        let old: HashMap<String, Vec<Episode>> = [("https://x/play/a".to_string(), vec![Episode { num: 1, url: "https://x/e1".into() }])].into_iter().collect();
        let raw = serde_json::to_string(&old).unwrap();
        let v: Value = serde_json::from_str(&raw).unwrap();
        assert!(v.get("episodes").is_none());
        let c: EpisodeCache = serde_json::from_value::<HashMap<String, Vec<Episode>>>(v)
            .map(|episodes| EpisodeCache { episodes, seasons: HashMap::new() }).unwrap();
        assert_eq!(c.episodes["https://x/play/a"].len(), 1);
        assert!(c.seasons.is_empty());
    }
}
