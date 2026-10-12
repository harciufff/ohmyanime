// Title catalogue: the whole site index on disk (`catalog.json`).
// A hit here is free and offline; the /az-list crawl (see site.rs) runs
// only when the file is missing or `--update` is asked for.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{fs, path::PathBuf};

use crate::config::{app_dir, now_secs};

const CATALOG_FILE: &str = "catalog.json";

#[derive(Serialize, Deserialize, Clone, Default)]
pub(crate) struct Entry {
    pub(crate) t: String,
    pub(crate) u: String,
}

#[derive(Serialize, Deserialize, Default)]
pub(crate) struct Catalog {
    pub(crate) fetched_at: u64,
    pub(crate) entries: Vec<Entry>,
}

pub(crate) fn catalog_path() -> Option<PathBuf> { app_dir().map(|d| d.join(CATALOG_FILE)) }

pub(crate) fn load_catalog(path: Option<&PathBuf>) -> Result<Catalog> {
    let p = path.with_context(|| format!("missing {}, run ohmyanime --update", CATALOG_FILE))?;
    let raw = fs::read_to_string(p)?;
    let c: Catalog = serde_json::from_str(&raw)?;
    if c.entries.is_empty() {
        bail!("{} loaded but empty — run ohmyanime --update", p.display());
    }
    Ok(c)
}

pub(crate) fn save_catalog(c: &Catalog) -> Result<()> {
    let dir = app_dir().context("cannot resolve the user config directory")?;
    fs::create_dir_all(&dir)?;
    let path = dir.join(CATALOG_FILE);
    fs::write(&path, serde_json::to_vec(c)?)?;
    Ok(())
}

/// One search against the local catalogue: match words in any order.
pub(crate) fn match_catalogue<'a>(entries: &'a [Entry], q: &str) -> Vec<&'a Entry> {
    let words: Vec<String> = q.split_whitespace().map(str::to_lowercase).collect();
    if words.is_empty() { return vec![]; }
    entries.iter()
        .filter(|e| words.iter().all(|w| e.t.to_lowercase().contains(w)))
        .collect()
}

/// Stamp helper for crawls (see site.rs).
pub(crate) fn stamp_catalog(entries: Vec<Entry>) -> Catalog {
    Catalog { fetched_at: now_secs(), entries }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(t: &str, u: &str) -> Entry { Entry { t: t.into(), u: u.into() } }

    #[test]
    fn catalogue_matches_words_in_any_order() {
        let entries = vec![
            entry("Serial Experiments Lain (ITA)", "https://x/play/lain.1"),
            entry("One Piece (ITA)", "https://x/play/op.1"),
        ];
        assert_eq!(match_catalogue(&entries, "ITA lain").len(), 1);
        assert_eq!(match_catalogue(&entries, "lain (ITA)").len(), 1);
        assert_eq!(match_catalogue(&entries, "one piece").len(), 1);
        assert_eq!(match_catalogue(&entries, "nonexistent").len(), 0);
    }
}
