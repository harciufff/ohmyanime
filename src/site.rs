// Site layer (AnimeWorld): az-list crawl, episode/season fetching and all
// response parsing. Fetching goes through cdp.rs; snippets live in js.rs;
// results land in catalog.rs / cache.rs.

use anyhow::{anyhow, bail, Result};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
};

use crate::{
    cache::{save_episode_cache, Episode, EpisodeCache, Season},
    catalog::{save_catalog, stamp_catalog, Entry},
    cdp::{fetch_page_js, PageRunner},
    config::Config,
    js::{AZ_PAGE_JS, EPISODES_JS, HARVEST_JS, SEASONS_JS},
    obscura::ensure_obscura,
};

/// Crawl the site catalogue via Obscura and cache it.
pub(crate) async fn update(cfg: &Config) -> Result<usize> {
    let mut app = ensure_obscura(cfg).await?;
    let outcome = match update_crawl(cfg).await {
        Ok(n) => Ok(n),
        Err(e) => {
            eprintln!("fast crawl failed ({e}); falling back to one-page-at-a-time navigation");
            update_crawl_nav(cfg).await
        }
    };
    app.shutdown().await;
    outcome
}

/// Safety cap on az-list pages. The site publishes its real page count
/// (`window.paginationMaxPage`, ~275 today), so this only bounds a site that
/// stops reporting one (or reports nonsense).
const AZ_PAGES_MAX: u32 = 600;
/// Pages harmonized into one Runtime.evaluate call; each fetch performs the
/// batch with `crawl_fetch_conc()` concurrent XHRs.
const HARVEST_BATCH: u32 = 24;

/// Concurrent in-page fetches during a harvest batch. Override with
/// `OHMYANIME_FETCH_CONC` (clamped to 1..=32).
fn crawl_fetch_conc() -> u32 {
    env::var("OHMYANIME_FETCH_CONC").ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .unwrap_or(12)
        .clamp(1, 32)
}

/// One parsed/validated harvest page (settings from HARVEST_JS answer).
#[derive(serde::Deserialize)]
struct FetchedPage { status: u32, #[serde(default)] max: u32, #[serde(default)] items: Vec<Vec<String>>, #[serde(default)] _err: Option<String> }

#[derive(serde::Deserialize)]
struct FetchedBatch { #[serde(default)] ms: u64, #[serde(default)] pages: HashMap<String, FetchedPage> }

/// Episodes for one series: cache first, else a fetch() harvest, storing the
/// result. Returns Err only when fetching fails and there is no cache.
pub(crate) async fn episodes_for(cfg: &Config, cache: &mut EpisodeCache, series_url: &str) -> Result<Vec<Episode>> {
    if let Some(eps) = cache.episodes.get(series_url) {
        if !eps.is_empty() { return Ok(eps.clone()); }
    }
    let raw = fetch_page_js(cfg, series_url, EPISODES_JS).await
        .map_err(|e| anyhow!("cannot read {} episodes: {e}", series_url))?;
    let eps = parse_episodes(raw.as_str().unwrap_or("[]"));
    if eps.is_empty() {
        if let Some(eps) = cache.episodes.get(series_url) {
            if !eps.is_empty() { return Ok(eps.clone()); }
        }
        bail!("no episodes found for {series_url}");
    }
    cache.episodes.insert(series_url.to_string(), eps.clone());
    save_episode_cache(cache);
    Ok(eps)
}

/// Seasons for one picked title: cache first, else the "Correlati" widget via
/// fetch(). The current page is always entry #0 (marked `cur`), so the season
/// menu is uniform even when the widget is empty: 1 entry = skip the menu and
/// go straight to episodes, N entries = fzf over them.
pub(crate) async fn seasons_for(cfg: &Config, cache: &mut EpisodeCache, anime: &Entry) -> Result<Vec<Season>> {
    if let Some(ss) = cache.seasons.get(&anime.u) {
        if !ss.is_empty() { return Ok(ss.clone()); }
    }
    let raw = fetch_page_js(cfg, &anime.u, SEASONS_JS).await
        .map_err(|e| anyhow!("cannot read {} seasons: {e}", anime.u))?;
    let raw_s = raw.as_str().unwrap_or("{}");
    let mut rel = parse_seasons(raw_s);
    // Current page first: hide the jtitle noise, show the catalogue title.
    let cur_title = parse_seasons_current_title(raw_s);
    let cur_title = if cur_title.is_empty() { anime.t.clone() } else { cur_title };
    rel.retain(|s| s.url != anime.u);
    let mut out = vec![Season { title: cur_title, url: anime.u.clone(), cur: true }];
    out.extend(rel);
    cache.seasons.insert(anime.u.clone(), out.clone());
    save_episode_cache(cache);
    Ok(out)
}

/// Navigate a scratch page on the target origin and run one harvest batch.
/// The scratch page is chosen cheap on purpose: HTML 404 pages cost <400ms to
/// navigate, the real az-list would cost ~15s (see the probe notes in HISTORY).
async fn harvest_batch(runner: &mut PageRunner, cfg: &Config, pages: &[u32]) -> Result<FetchedBatch> {
    let js = HARVEST_JS
        .replace("__PAGES__", &serde_json::to_string(pages)?)
        .replace("__CONC__", &crawl_fetch_conc().to_string());
    let raw = runner.eval(&format!("{}/sidebar.html", cfg.source.base_url.trim_end_matches('/')), &js).await?;
    let v: FetchedBatch = if raw.is_string() {
        serde_json::from_str(raw.as_str().unwrap_or("{}"))?
    } else {
        serde_json::from_value(raw.clone())?
    };
    Ok(v)
}

/// Crawl the whole az-list with in-page fetches. The az-list is 1-based; every
/// page reveals the true page count, so the first batch sizes the crawl and the
/// rest runs in parallel batches until every page is in. Pages whose HTML
/// reports no page count (or repeats an earlier page) stop a count-free site.
///
/// Steady-state cost is one harvest batch (~1-2s) per ~24 pages, plus ONE full
/// navigation at the start to park the runner on the site's origin (the only
/// slow one — obscura waits for images/ads subresources there).
async fn update_crawl(cfg: &Config) -> Result<usize> {
    let mut runner = PageRunner::start(cfg).await?;
    let end = runner.eval(&format!("{}/sidebar.html", cfg.source.base_url.trim_end_matches('/')), "location.href").await
        .map_err(|e| anyhow!("origin boot page failed: {e}"))?;
    let _ = end.as_str().unwrap_or("");

    let mut page_rows: BTreeMap<u32, Vec<(String, String)>> = BTreeMap::new();
    let mut seen_first: HashSet<String> = HashSet::new();
    let mut max: u32 = 0;
    let mut next = 1;
    let mut failed: Vec<u32> = Vec::new();

    while next <= AZ_PAGES_MAX {
        let hi = if max > 0 { max } else { next + HARVEST_BATCH - 1 };
        let chunk: Vec<u32> = (next..=hi.min(next + HARVEST_BATCH - 1)).collect();
        if chunk.is_empty() { break; }
        let batch = harvest_batch(&mut runner, cfg, &chunk).await?;
        let mut stop = false;
        let mut got = 0;
        for p in &chunk {
            let Some(fp) = batch.pages.get(&p.to_string()) else {
                if max > 0 { failed.push(*p); } else { stop = true; }
                continue;
            };
            if fp.max > max { max = fp.max; }
            if fp.status != 200 || fp.items.is_empty() {
                if max > 0 { failed.push(*p); } else {
                    eprintln!("az-list page {p} ... empty/status {} - end of az-list", fp.status);
                    stop = true;
                }
                continue;
            }
            let rows: Vec<(String, String)> = fp.items.iter().filter_map(|it| {
                let mut it = it.iter();
                let u = it.next()?.trim().to_string();
                let t = it.next()?.trim().to_string();
                if u.is_empty() || t.is_empty() { None } else { Some((u, t)) }
            }).collect();
            let first = rows.first().map(|(_, t)| t.clone()).unwrap_or_default();
            if rows.is_empty() || (!first.is_empty() && !seen_first.insert(first.clone())) {
                eprintln!("az-list page {p} ... repeat of an earlier page - end of az-list");
                stop = true;
                continue;
            }
            eprintln!("az-list page {p} ... {} titles", rows.len());
            page_rows.insert(*p, rows);
            got += 1;
        }
        eprintln!("batch [{}-{}] ... {} pages, {} titles, {:.1}s\n", chunk.first().unwrap_or(&0), chunk.last().unwrap_or(&0), got, page_rows.values().map(|v| v.len()).sum::<usize>(), batch.ms as f64 / 1000.0);
        if stop && max == 0 { break; }
        next += HARVEST_BATCH;
        if max > 0 && next > max { break; }
    }
    runner.close().await;

    // Retry any page the first pass dropped, one small batch, then give up on it.
    if !failed.is_empty() {
        eprintln!("retrying {} failed pages", failed.len());
        let retry = failed.clone();
        // A fresh runner: the first one may have wedged on the last failed page.
        // The origin-boot step runs first again so the fetch()es stay same-origin.
        match (async {
            let mut r = PageRunner::start(cfg).await?;
            r.eval(&format!("{}/sidebar.html", cfg.source.base_url.trim_end_matches('/')), "location.href").await?;
            harvest_batch(&mut r, cfg, &retry[..retry.len().min(HARVEST_BATCH as usize)]).await
        }).await {
            Ok(batch) => {
                for p in &retry[..retry.len().min(HARVEST_BATCH as usize)] {
                    if let Some(fp) = batch.pages.get(&p.to_string()) {
                        if fp.status == 200 {
                            let rows: Vec<(String, String)> = fp.items.iter().filter_map(|it| {
                                let mut it = it.iter();
                                let u = it.next()?.trim().to_string();
                                let t = it.next()?.trim().to_string();
                                if u.is_empty() || t.is_empty() { None } else { Some((u, t)) }
                            }).collect();
                            if !rows.is_empty() {
                                eprintln!("az-list page {p} ... {} titles (retry ok)", rows.len());
                                page_rows.insert(*p, rows);
                            }
                        }
                    }
                }
            }
            Err(e) => eprintln!("retry batch failed: {e}"),
        }
        let still = failed.iter().filter(|p| !page_rows.contains_key(p)).count();
        if still > 0 { eprintln!("{still} pages could not be fetched - catalogue is missing those titles"); }
    }

    if max > 0 {
        let missing = (1..=max).filter(|p| !page_rows.contains_key(p)).count();
        if missing > 0 { eprintln!("az-list: {missing}/{max} pages missing"); }
    }
    save_pages(page_rows)
}

/// Fallback path used only when in-page harvesting is not possible (e.g. a
/// proxy/anti-bot setup where the browser exists but XHR is denied): walk the
/// az-list one navigation at a time with the HTML-extraction script.
async fn update_crawl_nav(cfg: &Config) -> Result<usize> {
    let mut runner = PageRunner::start(cfg).await?;
    let mut pages: BTreeMap<u32, Vec<(String, String)>> = BTreeMap::new();
    let mut seen_first: HashSet<String> = HashSet::new();
    let mut n = 1;
    while n <= AZ_PAGES_MAX {
        let raw = match runner.eval(&az_url(cfg, n), AZ_PAGE_JS).await {
            Ok(v) => v,
            Err(e) => {
                eprintln!("az-list page {n} ... FAILED ({e})");
                break;
            }
        };
        let p = parse_az_page(raw.as_str().unwrap_or(""));
        let title = p.entries.first().map(|e| e.t.clone()).unwrap_or_default();
        if p.entries.is_empty() || (!title.is_empty() && !seen_first.insert(title.clone())) {
            if n == 1 { runner.close().await; bail!("az-list page 1 returned no titles - is the site up?"); }
            eprintln!("az-list page {n} ... repeat of an earlier page - end of az-list");
            break;
        }
        eprintln!("az-list page {n} ... {} titles", p.entries.len());
        pages.insert(n, p.entries.into_iter().map(|e| (e.u, e.t)).collect());
        if p.max > 0 && n >= p.max { break; }
        n += 1;
    }
    runner.close().await;
    save_pages(pages)
}

/// Merge per-page rows (ascending page order, so nothing depends on worker
/// completion order), de-duplicate by href and write the catalogue.
fn save_pages(pages: BTreeMap<u32, Vec<(String, String)>>) -> Result<usize> {
    let mut found: HashMap<String, String> = HashMap::new();
    for (_pg, rows) in pages {
        for (u, t) in rows { found.insert(u, t); }
    }
    if found.is_empty() {
        bail!("catalogue crawl found nothing - is the site up?");
    }
    let entries = found.into_iter().map(|(u, t)| Entry { t, u }).collect();
    let c = stamp_catalog(entries);
    let n = c.entries.len();
    save_catalog(&c)?;
    Ok(n)
}

/// Percent-encode a query string. Used by the test suite to verify URL forms;
/// production sends whole az-list URLs (no free text).
#[cfg(test)]
fn urlencode(q: &str) -> String {
    q.bytes().map(|b| match b {
        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(),
        _ => format!("%{b:02X}"),
    }).collect()
}

/// /az-list pages: ?page=N, alphabetical.
pub(crate) fn az_url(cfg: &Config, page: u32) -> String {
    format!("{}/az-list?page={}", cfg.source.base_url.trim_end_matches('/'), page)
}

#[cfg(test)]
fn search_url(cfg: &Config, q: &str) -> String {
    format!("{}{}{}", cfg.source.base_url.trim_end_matches('/'), cfg.source.search_path, urlencode(q))
}

/// One parsed az-list page: its title rows plus the site's advertised page
/// count (`window.paginationMaxPage`, 0 when the site does not publish one).
struct AzPage { max: u32, entries: Vec<Entry> }

/// Parse an AZ_PAGE_JS answer into rows and the published page count.
fn parse_az_page(raw: &str) -> AzPage {
    let v: Value = serde_json::from_str(raw).unwrap_or(Value::Null);
    let max = v["max"].as_u64().unwrap_or(0) as u32;
    let entries = v["items"].as_array().cloned().unwrap_or_default().into_iter().filter_map(|o| {
        let t = o["t"].as_str()?.trim().to_string();
        let u = o["u"].as_str()?.to_string();
        if t.is_empty() { None } else { Some(Entry { t, u }) }
    }).collect();
    AzPage { max, entries }
}

fn parse_seasons(raw: &str) -> Vec<Season> {
    let v: Value = serde_json::from_str(raw).unwrap_or(Value::Null);
    // SEASONS_JS returns `{cur, rel:[{t,u}]}`; accept a bare array too.
    let rel = v.get("rel").and_then(|r| r.as_array()).cloned()
        .or_else(|| v.as_array().cloned())
        .unwrap_or_default();
    rel.into_iter().filter_map(|o| {
            let u = o["u"].as_str()?.to_string();
            let t = o["t"].as_str().unwrap_or("").trim().to_string();
            if u.is_empty() { None } else { Some(Season { title: if t.is_empty() { u.clone() } else { t }, url: u, cur: false }) }
        }).collect()
}

fn parse_seasons_current_title(raw: &str) -> String {
    serde_json::from_str::<Value>(raw).ok()
        .and_then(|v| v.get("cur").and_then(|c| c.as_str()).map(|s| s.trim().to_string()))
        .unwrap_or_default()
}

fn parse_episodes(raw: &str) -> Vec<Episode> {
    let mut eps: Vec<Episode> = serde_json::from_str::<Vec<Value>>(raw).unwrap_or_default()
        .into_iter().filter_map(|o| {
            let u = o["u"].as_str()?.to_string();
            let num = o["n"].as_u64().unwrap_or(0) as u32;
            if num == 0 || u.is_empty() { None } else { Some(Episode { num, url: u }) }
        }).collect();
    eps.sort_by_key(|e| e.num);
    eps
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queries_are_percent_encoded_and_joined() {
        assert_eq!(urlencode("a&b=c"), "a%26b%3Dc");
        let cfg = Config::default();
        assert_eq!(search_url(&cfg, "one punch man"), "https://www.animeworld.ac/search?keyword=one%20punch%20man");
        assert_eq!(az_url(&cfg, 2), "https://www.animeworld.ac/az-list?page=2");
    }

    #[test]
    fn seasons_parse_cur_plus_related() {
        let raw = r#"{"cur":"L'attacco dei Giganti (ITA)","rel":[{"t":"L'attacco dei Giganti 2","u":"https://x/play/a2"},{"t":"L'attacco dei Giganti 3","u":"https://x/play/a3"}]}"#;
        assert_eq!(parse_seasons_current_title(raw), "L'attacco dei Giganti (ITA)");
        let ss = parse_seasons(raw);
        assert_eq!(ss.len(), 2);
        assert_eq!(ss[0].url, "https://x/play/a2");
        // bare-array shape accepted too
        let bare = r#"[{"t":"Solo","u":"https://x/play/solo"}]"#;
        assert_eq!(parse_seasons(bare).len(), 1);
        assert!(parse_seasons("not json").is_empty());
        assert!(parse_seasons_current_title("not json").is_empty());
    }
}
