// ohmyanime — minimal anime search & play CLI.
// A local catalogue + fzf pick the title; Obscura over CDP lists episodes and
// resolves the stream; mpv plays it. The browser is launched only when needed:
// searching stays instant and offline.
//
// Module map:
//   config  — site/endpoint config + app dirs
//   catalog — title catalogue (catalog.json) + local matching
//   cache   — per-series episode/season cache (episodes.json)
//   js      — extraction snippets evaluated in the browser
//   cdp     — minimal Obscura/CDP client + page runners
//   obscura — Obscura process lifecycle + AppGuard (owns spawned children)
//   site    — az-list crawl, episode/season fetching, response parsing
//   ui      — fzf menus (series -> season -> block -> episode)

mod cache;
mod catalog;
mod cdp;
mod config;
mod js;
mod obscura;
mod player;
mod site;
mod ui;

use anyhow::{bail, Context, Result};
use std::{env, fs};

use crate::{
    cache::{episodes_path, load_episode_cache},
    catalog::{catalog_path, load_catalog},
    cdp::{fetch_page_js, page_eval},
    config::load_config,
    js::STREAM_JS,
    obscura::ensure_obscura,
    site::{episodes_for, seasons_for, update},
    ui::{flow_pick_anime, flow_pick_episode, flow_pick_season},
};

#[cfg(unix)]
use crate::obscura::kill_tracked_sync;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!("ohmyanime — search & play anime from AnimeWorld via Obscura+mpv");
        println!();
        println!("usage:");
        println!("  ohmyanime [query] [--update [--episodes]]");
        println!();
        println!("flow: pick series (fzf) -> season (skipped when alone) ->");
        println!("      episode block (series > 50 eps) -> episode -> mpv");
        println!("player: mpv, or mac mpv inside OrbStack (mac-mpv bridge).");
        println!();
        println!("  --update            recrawl the title catalogue");
        println!("  --update --episodes drop the episode/season cache too");
        println!("  -h, --help          this help");
        return Ok(());
    }
    // Ctrl-C / SIGTERM racing the app: `process::exit` runs no destructors,
    // so the AppGuard Drop never fires here — the branches below kill what
    // this run spawned via kill_tracked_sync(). A borrowed (pre-existing)
    // Obscura is left running either way.
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let fut = run(args);
        tokio::pin!(fut);
        tokio::select! {
            r = &mut fut => return r,
            _ = tokio::signal::ctrl_c() => {
                eprintln!("interrupted — shutting down …");
                kill_tracked_sync();
                std::process::exit(130);
            }
            _ = sigterm.recv() => {
                eprintln!("terminated — shutting down …");
                kill_tracked_sync();
                std::process::exit(143);
            }
        }
    }
    #[cfg(not(unix))]
    {
        run(args).await
    }
}

/// Lazy lifecycle: searching the local catalogue needs no browser; Obscura
/// is ensured only once playback data is needed (seasons/episodes/stream).
/// The AppGuard below owns every child this run started, so all exits —
/// clean, `?`, `bail!`, panic — tear down what we spawned.
async fn run(args: Vec<String>) -> Result<()> {
    let cfg = load_config().context("loading config")?;

    // Debug probe: send STREAM_JS at one episode URL through both paths and
    // print exactly what Obscura answers (payload or full exception text).
    if let Some(i) = args.iter().position(|a| a == "--probe-stream") {
        let url = args.get(i + 1).cloned().unwrap_or_default();
        if url.is_empty() { bail!("usage: ohmyanime --probe-stream <episode-url>"); }
        let mut app = ensure_obscura(&cfg).await?;
        eprintln!("probe: fetch-wrapper path …");
        match fetch_page_js(&cfg, &url, STREAM_JS).await {
            Ok(v) => println!("fetch-wrapper OK: {}", v),
            Err(e) => println!("fetch-wrapper ERR: {e:#}"),
        }
        eprintln!("probe: page_eval path …");
        match page_eval(&cfg, &url, STREAM_JS).await {
            Ok(v) => println!("page_eval OK: {}", v),
            Err(e) => println!("page_eval ERR: {e:#}"),
        }
        app.shutdown().await;
        return Ok(());
    }

    let update_only = args.iter().any(|a| a == "--update");
    if update_only {
        if args.iter().any(|a| a == "--episodes") {
            // --update --episodes also drops the episode/season cache: titles
            // are stable, episode indexes are not (new eps air weekly).
            if let Some(path) = episodes_path() {
                if path.exists() {
                    fs::remove_file(&path).context("clearing episode cache")?;
                    eprintln!("episode/season cache cleared");
                }
            }
        }
        let n = update(&cfg).await?;
        eprintln!("catalogue saved: {n} titles");
        return Ok(());
    }

    let cat = load_catalog(catalog_path().as_ref()).context("loading catalogue")?;
    // First positional arg is the search query; flags (and the --probe-stream
    // URL, which starts with https://) are never a query.
    let arg_query: Option<String> = args.iter()
        .position(|a| a == "--probe-stream")
        .map(|i| args.iter().take(i).find(|a| !a.starts_with("--")).cloned())
        .unwrap_or_else(|| args.iter().find(|a| !a.starts_with("--")).cloned());

    // Search needs no browser — pure local fuzzy over the cached catalogue.
    let picked = flow_pick_anime(arg_query, &cat.entries).await?;

    let Some(mut anime) = picked else {
        if env::var_os("OHMYANIME_DEBUG").is_some() { eprintln!("no series picked — exiting quietly"); }
        return Ok(()); // user bailed: exit 0, silent
    };

    // From here only, bring up the browser. The guard below owns a spawned
    // obscura (a pre-existing one is borrowed) and every mpv child; it is
    // torn down only when the user leaves entirely.
    let mut app = ensure_obscura(&cfg).await?;

    // Watch/replay loop (ani-cli behaviour):
    //   series -> season (skipped when alone) -> block/episode -> mpv,
    // then back to the episode menu. Esc in the episode menu goes back to the
    // season menu, Esc there back to the series menu. `app` keeps obscura
    // alive between menus; it is torn down only when the user leaves entirely.
    let mut epcache = load_episode_cache();
    'series: loop {
        eprintln!("loading seasons for {} …", anime.t);
        let seasons = match seasons_for(&cfg, &mut epcache, &anime).await {
            Ok(s) => s,
            Err(e) => { app.shutdown().await; return Err(e); }
        };
        if seasons.is_empty() { app.shutdown().await; bail!("no seasons found for {}", anime.t); }
        if seasons.len() > 1 {
            eprintln!("found {} related entries", seasons.len());
        }

        let Some(season) = flow_pick_season(&seasons).await? else {
            // Esc in the season menu: back to the series menu.
            let picked = flow_pick_anime(None, &cat.entries).await?;
            let Some(next) = picked else { break 'series };
            anime = next;
            continue 'series;
        };
        let label = if season.title.is_empty() { anime.t.clone() } else { season.title.clone() };

        'episodes: loop {
            eprintln!("loading episodes for {label} …");
            let eps = match episodes_for(&cfg, &mut epcache, &season.url).await {
                Ok(eps) => eps,
                Err(e) => {
                    eprintln!("{e} - back to the season menu");
                    continue 'series;
                }
            };
            if eps.is_empty() { app.shutdown().await; bail!("no episodes found for {label}"); }

            let Some(ep_url) = flow_pick_episode(&eps).await? else {
                // Esc in the episode menu: back to the season menu when there
                // is one, else back to the series menu (reloading the same
                // episode list forever would be an infinite loop).
                if seasons.len() > 1 { continue 'series; }
                let picked = flow_pick_anime(None, &cat.entries).await?;
                let Some(next) = picked else { break 'series };
                anime = next;
                continue 'series;
            };

            eprintln!("resolving stream …");
            let raw_stream = match fetch_page_js(&cfg, &ep_url, STREAM_JS).await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("stream resolve failed ({e}) - pick another episode");
                    continue 'episodes;
                }
            };
            let stream = raw_stream.as_str().unwrap_or("").trim().to_string();
            if stream.is_empty() {
                eprintln!("no video URL found for that episode - pick another");
                continue 'episodes;
            }
            if let Err(e) = app.play(&stream).await {
                // keep the menu: a missing player must not kill the whole session
                eprintln!("{e} - returning to the episode menu");
            }
        }
    }
    app.shutdown().await;
    Ok(())
}
