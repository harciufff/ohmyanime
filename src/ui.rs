// Interactive selection: one fzf menu per step (series -> season ->
// episode block -> episode). Rows carry the URL after a TAB; fzf shows only
// the title via `--with-nth 1`. Every menu is traced to menu.log so a silent
// fzf failure is always diagnosable.

use anyhow::{anyhow, Result};
use std::{
    env, fs,
    fs::OpenOptions,
    path::PathBuf,
    process::Stdio,
};
use tokio::{io::AsyncWriteExt, process};

use crate::{
    cache::{Episode, Season},
    catalog::{match_catalogue, Entry},
    config::{app_dir, now_secs},
};

/// Build and run one interactive fzf over the given lines. Returns the picked
/// line, or None on Esc/Ctrl-C/empty input. Zero-care fzf flags: --filter would
/// close the menu instead of letting the user bail.
///
/// fzf reads KEYS from /dev/tty (never from the piped stdin), so the menu draws
/// on the user's terminal and keystrokes go to the app even though stdin here is
/// a pipe we own.
pub(crate) async fn pick(lines: Vec<String>, prompt: &str, opts: &[&str]) -> Result<Option<String>> {
    if lines.is_empty() { return Ok(None); }
    let mut cmd = process::Command::new("fzf");

    // NEVER inherit the user's fzf globals (ohmyanime owns its flags; custom
    // FZF_DEFAULT_OPTS like --exit-0 / weird layouts change fzf's behaviour
    // and used to make menus exit instantly and silently).
    cmd.env_remove("FZF_DEFAULT_OPTS").env_remove("FZF_DEFAULT_COMMAND");

    // Always trace menus to menu.log — the silent-exit killer: count, prompt,
    // stdout/stderr of fzf for every invocation.
    let trace = |msg: &str| {
        if let Some(dir) = app_dir() {
            let p = dir.join("menu.log");
            if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&p) {
                use std::io::Write;
                let _ = writeln!(f, "[{} {}] {}", now_secs(), prompt.trim(), msg);
            }
        }
    };
    trace(&format!("open: {} rows", lines.len()));

    // Debugging hook: dump the exact rows of this menu to a file so a failing
    // run can be inspected (which menu, what its content was).
    if let Some(slot) = env::var_os("OHMYANIME_MENU_PATH") {
        let path = PathBuf::from(&slot);
        if let Some(dir) = path.parent() { let _ = fs::create_dir_all(dir); }
        let _ = fs::write(&path, lines.join("\n"));
    }

    let height: &str = "~100%";
    // --read0 on INPUT mirrors --print0 on output; without it fzf treats the
    // whole NUL-fed stream as one giant line (--with-nth then shows a single
    // result no matter how many entries exist).
    cmd.args(["--prompt", prompt, "--with-nth", "1", "--delimiter", "\t",
              "--height", height, "--reverse",
              "--read0", "--print0", "--no-multi"])
        .args(opts)
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| anyhow!("fzf not found — install it (e.g. brew install fzf): {e}"))?;
    if let Some(mut stdin) = child.stdin.take() {
        // fzf may die while we are still feeding rows (bad flags, downgraded
        // TERM, wrapper quirks). EPIPE here must NOT crash the app: fall into
        // the wait below which explains the failure.
        let mut feed_ok = true;
        for l in &lines {
            if stdin.write_all(l.as_bytes()).await.is_err()
                || stdin.write_all(b"\0").await.is_err()
            {
                feed_ok = false;
                break;
            }
        }
        if feed_ok { let _ = stdin.flush().await; }
        drop(stdin);
    }
    let out = child.wait_with_output().await?;
    let fzf_err = String::from_utf8_lossy(&out.stderr);
    let picked_line = String::from_utf8_lossy(&out.stdout).trim_matches('\0').to_string();
    trace(&format!("closed: rc={:?} stdout={:.0?} stderr={:.120?}", out.status.code(), picked_line, fzf_err.trim()));
    match out.status.code() {
        Some(0) => {
            if picked_line.is_empty() {
                // rc==0 with NOTHING selected: fzf misbehaved (weird options,
                // broken TERM). Never silently throw the session away.
                eprintln!("menu '{prompt}' returned no selection (empty). Check ~/.config/ohmyanime/menu.log");
                Ok(None)
            } else {
                // Return the FULL line ("title\turl"): fzf already hides the
                // URL via --with-nth 1, and every caller splits on '\t' to
                // recover it (picked_url / pick_episode_from). Returning only
                // the title would strip the URL and silently abort the flow.
                Ok(Some(picked_line))
            }
        }
        None => { let t = out.status.to_string(); eprintln!("menu '{prompt}' ended: {t}"); Ok(None) }
        Some(code) => {
            // Any failure becomes VISIBLE: the user must never lose the session
            // to a silent fzf issue.
            eprintln!("menu '{prompt}' aborted: fzf exited {code}(s)");
            let e = fzf_err.trim();
            if !e.is_empty() { eprintln!("fzf stderr: {e}"); }
            if code == 130 { eprintln!("(you pressed Esc/Ctrl-C)"); }
            else if code == 1 { eprintln!("(no rows matched your typing; Esc aborts)"); }
            else if code == 2 {
                eprintln!("(fzf flag/env problem — send: echo $TERM; echo \"$FZF_DEFAULT_OPTS\"; fzf --version)");
            }
            else {
                eprintln!("(unexpected: full log in ~/.config/ohmyanime/menu.log)");
            }
            Ok(None)
        }
    }
}

/// Row the user sees: only the title; url kept after a TAB for plumbing.
pub(crate) fn row(e: &Entry) -> String { format!("{}\t{}", e.t, e.u) }

pub(crate) fn picked_url(line: &Option<String>) -> Option<String> {
    let l = line.as_ref()?;
    l.split('\t').nth(1).map(str::to_string)
}

/// Search + pick a title; returns the picked entry, if any.
pub(crate) async fn flow_pick_anime(arg_query: Option<String>, entries: &[Entry]) -> Result<Option<Entry>> {
    let q = match arg_query {
        Some(q) if !q.trim().is_empty() => q.trim().to_string(),
        // No argument: interactive search over everything we have.
        _ => {
            let lines: Vec<String> = entries.iter().map(row).collect();
            let line = pick(lines, "search: ", &[]).await?;
            let entry = picked_url(&line).and_then(|u| {
                entries.iter().find(|e| e.u == u).cloned()
                    .or_else(|| Some(Entry { t: line.clone().unwrap_or_default(), u }))
            });
            return Ok(entry);
        }
    };
    // Argument given: templated search, but still fuzzy-tolerant over the
    // catalogue (words can come in any order).
    let hits = match_catalogue(entries, &q);
    let line = pick(hits.iter().map(|e| row(e)).collect(), "pick: ", &[]).await?;
    let entry = picked_url(&line).map(|u| {
        match hits.iter().find(|e| e.u == u) {
            Some(e) => (*e).clone(),
            None => Entry { t: line.clone().unwrap_or_default(), u },
        }
    });
    Ok(entry)
}

/// ani-cli style episode browsing: series -> (block) -> episode -> play. Long
/// series are split into "Episodi 1-50", "Episodi 51-100", … blocks because the
/// site itself paginates the hidden episode lists that way. Short series go
/// straight to the episode menu. Esc inside a block goes back to the block menu;
/// Ctrl-C exits.
const EPISODE_BLOCK: u32 = 50;

pub(crate) async fn flow_pick_episode(eps: &[Episode]) -> Result<Option<String>> {
    if eps.len() as u64 <= EPISODE_BLOCK as u64 {
        return pick_episode_from(eps.iter().collect()).await;
    }
    let max = eps.last().unwrap().num.max(eps.len() as u32);
    let mut menu = Vec::new();
    let mut start = 1u32;
    while start <= max {
        let end = (start + EPISODE_BLOCK - 1).min(max);
        menu.push((format!("Episodi {}-{}", start, end), start, end));
        start = end + 1;
    }
    loop {
        let Some(label) = pick(menu.iter().map(|(l, _, _)| l.clone()).collect(), "blocco: ", &[]).await? else { return Ok(None) };
        let Some((_, a, b)) = menu.iter().find(|(l, _, _)| *l == label) else { return Ok(None) };
        let block: Vec<&Episode> = eps.iter().filter(|e| e.num >= *a && e.num <= *b).collect();
        if let Some(u) = pick_episode_from(block).await? { return Ok(Some(u)); }
        // user pressed Esc in the block menu: back to the block list
    }
}

/// One fzf over the given episodes, rows are `Episode N<TAB>url`.
async fn pick_episode_from(eps: Vec<&Episode>) -> Result<Option<String>> {
    let rows: Vec<String> = eps.iter()
        .map(|e| format!("Episode {}\t{}", e.num, e.url))
        .collect();
    let line = pick(rows, "episode: ", &[]).await?;
    Ok(line.as_ref().and_then(|l| l.split('\t').nth(1)).map(str::to_string))
}

/// Season menu: series -> season -> (block) -> episode -> play. Rows are
/// `title<TAB>url` so `picked_url` recovers the season page. One entry (just
/// the current page) skips the menu and goes straight to episodes; Esc backs
/// out to the series menu.
pub(crate) async fn flow_pick_season(seasons: &[Season]) -> Result<Option<Season>> {
    if seasons.is_empty() { return Ok(None); }
    if seasons.len() == 1 {
        return Ok(Some(seasons[0].clone()));
    }
    let rows: Vec<String> = seasons.iter()
        .map(|s| format!("{}{}\t{}", if s.cur { "▶ " } else { "" }, s.title, s.url))
        .collect();
    let line = pick(rows, "stagione: ", &[]).await?;
    let url = picked_url(&line);
    Ok(url.and_then(|u| seasons.iter().find(|s| s.url == u).cloned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(t: &str, u: &str) -> Entry { Entry { t: t.into(), u: u.into() } }

    #[test]
    fn rows_hide_the_url_from_the_user() {
        let e = entry("Lain (ITA)", "https://x/play/lain.1");
        let r = row(&e);
        assert_eq!(r.split('\t').next(), Some("Lain (ITA)"));
        assert_eq!(picked_url(&Some(r)).as_deref(), Some("https://x/play/lain.1"));
    }

    #[test]
    fn full_pick_line_keeps_the_url() {
        // Regression: pick() must return the whole "title\turl" line; the
        // old code truncated at the tab and every downstream picked_url()
        // returned None, silently killing the season/episode menus.
        let line = Some("Lain (ITA)\thttps://x/play/lain.1".to_string());
        assert_eq!(picked_url(&line).as_deref(), Some("https://x/play/lain.1"));
    }
}
