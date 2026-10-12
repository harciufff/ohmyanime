// ohMyAnime — minimal anime search & play CLI.
// A local catalogue + fzf pick the title; Obscura over CDP lists episodes and
// resolves the stream; mpv plays it. The browser is launched only when needed:
// searching stays instant and offline.

use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env, fs,
    fs::OpenOptions,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant, UNIX_EPOCH},
};
use tokio::{io::AsyncWriteExt, net::TcpStream, process};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

// ---------------------------------------------------------------- config

#[derive(Serialize, Deserialize, Clone)]
struct Source { base_url: String, search_path: String }

#[derive(Serialize, Deserialize, Clone)]
struct Scrapper { obscura_endpoint: String }

#[derive(Serialize, Deserialize, Clone)]
struct Config { source: Source, scrapper: Scrapper }

impl Default for Config {
    fn default() -> Self {
        let source = Source { base_url: "https://www.animeworld.ac".into(), search_path: "/search?keyword=".into() };
        Self { source, scrapper: Scrapper { obscura_endpoint: "ws://127.0.0.1:9222".into() } }
    }
}

fn app_dir() -> Option<PathBuf> { dirs::config_dir().map(|d| d.join("ohMyAnime")) }

fn load_config() -> Result<Config> {
    let dir = app_dir().ok_or_else(|| anyhow!("cannot resolve the user config directory"))?;
    let path = dir.join("config.toml");
    if !path.exists() {
        fs::create_dir_all(&dir).context("creating config dir")?;
        fs::write(&path, toml::to_string_pretty(&Config::default())?)?;
        eprintln!("Seeded default config at {}", path.display());
    }
    Ok(toml::from_str(&fs::read_to_string(&path)?)?)
}

// ------------------------------------------------------------- catalogue

/// The whole site catalogue on disk: one JSON file under the app dir. A hit in
/// here is free; the site's /az-list pages are only crawled when missing or
/// asked for (--update).
const CATALOG_FILE: &str = "catalog.json";

#[derive(Serialize, Deserialize, Clone, Default)]
struct Entry { t: String, u: String }

#[derive(Serialize, Deserialize, Default)]
struct Catalog { fetched_at: u64, entries: Vec<Entry> }

fn catalog_path() -> Option<PathBuf> { app_dir().map(|d| d.join(CATALOG_FILE)) }

fn now_secs() -> u64 {
    std::time::SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn load_catalog(path: Option<&PathBuf>) -> Result<Catalog> {
    let p = path.with_context(|| format!("missing {}, run ohmyanime --update", CATALOG_FILE))?;
    let raw = fs::read_to_string(p)?;
    let c: Catalog = serde_json::from_str(&raw)?;
    if c.entries.is_empty() {
        bail!("{} loaded but empty — run ohmyanime --update", p.display());
    }
    Ok(c)
}

fn save_catalog(c: &Catalog) -> Result<()> {
    let dir = app_dir().context("cannot resolve the user config directory")?;
    fs::create_dir_all(&dir)?;
    let path = dir.join(CATALOG_FILE);
    fs::write(&path, serde_json::to_vec(c)?)?;
    Ok(())
}

// ------------------------------------------------------------- selection

/// Build and run one interactive fzf over the given lines. Returns the picked
/// line, or None on Esc/Ctrl-C/empty input. Zero-care fzf flags: --filter would
/// close the menu instead of letting the user bail.
///
/// fzf reads KEYS from /dev/tty (never from the piped stdin), so the menu draws
/// on the user's terminal and keystrokes go to the app even though stdin here is
/// a pipe we own.
async fn pick(lines: Vec<String>, prompt: &str, opts: &[&str]) -> Result<Option<String>> {
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
                eprintln!("menu '{prompt}' returned no selection (empty). Check ~/.config/ohMyAnime/menu.log");
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
                eprintln!("(unexpected: full log in ~/.config/ohMyAnime/menu.log)");
            }
            Ok(None)
        }
    }
}

/// Row the user sees: only the title; url kept after a TAB for plumbing.
fn row(e: &Entry) -> String { format!("{}\t{}", e.t, e.u) }

fn picked_url(line: &Option<String>) -> Option<String> {
    let l = line.as_ref()?;
    l.split('\t').nth(1).map(str::to_string)
}

// ------------------------------------------------------- episode index cache

/// Per-series episode + season indexes, cached next to the catalogue
/// (episodes.json). Two maps keyed by the series /play/ URL: episodes are
/// fetched once per season page and remembered; seasons once per picked
/// title. Every visit after the first is instant and offline, exactly like
/// the title catalogue. `--update` keeps the cache (titles are stable),
/// episodes/seasons are re-checked only via `--update --episodes`.
const EPISODES_FILE: &str = "episodes.json";

#[derive(Serialize, Deserialize, Default)]
struct EpisodeCache {
    #[serde(default)]
    episodes: HashMap<String, Vec<Episode>>,
    #[serde(default)]
    seasons: HashMap<String, Vec<Season>>,
}

fn episodes_path() -> Option<PathBuf> { app_dir().map(|d| d.join(EPISODES_FILE)) }

fn load_episode_cache() -> EpisodeCache {
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

fn save_episode_cache(c: &EpisodeCache) {
    if let Some(dir) = app_dir() {
        let _ = fs::create_dir_all(&dir);
        if let Some(p) = episodes_path() {
            let _ = fs::write(&p, serde_json::to_vec(c).unwrap_or_default());
        }
    }
}

// ----------------------------------------------------------- obscura / CDP

struct Cdp {
    ws: Ws,
    id: u64,
    pending: HashMap<u64, Value>,
    load_fired: bool,
}

fn cdp_candidates(endpoint: &str) -> Vec<String> {
    let base = endpoint.trim_end_matches('/');
    let mut urls = vec![base.to_string()];
    if base.matches('/').count() <= 2 {
        urls.push(format!("{base}/devtools/browser"));
    }
    urls
}

async fn read_frame(ws: &mut Ws) -> Result<Value> {
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(45), ws.next())
            .await
            .map_err(|_| anyhow!("CDP read timeout"))?
            .ok_or_else(|| anyhow!("CDP socket closed"))??;
        match msg {
            Message::Ping(p) => { let _ = ws.send(Message::Pong(p)).await; }
            Message::Text(t) => return serde_json::from_str(&t).context("bad CDP frame"),
            _ => {}
        }
    }
}

impl Cdp {
    async fn connect(endpoint: &str) -> Result<Self> {
        for url in cdp_candidates(endpoint) {
            let attempt = tokio::time::timeout(Duration::from_secs(4), connect_async(url.as_str())).await;
            if let Ok(Ok((ws, _))) = attempt {
                return Ok(Self { ws, id: 0, pending: HashMap::new(), load_fired: false });
            }
        }
        bail!("cannot reach Obscura CDP at {endpoint}");
    }

    async fn call(&mut self, method: &str, params: Value, session: Option<&str>) -> Result<Value> {
        self.id += 1;
        let id = self.id;
        if let Some(v) = self.pending.remove(&id) {
            return Cdp::result_of(method, v);
        }
        let mut msg = json!({ "id": id, "method": method, "params": params });
        if let Some(s) = session { msg["sessionId"] = json!(s); }
        self.ws.send(Message::Text(msg.to_string())).await?;
        loop {
            let v = read_frame(&mut self.ws).await?;
            if let Some(rid) = v["id"].as_u64() {
                if rid == id {
                    return Cdp::result_of(method, v);
                }
                self.pending.insert(rid, v);
            } else if v["method"] == "Page.loadEventFired" {
                self.load_fired = true;
            }
        }
    }

    fn result_of(method: &str, v: Value) -> Result<Value> {
        if let Some(e) = v.get("error") {
            bail!("CDP {method} failed: {e}");
        }
        Ok(v["result"].clone())
    }

    async fn wait_load(&mut self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while !self.load_fired {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() { break; }
            // Quiet periods must NOT end the wait: keep polling the socket until
            // the load event arrives or the deadline passes (a bare `break` here
            // made the advertised wait as short as the first 600ms gap).
            match tokio::time::timeout(left.min(Duration::from_millis(600)), read_frame(&mut self.ws)).await {
                Err(_) => continue,
                Ok(Err(_)) => break,
                Ok(Ok(v)) => match v["id"].as_u64() {
                    Some(rid) => { self.pending.insert(rid, v); }
                    None if v["method"] == "Page.loadEventFired" => { self.load_fired = true; }
                    None => {}
                },
            }
        }
    }
}

/// Navigate on a fresh target, wait for the load event, then run JS.
/// Worker-local page runner: one websocket + one reusable browser target.
/// A crawl page is navega→poll→harvest, ~1.5-2s in the good case; creating a
/// target per page (old behaviour) costs an extra ~2-4s per page on top.
struct PageRunner {
    cdp: Cdp,
    tsid: String,
}

impl PageRunner {
    /// Fresh connection + target, to be reused across many page_eval calls.
    async fn start(cfg: &Config) -> Result<Self> {
        let mut cdp = Cdp::connect(&cfg.scrapper.obscura_endpoint).await?;
        let tid = cdp.call("Target.createTarget", json!({"url": "about:blank"}), None)
            .await?["targetId"].as_str().context("missing targetId")?.to_string();
        let s = cdp.call("Target.attachToTarget", json!({"targetId": tid, "flatten": true}), None)
            .await?["sessionId"].as_str().context("missing sessionId")?.to_string();
        cdp.call("Page.enable", json!({}), Some(&s)).await?;
        Ok(PageRunner { cdp, tsid: format!("{tid}|{s}") })
    }

    /// One page via the reused target: navigate, short load grace, poll JS.
    async fn eval(&mut self, url: &str, js: &str) -> Result<Value> {
        let (tid, s) = self.tsid.split_once('|').context("bad runner state")?;
        self.cdp.call("Page.navigate", json!({"url": url}), Some(s)).await?;
        // Poll immediately: the first Runtime.evaluate after navigate() blocks
        // until the navigation settles (CDP semantics), so no fixed 12s wait
        // is needed; the settled-poll handles XHR-driven lists.
        self.cdp.load_fired = false;
        let deadline = Instant::now() + Duration::from_secs(8);
        let res = loop {
            let res = self.cdp.call("Runtime.evaluate", json!({"expression": js, "returnByValue": true, "awaitPromise": true}), Some(s)).await?;
            if let Some(e) = res.get("exceptionDetails") {
                bail!("page script error: {}", e["exception"]["description"].as_str().unwrap_or("unknown"));
            }
            let v = &res["result"]["value"];
            let settled = !v.is_null()
                && v.as_str().is_none_or(|s_| !(s_.is_empty() || s_ == "[]"));
            if settled || Instant::now() >= deadline {
                break res;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        };
        let _ = tid;
        Ok(res["result"]["value"].clone())
    }

    async fn close(&mut self) {
        let (tid, _) = self.tsid.split_once('|').unwrap_or(("", ""));
        let _ = self.cdp.call("Target.closeTarget", json!({"targetId": tid}), None).await;
    }
}

/// One-shot page eval (episodes, stream): fresh target, full load wait.
async fn page_eval(cfg: &Config, url: &str, js: &str) -> Result<Value> {
    let mut cdp = Cdp::connect(&cfg.scrapper.obscura_endpoint).await?;
    let tid = cdp.call("Target.createTarget", json!({"url": "about:blank"}), None)
        .await?["targetId"].as_str().context("missing targetId")?.to_string();
    let s = cdp.call("Target.attachToTarget", json!({"targetId": tid, "flatten": true}), None)
        .await?["sessionId"].as_str().context("missing sessionId")?.to_string();
    cdp.call("Page.enable", json!({}), Some(&s)).await?;
    cdp.call("Page.navigate", json!({"url": url}), Some(&s)).await?;
    cdp.wait_load(Duration::from_secs(12)).await;
    // XHR-driven lists may render after the load event; poll the snippet and
    // take the first non-empty answer (cap keeps empty pages fast).
    let deadline = Instant::now() + Duration::from_millis(2500);
    let res = loop {
        let res = cdp.call("Runtime.evaluate", json!({"expression": js, "returnByValue": true, "awaitPromise": true}), Some(&s)).await?;
        if let Some(e) = res.get("exceptionDetails") {
            bail!("page script error: {}", e["exception"]["description"].as_str().unwrap_or("unknown"));
        }
        let v = &res["result"]["value"];
        let settled = !v.is_null()
            && v.as_str().is_none_or(|s| !(s.is_empty() || s == "[]"));
        if settled || Instant::now() >= deadline {
            break res;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    };
    let _ = cdp.call("Target.closeTarget", json!({"targetId": tid}), None).await;
    Ok(res["result"]["value"].clone())
}

// ---------------------------------------------------------- extraction JS

/// Anchor harvesting for one az-list page. The az-list markup carries each
/// entry twice: a text anchor (`a.name`) and a thumb anchor wrapping
/// `<img alt="title">` at the same /play/ href. Rows are merged by href, keeping
/// whichever label is non-empty, so thumbs-only entries (the ones whose text
/// anchor is missing) are not lost.
///
/// Returns `{"max":N,"items":[{t,u}...]}` where `max` is the page count the site
/// publishes as `window.paginationMaxPage` (0 when absent) — one fetch of page 1
/// is therefore enough to size the whole crawl.
/// An empty page returns `''` on purpose: the caller keeps polling until the
/// value is non-empty, and a rendered-but-empty page must not look "settled".
const AZ_PAGE_JS: &str = r##"
(() => {
  const byHref = {};
  Array.from(document.querySelectorAll('a[href]')).forEach(a => {
    const u = a.href;
    if (!u || !/^https?:/i.test(u) || u.indexOf('/play/') < 0) return;
    let t = (a.textContent || '').trim();
    if (!t) {
      const img = a.querySelector('img[alt]');
      if (img) t = (img.getAttribute('alt') || '').trim();
    }
    if (!t && a.getAttribute('data-jtitle')) t = a.getAttribute('data-jtitle').trim();
    if (!t) return;
    if (t.length >= 120) t = t.slice(0, 119);
    if (!byHref[u] || t) byHref[u] = t;
  });
  const tags = document.querySelectorAll('a.name[data-jtitle]');
  tags.forEach(a => {
    const u = a.href;
    const t = (a.textContent || '').trim();
    if (u && /^https?:/i.test(u) && t && t.length < 120 && byHref[u] !== t) {
      // keep the visible text over the jtitle when both exist for one href
      if (!byHref[u]) byHref[u] = t;
    }
  });
  const items = Object.keys(byHref)
    .filter(u => byHref[u])
    .map(u => ({ t: byHref[u], u: u }));
  if (!items.length) return '';
  return JSON.stringify({ max: Number(window.paginationMaxPage) || 0, items: items });
})()
"##;

/// Episode list from the anime page. A long series splits its episodes into
/// per-server block `<ul>`s ("1 - 50", "51 - 100", …); every anchor carries
/// `data-episode-num`, and the same episode number can appear in more than one
/// block (re-pegged servers). Dedupe keeps the first URL per number, so the
/// numbering is always what the user sees in fzf.
const EPISODES_JS: &str = r##"
(() => {
  const seen = {};
  const out = [];
  Array.from(document.querySelectorAll('a[data-episode-num]')).forEach(a => {
    const n = Number(a.getAttribute('data-episode-num')) || 0;
    const u = a.href;
    if (n > 0 && u && /^https?:/i.test(u) && !seen[u]) { seen[u] = 1; out.push({ n: n, u: u }); }
  });
  out.sort((x, y) => x.n - y.n);
  return JSON.stringify(out);
})()
"##;

/// Related-series list ("Correlati" widget) + current page title. On
/// AnimeWorld seasons are separate `/play/...` pages, so the season menu is
/// this list with the current page prepended by the caller.
const SEASONS_JS: &str = r##"
(() => {
  const seen = {};
  const rel = [];
  const push = (u, t) => {
    if (!u || !/^https?:/i.test(u) || u.indexOf('/play/') < 0) return;
    u = u.split('#')[0];
    if (seen[u]) return;
    seen[u] = 1;
    t = (t || '').trim();
    if (t) rel.push({ t: t.slice(0, 119), u: u });
  };
  const widget = document.querySelector('.widget-body.related')
    || (() => {
      const els = Array.from(document.querySelectorAll('.widget-title .title'));
      const h = els.find(e => /correlati/i.test(e.textContent || ''));
      return h ? h.closest('.widget') : null;
    })();
  const scope = widget || document;
  Array.from(scope.querySelectorAll('a.name[href], a[href].name')).forEach(a => {
    push(a.href, a.textContent);
  });
  const titleEl = document.querySelector('#anime-title, h1.title');
  const curT = titleEl ? (titleEl.textContent || '').trim().slice(0, 119) : '';
  return JSON.stringify({ cur: curT, rel: rel });
})()
"##;

/// Resolve the video URL. Runs inside fetch_page_js (fetched HTML parsed with
/// DOMParser, `document` rebound via with) AND inside page_eval (live page):
/// keep it free of page-relative fetches. The API URL is absolute, the
/// response status is checked before .json() (a CF challenge page is HTML,
/// not JSON — parsing it must fall through to the regex, not throw), and the
/// regex fallback scans the fetched/live HTML for a direct file.
const STREAM_JS: &str = r##"
(async (doc, origin) => {
  doc = doc || document;
  origin = origin || location.origin;
  try {
    const box = doc.querySelector('#player[data-id], .player-box[data-id]');
    const id = box && box.getAttribute('data-id');
    const meta = doc.getElementById('csrf-token');
    const csrf = (typeof window !== 'undefined' && window.csrfToken) || (meta && meta.content) || '';
    if (id) {
      const r = await fetch(origin + '/api/episode/info?id=' + encodeURIComponent(id) + '&alt=0', {
        credentials: 'same-origin',
        headers: { 'CSRF-Token': csrf },
      });
      if (r && r.ok) {
        const j = await r.json();
        if (j && j.grabber) return j.grabber;
        if (j && j.target) return new URL(j.target, origin).href;
      }
    }
  } catch (e) {}
  try {
    const el = doc.querySelector('video source, video');
    let u = el && (el.currentSrc || el.src || el.getAttribute('src'));
    if (!u) {
      const html = (doc.documentElement && doc.documentElement.innerHTML) || '';
      const m = html.match(/https?:\/\/[^"'\s\\]+?\.(?:m3u8|mp4)[^"'\s\\]*/i);
      u = m ? m[0] : '';
    }
    return u || '';
  } catch (e) { return ''; }
})((typeof doc !== 'undefined' ? doc : document), (typeof origin !== 'undefined' ? origin : location.origin))
"##;

/// Run `js` against a *fetched* page without ever navigating to it: the page's
/// HTML is fetched from a cheap scratch page on the same origin and parsed with
/// DOMParser, so the heavy subresource wait never happens. Anchors parsed this
/// way are rewired to absolute URLs (the JS snippets use `a.href`, which on a
/// DOMParser document is just the raw attribute); a scratch `base` element
/// makes `a.href` resolve correctly, so the snippets need no special casing.
///
/// Falls back to `page_eval` (a real navigation) when the same-origin fetch is
/// not possible — the snippets there run against the live DOM unchanged.
async fn fetch_page_js(cfg: &Config, url: &str, js: &str) -> Result<Value> {
    let wrapped = format!(
        r#"
(async () => {{
  const t0 = Date.now();
  const r = await fetch({url_json}, {{ credentials: 'same-origin' }});
  if (!r.ok) return JSON.stringify({{ error: 'HTTP ' + r.status }});
  const html = await r.text();
  const doc = new DOMParser().parseFromString(html, 'text/html');
  const base = doc.createElement('base'); base.href = {origin_json};
  doc.head.appendChild(base);
  const js = {js_json};
  // Snippets reference `document.` freely; inside this wrapper that would hit
  // the scratch page's document, not the fetched one. `with (doc)` rebinds the
  // global `document` lookup to the parsed document for the snippet's body.
  const run = async function () {{
    const __Window__ = window.self;
    with ({{ document: doc }}) {{
      return eval(js);
    }}
  }};
  const out = await run(); // snippets are sync OR async IIFEs: await both
  return (typeof out === 'string' && out.startsWith('{{"error"')) ? out
    : JSON.stringify({{ payload: out, ms: Date.now() - t0 }});
}})()
"#,
        url_json = serde_json::to_string(url)?,
        origin_json = serde_json::to_string(&format!("{}/", cfg.source.base_url.trim_end_matches('/')))?,
        js_json = serde_json::to_string(js)?,
    );
    match runner_fetch_eval(cfg, &wrapped).await {
        Ok(v) if v.as_str().is_some_and(|s| s.contains("{\"error\"")) => {
            // Same-origin fetch refused (proxy rules etc): real navigation.
            page_eval(cfg, url, js).await
        }
        Ok(v) => {
            let s = v.as_str().unwrap_or("");
            if let Ok(inner) = serde_json::from_str::<Value>(s) {
                if let Some(payload) = inner.get("payload") {
                    // Empty payload (no stream on the fetched HTML, e.g. a CF
                    // challenge instead of the episode page): retry once with
                    // a real navigation before reporting "no video".
                    let empty = payload.is_null()
                        || payload.as_str().is_some_and(|st| st.is_empty() || st == "[]");
                    if empty {
                        if let Ok(live) = page_eval(cfg, url, js).await {
                            let live_empty = live.is_null()
                                || live.as_str().is_some_and(|st| st.is_empty() || st == "[]");
                            if !live_empty { return Ok(live); }
                        }
                    }
                    return Ok(payload.clone());
                }
            }
            Ok(v)
        }
        Err(e) => page_eval(cfg, url, js).await.map_err(|_| e),
    }
}

/// One reused runner parked on the origin, running the wrapped snippet.
async fn runner_fetch_eval(cfg: &Config, wrapped: &str) -> Result<Value> {
    let mut runner = PageRunner::start(cfg).await?;
    let out = runner.eval(&format!("{}/sidebar.html", cfg.source.base_url.trim_end_matches('/')), wrapped).await;
    runner.close().await;
    out
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
fn az_url(cfg: &Config, page: u32) -> String {
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

#[derive(Serialize, Deserialize, Clone, Default)]
struct Episode { num: u32, url: String }

/// One related `/play/...` entry from the "Correlati" widget: on AnimeWorld
/// every season/sequel/prequel/movie is its own page, so the season menu is
/// just this list plus the current page on top (marked `cur`).
#[derive(Serialize, Deserialize, Clone, Default)]
struct Season { title: String, url: String, cur: bool }

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

// ------------------------------------------------------- obscura process

/// Locate the `obscura` binary.
fn find_obscura() -> Option<PathBuf> {
    let name = if cfg!(windows) { "obscura.exe" } else { "obscura" };
    if let Some(p) = env::var_os("OBSCURA_BIN") {
        let p = PathBuf::from(p);
        if p.is_file() { return Some(p); }
    }
    if let Some(paths) = env::var_os("PATH") {
        let dirs = env::split_paths(&paths)
            .chain(dirs::home_dir().map(|h| h.join(".local/bin")).into_iter());
        if let Some(p) = dirs.map(|d| d.join(name)).find(|c| c.is_file()) {
            return Some(p);
        }
    }
    None
}

fn endpoint_host_port(ep: &str) -> (String, u16) {
    let s = ep.split("//").nth(1).unwrap_or(ep).split('/').next().unwrap_or(ep);
    match s.rsplit_once(':') {
        Some((h, p)) => (h.trim().to_string(), p.trim().parse().unwrap_or(9222)),
        None => (s.to_string(), 9222),
    }
}

fn is_loopback(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "::1" | "localhost")
}

/// A real CDP handshake is the only reliable "ready" probe.
async fn cdp_alive(cfg: &Config) -> bool {
    let checked = Cdp::connect(&cfg.scrapper.obscura_endpoint);
    match tokio::time::timeout(Duration::from_secs(6), checked).await {
        Ok(Ok(mut cdp)) => cdp.call("Target.getTargets", json!({}), None).await
            .map(|v| v["targetInfos"].is_array())
            .unwrap_or(false),
        _ => false,
    }
}

async fn wait_cdp_alive(cfg: &Config, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while !cdp_alive(cfg).await {
        if Instant::now() >= deadline { return false; }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    true
}

fn stdout_bar(stderr_paths: [Option<PathBuf>; 1]) -> Stdio {
    stderr_paths.into_iter()
        .flatten()
        .find_map(|p| OpenOptions::new().create(true).append(true).open(p).ok())
        .map(Stdio::from)
        .unwrap_or(Stdio::null())
}

/// Make sure an Obscura CDP server is reachable, starting the binary if needed.
async fn ensure_obscura(cfg: &Config) -> Result<ObscuraServer> {
    let (host, port) = endpoint_host_port(&cfg.scrapper.obscura_endpoint);
    if !is_loopback(&host) {
        eprintln!("endpoint is remote — start obscura on that host yourself");
        bail!("remote CDP endpoint not managed here");
    }
    if cdp_alive(cfg).await {
        return Ok(ObscuraServer { spawned_pid: None });
    }
    let Some(bin) = find_obscura() else {
        eprintln!("obscura binary not found — install it or set OBSCURA_BIN");
        bail!("obscura missing");
    };
    eprintln!("obscura not running — starting {} …", bin.display());
    let stderr = app_dir().map(|d| d.join("obscura.log"));
    let mut cmd = process::Command::new(&bin);
    cmd.arg("serve").arg("--port").arg(port.to_string())
        .stdout(Stdio::null())
        .stderr(stdout_bar([stderr]))
        .stdin(Stdio::null());
    #[cfg(unix)]
    cmd.process_group(0);
    match cmd.spawn() {
        Err(e) => bail!("cannot spawn {}: {e}", bin.display()),
        Ok(mut child) => {
            let pid = child.id();
            if !wait_cdp_alive(cfg, Duration::from_secs(30)).await {
                let _ = child.start_kill();
                bail!("obscura did not reach CDP within 30s");
            }
            return Ok(ObscuraServer { spawned_pid: pid });
        }
    }
}

struct ObscuraServer { spawned_pid: Option<u32> }

impl ObscuraServer {
    /// Terminate a server this run spawned; a pre-existing one stays untouched.
    #[cfg(unix)]
    async fn shutdown(&self) {
        let Some(pid) = self.spawned_pid else { return };
        unsafe { libc::kill(-(pid as i32), libc::SIGTERM); }
        for _ in 0..20 {
            if unsafe { libc::kill(pid as i32, 0) } != 0 { break; } // gone: ESRCH
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if unsafe { libc::kill(pid as i32, 0) } == 0 {
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
                libc::kill(pid as i32, libc::SIGKILL);
            }
        }
    }

    #[cfg(not(unix))]
    async fn shutdown(&self) {}
}

// -------------------------------------------------------- playback paths

/// OrbStack rides a `mac` bridge to the host's Homebrew player.
fn player_command(url: &str) -> (String, Vec<String>) {
    if is_orbstack() {
        ("mac".into(), vec!["mpv".into(), url.into()])
    } else {
        ("mpv".into(), vec![url.into()])
    }
}

fn is_orbstack() -> bool {
    env::var_os("ORBSTACK_CONTAINER").is_some()
        || env::var_os("ORBSTACK_VM").is_some()
        || Path::new("/subsystem/name").exists()
        || Path::new("/opt/orbstack-guest").exists()
        || fs::read_to_string("/proc/sys/kernel/osrelease").is_ok_and(|s| s.contains("orbstack"))
}

async fn launch(url: &str) -> Result<()> {
    let (prog, args) = player_command(url);
    let hint = if prog == "mac" {
        " (OrbStack bridge: is mpv installed on the Mac host via brew?)"
    } else {
        " (is mpv installed? `sudo apt install mpv` / `brew install mpv`)"
    };
    match process::Command::new(&prog).args(&args)
        .stdout(Stdio::null()).stderr(Stdio::null()).spawn()
    {
        Ok(_) => Ok(()),
        Err(e) => bail!("player failed: cannot run `{prog}`: {e}{hint}"),
    }
}

// --------------------------------------------------------------- flows

/// Crawl the site catalogue via Obscura and cache it.
async fn update(cfg: &Config) -> Result<usize> {
    let server = ensure_obscura(cfg).await?;
    let outcome = match update_crawl(cfg).await {
        Ok(n) => Ok(n),
        Err(e) => {
            eprintln!("fast crawl failed ({e}); falling back to one-page-at-a-time navigation");
            update_crawl_nav(cfg).await
        }
    };
    server.shutdown().await;
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

/// One HTML validation harvest: every az-list page's server-rendered markup
/// already contains the whole listing, so the JS only needs the HTML — no
/// image subresource wait, no per-page navigation. Each page also ships the
/// site's published page count, read straight from the HTML text.
const HARVEST_JS: &str = r##"
(async () => {
  const pages = __PAGES__;
  const conc = __CONC__;
  const t0 = Date.now();
  const out = {};
  let i = 0;
  async function worker() {
    while (i < pages.length) {
      const p = pages[i++];
      try {
        const r = await fetch('/az-list?page=' + p);
        const html = await r.text();
        const doc = new DOMParser().parseFromString(html, 'text/html');
        const byHref = {};
        doc.querySelectorAll('a[href]').forEach(a => {
          const u = a.getAttribute('href') || '';
          if (u.indexOf('/play/') < 0) return;
          let t = (a.textContent || '').trim();
          if (!t) { const img = a.querySelector('img[alt]'); if (img) t = (img.getAttribute('alt') || '').trim(); }
          if (!t) return;
          if (t.length >= 120) t = t.slice(0, 119);
          const abs = u.indexOf('http') === 0 ? u : new URL(u, location.origin).href;
          if (!byHref[abs] || t) byHref[abs] = t;
        });
        const m = html.match(/paginationMaxPage\s*=\s*parseInt\("(\d+)"/);
        const items = Object.keys(byHref).filter(u => byHref[u]).map(u => [u, byHref[u]]);
        out[p] = { status: r.status, max: m ? Number(m[1]) : 0, items: items };
      } catch (e) {
        out[p] = { status: 0, max: 0, items: [], err: String(e && e.message || e) };
      }
    }
  }
  await Promise.all(Array.from({ length: Math.max(1, Math.min(conc, pages.length)) }, worker));
  return JSON.stringify({ ms: Date.now() - t0, pages: out });
})()
"##;

/// One parsed/validated harvest page (settings from HARVEST_JS answer).
#[derive(serde::Deserialize)]
struct FetchedPage { status: u32, #[serde(default)] max: u32, #[serde(default)] items: Vec<Vec<String>>, #[serde(default)] _err: Option<String> }

#[derive(serde::Deserialize)]
struct FetchedBatch { #[serde(default)] ms: u64, #[serde(default)] pages: HashMap<String, FetchedPage> }

/// Episodes for one series: cache first, else a fetch() harvest, storing the
/// result. Returns Err only when fetching fails and there is no cache.
async fn episodes_for(cfg: &Config, cache: &mut EpisodeCache, series_url: &str) -> Result<Vec<Episode>> {
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
async fn seasons_for(cfg: &Config, cache: &mut EpisodeCache, anime: &Entry) -> Result<Vec<Season>> {
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
    let c = Catalog { fetched_at: now_secs(), entries };
    let n = c.entries.len();
    save_catalog(&c)?;
    Ok(n)
}


/// One search against the local catalogue: match words in any order.
fn match_catalogue<'a>(entries: &'a [Entry], q: &str) -> Vec<&'a Entry> {
    let words: Vec<String> = q.split_whitespace().map(str::to_lowercase).collect();
    if words.is_empty() { return vec![]; }
    entries.iter()
        .filter(|e| words.iter().all(|w| e.t.to_lowercase().contains(w)))
        .collect()
}

/// Search + pick a title; returns the picked entry, if any.
async fn flow_pick_anime(cfg: &Config, arg_query: Option<String>, entries: &[Entry]) -> Result<Option<Entry>> {
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
    let _ = cfg;
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

async fn flow_pick_episode(eps: &[Episode]) -> Result<Option<String>> {
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
async fn pick_episode_from<'e>(eps: Vec<&'e Episode>) -> Result<Option<String>> {
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
async fn flow_pick_season(seasons: &[Season]) -> Result<Option<Season>> {
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
    let cfg = load_config().context("loading config")?;

    // Debug probe: send STREAM_JS at one episode URL through both paths and
    // print exactly what Obscura answers (payload or full exception text).
    if let Some(i) = args.iter().position(|a| a == "--probe-stream") {
        let url = args.get(i + 1).cloned().unwrap_or_default();
        if url.is_empty() { bail!("usage: ohmyanime --probe-stream <episode-url>"); }
        let server = ensure_obscura(&cfg).await?;
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
        server.shutdown().await;
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
    let picked = flow_pick_anime(&cfg, arg_query, &cat.entries).await?;

    let Some(mut anime) = picked else {
        if env::var_os("OHMYANIME_DEBUG").is_some() { eprintln!("no series picked — exiting quietly"); }
        return Ok(()); // user bailed: exit 0, silent
    };

    // From here only, bring up the browser. The server guard is dropped after
    // launch: a spawned obscura is torn down then, a running one is left alone.
    let server = ensure_obscura(&cfg).await?;

    // Watch/replay loop (ani-cli behaviour):
    //   series -> season (skipped when alone) -> block/episode -> mpv,
    // then back to the episode menu. Esc in the episode menu goes back to the
    // season menu, Esc there back to the series menu. `server` keeps obscura
    // alive between menus; it is torn down only when the user leaves entirely.
    let mut epcache = load_episode_cache();
    'series: loop {
        eprintln!("loading seasons for {} …", anime.t);
        let seasons = match seasons_for(&cfg, &mut epcache, &anime).await {
            Ok(s) => s,
            Err(e) => { server.shutdown().await; return Err(e); }
        };
        if seasons.is_empty() { bail!("no seasons found for {}", anime.t); }
        if seasons.len() > 1 {
            eprintln!("found {} related entries", seasons.len());
        }

        let Some(season) = flow_pick_season(&seasons).await? else {
            // Esc in the season menu: back to the series menu.
            let picked = flow_pick_anime(&cfg, None, &cat.entries).await?;
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
            if eps.is_empty() { bail!("no episodes found for {label}"); }

            let Some(ep_url) = flow_pick_episode(&eps).await? else {
                // Esc in the episode menu: back to the season menu when there
                // is one, else back to the series menu (reloading the same
                // episode list forever would be an infinite loop).
                if seasons.len() > 1 { continue 'series; }
                let picked = flow_pick_anime(&cfg, None, &cat.entries).await?;
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
            if let Err(e) = launch(&stream).await {
                // keep the menu: a missing player must not kill the whole session
                eprintln!("{e} - returning to the episode menu");
            }
        }
    }
    server.shutdown().await;
    Ok(())
}

// ---------------------------------------------------------------- tests

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

    #[test]
    fn queries_are_percent_encoded_and_joined() {
        assert_eq!(urlencode("a&b=c"), "a%26b%3Dc");
        let cfg = Config::default();
        assert_eq!(search_url(&cfg, "one punch man"), "https://www.animeworld.ac/search?keyword=one%20punch%20man");
        assert_eq!(az_url(&cfg, 2), "https://www.animeworld.ac/az-list?page=2");
    }

    #[test]
    fn endpoints_parse_into_host_and_port() {
        for ep in ["ws://127.0.0.1:9222", "127.0.0.1:9222", "ws://127.0.0.1:9222/devtools/browser"] {
            assert_eq!(endpoint_host_port(ep), ("127.0.0.1".to_string(), 9222), "{ep}");
        }
        assert!(is_loopback("127.0.0.1") && is_loopback("localhost") && !is_loopback("10.0.0.1"));
    }

    #[test]
    fn full_pick_line_keeps_the_url() {
        // Regression: pick() must return the whole "title\turl" line; the
        // old code truncated at the tab and every downstream picked_url()
        // returned None, silently killing the season/episode menus.
        let line = Some("Lain (ITA)\thttps://x/play/lain.1".to_string());
        assert_eq!(picked_url(&line).as_deref(), Some("https://x/play/lain.1"));
    }

    #[test]
    fn js_snippets_hold_no_double_escaped_sequences() {
        // Regression: the *_JS consts are Rust raw strings, so what you read
        // is what the browser evals. The broken shape was backslash-backslash
        // +slash inside a regex literal (it closes the regex early and the
        // rest parses as flags: "Invalid regular expression flags").
        // Byte-explicit checks: [92,92,47] = two backslashes + slash.
        let triple_slash: Vec<u8> = vec![92, 92, 92, 47];
        let quad_bs: Vec<u8> = vec![92, 92, 92, 92];
        for (name, js) in [("EPISODES_JS", EPISODES_JS), ("SEASONS_JS", SEASONS_JS), ("STREAM_JS", STREAM_JS), ("AZ_PAGE_JS", AZ_PAGE_JS)] {
            let b = js.as_bytes();
            assert!(b.windows(triple_slash.len()).all(|w| w != triple_slash.as_slice()), "{name} holds a triple-escaped slash");
            assert!(b.windows(quad_bs.len()).all(|w| w != quad_bs.as_slice()), "{name} holds a quad-escaped backslash");
        }
        // STREAM_JS must resolve through parameters, never page-relative fetch.
        assert!(STREAM_JS.contains("(doc, origin)"), "STREAM_JS must take (doc, origin)");
        assert!(STREAM_JS.contains("r.ok"), "STREAM_JS must check the API response before .json()");
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

    #[test]
    fn player_uses_mac_bridge_inside_orbstack() {
        // Outside OrbStack: plain mpv.
        let (prog, args) = player_command("https://x/stream.m3u8");
        if !is_orbstack() {
            assert_eq!(prog, "mpv");
            assert_eq!(args, vec!["https://x/stream.m3u8".to_string()]);
        } else {
            assert_eq!(prog, "mac");
            assert_eq!(args, vec!["mpv".to_string(), "https://x/stream.m3u8".to_string()]);
        }
    }

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
