// Obscura over CDP: minimal websocket client plus two page runners.
// - `PageRunner`: one websocket + one reusable browser target for crawls.
// - `page_eval`: one-shot fresh target with full load wait (episodes, stream).
// - `fetch_page_js`: runs a snippet against *fetched* HTML (no navigation),
//   falling back to `page_eval` when the fetch path refuses or comes back empty.

use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{collections::HashMap, time::{Duration, Instant}};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use crate::config::Config;

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(crate) struct Cdp {
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
pub(crate) struct PageRunner {
    cdp: Cdp,
    tsid: String,
}

impl PageRunner {
    /// Fresh connection + target, to be reused across many page_eval calls.
    pub(crate) async fn start(cfg: &Config) -> Result<Self> {
        let mut cdp = Cdp::connect(&cfg.scrapper.obscura_endpoint).await?;
        let tid = cdp.call("Target.createTarget", json!({"url": "about:blank"}), None)
            .await?["targetId"].as_str().context("missing targetId")?.to_string();
        let s = cdp.call("Target.attachToTarget", json!({"targetId": tid, "flatten": true}), None)
            .await?["sessionId"].as_str().context("missing sessionId")?.to_string();
        cdp.call("Page.enable", json!({}), Some(&s)).await?;
        Ok(PageRunner { cdp, tsid: format!("{tid}|{s}") })
    }

    /// One page via the reused target: navigate, short load grace, poll JS.
    pub(crate) async fn eval(&mut self, url: &str, js: &str) -> Result<Value> {
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

    pub(crate) async fn close(&mut self) {
        let (tid, _) = self.tsid.split_once('|').unwrap_or(("", ""));
        let _ = self.cdp.call("Target.closeTarget", json!({"targetId": tid}), None).await;
    }
}

/// One-shot page eval (episodes, stream): fresh target, full load wait.
pub(crate) async fn page_eval(cfg: &Config, url: &str, js: &str) -> Result<Value> {
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

/// Run `js` against a *fetched* page without ever navigating to it: the page's
/// HTML is fetched from a cheap scratch page on the same origin and parsed with
/// DOMParser, so the heavy subresource wait never happens. Anchors parsed this
/// way are rewired to absolute URLs (the JS snippets use `a.href`, which on a
/// DOMParser document is just the raw attribute); a scratch `base` element
/// makes `a.href` resolve correctly, so the snippets need no special casing.
///
/// Falls back to `page_eval` (a real navigation) when the same-origin fetch is
/// not possible — the snippets there run against the live DOM unchanged.
pub(crate) async fn fetch_page_js(cfg: &Config, url: &str, js: &str) -> Result<Value> {
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

/// A real CDP handshake is the only reliable "ready" probe.
pub(crate) async fn cdp_alive(cfg: &Config) -> bool {
    let checked = Cdp::connect(&cfg.scrapper.obscura_endpoint);
    match tokio::time::timeout(Duration::from_secs(6), checked).await {
        Ok(Ok(mut cdp)) => cdp.call("Target.getTargets", json!({}), None).await
            .map(|v| v["targetInfos"].is_array())
            .unwrap_or(false),
        _ => false,
    }
}

pub(crate) async fn wait_cdp_alive(cfg: &Config, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while !cdp_alive(cfg).await {
        if Instant::now() >= deadline { return false; }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    true
}
