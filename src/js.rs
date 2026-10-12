// Extraction snippets evaluated inside the browser (see cdp.rs).
// Rust raw strings: what you read is what the browser evals — no extra
// escaping layer, keep regexes byte-clean (see the test below).

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
pub(crate) const AZ_PAGE_JS: &str = r##"
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
pub(crate) const EPISODES_JS: &str = r##"
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
pub(crate) const SEASONS_JS: &str = r##"
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
pub(crate) const STREAM_JS: &str = r##"
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

/// One HTML validation harvest: every az-list page's server-rendered markup
/// already contains the whole listing, so the JS only needs the HTML — no
/// image subresource wait, no per-page navigation. Each page also ships the
/// site's published page count, read straight from the HTML text.
pub(crate) const HARVEST_JS: &str = r##"
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
