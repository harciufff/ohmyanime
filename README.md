# ohmyanime

Search and play anime from your terminal: pick a title with `fzf`,
`ohmyanime` resolves episodes through a headless browser and plays them in `mpv`.

```text
ohmyanime --update        # build the local title catalogue (one time)
ohmyanime                 # pick series -> season -> episode -> mpv
ohmyanime "one piece"     # skip straight to matching titles
```

Flow: pick series (`fzf`) → season (skipped when alone) → episode block
(series over 50 episodes) → episode → `mpv`. `Esc` goes back one menu,
`Ctrl-C` exits and tears down everything this run started.

## Why not `ani-cli`?

- **Headless browser scraping.** Instead of `curl` + `grep`, `ohmyanime`
  drives [Obscura](https://github.com/h4ckf0r0day/obscura) over CDP, so it
  survives XHR-rendered lists and anti-bot pages where plain HTTP scraping dies.
- **Instant offline search.** Titles live in a local catalogue: searching needs
  no browser and answers immediately. The browser starts lazily, only when
  episode/stream data is needed.
- **Works where `ani-cli` doesn't.** Native on macOS, Linux and WSL2, plus
  OrbStack containers via the `mac mpv` bridge to the Mac host's player.

## Requirements

| What | Why | Install |
|---|---|---|
| Rust + Cargo | build `ohmyanime` | [rustup.rs](https://rustup.rs) |
| `obscura` binary | headless browser (CDP server) | see below |
| `fzf` | interactive menus | `brew install fzf` / `sudo apt install fzf` |
| `mpv` | playback | `brew install mpv` / `sudo apt install mpv` |

### Obscura binary

Download it from the
[Obscura releases page](https://github.com/h4ckf0r0day/obscura/releases):

1. Pick the release matching **your OS and architecture**.
2. Choose the build tagged **`-stealth`** and **without `-no-render`** —
   that is the most capable build (full rendering + stealth).
3. Put the binary on your `PATH` (e.g. `~/.local/bin`), or point to it with
   the `OBSCURA_BIN` environment variable.

`ohmyanime` starts `obscura serve` automatically when needed and shuts down
only the server it started itself: a pre-existing Obscura is left running.

### OrbStack note

Inside an OrbStack container there is no local player, so `ohmyanime` runs
`mac mpv` — the OrbStack bridge to the Mac host. That means **`mpv` must be
installed on the Mac host via Homebrew** (`brew install mpv`), not inside the
container. `fzf` and `obscura` run inside the container as usual.

## Install

No prebuilt binaries are published (too many OS/arch combinations to get
wrong). Build from source — it takes under a minute:

```bash
git clone <repo-url> ohmyanime && cd ohmyanime
cargo build --release
# then put it on your PATH, e.g.:
cp target/release/ohmyanime ~/.local/bin/
```

Update later with `git pull && cargo build --release`.

## Quickstart

```bash
ohmyanime --update          # crawl the title catalogue (a few minutes, once)
ohmyanime                   # fuzzy-pick a series, season, episode; mpv plays it
ohmyanime "solo leveling"   # start from matching titles directly
ohmyanime --update --episodes   # also drop the cached episode/season indexes
```

## Usage

```text
ohmyanime [query] [--update [--episodes]]
```

- `query` — words matched against catalogue titles in any order
  (`ohmyanime "one piece"`). Omit it for the full interactive list.
- `--update` — recrawl the title catalogue and save it locally.
- `--update --episodes` — also clear the episode/season cache
  (titles are stable; episode lists change as new episodes air).
- `--probe-stream <episode-url>` — debug helper: resolves a stream through
  both fetch paths and prints exactly what Obscura answers.
- `-h, --help` — this help.

While watching, return to the terminal: the episode menu is still there, so
you can queue the next episode. Launching a new episode stops the previous
player (single playback, no stacked audio).

## Configuration

Everything lives under `~/.config/ohmyanime/`:

| File | Content |
|---|---|
| `config.toml` | source site (`base_url`) and scrapper endpoint (`obscura_endpoint`, default `ws://127.0.0.1:9222`). Auto-seeded on first run — edit freely. If it ever fails to parse after an upgrade, delete it and let it re-seed. |
| `catalog.json` | title index built by `--update`. Missing or empty → run `--update`. |
| `episodes.json` | cached episode/season indexes. Safe to delete any time (re-fetched on demand, or cleared via `--update --episodes`). |
| `menu.log` | append-only trace of every `fzf` menu (rows, prompts, exit codes). First place to look when a menu misbehaves. |
| `obscura.log` | `stderr` of the Obscura server when started by `ohmyanime`. |

Upgrading from an older version that used `~/.config/ohMyAnime` (mixed case)?
The new location is `~/.config/ohmyanime`, and the old directory is moved
there automatically on first run — catalogues and config survive.

Environment knobs:

| Variable | Effect |
|---|---|
| `OBSCURA_BIN` | explicit path to the `obscura` binary |
| `OHMYANIME_FETCH_CONC` | concurrent in-page fetches per crawl batch (default `12`, clamped `1–32`) |
| `OHMYANIME_MENU_PATH` | dump the current menu rows to this file (menu debugging) |
| `OHMYANIME_DEBUG` | extra exit chatter |

## Troubleshooting

| Symptom | Fix |
|---|---|
| `missing catalog.json, run ohmyanime --update` | run `ohmyanime --update` first |
| Catalogue empty / no titles | site may be down or changed — retry `--update`, check `obscura.log` |
| `obscura binary not found` | install it (see above) or set `OBSCURA_BIN` |
| `cannot reach Obscura CDP` | kill stale instances, check nothing else uses port `9222` |
| Menu opens and closes instantly | check `menu.log`; then `echo $TERM; echo "$FZF_DEFAULT_OPTS"; fzf --version` (custom `FZF_DEFAULT_OPTS` are ignored by design) |
| `player failed: cannot run mpv` | install `mpv`; on OrbStack install it on the Mac host via brew |
| Stream fails for one episode | pick another; or diagnose with `ohmyanime --probe-stream <episode-url>` |
| Obscura left running after `kill -9` | stop it manually (`pkill obscura`) — graceful exit and `Ctrl-C` always clean up |

## How it works

1. `--update` crawls the site's `/az-list` pages in parallel batches through
   one reused browser target (plus a one-at-a-time fallback) and saves
   `catalog.json`.
2. Searching filters that file in memory — no browser involved.
3. Picking a series fetches its episode list and related-seasons widget
   (cached in `episodes.json` afterwards).
4. Picking an episode resolves the stream URL (site API with CSRF, falling
   back to `<video>` tags and `m3u8`/`mp4` patterns) and launches `mpv`.
5. On exit, error, or `Ctrl-C`, everything this run started (spawned Obscura,
   `mpv` children) is torn down; a pre-existing Obscura is untouched.

## Limitations

- Single source: AnimeWorld (`animeworld.ac`). If the site changes its markup,
  crawls can break until the extraction snippets are updated.
- Streaming availability depends on the site and your network; a VPN may be
  required in some regions.

## Roadmap

- Per-site profiles (TOML) so new sources need no recompile.
- `doctor` command: validate each stage (list → episodes → seasons → stream)
  against the live site and report exactly what broke.
- More sources (movies / TV series).

## Acknowledgments

- [Obscura](https://github.com/h4ckf0r0day/obscura) — the headless browser
  this tool is built on.
- [`fzf`](https://github.com/junegunn/fzf) and [`mpv`](https://mpv.io/) —
  the picker and the player that make the whole flow work.

## License

MIT — see [LICENSE](LICENSE). For personal and educational use; respect the
terms of the sites you browse.
