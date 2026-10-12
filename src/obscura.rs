// Obscura process lifecycle + the AppGuard that owns what this run started.
// Lazy by design: searching the local catalogue needs no browser; Obscura is
// ensured only once playback data is needed (seasons/episodes/stream).
// A pre-existing Obscura is borrowed and left running on exit — only a server
// this run spawned is torn down (graceful `shutdown()`, sync `Drop` backstop,
// `kill_tracked_sync()` for the signal path where `process::exit` skips Drop).

use anyhow::{bail, Result};
use std::{
    env,
    fs::OpenOptions,
    path::PathBuf,
    process::Stdio,
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};
use tokio::process;

use crate::{
    cdp::{cdp_alive, wait_cdp_alive},
    config::{app_dir, Config},
    player::player_command,
};

/// Locate the `obscura` binary.
pub(crate) fn find_obscura() -> Option<PathBuf> {
    let name = if cfg!(windows) { "obscura.exe" } else { "obscura" };
    if let Some(p) = env::var_os("OBSCURA_BIN") {
        let p = PathBuf::from(p);
        if p.is_file() { return Some(p); }
    }
    if let Some(paths) = env::var_os("PATH") {
        let dirs = env::split_paths(&paths)
            .chain(dirs::home_dir().map(|h| h.join(".local/bin")));
        if let Some(p) = dirs.map(|d| d.join(name)).find(|c| c.is_file()) {
            return Some(p);
        }
    }
    None
}

pub(crate) fn endpoint_host_port(ep: &str) -> (String, u16) {
    let s = ep.split("//").nth(1).unwrap_or(ep).split('/').next().unwrap_or(ep);
    match s.rsplit_once(':') {
        Some((h, p)) => (h.trim().to_string(), p.trim().parse().unwrap_or(9222)),
        None => (s.to_string(), 9222),
    }
}

fn is_loopback(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "::1" | "localhost")
}

fn stdout_bar(stderr_paths: [Option<PathBuf>; 1]) -> Stdio {
    stderr_paths.into_iter()
        .flatten()
        .find_map(|p| OpenOptions::new().create(true).append(true).open(p).ok())
        .map(Stdio::from)
        .unwrap_or(Stdio::null())
}

/// Make sure an Obscura CDP server is reachable, starting the binary if needed.
pub(crate) async fn ensure_obscura(cfg: &Config) -> Result<AppGuard> {
    let (host, port) = endpoint_host_port(&cfg.scrapper.obscura_endpoint);
    if !is_loopback(&host) {
        eprintln!("endpoint is remote — start obscura on that host yourself");
        bail!("remote CDP endpoint not managed here");
    }
    if cdp_alive(cfg).await {
        // Borrowed: a pre-existing Obscura is left running on exit.
        SPAWNED_PID.store(0, Ordering::SeqCst);
        return Ok(AppGuard { obscura_pid: None, mpv_kids: Vec::new() });
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
            // Track for the signal path (process::exit skips Drop).
            SPAWNED_PID.store(pid.unwrap_or(0), Ordering::SeqCst);
            Ok(AppGuard { obscura_pid: pid, mpv_kids: Vec::new() })
        }
    }
}

/// Pids this run spawned, for the signal path in main(): `process::exit`
/// runs no destructors, so the AppGuard Drop never fires there — the signal
/// branch kills these directly via `kill_tracked_sync()`. A borrowed
/// (pre-existing) Obscura never lands here and is left running.
static SPAWNED_PID: AtomicU32 = AtomicU32::new(0);
static MPV_PIDS: std::sync::Mutex<Vec<u32>> = std::sync::Mutex::new(Vec::new());

/// Sync backstop for Ctrl-C / SIGTERM: SIGTERM the spawned Obscura group,
/// brief grace, escalate to SIGKILL; SIGTERM tracked mpv kids. No-op when
/// Obscura was borrowed (pid 0). Unix only; other platforms rely on Drop.
#[cfg(unix)]
pub(crate) fn kill_tracked_sync() {
    let pid = SPAWNED_PID.swap(0, Ordering::SeqCst);
    if pid != 0 {
        unsafe { libc::kill(-(pid as i32), libc::SIGTERM); }
        std::thread::sleep(std::time::Duration::from_millis(300));
        if unsafe { libc::kill(pid as i32, 0) } == 0 {
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
                libc::kill(pid as i32, libc::SIGKILL);
            }
        }
    }
    let kids = MPV_PIDS.lock().map(|mut v| std::mem::take(&mut *v)).unwrap_or_default();
    let empty = kids.is_empty();
    for k in &kids {
        unsafe { libc::kill(*k as i32, libc::SIGTERM); }
    }
    if !empty {
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

/// Owns every child this run started: an Obscura server it spawned (a
/// pre-existing one is borrowed and left running) plus each mpv it launched.
/// `shutdown()` is the graceful path clean exits take; `Drop` is the sync
/// backstop for panic / early-return (Ctrl-C / SIGTERM go through
/// `kill_tracked_sync()` instead, since `process::exit` skips Drop).
pub(crate) struct AppGuard {
    obscura_pid: Option<u32>,
    mpv_kids: Vec<process::Child>,
}

impl AppGuard {
    /// Graceful teardown: `start_kill` every mpv child, then SIGTERM the
    /// spawned Obscura group, wait ~2s, escalate to SIGKILL. Idempotent
    /// (second call is a no-op) and a no-op when Obscura was borrowed.
    pub(crate) async fn shutdown(&mut self) {
        for kid in &mut self.mpv_kids {
            let _ = kid.start_kill();
        }
        self.mpv_kids.clear();
        if let Ok(mut v) = MPV_PIDS.lock() { v.clear(); }
        SPAWNED_PID.store(0, Ordering::SeqCst);
        #[cfg(unix)]
        if let Some(pid) = self.obscura_pid.take() {
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
        let _ = self.obscura_pid.take();
    }

    /// Single-playback launch: kill the previous mpv (no stacked audio),
    /// then start the new one and track it so exit / Ctrl-C can reap it.
    /// The stream URL is already resolved, so mpv outlives Obscura fine.
    pub(crate) async fn play(&mut self, url: &str) -> Result<()> {
        for kid in &mut self.mpv_kids {
            let _ = kid.start_kill();
        }
        self.mpv_kids.clear();
        let (prog, args) = player_command(url);
        let hint = if prog == "mac" {
            " (OrbStack bridge: is mpv installed on the Mac host via brew?)"
        } else {
            " (is mpv installed? `sudo apt install mpv` / `brew install mpv`)"
        };
        match process::Command::new(&prog).args(&args)
            .stdout(Stdio::null()).stderr(Stdio::null())
            .kill_on_drop(true).spawn()
        {
            Ok(kid) => {
                if let Some(id) = kid.id() {
                    if let Ok(mut v) = MPV_PIDS.lock() { v.push(id); }
                }
                self.mpv_kids.push(kid);
                Ok(())
            }
            Err(e) => bail!("player failed: cannot run `{prog}`: {e}{hint}"),
        }
    }
}

impl Drop for AppGuard {
    fn drop(&mut self) {
        for kid in &mut self.mpv_kids {
            let _ = kid.start_kill();
        }
        if let Ok(mut v) = MPV_PIDS.lock() { v.clear(); }
        #[cfg(unix)]
        if let Some(pid) = self.obscura_pid.take() {
            unsafe { libc::kill(-(pid as i32), libc::SIGTERM); }
            // Sync backstop only (no .await in Drop): brief grace, escalate.
            // Clean exits take shutdown() above and arrive here already clear.
            std::thread::sleep(std::time::Duration::from_millis(300));
            if unsafe { libc::kill(pid as i32, 0) } == 0 {
                unsafe {
                    libc::kill(-(pid as i32), libc::SIGKILL);
                    libc::kill(pid as i32, libc::SIGKILL);
                }
            }
            SPAWNED_PID.store(0, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_parse_into_host_and_port() {
        for ep in ["ws://127.0.0.1:9222", "127.0.0.1:9222", "ws://127.0.0.1:9222/devtools/browser"] {
            assert_eq!(endpoint_host_port(ep), ("127.0.0.1".to_string(), 9222), "{ep}");
        }
        assert!(is_loopback("127.0.0.1") && is_loopback("localhost") && !is_loopback("10.0.0.1"));
    }
}
