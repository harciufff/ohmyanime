// Playback: mpv locally, or the OrbStack `mac` bridge to the Mac host's
// Homebrew mpv. Tracked by AppGuard (see obscura.rs), not fire-and-forget.

use std::{env, fs, path::Path};

/// OrbStack rides a `mac` bridge to the host's Homebrew player.
pub(crate) fn player_command(url: &str) -> (String, Vec<String>) {
    if is_orbstack() {
        ("mac".into(), vec!["mpv".into(), url.into()])
    } else {
        ("mpv".into(), vec![url.into()])
    }
}

pub(crate) fn is_orbstack() -> bool {
    env::var_os("ORBSTACK_CONTAINER").is_some()
        || env::var_os("ORBSTACK_VM").is_some()
        || Path::new("/subsystem/name").exists()
        || Path::new("/opt/orbstack-guest").exists()
        || fs::read_to_string("/proc/sys/kernel/osrelease").is_ok_and(|s| s.contains("orbstack"))
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
