//! The client's frame deadline, overridable for runs that model ALVR's own client (as on main, `client_rules.rs`).
//!
//! By default (since b5a5522 / 29249dc, 2026-09) a frame not complete 60 ms (`DEADLINE_PACKETS_S`) after the server
//! generated it is lost: no statistics row, a loss report. ALVR's client (the paper's validation testbed) has no such
//! deadline: it reconstructs late frames and reports them. Under the validation's 100 Mb/s cap the emulator holds frames
//! ~120 ms, so with the default every one of them is lost and the client's RTT never shows the plateau.
//!
//! - `XRSIM_CLIENT_DEADLINE_MS=<ms>`: the deadline (default 60). A large value (e.g. 1000) behaves as ALVR's client.
//! - `XRSIM_CLIENT_DEADLINE_ANCHOR=first`: count it from the frame's first shard at the client instead of its generation
//!   (with 30 ms: the rule of the builds up to bcf1358, 2026-07-21, which made the published capacity panel).
//!
//! Unset, the output is byte-identical to the build before these switches.

use std::time::Duration;

use once_cell::sync::Lazy;

use crate::lib::alvr_stream_socket::DEADLINE_PACKETS_S;

static DEADLINE: Lazy<Duration> = Lazy::new(|| match std::env::var("XRSIM_CLIENT_DEADLINE_MS") {
    Err(_) => DEADLINE_PACKETS_S,
    Ok(v) => match v.trim().parse::<f64>() {
        Ok(ms) if ms > 0.0 && ms.is_finite() => Duration::from_secs_f64(ms / 1e3),
        _ => panic!("XRSIM_CLIENT_DEADLINE_MS={v}: a positive number of milliseconds"),
    },
});

static ANCHOR_FIRST: Lazy<bool> = Lazy::new(|| match std::env::var("XRSIM_CLIENT_DEADLINE_ANCHOR").as_deref() {
    Ok("first") => true,
    Ok("generation") | Ok("") | Err(_) => false,
    Ok(v) => panic!("XRSIM_CLIENT_DEADLINE_ANCHOR={v}: generation (default) or first"),
});

/// How long after its anchor the client gives up on a frame.
pub fn deadline() -> Duration {
    *DEADLINE
}

/// Whether the deadline counts from the frame's first shard at the client (else from its generation).
pub fn anchor_first() -> bool {
    *ANCHOR_FIRST
}
