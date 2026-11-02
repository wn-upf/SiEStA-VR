//! The emulated link's flat NIC-speed cap (`EmulatedLink::new_with_bandwidth`, 1 Gb/s) paces packets but never drops them.
//!
//! Since 0989336 (2026-07-30) the NIC cap's token bucket holds two MTUs (`NIC_CAP_BURST_MS`), so it runs short whenever a
//! burst becomes eligible at once. `try_send_bandwidth` then folded the NIC cap's own netem limit (1000 packets at 1 Gb/s =
//! 12 ms) into the drop threshold and dropped every packet that had spent more than 12 ms in the emulator, the jitter
//! delay or the wait behind a slower bandwidth pattern included. In the validation's jitter test (10 +- 10 ms) that dropped
//! nearly every packet, so no frame completed; under a 100 Mb/s cap it dropped part of the backlog the cap's own 120 ms
//! limit would have kept. Before 0989336 the NIC bucket (333 ms of credit) never ran short, so it never set the threshold,
//! as `try_send_bandwidth`'s comment intends. The cap now keeps its pacing and has no limit of its own: a slower pattern's
//! limit, or none, decides drops.
//!
//! `XRSIM_NIC_CAP_LIMIT_LEGACY=1` restores the 12 ms limit (the paper build's emulated-link results).

use std::time::Duration;

use once_cell::sync::Lazy;

use crate::lib::models_mm1k::NetworkPattern;

static LEGACY: Lazy<bool> = Lazy::new(|| match std::env::var("XRSIM_NIC_CAP_LIMIT_LEGACY").as_deref() {
    Ok("1") => true,
    Ok("0") | Ok("") | Err(_) => false,
    Ok(v) => panic!("XRSIM_NIC_CAP_LIMIT_LEGACY={v}: 0 (default) or 1"),
});

/// Removes the NIC cap's drop limit (unless `XRSIM_NIC_CAP_LIMIT_LEGACY=1`).
pub fn make_pacing_only(pattern: &mut NetworkPattern) {
    if *LEGACY {
        return;
    }
    if let NetworkPattern::Bandwidth { max_latency, .. } = pattern {
        *max_latency = Duration::MAX;
    }
}

#[cfg(test)]
mod tests;
