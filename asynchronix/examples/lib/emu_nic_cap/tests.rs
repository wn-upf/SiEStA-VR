use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use tai_time::TaiTime;

use crate::lib::models_mm1k::{BandwidthOutcome, EmulatedLink, NetworkPattern, NetworkPatternEmulator};

const MTU_BITS: f64 = 12_000.0;

fn at(s: f64) -> TaiTime<0> {
    TaiTime::<0>::EPOCH.checked_add(Duration::from_secs_f64(s)).unwrap()
}

/// The emulated link's patterns with only the 1 Gb/s NIC cap (no test pattern), copied into a fresh emulator.
fn nic_cap_emulator(extra: Option<NetworkPattern>) -> NetworkPatternEmulator {
    let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let link = EmulatedLink::new_with_bandwidth(100, at(0.0), None, ip, Some(1_000_000_000), 90.0, None);
    let mut emu = NetworkPatternEmulator::new(ip);
    for p in link.get_network_patterns() {
        if let NetworkPattern::Bandwidth { max_latency, .. } = p {
            assert_eq!(*max_latency, Duration::MAX, "the NIC cap must have no drop limit");
        }
        emu.add_pattern(p.clone());
    }
    if let Some(p) = extra {
        emu.add_pattern(p);
    }
    emu
}

/// A burst made eligible at once after 20 ms of jitter: the NIC cap's two-MTU bucket runs short after two packets, and
/// the third must wait for credit (pacing), not be dropped against the NIC's 12 ms netem limit.
#[test]
fn nic_cap_paces_a_jittered_burst_without_dropping() {
    let mut emu = nic_cap_emulator(None);
    let now = at(1.0);
    let waited = Duration::from_millis(20);
    assert!(matches!(emu.try_send_bandwidth(now, MTU_BITS, waited), BandwidthOutcome::Sent));
    assert!(matches!(emu.try_send_bandwidth(now, MTU_BITS, waited), BandwidthOutcome::Sent));
    match emu.try_send_bandwidth(now, MTU_BITS, waited) {
        BandwidthOutcome::Wait(d) => assert!(d <= Duration::from_micros(13), "waits one MTU at 1 Gb/s, got {d:?}"),
        other => panic!("expected Wait, got {}", outcome_name(&other)),
    }
}

/// Under a slower cap (100 Mb/s, netem limit 1000 packets = 120 ms) the drop threshold is that cap's, also when the NIC
/// bucket is short at the same moment.
#[test]
fn slower_cap_sets_the_drop_threshold() {
    let cap = NetworkPattern::new_bandwidth(100e6, 100e6, at(0.0), at(90.0));
    let mut emu = nic_cap_emulator(Some(cap));
    let now = at(1.0); // the 100 Mb/s bucket has refilled for 1 s (capped at its burst window), the NIC's to two MTUs
    let mut sent = 0;
    while matches!(emu.try_send_bandwidth(now, MTU_BITS, Duration::from_millis(50)), BandwidthOutcome::Sent) {
        sent += 1;
    }
    assert_eq!(sent, 2, "the NIC bucket holds two MTUs");
    // NIC short, 100 Mb/s not: 50 ms in the queue is under 120 ms.
    assert!(matches!(emu.try_send_bandwidth(now, MTU_BITS, Duration::from_millis(50)), BandwidthOutcome::Wait(_)));
    // Past the 100 Mb/s cap's 120 ms only once that cap is short too: drain its credit first.
    let mut t = now;
    loop {
        t = t.checked_add(Duration::from_micros(24)).unwrap(); // refills the NIC bucket
        match emu.try_send_bandwidth(t, MTU_BITS, Duration::from_millis(50)) {
            BandwidthOutcome::Sent => continue,
            BandwidthOutcome::Wait(_) => break,
            BandwidthOutcome::ExceedsLimit => panic!("dropped below the 120 ms limit"),
        }
    }
    assert!(matches!(emu.try_send_bandwidth(t, MTU_BITS, Duration::from_millis(130)), BandwidthOutcome::ExceedsLimit));
}

fn outcome_name(o: &BandwidthOutcome) -> &'static str {
    match o {
        BandwidthOutcome::Sent => "Sent",
        BandwidthOutcome::Wait(_) => "Wait",
        BandwidthOutcome::ExceedsLimit => "ExceedsLimit",
    }
}
