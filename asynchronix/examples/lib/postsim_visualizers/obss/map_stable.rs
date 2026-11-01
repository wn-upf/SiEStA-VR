//! The STA map's fitted view (zoom and centre) is a property of the run and the map size, not of the playback cursor.

use super::*;
use std::net::Ipv4Addr;

const T_END: f32 = 60.0;

fn ip(k: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(10, 0, 0, k))
}

/// Station `k` at time `t`: a wander around its own spot; station 1 walks onto station 0 between 40 s and 46 s only.
fn walk(k: u8, t: f64) -> (f32, f32) {
    let (bx, by) = [(2.0, 1.0), (-6.0, 4.0), (5.0, -5.0)][k as usize];
    let ph = (t * 0.4) as f32 + k as f32;
    let (mut x, mut y) = (bx + 3.0 * ph.sin() * (1.0 + t as f32 / T_END), by + 2.0 * (1.7 * ph).cos());
    if k == 1 && (40.0..46.0).contains(&t) {
        let (x0, y0) = walk(0, t);
        x = x0 + 0.1;
        y = y0 - 0.1;
    }
    (x, y)
}

fn render(mb: &MultiBss, mv: &mut MapView, cursor_t: f64) {
    let (w, h) = (300usize, 360usize);
    let mut buf = vec![0u32; w * h];
    render_minimap(&mut buf, w, 0, w, 100, 210, mb, 0, cursor_t, cursor_t, None, MapHover::default(), mv);
}

fn assert_still(mb: &MultiBss) {
    let mut reference: Option<(f32, Option<(MapKey, f32)>, Option<(MapKey, bool)>)> = None;
    for step in 0..=60 {
        let mut mv = MapView::default();
        render(mb, &mut mv, step as f64);
        let now = (mv.m_per_px, mv.shift_run, mv.grouped_run);
        match &reference {
            None => reference = Some(now),
            Some(r) => assert_eq!(*r, now, "the fitted view changed at t = {} s", step),
        }
    }
}

#[test]
fn view_holds_still_with_positions_from_the_mac() {
    let evs: Vec<AbrEvent> = (0..3u8).flat_map(|k| (0..=60).map(move |s| {
        let (x, y) = walk(k, s as f64);
        AbrEvent::StaLocation { t: s as f64, ip_sta: ip(k), x, y, z: 0.0, is_bg: false }
    })).collect();
    let mut mb = MultiBss::single(VizIndex::build(Vec::new()), AbrVizIndex::build(evs), Vec::new());
    for k in 0..3u8 {
        mb.net_pos.insert(ip(k), (0..=60).map(|s| { let (x, y) = walk(k, s as f64); (s as f64, x, y) }).collect());
    }
    assert_still(&mb);
}

#[test]
fn view_holds_still_with_positions_from_the_reports() {
    let evs: Vec<AbrEvent> = (0..3u8).flat_map(|k| (0..=60).map(move |s| {
        let (x, y) = walk(k, s as f64);
        AbrEvent::StaLocation { t: s as f64, ip_sta: ip(k), x, y, z: 0.0, is_bg: false }
    })).collect();
    let mb = MultiBss::single(VizIndex::build(Vec::new()), AbrVizIndex::build(evs), Vec::new());
    assert_still(&mb);
}

/// Stations that the MAC moves are never boxed, however close they get; static ones at one spot are.
#[test]
fn only_static_stations_are_boxed() {
    let evs: Vec<AbrEvent> = (0..2u8).flat_map(|k| (0..=60).map(move |s| {
        let (x, y) = walk(k, s as f64);
        AbrEvent::StaLocation { t: s as f64, ip_sta: ip(k), x, y, z: 0.0, is_bg: false }
    })).collect();
    let mut mb = MultiBss::single(VizIndex::build(Vec::new()), AbrVizIndex::build(evs), Vec::new());
    let mut mv = MapView::default();
    // static: the headsets sit together for the whole run
    for k in 0..2u8 {
        mb.net_pos.insert(ip(k), vec![(0.0, 1.0, 1.0)]);
    }
    render(&mb, &mut mv, 10.0);
    assert_eq!(mv.grouped_run.map(|g| g.1), Some(true));
    // walking: the same two meet between 40 s and 46 s, and nothing is boxed
    let mut mv = MapView::default();
    for k in 0..2u8 {
        mb.net_pos.insert(ip(k), (0..=60).map(|s| { let (x, y) = walk(k, s as f64); (s as f64, x, y) }).collect());
    }
    render(&mb, &mut mv, 43.0);
    assert_eq!(mv.grouped_run.map(|g| g.1), Some(false));
    assert_eq!(mv.shift_run.map(|g| g.1), Some(0.0));
}
