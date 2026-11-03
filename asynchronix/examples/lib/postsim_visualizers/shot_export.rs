//! For scripts that make figures of the viewer (`scripts/viz_readme_figs.py`); nothing here runs unless asked.
//!
//! - `XRSIM_VIZ_EVENTS=<path>`: as the viewer opens, the run's TXOPs (with their parts and the holder's channel access, as
//!   the lanes draw them), collisions and the quick tour's examples, as JSON, so a script can pick instants to show.
//! - `XRSIM_VIZ_SHOT=<prefix>`: the first frame as `<prefix>.png`, and where it drew each part as `<prefix>.json`
//!   (logical pixels: the regions, each link lane, each ABR plot, and the channel view's time-to-x mapping), so a script
//!   can place markers on it. With `XRSIM_VIZ_EXIT=1` the viewer then closes.

use serde_json::{json, Value};

use super::tour::Rect;
use super::tour_examples::Example;
use super::{access_window, VizEvent, VizIndex};

/// Where the first frame drew its parts.
pub struct Layout {
    pub w: usize,
    pub h: usize,
    /// The channel plots' left edge and width (x of time t: `panel_x + (t - (center - span/2)) / span * panel_w`).
    pub panel_x: usize,
    pub panel_w: usize,
    /// Channel view (centre, span, cursor), s.
    pub view: (f64, f64, f64),
    /// (link, y, height) of each lane.
    pub lanes: Vec<(u8, usize, usize)>,
    pub regions: Vec<(&'static str, Option<Rect>)>,
    /// (title, rect) of each ABR plot shown.
    pub abr_strips: Vec<(String, Rect)>,
}

pub fn wanted() -> bool {
    std::env::var("XRSIM_VIZ_SHOT").is_ok_and(|v| !v.is_empty())
}

fn rect(r: Rect) -> Value {
    json!([r.0, r.1, r.2, r.3])
}

/// Saves `<prefix>.png` (the frame) and `<prefix>.json` (the layout).
pub fn save(buf: &[u32], l: &Layout) {
    let Ok(prefix) = std::env::var("XRSIM_VIZ_SHOT") else { return };
    let mut img = image::RgbImage::new(l.w as u32, l.h as u32);
    for (i, px) in buf.iter().take(l.w * l.h).enumerate() {
        img.put_pixel((i % l.w) as u32, (i / l.w) as u32, image::Rgb([(px >> 16) as u8, (px >> 8) as u8, *px as u8]));
    }
    if let Some(dir) = std::path::Path::new(&prefix).parent().filter(|d| !d.as_os_str().is_empty()) {
        let _ = std::fs::create_dir_all(dir);
    }
    if let Err(e) = img.save(format!("{prefix}.png")) {
        eprintln!("[VIZ] XRSIM_VIZ_SHOT: cannot save {prefix}.png: {e}");
        return;
    }
    let regions: serde_json::Map<String, Value> =
        l.regions.iter().map(|(k, r)| (k.to_string(), r.map_or(Value::Null, rect))).collect();
    let doc = json!({
        "w": l.w, "h": l.h, "panel_x": l.panel_x, "panel_w": l.panel_w,
        "center": l.view.0, "span": l.view.1, "cursor": l.view.2,
        "lanes": l.lanes.iter().map(|&(link, y, h)| json!({"link": link, "y": y, "h": h})).collect::<Vec<_>>(),
        "regions": regions,
        "abr_strips": l.abr_strips.iter().map(|(n, r)| json!({"name": n, "rect": rect(*r)})).collect::<Vec<_>>(),
    });
    match std::fs::write(format!("{prefix}.json"), serde_json::to_string_pretty(&doc).unwrap_or_default()) {
        Ok(()) => println!("[VIZ] Saved {prefix}.png and {prefix}.json"),
        Err(e) => eprintln!("[VIZ] XRSIM_VIZ_SHOT: cannot save {prefix}.json: {e}"),
    }
}

/// Writes the run's TXOPs, collisions and the tour's examples to `XRSIM_VIZ_EVENTS`, if set.
pub fn dump_events(idx: &VizIndex, examples: &[Example]) {
    let Ok(path) = std::env::var("XRSIM_VIZ_EVENTS") else { return };
    let mut links: Vec<u8> = idx.txop_groups_by_link.keys().copied().collect();
    links.sort_unstable();
    let mut txops = Vec::new();
    for &l in &links {
        for g in &idx.txop_groups_by_link[&l] {
            let Some(&e) = g.legs.first() else { continue };
            let VizEvent::TxopStart { dest_id, ampdu_packets, mcs, ppdu_t0, ppdu_t1, preamble_s, ctrl_tx_s, ctrl_rx_s,
                                      resp_s, bo_drawn, .. } = &idx.all[e] else { continue };
            let access = access_window(&idx.all[e], g.t)
                .map(|(a0, a1, b0, b1, slots)| json!({"aifs": [a0, a1], "bo": [b0, b1], "slots": slots, "drawn": bo_drawn}));
            txops.push(json!({
                "link": l, "t": g.t, "end": g.end, "sta": g.holder.0, "ac": format!("{:?}", g.holder.1), "dest": dest_id,
                "mpdus": ampdu_packets, "mcs": mcs, "data": [ppdu_t0, ppdu_t1], "preamble": preamble_s,
                "rts": ctrl_tx_s, "cts": ctrl_rx_s, "ba": resp_s, "access": access,
            }));
        }
    }
    let mut collisions = Vec::new();
    for &l in &links {
        for &i in idx.collisions_by_link.get(&l).map(|v| v.as_slice()).unwrap_or(&[]) {
            if let VizEvent::Collision { t, end, contenders, .. } = &idx.all[i] {
                let who: Vec<Value> = contenders.iter().map(|k| json!([k.0, format!("{:?}", k.1)])).collect();
                collisions.push(json!({"link": l, "t": t, "end": end, "contenders": who}));
            }
        }
    }
    let ex: Vec<Value> = examples.iter().map(|e| json!({
        "kind": format!("{:?}", e.kind), "lanes": [e.lanes.0, e.lanes.1], "t0": e.t0, "t1": e.t1, "detail": e.detail,
        "anim": e.anim,
    })).collect();
    let doc = json!({"t_min": idx.t_min, "t_max": idx.t_max, "links": links, "txops": txops, "collisions": collisions,
                     "examples": ex});
    match std::fs::write(&path, doc.to_string()) {
        Ok(()) => println!("[VIZ] Saved the events to {path}"),
        Err(e) => eprintln!("[VIZ] XRSIM_VIZ_EVENTS: cannot save {path}: {e}"),
    }
}
