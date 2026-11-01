use std::collections::HashMap;
use std::net::IpAddr;
use std::path::Path;
use minifb::{Key, MouseButton, Window, WindowOptions};
use image::{ImageBuffer, Rgb};
use rand::Rng;
use crate::lib::models_XR::AbrEvent;
use crate::lib::{render_text, ac_prio,} ;

pub mod viz_event; // postsim_visualizers/viz_event.rs: the viewer's events, built from the MAC's
pub mod obss; // postsim_visualizers/obss.rs: the station map (named after the multi-BSS viewer it comes from)
pub mod panel_resize; // postsim_visualizers/panel_resize.rs: drag the backoff and queue panels' top edges
pub mod sidebar_resize; // postsim_visualizers/sidebar_resize.rs: drag the ABR sidebar's edge and the station map's top
#[cfg(test)]
mod robustness; // postsim_visualizers/robustness.rs: the layout maths at any window size
pub mod ui_scale; // postsim_visualizers/ui_scale.rs: draws at a logical resolution so the viewers look alike on 1080p and 4K
pub mod ui_input; // postsim_visualizers/ui_input.rs: a window's mouse and keyboard (minifb's, or merged with a web viewer's)
#[cfg(test)]
mod ui_input_tests; // postsim_visualizers/ui_input_tests.rs
mod layout; // postsim_visualizers/layout.rs: where the two sides and the ABR sidebar go (View and Layout menus)
pub(crate) mod menu_bar; // postsim_visualizers/menu_bar.rs: the Windows-style menu bar widget
mod menus; // postsim_visualizers/menus.rs: the unified viewer's menus and what each item does
mod strip_order; // postsim_visualizers/strip_order.rs: which ABR plots are shown, their order, dragging a title
mod tour; // postsim_visualizers/tour.rs: quick tour, keyboard and mouse sheet, About
mod tour_examples; // postsim_visualizers/tour_examples.rs: real transmissions of the run for the tour to zoom into
mod hover_focus; // postsim_visualizers/hover_focus.rs: a collision or one A-MPDU under the mouse in a link lane

// ── Auto-screenshot: dumps the first rendered frame of each viewer window to
// disk so batch/eval runs (see run_eval_workers.sh) get a visual record without
// requiring a human to look at (or close) the interactive window.
const SCREENSHOT_DIR: &str = "viz_screenshots";

fn sanitize_filename(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}

/// `XRSIM_VIZ_VIEW=center_s,span_ms`: the starting channel view of the unified viewer (and so of its screenshot).
fn viz_view_override() -> Option<(f64, f64)> {
    let v = std::env::var("XRSIM_VIZ_VIEW").ok()?;
    let (c, w) = v.split_once(',')?;
    Some((c.trim().parse().ok()?, w.trim().parse::<f64>().ok()? * 1e-3))
}

/// `XRSIM_VIZ_HOVER=link,t_s`: with the mouse outside the link lanes, act as if it were over the TXOP of `link` running at
/// `t_s` (for screenshots of the hover marks).
fn viz_hover_override(idx: &VizIndex) -> Option<HoverOwner> {
    let v = std::env::var("XRSIM_VIZ_HOVER").ok()?;
    let (l, t) = v.split_once(',')?;
    let (link, t): (u8, f64) = (l.trim().parse().ok()?, t.trim().parse().ok()?);
    let gs = idx.txop_groups_by_link.get(&link)?;
    let g = gs.get(gs.partition_point(|g| g.end < t))?;
    let mut keys = vec![g.holder];
    for &e in &g.legs {
        if let VizEvent::TxopStart { sender, .. } = &idx.all[e] {
            if !keys.contains(sender) { keys.push(*sender); }
        }
    }
    Some(HoverOwner { t: g.t, link, holder: g.holder, keys })
}

/// Saves `buf` (minifb's 0RGB pixel format) as `viz_screenshots/<sim_tag>_<viewer_kind>.png`.
fn save_screenshot(buf: &[u32], w: usize, h: usize, sim_tag: &str, viewer_kind: &str) {
    if let Err(e) = std::fs::create_dir_all(SCREENSHOT_DIR) {
        eprintln!("[VIZ] Failed to create screenshot dir {}: {}", SCREENSHOT_DIR, e);
        return;
    }
    let mut img: ImageBuffer<Rgb<u8>, Vec<u8>> = ImageBuffer::new(w as u32, h as u32);
    for y in 0..h {
        for x in 0..w {
            let px = buf[y * w + x];
            let r = ((px >> 16) & 0xFF) as u8;
            let g = ((px >> 8)  & 0xFF) as u8;
            let b = ( px        & 0xFF) as u8;
            img.put_pixel(x as u32, y as u32, Rgb([r, g, b]));
        }
    }
    // sim_tag alone can collide across parallel eval workers (same seed/sim_id
    // reused in different runs), so tack on a random suffix to keep filenames unique.
    let rand_suffix: u32 = rand::thread_rng().gen();
    let filename = format!("{}_{}_{:08x}.png", sanitize_filename(sim_tag), viewer_kind, rand_suffix);
    let path = Path::new(SCREENSHOT_DIR).join(&filename);
    match img.save(&path) {
        Ok(())  => println!("[VIZ] Saved screenshot: {}", path.display()),
        Err(e) => eprintln!("[VIZ] Failed to save screenshot {}: {}", path.display(), e),
    }
}

use std::time::Instant;
const SPEED_MIN:  f64   = 1e-6;   // 1 µs of sim-time per real-second
const SPEED_MAX:  f64   = 1.0;    // 1 s of sim-time per real-second

// Playback bar: one row  [PLAY] [REWIND]  SPEED [-----o-----] 1.00 ms/s   cursor t
const CONTROLS_H:   usize = 44;
const BTN_Y:        usize = HUD_H + 6;
const BTN_H:        usize = 26;
const SLIDER_ROW_Y: usize = BTN_Y;
const SLIDER_ROW_H: usize = BTN_H;
const BTN_PLAY_X:   usize = 8;
const BTN_PLAY_W:   usize = 84;
const BTN_RESET_X:  usize = BTN_PLAY_X + BTN_PLAY_W + 8;
const BTN_RESET_W:  usize = 96;
const SLIDER_X0:    usize = BTN_RESET_X + BTN_RESET_W + 96;   // after the "SPEED" label
const SLIDER_X1:    usize = SLIDER_X0 + 360;

fn speed_to_frac(speed: f64) -> f64 {
    let lo = SPEED_MIN.log10();
    let hi = SPEED_MAX.log10();
    ((speed.log10() - lo) / (hi - lo)).clamp(0.0, 1.0)
}
fn frac_to_speed(frac: f64) -> f64 {
    let lo = SPEED_MIN.log10();
    let hi = SPEED_MAX.log10();
    10f64.powf(lo + frac.clamp(0.0, 1.0) * (hi - lo))
}
fn fmt_speed(s: f64) -> String {
    if s < 1e-3 { format!("{:.2} us/s", s * 1e6) }
    else if s < 1.0 { format!("{:.2} ms/s", s * 1e3) }
    else { format!("{:.2} s/s", s) }
}

/// The largest integer text scale (2 or 1) at which `text` fits in `avail` pixels; fractional scales blur the bitmap font.
fn fit_scale(text: &str, avail: usize) -> f32 {
    if text_w(text, 2.0) as usize <= avail { 2.0 } else { 1.0 }
}


// ── Palette: one colour per user ─────────────────────────────────────────────
const USER_PALETTE: &[u32] = &[
    0x4fc3f7, // sky blue
    0xff8a65, // coral
    0xa5d6a7, // sage green
    0xce93d8, // lavender
    0xffd54f, // amber
    0xef9a9a, // rose
    0x80cbc4, // teal
    0xffcc02, // yellow
];
const CW_H: usize = 140;


const ZOOM_BTN_X: usize = 850;
const RESET_BTN_X: usize = 950;

const ZOOM_BTN_W:   usize = 88;
const RESET_BTN_W:  usize = 88;

#[inline]
fn user_color(idx: usize) -> u32 {
    USER_PALETTE[idx % USER_PALETTE.len()]
}

// ── Helper: timestamp from an AbrEvent ───────────────────────────────────────
fn abr_event_t(ev: &AbrEvent) -> f64 {
    match ev {
        AbrEvent::FrameMetrics  { t, .. } => *t,
        AbrEvent::BitrateUpdate { t, .. } => *t,
        AbrEvent::Reset         { t, .. } => *t,
                AbrEvent::StaLocation   { t, .. } => *t,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// AbrVizIndex
// ─────────────────────────────────────────────────────────────────────────────
pub struct AbrVizIndex {
    pub events:               Vec<AbrEvent>,
    /// Event-index lists per IP, sorted by time.
    // pub by_ip:                HashMap<IpAddr, Vec<usize>>,
    // pub by_ip_bitra: HashMap<IpAddr, Vec<usize>>, // indexes BitrateUpdate events
    pub by_ip_frame:          HashMap<IpAddr, Vec<usize>>, // indexes FrameMetrics events
    pub by_ip_bitrate:        HashMap<IpAddr, Vec<usize>>, // indexes

    /// Stable IP ordering (insertion order = creation order).
    pub ip_order:             Vec<IpAddr>,
    /// ABR mode label per IP (last seen wins).
    pub abr_mode_label:       HashMap<IpAddr, String>,
    pub t_min:                f64,
    pub t_max:                f64,
    // Global y-axis ceilings (padded slightly above observed max).
    pub ceil_bitrate_mbps:    f32,
    pub ceil_rtt_ms:          f32,
    pub ceil_throughput_mbps: f32,
    pub ceil_instant_throughput_mbps: f32,

    pub by_ip_sta:    HashMap<IpAddr, Vec<usize>>,
    /// Stable STA IP ordering (insertion = first-seen order).
    pub ip_sta_order: Vec<IpAddr>,
    /// World-space extents over all StaLocation events (padded).
    pub sta_x_range:  (f32, f32),
    pub sta_y_range: (f32, f32),   // was sta_z_range
    // pub sta_z_range:  (f32, f32),

}

impl AbrVizIndex {
    pub fn build(mut events: Vec<AbrEvent>) -> Self {
        events.sort_by(|a, b| abr_event_t(a).partial_cmp(&abr_event_t(b)).unwrap());

        let t_min = events.first().map(abr_event_t).unwrap_or(0.0);
        let t_max = events.last() .map(abr_event_t).unwrap_or(1.0);

        let mut by_ip_frame:   HashMap<IpAddr, Vec<usize>> = HashMap::new();
        let mut by_ip_bitrate: HashMap<IpAddr, Vec<usize>> = HashMap::new();
        let mut by_ip_sta:     HashMap<IpAddr, Vec<usize>> = HashMap::new();  // ← new

        let mut ip_order:       Vec<IpAddr>             = Vec::new();
        let mut ip_sta_order:   Vec<IpAddr>             = Vec::new();          // ← new
        let mut abr_mode_label: HashMap<IpAddr, String> = HashMap::new();

        let (mut max_br, mut max_rtt, mut max_tp) = (1.0f32, 1.0f32, 1.0f32);
        let mut max_itp = 1.0f32;

        // ── STA spatial extents ──────────────────────────────────────────────
        let (mut sta_x_min, mut sta_x_max) = (f32::MAX, f32::MIN);
        let (mut sta_y_min, mut sta_y_max) = (f32::MAX, f32::MIN);

        for (i, ev) in events.iter().enumerate() {
            match ev {
                AbrEvent::FrameMetrics { ip_server, peak_throughput_mbps, instant_throughput_mbps, rtt_ms, .. } => {
                    max_rtt = max_rtt.max(*rtt_ms);
                    max_tp  = max_tp .max(*peak_throughput_mbps);
                    max_itp = max_itp.max(*instant_throughput_mbps);
                    let entry = by_ip_frame.entry(*ip_server).or_insert_with(|| {
                        ip_order.push(*ip_server);
                        Vec::new()
                    });
                    entry.push(i);
                }
                AbrEvent::BitrateUpdate { ip_server, abr_mode, new_bitrate_mbps, .. } => {
                    max_br = max_br.max(*new_bitrate_mbps);
                    abr_mode_label.insert(*ip_server, abr_mode.clone());
                    by_ip_frame.entry(*ip_server).or_insert_with(|| {
                        ip_order.push(*ip_server);
                        Vec::new()
                    });
                    by_ip_bitrate.entry(*ip_server).or_default().push(i);
                }
                AbrEvent::Reset { .. } => {}
                // ── new ──────────────────────────────────────────────────────
                AbrEvent::StaLocation { ip_sta, x,y, z, .. } => {
                    sta_x_min = sta_x_min.min(*x);
                    sta_x_max = sta_x_max.max(*x);
                    sta_y_min = sta_y_min.min(*y);
                    sta_y_max = sta_y_max.max(*y);
                    by_ip_sta.entry(*ip_sta).or_insert_with(|| {
                    ip_sta_order.push(*ip_sta);
                        Vec::new()
                    }).push(i);
                }
            }
        }

        // Pad spatial extents 10 % on each side; fall back to a 10 m box when empty.
        let sta_x_range = if sta_x_min <= sta_x_max {
            let p = ((sta_x_max - sta_x_min) * 0.10).max(0.5);
            (sta_x_min - p, sta_x_max + p)
        } else {
            (-5.0, 5.0)
        };
        let y_pad = ((sta_y_max - sta_y_min) * 0.10).max(0.5);
        let sta_y_range: (f32, f32) = (sta_y_min - y_pad, sta_y_max + y_pad);

        Self {
            events,
            by_ip_frame,
            by_ip_bitrate,
            by_ip_sta,
            ip_order,
            ip_sta_order,
            abr_mode_label,
            t_min,
            t_max,
            ceil_bitrate_mbps:    (max_br  * 1.15).max(1.0),
            ceil_rtt_ms:          (max_rtt * 1.15).max(1.0),
            ceil_throughput_mbps: (max_tp  * 1.15).max(1.0),
            ceil_instant_throughput_mbps: (max_itp * 1.15).max(1.0),
            sta_x_range,
            sta_y_range,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// View state (shared pan/zoom, same idea as VizEvent viewer)
// ─────────────────────────────────────────────────────────────────────────────
struct AbrViewState {
    center_t:   f64,
    span_t:     f64,
    cursor_t:   f64,
    mouse_drag: Option<(f32, f64)>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Layout constants
// ─────────────────────────────────────────────────────────────────────────────
const ABR_W:     usize = 1700;  // was 1500 — recover panel width lost to wider sidebar
const ABR_H:     usize = 950;   // was 900  — a little more vertical breathing room
const SIDEBAR_W: usize = 370;   // was 220  — must fit ~30-char readout line at scale 1.8
const PANEL_PAD: usize = 10;    // was 8
const LABEL_H:   usize = 26;    // was 18   — strip label height at scale 1.8

const TEXT_SIZE_ABR: f32 = 1.8;
// Fixed y-axis ceiling for the Bitrate strip (unlike the other strips, which
// autoscale to the visible time window) — keeps the scale comparable across
// scenarios/checkpoints instead of rescaling with whatever bitrate is onscreen.
const BITRATE_CEIL_MBPS: f32 = 120.0;
#[derive(Clone, Copy)]
enum StripSource { FrameMetrics, BitrateUpdate }

/// One metric strip descriptor.
struct MetricStrip {
    label:   &'static str,
    unit:    &'static str,
    extract: fn(&AbrEvent) -> Option<f32>,
    ceil:    f32,
    source:  StripSource,
    decimals: usize,          
}



/// Thin scrollbar at the left edge of a strip panel's plot: `start` of `n` rows, `shown` visible.
fn draw_row_scrollbar(buf: &mut [u32], stride: usize, panel_y: usize, panel_h: usize, panel_x: usize, n: usize, shown: usize, start: usize) {
    if n <= shown { return; }
    let x = panel_x.saturating_sub(5);
    fill_rect(buf, stride, x, panel_y, 3, panel_h, 0x1c1c28);
    let th = (panel_h * shown / n).max(8);
    let ty = panel_y + (panel_h - th) * start / (n - shown).max(1);
    fill_rect(buf, stride, x, ty, 3, th, 0x7a8aaa);
}

/// (rows shown, row height) of a strip panel of `panel_h` px holding `n` rows of at least `min_row` px.
fn strip_geometry(n: usize, panel_h: usize, min_row: usize) -> (usize, usize) {
    let n = n.max(1);
    let fit = (panel_h.saturating_sub(2) / min_row).max(1);
    let shown = n.min(fit);
    (shown, (panel_h.saturating_sub(2) / shown).max(min_row))
}

/// Most queue strips on screen at once; the rest are reached by scrolling the label column.
const Q_MAX_ROWS: usize = 12;

/// `strip_geometry` for the queue panel: at most `Q_MAX_ROWS` rows, each stretched to share the panel height.
fn qstrip_geometry(n: usize, panel_h: usize) -> (usize, usize) {
    let shown = strip_geometry(n, panel_h, 9).0.min(Q_MAX_ROWS);
    (shown, (panel_h.saturating_sub(2) / shown).max(9))
}

/// The CW strips in display order (and how many constant-CW queues are left out).
fn cw_strip_keys(view: &ViewState, idx: &VizIndex) -> (Vec<MacKey>, usize) {
    let t_hi = view.center_t + view.span_t * 0.5;
    let varies = |ser: &Vec<(f64, u32, bool)>| ser.first().is_some_and(|f| ser.iter().any(|x| x.1 != f.1));
    let mut keys: Vec<MacKey> = idx.cw_series.iter()
        .filter(|(_, ser)| varies(ser) && ser.first().is_some_and(|f| f.0 <= t_hi))
        .map(|(k, _)| *k)
        .collect();
    keys.sort_by_key(|k| (k.0 >= 0, k.0, std::cmp::Reverse(ac_prio(k.1)), k.2));
    (keys, idx.cw_series.values().filter(|ser| !varies(ser)).count())
}

/// The queue strips in display order.
fn qdepth_strip_keys(view: &ViewState, idx: &VizIndex) -> Vec<(i32, EdcaAc)> {
    let t_hi = view.center_t + view.span_t * 0.5;
    idx.qdepth_series.iter()
        .filter(|(_, ser)| ser.first().is_some_and(|f| f.0 <= t_hi))
        .map(|(k, _)| *k)
        .collect()
}

fn hit_test_cw_legend(
    mx: f32, my: f32,
    panel_y: usize, panel_h: usize,
    panel_x: usize,
    view:    &ViewState,
    idx:     &VizIndex,
) -> Option<ActiveHighlights> {
    if (mx as usize) >= panel_x { return None; }
    let my_u = my as usize;
    if view.q_mode != 0 && view.show_bo_matrix {
        let rows = bo_matrix_rows(view, idx);
        let (shown, row_h) = strip_geometry(rows.len(), panel_h.saturating_sub(12), 13);
        if my_u < panel_y + 12 { return None; }
        let r = (my_u - panel_y - 12) / row_h;
        if r >= shown { return None; }
        let (sta, link) = rows[r + view.cw_scroll.min(rows.len().saturating_sub(shown))];
        let mut h = ActiveHighlights { keys: HashSet::new(), txops: HashSet::new() };
        for key in &idx.mac_keys_sorted {
            if key.0 == sta && key.2 == link { h.keys.insert(*key); h.txops.insert((*key, -1)); }
        }
        return Some(h);
    }
    if view.q_mode != 0 {
        let (keys, _) = cw_strip_keys(view, idx);
        let (shown, row_h) = strip_geometry(keys.len(), panel_h, 14);
        if my_u < panel_y + 1 { return None; }
        let r = (my_u - panel_y - 1) / row_h;
        if r >= shown { return None; }
        let key = keys[r + view.cw_scroll.min(keys.len().saturating_sub(shown))];
        let mut h = ActiveHighlights { keys: HashSet::new(), txops: HashSet::new() };
        h.keys.insert(key);
        h.txops.insert((key, -1));
        return Some(h);
    }

    let t_lo = view.center_t - view.span_t * 0.5;
    let t_hi = view.center_t + view.span_t * 0.5;

    // Must exactly mirror the sort in render_cw_panel
    let mut keys: Vec<MacKey> = idx.cw_series.keys().copied().collect();
    keys.sort_by_key(|k| (ac_prio(k.1), k.0, k.2));
    keys.reverse();

    let mut legend_y = panel_y + 24;
    const ITEM_H: usize = 20;

    for &key in &keys {
        let series = &idx.cw_series[&key];
        let s = series.partition_point(|(t, _, _)| *t < t_lo);
        let is_active = s > 0 || (s < series.len() && series[s].0 <= t_hi);
        if !is_active { continue; }
        if legend_y + 12 >= panel_y + panel_h { break; }

        if my_u >= legend_y && my_u < legend_y + ITEM_H {
            let mut h = ActiveHighlights { keys: HashSet::new(), txops: HashSet::new() };
            h.keys.insert(key);
            h.txops.insert((key, -1)); // wildcard dest so render_link_lane matches on sta+ac
            return Some(h);
        }
        legend_y += ITEM_H;
    }
    None
}
/// Height of one legend line (swatch, IP, mode) of the ABR sidebar.
const ABR_LEGEND_ITEM_H: usize = 16;

/// Height `render_abr_sidebar` fills from its first legend line down: the legend, the time readout and the cursor table
/// (one line and one row per IP), plus a small gap. The station map takes what is left below it.
fn abr_sidebar_content_h(n_ips: usize) -> usize {
    // 8 gap + 18 readout + 2 + 16 header + 2 + 1 border, then a line and a row per IP
    const FIXED: usize = 8 + 18 + 2 + 16 + 2 + 1 + 6;
    FIXED + n_ips * (ABR_LEGEND_ITEM_H + 16)
}

fn hit_test_abr_legend(mx: f32, my: f32, idx: &AbrVizIndex, sidebar_w: usize, legend_start: usize) -> Option<usize> {
    // Legend lives entirely inside the sidebar; matches the layout in render_abr_sidebar
    if mx < 0.0 || mx as usize >= sidebar_w { return None; }
    let my_u = my as usize;
    let item_h = ABR_LEGEND_ITEM_H;

    for (ip_idx, _) in idx.ip_order.iter().enumerate() {
        let item_y = legend_start + ip_idx * item_h;
        if my_u >= item_y && my_u < item_y + item_h {
            return Some(ip_idx);
        }
    }
    None
}


fn make_strips(idx: &AbrVizIndex) -> Vec<MetricStrip> {
    vec![
        MetricStrip {
            label:   "Bitrate",
            unit:    "Mbps",
            extract: |ev| match ev {
                AbrEvent::BitrateUpdate { new_bitrate_mbps, .. } => Some(*new_bitrate_mbps),
                _ => None,
            },
            ceil:     BITRATE_CEIL_MBPS,
            source:   StripSource::BitrateUpdate,
            decimals: 1,
        },
        MetricStrip {
            label:   "Peak Throughput",
            unit:    "Mbps",
            extract: |ev| match ev {
                AbrEvent::FrameMetrics { peak_throughput_mbps, .. } => Some(*peak_throughput_mbps),
                _ => None,
            },
            ceil:     idx.ceil_throughput_mbps,
            source:   StripSource::FrameMetrics,
            decimals: 1,
        },
        MetricStrip {
            label:   "Instant Throughput",
            unit:    "Mbps",
            extract: |ev| match ev {
                AbrEvent::FrameMetrics { instant_throughput_mbps, .. } => Some(*instant_throughput_mbps),
                _ => None,
            },
            ceil:     idx.ceil_instant_throughput_mbps,
            source:   StripSource::FrameMetrics,
            decimals: 1,
        },
        MetricStrip {
            label:   "RTT",
            unit:    "ms",
            extract: |ev| match ev {
                AbrEvent::FrameMetrics { rtt_ms, .. } => Some(*rtt_ms),
                _ => None,
            },
            ceil:     idx.ceil_rtt_ms,
            source:   StripSource::FrameMetrics,
            decimals: 1,
        },
        MetricStrip {
            label:   "Frame-Loss Ratio",
            unit:    "FLR",
            extract: |ev| match ev {
                AbrEvent::FrameMetrics { flr, .. } => Some(*flr),
                _ => None,
            },
            ceil:     0.2,
            source:   StripSource::FrameMetrics,
            decimals: 2,
        },
    ]
}

// ─────────────────────────────────────────────────────────────────────────────
// Coordinate helpers
// ─────────────────────────────────────────────────────────────────────────────
#[inline]
fn abr_x_of(t: f64, view: &AbrViewState, panel_x: usize, panel_w: usize) -> i32 {
    let t0 = view.center_t - view.span_t * 0.5;
    let n  = (t - t0) / view.span_t;
    (panel_x as f64 + n * panel_w as f64) as i32
}

#[inline]
fn abr_y_of(val: f32, ceil: f32, strip_top: usize, strip_h: usize) -> i32 {
    let frac = (val / ceil.max(1e-9)).clamp(0.0, 1.0) as f64;
    let y    = strip_top as f64 + (1.0 - frac) * (strip_h as f64 - 4.0);
    y as i32
}

// ─────────────────────────────────────────────────────────────────────────────
// Low-level pixel primitives (mirror your existing ones)
// ─────────────────────────────────────────────────────────────────────────────
// fn abr_fill_rect(buf: &mut [u32], stride: usize,
//                  x: usize, y: usize, w: usize, h: usize, color: u32) {
//     for row in y..(y + h) {
//         for col in x..(x + w) {
//             let i = row * stride + col;
//             if i < buf.len() { buf[i] = color; }
//         }
//     }
// }
fn abr_fill_rect(buf: &mut [u32], stride: usize,
                 x: usize, y: usize, w: usize, h: usize, color: u32) {
    let x_end = (x + w).min(stride);
    for row in y..(y + h) {
        let base = row * stride;
        if base + x_end <= buf.len() {
            buf[base + x..base + x_end].fill(color);
        }
    }
}


fn abr_draw_line(buf: &mut [u32], stride: usize,
                 mut x0: i32, mut y0: i32,
                 x1: i32, y1: i32, color: u32) {
    let dx =  (x1 - x0).abs();
    let dy = -((y1 - y0).abs());
    let sx = if x0 < x1 { 1 } else { -1 };
    let sy = if y0 < y1 { 1 } else { -1 };
    let mut err = dx + dy;
    loop {
        if x0 >= 0 && y0 >= 0 {
            let i = y0 as usize * stride + x0 as usize;
            if i < buf.len() { buf[i] = color; }
        }
        if x0 == x1 && y0 == y1 { break; }
        let e2 = 2 * err;
        if e2 >= dy { err += dy; x0 += sx; }
        if e2 <= dx { err += dx; y0 += sy; }
    }
}

fn abr_draw_hline(buf: &mut [u32], stride: usize,
                  x0: i32, x1: i32, y: i32, color: u32) {
    if y < 0 { return; }
    for x in x0.max(0)..x1.min(stride as i32) {
        let i = y as usize * stride + x as usize;
        if i < buf.len() { buf[i] = color; }
    }
}

fn abr_draw_vline(buf: &mut [u32], stride: usize,
                  x: i32, y0: i32, y1: i32, color: u32) {
    if x < 0 || x >= stride as i32 { return; }
    let (a, b) = if y0 <= y1 { (y0, y1) } else { (y1, y0) };
    for y in a..=b {
        if y >= 0 {
            let i = y as usize * stride + x as usize;
            if i < buf.len() { buf[i] = color; }
        }
    }
}

fn abr_draw_hline_blend(buf: &mut [u32], stride: usize,
                         x0: i32, x1: i32, y: i32, color: u32) {
    if y < 0 { return; }
    for x in x0.max(0)..x1.min(stride as i32) {
        let i = y as usize * stride + x as usize;
        if i < buf.len() { buf[i] = blend_screen(buf[i], color); }
    }
}

fn abr_draw_vline_blend(buf: &mut [u32], stride: usize,
                         x: i32, y0: i32, y1: i32, color: u32) {
    if x < 0 || x >= stride as i32 { return; }
    let (a, b) = if y0 <= y1 { (y0, y1) } else { (y1, y0) };
    for y in a..=b {
        if y >= 0 {
            let i = y as usize * stride + x as usize;
            if i < buf.len() { buf[i] = blend_screen(buf[i], color); }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// STA Trajectory Grid (24 cols × 12 rows, x/z horizontal plane)
// ─────────────────────────────────────────────────────────────────────────────
const STA_GRID_COLS: usize = 24;
const STA_GRID_ROWS: usize = 12;

fn render_sta_grid(
    buf:      &mut [u32],
    stride:   usize,
    panel_x:  usize,
    panel_w:  usize,
    y_top:    usize,
    height:   usize,
    idx:      &AbrVizIndex,
    cursor_t: f64,
) {
    // ── Backgrounds ──────────────────────────────────────────────────────────
    abr_fill_rect(buf, stride, 0,       y_top, SIDEBAR_W, height, 0x10101a);
    abr_fill_rect(buf, stride, panel_x, y_top, panel_w,   height, 0x0c0c16);

    // ── Sidebar: title + per-STA legend ──────────────────────────────────────
    render_text(buf, "STA TRAJECTORIES", 8, y_top + 6,  stride, 0xffffff, TEXT_SIZE_ABR);
    render_text(buf, &format!("t = {:.3}s", cursor_t),
                8, y_top + 24, stride, 0x888899, TEXT_SIZE_ABR);

    let mut ly = y_top + 48;
    for (si, ip) in idx.ip_sta_order.iter().enumerate() {
        let color = user_color(si);
        abr_fill_rect(buf, stride, 8, ly, 14, 10, color);
        render_text(buf, &format!("{}", ip), 28, ly, stride, 0xdddddd, TEXT_SIZE_ABR);
        ly += 22;
    }

    // ── Grid lines ───────────────────────────────────────────────────────────
    let cell_w = panel_w as f32 / STA_GRID_COLS as f32;
    let cell_h = height  as f32 / STA_GRID_ROWS as f32;

    for col in 0..=STA_GRID_COLS {
        let gx = panel_x as i32 + (col as f32 * cell_w) as i32;
        abr_draw_vline(buf, stride, gx, y_top as i32, (y_top + height) as i32, 0x18182a);
    }
    for row in 0..=STA_GRID_ROWS {
        let gy = y_top as i32 + (row as f32 * cell_h) as i32;
        abr_draw_hline(buf, stride,
            panel_x as i32, (panel_x + panel_w) as i32, gy, 0x18182a);
    }

    // ── Axis corner labels ───────────────────────────────────────────────────
    let (x_min, x_max) = idx.sta_x_range;
    let (z_min, z_max) = idx.sta_y_range;
    let x_span = (x_max - x_min).max(1e-6);
    let z_span = (z_max - z_min).max(1e-6);

    render_text(buf, &format!("x {:.1}m", x_min),
        panel_x + 2,              y_top + height - 14, stride, 0x445566, TEXT_SIZE_ABR);
    render_text(buf, &format!("{:.1}m", x_max),
        panel_x + panel_w - 46,  y_top + height - 14, stride, 0x445566, TEXT_SIZE_ABR);
    render_text(buf, &format!("z {:.1}m", z_max),
        panel_x + 2, y_top + 2,               stride, 0x445566, TEXT_SIZE_ABR);
    render_text(buf, &format!("z {:.1}m", z_min),
        panel_x + 2, y_top + height - 28,     stride, 0x445566, TEXT_SIZE_ABR);

    // ── World → pixel helper (z axis: larger z = higher on screen) ───────────
    let to_px = |x: f32, z: f32| -> (i32, i32) {
        let nx = ((x - x_min) / x_span) as f64;
        let nz = 1.0 - ((z - z_min) / z_span) as f64;          // flip so +z = up
        (
            panel_x as i32 + (nx * panel_w  as f64) as i32,
            y_top   as i32 + (nz * height   as f64) as i32,
        )
    };

    // ── Per-STA: trail up to cursor, then bright dot at current position ─────
    for (si, ip) in idx.ip_sta_order.iter().enumerate() {
        let base  = user_color(si);
        let trail = dim_color(base, 2);

        let Some(indices) = idx.by_ip_sta.get(ip) else { continue };

        // binary-search to find how many samples are ≤ cursor_t
        let end = indices.partition_point(|&i| abr_event_t(&idx.events[i]) <= cursor_t);
        if end == 0 { continue; }

        let mut prev_px: Option<(i32, i32)> = None;

        for &ei in &indices[..end] {
            if let AbrEvent::StaLocation { x, y, .. } = &idx.events[ei] {
                let (px, py) = to_px(*x, *y);
                if let Some((ppx, ppy)) = prev_px {
                    abr_draw_line(buf, stride, ppx, ppy, px, py, trail);
                }
                prev_px = Some((px, py));
            }
        }

        // Current position: filled circle (r = 4 px)
        if let Some((px, py)) = prev_px {
            let bright = brighten_color(base);
            for dy in -4i32..=4 {
                for dx in -4i32..=4 {
                    if dx * dx + dy * dy <= 16 {
                        let fx = px + dx;
                        let fy = py + dy;
                        if fx >= 0 && fy >= 0 {
                            let i = fy as usize * stride + fx as usize;
                            if i < buf.len() { buf[i] = bright; }
                        }
                    }
                }
            }
        }
    }
}

/// Purple dashed step-line color — traces the applied bandwidth cap (see
/// `VizEvent::BandwidthChange`) on the Peak Throughput strip.
const BANDWIDTH_MARKER_COLOR: u32 = 0xb266ff;

/// High-frequency dashed horizontal segment (short on/off so it reads as a distinct
/// "limit" line rather than a solid measured series), `width` px thick.
fn abr_draw_dashed_hline(buf: &mut [u32], stride: usize, x0: i32, x1: i32, y: i32, color: u32, width: i32) {
    if y < 0 { return; }
    const DASH_ON: i32 = 3;
    const DASH_OFF: i32 = 2;
    let mut x = x0.max(0);
    let x_end = x1.min(stride as i32);
    while x < x_end {
        let seg_end = (x + DASH_ON).min(x_end);
        for dy in 0..width {
            let yy = y + dy;
            if yy < 0 { continue; }
            let base = yy as usize * stride;
            if base + seg_end as usize <= buf.len() {
                buf[base + x as usize..base + seg_end as usize].fill(color);
            }
        }
        x += DASH_ON + DASH_OFF;
    }
}

/// High-frequency dashed vertical segment — connects consecutive bandwidth-cap steps.
fn abr_draw_dashed_vline(buf: &mut [u32], stride: usize, x: i32, y0: i32, y1: i32, color: u32, width: i32) {
    if x < 0 { return; }
    let (a, b) = if y0 <= y1 { (y0, y1) } else { (y1, y0) };
    const DASH_ON: i32 = 3;
    const DASH_OFF: i32 = 2;
    let mut y = a;
    while y <= b {
        let seg_end = (y + DASH_ON).min(b);
        for yy in y.max(0)..=seg_end {
            for dx in 0..width {
                let xx = x + dx;
                if xx < 0 || xx >= stride as i32 { continue; }
                let i = yy as usize * stride + xx as usize;
                if i < buf.len() { buf[i] = color; }
            }
        }
        y += DASH_ON + DASH_OFF;
    }
}

/// Overlays a purple, high-frequency dashed step-line tracing the applied bandwidth cap
/// (from `NetworkPatternEmulator::add_markov_modulated_bandwidth`) at its own Mbps scale on
/// the Bitrate strip, so tuning the Markov chain's dwell/transition parameters can be
/// visually cross-checked against the ABR's chosen bitrate.
fn render_bandwidth_overlay(
    buf:     &mut [u32],
    stride:  usize,
    strip_y: usize,
    strip_h: usize,
    panel_x: usize,
    panel_w: usize,
    view:    &AbrViewState,
    ceil:    f32,
    viz_idx: &VizIndex,
) {
    let t_lo = view.center_t - view.span_t * 0.5;
    let t_hi = view.center_t + view.span_t * 0.5;

    let start = viz_idx.all.partition_point(|e| event_t(e) < t_lo).saturating_sub(1);
    let mut prev_end: Option<(i32, i32)> = None;
    for ev in &viz_idx.all[start..] {
        let t = event_t(ev);
        if t > t_hi { break; }
        let VizEvent::BandwidthChange { end, mbps, .. } = ev else { continue };

        let x0 = abr_x_of(t, view, panel_x, panel_w).max(panel_x as i32);
        let x1 = abr_x_of(*end, view, panel_x, panel_w).min((panel_x + panel_w) as i32);
        let y  = abr_y_of(*mbps, ceil, strip_y, strip_h);

        if let Some((px, py)) = prev_end {
            abr_draw_dashed_vline(buf, stride, px, py, y, BANDWIDTH_MARKER_COLOR, 2);
        }
        if x0 < x1 {
            abr_draw_dashed_hline(buf, stride, x0, x1, y, BANDWIDTH_MARKER_COLOR, 2);
        }
        prev_end = Some((x1, y));
    }
}

fn render_abr_strip(
    buf:      &mut [u32],
    stride:   usize,
    strip:    &MetricStrip,
    strip_y:  usize,
    strip_h:  usize,
    panel_x:  usize,
    panel_w:  usize,
    view:     &AbrViewState,
    idx:      &AbrVizIndex,
    highlight_ip: Option<usize>,
    viz_idx:  Option<&VizIndex>,
) {
    let t_lo = view.center_t - view.span_t * 0.5;
    let t_hi = view.center_t + view.span_t * 0.5;

    // Bitrate stays pinned to a fixed scale — zoom/visible-window rescaling
    // makes it hard to visually compare the ABR's chosen bitrate against the
    // ladder ceiling across scenarios, unlike the other (genuinely unbounded)
    // strips below.
    let ceil = if strip.label == "Bitrate" {
        BITRATE_CEIL_MBPS
    } else {
        // dynamic Y ceiling from visible data
        let mut visible_max = 0.0f32;
        for ip in &idx.ip_order {
            let ip_map: &HashMap<IpAddr, Vec<usize>> = match strip.source {
                StripSource::FrameMetrics  => &idx.by_ip_frame,
                StripSource::BitrateUpdate => &idx.by_ip_bitrate,
            };
            let Some(indices) = ip_map.get(ip) else { continue };
            let start = indices.partition_point(|&i| abr_event_t(&idx.events[i]) < t_lo)
                .saturating_sub(1);
            for &ei in &indices[start..] {
                let t = abr_event_t(&idx.events[ei]);
                if t > t_hi { break; }
                if let Some(v) = (strip.extract)(&idx.events[ei]) {
                    visible_max = visible_max.max(v);
                }
            }
        }
        // Pad 15 % above visible max; fall back to the global ceil when empty.
        if visible_max > 0.0 {
            (visible_max * 1.15).max(1e-3)
        } else {
            strip.ceil          // global fallback when no data in view
        }
    };
    // ─────────────────────────────────────────────────────────────────────────

    // Background
    abr_fill_rect(buf, stride, panel_x, strip_y, panel_w, strip_h, 0x10101a);

    // Grid lines — now use `ceil` instead of `strip.ceil`
    for frac in [0.25f32, 0.50, 0.75] {
        let gy = abr_y_of(ceil * frac, ceil, strip_y, strip_h);
        abr_draw_hline(buf, stride, panel_x as i32, (panel_x + panel_w) as i32, gy, 0x1e1e2e);
        // let label = format!("{:.1}", ceil * frac);
        let label = format!("{:.prec$}", ceil * frac, prec = strip.decimals);
        render_text(buf, &label, panel_x + 2, gy as usize + 2, stride, 0x444455, TEXT_SIZE_ABR);
    }

    // Ceiling label
    render_text(
        buf,
        &format!("{} [{}]  max={:.prec$}", strip.label, strip.unit, ceil, prec = strip.decimals),
        panel_x + 6, strip_y + 3, stride, 0x888899, TEXT_SIZE_ABR,
    );

    // Step-plot series — pass `ceil` to abr_y_of
   
    let draw_series = |buf: &mut [u32], ip_idx: usize, color: u32| {
        let ip = &idx.ip_order[ip_idx];
        let ip_map: &HashMap<IpAddr, Vec<usize>> = match strip.source {
            StripSource::FrameMetrics  => &idx.by_ip_frame,
            StripSource::BitrateUpdate => &idx.by_ip_bitrate,
        };
        let Some(indices) = ip_map.get(ip) else { return; };
        let start = indices.partition_point(|&i| abr_event_t(&idx.events[i]) < t_lo)
            .saturating_sub(1);
        let mut prev_x: Option<(i32, i32)> = None;
        for &ei in &indices[start..] {
            let ev = &idx.events[ei];
            let t  = abr_event_t(ev);
            if t > t_hi { break; }
            let val = match (strip.extract)(ev) { Some(v) => v, None => continue };
            let x   = abr_x_of(t, view, panel_x, panel_w);
            let y   = abr_y_of(val, ceil, strip_y, strip_h);
            if let Some((px, py)) = prev_x {
                let x0 = px.max(panel_x as i32);
                let x1 = x.min((panel_x + panel_w) as i32);
                if x0 < x1 {
                    abr_draw_hline_blend(buf, stride, x0, x1, py, color);
                }
                if x >= panel_x as i32 && x < (panel_x + panel_w) as i32 {
                    abr_draw_vline_blend(buf, stride, x, py, y, dim_color(color, 2));
                }
            }
            prev_x = Some((x, y));
        }
        if let Some((px, py)) = prev_x {
            let t_max_x = abr_x_of(idx.t_max, view, panel_x, panel_w)
                .min((panel_x + panel_w) as i32);
            let x0 = px.max(panel_x as i32);
            if x0 < t_max_x {
                abr_draw_hline_blend(buf, stride, x0, t_max_x, py, color);
            }
        }
    };

    for (ip_idx, _) in idx.ip_order.iter().enumerate() {
        let base  = user_color(ip_idx);
        let color = match (highlight_ip, highlight_ip.map_or(false, |h| h == ip_idx)) {
            (Some(_), true)  => continue,
            (Some(_), false) => dim_color(base, 5),
            (None,    _)     => base,
        };
        draw_series(buf, ip_idx, color);
    }

    if let Some(hi) = highlight_ip {
        if hi < idx.ip_order.len() {
            draw_series(buf, hi, brighten_color(user_color(hi)));
        }
    }

    if strip.label == "Bitrate" {
        if let Some(viz_idx) = viz_idx {
            render_bandwidth_overlay(buf, stride, strip_y, strip_h, panel_x, panel_w, view, ceil, viz_idx);
        }
    }

    abr_draw_hline(buf, stride,
        panel_x as i32, (panel_x + panel_w) as i32,
        (strip_y + strip_h - 1) as i32, 0x2a2a3a);
}

// ─────────────────────────────────────────────────────────────────────────────
// Sidebar: legend + cursor readout
// ─────────────────────────────────────────────────────────────────────────────
fn render_abr_sidebar(
    buf:          &mut [u32],
    stride:       usize,
    height:       usize,
    x_off:        usize,            // ← NEW: 0 for standalone, abr_left for unified
        y_start:      usize,
    sidebar_w:    usize,
    idx:          &AbrVizIndex,
    view:         &AbrViewState,
    highlight_ip: Option<usize>,
    max_y:        Option<usize>,
) {
    abr_fill_rect(buf, stride, x_off, 0, sidebar_w, height, 0x14141c);
    render_text(buf, "ABR METRICS", x_off + 8, 8, stride, 0xffffff, 2.0);


    let mut ly = y_start;   // ← was hardcoded 52
    for (ip_idx, ip) in idx.ip_order.iter().enumerate() {
        let mode  = idx.abr_mode_label.get(ip).map(|s| s.as_str()).unwrap_or("?");
        let base  = user_color(ip_idx);
        let color = match (highlight_ip, highlight_ip.map_or(false, |h| h == ip_idx)) {
            (Some(_), true)  => brighten_color(base),
            (Some(_), false) => dim_color(base, 5),
            (None,    _)     => base,
        };
        let text_col = if highlight_ip.map_or(false, |h| h == ip_idx) { 0xffffff }
                       else if highlight_ip.is_some() { 0x666677 }
                       else { 0xdddddd };

        abr_fill_rect(buf, stride, x_off + 8, ly + 1, 12, 7, color);
        let ip_s = format!("{}", ip);
        render_text(buf, &ip_s, x_off + 26, ly, stride, text_col, 1.0);
        render_text(buf, mode, x_off + 26 + text_w(&ip_s, 1.0) as usize + 8, ly, stride, 0x888899, 1.0);

        if let Some(limit) = max_y {
            if ly + ABR_LEGEND_ITEM_H > limit { break; }
        }
        ly += ABR_LEGEND_ITEM_H;
    }

    ly += 8;
    render_text(buf, &format!("t = {:.3}s", view.cursor_t),
                x_off + 8, ly, stride, 0xaaaaaa, 2.0);
    ly += 18;

    // ── Cursor values table ───────────────────────────────────────────────────────
    ly += 2;

    // Column x-positions relative to x_off
    // After — table_w fills the sidebar; RTT and FLR scale with it
    const C_SWATCH: usize = 8;
    const C_MBPS:   usize = 26;
    const ROW_H:    usize = 16;
    let table_w = sidebar_w.saturating_sub(8);   // 4 px margin each side
    let c_rtt   = table_w * 42 / 100;            // 42 % across
    let c_flr   = table_w * 69 / 100;            // 69 % across

    // ── Header row ────────────────────────────────────────────────────────────────
    abr_fill_rect(buf, stride, x_off + 4, ly, table_w, ROW_H, 0x1c1c2c);
    render_text(buf, "Mbps", x_off + C_MBPS, ly + 4, stride, 0x6677aa, 1.0);
    render_text(buf, "RTT",  x_off + c_rtt,  ly + 4, stride, 0x6677aa, 1.0);
    render_text(buf, "FLR",  x_off + c_flr,  ly + 4, stride, 0x6677aa, 1.0);
    ly += ROW_H;

    // Header / data separator
    abr_draw_hline(buf, stride,
        (x_off + 4) as i32, (x_off + 4 + table_w) as i32,
        ly as i32, 0x2a3a4a);
    ly += 2;

    // ── One row per IP ────────────────────────────────────────────────────────────
    for (ip_idx, ip) in idx.ip_order.iter().enumerate() {
        let base      = user_color(ip_idx);
        let is_hi     = highlight_ip.map_or(true, |h| h == ip_idx);
        let swatch_c  = if is_hi { base } else { dim_color(base, 4) };
        let val_c     = if is_hi { 0xccccdd_u32 } else { 0x444455 };

        // ── Data queries (unchanged logic) ────────────────────────────────────────
        let (rtt_ms, flr, _tp) = idx.by_ip_frame.get(ip)
            .and_then(|indices| {
                let pos = indices.partition_point(
                    |&i| abr_event_t(&idx.events[i]) <= view.cursor_t);
                if pos == 0 { return None; }
                match &idx.events[indices[pos - 1]] {
                    AbrEvent::FrameMetrics { rtt_ms, flr, peak_throughput_mbps, .. } =>
                        Some((*rtt_ms, *flr, *peak_throughput_mbps)),
                    _ => None,
                }
            })
            .unwrap_or((0.0, 0.0, 0.0));

        let bitrate_mbps = idx.by_ip_bitrate.get(ip)
            .and_then(|indices| {
                let pos = indices.partition_point(
                    |&i| abr_event_t(&idx.events[i]) <= view.cursor_t);
                if pos == 0 { return None; }
                match &idx.events[indices[pos - 1]] {
                    AbrEvent::BitrateUpdate { new_bitrate_mbps, .. } => Some(*new_bitrate_mbps),
                    _ => None,
                }
            })
            .unwrap_or(0.0);

        // ── Row background (alternating) ──────────────────────────────────────────
        let row_bg = if ip_idx % 2 == 0 { 0x14141e } else { 0x111118 };
        abr_fill_rect(buf, stride, x_off + 4, ly, table_w, ROW_H, row_bg);

        // Left accent bar — 3 px wide, full row height, in the IP's color
        abr_fill_rect(buf, stride, x_off + 4, ly, 3, ROW_H, swatch_c);

        // Color swatch square
        abr_fill_rect(buf, stride, x_off + C_SWATCH, ly + 4, 12, 8, swatch_c);

        // Values — bitrate in the IP colour, RTT/FLR neutral
        render_text(buf, &format!("{:.1}", bitrate_mbps),
                    x_off + C_MBPS, ly + 4, stride, swatch_c, 1.0);
        render_text(buf, &format!("{:.0}ms", rtt_ms),
                    x_off + c_rtt,  ly + 4, stride, val_c,    1.0);
        render_text(buf, &format!("{:.3}", flr),
                    x_off + c_flr,  ly + 4, stride, val_c,    1.0);

        ly += ROW_H;
    }

    // Table bottom border
    abr_draw_hline(buf, stride,
        (x_off + 4) as i32, (x_off + 4 + table_w) as i32,
        ly as i32, 0x2a3a4a);
}


// ─────────────────────────────────────────────────────────────────────────────
// HUD: time axis ticks at bottom of panel
// ─────────────────────────────────────────────────────────────────────────────
/// A time-axis tick, in seconds, with the decimals the tick step needs to tell neighbours apart: two at least, more once
/// zoomed in (a step of 0.0002 s shows four).
fn tick_label(t: f64, step: f64) -> String {
    let decimals = if step.is_finite() && step > 0.0 { (-(step.log10() + 1e-9).floor()).clamp(2.0, 12.0) as usize } else { 2 };
    format!("{:.*}s", decimals, t)
}

fn render_abr_time_axis(
    buf:     &mut [u32],
    stride:  usize,
    y:       usize,
    panel_x: usize,
    panel_w: usize,
    view:    &AbrViewState,
) {
    abr_fill_rect(buf, stride, panel_x, y, panel_w, 20, 0x0d0d12);
    let t0  = view.center_t - view.span_t * 0.5;
    let t1  = view.center_t + view.span_t * 0.5;

    // Pick a round tick interval
    let raw_step = view.span_t / 8.0;
    let magnitude = 10f64.powf(raw_step.log10().floor());
    let step = (raw_step / magnitude).ceil() * magnitude;
    if step <= 0.0 { return; }

    let first = (t0 / step).ceil() * step;
    let mut t  = first;
    while t <= t1 {
        let x = abr_x_of(t, view, panel_x, panel_w);
        abr_draw_vline(buf, stride, x, y as i32, (y + 6) as i32, 0x555566);
        render_text(buf, &tick_label(t, step), x as usize + 2, y + 4, stride, 0x888899, TEXT_SIZE_ABR);
        t += step;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Input handling (mirrors the VizEvent viewer)
// ─────────────────────────────────────────────────────────────────────────────
fn handle_abr_input(
    window:  &Window,
    view:    &mut AbrViewState,
    idx:     &AbrVizIndex,
    panel_w: usize,
    panel_x: usize,
) {
    if window.is_key_down(Key::Left)  || window.is_key_down(Key::A) { view.center_t -= view.span_t * 0.05; }
    if window.is_key_down(Key::Right) || window.is_key_down(Key::D) { view.center_t += view.span_t * 0.05; }

    if window.is_key_pressed(Key::Equal,      minifb::KeyRepeat::Yes)
    || window.is_key_pressed(Key::NumPadPlus, minifb::KeyRepeat::Yes) { view.span_t *= 0.8; }
    if window.is_key_pressed(Key::Minus,       minifb::KeyRepeat::Yes)
    || window.is_key_pressed(Key::NumPadMinus, minifb::KeyRepeat::Yes) { view.span_t *= 1.25; }

    if window.is_key_pressed(Key::Home, minifb::KeyRepeat::No) {
        view.center_t = idx.t_min + view.span_t * 0.5;
    }
    if window.is_key_pressed(Key::End, minifb::KeyRepeat::No) {
        view.center_t = idx.t_max - view.span_t * 0.5;
    }

    let (mx, _my) = ui_scale::mouse_pos(window).unwrap_or((0.0, 0.0));

    // Left drag: pan
    if window.get_mouse_down(MouseButton::Left) {
        if let Some((mx0, ct0)) = view.mouse_drag {
            let dx_px    = (mx - mx0) as f64;
            let dt_per_px = view.span_t / panel_w as f64;
            view.center_t = ct0 - dx_px * dt_per_px;
        } else {
            view.mouse_drag = Some((mx, view.center_t));
        }
    } else {
        view.mouse_drag = None;
    }

    // Right click: move cursor
    if window.get_mouse_down(MouseButton::Right) {
        let n = ((mx as f64) - panel_x as f64) / panel_w as f64;
        view.cursor_t = (view.center_t - view.span_t * 0.5 + view.span_t * n)
            .clamp(idx.t_min, idx.t_max);
    }

    // Scroll: zoom around mouse
    if let Some((_, scroll_y)) = window.get_scroll_wheel() {
        if scroll_y.abs() > 0.0 {
            let factor = if scroll_y > 0.0 { 0.85 } else { 1.18 };
            let n = ((mx as f64) - panel_x as f64) / panel_w as f64;
            let n = n.clamp(0.0, 1.0);
            let t_anchor = view.center_t - view.span_t * 0.5 + view.span_t * n;
            view.span_t   *= factor;
            view.center_t  = t_anchor - view.span_t * (n - 0.5);
        }
    }

    // Clamp
    let full_range = (idx.t_max - idx.t_min).max(0.001);
    view.span_t = view.span_t.max(1e-3).min(full_range * 2.0); // not `clamp`: it panics when the range is under 1 ms

    view.center_t = if view.span_t >= full_range {
        (idx.t_min + idx.t_max) * 0.5
    } else {
        view.center_t.clamp(
            idx.t_min + view.span_t * 0.5,
            idx.t_max - view.span_t * 0.5,
        )
    };

}

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────
const STA_GRID_H: usize = 220; // height reserved for trajectory panel

/// `close_requested`, if given, is polled each frame alongside the window's own
/// close button / Escape -- lets a caller that reopens this viewer across repeated
/// runs (e.g. the real-mode server's loop-back-to-menu) close a still-open previous
/// window itself instead of letting old graphs pile up window after window. `None`
/// (the simulated-mode call site) behaves exactly as before: open until the user
/// closes it by hand.
pub fn run_abr_viewer(
    idx: AbrVizIndex,
    sim_tag: &str,
    close_requested: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
) {
    let mut window = Window::new(
        "ABR Metrics Viewer",
        ABR_W, ABR_H + STA_GRID_H,          // ← taller initial window
        WindowOptions {
            resize:     true,
            scale_mode: minifb::ScaleMode::Stretch,
            ..WindowOptions::default()
        },
    ).expect("Failed to open ABR viewer window");
    let mut ui = ui_scale::UiScale::new();

    let mut buf = vec![0u32; ABR_W * (ABR_H + STA_GRID_H)];

    let strips   = make_strips(&idx);
    let n_strips = strips.len();
    let full_range = (idx.t_max - idx.t_min).max(0.001);

    let mut view = AbrViewState {
        center_t:   (idx.t_min + idx.t_max) * 0.5,
        span_t:     full_range,
        cursor_t:   idx.t_min,
        mouse_drag: None,
    };

    let mut screenshot_taken = false;

    while window.is_open()
        && !window.is_key_down(Key::Escape)
        && !close_requested.as_ref().is_some_and(|f| f.load(std::sync::atomic::Ordering::Relaxed))
    {
        let (w, h) = ui.frame(&window); // logical size: the window over the UI scale
        if buf.len() != w * h { buf.resize(w * h, 0); }

        // ── Layout ───────────────────────────────────────────────────────────
        let panel_x      = SIDEBAR_W;
        let panel_w      = w.saturating_sub(SIDEBAR_W + 4);
        let time_axis_h  = 22usize;
        let sta_grid_h   = STA_GRID_H;

        // strips occupy everything above time-axis and STA grid
        let usable_h = h.saturating_sub(time_axis_h + PANEL_PAD + sta_grid_h);
        let strip_h  = (usable_h / n_strips).max(1);

        let time_axis_y  = PANEL_PAD + n_strips * strip_h;
        let sta_grid_y   = time_axis_y + time_axis_h;

        // ── Input + mouse ────────────────────────────────────────────────────
        handle_abr_input(&window, &mut view, &idx, panel_w, panel_x);

        let (mx, my)     = ui_scale::mouse_pos(&window).unwrap_or((0.0, 0.0));
        let highlight_ip = hit_test_abr_legend(mx, my, &idx, SIDEBAR_W, 52);

        buf.fill(0x0d0d12);

        // ── Metric strips ────────────────────────────────────────────────────
        for (si, strip) in strips.iter().enumerate() {
            let strip_y = PANEL_PAD + si * strip_h;
            render_abr_strip(
                &mut buf, w, strip, strip_y, strip_h,
                panel_x, panel_w, &view, &idx, highlight_ip, None,
            );
        }

        // ── Time axis ────────────────────────────────────────────────────────
        render_abr_time_axis(&mut buf, w, time_axis_y, panel_x, panel_w, &view);

        // ── STA trajectory grid ──────────────────────────────────────────────
        if !idx.by_ip_sta.is_empty() {
            render_sta_grid(
                &mut buf, w, panel_x, panel_w,
                sta_grid_y, sta_grid_h,
                &idx, view.cursor_t,
            );
        }

        // ── Sidebar (covers full height) ─────────────────────────────────────

        // render_abr_sidebar(&mut buf, w, h, &idx, &view, highlight_ip, None );
        // render_abr_sidebar(&mut buf, w, h, 0, &idx, &view, highlight_ip, None);
        render_abr_sidebar(&mut buf, w, h, 0, 52, SIDEBAR_W, &idx, &view, highlight_ip, None);
        // ── Cursor line (strips + time axis only) ────────────────────────────
        let cx = abr_x_of(view.cursor_t, &view, panel_x, panel_w);
        if cx >= panel_x as i32 && cx < (panel_x + panel_w) as i32 {
            abr_draw_vline(
                &mut buf, w, cx,
                PANEL_PAD as i32,
                (time_axis_y + time_axis_h) as i32,
                0xffff66,
            );
        }

        window.update_with_buffer(&buf, w, h).unwrap();

        if !screenshot_taken {
            save_screenshot(&buf, w, h, sim_tag, "abr");
            screenshot_taken = true;
        }
    }
}

// ##########################################################################################################################################################
// ##########################################################################################################################################################
// ##########################################################################################################################################################
// ##########################################################################################################################################################
// ##########################################################################################################################################################
// ##########################################################################################################################################################

// END ABR VIEWER
// #############################################################################
/////////////////////////////////////// CHANNEL VIEWER /////////////////////////////////////////

// ##########################################################################################################################################################
// ##########################################################################################################################################################
// ##########################################################################################################################################################
// ##########################################################################################################################################################

use std::collections::{HashSet};

use crate::lib::{PREFIX_ID_UPLINK, MacKey, PREFIX_ID_DOWNLINK, EdcaAc,
                models_mm1k::LinkConfig,
                alvr_stream_socket::FRAMELOSS_PACKET, };
use viz_event::VizEvent;

/// Dim a colour by `factor` (integer division per channel, no bleed).
fn dim_color(color: u32, factor: u32) -> u32 {
    let r = ((color >> 16) & 0xFF) / factor;
    let g = ((color >> 8)  & 0xFF) / factor;
    let b = ( color        & 0xFF) / factor;
    (r << 16) | (g << 8) | b
}

// Blend base color toward full brightness — adjust the fraction freely
fn mid_color(color: u32, frac: f32) -> u32 {
    let ch = |c: u32| -> u32 {
        let boosted = (c as f32 * frac) as u32;
        boosted.min(255)
    };
    let r = ch((color >> 16) & 0xFF);
    let g = ch((color >>  8) & 0xFF);
    let b = ch( color        & 0xFF);
    (r << 16) | (g << 8) | b
}
 
/// Return `color` dimmed ×5 when `dimmed` is true, unchanged otherwise.
#[inline]
fn maybe_dim(color: u32, dimmed: bool) -> u32 {
    if dimmed { dim_color(color, 5) } else { color }
}
 
/// Is this MacKey fully represented in the highlight set?
/// `None` highlight = everything is full-bright.
#[inline]
fn is_key_highlighted(key: &MacKey, highlight: &Option<ActiveHighlights>) -> bool {
    match highlight {
        None => true, // Default to bright if nothing is hovered
        Some(h) => h.keys.contains(key),
    }
}

fn collect_highlights_at(t: f64, link_id: u8, idx: &VizIndex, h: &mut ActiveHighlights) {
    // Check TXOPs
    if let Some(indices) = idx.txops_by_link.get(&link_id) {
        for &ii in indices {
            let ev = &idx.all[ii];
            if let VizEvent::TxopStart { t: start, end, sender, dest_id, .. } = ev {
                if t >= *start && t <= *end {
                    h.keys.insert(*sender);
                    h.txops.insert((*sender, *dest_id));
                }
            }
        }
    }
    // Check Collisions
    if let Some(indices) = idx.collisions_by_link.get(&link_id) {
        for &ii in indices {
            let ev = &idx.all[ii];
            if let VizEvent::Collision { t: start, end, contenders, .. } = ev {
                if t >= *start && t <= *end {
                    for &c in contenders {
                        h.keys.insert(c);
                    }
                }
            }
        }
    }
}

fn find_highlight_from_cursor(view: &ViewState, idx: &VizIndex) -> Option<ActiveHighlights> {
    let mut h = ActiveHighlights {
        keys: HashSet::new(),
        txops: HashSet::new(),
    };

    for &link_id in &[0u8, 1u8] {
        collect_highlights_at(view.cursor_t, link_id, idx, &mut h);
    }

    if h.keys.is_empty() {
        None
    } else {
        Some(h)
    }
}
fn find_highlight_from_mouse(
    mx: f32, my: f32,
    view: &ViewState, idx: &VizIndex,
    panel_x: usize, panel_w: usize,
) -> Option<ActiveHighlights> {
    if (mx as usize) < panel_x { return None; }

    let t0 = view.center_t - view.span_t * 0.5;
    let n = ((mx as f64) - panel_x as f64) / panel_w as f64;
    let t_mouse = t0 + n * view.span_t;

    const LANE_H: usize = 50;
    let mut lane_y = 60usize;
    
    for &link_id in &[0u8, 1u8] {
        let my_u = my as usize;
        if my_u >= lane_y && my_u < lane_y + LANE_H {
            let mut h = ActiveHighlights {
                keys: HashSet::new(),
                txops: HashSet::new(),
            };
            
            collect_highlights_at(t_mouse, link_id, idx, &mut h);
            
            if !h.keys.is_empty() {
                return Some(h);
            }
        }
        lane_y += LANE_H + 5;
    }
    None
}
 
// ----------------------------------------------------------------
// ViewState  (unchanged struct, shown for context)
// ----------------------------------------------------------------
pub struct ViewState {
    pub center_t:    f64,
    pub span_t:      f64,
    pub cursor_t:    f64,
    pub paused:      bool,
    pub selected_link: Option<u8>,
    pub row_scroll:  i32,
    pub mouse_drag:  Option<(f32, f64)>,
    /// AIFS / back-off before each winning TXOP and the PHY preamble in front of each PPDU's data; `O` toggles it.
    pub show_overhead: bool,
    /// Queue panel style, `Q` cycles: 0 lines, 1 one strip per queue, 2 heatmap, 3 stacked area.
    pub q_mode: u8,
    /// Strips share one vertical scale (the largest queue) instead of one scale each; `W` toggles.
    pub q_shared: bool,
    /// First visible row of the queue and CW strip panels (wheel over their label column scrolls).
    pub q_scroll: usize,
    pub cw_scroll: usize,
    /// The CW panel shows the backoff counters of every station and AC as a grid instead of the CW strips; `B` toggles.
    pub show_bo_matrix: bool,
    /// Start of the TXOP under the mouse in a link lane, for marking its win in the CW / queue rows.
    pub hover_t: Option<f64>,
    /// (link, start) of that TXOP, for marking when its MPDUs arrived in the queue rows.
    pub hover_txop: Option<(u8, f64)>,
    /// A band at the bottom of each queue strip marks the packet arrivals; `I` toggles it.
    pub show_arrivals: bool,
    /// (link, start) of the collision under the mouse in a link lane, and the A-MPDU (`TxopStart` index) whose tile is under
    /// it, for the station map's arrows.
    pub hover_coll: Option<(u8, f64)>,
    pub hover_leg: Option<usize>,
}
 fn brighten_color(color: u32) -> u32 {
    let r = (((color >> 16) & 0xFF) * 5 / 4).min(255);
    let g = (((color >>  8) & 0xFF) * 5 / 4).min(255);
    let b = (( color        & 0xFF) * 5 / 4).min(255);
    (r << 16) | (g << 8) | b
}
// ----------------------------------------------------------------
// run_viewer  – now queries mouse pos and builds highlight each frame
// ----------------------------------------------------------------
pub fn run_viewer(idx: VizIndex, link_configs: &[LinkConfig], sim_tag: &str) {
    const W: usize = 1500;
    const H: usize = 900;


    let mut window = Window::new(
        "WLAN Sim Playback",
        W, H,
        WindowOptions {
            resize:     true,
            scale_mode: minifb::ScaleMode::Stretch,
            ..WindowOptions::default()
        },
    ).expect("Failed to open viewer window");
    let mut ui = ui_scale::UiScale::new();

    let mut buf = vec![0u32; W * H];
    let mut view = ViewState {
        center_t:      (idx.t_min + idx.t_max) * 0.5,
        span_t:        ((idx.t_max - idx.t_min) * 0.05).max(0.001),
        cursor_t:      idx.t_min,
        paused:        false,
        selected_link: None,
        row_scroll:    0,
        mouse_drag:    None,
        show_overhead: true,
        q_mode:        1,
        q_shared:      false,
        q_scroll:      0,
        cw_scroll:     0,
        show_bo_matrix: false,
        hover_t:       None,
        hover_txop:    None,
        show_arrivals: true,
        hover_coll: None,
        hover_leg: None,
    };

    let mut screenshot_taken = false;

    while window.is_open() && !window.is_key_down(Key::Escape) {
        // ── live dimensions ───────────────────────────────────────────────
        let (w, h) = ui.frame(&window); // logical size: the window over the UI scale
        if buf.len() != w * h {
            buf.resize(w * h, 0);
        }

        // ── layout derived from live size ─────────────────────────────────
        let panel_x = 270;
        let panel_w = w.saturating_sub(panel_x + 20);

        // ── input ─────────────────────────────────────────────────────────
        let (mx, my) = ui_scale::mouse_pos(&window).unwrap_or((0.0, 0.0));

        handle_input(&window, &mut view, &idx, panel_w, panel_x, link_configs.len() as u8);
        clamp_view(&mut view, &idx);

        let highlight_keys =
            hit_test_qdepth_legend(mx, my, h - 180, 160, panel_x, &view, &idx);

        // ── render ────────────────────────────────────────────────────────
        buf.fill(0x0d0d12);
        render_hud(&mut buf, w, &view, &idx);

        let mut lane_y = 60usize;
        let mut lanes: Vec<(usize, usize, u8)> = Vec::new();

        for (link_id_usize, link_config) in link_configs.iter().enumerate() {
            let link_id  = link_id_usize as u8;
            let bw_link  = link_config.bandwidth_mhz;
            let lane_h   = lane_height(&idx, bw_link);
            render_link_lane(
                &mut buf, w, lane_y, lane_h,
                panel_x, panel_w, &view, &idx, link_id, bw_link,
                &highlight_keys,
            );
            lanes.push((lane_y, lane_h, link_id));
            lane_y += lane_h + 5;
        }

        let rows_top = lane_y + 10;
        let rows_bottom = h.saturating_sub(200);
        let rows_top    = rows_top.min(rows_bottom);
        render_mackey_rows(
            &mut buf, w, rows_top, rows_bottom,
            panel_x, panel_w, &view, &idx,
            &None,
        );

        let cw_panel_y = rows_bottom + 4;
        render_cw_panel(
            &mut buf, w, cw_panel_y, CW_H,
            panel_x, panel_w, &view, &idx,
            &highlight_keys,           // shares the same hover highlight
        );

        render_qdepth_panel(
            &mut buf, w, h - 180, 160,
            panel_x, panel_w, &view, &idx,
            &highlight_keys,
        );

        // cursor line
        let cursor_x = x_of(view.cursor_t, &view, panel_x, panel_w);
        if cursor_x >= panel_x as i32 && cursor_x <= (panel_x + panel_w) as i32 {
            draw_vline(&mut buf, w, cursor_x, 50, (h - 20) as i32, 0xffff66);
        }
        render_hover_tooltip(&mut buf, w, mx, my, &lanes, panel_x, panel_w, &view, &idx);

        window.update_with_buffer(&buf, w, h).unwrap();

        if !screenshot_taken {
            save_screenshot(&buf, w, h, sim_tag, "channel");
            screenshot_taken = true;
        }
    }
}


/// Name of an AP's station id (`-1`).
fn ap_name(id: i32) -> String {
    if id == -1 { "AP".to_string() } else { format!("AP{}", id) }
}

fn is_uplink(dest_id: i32) -> bool {
    dest_id > PREFIX_ID_DOWNLINK && dest_id < PREFIX_ID_UPLINK   // AP is always STA-id 0; adjust if your topology differs
}

/// Left edge of the CURSOR readout in the controls bar.
fn cursor_text_x() -> usize { SLIDER_X1 + 16 + text_w("000.00 ms/s", 2.0) as usize + 40 }

/// Right edge of the widest CURSOR readout: what the tabs' hint must stay clear of.
pub(crate) fn cursor_text_end() -> usize { cursor_text_x() + text_w("CURSOR 000.000000 s", 2.0) as usize }

fn render_controls_bar(buf: &mut [u32], stride: usize, state: &UnifiedState) {
    fill_rect(buf, stride, 0, HUD_H, stride, CONTROLS_H, 0x161620);
    for x in 0..stride {
        let i = (HUD_H + CONTROLS_H - 1) * stride + x;
        if i < buf.len() { buf[i] = 0x2a2a40; }
    }

    // a bordered button with its label centred
    let button = |buf: &mut [u32], bx: usize, bw: usize, label: &str, bg: u32, border: u32, ink: u32| {
        fill_rect(buf, stride, bx, BTN_Y, bw, BTN_H, bg);
        abr_draw_hline(buf, stride, bx as i32, (bx + bw) as i32, BTN_Y as i32, border);
        abr_draw_hline(buf, stride, bx as i32, (bx + bw) as i32, (BTN_Y + BTN_H) as i32, border);
        abr_draw_vline(buf, stride, bx as i32, BTN_Y as i32, (BTN_Y + BTN_H) as i32, border);
        abr_draw_vline(buf, stride, (bx + bw) as i32, BTN_Y as i32, (BTN_Y + BTN_H) as i32, border);
        let tw = text_w(label, 2.0) as usize;
        render_text(buf, label, bx + bw.saturating_sub(tw) / 2, BTN_Y + (BTN_H - 14) / 2, stride, ink, 2.0);
    };
    let playing = state.playing;
    button(buf, BTN_PLAY_X, BTN_PLAY_W, if playing { "PAUSE" } else { "PLAY" },
           if playing { 0x1a3a1a } else { 0x1a1a3a }, 0x4488aa, 0xddeeff);
    button(buf, BTN_RESET_X, BTN_RESET_W, "REWIND", 0x2a1a1a, 0xaa4444, 0xffcccc);

    // speed: label, log-scale slider with decade ticks, value
    render_text(buf, "SPEED", SLIDER_X0 - 80, BTN_Y + (BTN_H - 14) / 2, stride, 0xaabbcc, 2.0);
    let slider_w = SLIDER_X1 - SLIDER_X0;
    let track_y  = BTN_Y + 8;
    let frac     = speed_to_frac(state.playback_speed);
    fill_rect(buf, stride, SLIDER_X0, track_y, slider_w, 4, 0x252535);
    let filled_w = ((frac * slider_w as f64) as usize).min(slider_w);
    fill_rect(buf, stride, SLIDER_X0, track_y, filled_w, 4, 0x3355aa);
    // one tick + label per decade of the log scale
    let mut dec = SPEED_MIN;
    while dec <= SPEED_MAX * 1.0001 {
        let x = SLIDER_X0 + (speed_to_frac(dec) * slider_w as f64) as usize;
        fill_rect(buf, stride, x, track_y + 4, 1, 4, 0x556677);
        let lbl = if dec < 1e-3 { format!("{:.0}us", dec * 1e6) } else if dec < 1.0 { format!("{:.0}ms", dec * 1e3) } else { "1s".to_string() };
        render_text(buf, &lbl, (x + 1).saturating_sub(text_w(&lbl, 1.0) as usize / 2), BTN_Y + 17, stride, 0x667788, 1.0);
        dec *= 10.0;
    }
    let thumb_x = SLIDER_X0 + (frac * slider_w as f64) as usize;
    fill_rect(buf, stride, thumb_x.saturating_sub(4), BTN_Y + 3, 9, 14, 0x7799ff);
    abr_draw_vline(buf, stride, thumb_x as i32, (BTN_Y + 3) as i32, (BTN_Y + 17) as i32, 0xddeeff);
    render_text(buf, &fmt_speed(state.playback_speed), SLIDER_X1 + 16, BTN_Y + (BTN_H - 14) / 2, stride, 0xeeeeff, 2.0);

    // where the playback cursor is
    let cur = format!("CURSOR {:.6} s", state.ch_view.cursor_t);
    let cx = cursor_text_x();
    if cx + (text_w(&cur, 2.0) as usize) < stride.saturating_sub(RESET_BTN_W + ZOOM_BTN_W + 40) {
        render_text(buf, &cur, cx, BTN_Y + (BTN_H - 14) / 2, stride, 0xffee88, 2.0);
    }
}


// ----------------------------------------------------------------
// render_link_lane  – one link over time, dims or highlights each A-MPDU / collision bar. A lone
// A-MPDU fills its TXOP; several in one TXOP sit inside a frame marking the whole TXOP.
// ----------------------------------------------------------------

const LANE_BG: u32 = 0x14141c;
/// Link lane tones: nothing on the air / airtime held by a TXOP but not carrying payload (protection, SIFS).
const IDLE_BG: u32 = 0x040407;
const BUSY_BG: u32 = 0x464c62;
const DL_BE_COLOR: u32 = 0x4fc3f7; // cool blue  → downlink (AP → STA)
const UL_BE_COLOR: u32 = 0xffb74d; // warm amber → uplink   (STA → AP)

/// Colour of an A-MPDU: red for a frame-loss notice, else by AC, Best Effort split by direction
/// so lanes are readable at a glance.
fn txop_base_color(sender: MacKey, dest_id: i32, stream_ids: &[u16]) -> u32 {
    if stream_ids.contains(&FRAMELOSS_PACKET) {
        return 0xff0000;
    }
    match sender.1 {
        EdcaAc::BestEffort => if is_uplink(dest_id) { UL_BE_COLOR } else { DL_BE_COLOR },
        ac => ac_color(ac),
    }
}

/// Colour of an EDCAF (a TXOP holder): by AC, Best Effort split into the AP's and a STA's.
fn key_color(k: MacKey) -> u32 {
    match k.1 {
        EdcaAc::BestEffort => if k.0 < 0 { DL_BE_COLOR } else { UL_BE_COLOR },
        ac => ac_color(ac),
    }
}

/// A STA's own colour, the same in both directions and on every link.
fn sta_color(sta: i32) -> u32 {
    user_color(sta.rem_euclid(1000) as usize)
}

/// Black or white, whichever reads on `bg`.
fn text_on(bg: u32) -> u32 {
    let (r, g, b) = ((bg >> 16) & 0xff, (bg >> 8) & 0xff, bg & 0xff);
    if 299 * r + 587 * g + 114 * b > 140_000 { 0x000000 } else { 0xffffff }
}

/// `base` brightened when highlighted, dimmed when something else is.
fn highlight_color(base: u32, lit: bool, any_highlight: bool) -> u32 {
    match (lit, any_highlight) {
        (true, true) => brighten_color(base),
        (true, false) => base,
        (false, _) => dim_color(base, 5),
    }
}

/// Fills `[x0, x1) × [y0, y1)` (opaque), clipped to the buffer.
fn fill_rect_clip(buf: &mut [u32], stride: usize, x0: i32, y0: i32, x1: i32, y1: i32, c: u32) {
    let h_buf = (buf.len() / stride) as i32;
    let (x0, x1) = (x0.max(0), x1.min(stride as i32));
    let (y0, y1) = (y0.max(0), y1.min(h_buf));
    if x1 <= x0 {
        return;
    }
    for y in y0..y1 {
        let row = y as usize * stride;
        buf[row + x0 as usize..row + x1 as usize].fill(c);
    }
}

/// `[x0, x1) × [y0, y1)` hatched with diagonal stripes (unallocated tones).
fn hatch_rect_clip(buf: &mut [u32], stride: usize, x0: i32, y0: i32, x1: i32, y1: i32, bg: u32, stripe: u32) {
    let h_buf = (buf.len() / stride) as i32;
    for y in y0.max(0)..y1.min(h_buf) {
        for x in x0.max(0)..x1.min(stride as i32) {
            buf[y as usize * stride + x as usize] = if (x + y).rem_euclid(6) == 0 { stripe } else { bg };
        }
    }
}

/// 1-px border of `[x0, x1) × [y0, y1)`.
fn outline_rect_clip(buf: &mut [u32], stride: usize, x0: i32, y0: i32, x1: i32, y1: i32, c: u32) {
    fill_rect_clip(buf, stride, x0, y0, x1, y0 + 1, c);
    fill_rect_clip(buf, stride, x0, y1 - 1, x1, y1, c);
    fill_rect_clip(buf, stride, x0, y0, x0 + 1, y1, c);
    fill_rect_clip(buf, stride, x1 - 1, y0, x1, y1, c);
}

/// Pixel width of `s` in `render_text` at `scale` (5-px glyphs + 1-px spacing).
fn text_w(s: &str, scale: f32) -> i32 {
    (s.chars().count() as f32 * 6.0 * scale).ceil() as i32
}

/// The frame ids worth printing for an A-MPDU: lost frames for a frame-loss report, the frame ids of video, and for
/// any other traffic only ids that are really set (traffic without a frame id carries 0, which is not worth a label).
fn frames_label(stream_ids: &[u16], frame_ids: &[u32], frame_losses: &Option<Vec<u32>>) -> Option<(&'static str, Vec<u32>)> {
    if stream_ids.contains(&FRAMELOSS_PACKET) {
        return Some(("lost frames", frame_losses.clone().unwrap_or_default()));
    }
    let real = stream_ids.contains(&crate::lib::alvr_stream_socket::VIDEO) || frame_ids.iter().any(|&f| f != 0);
    (real && !frame_ids.is_empty()).then(|| ("frames", frame_ids.to_vec()))
}

/// The first of `candidates` (longest first) that fits in `px` pixels at `scale`.
fn pick_text<'a>(candidates: &'a [String], px: i32, scale: f32) -> Option<&'a str> {
    candidates.iter().find(|s| text_w(s, scale) + 4 <= px).map(|s| s.as_str())
}

/// Screen rectangle `[x0, x1) × [y0, y1)` of (one frequency run of) an A-MPDU in a lane.
#[derive(Clone, Copy)]
struct LegRect { ev: usize, x0: i32, x1: i32, y0: i32, y1: i32 }

/// Time window `ev`'s PPDU is on air (data symbols, no preamble): a lone A-MPDU fills its TXOP.
fn ppdu_window(g: &TxopGroup, idx: &VizIndex, ev: usize) -> (f64, f64) {
    let VizEvent::TxopStart { ppdu_t0, ppdu_t1, .. } = &idx.all[ev] else { return (g.t, g.end) };
    if g.is_plain_su() || *ppdu_t1 <= *ppdu_t0 {
        return (g.t, g.end);
    }
    (*ppdu_t0, *ppdu_t1)
}

/// Where the A-MPDUs of `g` go in a lane band `[top, top + h)`: x over their PPDU's data symbols (the whole TXOP for a
/// lone A-MPDU), y over the whole band. Clipped to the panel in x.
fn leg_rects(g: &TxopGroup, idx: &VizIndex, view: &ViewState, panel_x: usize, panel_w: usize, top: i32, h: i32) -> Vec<LegRect> {
    let (px0, px1) = (panel_x as i32, (panel_x + panel_w) as i32);
    let plain = g.is_plain_su();
    let mut out = Vec::new();
    for &ev in &g.legs {
        let VizEvent::TxopStart { .. } = &idx.all[ev] else { continue };
        let (t0, t1) = ppdu_window(g, idx, ev);
        let (x0, x1) = (x_of(t0, view, panel_x, panel_w), x_of(t1, view, panel_x, panel_w));
        if x1 < px0 || x0 >= px1 {
            continue;
        }
        let (x0, x1) = (x0.max(px0), x1.min(px1));
        // a PPDU inside a wider TXOP frame stays visible as a 1-px sliver when zoomed out
        let x1 = if plain { x1 } else { x1.max(x0 + 1) };
        if x1 <= x0 {
            continue;
        }
        out.push(LegRect { ev, x0, x1, y0: top, y1: top + h });
    }
    out
}

/// The STA an A-MPDU serves: its destination for an AP transmission, else its sender.
fn leg_user(sender: MacKey, dest_id: i32) -> i32 {
    if sender.0 < 0 { dest_id } else { sender.0 }
}

fn render_link_lane(
    buf: &mut [u32], stride: usize,
    lane_y: usize, lane_h: usize,
    panel_x: usize, panel_w: usize,
    view: &ViewState, idx: &VizIndex, link_id: u8,
    bandwidth_mhz: u16,
    highlight: &Option<ActiveHighlights>,
) {
    fill_rect(buf, stride, panel_x, lane_y, panel_w, lane_h, IDLE_BG);
    // a faint dot grid marks empty air; anything drawn over it (busy time, packets) hides it
    for yy in (lane_y + 2..lane_y + lane_h).step_by(6) {
        for xx in (panel_x + 2..panel_x + panel_w).step_by(6) {
            let i = yy * stride + xx;
            if i < buf.len() { buf[i] = 0x1c1c28; }
        }
    }

    let t_lo = view.center_t - view.span_t * 0.5;
    let t_hi = view.center_t + view.span_t * 0.5;
    let (px0, px1) = (panel_x as i32, (panel_x + panel_w) as i32);
    let (top, h) = ((lane_y + 2) as i32, lane_h as i32 - 4);
    let any_highlight = highlight.is_some();

    if let Some(groups) = idx.txop_groups_by_link.get(&link_id) {
        let start = groups.partition_point(|g| g.end < t_lo);
        for g in &groups[start..] {
            if g.t > t_hi { break; }
            let plain = g.is_plain_su();

            // Frame of a TXOP holding several PPDUs: the time not covered by data symbols
            // is protection and BlockAcks.
            let frame = (!plain).then(|| {
                let (x0, x1) = (x_of(g.t, view, panel_x, panel_w), x_of(g.end, view, panel_x, panel_w));
                (x0.max(px0), x1.min(px1).max(x0.max(px0) + 1))
            }).filter(|&(x0, _)| x0 < px1);
            if let Some((fx0, fx1)) = frame {
                let hc = key_color(g.holder);
                let lit = is_key_highlighted(&g.holder, highlight);
                fill_rect_clip(buf, stride, fx0, top, fx1, top + h, if lit { BUSY_BG } else { dim_color(BUSY_BG, 2) });
                if fx1 - fx0 >= 3 {
                    outline_rect_clip(buf, stride, fx0, top, fx1, top + h, dim_color(hc, if lit { 2 } else { 5 }));
                }
            }

            let mut labels: Vec<(String, usize, usize, u32)> = Vec::new();
            let rects = leg_rects(g, idx, view, panel_x, panel_w, top, h);
            let mut labelled = usize::MAX;
            for r in &rects {
                let VizEvent::TxopStart {
                    sender, dest_id, ampdu_packets, mcs, alvr_stream_ids, alvr_frame_ids,
                    alvr_frame_losses, ppdu_t0, ppdu_t1, ..
                } = &idx.all[r.ev] else { continue };
                let lit = is_key_highlighted(sender, highlight);
                let user = leg_user(*sender, *dest_id);
                let base = txop_base_color(*sender, *dest_id, alvr_stream_ids);
                let color = highlight_color(base, lit, any_highlight);
                let (w, hh) = (r.x1 - r.x0, r.y1 - r.y0);
                // tiles sharing a TXOP keep a 1-px gap so neighbouring RUs / PPDUs stay apart
                let (gx, gy) = if plain { (0, 0) } else { ((w >= 3) as i32, (hh >= 4) as i32) };
                fill_rect_clip(buf, stride, r.x0, r.y0, r.x1 - gx, r.y1 - gy, color);
                if lit && any_highlight {
                    outline_rect_clip(buf, stride, r.x0, r.y0, r.x1 - gx, r.y1 - gy, 0xffffff);
                }
                // one label per A-MPDU, on its largest run (the first)
                if !lit || labelled == r.ev {
                    continue;
                }
                labelled = r.ev;

                let streams = alvr_stream_ids.iter()
                    .map(|&id| crate::lib::alvr_stream_socket::get_stream_name(id))
                    .collect::<Vec<_>>()
                    .join("+");
                let frames = frames_label(alvr_stream_ids, alvr_frame_ids, alvr_frame_losses);
                let su_line = format!("{:?} STA{}->{} MCS{}", sender.1, sender.0, dest_id, mcs);
                let line2 = match &frames {
                    Some((_, f)) => format!("{}|frames: {:?} ({} MPDUs)", streams, f, ampdu_packets),
                    None => format!("{} ({} MPDUs)", streams, ampdu_packets),
                };
                if plain {
                    // the text shrunk to what fits in the (panel-clipped) rectangle; with the
                    // overhead drawn, only the data symbols are the payload, so the text goes there
                    let (lx0, lx1) = if view.show_overhead && *ppdu_t1 > *ppdu_t0 {
                        (r.x0.max(x_of(*ppdu_t0, view, panel_x, panel_w)), r.x1.min(x_of(*ppdu_t1, view, panel_x, panel_w)))
                    } else {
                        (r.x0, r.x1)
                    };
                    let avail = lx1.min(px1) - lx0 - 2;
                    let l1 = [su_line, format!("STA{}->{} MCS{}", sender.0, dest_id, mcs), format!("s{} MCS{}", user, mcs)];
                    if let Some(s) = pick_text(&l1, avail, 1.0) {
                        labels.push((s.to_string(), lx0 as usize + 4, lane_y + 6, 0x000000));
                        let l2 = [line2, format!("{} ({} MPDUs)", streams, ampdu_packets), streams.clone()];
                        if let Some(s) = pick_text(&l2, avail, 1.0) {
                            labels.push((s.to_string(), lx0 as usize + 4, lane_y + 18, 0x000000));
                        }
                    }
                    continue;
                }
                if hh < 8 {
                    continue; // too thin for text: the tooltip tells
                }
                let line1: Vec<String> = vec![su_line, format!("s{} MCS{}", user, mcs), format!("s{}", user)];
                let ink = text_on(color);
                // vertically centred when there is room for one line only
                let one_line_y = r.y0 + ((hh - gy - 7) / 2).max(1);
                if let Some(s) = pick_text(&line1, w, 1.0) {
                    let y = if hh >= 20 { r.y0 + 2 } else { one_line_y };
                    labels.push((s.to_string(), r.x0 as usize + 2, y as usize, ink));
                }
                if hh >= 20 {
                    if let Some(s) = pick_text(&[line2], w, 1.0) {
                        labels.push((s.to_string(), r.x0 as usize + 2, r.y0 as usize + 12, ink));
                    }
                }
            }

            // Channel-access and PHY overhead, each part labelled: AIFS (bright) and back-off slots (dim) before the TXOP,
            // RTS / CTS / BlockAck of a lone SU exchange (slate, with the SIFS gaps dark), and the preamble in front of
            // each PPDU's data symbols (L-PHY amber, EHT-PHY white). What keeps the leg colour is the payload.
            if view.show_overhead {
                // dim the overhead of TXOPs that are not the highlighted ones, like their data
                let ov_dim = any_highlight && !(is_key_highlighted(&g.holder, highlight)
                    || g.legs.iter().any(|&e| matches!(&idx.all[e], VizEvent::TxopStart { sender, .. } if is_key_highlighted(sender, highlight))));
                let dc = |c: u32| maybe_dim(c, ov_dim);
                let GREY: u32 = dc(0xa4a8b4);
                let DARK_INK: u32 = dc(0x20202c);
                let cx = |t: f64| x_of(t, view, panel_x, panel_w).clamp(px0, px1);
                let mut ov: Vec<(&[String], i32, i32, i32, u32)> = Vec::new(); // (candidates, x0, x1, y, ink)
                let mut ov_text: Vec<Vec<String>> = Vec::new();
                let mut seg = |buf: &mut [u32], t0: f64, t1: f64, color: u32, cands: &[&str], y: i32, ink: u32,
                               ov_text: &mut Vec<Vec<String>>, ov_meta: &mut Vec<(i32, i32, i32, u32)>| {
                    let (xa, xb) = (cx(t0), cx(t1));
                    if xb <= xa { return; }
                    if color != 0 { fill_rect_clip(buf, stride, xa, top, xb, top + h, color); }
                    ov_text.push(cands.iter().map(|c| c.to_string()).collect());
                    ov_meta.push((xa, xb, y, ink));
                };
                let mut ov_meta: Vec<(i32, i32, i32, u32)> = Vec::new();
                let mid = top + h / 2 - 3;
                let first = g.legs.first().and_then(|&e| match &idx.all[e] { VizEvent::TxopStart { .. } => Some(&idx.all[e]), _ => None });
                if let Some(VizEvent::TxopStart { ppdu_t0, ppdu_t1, preamble_s, ctrl_tx_s, ctrl_rx_s, resp_s, .. }) = first {
                    // Channel access: AIFS from when the medium was free for the holder, then the slots it counted in this
                    // idle period, up to where its counter reached zero. That is the TXOP start when it was contending
                    // for it; earlier when the countdown ran out with nothing to send (post-backoff), and the time from
                    // there to the TXOP is idle air, as is the time before AIFS.
                    if let Some((aifs_start, aifs_end, bo_start, bo_end, slots)) = first.and_then(|e| access_window(e, g.t)) {
                        let hc = key_color(g.holder);
                        let (xs, xa, xb, xt) = (cx(aifs_start), cx(aifs_end), cx(bo_start), cx(bo_end));
                        // the cursor makes it a countdown: what it has passed is spent (faded), what is ahead is still to wait
                        let cur = view.cursor_t;
                        let aifs_c = cx(cur.clamp(aifs_start, aifs_end));
                        let bo_c = cx(cur.clamp(bo_start, bo_end));
                        let faded = |c: u32| dc(mid_color_pair(IDLE_BG, c, 0.25));
                        let bo_col = mid_color_pair(IDLE_BG, hc, 0.6);
                        fill_rect_clip(buf, stride, xs, top, aifs_c, top + 6, faded(hc));
                        fill_rect_clip(buf, stride, aifs_c, top, xa, top + 6, dc(hc));
                        fill_rect_clip(buf, stride, xb, top, bo_c, top + 6, faded(bo_col));
                        fill_rect_clip(buf, stride, bo_c, top, xt, top + 6, dc(bo_col));
                        // one tick per back-off slot, when the slots are wide enough to tell apart
                        let slot_px = crate::lib::SLOT / view.span_t * panel_w as f64;
                        if slot_px >= 3.0 {
                            for k in 1..slots {
                                let x = x_of(bo_start + k as f64 * crate::lib::SLOT, view, panel_x, panel_w);
                                if x > xb && x < xt {
                                    fill_rect_clip(buf, stride, x, top, x + 1, top + 6, dc(0x101018));
                                }
                            }
                        }
                        // slots still to wait at the cursor
                        let left = if cur <= bo_start { slots } else if cur >= bo_end { 0 } else { ((bo_end - cur) / crate::lib::SLOT).ceil() as i64 };
                        seg(buf, aifs_start, aifs_end, 0, &["AIFS"], top + 8, GREY, &mut ov_text, &mut ov_meta);
                        let cands: Vec<String> = if cur > bo_start && cur < bo_end {
                            vec![format!("BO {} ({} left)", slots, left), format!("BO {}", left), "BO".to_string()]
                        } else {
                            vec![format!("BO {}", slots), "BO".to_string()]
                        };
                        let refs: Vec<&str> = cands.iter().map(|c| c.as_str()).collect();
                        seg(buf, bo_start, bo_end, 0, &refs, top + 8, GREY, &mut ov_text, &mut ov_meta);
                    }
                    // the exchange around a lone SU PPDU
                    let p0 = ppdu_t0 - preamble_s;
                    if plain && *ppdu_t1 > *ppdu_t0 && p0 >= g.t - 1e-9 && *ppdu_t1 <= g.end + 1e-9 {
                        let sifs = crate::lib::SIFS;
                        let (slate, dark) = (dc(0x68749a), dc(BUSY_BG));
                        fill_rect_clip(buf, stride, cx(g.t), top, cx(p0), top + h, dark);
                        fill_rect_clip(buf, stride, cx(*ppdu_t1), top, cx(g.end), top + h, dark);
                        if *ctrl_tx_s > 0.0 { seg(buf, g.t, g.t + ctrl_tx_s, slate, &["RTS", "R"], mid, GREY, &mut ov_text, &mut ov_meta); }
                        if *ctrl_rx_s > 0.0 { seg(buf, p0 - sifs - ctrl_rx_s, p0 - sifs, slate, &["CTS", "C"], mid, GREY, &mut ov_text, &mut ov_meta); }
                        if *resp_s > 0.0 { seg(buf, ppdu_t1 + sifs, ppdu_t1 + sifs + resp_s, slate, &["BA"], mid, GREY, &mut ov_text, &mut ov_meta); }
                    }
                }
                // one preamble per PPDU
                let mut pre: Vec<(i64, f64, f64)> = Vec::new(); // (key, data start, preamble)
                for &ev in &g.legs {
                    let VizEvent::TxopStart { ppdu_t0, ppdu_t1, preamble_s, .. } = &idx.all[ev] else { continue };
                    if *ppdu_t1 <= *ppdu_t0 || *preamble_s <= 0.0 {
                        continue;
                    }
                    let key = (ppdu_t0 * 1e9).round() as i64;
                    match pre.iter_mut().find(|e| e.0 == key) {
                        Some(e) => if *ppdu_t0 < e.1 { e.1 = *ppdu_t0; },
                        None => pre.push((key, *ppdu_t0, *preamble_s)),
                    }
                }
                for &(_, t0, preamble_s) in &pre {
                    let p0 = t0 - preamble_s;
                    let legacy_end = p0 + crate::lib::LEGACY_PHY_DURATION.min(preamble_s);
                    seg(buf, p0, legacy_end, dc(0xffc83c), &["L-PHY", "L"], mid, DARK_INK, &mut ov_text, &mut ov_meta);
                    seg(buf, legacy_end, t0, dc(0xf4f4ff), &["EHT-PHY", "EHT", "E"], mid, DARK_INK, &mut ov_text, &mut ov_meta);
                }
                let _ = &mut ov;
                for (cands, &(xa, xb, y, ink)) in ov_text.iter().zip(&ov_meta) {
                    if let Some(s) = pick_text(cands, xb - xa, 1.0) {
                        labels.push((s.to_string(), (xa + 2) as usize, y as usize, ink));
                    }
                }
            }

            for (text, x, y, ink) in &labels {
                render_text(buf, text, *x, *y, stride, *ink, 1);
            }
        }
    }

    // Collisions overlay
    if let Some(indices) = idx.collisions_by_link.get(&link_id) {
        let start = indices.partition_point(|&i| event_end(&idx.all[i]) < t_lo);
        for &ii in &indices[start..] {
            let ev = &idx.all[ii];
            let (t, end) = match ev {
                VizEvent::Collision { t, end, .. } => (*t, *end),
                _ => continue,
            };
            if t > t_hi { break; }
            let x0 = x_of(t,   view, panel_x, panel_w).max(panel_x as i32);
            let x1 = x_of(end, view, panel_x, panel_w).min((panel_x + panel_w) as i32);
            if x1 <= x0 { continue; }
            for yy in (lane_y + 6)..(lane_y + lane_h - 6) {
                let c = if (yy & 2) == 0 { 0xff3344 } else { 0x661010 };
                // blended, so the labels of a TXOP under it (a capture or out-of-range winner) stay readable
                let row = yy * stride;
                for xx in (x0 as usize)..(x1 as usize).min(stride) {
                    blend_pixel(buf, row + xx, c, 0.7);
                }
            }
        }
    }
    // key to the lane tones, once, above the first lane's label
    if link_id == 0 && lane_h >= 90 {
        let mut ly = lane_y + 4;
        for (c, name) in [(IDLE_BG, "idle air"), (BUSY_BG, "busy, no payload"), (0x4fc3f7, "payload")] {
            fill_rect(buf, stride, 8, ly, 10, 7, c);
            outline_rect_clip(buf, stride, 8, ly as i32, 18, ly as i32 + 7, 0x666677);
            render_text(buf, name, 24, ly, stride, 0x9aa0b4, 1);
            ly += 11;
        }
    }
    let link_label = idx.lane_names.get(&link_id).cloned().unwrap_or_else(|| format!("LINK {} ({}MHz)", link_id, bandwidth_mhz));
    render_text(buf, &link_label, 8, lane_y + lane_h / 2 - 8, stride, 0xeeeeee, 2);
}

/// Height of a link lane.
fn lane_height(idx: &VizIndex, _bandwidth_mhz: u16) -> usize {
    let h = 50;
    if idx.compact_lanes { h * 2 / 3 } else { h }
}

/// Tooltip lines of A-MPDU `ev` of TXOP `g`.
fn leg_tooltip_lines(g: &TxopGroup, ev: usize, idx: &VizIndex) -> Vec<String> {
    let VizEvent::TxopStart {
        sender, dest_id, ampdu_packets, mcs, alvr_stream_ids, alvr_frame_ids, alvr_frame_losses,
        ppdu_t0, ppdu_t1, mpdu_arrivals, ..
    } = &idx.all[ev] else { return Vec::new() };
    let who = |s: i32| if s < 0 { ap_name(s) } else { format!("s{}", s) };
    let mut lines = vec![format!("SU PPDU  {:?}  {} -> s{}", sender.1, who(sender.0), dest_id)];
    lines.push(format!("MCS {}", mcs));
    let streams = alvr_stream_ids.iter()
        .map(|&id| crate::lib::alvr_stream_socket::get_stream_name(id))
        .collect::<Vec<_>>()
        .join("+");
    lines.push(format!("{} MPDUs  {}", ampdu_packets, streams));
    if !mpdu_arrivals.is_empty() {
        let waits: Vec<f64> = mpdu_arrivals.iter().map(|a| (g.t - a) * 1e6).collect();
        let (lo, hi) = waits.iter().fold((f64::MAX, f64::MIN), |(l, h), &w| (l.min(w), h.max(w)));
        let mean = waits.iter().sum::<f64>() / waits.len() as f64;
        lines.push(if hi - lo < 0.5 {
            format!("queued {:.0} us before the TXOP", mean)
        } else {
            format!("queued {:.0}-{:.0} us before the TXOP (mean {:.0} us)", lo, hi, mean)
        });
    }
    if let Some((label, frames)) = frames_label(alvr_stream_ids, alvr_frame_ids, alvr_frame_losses) {
        let mut f = format!("{}: {:?}", label, frames);
        if f.len() > 60 { f.truncate(57); f += "..."; }
        lines.push(f);
    }
    if let VizEvent::TxopStart { preamble_s, idle_since, aifs_s, .. } = &idx.all[ev] {
        // (only what the MAC reported: this build reports no preamble, control frames or channel-access window)
        let mut o = if *preamble_s > 0.0 { format!("preamble {:.0} us", preamble_s * 1e6) } else { String::new() };
        if let VizEvent::TxopStart { ctrl_tx_s, ctrl_rx_s, resp_s, .. } = &idx.all[ev] {
            if *ctrl_tx_s > 0.0 { o += &format!(";  RTS {:.0} us  CTS {:.0} us", ctrl_tx_s * 1e6, ctrl_rx_s * 1e6); }
            if *resp_s > 0.0 { o += &format!(";  BA {:.0} us", resp_s * 1e6); }
        }
        if let Some((_, _, _, bo_end, slots)) = access_window(&idx.all[ev], g.t) {
            let drawn = if let VizEvent::TxopStart { bo_drawn, .. } = &idx.all[ev] { *bo_drawn } else { 0 };
            o += &format!(";  access: medium idle {:.0} us before it, AIFS {:.0} us + {} of {} drawn back-off slots",
                          (g.t - idle_since) * 1e6, aifs_s * 1e6, slots, drawn);
            if g.t - bo_end > 0.5e-6 {
                o += &format!(", counter at zero {:.0} us before it", (g.t - bo_end) * 1e6);
            }
        }
        let o = o.trim_start_matches(";  ").to_string();
        if !o.is_empty() { lines.push(o); }
    }
    if ppdu_t1 > ppdu_t0 {
        let (w0, w1) = ppdu_window(g, idx, ev);
        lines.push(format!("own data {:.1} us;  PPDU data on air {:.1} us @ {:.6} s", (ppdu_t1 - ppdu_t0) * 1e6, (w1 - w0) * 1e6, w0));
    }
    lines.push(txop_tooltip_line(g, idx));
    lines
}

fn txop_tooltip_line(g: &TxopGroup, idx: &VizIndex) -> String {
    let holder = if g.holder.0 < 0 { ap_name(g.holder.0) } else { format!("s{}", g.holder.0) };
    format!("TXOP  holder {}  {:.1} us @ {:.6} s", holder, (g.end - g.t) * 1e6, g.t)
}

/// Tooltip lines for the mouse at (`mx`, `my`) over a link lane (`lanes` = (y, height, link)).
fn lane_hover_lines(
    mx: f32, my: f32,
    lanes: &[(usize, usize, u8)],
    panel_x: usize, panel_w: usize,
    view: &ViewState, idx: &VizIndex,
) -> Option<Vec<String>> {
    let (mxi, myi) = (mx as i32, my as i32);
    if mxi < panel_x as i32 || mxi >= (panel_x + panel_w) as i32 {
        return None;
    }
    let &(lane_y, lane_h, link_id) = lanes.iter().find(|(y, h, _)| myi >= *y as i32 && myi < (*y + *h) as i32)?;
    let groups = idx.txop_groups_by_link.get(&link_id)?;
    let (top, h) = ((lane_y + 2) as i32, lane_h as i32 - 4);
    let t_px = view.span_t / panel_w as f64;
    let t_m = view.center_t - view.span_t * 0.5 + (mx as f64 - panel_x as f64) * t_px;
    let start = groups.partition_point(|g| g.end < t_m - 2.0 * t_px);
    for g in &groups[start..] {
        if g.t > t_m + 2.0 * t_px { break; }
        let hit = leg_rects(g, idx, view, panel_x, panel_w, top, h).into_iter()
            .find(|r| mxi >= r.x0 && mxi < r.x1 && myi >= r.y0 && myi < r.y1);
        if let Some(r) = hit {
            return Some(leg_tooltip_lines(g, r.ev, idx));
        }
        // inside a frame but on no tile: the TXOP itself
        if !g.is_plain_su() && t_m >= g.t && t_m <= g.end {
            let users: HashSet<i32> = g.legs.iter().filter_map(|&e| match &idx.all[e] {
                VizEvent::TxopStart { sender, dest_id, .. } => Some(leg_user(*sender, *dest_id)),
                _ => None,
            }).collect();
            return Some(vec![
                txop_tooltip_line(g, idx),
                format!("{} A-MPDU(s) to/from {} STA(s); rest: protection, BlockAcks", g.legs.len(), users.len()),
            ]);
        }
    }
    None
}


/// The TXOP under the mouse in a link lane: its start, the queue that won it and the queues that sent in it.
struct HoverOwner { t: f64, link: u8, holder: MacKey, keys: Vec<MacKey> }

fn lane_hover_owner(
    mx: f32, my: f32,
    lanes: &[(usize, usize, u8)],
    panel_x: usize, panel_w: usize,
    view: &ViewState, idx: &VizIndex,
) -> Option<HoverOwner> {
    let (mxi, myi) = (mx as i32, my as i32);
    if mxi < panel_x as i32 || mxi >= (panel_x + panel_w) as i32 {
        return None;
    }
    let &(lane_y, lane_h, link_id) = lanes.iter().find(|(y, h, _)| myi >= *y as i32 && myi < (*y + *h) as i32)?;
    let groups = idx.txop_groups_by_link.get(&link_id)?;
    let t_px = view.span_t / panel_w as f64;
    let t_m = view.center_t - view.span_t * 0.5 + (mx as f64 - panel_x as f64) * t_px;
    let start = groups.partition_point(|g| g.end < t_m - 2.0 * t_px);
    let _ = lane_h;
    for g in &groups[start..] {
        if g.t > t_m + 2.0 * t_px { break; }
        if t_m >= g.t - 2.0 * t_px && t_m <= g.end + 2.0 * t_px {
            let mut keys = vec![g.holder];
            for &e in &g.legs {
                if let VizEvent::TxopStart { sender, .. } = &idx.all[e] {
                    if !keys.contains(sender) { keys.push(*sender); }
                }
            }
            return Some(HoverOwner { t: g.t, link: link_id, holder: g.holder, keys });
        }
    }
    None
}

/// Draws `lines` in a box next to the mouse, kept inside the window.
fn draw_tooltip(buf: &mut [u32], stride: usize, mx: f32, my: f32, lines: &[String]) {
    if lines.is_empty() {
        return;
    }
    const SCALE: f32 = 1.0;
    const LINE_H: i32 = 11;
    let (mxi, myi) = (mx as i32, my as i32);
    let bw = lines.iter().map(|l| text_w(l, SCALE)).max().unwrap_or(0) + 12;
    let bh = lines.len() as i32 * LINE_H + 8;
    let h_buf = (buf.len() / stride) as i32;
    let mut x = mxi + 16;
    if x + bw > stride as i32 { x = (mxi - 16 - bw).max(0); }
    let y = (myi + 16).min(h_buf - bh).max(0);
    fill_rect_clip(buf, stride, x, y, x + bw, y + bh, 0x101018);
    outline_rect_clip(buf, stride, x, y, x + bw, y + bh, 0x5566aa);
    for (i, l) in lines.iter().enumerate() {
        render_text(buf, l, (x + 6) as usize, (y + 5 + i as i32 * LINE_H) as usize, stride,
                    if i == 0 { 0xffffff } else { 0xccccdd }, SCALE);
    }
}

/// Hover tooltip over a link lane.
fn render_hover_tooltip(
    buf: &mut [u32], stride: usize,
    mx: f32, my: f32,
    lanes: &[(usize, usize, u8)],
    panel_x: usize, panel_w: usize,
    view: &ViewState, idx: &VizIndex,
) {
    if let Some(l) = hover_tooltip_lines(mx, my, lanes, panel_x, panel_w, view, idx) {
        draw_tooltip(buf, stride, mx, my, &l);
    }
}

/// Lines of the hover tooltip over a link lane.
fn hover_tooltip_lines(
    mx: f32, my: f32,
    lanes: &[(usize, usize, u8)],
    panel_x: usize, panel_w: usize,
    view: &ViewState, idx: &VizIndex,
) -> Option<Vec<String>> {
    lane_hover_lines(mx, my, lanes, panel_x, panel_w, view, idx)
}

fn render_mackey_rows(
    buf: &mut [u32], stride: usize,
    top: usize, bottom: usize,
    panel_x: usize, panel_w: usize,
    view: &ViewState, idx: &VizIndex,
    highlight: &Option<ActiveHighlights>,
) {
    let cursor = view.cursor_t;

    let visible_keys: Vec<_> = idx.mac_keys_sorted.iter()
        .filter(|k| view.selected_link.map_or(true, |f| k.2 == f))
        .collect();

    // ── Column count ─────────────────────────────────────────────────────
    let base_row_h        = 36usize;
    let rows_per_col_base = ((bottom - top) / base_row_h).max(1);
    let num_cols = ((visible_keys.len() as f64 / rows_per_col_base as f64)
        .ceil() as usize)
        .max(1);

    // ── Per-column-count layout table ────────────────────────────────────
    // Increased top_h values by ~40% to add vertical breathing room
    let (row_h, top_h, label_scale, aifs_w, cw_label_w) = match num_cols {
        1 => (42usize, 20usize, 2usize, 10usize, 90usize), // top_h: 14 -> 20 (+42%)
        2 => (36,      17,      1,       7,       80),       // top_h: 12 -> 17 (+41%)
        3 => (30,      14,      1,       5,       68),       // top_h: 10 -> 14 (+40%)
        _ => (26,      13,      1,       4,       58),       // top_h: 9  -> 13 (+44%)
    };

    let rows_per_col = ((bottom - top) / row_h).max(1);
    
    // Narrow panel to half-width if only one column
    let col_w = if num_cols == 1 { panel_w / 2 } else { panel_w / num_cols };

    // Horizontal spacing between AIFS pill and bars
    let bar_x_offset = aifs_w + 20; 
    let bar_w = col_w
        .saturating_sub(bar_x_offset + cw_label_w + 12)
        .max(20);

    // Background clearing
    fill_rect(buf, stride, 0,       top, panel_x, bottom - top, 0x14141c);
    fill_rect(buf, stride, panel_x, top, panel_w, bottom - top, 0x0d0d12);

    for col in 1..num_cols {
        let div_x = panel_x + col * col_w;
        for y in top..bottom {
            let i = y * stride + div_x;
            if i < buf.len() { buf[i] = 0x222233; }
        }
    }

    let start_row = view.row_scroll as usize;
    for (vrow, key) in visible_keys.iter().skip(start_row).enumerate() {
        let col        = vrow / rows_per_col;
        let row_in_col = vrow % rows_per_col;
        if col >= num_cols { break; }

        let col_x = panel_x + col * col_w;
        let y     = top + row_in_col * row_h;

        let row_bg = if row_in_col % 2 == 0 { 0x10101a } else { 0x0d0d12 };
        fill_rect(buf, stride, col_x, y, col_w, row_h, row_bg);

        let dimmed = !is_key_highlighted(key, highlight);

        // ── TOP LINE (STA Label) ──
        let lbl = format!("s{:>3} {:?} L{}", key.0, key.1, key.2);
        render_text(buf, &lbl, col_x + 4, y + 2, stride,
                    maybe_dim(0xffffff, dimmed), label_scale);

        // ── BOTTOM LINE (Pill + Bar) ──
        // The vertical gap is created because bot_y starts further down (at y + top_h)
        let bot_y   = y + top_h;
        let bot_h   = row_h.saturating_sub(top_h);
        let pill_h  = bot_h.saturating_sub(4).max(4); // Slightly more padding inside the row
        let pill_y  = bot_y + (bot_h - pill_h) / 2;
        let bar_h   = pill_h;
        let bar_y   = pill_y;

        let bo = idx.backoff_by_key.get(*key)
            .and_then(|v| latest_at(v, &idx.all, cursor));

        if let Some(VizEvent::BackoffSnap { counter, cw, frozen, medium_free_since, .. }) = bo {
            let aifs_s      = aifs_secs_for_ac(key.1);
            let aifs_active = cursor < medium_free_since + aifs_s;
            let aifs_color  = maybe_dim(if aifs_active { 0xffaa00 } else { 0x333333 }, dimmed);

            fill_rect(buf, stride, col_x + 4, pill_y, aifs_w, pill_h, aifs_color);

            let bar_x  = col_x + bar_x_offset;
            let cells  = (*cw as usize).min(64);
            let cell_w = (bar_w / cells.max(1)).max(1);

            for c in 0..cells {
                let x = bar_x + c * cell_w;
                if x + cell_w > col_x + col_w { break; }
                let color = maybe_dim(
                    if c < (*counter as usize) {
                        if *frozen { 0x664488 } else { 0x44aaff }
                    } else { 0x222230 },
                    dimmed,
                );
                fill_rect(buf, stride, x, bar_y,
                          cell_w.saturating_sub(1).max(1), bar_h, color);
            }

            let cw_x = bar_x + bar_w + 6;
            if cw_x + cw_label_w <= col_x + col_w {
                let cw_str = if num_cols == 1 {
                    format!("CW={} BO={}{}", cw, counter, if *frozen { " *" } else { "" })
                } else {
                    format!("C{} B{}{}", cw, counter, if *frozen { "*" } else { "" })
                };
                render_text(buf, &cw_str, cw_x, pill_y, stride,
                            maybe_dim(0xcccccc, dimmed), 1.5);
            }
        }
    }
}


fn blend_pixel(buf: &mut [u32], idx: usize, color: u32, alpha: f32) {
    if idx >= buf.len() { return; }
    let bg = buf[idx];
    
    // Extract RGB components
    let rb = bg & 0xFF00FF;
    let g  = bg & 0x00FF00;
    let rf = color & 0xFF00FF;
    let gf = color & 0x00FF00;

    // Linear interpolation: bg + alpha * (fg - bg)
    let a = (alpha * 256.0) as u32;
    let out_rb = (rb + ((rf.wrapping_sub(rb)).wrapping_mul(a) >> 8)) & 0xFF00FF;
    let out_g  = (g + ((gf.wrapping_sub(g)).wrapping_mul(a) >> 8)) & 0x00FF00;

    buf[idx] = out_rb | out_g;
}

// A version of draw_vline that supports alpha
fn draw_vline_alpha(buf: &mut [u32], stride: usize, x: i32, y0: i32, y1: i32, color: u32, alpha: f32) {
    if x < 0 || x >= stride as i32 { return; }
    let (start, end) = if y0 < y1 { (y0, y1) } else { (y1, y0) };
    for y in start..=end {
        if y >= 0 {
            blend_pixel(buf, (y as usize) * stride + (x as usize), color, alpha);
        }
    }
}
fn build_qdepth_series<'a>(
    _view: &ViewState,
    idx:   &'a VizIndex,
) -> &'a [((i32, EdcaAc), Vec<(f64, usize)>)] {
    &idx.qdepth_series
}

// fn build_qdepth_series(
//     view: &ViewState,
//     idx: &VizIndex,
// ) -> Vec<((i32, EdcaAc), Vec<(f64, usize)>)> {
//     let mut per_link_map: HashMap<(i32, EdcaAc, u8), Vec<usize>> = HashMap::new();
//     for (key, indices) in &idx.qdepth_by_key {
//         for &ii in indices {
//             if let VizEvent::QueueDepth { .. } = &idx.all[ii] {
//                 per_link_map.entry((key.0, key.1, key.2)).or_default().push(ii);
//             }
//         }
//     }
//     for v in per_link_map.values_mut() {
//         v.sort_by(|&a, &b| event_t(&idx.all[a]).partial_cmp(&event_t(&idx.all[b])).unwrap());
//     }

//     let flow_keys: HashSet<(i32, EdcaAc)> = per_link_map
//         .keys()
//         .map(|&(s, a, _)| (s, a))
//         .collect();

//     let mut agg_map: HashMap<(i32, EdcaAc), Vec<(f64, usize)>> = HashMap::new();
//     for (sta_id, ac) in &flow_keys {
//         let mut all_events: Vec<(f64, u8, usize)> = per_link_map
//             .iter()
//             .filter(|(&(s, a, _), _)| s == *sta_id && a == *ac)
//             .flat_map(|(&(_, _, lid), indices)| {
//                 indices.iter().filter_map(move |&ii| {
//                     if let VizEvent::QueueDepth { t, depth, .. } = &idx.all[ii] {
//                         Some((*t, lid, *depth))
//                     } else {
//                         None
//                     }
//                 })
//             })
//             .collect();
//         all_events.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

//         let mut link_depths: HashMap<u8, usize> = HashMap::new();
//         let series = all_events
//             .into_iter()
//             .map(|(t, lid, depth)| {
//                 link_depths.insert(lid, depth);
//                 (t, link_depths.values().sum::<usize>())
//             })
//             .collect();
//         agg_map.insert((*sta_id, *ac), series);
//     }

//     let mut keys: Vec<_> = agg_map.keys().copied().collect();
//     keys.sort_by_key(|k| (ac_prio(k.1), k.0));
//     keys.reverse();
//     keys.into_iter().map(|k| (k, agg_map.remove(&k).unwrap())).collect()
// }

fn hit_test_qdepth_legend(
    mx: f32, my: f32,
    panel_y: usize, panel_h: usize, panel_x: usize,
    view: &ViewState, idx: &VizIndex,
) -> Option<ActiveHighlights> {
    // Legend lives in the left sidebar only
    if (mx as usize) >= panel_x { return None; }
    let my_u = my as usize;
    if view.q_mode != 0 {
        let rows = qdepth_strip_keys(view, idx);
        let (shown, row_h) = qstrip_geometry(rows.len(), panel_h);
        if my_u < panel_y + 1 { return None; }
        let r = (my_u - panel_y - 1) / row_h;
        if r >= shown { return None; }
        let (sta_id, ac) = rows[r + view.q_scroll.min(rows.len().saturating_sub(shown))];
        let mut h = ActiveHighlights { keys: HashSet::new(), txops: HashSet::new() };
        for key in &idx.mac_keys_sorted {
            if key.0 == sta_id && key.1 == ac { h.keys.insert(*key); }
        }
        for key in &h.keys { h.txops.insert((*key, -1)); }
        return Some(h);
    }

    let t_lo = view.center_t - view.span_t * 0.5;
    let t_hi = view.center_t + view.span_t * 0.5;

    let series_list = build_qdepth_series(view, idx);
    let mut legend_y = panel_y + 24;
    const LEGEND_ITEM_H: usize = 20;

    for ((sta_id, ac), series) in series_list {
        let s = series.partition_point(|(t, _)| *t < t_lo);
        let is_active = s > 0 || s < series.len() && series[s].0 <= t_hi;
        if !is_active { continue; }

        if my_u >= legend_y && my_u < legend_y + LEGEND_ITEM_H {
            // Build an ActiveHighlights that lights up every MacKey for this flow
            let mut h = ActiveHighlights {
                keys: HashSet::new(),
                txops: HashSet::new(),
            };
            for key in &idx.mac_keys_sorted {
                if key.0 == *sta_id && key.1 == *ac {
                    h.keys.insert(*key);
                }
            }
            // Also mark matching txops so render_link_lane can see them
            for key in &h.keys {
                h.txops.insert((*key, -1)); // dest wildcard — checked by sta_id+ac below
            }
            return Some(h);
        }
        legend_y += LEGEND_ITEM_H;
    }
    None
}

/// For a step-function series, returns one `(y_min, y_max)` per pixel column.
/// Gaps (columns with no transition) are filled with the last known value,
/// which is correct for a step function (value holds until next event).
fn build_step_envelope(
    series_y: &[(i32, i32)],
    panel_x:  usize,
    panel_w:  usize,
    hold_y:   Option<i32>,
    out:      &mut Vec<Option<(i32, i32)>>,
) {
    out.clear();
    out.resize(panel_w, None);

    let mut cols: Vec<Option<(i32, i32, i32)>> = vec![None; panel_w];

    for &(px, py) in series_y {
        let col = px - panel_x as i32;
        if col < 0 || col as usize >= panel_w { continue; }
        let col = col as usize;
        cols[col] = Some(match cols[col] {
            None              => (py, py, py),
            Some((mn, mx, _)) => (mn.min(py), mx.max(py), py),
        });
    }

    // Write directly into `out` — no shadowing local variable
    let mut last = hold_y;
    for col in 0..panel_w {
        match cols[col] {
            Some((mn, mx, fy)) => {
                out[col] = Some((mn, mx));
                last = Some(fy);
            }
            None => {
                if let Some(y) = last {
                    out[col] = Some((y, y));
                }
            }
        }
    }
}


/// Screen blend with a per-channel brightness ceiling.
/// `cap` is 0–255; 180 prevents saturation to white while keeping colours vivid.
#[inline]
fn blend_screen(dst: u32, src: u32) -> u32 {
    let cap = 210; 
    let ch = |d: u32, s: u32| -> u32 {
        let df = d as f32 / 255.0;
        let sf = s as f32 / 255.0;
        (((1.0 - (1.0 - df) * (1.0 - sf)) * 255.0) as u32).min(cap as u32)
    };
    let r = ch((dst >> 16) & 0xFF, (src >> 16) & 0xFF);
    let g = ch((dst >>  8) & 0xFF, (src >>  8) & 0xFF);
    let b = ch( dst        & 0xFF,  src        & 0xFF);
    (r << 16) | (g << 8) | b
}// #[inline]
// fn blend_screen(dst: u32, src: u32) -> u32 {
//     // Screen blend: result = 1 - (1-dst)(1-src)  — never clips to white for typical colors
//     let ch = |d: u32, s: u32| -> u32 {
//         let df = d as f32 / 255.0;
//         let sf = s as f32 / 255.0;
//         ((1.0 - (1.0 - df) * (1.0 - sf)) * 255.0) as u32
//     };
//     let r = ch((dst >> 16) & 0xFF, (src >> 16) & 0xFF);
//     let g = ch((dst >>  8) & 0xFF, (src >>  8) & 0xFF);
//     let b = ch( dst        & 0xFF,  src        & 0xFF);
//     (r << 16) | (g << 8) | b
// }
#[inline]
fn is_dimmed(sta_id: i32, ac: EdcaAc, highlight: &Option<ActiveHighlights>) -> bool {
    if let Some(h) = highlight {
        let lit = h.txops.iter().any(|&(m, _)| m.0 == sta_id && m.1 == ac)
            || (h.txops.is_empty() && h.keys.iter().any(|k| k.0 == sta_id && k.1 == ac));
        !lit
    } else {
        false
    }
}


// ----------------------------------------------------------------
// Queue panel variations (`Q`): the same cached per-(station, AC) depth series drawn as one strip per queue,
// a heatmap, or a stacked area.
// ----------------------------------------------------------------
fn qkey_color(sta_id: i32, ac: EdcaAc) -> u32 {
    if sta_id < 0 {
        ac_color(ac)
    } else {
        [0xFF4444, 0xFF8822, 0xFFCC33, 0xFF55AA, 0x44AAFF, 0x44FF88, 0x8844FF, 0x44FFEE][(sta_id.unsigned_abs() as usize) % 8]
    }
}

/// Queue depth per pixel column of `[t_lo, t_lo + span]`: the maximum within the column, so spikes survive zooming out.
fn column_depths(series: &[(f64, usize)], t_lo: f64, span: f64, w: usize) -> Vec<usize> {
    column_values(series, |&(t, d)| (t, d), t_lo, span, w)
}

/// `column_depths` for any series, with `get` reading (time, value) of an element.
fn column_values<T>(series: &[T], get: impl Fn(&T) -> (f64, usize), t_lo: f64, span: f64, w: usize) -> Vec<usize> {
    // a panel dragged down to no width has no columns
    if w == 0 { return Vec::new(); }
    let s = series.partition_point(|x| get(x).0 < t_lo);
    let hold = if s > 0 { get(&series[s - 1]).1 } else { 0 };
    let t_hi = t_lo + span;
    let mut ev_max: Vec<Option<usize>> = vec![None; w];
    let mut ev_last = vec![0usize; w];
    for x in &series[s..] {
        let (t, d) = get(x);
        if t > t_hi { break; }
        let c = ((((t - t_lo) / span) * w as f64) as usize).min(w - 1);
        ev_max[c] = Some(ev_max[c].map_or(d, |m| m.max(d)));
        ev_last[c] = d;
    }
    let mut out = vec![0usize; w];
    let mut carry = hold;
    for c in 0..w {
        match ev_max[c] {
            Some(m) => { out[c] = carry.max(m); carry = ev_last[c]; }
            None => out[c] = carry,
        }
    }
    out
}

fn heat_color(f: f64) -> u32 {
    const STOPS: [u32; 5] = [0x0d0d18, 0x2b2b8f, 0x1fa3c4, 0xf0d040, 0xffffff];
    let x = f.clamp(0.0, 1.0) * 4.0;
    let i = (x as usize).min(3);
    mid_color_pair(STOPS[i], STOPS[i + 1], (x - i as f64) as f32)
}

fn mid_color_pair(a: u32, b: u32, f: f32) -> u32 {
    let ch = |sh: u32| {
        let (x, y) = (((a >> sh) & 0xFF) as f32, ((b >> sh) & 0xFF) as f32);
        ((x + (y - x) * f) as u32).min(255) << sh
    };
    ch(16) | ch(8) | ch(0)
}

fn render_qdepth_variant(
    buf: &mut [u32], stride: usize,
    panel_y: usize, panel_h: usize,
    panel_x: usize, panel_w: usize,
    view: &ViewState, idx: &VizIndex,
    highlight: &Option<ActiveHighlights>,
) {
    let t_lo = view.center_t - view.span_t * 0.5;
    let t_hi = t_lo + view.span_t;
    let mode = view.q_mode;

    // queues that exist by the end of the window (a queue is born at its first event)
    let rows: Vec<(&(i32, EdcaAc), &Vec<(f64, usize)>)> = idx.qdepth_series.iter()
        .filter(|(_, ser)| ser.first().is_some_and(|f| f.0 <= t_hi))
        .map(|(k, ser)| (k, ser))
        .collect();
    let cols: Vec<Vec<usize>> = rows.iter().map(|(_, ser)| column_depths(ser, t_lo, view.span_t, panel_w)).collect();
    let gmax = cols.iter().flat_map(|c| c.iter().copied()).max().unwrap_or(0).max(1);

    let title = ["lines", "strips", "heatmap", "stacked"][mode as usize % 4];
    let shown_rows = qstrip_geometry(rows.len(), panel_h).0;
    let more = if rows.len() > shown_rows { format!("  rows {}-{} of {} (wheel on labels)", view.q_scroll.min(rows.len() - shown_rows) + 1, view.q_scroll.min(rows.len() - shown_rows) + shown_rows, rows.len()) } else { String::new() };
    let arr_tag = if view.show_arrivals && matches!(mode, 1 | 2) { "  arrivals [I]" } else { "" };
    let head = format!("QUEUES: {}{}{}{}  [Q]", title, if mode == 1 { if view.q_shared { " (shared scale)" } else { " (scale per row)" } } else { "" }, more, arr_tag);
    let hx = (panel_x + panel_w).saturating_sub(text_w(&head, 1.0) as usize + 6);
    let label_of = |sta: i32, ac: EdcaAc| if sta < 0 { format!("{} {}", ap_name(sta), ac.to_string()) } else { format!("STA{} {}", sta, ac.to_string()) };

    let (shown, row_h) = qstrip_geometry(rows.len(), panel_h);
    let fit = (panel_h.saturating_sub(2) / 9).max(1);
    let start = view.q_scroll.min(rows.len().saturating_sub(shown));
    draw_row_scrollbar(buf, stride, panel_y, panel_h, panel_x, rows.len(), shown, start);
    let (small, scale) = if row_h >= 16 { (false, 2.0f32) } else { (true, 1.0f32) };

    let draw_label = |buf: &mut [u32], r: usize, y: usize, sta: i32, ac: EdcaAc, dimmed: bool| {
        let c = maybe_dim(qkey_color(sta, ac), dimmed);
        fill_rect(buf, stride, 0, y + 1, 3, row_h.saturating_sub(2).max(1), c);
        let ty = y + (row_h.saturating_sub(if small { 7 } else { 14 })) / 2;
        render_text(buf, &label_of(sta, ac), 8, ty, stride, maybe_dim(0xdddddd, dimmed), scale);
        let _ = r;
    };

    match mode {
        1 | 2 => {
            // the TXOP under the mouse: (queue, TXOP start, end, arrival instant of each of its MPDUs)
            let hovered: Vec<((i32, EdcaAc), f64, f64, &Vec<f64>)> = view.hover_txop
                .and_then(|(link, t)| {
                    let gs = idx.txop_groups_by_link.get(&link)?;
                    gs.get(gs.partition_point(|g| g.t < t)).filter(|g| g.t == t)
                })
                .map(|g| g.legs.iter().filter_map(|&e| match &idx.all[e] {
                    VizEvent::TxopStart { sender, mpdu_arrivals, .. } => Some(((sender.0, sender.1), g.t, g.end, mpdu_arrivals)),
                    _ => None,
                }).collect())
                .unwrap_or_default();
            for (r, ((sta, ac), _)) in rows.iter().enumerate().skip(start).take(shown) {
                let y = panel_y + 1 + (r - start) * row_h;
                // arrival band at the bottom of the row: one tick per arrival, taller (log scale) where several share a pixel
                let band = if view.show_arrivals { (row_h / 4).clamp(3, 6) } else { 0 };
                let dimmed = is_dimmed(*sta, *ac, highlight);
                fill_rect(buf, stride, panel_x, y, panel_w, row_h, if r % 2 == 0 { 0x10101a } else { 0x0d0d14 });
                draw_label(buf, r, y, *sta, *ac, dimmed);
                let own_max = cols[r].iter().copied().max().unwrap_or(0).max(1);
                let rmax = if view.q_shared { gmax } else { own_max };
                let base = (y + row_h - 1 - band) as i32;
                let plot_h = (row_h.saturating_sub(3 + band)).max(1) as f64;
                let color = maybe_dim(qkey_color(*sta, *ac), dimmed);
                // magnitude readout in the label column: depth at the cursor / the row's scale maximum
                let ser = rows[r].1;
                let cur = ser.partition_point(|(t, _)| *t <= view.cursor_t).checked_sub(1).map_or(0, |i| ser[i].1);
                let mag = format!("{}/{}", cur, own_max);
                // the biggest size the number and the name fit together in the label column
                let name_w = text_w(&label_of(*sta, *ac), scale) as usize;
                let mscale = if !small && name_w + text_w(&mag, 2.0) as usize + 22 <= panel_x { 2.0 } else { 1.0 };
                let mw = text_w(&mag, mscale) as usize;
                let mh = if mscale >= 2.0 { 14 } else { 7 };
                render_text(buf, &mag, panel_x.saturating_sub(mw + 8), y + (row_h.saturating_sub(mh + 3)) / 2, stride, maybe_dim(0xf0f0ff, dimmed), mscale);
                // peak of this queue relative to the largest one: the same length rule for every row
                let bar_w = ((own_max as f64 / gmax as f64) * (panel_x.saturating_sub(14)) as f64).ceil().max(2.0) as usize;
                fill_rect(buf, stride, 8, y + row_h - 3, bar_w, 2, maybe_dim(0x8888aa, dimmed));
                if mode == 1 {
                    // half-scale guide
                    let gy = base - (plot_h / 2.0) as i32;
                    for gx in (panel_x..panel_x + panel_w).step_by(6) {
                        fill_rect_clip(buf, stride, gx as i32, gy, gx as i32 + 2, gy + 1, 0x2a2a3c);
                    }
                }
                for (c, &d) in cols[r].iter().enumerate() {
                    let x = (panel_x + c) as i32;
                    if mode == 1 {
                        if d > 0 {
                            let hgt = ((d as f64 / rmax as f64) * plot_h).ceil().max(1.0) as i32;
                            draw_vline_alpha(buf, stride, x, base - hgt + 1, base, color, 0.55);
                            fill_rect_clip(buf, stride, x, base - hgt + 1, x + 1, base - hgt + 2, color);
                        }
                    } else {
                        let f = ((1.0 + d as f64).ln() / (1.0 + gmax as f64).ln()).clamp(0.0, 1.0);
                        let hc = heat_color(if dimmed { f * 0.4 } else { f });
                        fill_rect_clip(buf, stride, x, y as i32, x + 1, (y + row_h - 1 - band) as i32, hc);
                    }
                }
                let yb = (y + row_h - 1) as i32;
                if band > 0 {
                    if let Some(arr) = idx.arrivals.get(&(*sta, *ac)) {
                        let mut counts = vec![0u32; panel_w];
                        for &t in &arr[arr.partition_point(|&t| t < t_lo)..] {
                            if t > t_hi { break; }
                            counts[((((t - t_lo) / view.span_t) * panel_w as f64) as usize).min(panel_w - 1)] += 1;
                        }
                        for (c, &n) in counts.iter().enumerate().filter(|(_, &n)| n > 0) {
                            let hgt = (2 + (32 - n.leading_zeros()) as i32).min(band as i32);
                            let x = (panel_x + c) as i32;
                            fill_rect_clip(buf, stride, x, yb - hgt + 1, x + 1, yb + 1, color);
                        }
                    }
                }
                // the hovered TXOP's MPDUs of this queue: a white tick over the arrival band where each arrived, a line
                // along the top from the first arrival to the TXOP start (how long they waited), and the TXOP itself thicker
                for (_, t0, t1, arr) in hovered.iter().filter(|(f, ..)| *f == (*sta, *ac)) {
                    let mark = 0xf0f0ff;
                    let first = arr.iter().copied().fold(*t0, f64::min);
                    let (xa, xs, xe) = (x_of(first, view, panel_x, panel_w), x_of(*t0, view, panel_x, panel_w), x_of(*t1, view, panel_x, panel_w));
                    let (lo, hi) = (panel_x as i32, (panel_x + panel_w) as i32);
                    let clip = |x: i32| x.clamp(lo, hi);
                    let ty = y as i32 + 1;
                    // waiting: thin line with an end cap at the first arrival; the TXOP: thick
                    fill_rect_clip(buf, stride, clip(xa), ty + 1, clip(xs) + 1, ty + 3, mark);
                    if xa >= lo && xa < hi { fill_rect_clip(buf, stride, xa, ty, xa + 2, ty + 7, mark); }
                    fill_rect_clip(buf, stride, clip(xs), ty, clip(xe).max(clip(xs) + 1), ty + 5, mark);
                    let tick_h = (row_h as i32 / 2).max(band as i32 + 4);
                    for &a in arr.iter() {
                        let x = x_of(a, view, panel_x, panel_w);
                        if x >= lo && x < hi {
                            fill_rect_clip(buf, stride, x, yb - tick_h + 1, x + 2, yb + 1, mark);
                        }
                    }
                    if !arr.is_empty() {
                        let waits = arr.iter().map(|a| (t0 - a) * 1e6);
                        let (wl, wh) = waits.fold((f64::MAX, f64::MIN), |(l, h), w| (l.min(w), h.max(w)));
                        let label = if wh - wl < 0.5 {
                            format!("{} MPDU{} waited {:.0} us", arr.len(), if arr.len() == 1 { "" } else { "s" }, wh)
                        } else {
                            format!("{} MPDUs waited {:.0}-{:.0} us", arr.len(), wl, wh)
                        };
                        let lw = text_w(&label, 1.0) as i32;
                        // right of the first arrival, kept inside the panel
                        let lx = (clip(xa) + 4).min(hi - lw - 2).max(lo + 2);
                        fill_rect_clip(buf, stride, lx - 2, ty + 8, lx + lw + 2, ty + 17, 0x0a0a12);
                        render_text(buf, &label, lx as usize, (ty + 9) as usize, stride, mark, 1.0);
                    }
                }
            }
            if mode == 2 {
                // colour scale (log): 0 .. global max
                let (bw, bx) = (70usize, hx.saturating_sub(70 + 60 + 20));
                for i in 0..bw {
                    fill_rect(buf, stride, bx + i, panel_y + 2, 1, 6, heat_color(i as f64 / bw as f64));
                }
                render_text(buf, &format!("0..{}", gmax), bx + bw + 4, panel_y + 1, stride, 0x9090a8, 1.0);
            }
        }
        _ => {
            // stacked area, lowest priority at the bottom
            let order: Vec<usize> = (0..rows.len()).rev().collect();
            let mut totals = vec![0usize; panel_w];
            for c in &cols { for (i, &d) in c.iter().enumerate() { totals[i] += d; } }
            let tmax = totals.iter().copied().max().unwrap_or(0).max(1);
            let plot_h = panel_h.saturating_sub(4) as f64;
            let base = (panel_y + panel_h - 2) as i32;
            for col in 0..panel_w {
                let x = (panel_x + col) as i32;
                let mut cum = 0usize;
                for &r in &order {
                    let d = cols[r][col];
                    if d == 0 { continue; }
                    let (sta, ac) = *rows[r].0;
                    let y_lo = base - ((cum as f64 / tmax as f64) * plot_h) as i32;
                    cum += d;
                    let y_hi = base - ((cum as f64 / tmax as f64) * plot_h).ceil() as i32;
                    let dimmed = is_dimmed(sta, ac, highlight);
                    fill_rect_clip(buf, stride, x, y_hi, x + 1, y_lo, maybe_dim(qkey_color(sta, ac), dimmed));
                }
            }
            // legend: swatch + name, one compact line per queue
            for (r, ((sta, ac), _)) in rows.iter().enumerate().skip(start).take(shown.min(fit)) {
                let y = panel_y + 1 + (r - start) * row_h;
                let dimmed = is_dimmed(*sta, *ac, highlight);
                fill_rect(buf, stride, 0, y + 1, 3, row_h.saturating_sub(2).max(1), maybe_dim(qkey_color(*sta, *ac), dimmed));
                let ty = y + (row_h.saturating_sub(if small { 7 } else { 14 })) / 2;
                render_text(buf, &label_of(*sta, *ac), 8, ty, stride, maybe_dim(0xdddddd, dimmed), scale);
            }
            render_text(buf, &format!("total max {}", tmax), panel_x + 4, panel_y + 1, stride, 0x8a8aa0, 1.0);
        }
    }
    for (r, ((sta, ac), _)) in rows.iter().enumerate().skip(start).take(shown) {
        let y = panel_y + 1 + (r - start) * row_h;
        draw_hover_mark(buf, stride, view, panel_x, panel_w, y, y + row_h - 1, highlight.is_some() && !is_dimmed(*sta, *ac, highlight));
    }
    render_text(buf, &head, hx, panel_y + 1, stride, 0x707088, 1.0);
}

// ----------------------------------------------------------------
fn render_qdepth_panel(
    buf: &mut [u32], stride: usize,
    panel_y: usize, panel_h: usize,
    panel_x: usize, panel_w: usize,
    view: &ViewState, idx: &VizIndex,
    highlight: &Option<ActiveHighlights>,
) {
    fill_rect(buf, stride, panel_x, panel_y, panel_w, panel_h, 0x10101a);
    fill_rect(buf, stride, 0, panel_y, panel_x, panel_h, 0x14141c);
    if view.q_mode != 0 {
        render_qdepth_variant(buf, stride, panel_y, panel_h, panel_x, panel_w, view, idx, highlight);
        return;
    }
    render_text(buf, "QUEUES", 8, panel_y + 6, stride, 0xcccccc, 2);

    let t_lo = view.center_t - view.span_t * 0.5;
    let t_hi = view.center_t + view.span_t * 0.5;

    // ── 1. Aggregate data ────────────────────────────────────────────────
    let mut per_link_map: HashMap<(i32, EdcaAc, u8), Vec<usize>> = HashMap::new();
    for (key, indices) in &idx.qdepth_by_key {
        for &ii in indices {
            if let VizEvent::QueueDepth { .. } = &idx.all[ii] {
                per_link_map
                    .entry((key.0, key.1, key.2))
                    .or_default()
                    .push(ii);
            }
        }
    }
    for v in per_link_map.values_mut() {
        v.sort_by(|&a, &b| event_t(&idx.all[a]).partial_cmp(&event_t(&idx.all[b])).unwrap());
    }

    let mut agg_map: HashMap<(i32, EdcaAc), Vec<(f64, usize)>> = HashMap::new();

    let flow_keys: std::collections::HashSet<(i32, EdcaAc)> = per_link_map
        .keys()
        .map(|&(sta_id, ac, _lid)| (sta_id, ac))
        .collect();

    for (sta_id, ac) in flow_keys {
        let mut all_events: Vec<(f64, u8, usize)> = per_link_map
            .iter()
            .filter(|(&(s, a, _), _)| s == sta_id && a == ac)
            .flat_map(|(&(_, _, lid), indices)| {
                indices.iter().filter_map(move |&ii| {
                    if let VizEvent::QueueDepth { t, depth, .. } = &idx.all[ii] {
                        Some((*t, lid, *depth))
                    } else {
                        None
                    }
                })
            })
            .collect();

        all_events.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let mut link_depths: HashMap<u8, usize> = HashMap::new();
        let series: Vec<(f64, usize)> = all_events
            .into_iter()
            .map(|(t, lid, depth)| {
                link_depths.insert(lid, depth);
                let total: usize = link_depths.values().sum();
                (t, total)
            })
            .collect();

        agg_map.insert((sta_id, ac), series);
    }

    // ── 2. Max depth (zoom-safe) ─────────────────────────────────────────
    let mut max_depth = 1usize;
    for series in agg_map.values() {
        let s = series.partition_point(|(t, _)| *t < t_lo);
        if s > 0 {
            if series[s - 1].1 > max_depth { max_depth = series[s - 1].1; }
        }
        for &(t, depth) in &series[s..] {
            if t > t_hi { break; }
            if depth > max_depth { max_depth = depth; }
        }
    }

    // ── 3. Coordinate helpers ─────────────────────────────────────────────
    let base_y = panel_y as i32 + panel_h as i32 - 2;
    let calc_y = |depth: usize, max: usize| -> i32 {

        let max = max.max(1); // avoid div-by-zero; if max_depth is 0, we'll just draw a flat line at the bottom
        let raw = base_y - (((depth as f64 / max as f64) * (panel_h as f64 - 12.0)) as i32);
        raw.clamp(panel_y as i32, base_y)
    };

    // ── 4. Grid Lines ─────────────────────────────────────────────────────
    let grid_step = match max_depth {
        0..=10   => 2,
        11..=30  => 5,
        31..=100 => 20,
        _        => 50,
    };
    let mut current_unit = grid_step;
    while current_unit <= max_depth {
        let grid_y = calc_y(current_unit, max_depth);
        for gx in panel_x..(panel_x + panel_w) {
            if (gx % 8) < 4 {
                let idx_grid = (grid_y as usize) * stride + gx;
                if idx_grid < buf.len() { buf[idx_grid] = 0x222233; }
            }
        }
        render_text(buf, &format!("{}", current_unit), panel_x + 2, grid_y as usize - 8, stride, 0x444455, 1);
        current_unit += grid_step;
    }

    // ── 5. Draw Series — TWO PASS: fills first, outlines second ──────────
    let mut keys_to_draw: Vec<_> = agg_map.keys().collect();
    keys_to_draw.sort_by_key(|k| (ac_prio(k.1), k.0));
    keys_to_draw.reverse();

    // Helper closure: iterate visible segments for a series
    let visible_segments = |series: &Vec<(f64, usize)>| -> (usize, Vec<(f64, usize)>) {
        let s = series.partition_point(|(t, _)| *t < t_lo);
        (s, series.clone())
    };

    // ── Pass 1: fills only ────────────────────────────────────────────────
    for &&(sta_id, ac) in &keys_to_draw {
        let series = &agg_map[&(sta_id, ac)];
        let is_dl = sta_id < 0;
        let pattern_type = if is_dl { 0 } else { 2 };
        let base_outline = if is_dl {
            ac_color(ac)
        } else {
            [0xFF4444, 0xFF8822, 0xFFCC33, 0xFF55AA, 0x44AAFF, 0x44FF88, 0x8844FF, 0x44FFEE]
                [(sta_id.unsigned_abs() as usize) % 8]
        };
        let dimmed = is_dimmed(sta_id, ac, highlight); // extract your dim logic to a fn
        let outline_color = maybe_dim(base_outline, dimmed);
        let fill_color    = dim_color(outline_color, if dimmed { 2 } else { 6 });
        let fill_alpha    = if dimmed { 0.08 } else { 0.25 }; // slightly reduced for cleaner overlap

        let s = series.partition_point(|(t, _)| *t < t_lo);
        let mut prev: Option<(i32, i32, usize)> = None;
        if s > 0 {
            let (_, depth) = series[s - 1];
            prev = Some((x_of(t_lo, view, panel_x, panel_w), calc_y(depth, max_depth), depth));
        }

        for &(t, depth) in &series[s..] {
            if t > t_hi { break; }
            let x = x_of(t, view, panel_x, panel_w);
            let y = calc_y(depth, max_depth);
            if let Some((px, py, prev_depth)) = prev {
                if prev_depth > 0 {
                    for fill_x in px..x {
                        if fill_x >= panel_x as i32 && fill_x < (panel_x + panel_w) as i32 {
                            draw_vline_alpha(buf, stride, fill_x, py + 2, base_y, fill_color, fill_alpha);
                        }
                    }
                }
            }
            prev = Some((x, y, depth));
        }
        // Extend fill to right edge
        if let Some((px, py, prev_depth)) = prev {
            if prev_depth > 0 {
                let end_x = (panel_x + panel_w) as i32;
                for fill_x in px..end_x {
                    if fill_x >= panel_x as i32 && fill_x < end_x {
                        draw_vline_alpha(buf, stride, fill_x, py + 2, base_y, fill_color, fill_alpha);
                    }
                }
            }
        }
    }

    // ── Pass 2: outlines + legend (drawn over all fills) ──────────────────
    let mut legend_y = panel_y + 24;
    for &&(sta_id, ac) in &keys_to_draw {
        let series = &agg_map[&(sta_id, ac)];
        let is_dl = sta_id < 0;
        let pattern_type = if is_dl { 0 } else { 2 };
        let base_outline = if is_dl {
            ac_color(ac)
        } else {
            [0xFF4444, 0xFF8822, 0xFFCC33, 0xFF55AA, 0x44AAFF, 0x44FF88, 0x8844FF, 0x44FFEE]
                [(sta_id.unsigned_abs() as usize) % 8]
        };
        let dimmed = is_dimmed(sta_id, ac, highlight);
        let outline_color = maybe_dim(base_outline, dimmed);

        let s = series.partition_point(|(t, _)| *t < t_lo);
        let is_active = (s > 0) || (s < series.len() && series[s].0 <= t_hi);

        // Legend
        if is_active && legend_y + 12 < panel_y + panel_h {
            let lbl = if is_dl {
                format!("{:<4} {}", ap_name(sta_id), ac.to_string())
            } else {
                format!("STA{:<3} {}", sta_id, ac.to_string())
            };
            render_text(buf, &lbl, 35, legend_y, stride, maybe_dim(0xdddddd, dimmed), 2);
            for px in 0..22usize {
                if should_draw_pixel(px as i32, pattern_type) {
                    for ty in 0..2 { // 2px instead of 3
                        let idx2 = (legend_y + 5 + ty) * stride + (8 + px);
                        if idx2 < buf.len() { buf[idx2] = outline_color; }
                    }
                }
            }
            legend_y += 20;
        }

        // Outlines
        let hold_y = if s > 0 {
            let (_, depth) = series[s - 1];
            Some(calc_y(depth, max_depth))
        } else {
            None
        };

        let mut pts: Vec<(i32, i32)> = Vec::new();
        let mut prev_y = hold_y;
        for &(t, depth) in &series[s..] {
            if t > t_hi { break; }
            let x = x_of(t, view, panel_x, panel_w);
            let y = calc_y(depth, max_depth);
            if let Some(py) = prev_y {
                pts.push((x, py));   // outgoing level — captures the full step span
            }
            pts.push((x, y));
            prev_y = Some(y);
        }
        let mut envelope_buf: Vec<Option<(i32, i32)>> = Vec::with_capacity(panel_w);
        build_step_envelope(&pts, panel_x, panel_w, hold_y, &mut envelope_buf);

        for (col, entry) in envelope_buf.iter().enumerate() {
            let Some((y_min, y_max)) = *entry else { continue };
            let x = panel_x as i32 + col as i32;
            if !should_draw_pixel(x, pattern_type) { continue; }

            // Bounds guard — y values can be negative or out of range at extreme zoom
            if y_max < panel_y as i32 || y_min > (panel_y + panel_h) as i32 { continue; }
            let y_max_safe = y_max.clamp(panel_y as i32, (panel_y + panel_h - 1) as i32);
            let y_min_safe = y_min.clamp(panel_y as i32, (panel_y + panel_h - 1) as i32);

            // Horizontal step mark (2 px tall) at the current value level —
            // blend so overlapping series stay visible instead of erasing each other
            for ty in 0..2i32 {
                let row = (y_max_safe + ty) as usize;
                let oi  = row * stride + x as usize;
                if oi < buf.len() {
                    buf[oi] = blend_screen(buf[oi], outline_color);
                }
            }

            // Vertical transition bar when the step spans multiple rows
            if y_min_safe < y_max_safe {
                for y in y_min_safe..=y_max_safe {
                    let oi = y as usize * stride + x as usize;
                    if oi < buf.len() {
                        buf[oi] = blend_screen(buf[oi], outline_color);
                    }
                }
            }
        }
        


    }

    render_text(buf, &format!("max={}", max_depth), panel_x + 6, panel_y + 6, stride, 0x888899, 2);
}


/// One TXOP on a link: the A-MPDUs sent in it (`TxopStart` indices into `VizIndex::all`).
pub struct TxopGroup {
    pub t: f64,
    pub end: f64,
    pub holder: MacKey,
    pub legs: Vec<usize>,
}

impl TxopGroup {
    /// A lone A-MPDU: drawn over the whole TXOP.
    fn is_plain_su(&self) -> bool {
        self.legs.len() == 1
    }
}

pub struct VizIndex {
    pub all: Vec<VizEvent>,
    pub txops_by_link: HashMap<u8, Vec<usize>>,
    /// TXOPs per link in time order.
    pub txop_groups_by_link: HashMap<u8, Vec<TxopGroup>>,
    pub collisions_by_link: HashMap<u8, Vec<usize>>,
    pub backoff_by_key: HashMap<MacKey, Vec<usize>>,
    pub qdepth_by_key: HashMap<MacKey, Vec<usize>>,
    pub bandwidth_by_ip: HashMap<IpAddr, Vec<usize>>,
    pub mac_keys_sorted: Vec<MacKey>,
    pub t_min: f64,
    pub t_max: f64,
    pub qdepth_series: Vec<((i32, EdcaAc), Vec<(f64, usize)>)>,
    pub cw_series: HashMap<MacKey, Vec<(f64, u32, bool)>>,
    /// Packet arrivals per (station, AC) queue, in time order: the +1 steps of its `QueueDepth` events.
    pub arrivals: HashMap<(i32, EdcaAc), Vec<f64>>,
    /// Lane titles (default "LINK n").
    pub lane_names: HashMap<u8, String>,
    /// Lanes two thirds as tall (three or more lanes), leaving the room to the panels below.
    pub compact_lanes: bool,
}
 impl VizIndex {
    pub fn build(mut events: Vec<VizEvent>) -> Self {
        events.sort_by(|a, b| event_t(a).partial_cmp(&event_t(b)).unwrap());

        let t_min = events.first().map(event_t).unwrap_or(0.0);
        let t_max = events.last().map(|e| event_end(e)).unwrap_or(1.0);

        // ── Phase 1: build index maps from events ─────────────────────────
        let mut txops_by_link:      HashMap<u8,     Vec<usize>> = HashMap::new();
        let mut collisions_by_link: HashMap<u8,     Vec<usize>> = HashMap::new();
        let mut backoff_by_key:     HashMap<MacKey, Vec<usize>> = HashMap::new();
        let mut qdepth_by_key:      HashMap<MacKey, Vec<usize>> = HashMap::new();
        let mut bandwidth_by_ip:    HashMap<IpAddr, Vec<usize>> = HashMap::new();
        let mut keys:               HashSet<MacKey>             = HashSet::new();
        let mut txop_groups_by_link: HashMap<u8, Vec<TxopGroup>> = HashMap::new();

        // A TXOP's events share `t`/`end`/holder and, the sort being stable, arrive together.
        fn group(by_link: &mut HashMap<u8, Vec<TxopGroup>>, link_id: u8, t: f64, end: f64, holder: MacKey) -> &mut TxopGroup {
            let v = by_link.entry(link_id).or_default();
            let same = v.last().map_or(false, |g| g.t == t && g.end == end && g.holder == holder);
            if !same {
                v.push(TxopGroup { t, end, holder, legs: Vec::new() });
            }
            v.last_mut().unwrap()
        }

        for (i, ev) in events.iter().enumerate() {
            match ev {
                VizEvent::TxopStart { t, end, link_id, owner, sender, .. } => {
                    txops_by_link.entry(*link_id).or_default().push(i);
                    keys.insert(*sender);
                    group(&mut txop_groups_by_link, *link_id, *t, *end, *owner).legs.push(i);
                }
                VizEvent::Collision { link_id, contenders, .. } => {
                    collisions_by_link.entry(*link_id).or_default().push(i);
                    for k in contenders { keys.insert(*k); }
                }
                VizEvent::BackoffSnap { mac_key, .. } => {
                    backoff_by_key.entry(*mac_key).or_default().push(i);
                    keys.insert(*mac_key);
                }
                VizEvent::QueueDepth { mac_key, .. } => {
                    qdepth_by_key.entry(*mac_key).or_default().push(i);
                    keys.insert(*mac_key);
                }
                VizEvent::BandwidthChange { ip, .. } => {
                    bandwidth_by_ip.entry(*ip).or_default().push(i);
                }
            }
        }

        let mut mac_keys_sorted: Vec<MacKey> = keys.into_iter().collect();
        mac_keys_sorted.sort_by_key(|k| (k.2, k.0 >= 0, k.0, ac_prio(k.1)));

        // ── Phase 2: pre-aggregate qdepth series ──────────────────────────
        // `events` is a plain local Vec here — no borrow conflict possible.
        let qdepth_series: Vec<((i32, EdcaAc), Vec<(f64, usize)>)> = {
            // per-(sta_id, ac, link_id) → sorted event indices
            let mut per_link: HashMap<(i32, EdcaAc, u8), Vec<usize>> = HashMap::new();
            for (key, indices) in &qdepth_by_key {
                for &ii in indices {
                    if let VizEvent::QueueDepth { .. } = &events[ii] {
                        per_link.entry((key.0, key.1, key.2)).or_default().push(ii);
                    }
                }
            }
            for v in per_link.values_mut() {
                v.sort_by(|&a, &b|
                    event_t(&events[a]).partial_cmp(&event_t(&events[b])).unwrap());
            }

            let flow_keys: HashSet<(i32, EdcaAc)> =
                per_link.keys().map(|&(s, a, _)| (s, a)).collect();

            let mut agg: HashMap<(i32, EdcaAc), Vec<(f64, usize)>> = HashMap::new();
            for (sta_id, ac) in &flow_keys {
                // Bind a shared reference to events BEFORE entering the closure.
                // Shared references are Copy, so `move` inside the inner closure
                // just copies the thin pointer — no move-out-of-Vec error.
                let ev_ref: &[VizEvent] = &events;
                let mut all_ev: Vec<(f64, u8, usize)> = per_link.iter()
                    .filter(|(&(s, a, _), _)| s == *sta_id && a == *ac)
                    .flat_map(|(&(_, _, lid), idxs)| {
                        // `ev_ref` is Copy (&[_]), so moving it into each inner
                        // closure is identical to copying — compiles fine.
                        idxs.iter().filter_map(move |&ii| {
                            if let VizEvent::QueueDepth { t, depth, .. } = &ev_ref[ii] {
                                Some((*t, lid, *depth))
                            } else { None }
                        })
                    })
                    .collect();

                all_ev.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

                let mut link_depths: HashMap<u8, usize> = HashMap::new();
                let series: Vec<(f64, usize)> = all_ev.into_iter()
                    .map(|(t, lid, depth)| {
                        link_depths.insert(lid, depth);
                        (t, link_depths.values().sum::<usize>())
                    })
                    .collect();
                agg.insert((*sta_id, *ac), series);
            }

            let mut sorted_keys: Vec<(i32, EdcaAc)> = agg.keys().copied().collect();
            sorted_keys.sort_by_key(|k| (ac_prio(k.1), k.0));
            sorted_keys.reverse();
            sorted_keys.into_iter()
                .map(|k| (k, agg.remove(&k).unwrap()))
                .collect()
        };

        // ── Phase 3: pre-aggregate CW series ─────────────────────────────
        let cw_series: HashMap<MacKey, Vec<(f64, u32, bool)>> = {
            let mut map: HashMap<MacKey, Vec<(f64, u32, bool)>> = HashMap::new();
            for (key, indices) in &backoff_by_key {
                let mut series: Vec<(f64, u32, bool)> = indices.iter()
                    .filter_map(|&ii| {
                        // Plain borrow of events — no closure capture issue here.
                        if let VizEvent::BackoffSnap { t, cw, frozen, .. } = &events[ii] {
                            Some((*t, *cw, *frozen))
                        } else { None }
                    })
                    .collect();
                series.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                if !series.is_empty() { map.insert(*key, series); }
            }
            map
        };

        // XRSIM_VIZ_DUMP_QDEPTH=t_lo,t_hi: print the aggregated queue series inside [t_lo, t_hi] and exit.
        if let Ok(win) = std::env::var("XRSIM_VIZ_DUMP_QDEPTH") {
            let mut it = win.split(',').filter_map(|s| s.trim().parse::<f64>().ok());
            let (lo, hi) = (it.next().unwrap_or(0.0), it.next().unwrap_or(f64::MAX));
            for ((sta, ac), ser) in &qdepth_series {
                let in_win: Vec<&(f64, usize)> = ser.iter().filter(|(t, _)| *t >= lo && *t <= hi).collect();
                let held = ser.iter().take_while(|(t, _)| *t < lo).last();
                let (mn, mx) = in_win.iter().fold((usize::MAX, 0), |(a, b), (_, d)| (a.min(*d), b.max(*d)));
                println!("[QDUMP] ({sta},{ac:?}) {} events in [{lo},{hi}], min={mn} max={mx}, value held at window start: {:?}", in_win.len(), held);
                for (t, d) in in_win.iter().take(400) {
                    println!("[QDUMP]   t={t:.9} depth={d}");
                }
            }
            for ev in &events {
                match ev {
                    VizEvent::QueueDepth { t, mac_key, depth, .. } if *t >= lo && *t <= hi =>
                        println!("[QRAW] t={t:.9} Q   key={mac_key:?} depth={depth}"),
                    VizEvent::TxopStart { t, end, link_id, owner, sender, dest_id, ampdu_packets, idle_since, ppdu_t0, ppdu_t1, .. } if *t >= lo && *t <= hi =>
                    {
                        println!("[QRAW] t={t:.9} TX  link={link_id} owner={owner:?} sender={sender:?} dest={dest_id} mpdus={ampdu_packets} end={end:.9} ppdu=[{ppdu_t0:.9},{ppdu_t1:.9}] idle_since={idle_since:.9}");
                        println!("[QRAW] t={t:.9} TXD {ev:?}");
                    }
                    other if (lo..=hi).contains(&event_t(other)) =>
                        println!("[QRAW] t={:.9} EV  {other:?}", event_t(other)),
                    _ => {}
                }
            }
            std::process::exit(0);
        }

        let arrivals: HashMap<(i32, EdcaAc), Vec<f64>> = {
            let mut last: HashMap<MacKey, usize> = HashMap::new();
            let mut map: HashMap<(i32, EdcaAc), Vec<f64>> = HashMap::new();
            for ev in &events {
                if let VizEvent::QueueDepth { t, mac_key, depth, .. } = ev {
                    if last.insert(*mac_key, *depth).unwrap_or(0) + 1 == *depth {
                        map.entry((mac_key.0, mac_key.1)).or_default().push(*t);
                    }
                }
            }
            map
        };

        // ── Assemble — events is moved here once, everything else is ready ─
        VizIndex {
            t_min,
            t_max,
            all: events,
            txops_by_link,
            txop_groups_by_link,
            collisions_by_link,
            backoff_by_key,
            qdepth_by_key,
            bandwidth_by_ip,
            mac_keys_sorted,
            qdepth_series,
            cw_series,
            arrivals,
            lane_names: HashMap::new(),
            compact_lanes: false,
        }
    }
}
 
fn latest_at<'a>(indices: &'a [usize], all: &'a [VizEvent], t_cursor: f64)
    -> Option<&'a VizEvent>
{
    let pos = indices.partition_point(|&i| event_t(&all[i]) <= t_cursor);
    if pos == 0 { None } else { Some(&all[indices[pos - 1]]) }
}

fn handle_controls_input(
    window:   &Window,
    state:    &mut UnifiedState,
    t_min:    f64,
    stride:   usize,
) {
    let (mx, my) = ui_scale::mouse_pos(window).unwrap_or((0.0, 0.0));
    let left_down = window.get_mouse_down(MouseButton::Left);
    let clicked   = left_down && !state.prev_left_down;
    let in_bar    = (my as usize) >= HUD_H && (my as usize) < HUD_H + CONTROLS_H;

    // ── Row-1 button clicks ───────────────────────────────────────────────────
    if clicked && in_bar {
        let mx_u = mx as usize;
        let my_u = my as usize;
        if my_u >= BTN_Y && my_u < BTN_Y + BTN_H {
            if mx_u >= BTN_PLAY_X  && mx_u < BTN_PLAY_X  + BTN_PLAY_W {
                state.playing    = !state.playing;
                state.last_frame = Instant::now();
            }
            if mx_u >= BTN_RESET_X && mx_u < BTN_RESET_X + BTN_RESET_W {
                state.ch_view.cursor_t  = t_min;
                state.abr_view.cursor_t = t_min;
                state.playing           = false;
            }
        }
    }

    // ── Speed slider drag ─────────────────────────────────────────────────────
    let _ = stride;
    let over_slider = in_bar
        && (my as usize) >= SLIDER_ROW_Y && (my as usize) < SLIDER_ROW_Y + SLIDER_ROW_H
        && (mx as usize) + 6 >= SLIDER_X0 && (mx as usize) <= SLIDER_X1 + 6;

    if left_down && (over_slider || state.slider_drag) {
        state.slider_drag = true;
        let frac = ((mx as f64) - SLIDER_X0 as f64) / (SLIDER_X1 - SLIDER_X0) as f64;
        state.playback_speed = frac_to_speed(frac);
    } else {
        state.slider_drag = false;
    }

    state.prev_left_down = left_down;
}
 
fn handle_input(window: &Window, view: &mut ViewState, idx: &VizIndex, panel_w: usize, panel_x: usize, num_links: u8) {
    if window.is_key_down(Key::Left)  || window.is_key_down(Key::A) { view.center_t -= view.span_t * 0.02; }
    if window.is_key_down(Key::Right) || window.is_key_down(Key::D) { view.center_t += view.span_t * 0.02; }
    if window.is_key_pressed(Key::Equal,      minifb::KeyRepeat::Yes)
    || window.is_key_pressed(Key::NumPadPlus, minifb::KeyRepeat::Yes) { view.span_t *= 0.8; }
    if window.is_key_pressed(Key::Minus,       minifb::KeyRepeat::Yes)
    || window.is_key_pressed(Key::NumPadMinus, minifb::KeyRepeat::Yes) { view.span_t *= 1.25; }
    if window.is_key_pressed(Key::Home,  minifb::KeyRepeat::No) { view.center_t = idx.t_min + view.span_t * 0.5; }
    if window.is_key_pressed(Key::End,   minifb::KeyRepeat::No) { view.center_t = idx.t_max - view.span_t * 0.5; }
    if window.is_key_pressed(Key::Space, minifb::KeyRepeat::No) { view.paused = !view.paused; }
    if window.is_key_pressed(Key::Q, minifb::KeyRepeat::No) { view.q_mode = (view.q_mode + 1) % 4; }
    if window.is_key_pressed(Key::W, minifb::KeyRepeat::No) { view.q_shared = !view.q_shared; }
    if window.is_key_pressed(Key::B, minifb::KeyRepeat::No) { view.show_bo_matrix = !view.show_bo_matrix; }
    if window.is_key_pressed(Key::I, minifb::KeyRepeat::No) { view.show_arrivals = !view.show_arrivals; }
    if window.is_key_pressed(Key::Tab, minifb::KeyRepeat::No) {
        view.selected_link = match view.selected_link {
            None => Some(0),
            Some(l) if l + 1 < num_links => Some(l + 1),
            _ => None,
        };
    }
    let (mx, _my) = ui_scale::mouse_pos(window).unwrap_or((0.0, 0.0));
    if window.get_mouse_down(MouseButton::Left) {
        if let Some((mx0, ct0)) = view.mouse_drag {
            let dx_px    = (mx - mx0) as f64;
            let dt_per_px = view.span_t / panel_w as f64;
            view.center_t = ct0 - dx_px * dt_per_px;
        } else {
            view.mouse_drag = Some((mx, view.center_t));
        }
    } else {
        view.mouse_drag = None;
    }
    if window.get_mouse_down(MouseButton::Right) {
        // FIX: Use the actual panel coordinates
        let n = ((mx as f64) - panel_x as f64) / panel_w as f64;
        let clicked_t = view.center_t - view.span_t * 0.5 + view.span_t * n;
        view.cursor_t = clicked_t.clamp(idx.t_min, idx.t_max);
    }if let Some((_, scroll_y)) = window.get_scroll_wheel() {
        if scroll_y.abs() > 0.0 {
            let factor = if scroll_y > 0.0 { 0.85 } else { 1.18 };
            // FIX: Use the actual panel coordinates here too
            let n = ((mx as f64) - panel_x as f64) / panel_w as f64;
            let n = n.clamp(0.0, 1.0);
            let t_at_cur = view.center_t - view.span_t * 0.5 + view.span_t * n;
            view.span_t *= factor;
            view.center_t = t_at_cur - view.span_t * (n - 0.5);
        }
    }
    if window.is_key_down(Key::PageDown) { view.row_scroll += 4; }
    if window.is_key_down(Key::PageUp)   { view.row_scroll -= 4; }
}

 pub struct ActiveHighlights {
    pub keys: HashSet<MacKey>,
    pub txops: HashSet<(MacKey, i32)>, // (owner_mac, dest_id)
}

fn clamp_view(view: &mut ViewState, idx: &VizIndex) {
    let total = idx.t_max - idx.t_min;
    // Cap span to the full range — never allow span > total
    view.span_t = view.span_t.clamp(1e-6, total.max(1e-6));

    let lo = idx.t_min + view.span_t * 0.5;
    let hi = idx.t_max - view.span_t * 0.5;

    // Guard against FP rounding making lo > hi (span ≈ total edge case)
    view.center_t = if lo <= hi {
        view.center_t.clamp(lo, hi)
    } else {
        (idx.t_min + idx.t_max) * 0.5  // just center the view
    };

    view.row_scroll = view.row_scroll.max(0);
}
 
fn x_of(t: f64, view: &ViewState, panel_x: usize, panel_w: usize) -> i32 {
    let t0 = view.center_t - view.span_t * 0.5;
    let n  = (t - t0) / view.span_t;
    panel_x as i32 + (n * panel_w as f64) as i32
}
 
// fn fill_rect(buf: &mut [u32], stride: usize, x: usize, y: usize, w: usize, h: usize, c: u32) {
//     let h_buf = buf.len() / stride;
//     for yy in y..(y + h).min(h_buf) {
//         let row = yy * stride;
//         for xx in x..(x + w).min(stride) { buf[row + xx] = c; }
//     }
// }
 fn fill_rect(buf: &mut [u32], stride: usize, x: usize, y: usize, w: usize, h: usize, c: u32) {
    let h_buf = buf.len() / stride.max(1);
    // a rectangle starting past the right edge (a window dragged very narrow) is empty, not an inverted slice
    if x >= stride { return; }
    let x_end = (x + w).min(stride);
    for yy in y..(y + h).min(h_buf) {
        buf[yy * stride + x..yy * stride + x_end].fill(c);
    }
}
fn ac_color(ac: EdcaAc) -> u32 {
    match ac {
        EdcaAc::Voice      => 0xff66cc,
        EdcaAc::Video      => 0x66ccff,
        EdcaAc::BestEffort => 0x88dd88,
        EdcaAc::Background => 0xaaaaaa,
    }
}
 
fn render_hud(buf: &mut [u32], stride: usize, view: &ViewState, idx: &VizIndex) {
    fill_rect(buf, stride, 0, 0, stride, 50, 0x1a1a24);
    let span_ms = view.span_t * 1000.0;
    let label   = format!(
        "t = {:.6}s   span = {:.3}ms   range = [{:.3}, {:.3}]s   {} events   {}",
        view.center_t, span_ms, idx.t_min, idx.t_max, idx.all.len(),
        if view.paused { "PAUSED" } else { "" },
    );
    render_text(buf, &label, 12, 16, stride, 0xeeeeee, 2);
    let filter = match view.selected_link {
        Some(l) => format!("[link filter: L{}]", l),
        None    => "[link filter: ALL]".to_string(),
    };
    render_text(buf, &filter, stride.saturating_sub(260), 16, stride, 0xaaccff, 1);
    render_text(
        buf,
        "L-Click: drag pan   R-Click: set cursor   Scroll: zoom in/out   Tab filter-link   ",
        12, 36, stride, 0x888899, 1,
    );
    let mut leg_x = stride.saturating_sub(450);
    let leg_y     = 36;
    render_text(buf, "LEGEND:", leg_x, leg_y, stride, 0x888899, 1);
    leg_x += 60;
    for (_ac, name, col) in [
        (EdcaAc::Voice,      "VO", 0xff66cc_u32),
        (EdcaAc::Video,      "VI", 0x66ccff_u32),
        (EdcaAc::BestEffort, "BE", 0x88dd88_u32),
        (EdcaAc::Background, "BK", 0xaaaaaa_u32),
    ] {
        fill_rect(buf, stride, leg_x, leg_y, 10, 10, col);
        render_text(buf, name, leg_x + 14, leg_y - 2, stride, 0xeeeeee, 1);
        leg_x += 40;
    }
}
 
pub fn event_t(ev: &VizEvent) -> f64 {
    match ev {
        VizEvent::TxopStart      { t, .. } => *t,
        VizEvent::Collision      { t, .. } => *t,
        VizEvent::BackoffSnap    { t, .. } => *t,
        VizEvent::QueueDepth     { t, .. } => *t,
        VizEvent::BandwidthChange { t, .. } => *t,
    }
}

pub fn event_end(ev: &VizEvent) -> f64 {
    match ev {
        VizEvent::TxopStart      { end, .. } => *end,
        VizEvent::Collision      { end, .. } => *end,
        VizEvent::BackoffSnap    { t, .. } => *t,
        VizEvent::QueueDepth     { t, .. } => *t,
        VizEvent::BandwidthChange { end, .. } => *end,
    }
}
 
use crate::lib::models_mm1k::{AP_X, AP_Y, EDCA_TABLE};
fn aifs_secs_for_ac(ac: EdcaAc) -> f64 {
    // Standard Wi-Fi timings (adjust if your simulator uses 2.4GHz)
    const SIFS_US: f64 = 16.0;
    const SLOT_TIME_US: f64 = 9.0;

    let aifsn = match ac {
        EdcaAc::Voice      => EDCA_TABLE[0].aifsn,
        EdcaAc::Video      => EDCA_TABLE[1].aifsn,
        EdcaAc::BestEffort => EDCA_TABLE[2].aifsn,
        EdcaAc::Background => EDCA_TABLE[3].aifsn,
    };

    // (SIFS + AIFSN * SlotTime) converted to seconds
    (SIFS_US + (aifsn as f64 * SLOT_TIME_US)) * 1e-6
}
 
fn should_draw_pixel(x: i32, pattern_type: usize) -> bool {
    match pattern_type {
        1 => (x / 6) % 2 == 0,
        2 => (x / 2) % 2 == 0,
        _ => true,
    }
}
 
fn draw_hline(buf: &mut [u32], stride: usize, x0: i32, x1: i32, y: i32, c: u32) {
    let h = (buf.len() / stride) as i32;
    if y < 0 || y >= h { return; }
    let (a, b) = if x0 <= x1 { (x0, x1) } else { (x1, x0) };
    let a = a.max(0) as usize;
    let b = (b.min(stride as i32 - 1)) as usize;
    let row = (y as usize) * stride;
    for x in a..=b {
        buf[row + x] = blend_screen(buf[row + x], c);
    }
}
 
fn draw_vline(buf: &mut [u32], stride: usize, x: i32, y0: i32, y1: i32, color: u32) {
    if x < 0 { return; }
    let (y_min, y_max) = if y0 <= y1 { (y0, y1) } else { (y1, y0) };
    for y in y_min..=y_max {
        if y >= 0 {
            let i = y as usize * stride + x as usize;
            if i < buf.len() {
                buf[i] = blend_screen(buf[i], color);
            }
        }
    }
}


// ##########################################################################################################################################################
// ##########################################################################################################################################################
// ##########################################################################################################################################################
// ##########################################################################################################################################################
// ##########################################################################################################################################################
// ##########################################################################################################################################################
// #############################################################################
/////////////////////////////////////// UNIFIED VIEWER /////////////////////////////////////////
// ##########################################################################################################################################################
// ##########################################################################################################################################################
// ##########################################################################################################################################################
// ##########################################################################################################################################################

//
// Combines the ABR metrics viewer and the Channel/Queue viewer into a single
// resizable minifb window.  A draggable vertical splitter divides the two panels;
// the cursor is shared (right-click moves it in both); zoom/pan are independent.
//
// Drop-in usage:
//
//   run_unified_viewer(viz_idx, abr_idx, &link_configs);
//
// Everything else (render_abr_strip, render_link_lane, …) stays unchanged.

// Pull in the two existing viewer modules' internals.
// Adjust paths to match your actual module layout.

// ── Layout constants ──────────────────────────────────────────────────────────

const WIN_W:     usize = 2200;   // default window width  (user can resize)
const WIN_H:     usize = 950;    // default window height
const HUD_H:     usize = menu_bar::BAR_H; // the menu bar at the top
/// The real-net viewer's top HUD strip (two lines of text).
const TIME_AXIS: usize = 22;     // shared bottom time-axis strip
/// The channel panel's least width beyond its label column.
const CH_MIN_PLOT_W: usize = 160;
/// Leftmost and rightmost position (px) of the splitter in a window `w` wide: the channel panel keeps its label column
/// and a plot, the ABR side keeps 400 px.
fn split_limits(w: usize) -> (usize, usize) {
    let lo = 240 + CH_MIN_PLOT_W;
    (lo, w.saturating_sub(400).max(lo))
}
/// Smallest logical frame the unified layout can draw (the sidebars, the splitter limits and the bottom panels).
const MIN_FRAME_W: usize = 880;
const MIN_FRAME_H: usize = 400;
fn window_too_small(w: usize, h: usize) -> bool { w < MIN_FRAME_W || h < MIN_FRAME_H }
const SPLITTER_W: usize = 6;     // draggable divider width

// ── Input focus: which panel receives keyboard zoom/pan ───────────────────────

#[derive(Clone, Copy, PartialEq)]
enum Focus { Channel, Abr }

// ── Splitter drag state ───────────────────────────────────────────────────────

struct SplitterDrag {
    start_mx:     f32,
    start_split:  f32,   // split_frac at drag start
}

// ── Unified viewer state ──────────────────────────────────────────────────────

struct UnifiedState {
    // Fraction of (total width) given to the left (Channel) panel.
    // Range: 0.2 .. 0.8
    split_frac:    f32,
    splitter_drag: Option<SplitterDrag>,
    focus:         Focus,
    // ── Channel viewer ────────────────────────────────────────────────────────
    ch_view:       ViewState,
    // ── ABR viewer ────────────────────────────────────────────────────────────
    abr_view:      AbrViewState,
    // Sync mode: when true, right-click moves both cursors together.
    sync_cursor:   bool,
    zoom_mode:  bool,
    zoom_drag:  Option<ZoomDrag>,

    prev_mouse_left: bool,   // for toggle on click

    /// Playback mode: when true, the view auto-advances in real-time (scaled by playback_speed).
    playing:        bool,
    playback_speed: f64,       // sim-seconds advanced per real-second
    last_frame:     Instant,
    slider_drag:    bool,
    prev_left_down: bool,      // for click edge detection on buttons
    prev_right_down: bool,
    /// Time and x of the last zoom-mode click, for the double-click "back to the full view".
    last_zoom_click: Option<(Instant, f32)>,
    /// (y, height) of the CW and queue panels as drawn last frame, for routing the wheel to their rows.
    cw_panel_rect: (usize, usize),
    q_panel_rect: (usize, usize),
}



#[derive(Clone, Copy)]
struct ZoomDrag {
    x0: f32, y0: f32,   // anchor (mouse-down)
    x1: f32, y1: f32,   // current mouse
    panel: Focus,
}
impl UnifiedState {
    fn new(viz_idx: &VizIndex, abr_idx: &AbrVizIndex) -> Self {
        let ch_full  = viz_idx.t_max  - viz_idx.t_min;
        let abr_full = abr_idx.t_max - abr_idx.t_min;

        Self {
            split_frac:    0.50,
            splitter_drag: None,
            focus:         Focus::Channel,

            ch_view: ViewState {
                center_t:      viz_view_override().map_or((viz_idx.t_min  + viz_idx.t_max)  * 0.5, |v| v.0),
                span_t:        viz_view_override().map_or(ch_full, |v| v.1),
                cursor_t:      viz_view_override().map_or(viz_idx.t_min, |v| v.0),
                paused:        false,
                selected_link: None,
                row_scroll:    0,
                mouse_drag:    None,
                show_overhead: true,
                q_mode:        std::env::var("XRSIM_VIZ_QMODE").ok().and_then(|v| v.parse().ok()).unwrap_or(1),
                q_shared:      false,
                q_scroll:      0,
                cw_scroll:     0,
                show_bo_matrix: std::env::var("XRSIM_VIZ_NOBO").is_err(),
                hover_t:       None,
                hover_txop:    None,
                show_arrivals: true,
                hover_coll:    None,
                hover_leg:     None,
            },

            abr_view: AbrViewState {
                center_t:   (abr_idx.t_min + abr_idx.t_max) * 0.5,
                span_t:     abr_full,
                cursor_t:   abr_idx.t_min,
                mouse_drag: None,
            },

            sync_cursor: true,
            zoom_mode:  false,
            zoom_drag:  None,
            prev_mouse_left: false, 
            playing:        false,
            playback_speed: 18e-6,         // default: 18 us/s
            last_frame:     Instant::now(),
            slider_drag:    false,
            prev_left_down: false,
            prev_right_down: false,
            last_zoom_click: None,
            cw_panel_rect: (0, 0),
            q_panel_rect: (0, 0),
        }
    }
}

// ── Shared HUD ────────────────────────────────────────────────────────────────

fn draw_circle(buf: &mut [u32], stride: usize, cx: i32, cy: i32, r: i32, color: u32) {
    let mut x = r; let mut y = 0i32; let mut err = 0i32;
    while x >= y {
        for &(dx, dy) in &[
            ( x, y),( y, x),(-y, x),(-x, y),
            (-x,-y),(-y,-x),( y,-x),( x,-y),
        ] {
            let (px, py) = (cx + dx, cy + dy);
            if px >= 0 && py >= 0 {
                let i = py as usize * stride + px as usize;
                if i < buf.len() { buf[i] = color; }
            }
        }
        y += 1; err += 1 + 2 * y;
        if 2 * (err - x) + 1 > 0 { x -= 1; err += 1 - 2 * x; }
    }
}

fn draw_magnifier(buf: &mut [u32], stride: usize, cx: i32, cy: i32, color: u32) {
    let r = 6i32;
    draw_circle(buf, stride, cx, cy, r, color);
    draw_circle(buf, stride, cx, cy, r - 1, dim_color(color, 3)); // faint fill hint
    // handle
    for d in 1i32..=6 {
        let i = (cy + r + d) as usize * stride + (cx + r + d) as usize;
        if i < buf.len() { buf[i] = color; }
        // 2px wide handle
        let i2 = (cy + r + d) as usize * stride + (cx + r + d + 1) as usize;
        if i2 < buf.len() { buf[i2] = color; }
    }
}
/// ZOOM and RESET of one panel, a small pair over the panel's top right corner: (zoom x, reset x, y).
fn panel_btns(panel_x: usize, panel_w: usize) -> (usize, usize, usize) {
    let reset_x = (panel_x + panel_w).saturating_sub(PANEL_BTN_W + 4);
    (reset_x.saturating_sub(PANEL_BTN_W + 4), reset_x, HUD_H + CONTROLS_H + 4)
}
const PANEL_BTN_W: usize = 46;
const PANEL_BTN_H: usize = 16;

fn render_panel_buttons(buf: &mut [u32], stride: usize, zoom_mode: bool, panels: [(usize, usize); 2]) {
    use crate::lib::render_text;
    for (px, pw) in panels {
        if pw == 0 { continue; } // a hidden side
        let (zx, rx, y) = panel_btns(px, pw);
        for (x, label, bg, border, fg) in [
            (zx, "ZOOM", if zoom_mode { 0x1a4d1a } else { 0x1e1e2e }, if zoom_mode { 0x44dd44 } else { 0x445566 }, if zoom_mode { 0x88ff88 } else { 0x8899aa }),
            (rx, "RESET", 0x1e1e2e, 0x554433, 0xffcc88),
        ] {
            fill_rect(buf, stride, x, y, PANEL_BTN_W, PANEL_BTN_H, bg);
            abr_draw_hline(buf, stride, x as i32, (x + PANEL_BTN_W - 1) as i32, y as i32, border);
            abr_draw_hline(buf, stride, x as i32, (x + PANEL_BTN_W - 1) as i32, (y + PANEL_BTN_H - 1) as i32, border);
            abr_draw_vline(buf, stride, x as i32, y as i32, (y + PANEL_BTN_H - 1) as i32, border);
            abr_draw_vline(buf, stride, (x + PANEL_BTN_W - 1) as i32, y as i32, (y + PANEL_BTN_H - 1) as i32, border);
            render_text(buf, label, x + (PANEL_BTN_W - 6 * label.len()) / 2, y + 4, stride, fg, 1.0);
        }
    }
}

fn render_zoom_overlay(
    buf:    &mut [u32],
    stride: usize,
    drag:   &ZoomDrag,
    h:      usize,
) {
    let x0 = drag.x0.min(drag.x1) as usize;
    let x1 = drag.x0.max(drag.x1) as usize;
    // time-only zoom: the band covers the whole panel height
    let y0 = HUD_H;
    let y1 = h - TIME_AXIS;

    if x1 <= x0 || y1 <= y0 { return; }

    // Translucent fill
    for y in y0..y1 {
        for x in x0..x1 {
            let i = y * stride + x;
            if i < buf.len() {
                let c = buf[i];
                // brighten slightly — cheap "selection tint"
                let r = (((c >> 16) & 0xFF) + 30).min(255);
                let g = (((c >>  8) & 0xFF) + 30).min(255);
                let b = (( c        & 0xFF) + 50).min(255);
                buf[i] = (r << 16) | (g << 8) | b;
            }
        }
    }

    // Border
    let border = 0x88ccff;
    for x in x0..=x1 {
        let ti = y0 * stride + x; if ti < buf.len() { buf[ti] = border; }
        let bi = y1 * stride + x; if bi < buf.len() { buf[bi] = border; }
    }
    for y in y0..=y1 {
        let li = y * stride + x0; if li < buf.len() { buf[li] = border; }
        let ri = y * stride + x1; if ri < buf.len() { buf[ri] = border; }
    }

    // Magnifier in centre
    let mx = ((x0 + x1) / 2) as i32;
    let my = ((y0 + y1) / 2) as i32;
    draw_magnifier(buf, stride, mx, my, 0xffffff);
}
// ── Shared time-axis at the bottom ───────────────────────────────────────────
//
// The channel viewer's time range drives the left ruler;
// the ABR viewer's drives the right ruler.

fn render_unified_time_axis(
    buf:     &mut [u32],
    stride:  usize,
    w:       usize,
    h:       usize,
    state:   &UnifiedState,
    ch_px:   usize,   // left-panel content x-start
    ch_pw:   usize,   // left-panel content width
    abr_px:  usize,   // right-panel content x-start
    abr_pw:  usize,   // right-panel content width
) {
    use crate::lib::render_text;

    let y = h - TIME_AXIS;
    fill_rect(buf, stride, 0, y, w, TIME_AXIS, 0x0d0d12);

    // ── Left ruler (channel view) ─────────────────────────────────────────────
    let ch = &state.ch_view;
    let t0 = ch.center_t - ch.span_t * 0.5;
    let t1 = ch.center_t + ch.span_t * 0.5;
    let raw = ch.span_t / 8.0;
    let mag = 10f64.powf(raw.log10().floor());
    let step = (raw / mag).ceil() * mag;
    if step > 0.0 && ch_pw > 0 {
        let mut t = (t0 / step).ceil() * step;
        while t <= t1 {
            let x = x_of(t, ch, ch_px, ch_pw);
            draw_vline(buf, stride, x, y as i32, (y + 5) as i32, 0x555566);
            render_text(buf, &tick_label(t, step), x as usize + 2, y + 4, stride, 0x8899aa, 1.0);
            t += step;
        }
    }

    // ── Right ruler (ABR view) ────────────────────────────────────────────────
    let abr = &state.abr_view;
    let t0a = abr.center_t - abr.span_t * 0.5;
    let t1a = abr.center_t + abr.span_t * 0.5;
    let raw_a = abr.span_t / 8.0;
    let mag_a = 10f64.powf(raw_a.log10().floor());
    let step_a = (raw_a / mag_a).ceil() * mag_a;
    if step_a > 0.0 && abr_pw > 0 {
        let mut t = (t0a / step_a).ceil() * step_a;
        while t <= t1a {
            let x = abr_x_of(t, abr, abr_px, abr_pw);
            if x >= abr_px as i32 && x < (abr_px + abr_pw) as i32 {
                abr_draw_vline(buf, stride, x, y as i32, (y + 5) as i32, 0x554455);
                render_text(buf, &tick_label(t, step_a), x as usize + 2, y + 4, stride, 0xaa8899, 1.0);
            }
            t += step_a;
        }
    }
}

// ── Splitter rendering ────────────────────────────────────────────────────────

fn render_splitter(buf: &mut [u32], stride: usize, h: usize, x: usize) {
    let color_base  = 0x334455_u32;
    let color_light = 0x6688aa_u32;
    for yy in HUD_H..(h - TIME_AXIS) {
        for xx in x..(x + SPLITTER_W) {
            let i = yy * stride + xx;
            if i < buf.len() {
                // Draw a bright centre line in the splitter for grip affordance
                buf[i] = if xx == x + SPLITTER_W / 2 { color_light } else { color_base };
            }
        }
    }
}

// ── Input routing ─────────────────────────────────────────────────────────────

fn handle_unified_input(
    window:      &Window,
    state:       &mut UnifiedState,
    ch_idx:      &VizIndex,
    abr_idx:     &AbrVizIndex,
    w:           usize,
    h:           usize,
    // Pre-computed panel geometry so the router can decide quickly
    ch_panel_x:  usize,
    ch_panel_w:  usize,
    abr_panel_x: usize,
    abr_panel_w: usize,
    // the sides' extents and the splitter (either order, or one side alone)
    sides:       &layout::Sides,
    num_links:   u8,
) {
    let (mx, my) = ui_scale::mouse_pos(window).unwrap_or((0.0, 0.0));
    let mx_u     = mx as usize;
    let mouse_just_pressed = window.get_mouse_down(MouseButton::Left) && !state.prev_mouse_left;

    // ── Global keys ─────────────────────────────────────────────────────────

    if window.is_key_pressed(Key::S, minifb::KeyRepeat::No) {
        state.sync_cursor = !state.sync_cursor;
    }

    // Click inside a panel transfers keyboard focus to it (and lets scroll work)
    if window.get_mouse_down(MouseButton::Left) {
        if sides.in_ch(mx_u)       { state.focus = Focus::Channel; }
        else if sides.in_abr(mx_u) { state.focus = Focus::Abr;     }
    }

    // ── Splitter drag ─────────────────────────────────────────────────────────

    let in_splitter = sides.splitter.is_some_and(|sx| mx_u >= sx && mx_u < sx + SPLITTER_W);

    // a drag starts only from a fresh press on the splitter: a held press that began elsewhere (a pan, the ABR sidebar's
    // edge) and slides over it must not grab it
    if window.get_mouse_down(MouseButton::Left)
        && (state.splitter_drag.is_some() || (in_splitter && !state.prev_left_down)) {
        if let Some(ref drag) = state.splitter_drag {
            // outside the window the default mouse mode reports no position (read as x = 0, which threw the splitter to its
            // left limit): follow the pointer with the pass-through mode, and hold the last split if there is none
            let held_x = ui_scale::mouse_pos_any(window).map(|p| p.0).unwrap_or(drag.start_mx + (state.split_frac * w as f32 - drag.start_split * w as f32));
            let dx = (held_x - drag.start_mx) as f32;
            // held at the panels' limits (the same ones the layout applies), so the splitter stays put past them
            let (lo, hi) = sides.limits;
            state.split_frac = (drag.start_split * w as f32 + dx).clamp(lo as f32, hi as f32) / w as f32;
        } else if in_splitter {
            state.splitter_drag = Some(SplitterDrag {
                start_mx:    mx,
                start_split: state.split_frac,
            });
        }
        // Consume — don't route anything else while dragging the splitter
        if state.splitter_drag.is_some() { return; }
    } else {
        state.splitter_drag = None;
    }
    let left_down = window.get_mouse_down(MouseButton::Left);
    let clicked   = left_down && !state.prev_left_down;   // true only on the first frame

    // ── Button clicks — use `clicked`, not `left_down`: each panel's ZOOM (the one zoom mode) and RESET (that panel's view) ──
    if clicked {
        let (mxu, myu) = (mx as usize, my as usize);
        for (px, pw, abr) in [(ch_panel_x, ch_panel_w, false), (abr_panel_x, abr_panel_w, true)] {
            if pw == 0 { continue; } // a hidden side
            let (zx, rx, y) = panel_btns(px, pw);
            if myu < y || myu >= y + PANEL_BTN_H { continue; }
            if mxu >= zx && mxu < zx + PANEL_BTN_W {
                state.zoom_mode = !state.zoom_mode;
                state.zoom_drag = None;
                return;
            }
            if mxu >= rx && mxu < rx + PANEL_BTN_W {
                if abr {
                    state.abr_view.center_t = (abr_idx.t_min + abr_idx.t_max) * 0.5;
                    state.abr_view.span_t = abr_idx.t_max - abr_idx.t_min;
                } else {
                    state.ch_view.center_t = (ch_idx.t_min + ch_idx.t_max) * 0.5;
                    state.ch_view.span_t = ch_idx.t_max - ch_idx.t_min;
                }
                return;
            }
        }
    }
    if window.is_key_pressed(Key::Z, minifb::KeyRepeat::No) {
        state.zoom_mode = !state.zoom_mode;
        state.zoom_drag = None;
    }

    handle_controls_input(&window, state, ch_idx.t_min, w);
    if state.slider_drag {
        state.ch_view.mouse_drag  = None;   // discard any pan anchor
        state.abr_view.mouse_drag = None;
        state.prev_left_down = window.get_mouse_down(MouseButton::Left);
        return;                             // skip all panning / zoom logic
    }
    // ── Zoom mode (as MATLAB's): stays on until toggled off (button or Z). Drag a box to zoom to its time range,
    // click to zoom in x2 about the point, right-click to zoom out x2, double-click to restore the full view.
    if state.zoom_mode {
        let right_down = window.get_mouse_down(MouseButton::Right);
        let right_clicked = right_down && !state.prev_right_down;
        let in_y = (my as usize) >= HUD_H && (my as usize) < h.saturating_sub(TIME_AXIS);
        let panel_at = |x: f32| -> Option<Focus> {
            let xu = x as usize;
            if xu >= ch_panel_x && xu < ch_panel_x + ch_panel_w { Some(Focus::Channel) }
            else if xu >= abr_panel_x && xu < abr_panel_x + abr_panel_w { Some(Focus::Abr) }
            else { None }
        };
        // (panel x, panel w, full t range) of a panel
        let geom = |panel: Focus| match panel {
            Focus::Channel => (ch_panel_x, ch_panel_w, ch_idx.t_min, ch_idx.t_max),
            Focus::Abr => (abr_panel_x, abr_panel_w, abr_idx.t_min, abr_idx.t_max),
        };
        let t_at = |state: &UnifiedState, panel: Focus, x: f32| -> f64 {
            let (px, pw, _, _) = geom(panel);
            let v_center_span = match panel {
                Focus::Channel => (state.ch_view.center_t, state.ch_view.span_t),
                Focus::Abr => (state.abr_view.center_t, state.abr_view.span_t),
            };
            let n = ((x as f64 - px as f64) / pw as f64).clamp(0.0, 1.0);
            v_center_span.0 - v_center_span.1 * 0.5 + v_center_span.1 * n
        };
        // show [t0, t1] in `panel` (and in the other one too with the cursors synced)
        fn set_range(state: &mut UnifiedState, panel: Focus, t0: f64, t1: f64) {
            let (span, center) = ((t1 - t0).max(1e-4), (t0 + t1) * 0.5);
            match panel {
                Focus::Channel => { state.ch_view.span_t = span; state.ch_view.center_t = center; }
                Focus::Abr => { state.abr_view.span_t = span; state.abr_view.center_t = center; }
            }
            if state.sync_cursor {
                state.ch_view.span_t = span; state.ch_view.center_t = center;
                state.abr_view.span_t = span; state.abr_view.center_t = center;
            }
        }
        // zoom by `factor` (< 1 in) keeping time `t` under the same pixel
        let zoom_about = |state: &mut UnifiedState, panel: Focus, x: f32, factor: f64| {
            let (px, pw, _, _) = geom(panel);
            let n = ((x as f64 - px as f64) / pw as f64).clamp(0.0, 1.0);
            let t = t_at(state, panel, x);
            let span = match panel { Focus::Channel => state.ch_view.span_t, Focus::Abr => state.abr_view.span_t } * factor;
            set_range(state, panel, t - span * n, t + span * (1.0 - n));
        };

        if window.get_mouse_down(MouseButton::Left) {
            if let Some(ref mut drag) = state.zoom_drag {
                // already dragging — update endpoint as before
                drag.x1 = mx;
                drag.y1 = my;
            } else if clicked && in_y {
                if let Some(panel) = panel_at(mx) {
                    state.zoom_drag = Some(ZoomDrag { x0: mx, y0: my, x1: mx, y1: my, panel });
                }
            }
        } else if let Some(drag) = state.zoom_drag.take() {
            let (x_lo, x_hi) = (drag.x0.min(drag.x1), drag.x0.max(drag.x1));
            if x_hi - x_lo > 4.0 {
                let (t0, t1) = (t_at(state, drag.panel, x_lo), t_at(state, drag.panel, x_hi));
                set_range(state, drag.panel, t0, t1);
                state.last_zoom_click = None;
            } else {
                let now = Instant::now();
                let double = state.last_zoom_click.is_some_and(|(t, x)| now.duration_since(t).as_millis() < 400 && (x - drag.x0).abs() < 6.0);
                if double {
                    let (_, _, lo, hi) = geom(drag.panel);
                    set_range(state, drag.panel, lo, hi);
                    state.last_zoom_click = None;
                } else {
                    zoom_about(state, drag.panel, drag.x0, 0.5);
                    state.last_zoom_click = Some((now, drag.x0));
                }
            }
        }
        if right_clicked && in_y {
            if let Some(panel) = panel_at(mx) {
                zoom_about(state, panel, mx, 2.0);
            }
        }
    } else {
        state.zoom_drag = None;
    }

    // ── Route scroll / right-click to the panel under the mouse ──────────────

    let mouse_in_ch  = mx_u >= ch_panel_x  && mx_u < ch_panel_x  + ch_panel_w;
    let mouse_in_abr = mx_u >= abr_panel_x && mx_u < abr_panel_x + abr_panel_w;
    
    // Shared right-click cursor
    if window.get_mouse_down(MouseButton::Right) && !state.zoom_mode {
        if mouse_in_ch {
            let n = (mx as f64 - ch_panel_x as f64) / ch_panel_w as f64;
            let t = state.ch_view.center_t - state.ch_view.span_t * 0.5
                  + state.ch_view.span_t * n;
            let t = t.clamp(ch_idx.t_min, ch_idx.t_max);
            state.ch_view.cursor_t = t;
            if state.sync_cursor { state.abr_view.cursor_t = t; }
        } else if mouse_in_abr {
            let n = (mx as f64 - abr_panel_x as f64) / abr_panel_w as f64;
            let t = state.abr_view.center_t - state.abr_view.span_t * 0.5
                  + state.abr_view.span_t * n;
            let t = t.clamp(abr_idx.t_min, abr_idx.t_max);
            state.abr_view.cursor_t = t;
            if state.sync_cursor { state.ch_view.cursor_t = t; }
        }
    }

    // Scroll zoom — route by mouse position; over the label column of the CW / queue panels it scrolls their rows
    if let Some((_, scroll_y)) = window.get_scroll_wheel() {
        let in_labels = mx_u >= sides.ch_x0 && mx_u < ch_panel_x;
        let (cw_y, cw_hh) = state.cw_panel_rect;
        let (q_y, q_hh) = state.q_panel_rect;
        let myu = my as usize;
        if scroll_y.abs() > 0.0 && in_labels && state.ch_view.q_mode != 0 && myu >= cw_y && myu < cw_y + cw_hh {
            let (n_rows, avail_h, min_row) = if state.ch_view.show_bo_matrix {
                (bo_matrix_rows(&state.ch_view, ch_idx).len(), cw_hh.saturating_sub(12), 13)
            } else {
                (cw_strip_keys(&state.ch_view, ch_idx).0.len(), cw_hh, 14)
            };
            let (shown, _) = strip_geometry(n_rows, avail_h, min_row);
            let max = n_rows.saturating_sub(shown);
            let cur = state.ch_view.cw_scroll.min(max);
            state.ch_view.cw_scroll = if scroll_y > 0.0 { cur.saturating_sub(1) } else { (cur + 1).min(max) };
        } else if scroll_y.abs() > 0.0 && in_labels && state.ch_view.q_mode != 0 && myu >= q_y && myu < q_y + q_hh {
            let rows = qdepth_strip_keys(&state.ch_view, ch_idx);
            let (shown, _) = qstrip_geometry(rows.len(), q_hh);
            let max = rows.len().saturating_sub(shown);
            let cur = state.ch_view.q_scroll.min(max);
            state.ch_view.q_scroll = if scroll_y > 0.0 { cur.saturating_sub(1) } else { (cur + 1).min(max) };
        } else if scroll_y.abs() > 0.0 {
            let factor = if scroll_y > 0.0 { 0.85 } else { 1.18 };
            if mouse_in_ch {
                let n = ((mx as f64) - ch_panel_x as f64) / ch_panel_w as f64;
                let n = n.clamp(0.0, 1.0);
                let anchor = state.ch_view.center_t - state.ch_view.span_t * 0.5
                           + state.ch_view.span_t * n;
                state.ch_view.span_t   *= factor;
                state.ch_view.center_t  = anchor - state.ch_view.span_t * (n - 0.5);
            } else if mouse_in_abr {
                let n = ((mx as f64) - abr_panel_x as f64) / abr_panel_w as f64;
                let n = n.clamp(0.0, 1.0);
                let anchor = state.abr_view.center_t - state.abr_view.span_t * 0.5
                           + state.abr_view.span_t * n;
                state.abr_view.span_t   *= factor;
                state.abr_view.center_t  = anchor - state.abr_view.span_t * (n - 0.5);
            }
        }
    }

    // ── Left-drag pan — routed by focus ──────────────────────────────────────

    if window.get_mouse_down(MouseButton::Left) && !state.zoom_mode {
        match state.focus {
            Focus::Channel => {
                if let Some((mx0, ct0)) = state.ch_view.mouse_drag {
                    let dx = (mx - mx0) as f64;
                    state.ch_view.center_t = ct0 - dx * (state.ch_view.span_t / ch_panel_w as f64);
                } else if mouse_in_ch {
                    state.ch_view.mouse_drag = Some((mx, state.ch_view.center_t));
                }
            }
            Focus::Abr => {
                if let Some((mx0, ct0)) = state.abr_view.mouse_drag {
                    let dx = (mx - mx0) as f64;
                    state.abr_view.center_t = ct0 - dx * (state.abr_view.span_t / abr_panel_w as f64);
                } else if mouse_in_abr {
                    state.abr_view.mouse_drag = Some((mx, state.abr_view.center_t));
                }
            }
        }
    } else {
        state.ch_view.mouse_drag  = None;
        state.abr_view.mouse_drag = None;
    }

    // ── Keyboard — routed to the focused panel ────────────────────────────────

    // Global: Space always toggles playback regardless of focus
    if window.is_key_pressed(Key::Space, minifb::KeyRepeat::No) {
        state.playing    = !state.playing;
        state.last_frame = Instant::now();
    }
    match state.focus {
        Focus::Channel => {
            // Reuse handle_input but pass a view wrapper so drag/scroll don't double-apply.
            // Simpler: just replicate the key checks for the channel view.
            let v = &mut state.ch_view;
            if window.is_key_down(Key::Left)  || window.is_key_down(Key::A) { v.center_t -= v.span_t * 0.02; }
            if window.is_key_down(Key::Right) || window.is_key_down(Key::D) { v.center_t += v.span_t * 0.02; }
            if window.is_key_pressed(Key::Equal,      minifb::KeyRepeat::Yes)
            || window.is_key_pressed(Key::NumPadPlus, minifb::KeyRepeat::Yes) { v.span_t *= 0.8; }
            if window.is_key_pressed(Key::Minus,       minifb::KeyRepeat::Yes)
            || window.is_key_pressed(Key::NumPadMinus, minifb::KeyRepeat::Yes) { v.span_t *= 1.25; }
            if window.is_key_pressed(Key::Home, minifb::KeyRepeat::No) { v.center_t = ch_idx.t_min + v.span_t * 0.5; }
            if window.is_key_pressed(Key::End,  minifb::KeyRepeat::No) { v.center_t = ch_idx.t_max - v.span_t * 0.5; }
            if window.is_key_pressed(Key::Q, minifb::KeyRepeat::No) { v.q_mode = (v.q_mode + 1) % 4; }
            if window.is_key_pressed(Key::W, minifb::KeyRepeat::No) { v.q_shared = !v.q_shared; }
            if window.is_key_pressed(Key::B, minifb::KeyRepeat::No) { v.show_bo_matrix = !v.show_bo_matrix; }
            if window.is_key_pressed(Key::I, minifb::KeyRepeat::No) { v.show_arrivals = !v.show_arrivals; }
            if window.is_key_pressed(Key::Tab,  minifb::KeyRepeat::No) {
                v.selected_link = match v.selected_link {
                    None    => Some(0),
                    Some(l) if l + 1 < num_links => Some(l + 1),
                    _       => None,
                };
            }
            if window.is_key_down(Key::PageDown) { v.row_scroll += 4; }
            if window.is_key_down(Key::PageUp)   { v.row_scroll  = (v.row_scroll - 4).max(0); }
        }
        Focus::Abr => {
            let v = &mut state.abr_view;
            if window.is_key_down(Key::Left)  || window.is_key_down(Key::A) { v.center_t -= v.span_t * 0.05; }
            if window.is_key_down(Key::Right) || window.is_key_down(Key::D) { v.center_t += v.span_t * 0.05; }
            if window.is_key_pressed(Key::Equal,      minifb::KeyRepeat::Yes)
            || window.is_key_pressed(Key::NumPadPlus, minifb::KeyRepeat::Yes) { v.span_t *= 0.8; }
            if window.is_key_pressed(Key::Minus,       minifb::KeyRepeat::Yes)
            || window.is_key_pressed(Key::NumPadMinus, minifb::KeyRepeat::Yes) { v.span_t *= 1.25; }
            if window.is_key_pressed(Key::Home, minifb::KeyRepeat::No) { v.center_t = abr_idx.t_min + v.span_t * 0.5; }
            if window.is_key_pressed(Key::End,  minifb::KeyRepeat::No) { v.center_t = abr_idx.t_max - v.span_t * 0.5; }
        }
    }

    // ── Clamp both views ─────────────────────────────────────────────────────

    clamp_view(&mut state.ch_view, ch_idx);
    clamp_abr_view(&mut state.abr_view, abr_idx);
}

fn clamp_abr_view(v: &mut AbrViewState, idx: &AbrVizIndex) {
    let full = (idx.t_max - idx.t_min).max(1e-6);
    v.span_t = v.span_t.max(1e-3).min(full); // not `clamp`: it panics when the range is under 1 ms
    let lo = idx.t_min + v.span_t * 0.5;
    let hi = idx.t_max - v.span_t * 0.5;
    // Guard: FP rounding can make lo > hi when span_t ≈ full
    v.center_t = if lo <= hi {
        v.center_t.clamp(lo, hi)
    } else {
        (idx.t_min + idx.t_max) * 0.5
    };
}


pub(crate) fn draw_ap_icon(buf: &mut [u32], stride: usize,
                cx: i32, cy: i32, color: u32, scale: f32) {
    // ── Router box ────────────────────────────────────────────────────────
    let bw = (16.0 * scale).round() as i32;
    let bh = ( 6.0 * scale).round() as i32;
    let bx = cx - bw / 2;
    let by = cy - bh / 2;

    abr_fill_rect(buf, stride,
        (bx + 1).max(0) as usize, (by + 1).max(0) as usize,
        (bw - 2).max(1) as usize, (bh - 2).max(1) as usize,
        dim_color(color, 4));
    abr_draw_hline(buf, stride, bx, bx + bw, by,      color);
    abr_draw_hline(buf, stride, bx, bx + bw, by + bh, color);
    abr_draw_vline(buf, stride, bx,      by, by + bh, color);
    abr_draw_vline(buf, stride, bx + bw, by, by + bh, color);

    // Green LED
    let led_off = (3.0 * scale).round() as i32;
    let led_i = (by + bh / 2) as usize * stride
              + (bx + bw - led_off).max(0) as usize;
    if led_i < buf.len() { buf[led_i] = 0x55ff99; }

    // ── Two antennas (lean outward from the box top) ──────────────────────
    let ant_h    = (9.0 * scale).round() as i32;
    let ant_lean = (2.0 * scale).round() as i32;
    let ant_off  = bw / 4;   // base inset from each box edge

    // Left antenna
    let (lbx, ltx, lty) = (bx + ant_off,      bx + ant_off - ant_lean,      by - ant_h);
    abr_draw_line(buf, stride, lbx, by, ltx, lty, color);
    // Right antenna
    let (rbx, rtx, rty) = (bx + bw - ant_off, bx + bw - ant_off + ant_lean, by - ant_h);
    abr_draw_line(buf, stride, rbx, by, rtx, rty, color);

    // Small ball at each tip
    for &(tx, ty) in &[(ltx, lty), (rtx, rty)] {
        for dy in -1i32..=1 { for dx in -1i32..=1 {
            let fx = tx + dx; let fy = ty + dy;
            if fx >= 0 && fy >= 0 {
                let i = fy as usize * stride + fx as usize;
                if i < buf.len() { buf[i] = color; }
            }
        }}
    }

    // ── Wi-Fi dot (between antenna bases) ────────────────────────────────
    let dot_y  = by - (2.0 * scale).round() as i32;
    let dot_hw = (scale.round() as i32).max(1);
    let dot_h  = ((scale * 2.0).round() as i32).max(1);
    for dy in 0..dot_h { for dx in -dot_hw..=dot_hw {
        if (dot_y + dy) >= 0 && (cx + dx) >= 0 {
            let i = (dot_y + dy) as usize * stride + (cx + dx) as usize;
            if i < buf.len() { buf[i] = color; }
        }
    }}

    // ── Partial arcs: left half on left antenna side, right on right ──────
    // Each arc is a quarter-fan centred on its antenna tip, directed outward.
    // The inner arc (r=4) sits close to the box, the outer (r=9) reaches higher.
    let fan = 0.75_f32;   // angular limit: |dx/r| ≤ 0.75 ≈ ±49°

    for &r_f in &[3.5_f32 * scale, 6.0 * scale, 9.0 * scale] {
        let r_i = r_f.round() as i32;

        // Left antenna arcs: centre = (ltx, lty), dx ≤ 0 (fan opens left+up)
        for dx in -r_i..=0 {
            if (dx as f32).abs() > r_f * fan { continue; }
            let rr = r_f * r_f - (dx * dx) as f32;
            if rr < 0.0 { continue; }
            let dy = -(rr.sqrt() as i32);
            if dy >= 0 { continue; }
            let px = ltx + dx; let py = lty + dy;
            if px >= 0 && py >= 0 {
                let i = py as usize * stride + px as usize;
                if i < buf.len() { buf[i] = color; }
            }
        }

        // Right antenna arcs: centre = (rtx, rty), dx ≥ 0 (fan opens right+up)
        for dx in 0..=r_i {
            if (dx as f32).abs() > r_f * fan { continue; }
            let rr = r_f * r_f - (dx * dx) as f32;
            if rr < 0.0 { continue; }
            let dy = -(rr.sqrt() as i32);
            if dy >= 0 { continue; }
            let px = rtx + dx; let py = rty + dy;
            if px >= 0 && py >= 0 {
                let i = py as usize * stride + px as usize;
                if i < buf.len() { buf[i] = color; }
            }
        }
    }
}
fn draw_vr_hmd_final(buf: &mut [u32], stride: usize,
                     cx: i32, cy: i32, color: u32, scale: f32) {
    // ── Proportions ───────────────────────────────────────────────────────
    let v_w = (24.0 * scale).round() as i32; 
    let v_h = (16.0 * scale).round() as i32; // Taller visor as requested
    let v_x = cx - v_w / 2;
    let v_y = cy - v_h / 2;
    
    let notch_w = (6.0 * scale).round() as i32;
    let notch_h = (4.0 * scale).round() as i32;
    
    // Camera "sensor" offsets (Quest/PSVR2 style corners)
    let cam_off = (2.0 * scale).round() as i32;
    let cam_sz  = (scale.round() as i32).max(1);

    // ── 1. The Halo Strap (Background) ────────────────────────────────────
    let s_w = (18.0 * scale).round() as i32;
    let s_h = (20.0 * scale).round() as i32; // Taller strap to match visor
    let s_x = cx - s_w / 2;
    let s_y = cy - s_h / 2;
    let strap_col = dim_color(color, 6);

    for y in (cy + 2)..=(s_y + s_h) {
        for x in s_x..=(s_x + s_w) {
            let dx = (x - cx).abs() as f32;
            let inner = s_w as f32 * 0.35;
            let outer = s_w as f32 * 0.50;
            if dx < outer && dx > inner {
                if x >= 0 && y >= 0 {
                    let i = y as usize * stride + x as usize;
                    if i < buf.len() { buf[i] = strap_col; }
                }
            }
        }
    }

    // ── 2. The Main Visor Body (Foreground) ───────────────────────────────
    for dy in 0..v_h {
        for dx in 0..v_w {
            let px = v_x + dx;
            let py = v_y + dy;

            // --- SUBTRACTIVE FEATURES (LACK OF COLOR) ---
            
            // A. The Nose Notch (Bottom center)
            if dx >= (v_w - notch_w) / 2 && dx <= (v_w + notch_w) / 2 && dy >= v_h - notch_h {
                continue; 
            }

            // B. Side Cameras (4 corner sensors)
            let is_cam_x = dx == cam_off || dx == (v_w - cam_off - cam_sz);
            let is_cam_y = dy == cam_off || dy == (v_h - cam_off - cam_sz);
            if is_cam_x && is_cam_y {
                continue; // "Dark dots" where the background shows through
            }

            // C. Chamfered Corners (Top only for a "brow" look)
            if dy < 2 && (dx < 2 || dx >= v_w - 2) {
                continue;
            }

            if px >= 0 && py >= 0 {
                let i = py as usize * stride + px as usize;
                if i < buf.len() {
                    // Use a slightly brighter horizontal "visor glass" line
                    if dy >= 3 && dy <= 5 {
                        buf[i] = brighten_color(color);
                    } else {
                        buf[i] = color;
                    }
                }
            }
        }
    }
}

fn draw_stylized_vr_hmd(buf: &mut [u32], stride: usize,
                         cx: i32, cy: i32, color: u32, scale: f32) {
    let sc = |n: f32| -> i32 { (n * scale).round() as i32 };

    let bw = sc(26.0); let bh = sc(14.0);
    let bx = cx - bw / 2;  let by = cy - bh / 2;
    let cr = sc(3.0); // corner radius

    let notch_base_half = 3.5 * scale;
    let notch_depth     = 4.5 * scale;

    // ── Side Straps ──────────────────────────────────────────────────────
    let st_h = sc(4.0); let st_w = sc(7.0);
    let st_y = cy - st_h / 2;
    let strap_col = dim_color(color, 1);

    // Left strap
    for y in st_y..st_y + st_h {
        for x in (bx - st_w)..bx {
            if x >= 0 && y >= 0 {
                let i = y as usize * stride + x as usize;
                if i < buf.len() { buf[i] = strap_col; }
            }
        }
    }
    // Right strap
    for y in st_y..st_y + st_h {
        for x in (bx + bw)..(bx + bw + st_w) {
            if x >= 0 && y >= 0 {
                let i = y as usize * stride + x as usize;
                if i < buf.len() { buf[i] = strap_col; }
            }
        }
    }

    // ── Visor Body (rounded corners + nose notch) ────────────────────────
    for y in 0..bh {
        for x in 0..bw {
            let px = bx + x; let py = by + y;

            // Rounded corners via distance from each corner center
            let in_corner = [
                (cr,      cr     ),
                (bw-1-cr, cr     ),
                (cr,      bh-1-cr),
                (bw-1-cr, bh-1-cr),
            ].iter().any(|&(ccx, ccy)| {
                let dx = x - ccx; let dy = y - ccy;
                dx.abs() < cr && dy.abs() < cr && dx*dx + dy*dy > cr*cr
            });
            if in_corner { continue; }

            // Nose notch: convex arch at bottom-center
            let dy_up = (bh - 1 - y) as f32;
            if dy_up < notch_depth {
                let t = dy_up / notch_depth;
                // exponent 0.5 = parabolic arch sides (Quest 3-style)
                let half_w = notch_base_half * (1.0 - t.powf(0.5));
                if ((x - bw / 2) as f32).abs() < half_w { continue; }
            }

            if px >= 0 && py >= 0 {
                let i = py as usize * stride + px as usize;
                if i < buf.len() { buf[i] = color; }
            }
        }
    }

    // ── Lens Ellipses ────────────────────────────────────────────────────
    let lw = sc(8.0); let lh = sc(7.0);
    let lgap = sc(2.0);
    let glass_col = brighten_color(color);

    for &side in &[-1i32, 1i32] {
        let lcx = if side < 0 { cx - lgap / 2 - lw / 2 }
                  else        { cx + lgap / 2 + lw / 2 };
        let lcy = cy - sc(1.0); // nudge up slightly

        let rx = lw / 2; let ry = lh / 2;
        for dy in -ry..=ry {
            for dx in -rx..=rx {
                // Ellipse test (slightly inset so it sits inside the body)
                let ex = dx as f32 / rx as f32;
                let ey = dy as f32 / ry as f32;
                if ex*ex + ey*ey <= 0.92 {
                    let px = (lcx + dx).max(0) as usize;
                    let py = (lcy + dy).max(0) as usize;
                    let i = py * stride + px;
                    if i < buf.len() { buf[i] = glass_col; }
                }
            }
        }
    }
}

fn draw_bg_device_icon(buf: &mut [u32], stride: usize,
                       cx: i32, cy: i32, color: u32,
                       scale: f32) {
    let sw = (10.0 * scale).round() as i32;
    let sh = ( 7.0 * scale).round() as i32;
    let sx = cx - sw / 2;
    let sy = cy - sh - (2.0 * scale).round() as i32;

    abr_fill_rect(buf, stride,
        (sx + 1) as usize, (sy + 1) as usize,
        (sw - 2).max(1) as usize, (sh - 2).max(1) as usize,
        dim_color(color, 5));
    abr_draw_hline(buf, stride, sx, sx + sw, sy,      color);
    abr_draw_hline(buf, stride, sx, sx + sw, sy + sh, color);
    abr_draw_vline(buf, stride, sx,      sy, sy + sh, color);
    abr_draw_vline(buf, stride, sx + sw, sy, sy + sh, color);

    let bw = (14.0 * scale).round() as i32;
    let bh = ( 3.0 * scale).round() as i32;
    let bx = cx - bw / 2;
    let by = sy + sh + (scale.round() as i32).max(1);

    abr_draw_hline(buf, stride, bx, bx + bw, by,      color);
    abr_draw_hline(buf, stride, bx, bx + bw, by + bh, color);
    abr_draw_vline(buf, stride, bx,      by, by + bh, color);
    abr_draw_vline(buf, stride, bx + bw, by, by + bh, color);
}
// ─────────────────────────────────────────────────────────────────────────────
// STA mini-map — rendered inside the ABR sidebar's lower section.
// `x_origin` is the left edge of the full ABR right-panel (= abr_left).
// `y_top` / `height` define the reserved rectangle inside the sidebar.
// ─────────────────────────────────────────────────────────────────────────────

const STA_MINIMAP_H:    usize = 210;
const STA_MINIMAP_COLS: usize = 24;
const STA_MINIMAP_ROWS: usize = 12;

fn render_sta_sidebar_minimap(
    buf:       &mut [u32],
    stride:    usize,
    x_origin:  usize,
    sidebar_w: usize,
    y_top:     usize,
    height:    usize,
    idx:       &AbrVizIndex,
    cursor_t:  f64,
    ap_x:      f32,
    ap_y:      f32,
    highlight_ip: Option<usize>,
) {
    const PAD: usize = 6;
    let map_x = x_origin + PAD;
    let map_w = sidebar_w.saturating_sub(PAD * 2);
    let map_y = y_top + 18;
    let map_h = height.saturating_sub(22 + PAD);

    // Prevent division by zero if the sidebar is squished too small
    if map_w == 0 || map_h == 0 { return; }

    abr_fill_rect(buf, stride, x_origin, y_top, sidebar_w, height, 0x10101e);
    render_text(buf, "STA POSITIONS", x_origin + PAD, y_top + 3,
                stride, 0x888899, TEXT_SIZE_ABR);

    let cell_w = map_w as f32 / STA_MINIMAP_COLS as f32;
    let cell_h = map_h as f32 / STA_MINIMAP_ROWS as f32;

    for col in 0..=STA_MINIMAP_COLS {
        let gx = map_x as i32 + (col as f32 * cell_w) as i32;
        abr_draw_vline(buf, stride, gx, map_y as i32, (map_y + map_h) as i32, 0x1c1c2c);
    }
    for row in 0..=STA_MINIMAP_ROWS {
        let gy = map_y as i32 + (row as f32 * cell_h) as i32;
        abr_draw_hline(buf, stride, map_x as i32, (map_x + map_w) as i32, gy, 0x1c1c2c);
    }

    // ── Coordinate helpers ───────────────────────────────────────────────────
    let (raw_x_min, raw_x_max) = idx.sta_x_range;
    let (raw_y_min, raw_y_max) = idx.sta_y_range;

    // 1. Calculate the maximum distance any STA moves away from the AP
    let max_dist_x = (raw_x_max - ap_x).abs().max((ap_x - raw_x_min).abs()).max(1.0);
    let max_dist_y = (raw_y_max - ap_y).abs().max((ap_y - raw_y_min).abs()).max(1.0);

    // 2. Lock aspect ratio so spatial distances aren't squished/stretched
    let aspect = map_w as f32 / map_h as f32;
    let (mut half_span_x, mut half_span_y) = (max_dist_x, max_dist_y);

    if half_span_x / half_span_y > aspect {
        half_span_y = half_span_x / aspect; // Expand Y to fit
    } else {
        half_span_x = half_span_y * aspect; // Expand X to fit
    }

    // 3. Define the bounding box symmetrically centered right on the AP
    let x_min = ap_x - half_span_x;
    let x_max = ap_x + half_span_x;
    let y_min = ap_y - half_span_y;
    let y_max = ap_y + half_span_y;

    let x_span = half_span_x * 2.0;
    let y_span = half_span_y * 2.0;

    let to_px = |x: f32, y: f32| -> (i32, i32) {
        let nx =  ((x - x_min) / x_span) as f64;
        let ny = 1.0 - ((y - y_min) / y_span) as f64;  // +y = up
        (
            map_x as i32 + (nx * map_w as f64) as i32,
            map_y as i32 + (ny * map_h as f64) as i32,
        )
    };

    // ── AP marker ────────────────────────────────────────────────────────────
    {
        let (ax, ay) = {
            let (px, py) = to_px(ap_x, ap_y);
            (
                px.clamp(map_x as i32, (map_x + map_w - 1) as i32),
                py.clamp(map_y as i32, (map_y + map_h - 1) as i32),
            )
        };

        // Router + Wi-Fi arcs replace the old crosshair
        draw_ap_icon(buf, stride, ax, ay, 0xffffff, 1.5);

        // Label sits to the right of the arcs
        render_text(buf,
            &format!("AP ({:.1},{:.1})", ap_x, ap_y),
            (ax + 14).max(map_x as i32) as usize,
            (ay - 14).max(map_y as i32) as usize,
            stride, 0xccccaa, 1.3);
    }

    // ── Per-STA: trail + dot + live coordinate label ─────────────────────────
    const HMD_SCALE: f32 = 1.0;   // tweak freely

    for (si, ip) in idx.ip_sta_order.iter().enumerate() {
        let base  = user_color(si);
        let is_lit = highlight_ip.map_or(true, |h| h == si);
        let icon_color = if is_lit { base } else { dim_color(base, 5) };
        let trail_color = if is_lit { dim_color(base, 2) } else { dim_color(base, 8) };

        let Some(indices) = idx.by_ip_sta.get(ip) else { continue };
        let end = indices.partition_point(|&i| abr_event_t(&idx.events[i]) <= cursor_t);
        if end == 0 { continue; }

        // ── Trail ─────────────────────────────────────────────────────────────
        let mut prev:       Option<(i32, i32)> = None;
        let mut last_world: Option<(f32, f32)> = None;
        let mut is_bgg = false;

        for &ei in &indices[..end] {
            if let AbrEvent::StaLocation { x, y, is_bg, .. } = &idx.events[ei] {
                let (px, py) = to_px(*x, *y);
                if let Some((ppx, ppy)) = prev {
                    abr_draw_line(buf, stride, ppx, ppy, px, py, trail_color);
                }
                prev       = Some((px, py));
                last_world = Some((*x, *y));
                is_bgg     = *is_bg;
            }
        }

        let Some((px, py)) = prev else { continue };

        // ── Fetch metrics for this STA (matched by palette index) ─────────────
        let (bitrate_mbps, rtt_ms, flr) = if si < idx.ip_order.len() {
            let srv = &idx.ip_order[si];

            let br = idx.by_ip_bitrate.get(srv)
                .and_then(|idxs| {
                    let pos = idxs.partition_point(|&i| abr_event_t(&idx.events[i]) <= cursor_t);
                    if pos == 0 { return None; }
                    match &idx.events[idxs[pos - 1]] {
                        AbrEvent::BitrateUpdate { new_bitrate_mbps, .. } => Some(*new_bitrate_mbps),
                        _ => None,
                    }
                }).unwrap_or(0.0);

            let (rtt, flr) = idx.by_ip_frame.get(srv)
                .and_then(|idxs| {
                    let pos = idxs.partition_point(|&i| abr_event_t(&idx.events[i]) <= cursor_t);
                    if pos == 0 { return None; }
                    match &idx.events[idxs[pos - 1]] {
                        AbrEvent::FrameMetrics { rtt_ms, flr, .. } => Some((*rtt_ms, *flr)),
                        _ => None,
                    }
                }).unwrap_or((0.0, 0.0));

            (br, rtt, flr)
        } else {
            (0.0, 0.0, 0.0)
        };

        // ── Icon: HMD for XR user, laptop for background device ───────────────
        if is_bgg {
            draw_bg_device_icon(buf, stride, px, py, icon_color, HMD_SCALE);
        } else {
            draw_stylized_vr_hmd(buf, stride, px, py, icon_color, HMD_SCALE);
        }

        // ── Metrics badge above the icon ──────────────────────────────────────
        let badge_h  = (10.0 * HMD_SCALE).round() as i32 / 2 + 2;
        let badge_y  = py - badge_h;
        let text_col = if is_lit { icon_color } else { dim_color(icon_color, 2) };
        let dim_col  = dim_color(text_col, 2);

        // if bitrate_mbps > 0.0 || rtt_ms > 0.0 {
        //     let mx = (px - 14).max(map_x as i32);
        //     render_text(buf, &format!("{:.1}M", bitrate_mbps),
        //                 mx as usize, (badge_y - 22).max(map_y as i32) as usize,
        //                 stride, text_col, 1.1);
        //     // render_text(buf, &format!("R{:.0} F{:.2}", rtt_ms, flr),
        //     //             mx as usize, (badge_y - 11).max(map_y as i32) as usize,
        //     //             stride, dim_col, 1.1);
        // }

        // ── World coordinate label (highlighted STA only) ─────────────────────
        if is_lit {
            if let Some((wx, wy)) = last_world {
                let label   = format!("X:{:.1} Y:{:.1}", wx, wy);
                let label_x = if px + 60 < (map_x + map_w) as i32 { px + 6 } else { px - 52 };
                let label_y = (py + badge_h + 2).max(map_y as i32);
                render_text(buf, &label,
                            label_x.max(map_x as i32) as usize,
                            label_y as usize,
                            stride, brighten_color(base), 1.2);
            }
        }
    }
    // ── Grid Scale Indicator ──────────────────────────────────────────────────
    {
        let scale_px_w = 40i32; // Fixed pixel width for the scale bar
        let units_per_px = x_span / map_w as f32;
        let scale_units = units_per_px * (scale_px_w as f32);

        let scale_label = format!("{:.1}m", scale_units);
        let bar_x = (map_x + map_w) as i32 - scale_px_w - 8;
        let bar_y = (map_y + map_h) as i32 - 8;

        // Draw a neat `|----|` scale bracket at the bottom right
        abr_draw_hline(buf, stride, bar_x, bar_x + scale_px_w, bar_y, 0x888899);
        abr_draw_vline(buf, stride, bar_x, bar_y - 3, bar_y, 0x888899);
        abr_draw_vline(buf, stride, bar_x + scale_px_w, bar_y - 3, bar_y, 0x888899);

        render_text(buf, &scale_label,
                    (bar_x + 2).max(0) as usize, (bar_y - 14).max(0) as usize,
                    stride, 0x888899, 1.0);
    }

    // ── Borders ───────────────────────────────────────────────────────────────
    abr_draw_hline(buf, stride, map_x as i32, (map_x + map_w) as i32,  map_y as i32,           0x2a2a3e);
    abr_draw_hline(buf, stride, map_x as i32, (map_x + map_w) as i32, (map_y + map_h) as i32,  0x2a2a3e);
    abr_draw_vline(buf, stride, map_x as i32,           map_y as i32,  (map_y + map_h) as i32, 0x2a2a3e);
    abr_draw_vline(buf, stride, (map_x + map_w) as i32, map_y as i32,  (map_y + map_h) as i32, 0x2a2a3e);
}
// ── Entry point ───────────────────────────────────────────────────────────────

pub fn run_unified_viewer(
    viz_idx:      VizIndex,
    abr_idx:      AbrVizIndex,
    link_configs: Vec<LinkConfig>,     // ← now owned, move into thread freely
    sim_tag:      String,
    )
 {
    run_unified_viewer_multi(obss::MultiBss::single(viz_idx, abr_idx, link_configs), sim_tag);
}

/// The unified viewer: the channel side (link lanes, back-off and queue panels) and the ABR side (plots, legend, station
/// map) of the run in `mb`.
/// `XRSIM_VIZ_MAP_PNG=<dir>`: instead of opening the viewer, the STA map at the middle of 8 TXOPs spread over the run
/// (where its arrows show), as `<dir>/map_tab0_<n>.png` (a check of the map without a window).
fn save_map_pngs(mb: &obss::MultiBss, dir: &str) {
    let (w, h) = (420usize, 480usize);
    let _ = std::fs::create_dir_all(dir);
    for active in 0..mb.views.len() {
        let mut groups: Vec<&TxopGroup> = mb.views[active].idx.txop_groups_by_link.values().flatten().collect();
        groups.sort_by(|a, b| a.t.total_cmp(&b.t));
        let txops = (0..8).filter_map(|n| groups.get(groups.len() * (2 * n + 1) / 16).map(|g| (n.to_string(), 0.5 * (g.t + g.end))));
        for (n, t) in txops.collect::<Vec<_>>() {
            let mut buf = vec![0u32; w * h];
            let mut mv = obss::MapView::default();
            obss::render_minimap(&mut buf, w, 0, w, 0, h, mb, active, t, t, None, obss::MapHover::default(), &mut mv);
            let mut png = image::RgbImage::new(w as u32, h as u32);
            for (i, px) in buf.iter().enumerate() {
                png.put_pixel((i % w) as u32, (i / w) as u32, image::Rgb([(px >> 16) as u8, (px >> 8) as u8, *px as u8]));
            }
            let path = format!("{dir}/map_tab{active}_{n}.png");
            match png.save(&path) {
                Ok(()) => println!("[viz] {path}: t = {t:.6} s"),
                Err(e) => eprintln!("[viz] {path}: {e}"),
            }
        }
    }
}

pub fn run_unified_viewer_multi(mb: obss::MultiBss, sim_tag: String) {
    if let Ok(dir) = std::env::var("XRSIM_VIZ_MAP_PNG") {
        return save_map_pngs(&mb, &dir);
    }
    let active = 0;
    let mut map_view = obss::MapView::default();
    let mut sidebar = sidebar_resize::SidebarResize::default();
    let mut bottom_panels = panel_resize::PanelResize::default();
    let mut window = Window::new(
        "WLAN Sim + ABR Viewer",
        WIN_W, WIN_H,
        WindowOptions {
            resize:     true,
            scale_mode: minifb::ScaleMode::Stretch,
            ..WindowOptions::default()
        },
    ).expect("Failed to open unified viewer");
    let mut ui = ui_scale::UiScale::new();

    let mut buf   = vec![0u32; WIN_W * WIN_H];
    let mut state = UnifiedState::new(&mb.views[active].idx, &mb.views[active].abr);
    obss::on_switch(&mut state.ch_view, &mb, active);

    // Pre-build ABR strip descriptors
    let abr_strips  = make_strips(&mb.views[active].abr);

    // The menu bar: which panels are shown and where, the ABR plots' order, the help overlays (and the parts of the
    // window they point at, as drawn last frame)
    let mut prefs = menus::Prefs::default();
    let mut strips = strip_order::StripOrder::new(abr_strips.len());
    let strip_names: Vec<&'static str> = abr_strips.iter().map(|s| s.label).collect();
    let mut menu = menu_bar::MenuBar::default();
    let mut help = tour::Help::default();
    let mut regions: Option<tour::Regions> = None;
    let mut want_screenshot = false;
    // transmissions of the run for the tour to zoom into, and the view to put back after it (channel centre, span
    // and cursor, ABR cursor, overhead shown, playing)
    let examples = tour_examples::find(&mb.views[active].idx);
    #[derive(Clone, Copy)]
    struct TourSaved { center: f64, span: f64, cursor: f64, abr_cursor: f64, overhead: bool, playing: bool, bo_grid: bool, q_mode: u8, show_cw: bool }
    let mut tour_saved: Option<TourSaved> = None;
    // the example the tour is animating (the backoff countdown) and since when
    let mut tour_anim: Option<(usize, Instant)> = None;
    // the channel view the wheel and drags made of a tour step's example (step, centre, span), until the step changes,
    // and a drag's start (mouse x, centre)
    let mut tour_zoom: Option<(usize, f64, f64)> = None;
    let mut tour_drag: Option<(f32, f64)> = None;
    // `XRSIM_VIZ_TOUR=all|<step>`: as the viewer opens, a screenshot of every quick-tour step (or of one), then the viewer
    // carries on as usual; (step, frames drawn at it, all). With `all`, the shots go on past the tour: each menu open, the
    // keyboard sheet and the About box.
    let mut tour_shots: Option<(usize, usize, bool)> = std::env::var("XRSIM_VIZ_TOUR").ok().map(|v| match v.trim() {
        "all" => (0, 0, true),
        s => (s.parse().unwrap_or(0), 0, false),
    });
    // `XRSIM_VIZ_EXIT=1`: close the viewer once its screenshots are saved (the first frame's, and the tour's with
    // `XRSIM_VIZ_TOUR`), for checks that must not leave a window open
    let exit_after_shots = std::env::var("XRSIM_VIZ_EXIT").is_ok_and(|v| !v.is_empty() && v != "0");

    let mut screenshot_taken = false;

    while window.is_open() {
        // ── Live dimensions ───────────────────────────────────────────────────
        let (w, h) = ui.frame(&window); // logical size: the window over the UI scale
        if buf.len() != w * h { buf.resize(w * h, 0); }
        // a window dragged too small to hold the panels shows a blank frame until it is enlarged again
        if window_too_small(w, h) {
            buf.fill(0x14141c);
            state.prev_left_down = window.get_mouse_down(MouseButton::Left);
            state.splitter_drag = None;
            window.update_with_buffer(&buf, w.max(1), h.max(1)).unwrap();
            if window.is_key_down(Key::Escape) { break; }
            continue;
        }

        // ── Help overlays and menus: they take the mouse and keyboard before the panels ──
        let facts_view = &mb.views[active];
        let facts = menus::Facts {
            num_links: facts_view.links.len() as u8, has_map: !facts_view.abr.by_ip_sta.is_empty(),
            strip_names: &strip_names,
        };
        let esc = window.is_key_pressed(Key::Escape, minifb::KeyRepeat::No);
        let mut esc_used = false;
        let n_menus = menus::build(&state, &prefs, &strips, &facts).len();
        if let Some((step, _, _)) = tour_shots {
            let n_tour = regions.as_ref().map_or(usize::MAX, |r| tour::Help::tour_len(r, &examples));
            let extra = step.checked_sub(n_tour);
            help.overlay = match extra {
                None => Some(tour::Overlay::Tour(step)),
                Some(k) if k < n_menus => None,
                Some(k) if k == n_menus => Some(tour::Overlay::Keys),
                Some(_) => Some(tour::Overlay::About),
            };
            menu.open_at(extra.filter(|&k| k < n_menus));
        }
        let help_was_open = help.is_open();
        if let Some(r) = &regions {
            esc_used |= help.input(&window, r, &examples);
        }
        let mut captured = help_was_open || help.captures();
        let mut after = menus::After::Nothing;
        if !captured {
            let menu_list = menus::build(&state, &prefs, &strips, &facts);
            let out = menu.input(&window, &menu_list, w);
            esc_used |= out.esc_used;
            captured = out.captured;
            if let Some(cmd) = out.cmd {
                let (ch_idx, abr_idx) = (&facts_view.idx, &facts_view.abr);
                after = menus::apply(cmd, menus::Targets {
                    state: &mut state, prefs: &mut prefs, strips: &mut strips,
                    ui: &mut ui, help: &mut help, sidebar: &mut sidebar, bottom: &mut bottom_panels,
                    ch_range: (ch_idx.t_min, ch_idx.t_max), abr_range: (abr_idx.t_min, abr_idx.t_max), w,
                });
            }
        }
        // F1 and H open the help (and close it from inside)
        if !captured && !help_was_open {
            if window.is_key_pressed(Key::F1, minifb::KeyRepeat::No) {
                help.toggle(tour::Overlay::Tour(0));
                captured = true;
            } else if window.is_key_pressed(Key::H, minifb::KeyRepeat::No) {
                help.toggle(tour::Overlay::Keys);
                captured = true;
            }
        }
        if (esc && !esc_used) || after == menus::After::Quit {
            break;
        }
        want_screenshot |= after == menus::After::Screenshot;
        // the mouse as the panels see it: nowhere while a menu or the help has it, except that during the tour it still
        // points (tooltips, highlights, the map's arrows) where it is not over the tour's box
        const NO_MOUSE: (f32, f32) = (-10_000.0, -10_000.0);
        let points = !captured || (help.touring() && !menu.is_open()
            && ui_scale::mouse_pos(&window).is_some_and(|(x, y)| !help.over_box(x, y)));
        let mouse = |window: &Window| if points { ui_scale::mouse_pos(window).unwrap_or((0.0, 0.0)) } else { NO_MOUSE };

        // ── The station map's zoom and pan ────────────────────────────────────
        if !captured {
            obss::map_input(&window, &mut map_view);
        }
        let view = &mb.views[active];
        let (viz_idx, abr_idx, link_configs) = (&view.idx, &view.abr, &view.links);
        let num_links = link_configs.len() as u8;

        // ── The tour's example: the channel side shows it, zoomed in with the cursor on it; the view is put back when the
        // tour moves on ──
        let tour_ex = regions.as_ref().and_then(|r| help.example(r, &examples)).filter(|&i| i < examples.len());
        if let Some(i) = tour_ex {
            let v = &mut state.ch_view;
            let saved = *tour_saved.get_or_insert(TourSaved {
                center: v.center_t, span: v.span_t, cursor: v.cursor_t, abr_cursor: state.abr_view.cursor_t, overhead: v.show_overhead,
                playing: state.playing, bo_grid: v.show_bo_matrix, q_mode: v.q_mode, show_cw: prefs.show_cw,
            });
            let ex = &examples[i];
            let (center, span, mid) = ex.view(viz_idx.t_min, viz_idx.t_max);
            // what the wheel and drags made of it at this step; a new step starts from the example's own view
            let step = help.tour_step().unwrap_or(usize::MAX);
            let (center, span) = match tour_zoom {
                Some((st, c, s)) if st == step => (c, s),
                _ => {
                    tour_zoom = Some((step, center, span));
                    tour_drag = None;
                    (center, span)
                }
            };
            // an animated step (the countdown) runs from when it was reached
            let started = match tour_anim {
                Some((j, t0)) if j == i => t0,
                _ => tour_anim.insert((i, Instant::now())).1,
            };
            (v.center_t, v.span_t) = (center, span);
            v.cursor_t = ex.cursor_at(started.elapsed().as_secs_f64()).unwrap_or(mid);
            v.show_overhead = true;
            // the panels the viewer had before the tour
            (v.show_bo_matrix, v.q_mode, prefs.show_cw) = (saved.bo_grid, saved.q_mode, saved.show_cw);
            state.playing = false;
        } else if let Some(sv) = tour_saved.take() {
            let v = &mut state.ch_view;
            (v.center_t, v.span_t, v.cursor_t, v.show_overhead) = (sv.center, sv.span, sv.cursor, sv.overhead);
            (v.show_bo_matrix, v.q_mode, prefs.show_cw) = (sv.bo_grid, sv.q_mode, sv.show_cw);
            state.abr_view.cursor_t = sv.abr_cursor;
            state.playing = sv.playing;
            state.last_frame = Instant::now();
            tour_anim = None;
            tour_zoom = None;
            tour_drag = None;
        }

        // ── The two sides (View and Layout menus) ─────────────────────────────
        //
        // The channel side keeps its label column at its left edge and is drawn at x = 0 (its labels sit at fixed x),
        // then shifted to `ch_x0` when it is on the right: its own geometry below is local (`ch_panel_lx`), the input
        // routing uses the window's (`ch_panel_x`). The ABR side draws wherever it is told.
        let sides = layout::sides(w, state.split_frac, prefs.show_ch, prefs.show_abr, prefs.swap_sides);
        if sides.ch_w == 0 { state.focus = Focus::Abr; } else if sides.abr_w == 0 { state.focus = Focus::Channel; }
        const CH_SIDEBAR_W: usize = 240;
        let ch_x0 = sides.ch_x0;
        let ch_panel_lx = CH_SIDEBAR_W;
        let ch_panel_w = if sides.ch_w == 0 { 0 } else { sides.ch_w.saturating_sub(CH_SIDEBAR_W + 4) };
        let ch_panel_x = ch_x0 + ch_panel_lx;

        // ABR side: the sidebar (legend and table above the station map) left or right of the plots; its outer edge and
        // the map's top edge drag, the defaults until they do
        let abr_left = sides.abr_x0;
        let has_map = prefs.show_map && !abr_idx.by_ip_sta.is_empty();
        let has_sidebar = sides.abr_w > 0 && (prefs.show_legend || has_map);
        let sb_geom = sidebar_resize::Geometry {
            left: abr_left, room_w: sides.abr_w, top: HUD_H + CONTROLS_H, bottom: h.saturating_sub(TIME_AXIS),
            has_map,
            legend_h: if prefs.show_legend { abr_sidebar_content_h(abr_idx.ip_order.len()) + 4 } else { 0 },
            on_right: prefs.sidebar_right,
        };
        // the map fills the sidebar below the legend and the table, and is never smaller than it used to be
        let map_fill_h = (h.saturating_sub(TIME_AXIS)).saturating_sub(HUD_H + CONTROLS_H + sb_geom.legend_h);
        let sb_default = (210, STA_MINIMAP_H.max(map_fill_h));
        let (sw, mh) = sidebar.sizes(sb_default.0, sb_default.1, &sb_geom);
        let sidebar_drag = has_sidebar && !captured && sidebar.input(&window, &sb_geom, sw, mh);
        let (sw, minimap_h) = sidebar.sizes(sb_default.0, sb_default.1, &sb_geom);
        let inner = layout::abr_inner(abr_left, sides.abr_w, if has_sidebar { sw } else { 0 }, prefs.sidebar_right);
        let (sidebar_x, abr_sidebar_w) = (inner.sidebar_x, inner.sidebar_w);
        let abr_panel_x = inner.panel_x;
        let abr_panel_w = if sides.abr_w == 0 { 0 } else { inner.panel_w };

        // let content_top    = HUD_H;
        let content_top = HUD_H + CONTROLS_H;
        let content_bottom = h.saturating_sub(TIME_AXIS);
        let content_h      = content_bottom - content_top;

        // ABR strips vertical layout: the shown plots, in the order they were dragged to
        const PANEL_PAD: usize = 10;
        let shown_strips = strips.visible();
        let n_abr = shown_strips.len();
        let abr_usable_h = content_h.saturating_sub(PANEL_PAD);
        let abr_strip_h  = (abr_usable_h / n_abr.max(1)).max(1);
        let strip_geom = strip_order::Geometry {
            x: abr_panel_x, w: abr_panel_w, top: content_top + PANEL_PAD, strip_h: abr_strip_h,
            names: shown_strips.iter().map(|&k| format!("{} [{}]", abr_strips[k].label, abr_strips[k].unit)).collect(),
        };
        let strip_drag = sides.abr_w > 0 && !captured && strips.input(&window, &strip_geom);

        // the lanes' scale and the backoff (CW) and queue panels' heights: their defaults until an edge is dragged
        let lane_bodies: Vec<usize> = link_configs.iter().map(|lc| lane_height(&viz_idx, lc.bandwidth_mhz)).collect();
        let rows_top_def = content_top + 10 + link_configs.iter().map(|lc| lane_height(&viz_idx, lc.bandwidth_mhz) + 5).sum::<usize>() + 10;
        let cw_default = if state.ch_view.q_mode != 0 {
            // the CW panel grows with its number of queues (14 px per strip), up to a cap; the CW strips carry the backoff
            // state themselves, so the backoff table is not drawn and they get its room
            let cap = content_bottom.saturating_sub(200 + 12 + rows_top_def + 4).clamp(48, 330);
            // sized by what it draws: the backoff grid (`B`) has a row per station and link, more than CW series
            let rows = if state.ch_view.show_bo_matrix { bo_matrix_rows(&state.ch_view, &viz_idx).len() } else { viz_idx.cw_series.len() };
            (rows * 14 + 18).clamp(48, cap)
        } else { CW_H };
        let bp_geom = panel_resize::Geometry {
            right: ch_panel_lx + ch_panel_w, bottom: content_bottom,
            base_const: rows_top_def - lane_bodies.iter().sum::<usize>(), bodies: lane_bodies,
            below_lanes: 15,
        };
        // a hidden panel (Channel menu) gives its room to the other; with both hidden the lanes fill the side, unless
        // the station rows of the lines style (`Q` 0) take it; the edges of what is hidden neither draw nor drag
        let (show_cw, show_q) = (prefs.show_cw, prefs.show_q);
        let lanes_fill = !show_cw && !show_q && state.ch_view.q_mode != 0;
        bottom_panels.set_hidden([lanes_fill, !show_cw && !show_q, !(show_cw && show_q)]);
        ui_scale::set_origin_x(ch_x0 as f32);
        let bp0 = bottom_panels.sizes(cw_default, &bp_geom);
        let panels_drag = sides.ch_w > 0 && !captured && bottom_panels.input(&window, &bp_geom, cw_default, &bp0);
        ui_scale::set_origin_x(0.0);
        let bp = bottom_panels.sizes(cw_default, &bp_geom);
        let lane_f = if lanes_fill { panel_resize::PanelResize::fill_scale(&bp_geom, content_bottom as i64 - 20) } else { bp.f };
        let (mut cw_h, mut q_h, mut q_y) = (bp.cw_h, bp.q_h, bp.q_y.max(0) as usize);
        let cw_panel_y = content_bottom.saturating_sub(q_h + 40 + cw_h + 12) + 4;
        let rows_bottom = if show_cw || show_q { cw_panel_y - 4 } else { content_bottom.saturating_sub(20) };
        match (show_cw, show_q) {
            (true, false) => { cw_h += q_h + 28; q_h = 0; }
            (false, true) => { q_h += cw_h + 28; q_y = cw_panel_y; cw_h = 0; }
            (false, false) => { cw_h = 0; q_h = 0; }
            (true, true) => {}
        }

        // ── A tour step's example: the wheel zooms the channel view about the pointer and a drag pans it (over the channel
        // plots, not over the tour's box); the tour keeps that view for the step ──
        if let (Some((step, mut c, mut s)), true) = (tour_zoom, tour_ex.is_some() && ch_panel_w > 0) {
            let (mx, my) = ui_scale::mouse_pos(&window).unwrap_or((-1.0, -1.0));
            let over = mx >= ch_panel_x as f32 && mx < (ch_panel_x + ch_panel_w) as f32
                && my >= content_top as f32 && my < content_bottom as f32 && !help.over_box(mx, my);
            let n = ((mx as f64 - ch_panel_x as f64) / ch_panel_w as f64).clamp(0.0, 1.0);
            if let Some((_, wy)) = window.get_scroll_wheel().filter(|_| over) {
                if wy.abs() > 0.0 {
                    let anchor = c - s * 0.5 + s * n;
                    s = (s * if wy > 0.0 { 0.85 } else { 1.18 }).clamp(1e-6, (viz_idx.t_max - viz_idx.t_min).max(1e-6));
                    c = anchor - s * (n - 0.5);
                }
            }
            let left = window.get_mouse_down(MouseButton::Left);
            match tour_drag {
                Some((x0, c0)) if left => c = c0 - (mx - x0) as f64 * s / ch_panel_w as f64,
                None if left && !state.prev_left_down && over => tour_drag = Some((mx, c)),
                _ if !left => tour_drag = None,
                _ => {}
            }
            let v = &mut state.ch_view;
            (v.center_t, v.span_t) = (c, s);
            clamp_view(v, &viz_idx);
            tour_zoom = Some((step, v.center_t, v.span_t));
        }

        // ── Input ─────────────────────────────────────────────────────────────
        if !captured {
            handle_unified_input(
                &window, &mut state,
                &viz_idx, &abr_idx,
                w, h,
                ch_panel_x, ch_panel_w,
                abr_panel_x, abr_panel_w,
                &sides,
                num_links,
            );
        }
        if sidebar_drag || panels_drag || strip_drag {
            // the press is on a sidebar edge or a plot's title: no pan or zoom box in the panels
            state.ch_view.mouse_drag = None;
            state.abr_view.mouse_drag = None;
            state.zoom_drag = None;
        }
        state.prev_left_down  = window.get_mouse_down(MouseButton::Left);
        state.prev_right_down = window.get_mouse_down(MouseButton::Right);
        {
            // mouse pointer: hand over the draggable panels (closed while dragging), cross-hair in zoom mode,
            // resize arrows on the splitter
            let (mx, my) = mouse(&window);
            let (mxu, myu) = (mx as usize, my as usize);
            let in_y = myu >= HUD_H && myu < h.saturating_sub(TIME_AXIS);
            let in_panel = in_y && ((mxu >= ch_panel_x && mxu < ch_panel_x + ch_panel_w)
                                    || (mxu >= abr_panel_x && mxu < abr_panel_x + abr_panel_w));
            let on_splitter = sides.splitter.is_some_and(|sx| mxu >= sx && mxu < sx + SPLITTER_W) && myu >= HUD_H;
            let dragging = state.ch_view.mouse_drag.is_some() || state.abr_view.mouse_drag.is_some();
            ui_scale::set_origin_x(ch_x0 as f32);
            let bp_cursor = if sides.ch_w > 0 { bottom_panels.cursor(&window, &bp_geom, &bp) } else { None };
            ui_scale::set_origin_x(0.0);
            let sb_cursor = if has_sidebar { sidebar.cursor(&window, &sb_geom, sw, minimap_h) } else { None }
                .or(bp_cursor)
                .or_else(|| if sides.abr_w > 0 { strips.cursor(&window, &strip_geom) } else { None });
            window.set_cursor_style(
                if captured { minifb::CursorStyle::Arrow }
                else if state.splitter_drag.is_some() || on_splitter { minifb::CursorStyle::ResizeLeftRight }
                else if let Some(c) = sb_cursor.filter(|_| state.splitter_drag.is_none()) { c }
                else if state.zoom_mode && (in_panel || state.zoom_drag.is_some()) { minifb::CursorStyle::Crosshair }
                else if dragging { minifb::CursorStyle::ClosedHand }
                else { minifb::CursorStyle::Arrow },
            );
        }
        let now = Instant::now();
        if state.playing {
            let dt_real = now.duration_since(state.last_frame).as_secs_f64().min(0.1);
            let dt_sim  = state.playback_speed * dt_real;
            let new_t   = (state.ch_view.cursor_t + dt_sim)
                .min(viz_idx.t_max)
                .min(abr_idx.t_max);
            state.ch_view.cursor_t  = new_t;
            state.abr_view.cursor_t = new_t;

            let pan = |center: &mut f64, span: f64, lo: f64, hi: f64| {
                if new_t > hi || new_t < lo { *center = new_t + span * 0.30; }
            };
            let (ch_lo, ch_hi) = (
                state.ch_view.center_t - state.ch_view.span_t * 0.5,
                state.ch_view.center_t + state.ch_view.span_t * 0.5,
            );
            pan(&mut state.ch_view.center_t, state.ch_view.span_t, ch_lo, ch_hi);
            clamp_view(&mut state.ch_view, &viz_idx);

            if state.sync_cursor {
                let (al, ah) = (
                    state.abr_view.center_t - state.abr_view.span_t * 0.5,
                    state.abr_view.center_t + state.abr_view.span_t * 0.5,
                );
                pan(&mut state.abr_view.center_t, state.abr_view.span_t, al, ah);
                clamp_abr_view(&mut state.abr_view, &abr_idx);
            }

            if new_t >= viz_idx.t_max.min(abr_idx.t_max) {
                state.playing = false;
            }
        }
        state.last_frame = now;
        // ── Render ────────────────────────────────────────────────────────────
        buf.fill(0x0d0d12);

        // Link lanes (y, height, link) — the tooltip drawn last needs them
        let mut lanes: Vec<(usize, usize, u8)> = Vec::new();
        // the channel side's tooltip, worked out while its mouse origin is set and drawn over everything
        let mut ch_tip: Option<Vec<String>> = None;
        // the lanes' extent (y, height), for the tour
        let mut lanes_span = (content_top, 0usize);

        // ── 1. Channel panel (left half) ─────────────────────────────────────────────
        if sides.ch_w > 0 {
            ui_scale::set_origin_x(ch_x0 as f32);
            let ch_panel_x = ch_panel_lx; // drawn at x = 0, shifted below
            // Layout values that don't depend on lane_y: the CW and queue panels' rectangles, set above
            state.cw_panel_rect = if show_cw { (cw_panel_y, cw_h) } else { (0, 0) };
            state.q_panel_rect = if show_q { (q_y, q_h) } else { (0, 0) };

            // The lanes as they will be drawn (y, body height, link), for what the mouse is over
            let geo: Vec<(usize, usize, u8)> = {
                let mut geo = Vec::new();
                let mut y = content_top + 10;
                for (i, _lc) in link_configs.iter().enumerate() {
                    let lh = bp_geom.body(i, lane_f);
                    geo.push((y, lh, i as u8));
                    y += lh + 5;
                }
                geo
            };
            let (hmx, hmy) = mouse(&window);
            // A collision's stripes under the mouse come first: its contenders light up and the map draws it alone
            let coll_hover = hover_focus::collision_at(hmx, hmy, &geo, ch_panel_x, ch_panel_w, &state.ch_view, &viz_idx);
            // The TXOP under the mouse: highlight the queues that won / sent in it, keep their rows on screen and mark
            // the win in them
            let hover = if coll_hover.is_some() { None } else {
                lane_hover_owner(hmx, hmy, &geo, ch_panel_x, ch_panel_w, &state.ch_view, &viz_idx)
                    .or_else(|| viz_hover_override(&viz_idx))
            };
            state.ch_view.hover_t = hover.as_ref().map(|h| h.t);
            state.ch_view.hover_txop = hover.as_ref().map(|h| (h.link, h.t));
            state.ch_view.hover_coll = coll_hover.as_ref().map(|c| (c.link, c.t));
            // the one A-MPDU whose tile is under the mouse, for the map to stress its station
            state.ch_view.hover_leg = hover.as_ref()
                .and_then(|_| hover_focus::leg_at(hmx, hmy, &geo, ch_panel_x, ch_panel_w, &state.ch_view, &viz_idx));
            if let Some(hv) = &hover {
                if state.ch_view.q_mode != 0 {
                    if !state.ch_view.show_bo_matrix && show_cw {
                        let (keys, _) = cw_strip_keys(&state.ch_view, &viz_idx);
                        let (shown, _) = strip_geometry(keys.len(), cw_h, 14);
                        if let Some(pos) = keys.iter().position(|k| *k == hv.holder) {
                            let cur = state.ch_view.cw_scroll.min(keys.len().saturating_sub(shown));
                            if pos < cur || pos >= cur + shown {
                                state.ch_view.cw_scroll = pos.saturating_sub(shown / 2).min(keys.len().saturating_sub(shown));
                            }
                        }
                    }
                    let rows = qdepth_strip_keys(&state.ch_view, &viz_idx);
                    let (shown, _) = qstrip_geometry(rows.len(), q_h);
                    if let Some(pos) = rows.iter().position(|r| *r == (hv.holder.0, hv.holder.1)).filter(|_| show_q) {
                        let cur = state.ch_view.q_scroll.min(rows.len().saturating_sub(shown));
                        if pos < cur || pos >= cur + shown {
                            state.ch_view.q_scroll = pos.saturating_sub(shown / 2).min(rows.len().saturating_sub(shown));
                        }
                    }
                }
            }

            // Highlight — computed once, used by every render call below
            let ch_highlight = {
                let (mx, my) = mouse(&window);
                (if show_q { hit_test_qdepth_legend(mx, my, q_y, q_h, ch_panel_x, &state.ch_view, &viz_idx) } else { None })
                .or_else(|| if show_cw { hit_test_cw_legend(mx, my, cw_panel_y, cw_h, ch_panel_x, &state.ch_view, &viz_idx) } else { None })
                .or_else(|| coll_hover.as_ref().map(hover_focus::highlights))
                .or_else(|| hover.as_ref().map(|hv| {
                    let mut h = ActiveHighlights { keys: HashSet::new(), txops: HashSet::new() };
                    for k in &hv.keys { h.keys.insert(*k); h.txops.insert((*k, -1)); }
                    h
                }))
            };

            // Lane loop — lane_y advances here
            let mut lane_y = content_top + 10;
            for (link_id_usize, link_cfg) in link_configs.iter().enumerate() {
                let body_h = bp_geom.body(link_id_usize, lane_f);
                let lane_h = body_h;
                render_link_lane(
                    &mut buf, w, lane_y, body_h,
                    ch_panel_x, ch_panel_w,
                    &state.ch_view, &viz_idx,
                    link_id_usize as u8, link_cfg.bandwidth_mhz,
                    &ch_highlight,
                );
                lanes.push((lane_y, body_h, link_id_usize as u8));
                lane_y += lane_h + 5;
            }
            lanes_span = (content_top + 4, lane_y.saturating_sub(content_top + 4));
            if let Some(c) = &coll_hover {
                hover_focus::draw_outline(&mut buf, w, c, &lanes, ch_panel_x, ch_panel_w, &state.ch_view);
            }

            // rows_top is only valid AFTER the loop
            let rows_top = (lane_y + 10).min(rows_bottom);

            if state.ch_view.q_mode == 0 {
                render_mackey_rows(
                    &mut buf, w, rows_top, rows_bottom,
                    ch_panel_x, ch_panel_w,
                    &state.ch_view, &viz_idx,
                    &ch_highlight,
                );
            }
            if show_cw {
                render_cw_panel(
                    &mut buf, w, cw_panel_y, cw_h,
                    ch_panel_x, ch_panel_w,
                    &state.ch_view, &viz_idx,
                    &ch_highlight,
                );
            }
            if show_q {
                render_qdepth_panel(
                    &mut buf, w, q_y, q_h,
                    ch_panel_x, ch_panel_w,
                    &state.ch_view, &viz_idx,
                    &ch_highlight,
                );
            }

            if !captured {
                bottom_panels.draw(&mut buf, w, &window, &bp_geom, &bp);
            }

            // Channel cursor line
            let cx = x_of(state.ch_view.cursor_t, &state.ch_view, ch_panel_x, ch_panel_w);
            if cx >= ch_panel_x as i32 && cx < (ch_panel_x + ch_panel_w) as i32 {
                draw_vline(&mut buf, w, cx, content_top as i32, content_bottom as i32, 0xffff66);
            }

            // Details of the A-MPDU under the mouse
            {
                let (mx, my) = mouse(&window);
                ch_tip = coll_hover.as_ref().map(hover_focus::tooltip)
                    .or_else(|| hover_tooltip_lines(mx, my, &lanes, ch_panel_x, ch_panel_w, &state.ch_view, &viz_idx));
            }
            ui_scale::set_origin_x(0.0);

            // on the right (sides swapped): move what was drawn at x = 0 into place; the ABR side then draws over the
            // left part
            if ch_x0 > 0 {
                for y in content_top..content_bottom {
                    let row = y * w;
                    buf.copy_within(row..row + sides.ch_w, row + ch_x0);
                    buf[row..row + ch_x0].fill(0x0d0d12);
                }
            }
        }

        // 2. ABR panel (right half)
        if sides.abr_w > 0 {
            let (mx, my) = mouse(&window);
            // Translate mouse x into sidebar-local coordinates for the legend
            let highlight_ip = if prefs.show_legend && abr_sidebar_w > 0 {
                hit_test_abr_legend(mx - sidebar_x as f32, my, &abr_idx, abr_sidebar_w, content_top + 4)
            } else { None }
            // else the session of the A-MPDU under the mouse in a link lane
            .or_else(|| state.ch_view.hover_leg.and_then(|e| match &viz_idx.all[e] {
                VizEvent::TxopStart { sender, dest_id, .. } => obss::sta_highlight(&mb, active, if sender.0 < 0 { *dest_id } else { sender.0 }),
                _ => None,
            }));

            let minimap_y = content_bottom.saturating_sub(minimap_h);
            if abr_sidebar_w > 0 {
                if prefs.show_legend {
                    render_abr_sidebar(&mut buf, w, h, sidebar_x,
                           content_top + 4, abr_sidebar_w,
                           &abr_idx, &state.abr_view, highlight_ip, Some(if has_map { minimap_y } else { content_bottom }));
                } else {
                    abr_fill_rect(&mut buf, w, sidebar_x, 0, abr_sidebar_w, h, 0x14141c);
                }
            }
            for (si, &k) in shown_strips.iter().enumerate() {
                let strip_y = content_top + PANEL_PAD + si * abr_strip_h;
                render_abr_strip(&mut buf, w, &abr_strips[k], strip_y, abr_strip_h,
                                abr_panel_x, abr_panel_w, &state.abr_view, &abr_idx, highlight_ip,
                                Some(&viz_idx));
            }
            if shown_strips.is_empty() {
                render_text(&mut buf, "All ABR plots are hidden (ABR menu)", abr_panel_x + 12, content_top + 40, w, 0x777788, 1.5);
            }
            if !captured {
                strips.draw(&mut buf, w, &window, &strip_geom);
            }

            // ABR cursor line
            let cx = abr_x_of(state.abr_view.cursor_t, &state.abr_view, abr_panel_x, abr_panel_w);
            if cx >= abr_panel_x as i32 && cx < (abr_panel_x + abr_panel_w) as i32 {
                abr_draw_vline(&mut buf, w, cx, content_top as i32, content_bottom as i32, 0xffff66);
            }

            // ── STA minimap in the lower sidebar ─────────────────────────────────────
            if has_map && abr_sidebar_w > 0 {
                // the arrows: the tour's example, else what the mouse is over in a lane, else what is on the air at the cursor
                let v = &state.ch_view;
                let pointed = obss::MapHover { txop: v.hover_txop, coll: v.hover_coll, leg: v.hover_leg };
                let map_hover = match tour_ex {
                    Some(i) if v.hover_txop.is_none() && v.hover_coll.is_none() =>
                        obss::MapHover { txop: examples[i].txop, coll: examples[i].coll, leg: None },
                    _ => pointed,
                };
                obss::render_minimap(&mut buf, w, sidebar_x, abr_sidebar_w, minimap_y, minimap_h,
                                     &mb, active, state.abr_view.cursor_t, state.ch_view.cursor_t, highlight_ip,
                                     map_hover, &mut map_view);
            }
        }

        if let Some(ref drag) = state.zoom_drag {
            render_zoom_overlay(&mut buf, w, drag, h);
        }

        // 3. Splitter bar
        if let Some(sx) = sides.splitter {
            render_splitter(&mut buf, w, h, sx);
        }
        // 4. Each panel's ZOOM and RESET
        render_panel_buttons(&mut buf, w, state.zoom_mode, [(ch_panel_x, ch_panel_w), (abr_panel_x, abr_panel_w)]);
        // 5. Shared time axis
        render_controls_bar(&mut buf, w, &state);
        render_unified_time_axis(
            &mut buf, w, w, h,
            &state,
            ch_panel_x,  ch_panel_w,
            abr_panel_x, abr_panel_w,
        );

        // Details of the A-MPDU under the mouse, over everything else (over the tour's dimming too, below)
        if let Some(lines) = ch_tip.as_ref().filter(|_| !help.is_open()) {
            let (mx, my) = mouse(&window);
            draw_tooltip(&mut buf, w, mx, my, lines);
        }

        // 6. Focus indicator — thin coloured border on the active panel
        let focus_color = 0x334488_u32;
        let (fx, fw) = if state.focus == Focus::Channel { (sides.ch_x0, sides.ch_w) } else { (sides.abr_x0, sides.abr_w) };
        for x in fx..(fx + fw).min(w) {
            let top_i = HUD_H * w + x;
            let bot_i = (content_bottom - 1) * w + x;
            if top_i < buf.len() { buf[top_i] = focus_color; }
            if bot_i < buf.len() { buf[bot_i] = focus_color; }
        }

        // 7. The menu bar, and over everything the open menu and the help
        let menu_list = menus::build(&state, &prefs, &strips, &menus::Facts {
            num_links, has_map: !abr_idx.by_ip_sta.is_empty(), strip_names: &strip_names,
        });
        menu.draw_bar(&mut buf, w, &window, &menu_list, &menus::status(&state, &prefs));
        if want_screenshot {
            save_screenshot(&buf, w, h, &sim_tag, "unified");
            want_screenshot = false;
        }
        let ch_rect = |(y, hh): (usize, usize)| (sides.ch_x0, y, sides.ch_w, hh);
        // the tour's example as drawn: its time on its lanes, a little padded
        let example_rect = tour_ex.filter(|_| sides.ch_w > 0).and_then(|i| {
            let e = &examples[i];
            let first = lanes.iter().find(|l| l.2 == e.lanes.0)?;
            let last = lanes.iter().find(|l| l.2 == e.lanes.1)?;
            let y0 = first.0.saturating_sub(4);
            let y1 = last.0 + last.1 + 4;
            let (px0, px1) = (ch_panel_lx as i32, (ch_panel_lx + ch_panel_w) as i32);
            let xa = (x_of(e.t0, &state.ch_view, ch_panel_lx, ch_panel_w) - 6).clamp(px0, px1);
            let xb = (x_of(e.t1, &state.ch_view, ch_panel_lx, ch_panel_w) + 6).clamp(xa + 1, px1.max(xa + 1));
            Some((ch_x0 + xa as usize, y0, (xb - xa) as usize, y1 - y0))
        });
        let r = tour::Regions {
            w, h,
            menu: (0, 0, w, HUD_H),
            controls: (0, HUD_H, w, CONTROLS_H),
            lanes: (sides.ch_w > 0).then(|| ch_rect(lanes_span)),
            bottom: (sides.ch_w > 0 && (show_cw || show_q)).then(|| {
                let top = if show_cw { cw_panel_y } else { q_y }.saturating_sub(18);
                ch_rect((top, content_bottom.saturating_sub(top)))
            }),
            abr_plots: (abr_panel_w > 0 && n_abr > 0).then(|| (abr_panel_x, content_top, abr_panel_w, content_h)),
            legend: (abr_sidebar_w > 0 && prefs.show_legend).then(|| {
                let bottom = if has_map { content_bottom.saturating_sub(minimap_h) } else { content_bottom };
                (sidebar_x, content_top, abr_sidebar_w, bottom.saturating_sub(content_top))
            }),
            map: (abr_sidebar_w > 0 && has_map).then(|| (sidebar_x, content_bottom.saturating_sub(minimap_h), abr_sidebar_w, minimap_h)),
            time_axis: (0, h.saturating_sub(TIME_AXIS), w, TIME_AXIS),
            cw: (sides.ch_w > 0 && show_cw).then(|| (sides.ch_x0, cw_panel_y.saturating_sub(2), sides.ch_w, cw_h + 4)),
            example: example_rect,
        };
        menu.draw_menu(&mut buf, w, &window, &menu_list);
        if help.is_open() {
            let about = [
                "SiEStA-VR post-simulation viewer: one run replayed from its event log, the Wi-Fi channel on one side \
                 and the video sessions on the other.".to_string(),
                format!("Run: {}", sim_tag),
                format!("{} link(s), {} video session(s).", num_links, abr_idx.ip_order.len()),
                format!("File > Save screenshot writes a PNG into {}/.", SCREENSHOT_DIR),
            ];
            let mouse = ui_scale::mouse_pos(&window).unwrap_or((-1.0, -1.0));
            help.draw(&mut buf, w, mouse, &r, &examples, &about);
            if let Some(lines) = &ch_tip {
                draw_tooltip(&mut buf, w, mouse.0, mouse.1, lines);
            }
        }
        if let Some((step, frames, all)) = tour_shots.as_mut() {
            // two frames at a step: the first one moves the view to its example
            *frames += 1;
            if *frames >= 2 {
                let n_tour = tour::Help::tour_len(&r, &examples);
                let name = match step.checked_sub(n_tour) {
                    None => format!("tour{:02}", step),
                    Some(k) if k < menu_list.len() => format!("menu{}_{}", k, menu_list[k].title.to_lowercase()),
                    Some(k) if k == menu_list.len() => "keys".to_string(),
                    Some(_) => "about".to_string(),
                };
                save_screenshot(&buf, w, h, &sim_tag, &name);
                let last = if *all { n_tour + menu_list.len() + 2 } else { 0 };
                if *step + 1 < last {
                    (*step, *frames) = (*step + 1, 0);
                } else {
                    tour_shots = None;
                    help.overlay = None;
                    menu.open_at(None);
                }
            }
        }
        regions = Some(r);

        window.update_with_buffer(&buf, w, h).unwrap();

        if !screenshot_taken {
            save_screenshot(&buf, w, h, &sim_tag, "unified");
            screenshot_taken = true;
        }
        if exit_after_shots && tour_shots.is_none() {
            break;
        }
    }
}

// ── Helper: render ABR sidebar offset to the right half ──────────────────────
// The original render_abr_sidebar draws at x=0; we need it offset to `x_off`.
// Rather than refactoring the original, we render into a small temp buffer and
// blit it across.  This keeps both viewers' internals unchanged.




/// The holder's channel access before a TXOP starting at `t_txop`, from its first `TxopStart`: (AIFS start, AIFS end,
/// countdown start, countdown end, slots counted). The countdown ends at the TXOP when the holder was contending for it,
/// earlier when it ran out with nothing to send (post-backoff): the holder then waited at zero until a frame arrived. It
/// starts at the end of AIFS, or later when the holder drew again while the medium stayed idle (a win with nothing to
/// send). The lane shows the time outside AIFS and the countdown as idle air.
fn access_window(ev: &VizEvent, t_txop: f64) -> Option<(f64, f64, f64, f64, i64)> {
    let VizEvent::TxopStart { idle_since, aifs_s, bo_drawn, bo_origin, bo_start, bo_zero_at, .. } = ev else { return None };
    if *idle_since < 0.0 || *idle_since >= t_txop {
        return None;
    }
    if *bo_zero_at >= 0.0 && *bo_start >= *bo_origin && *bo_origin >= *idle_since && *bo_origin <= t_txop {
        let end = bo_zero_at.clamp(*bo_origin, t_txop);
        let start = bo_start.min(end);
        let slots = ((end - start) / crate::lib::SLOT).round() as i64;
        return Some((bo_origin - aifs_s, *bo_origin, start, end, slots));
    }
    // no zero instant recorded: assume the countdown ran right up to the TXOP
    let k = ((t_txop - idle_since - aifs_s) / crate::lib::SLOT).round().max(0.0) as i64;
    let slots = k.min(*bo_drawn as i64);
    let bo_start = t_txop - slots as f64 * crate::lib::SLOT;
    Some(((bo_start - aifs_s).max(*idle_since), bo_start, bo_start, t_txop, slots))
}

/// Back-off state of `key` at time `t`, reconstructed from the sparse `BackoffSnap`s: the model credits idle slots lazily, so
/// from the latest snapshot the counter drops one per slot time of every idle period after AIFS, up to `t`.
/// Returns (counter, cw, frozen at the snapshot, medium_free_since).
fn backoff_at(idx: &VizIndex, key: &MacKey, t: f64) -> Option<(u32, u32, bool, f64)> {
    let VizEvent::BackoffSnap { t: snap_t, counter: snap_counter, cw, frozen, medium_free_since, .. } =
        idx.backoff_by_key.get(key).and_then(|v| latest_at(v, &idx.all, t))? else { return None };
    let aifs = aifs_secs_for_ac(key.1);
    let empty: Vec<TxopGroup> = Vec::new();
    let groups = idx.txop_groups_by_link.get(&key.2).unwrap_or(&empty);
    // slots counted in an idle period that began at `i0`, up to `t`
    let counted = |i0: f64, t: f64| if t > i0 + aifs { ((t - i0 - aifs) / crate::lib::SLOT).floor() as u32 } else { 0 };
    let mut next = groups.partition_point(|g| g.end <= *snap_t);
    // where the idle period the snapshot falls in began, and how much of it the snapshot already holds
    let in_busy = groups.get(next).is_some_and(|g| g.t <= *snap_t);
    let (mut idle_start, mut credited) = if in_busy {
        let e = groups[next].end;
        next += 1;
        (e, 0)
    } else {
        let i0 = medium_free_since.max(if next > 0 { groups[next - 1].end } else { 0.0 });
        (i0, if *frozen { 0 } else { counted(i0, *snap_t) })
    };
    let mut dec = 0u32;
    loop {
        let busy_at = groups.get(next).map_or(f64::INFINITY, |g| g.t);
        let t_hi = t.min(busy_at);
        if t_hi > idle_start {
            dec += counted(idle_start, t_hi).saturating_sub(credited);
        }
        credited = 0;
        if t <= busy_at { break; }
        idle_start = groups[next].end;
        next += 1;
    }
    let mut counter = snap_counter.saturating_sub(dec);
    // Inside the access window drawn in the link lane for the TXOP this queue is about to win (AIFS, then the slots it
    // drew), show that same countdown, so the lane and this table always agree.
    let gi = groups.partition_point(|g| g.t <= t);
    if let Some(g) = groups.get(gi).filter(|g| g.holder == *key) {
        if let Some((_, _, bo_start, bo_end, slots)) = g.legs.first().and_then(|&e| access_window(&idx.all[e], g.t)) {
            if t >= bo_start {
                counter = if t >= bo_end { 0 } else { ((bo_end - t) / crate::lib::SLOT).ceil() as i64 }.max(0) as u32;
            }
        }
    }
    Some((counter, *cw, *frozen, *medium_free_since))
}

/// One backoff cell at (x, y, w, h): AIFS pill (amber while the AIFS runs), the counter as a bar of the current CW (blue;
/// purple when frozen) and `counter/CW` as text, from `backoff_at` the cursor.
fn draw_backoff_cell(buf: &mut [u32], stride: usize, x: usize, y: usize, w: usize, h: usize,
                     idx: &VizIndex, key: &MacKey, cursor: f64, dimmed: bool, ticks: bool) {
    let ty = y + h.saturating_sub(7) / 2;
    let Some((counter, cw, frozen, medium_free_since)) = backoff_at(idx, key, cursor) else {
        render_text(buf, "-", x + 2, ty, stride, 0x666677, 1.0);
        return;
    };
    let (counter, cw, frozen) = (&counter, &cw, &frozen);
    let txt_w = 10 * 6;
    let bar_x = x + 9;
    let bar_w = w.saturating_sub(9 + txt_w + 6).max(12);
    let bh = h.saturating_sub(5).max(4);
    let by = y + (h - bh) / 2;
    let aifs_active = cursor < medium_free_since + aifs_secs_for_ac(key.1);
    fill_rect(buf, stride, x, by, 6, bh, maybe_dim(if aifs_active { 0xffaa00 } else { 0x333344 }, dimmed));
    fill_rect(buf, stride, bar_x, by, bar_w, bh, maybe_dim(0x222230, dimmed));
    let frac = (*counter as f64 / (*cw).max(1) as f64).clamp(0.0, 1.0);
    let fill_col = maybe_dim(if *frozen { 0x8860b0 } else { 0x44aaff }, dimmed);
    // As in the original table: one tick per back-off slot of the CW (64 at most, each standing for CW/64 slots beyond that),
    // filled for the slots still to count
    let n_cells = (*cw as usize).clamp(1, 64);
    let cell_w = bar_w / n_cells;
    if ticks && cell_w >= 2 {
        let lit = ((*counter as usize * n_cells + (*cw as usize).max(1) - 1) / (*cw as usize).max(1)).min(n_cells);
        for c in 0..n_cells {
            fill_rect(buf, stride, bar_x + c * cell_w, by, cell_w - 1, bh, if c < lit { fill_col } else { maybe_dim(0x222230, dimmed) });
        }
    } else {
        fill_rect(buf, stride, bar_x, by, ((bar_w as f64 * frac) as usize).max(if *counter > 0 { 1 } else { 0 }), bh, fill_col);
    }
    let t = format!("{}/{}{}", counter, cw, if *frozen { "*" } else { "" });
    render_text(buf, &t, bar_x + bar_w + 6, ty, stride, maybe_dim(0xf0f0ff, dimmed), 1.0);
}

/// Rows of the backoff grid: every (station, link) that has an EDCA queue, AP first.
fn bo_matrix_rows(view: &ViewState, idx: &VizIndex) -> Vec<(i32, u8)> {
    let mut rows: Vec<(i32, u8)> = idx.mac_keys_sorted.iter()
        .filter(|k| view.selected_link.map_or(true, |f| k.2 == f))
        .map(|k| (k.0, k.2))
        .collect::<HashSet<_>>().into_iter().collect();
    rows.sort_by_key(|&(sta, link)| (sta >= 0, sta, link));
    rows
}

/// The CW panel's alternative (`B`): backoff counters of every queue in a station x AC grid.
fn render_backoff_matrix(
    buf: &mut [u32], stride: usize,
    panel_y: usize, panel_h: usize,
    panel_x: usize, panel_w: usize,
    view: &ViewState, idx: &VizIndex,
    highlight: &Option<ActiveHighlights>,
) {
    let rows = bo_matrix_rows(view, idx);
    let mut acs: Vec<EdcaAc> = idx.mac_keys_sorted.iter().map(|k| k.1).collect::<HashSet<_>>().into_iter().collect();
    acs.sort_by_key(|a| std::cmp::Reverse(ac_prio(*a)));
    let head_h = 12usize;
    let (shown, row_h) = strip_geometry(rows.len(), panel_h.saturating_sub(head_h), 13);
    let start = view.cw_scroll.min(rows.len().saturating_sub(shown));
    draw_row_scrollbar(buf, stride, panel_y + head_h, panel_h.saturating_sub(head_h), panel_x, rows.len(), shown, start);
    let col_w = panel_w / acs.len().max(1);

    // header: the AC of each column and what the cells show
    render_text(buf, "BACKOFF  [B] = CW history", 8, panel_y + 3, stride, 0xaab0c8, 1.0);
    for (c, ac) in acs.iter().enumerate() {
        render_text(buf, ac.to_string().trim_start_matches("AC_"), panel_x + c * col_w + 6, panel_y + 3, stride, qkey_color(0, *ac), 1.0);
        let cx = panel_x + c * col_w;
        for yy in panel_y..panel_y + panel_h { if yy * stride + cx < buf.len() { buf[yy * stride + cx] = 0x222233; } }
    }
    for (r, &(sta, link)) in rows.iter().enumerate().skip(start).take(shown) {
        let y = panel_y + head_h + (r - start) * row_h;
        fill_rect(buf, stride, panel_x, y, panel_w, row_h, if r % 2 == 0 { 0x10101a } else { 0x0d0d14 });
        let who = if sta < 0 { ap_name(sta) } else { format!("s{}", sta) };
        let ty = y + row_h.saturating_sub(7) / 2;
        render_text(buf, &format!("{} L{}", who, link), 8, ty, stride, 0xdddddd, 1.0);
        for (c, ac) in acs.iter().enumerate() {
            let key: MacKey = (sta, *ac, link);
            if !idx.backoff_by_key.contains_key(&key) { continue; }
            let dimmed = is_dimmed(sta, *ac, highlight);
            draw_backoff_cell(buf, stride, panel_x + c * col_w + 6, y, col_w.saturating_sub(12), row_h, idx, &key, view.cursor_t, dimmed, true);
        }
    }
}


/// Thin white mark at `view.hover_t` through a highlighted strip row (where the hovered TXOP was won).
fn draw_hover_mark(buf: &mut [u32], stride: usize, view: &ViewState, panel_x: usize, panel_w: usize, y0: usize, y1: usize, active: bool) {
    if !active { return; }
    if let Some(ht) = view.hover_t {
        let x = x_of(ht, view, panel_x, panel_w);
        if x >= panel_x as i32 && x < (panel_x + panel_w) as i32 {
            fill_rect_clip(buf, stride, x, y0 as i32, x + 2, y1 as i32, 0xffffff);
        }
    }
}

/// CW panel for `Q` modes 1-3: one strip per EDCA queue (station, AC, link) on a shared log2 scale, so nothing overlaps;
/// mode 2 shows the level as colour instead of height. Left: name and `CW now / max in view`.
fn render_cw_variant(
    buf: &mut [u32], stride: usize,
    panel_y: usize, panel_h: usize,
    panel_x: usize, panel_w: usize,
    view: &ViewState, idx: &VizIndex,
    highlight: &Option<ActiveHighlights>,
) {
    let t_lo = view.center_t - view.span_t * 0.5;
    let (keys, hidden_const) = cw_strip_keys(view, idx);
    let (shown, row_h) = strip_geometry(keys.len(), panel_h, 14);
    let start = view.cw_scroll.min(keys.len().saturating_sub(shown));
    draw_row_scrollbar(buf, stride, panel_y, panel_h, panel_x, keys.len(), shown, start);
    let scale = if row_h >= 16 { 2.0f32 } else { 1.0 };
    let log_max = 10.0f64; // log2(1023 + 1)

    for (r, key) in keys.iter().enumerate().skip(start).take(shown) {
        let ser = &idx.cw_series[key];
        let y = panel_y + 1 + (r - start) * row_h;
        let dimmed = is_dimmed(key.0, key.1, highlight);
        fill_rect(buf, stride, panel_x, y, panel_w, row_h, if r % 2 == 0 { 0x10101a } else { 0x0d0d14 });
        let color = maybe_dim(qkey_color(key.0, key.1), dimmed);
        fill_rect(buf, stride, 0, y + 1, 3, row_h.saturating_sub(2).max(1), color);

        let who = if key.0 < 0 { ap_name(key.0) } else { format!("s{}", key.0) };
        let name = format!("{} {} L{}", who, key.1.to_string().trim_start_matches("AC_"), key.2);
        let ty = y + row_h.saturating_sub(7) / 2;
        render_text(buf, &name, 8, ty, stride, maybe_dim(0xdddddd, dimmed), 1.0);

        let cols = column_values(ser, |&(t, cw, _)| (t, cw as usize), t_lo, view.span_t, panel_w);

        // backoff state at the cursor, in the label column
        let bx0 = 8 + 11 * 6 + 8;
        draw_backoff_cell(buf, stride, bx0, y, panel_x.saturating_sub(bx0 + 8), row_h, idx, key, view.cursor_t, dimmed, false);

        let base = (y + row_h - 1) as i32;
        let plot_h = row_h.saturating_sub(3).max(1) as f64;
        if view.q_mode != 2 {
            // guides at CW 15 and 255
            for lvl in [15.0f64, 255.0] {
                let gy = base - ((lvl + 1.0).log2() / log_max * plot_h) as i32;
                for gx in (panel_x..panel_x + panel_w).step_by(6) {
                    fill_rect_clip(buf, stride, gx as i32, gy, gx as i32 + 2, gy + 1, 0x2a2a3c);
                }
            }
        }
        for (c, &cw) in cols.iter().enumerate() {
            let x = (panel_x + c) as i32;
            let f = ((cw as f64 + 1.0).log2() / log_max).clamp(0.0, 1.0);
            if view.q_mode == 2 {
                fill_rect_clip(buf, stride, x, y as i32, x + 1, (y + row_h - 1) as i32, heat_color(if dimmed { f * 0.4 } else { f }));
            } else if cw > 0 {
                let hgt = (f * plot_h).ceil().max(1.0) as i32;
                draw_vline_alpha(buf, stride, x, base - hgt + 1, base, color, 0.55);
                fill_rect_clip(buf, stride, x, base - hgt + 1, x + 1, base - hgt + 2, color);
            }
        }
    }
    for (r, key) in keys.iter().enumerate().skip(start).take(shown) {
        let y = panel_y + 1 + (r - start) * row_h;
        draw_hover_mark(buf, stride, view, panel_x, panel_w, y, y + row_h - 1, highlight.is_some() && !is_dimmed(key.0, key.1, highlight));
    }
    let more = if keys.len() > shown { format!("  rows {}-{} of {} (wheel on labels)", start + 1, start + shown, keys.len()) } else { String::new() };
    let head = format!("CW: {} (log2, guides at 15 and 255; {} constant hidden){}  [Q]",
                       if view.q_mode == 2 { "heatmap" } else { "strips" }, hidden_const, more);
    let hx = (panel_x + panel_w).saturating_sub(text_w(&head, 1.0) as usize + 6);
    render_text(buf, &head, hx, panel_y + 1, stride, 0x707088, 1.0);
}

fn render_cw_panel(
    buf:       &mut [u32], stride: usize,
    panel_y:   usize, panel_h: usize,
    panel_x:   usize, panel_w: usize,
    view:      &ViewState,
    idx:       &VizIndex,
    highlight: &Option<ActiveHighlights>,
) {
    fill_rect(buf, stride, panel_x, panel_y, panel_w, panel_h, 0x10101a);
    fill_rect(buf, stride, 0,       panel_y, panel_x, panel_h, 0x14141c);
    if view.q_mode != 0 {
        if view.show_bo_matrix {
            render_backoff_matrix(buf, stride, panel_y, panel_h, panel_x, panel_w, view, idx, highlight);
        } else {
            render_cw_variant(buf, stride, panel_y, panel_h, panel_x, panel_w, view, idx, highlight);
        }
        return;
    }
    render_text(buf, "CW", 8, panel_y + 6, stride, 0xcccccc, 2);

    let t_lo = view.center_t - view.span_t * 0.5;
    let t_hi = view.center_t + view.span_t * 0.5;

    // ── Use the pre-built cache — no per-frame HashMap construction ───────────
    let series_map = &idx.cw_series;   // replaces the old build block entirely

    // ── Find visible maximum for scale ────────────────────────────────────────
    let mut max_cw = 4u32;
    for series in series_map.values() {
        let s = series.partition_point(|(t, _, _)| *t < t_lo);
        if s > 0 { max_cw = max_cw.max(series[s - 1].1); }
        for &(t, cw, _) in &series[s..] {
            if t > t_hi { break; }
            max_cw = max_cw.max(cw);
        }
    }

    // ── 3. Coordinate helper — log₂ scale ────────────────────────────────────
    //
    // CW is always a power-of-two minus one (3,7,15,31,…) so log₂ gives even
    // spacing and avoids tiny Voice rows drowning under BE.
    let base_y    = panel_y as i32 + panel_h as i32 - 2;
    let plot_h    = (panel_h as f64 - 12.0).max(1.0);
    let max_log   = (max_cw as f64 + 1.0).log2().max(1.0);

    let calc_y = |cw: u32| -> i32 {   // was u16
        let ratio = if cw == 0 {
            0.0
        } else {
            (cw as f64 + 1.0).log2() / max_log
        };
        let raw = base_y - (ratio * plot_h) as i32;
        raw.clamp(panel_y as i32, base_y)
    };


    // ── 4. Grid lines at exact EDCA CW power-of-two values ───────────────────
    for &cw_mark in &[3u32, 7, 15, 31, 63, 127, 255, 511, 1023] {  
        if cw_mark > max_cw { break; }
        let gy = calc_y(cw_mark);
        for gx in panel_x..(panel_x + panel_w) {
            if (gx % 8) < 4 {
                let gi = gy as usize * stride + gx;
                if gi < buf.len() { buf[gi] = 0x222233; }
            }
        }
        render_text(
            buf, &format!("{}", cw_mark),
            panel_x + 2, (gy as usize).saturating_sub(8),
            stride, 0x444455, 1,
        );
    }

    // ── 5. Consistent color / dimming helpers (mirrors render_qdepth_panel) ──
    let key_base_color = |(sta_id, ac, _link): MacKey| -> u32 {
        if sta_id < 0 {
            ac_color(ac)
        } else {
            [0xFF4444, 0xFF8822, 0xFFCC33, 0xFF55AA,
             0x44AAFF, 0x44FF88, 0x8844FF, 0x44FFEE]
                [(sta_id.unsigned_abs() as usize) % 8]
        }
    };

    // dash pattern: AP → solid, STA → dashed (same as qdepth)
    let pattern_for = |(sta_id, _, _): MacKey| -> usize {
        if sta_id < 0 { 0 } else { 2 }
    };

    // Sort: high-priority AC first, then by sta_id (mirrors qdepth ordering)
    let mut keys: Vec<MacKey> = series_map.keys().copied().collect();
    keys.sort_by_key(|k| (ac_prio(k.1), k.0, k.2));
    keys.reverse();

    // ── 6. Pass 1 — translucent fills (drawn below outlines) ─────────────────
    for &key in &keys {
        let series = &series_map[&key];
        let dimmed       = is_dimmed(key.0, key.1, highlight);
        let base_color   = key_base_color(key);
        let outline_c    = maybe_dim(base_color, dimmed);
        let fill_c       = dim_color(outline_c, if dimmed { 2 } else { 6 });
        let fill_alpha   = if dimmed { 0.08 } else { 0.25 };

        let s = series.partition_point(|(t, _, _)| *t < t_lo);
        // let mut prev: Option<(i32, i32, u16)> = None;
        let mut prev: Option<(i32, i32, u32)> = None;

        if s > 0 {
            let (_, cw, _) = series[s - 1];
            // prev = Some((x_of(t_lo, view, panel_x, panel_w), calc_y(cw), cw.try_into().unwrap()));
            prev = Some((x_of(t_lo, view, panel_x, panel_w), calc_y(cw), cw));
        }

        let emit_fill = |buf: &mut [u32], px: i32, x: i32, py: i32, prev_cw: u32| { 
            if prev_cw == 0 { return; }
            for fx in px..x {
                if fx >= panel_x as i32 && fx < (panel_x + panel_w) as i32 {
                    draw_vline_alpha(buf, stride, fx, py + 2, base_y, fill_c, fill_alpha);
                }
            }
        };

        for &(t, cw, _frozen) in &series[s..] {
            if t > t_hi { break; }
            let x = x_of(t, view, panel_x, panel_w);
            let y = calc_y(cw);
            if let Some((px, py, prev_cw)) = prev {
                emit_fill(buf, px, x, py, prev_cw);
            }
            prev = Some((x, y, cw));
        }
        if let Some((px, py, prev_cw)) = prev {
            emit_fill(buf, px, (panel_x + panel_w) as i32, py, prev_cw);
        }
    }

    // ── 7. Pass 2 — outlines + legend (drawn on top of all fills) ────────────
    let mut legend_y = panel_y + 24;

    for &key in &keys {
        let series       = &series_map[&key];
        let dimmed       = is_dimmed(key.0, key.1, highlight);
        let base_color   = key_base_color(key);
        let outline_c    = maybe_dim(base_color, dimmed);
        let pattern_type = pattern_for(key);

        let s = series.partition_point(|(t, _, _)| *t < t_lo);
        let is_active = s > 0 || (s < series.len() && series[s].0 <= t_hi);

        // Legend swatch
        if is_active && legend_y + 12 < panel_y + panel_h {
            let lbl = if key.0 < 0 {
                format!("{:<4} {:?} L{}", ap_name(key.0), key.1, key.2)
            } else {
                format!("s{:<3} {:?} L{}", key.0, key.1, key.2)
            };
            render_text(buf, &lbl, 35, legend_y, stride, maybe_dim(0xdddddd, dimmed), 2);
            for px in 0..22usize {
                if should_draw_pixel(px as i32, pattern_type) {
                    for ty in 0..2 {
                        let ii = (legend_y + 5 + ty) * stride + (8 + px);
                        if ii < buf.len() { buf[ii] = outline_c; }
                    }
                }
            }
            legend_y += 20;
        }

       // Outlines
        let hold_y = if s > 0 {
            let (_, cw, _) = series[s - 1];
            Some(calc_y(cw))
        } else {
            None
        };

        let mut pts: Vec<(i32, i32)> = Vec::new();
        let mut prev_y = hold_y;
        for &(t, cw, _frozen) in &series[s..] {
            if t > t_hi { break; }
            let x = x_of(t, view, panel_x, panel_w);
            let y = calc_y(cw);
            if let Some(py) = prev_y {
                pts.push((x, py));   // outgoing level
            }
            pts.push((x, y));
            prev_y = Some(y);
        }

        // let envelope = build_step_envelope(&pts, panel_x, panel_w, hold_y);
        let mut envelope_buf: Vec<Option<(i32, i32)>> = Vec::with_capacity(panel_w);
        // then inside the per-series loop:
        build_step_envelope(&pts, panel_x, panel_w, hold_y, &mut envelope_buf);
        for (col, entry) in envelope_buf.iter().enumerate() {
            let Some((y_min, y_max)) = *entry else { continue };
            let x = panel_x as i32 + col as i32;
            if !should_draw_pixel(x, pattern_type) { continue; }

            // Bounds guard — y values can be negative or out of range at extreme zoom
            if y_max < panel_y as i32 || y_min > (panel_y + panel_h) as i32 { continue; }
            let y_max_safe = y_max.clamp(panel_y as i32, (panel_y + panel_h - 1) as i32);
            let y_min_safe = y_min.clamp(panel_y as i32, (panel_y + panel_h - 1) as i32);

            // Horizontal step mark (2 px tall) at the current value level —
            // blend so overlapping series stay visible instead of erasing each other
            for ty in 0..2i32 {
                let row = (y_max_safe + ty) as usize;
                let oi  = row * stride + x as usize;
                if oi < buf.len() {
                    buf[oi] = blend_screen(buf[oi], outline_c);
                }
            }

            // Vertical transition bar when the step spans multiple rows
            if y_min_safe < y_max_safe {
                for y in y_min_safe..=y_max_safe {
                    let oi = y as usize * stride + x as usize;
                    if oi < buf.len() {
                        buf[oi] = blend_screen(buf[oi], outline_c);
                    }
                }
            }
        }
    }

    render_text(
        buf, &format!("max={}", max_cw),
        panel_x + 6, panel_y + 6,
        stride, 0x888899, 2,
    );
}



