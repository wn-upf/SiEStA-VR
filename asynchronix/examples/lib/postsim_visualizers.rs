use std::collections::HashMap;
use std::net::IpAddr;
use std::path::Path;
use minifb::{Key, MouseButton, MouseMode, Window, WindowOptions};
use image::{ImageBuffer, Rgb};
use rand::Rng;
use crate::lib::models_XR::AbrEvent;
use crate::lib::{render_text, ac_prio,} ;

// ── Auto-screenshot: dumps the first rendered frame of each viewer window to
// disk so batch/eval runs get a visual record without requiring a human to
// look at (or close) the interactive window.
const SCREENSHOT_DIR: &str = "viz_screenshots";

fn sanitize_filename(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
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
const SPEED_MIN:  f64   = 9e-6;   // 9 µs of sim-time per real-second
const SPEED_MAX:  f64   = 33e-3;  // 33 ms of sim-time per real-second

const CONTROLS_H:   usize = 52;            // taller: two rows
const BTN_Y:        usize = HUD_H + 4;     // row-1 button top  (y = 54)
const BTN_H:        usize = 22;            // row-1 button height
const SLIDER_ROW_Y: usize = HUD_H + 32;   // row-2 slider top  (y = 82)
const SLIDER_ROW_H: usize = 16;            // row-2 slider height
const BTN_PLAY_X:   usize = 8;
const BTN_PLAY_W:   usize = 76;
const BTN_RESET_X:  usize = BTN_PLAY_X + BTN_PLAY_W + 8;
const BTN_RESET_W:  usize = 76;
const SLIDER_X0:    usize = 40;            // row-2: slider starts near left edge

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
    if s < 1e-3 { format!("{:.2}µs/s", s * 1e6) }
    else        { format!("{:.2}ms/s", s * 1e3)  }
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

const HUD_BTN_H: usize = 34;   // height of zoom/reset buttons in the HUD strip
const HUD_BTN_Y: usize = 7;    // y of those buttons (inside HUD_H = 50)
const HUD_BTN_PAD: usize = 6;const ZOOM_BTN_W:   usize = 88;
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

fn hit_test_cw_legend(
    mx: f32, my: f32,
    panel_y: usize, panel_h: usize,
    panel_x: usize,
    view:    &ViewState,
    idx:     &VizIndex,
) -> Option<ActiveHighlights> {
    if (mx as usize) >= panel_x { return None; }
    let my_u = my as usize;

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
fn hit_test_abr_legend(mx: f32, my: f32, idx: &AbrVizIndex) -> Option<usize> {
    // Legend lives entirely inside the sidebar
    if mx as usize >= SIDEBAR_W { return None; }

    // Matches the layout in render_abr_sidebar: ly starts at 52, each item is 28px
    let my_u = my as usize;
    let legend_start = 52usize;
    // let item_h       = 28usize;
    let item_h = 44usize;

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
    idx:          &AbrVizIndex,
    view:         &AbrViewState,
    highlight_ip: Option<usize>,
    max_y:        Option<usize>,
) {
    abr_fill_rect(buf, stride, x_off, 0, SIDEBAR_W, height, 0x14141c);
    render_text(buf, "ABR METRICS", x_off + 8, 8, stride, 0xffffff, TEXT_SIZE_ABR * 2.0);


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

        abr_fill_rect(buf, stride, x_off + 8, ly, 18, 10, color);
        render_text(buf, &format!("{}", ip), x_off + 30, ly,      stride, text_col,  TEXT_SIZE_ABR);
        render_text(buf, mode,               x_off + 30, ly + 20, stride, 0x888899, TEXT_SIZE_ABR);

        if let Some(limit) = max_y {
            if ly + 44 > limit { break; }
        }
        ly += 44;
    }

    ly += 18;
    render_text(buf, &format!("t = {:.3}s", view.cursor_t),
                x_off + 8, ly, stride, 0xaaaaaa, TEXT_SIZE_ABR);
    ly += 22;

    // ── Cursor values table ───────────────────────────────────────────────────────
    ly += 6;

    // Column x-positions relative to x_off
    // After — table_w fills the sidebar; RTT and FLR scale with it
    const C_SWATCH: usize = 8;
    const C_MBPS:   usize = 26;
    const ROW_H:    usize = 20;
    let table_w = SIDEBAR_W.saturating_sub(8);   // 4 px margin each side
    let c_rtt   = table_w * 42 / 100;            // 42 % across
    let c_flr   = table_w * 69 / 100;            // 69 % across

    // ── Header row ────────────────────────────────────────────────────────────────
    abr_fill_rect(buf, stride, x_off + 4, ly, table_w, ROW_H, 0x1c1c2c);
    render_text(buf, "Mbps", x_off + C_MBPS, ly + 4, stride, 0x6677aa, 1.3);
    render_text(buf, "RTT",  x_off + c_rtt,  ly + 4, stride, 0x6677aa, 1.3);
    render_text(buf, "FLR",  x_off + c_flr,  ly + 4, stride, 0x6677aa, 1.3);
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
        abr_fill_rect(buf, stride, x_off + C_SWATCH, ly + 6, 12, 8, swatch_c);

        // Values — bitrate in the IP colour, RTT/FLR neutral
        render_text(buf, &format!("{:.1}", bitrate_mbps),
                    x_off + C_MBPS, ly + 4, stride, swatch_c, 1.3);
        render_text(buf, &format!("{:.0}ms", rtt_ms),
                    x_off + c_rtt,  ly + 4, stride, val_c,    1.3);
        render_text(buf, &format!("{:.3}", flr),
                    x_off + c_flr,  ly + 4, stride, val_c,    1.3);

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
        render_text(buf, &format!("{:.2}s", t), x as usize + 2, y + 4, stride, 0x888899, TEXT_SIZE_ABR);
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

    let (mx, _my) = window.get_mouse_pos(MouseMode::Discard).unwrap_or((0.0, 0.0));

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
    view.span_t = view.span_t.clamp(1e-3, full_range * 2.0);

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
/// runs close a still-open previous window itself instead of letting old graphs
/// pile up window after window. `None` behaves exactly as before: open until the
/// user closes it by hand.
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
        let (w, h) = window.get_size();
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

        let (mx, my)     = window.get_mouse_pos(MouseMode::Discard).unwrap_or((0.0, 0.0));
        let highlight_ip = hit_test_abr_legend(mx, my, &idx);

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
        render_abr_sidebar(&mut buf, w, h, 0, 52, &idx, &view, highlight_ip, None);
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
                models_mm1k::{LinkConfig, VizEvent}, 
                alvr_stream_socket::FRAMELOSS_PACKET, };

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
            if let VizEvent::TxopStart { t: start, end, owner, dest_id, .. } = ev {
                if t >= *start && t <= *end {
                    h.keys.insert(*owner);
                    h.txops.insert((*owner, *dest_id));
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

    let mut buf = vec![0u32; W * H];
    let mut view = ViewState {
        center_t:      (idx.t_min + idx.t_max) * 0.5,
        span_t:        ((idx.t_max - idx.t_min) * 0.05).max(0.001),
        cursor_t:      idx.t_min,
        paused:        false,
        selected_link: None,
        row_scroll:    0,
        mouse_drag:    None,
    };

    let mut screenshot_taken = false;

    while window.is_open() && !window.is_key_down(Key::Escape) {
        // ── live dimensions ───────────────────────────────────────────────
        let (w, h) = window.get_size();
        if buf.len() != w * h {
            buf.resize(w * h, 0);
        }

        // ── layout derived from live size ─────────────────────────────────
        let panel_x = 270;
        let panel_w = w.saturating_sub(panel_x + 20);

        // ── input ─────────────────────────────────────────────────────────
        let (mx, my) = window.get_mouse_pos(MouseMode::Discard).unwrap_or((0.0, 0.0));

        handle_input(&window, &mut view, &idx, panel_w, panel_x, link_configs.len() as u8);
        clamp_view(&mut view, &idx);

        let highlight_keys =
            hit_test_qdepth_legend(mx, my, h - 180, 160, panel_x, &view, &idx);

        // ── render ────────────────────────────────────────────────────────
        buf.fill(0x0d0d12);
        render_hud(&mut buf, w, &view, &idx);

        let mut lane_y = 60usize;
        let lane_h     = 50usize;

        for (link_id_usize, link_config) in link_configs.iter().enumerate() {
            let link_id  = link_id_usize as u8;
            let bw_link  = link_config.bandwidth_mhz;
            render_link_lane(
                &mut buf, w, lane_y, lane_h,
                panel_x, panel_w, &view, &idx, link_id, bw_link,
                &highlight_keys,
            );
            lane_y += lane_h + 5;
        }

        let rows_top    = lane_y + 10;
        let rows_bottom = h - 200;
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

        window.update_with_buffer(&buf, w, h).unwrap();

        if !screenshot_taken {
            save_screenshot(&buf, w, h, sim_tag, "channel");
            screenshot_taken = true;
        }
    }
}


fn is_uplink(dest_id: i32) -> bool {
    dest_id > PREFIX_ID_DOWNLINK && dest_id < PREFIX_ID_UPLINK   // AP is always STA-id 0; adjust if your topology differs
}

fn render_controls_bar(buf: &mut [u32], stride: usize, state: &UnifiedState) {
    // Background + bottom border
    fill_rect(buf, stride, 0, HUD_H, stride, CONTROLS_H, 0x161620);
    for x in 0..stride {
        let i = (HUD_H + CONTROLS_H - 1) * stride + x;
        if i < buf.len() { buf[i] = 0x2a2a40; }
    }

    // ── Row 1: Play/Pause  •  Reset  •  Speed readout ────────────────────────
    let draw_btn = |buf: &mut [u32], bx: usize, label: &str, bg: u32, border: u32| {
        fill_rect(buf, stride, bx, BTN_Y, BTN_PLAY_W, BTN_H, bg);
        abr_draw_hline(buf, stride, bx as i32, (bx + BTN_PLAY_W) as i32, BTN_Y as i32, border);
        abr_draw_hline(buf, stride, bx as i32, (bx + BTN_PLAY_W) as i32, (BTN_Y + BTN_H) as i32, border);
        abr_draw_vline(buf, stride, bx as i32, BTN_Y as i32, (BTN_Y + BTN_H) as i32, border);
        abr_draw_vline(buf, stride, (bx + BTN_PLAY_W) as i32, BTN_Y as i32, (BTN_Y + BTN_H) as i32, border);
    };

    draw_btn(buf, BTN_PLAY_X,
        if state.playing { " PAUSE" } else { "  PLAY" },
        if state.playing { 0x1a3a1a } else { 0x1a1a3a },
        0x4488aa);
    render_text(buf, if state.playing { " PAUSE" } else { "  PLAY" },
                BTN_PLAY_X + 6, BTN_Y + 4, stride, 0xddeeff, 1.4);

    draw_btn(buf, BTN_RESET_X, " RESET", 0x2a1a1a, 0xaa4444);
    render_text(buf, " RESET", BTN_RESET_X + 6, BTN_Y + 4, stride, 0xffcccc, 1.4);

    // Speed readout sits well to the right of the buttons, on row-1 — no slider on this row
    let speed_str = format!("Speed: {}", fmt_speed(state.playback_speed));
    render_text(buf, &speed_str,
                BTN_RESET_X + BTN_RESET_W + 18, BTN_Y + 4,
                stride, 0xaabbcc, 1.4);

    // ── Row 2: range label • slider • range label ─────────────────────────────
    // End slider before the HUD zoom+reset button zone on the right
    let slider_x1 = stride
        .saturating_sub(RESET_BTN_W + ZOOM_BTN_W + HUD_BTN_PAD * 2 + 24)
        .max(SLIDER_X0 + 40);

    if slider_x1 > SLIDER_X0 + 20 {
        let slider_w = slider_x1 - SLIDER_X0;
        let track_y  = SLIDER_ROW_Y + SLIDER_ROW_H / 2 - 2;
        let frac     = speed_to_frac(state.playback_speed);

        // Track
        fill_rect(buf, stride, SLIDER_X0, track_y, slider_w, 4, 0x252535);
        abr_draw_hline(buf, stride, SLIDER_X0 as i32, slider_x1 as i32,
                        track_y as i32, 0x334455);

        // Filled portion
        let filled_w = ((frac * slider_w as f64) as usize).min(slider_w);
        if filled_w > 0 {
            fill_rect(buf, stride, SLIDER_X0, track_y, filled_w, 4, 0x3355aa);
        }

        // Thumb
        let thumb_x = (SLIDER_X0 + (frac * slider_w as f64) as usize)
            .clamp(SLIDER_X0, slider_x1);
        fill_rect(buf, stride,
            thumb_x.saturating_sub(5), SLIDER_ROW_Y + 2,
            11, SLIDER_ROW_H - 4, 0x7799ff);
        abr_draw_vline(buf, stride, thumb_x as i32,
            SLIDER_ROW_Y as i32, (SLIDER_ROW_Y + SLIDER_ROW_H) as i32, 0xaabbff);

        // Range labels — flush against slider ends, on the same row
         render_text(buf, &fmt_speed(SPEED_MIN),
                    SLIDER_X0.saturating_sub(42), SLIDER_ROW_Y + 1,
                    stride, 0x778899, 1.5);
        render_text(buf, &fmt_speed(SPEED_MAX),
                    slider_x1 + 6, SLIDER_ROW_Y + 1,
                    stride, 0x778899, 1.5);
    }
}


// ----------------------------------------------------------------
// render_link_lane  – dims or highlights each TXOP / collision bar
// ----------------------------------------------------------------
fn render_link_lane(
    buf: &mut [u32], stride: usize,
    lane_y: usize, lane_h: usize,
    panel_x: usize, panel_w: usize,
    view: &ViewState, idx: &VizIndex, link_id: u8,
    bandwidth_mhz: u16, 
    highlight: &Option<ActiveHighlights>,
) {
    fill_rect(buf, stride, panel_x, lane_y, panel_w, lane_h, 0x14141c);
 
    let t_lo = view.center_t - view.span_t * 0.5;
    let t_hi = view.center_t + view.span_t * 0.5;
 
    if let Some(indices) = idx.txops_by_link.get(&link_id) {
        let start = indices.partition_point(|&i| event_end(&idx.all[i]) < t_lo);
        for &ii in &indices[start..] {
            let ev = &idx.all[ii];
            let (t, end, owner, dest_id, ampdu_packets, mcs, alvr_stream_ids, frame_id, frame_losses) = match ev {
                VizEvent::TxopStart {
                    t, 
                    end, 
                    owner, 
                    dest_id, 
                    ampdu_packets,
                    mcs, 
                    alvr_stream_ids, 
                    alvr_frame_ids, 
                    alvr_frame_losses, 
                    .. 
                    } => (*t, *end, *owner, *dest_id, *ampdu_packets, *mcs, alvr_stream_ids.clone(), alvr_frame_ids.clone(), alvr_frame_losses.clone()),
                    _ => continue,
            };
            if t > t_hi { break; }
 
            let x0 = x_of(t,   view, panel_x, panel_w).max(panel_x as i32);
            let x1 = x_of(end, view, panel_x, panel_w).min((panel_x + panel_w) as i32);
            if x1 <= x0 { continue; }
 
            let lit     = is_key_highlighted(&owner, highlight);
            // 1. Determine the base color: Bright Red if it's a frame loss, otherwise AC color
            
            let base_c = if alvr_stream_ids.contains(&FRAMELOSS_PACKET) {
                0xff0000 // Bright red — frame loss, always distinct
            } else {
                match owner.1 {
                    // When every frame is BestEffort, split uplink/downlink so lanes
                    // are readable at a glance.  Other ACs keep their own palette.
                    EdcaAc::BestEffort => {
                        if is_uplink(dest_id) {
                            0xffb74d   //  warm amber  → uplink   (STA → AP)
                        } else {
                               //cool blue → downlink (AP → STA)
                            0x4fc3f7
                        }
                    }
                    _ => ac_color(owner.1),
                }
            };
            // 2. Apply dimming if the stream isn't currently highlighted
            // let color = if lit { base_c } else { dim_color(base_c, 5) };
            let color = match (lit, highlight.is_some()) {
                (true,  true)  => brighten_color(base_c),  // actively highlighted → extra bright
                (true,  false) => base_c,                  // nothing hovered → normal
                (false, _)     => dim_color(base_c, 5),    // not in highlight set → dimmed
            };
            // 3. Render the rectangle (already uses 'color')
            fill_rect(buf, stride, x0 as usize, lane_y + 2,
                    (x1 - x0) as usize, lane_h - 4, color);
 
            if lit && highlight.is_some() {
                draw_hline(buf, stride, x0, x1, (lane_y + 2) as i32,          0xffffff);
                draw_hline(buf, stride, x0, x1, (lane_y + lane_h - 3) as i32, 0xffffff);
                draw_vline(buf, stride, x0, (lane_y + 2) as i32,
                           (lane_y + lane_h - 3) as i32, 0xffffff);
                draw_vline(buf, stride, x1 - 1, (lane_y + 2) as i32,
                           (lane_y + lane_h - 3) as i32, 0xffffff);
            }
 
            let width = x1 - x0;
            if width > 80 && lit {
                let label1 = format!("{:?} STA{}->{} MCS{}", owner.1, owner.0, dest_id, mcs);
                render_text(buf, &label1, x0 as usize + 4, lane_y + 6, stride, 0x000000, 1);
                if width > 100 {

                    let stream_names = alvr_stream_ids
                        .iter()
                        .map(|&id| crate::lib::alvr_stream_socket::get_stream_name(id))
                        .collect::<Vec<_>>()
                        .join("+"); // You can use ", " or "|" depending on your UI preference
                    let label2 = if !alvr_stream_ids.contains(&FRAMELOSS_PACKET) {
                        format!(
                            "{}|frames: {:?} ({} MPDUs)",
                            stream_names, 
                            frame_id, ampdu_packets
                        )
                    } else {
                        format!(
                            "{}|frames: {:?} ({} MPDUs)",
                            stream_names,
                            frame_losses.clone().unwrap(), ampdu_packets
                        )
                    };
                    render_text(buf, &label2, x0 as usize + 4, lane_y + 18, stride, 0x000000, 1);
                }
            }
        }
    }
    
    // Collisions (Adjusted for same height)
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
                let row = yy * stride;
                for xx in (x0 as usize)..(x1 as usize).min(stride) {
                    buf[row + xx] = c;
                }
            }
        }
    }
    let link_label = format!("LINK {} ({}MHz)", link_id, bandwidth_mhz);
    render_text(
        buf, 
        &link_label, 
        8, 
        lane_y + lane_h / 2 - 8, 
        stride, 
        0xeeeeee, 
        2
    );

    render_text(buf, &link_label, 8, lane_y + lane_h / 2 - 8, stride, 0xeeeeee, 2);

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
fn render_qdepth_panel(
    buf: &mut [u32], stride: usize,
    panel_y: usize, panel_h: usize,
    panel_x: usize, panel_w: usize,
    view: &ViewState, idx: &VizIndex,
    highlight: &Option<ActiveHighlights>,
) {
    fill_rect(buf, stride, panel_x, panel_y, panel_w, panel_h, 0x10101a);
    fill_rect(buf, stride, 0, panel_y, panel_x, panel_h, 0x14141c);
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
        let is_dl = sta_id == -1;
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
        let is_dl = sta_id == -1;
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
                format!("AP   {}", ac.to_string())
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


pub struct VizIndex {
    pub all: Vec<VizEvent>,
    pub txops_by_link: HashMap<u8, Vec<usize>>,
    pub collisions_by_link: HashMap<u8, Vec<usize>>,
    pub backoff_by_key: HashMap<MacKey, Vec<usize>>,
    pub qdepth_by_key: HashMap<MacKey, Vec<usize>>,
    pub bandwidth_by_ip: HashMap<IpAddr, Vec<usize>>,
    pub mac_keys_sorted: Vec<MacKey>,
    pub t_min: f64,
    pub t_max: f64,
    pub qdepth_series: Vec<((i32, EdcaAc), Vec<(f64, usize)>)>,
    pub cw_series: HashMap<MacKey, Vec<(f64, u32, bool)>>,
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

        for (i, ev) in events.iter().enumerate() {
            match ev {
                VizEvent::TxopStart { link_id, owner, .. } => {
                    txops_by_link.entry(*link_id).or_default().push(i);
                    keys.insert(*owner);
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
        mac_keys_sorted.sort_by_key(|k| (k.2, k.0 != -1, k.0, ac_prio(k.1)));

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

        // ── Assemble — events is moved here once, everything else is ready ─
        VizIndex {
            t_min,
            t_max,
            all: events,
            txops_by_link,
            collisions_by_link,
            backoff_by_key,
            qdepth_by_key,
            bandwidth_by_ip,
            mac_keys_sorted,
            qdepth_series,
            cw_series,
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
    let (mx, my) = window.get_mouse_pos(MouseMode::Discard).unwrap_or((0.0, 0.0));
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

    // ── Row-2 slider drag ─────────────────────────────────────────────────────
    let slider_x1 = stride
        .saturating_sub(RESET_BTN_W + ZOOM_BTN_W + HUD_BTN_PAD * 2 + 24)
        .max(SLIDER_X0 + 40);

    let in_slider_row = (my as usize) >= SLIDER_ROW_Y
                     && (my as usize) <  SLIDER_ROW_Y + SLIDER_ROW_H;
    let over_slider   = in_bar && in_slider_row
                     && (mx as usize) >= SLIDER_X0
                     && (mx as usize) <= slider_x1;

    if left_down && (over_slider || state.slider_drag) {
        state.slider_drag = true;
        if slider_x1 > SLIDER_X0 {
            let frac = ((mx as f64) - SLIDER_X0 as f64)
                     / (slider_x1 - SLIDER_X0) as f64;
            state.playback_speed = frac_to_speed(frac);
        }
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
    if window.is_key_pressed(Key::Tab, minifb::KeyRepeat::No) {
        view.selected_link = match view.selected_link {
            None => Some(0),
            Some(l) if l + 1 < num_links => Some(l + 1),
            _ => None,
        };
    }
    let (mx, _my) = window.get_mouse_pos(MouseMode::Discard).unwrap_or((0.0, 0.0));
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
    let h_buf = buf.len() / stride;
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
const HUD_H:     usize = 50;     // shared top HUD strip
const TIME_AXIS: usize = 22;     // shared bottom time-axis strip
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


    /// Playback mode: when true, the view auto-advances in real-time (scaled by playback_speed).
    playing:        bool,
    playback_speed: f64,       // sim-seconds advanced per real-second
    last_frame:     Instant,
    slider_drag:    bool,
    prev_left_down: bool,      // for click edge detection on buttons
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
                center_t:      (viz_idx.t_min  + viz_idx.t_max)  * 0.5,
                span_t:        ch_full,
                cursor_t:      viz_idx.t_min,
                paused:        false,
                selected_link: None,
                row_scroll:    0,
                mouse_drag:    None,
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
            playing:        false,
            playback_speed: 1e-3,          // default: 1 ms/s
            last_frame:     Instant::now(),
            slider_drag:    false,
            prev_left_down: false,
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
fn render_unified_hud(
    buf:    &mut [u32],
    stride: usize,
    state:  &UnifiedState,
    ch_idx: &VizIndex,
    abr_idx: &AbrVizIndex,
) {
    use crate::lib::render_text;

    fill_rect(buf, stride, 0, 0, stride, HUD_H, 0x1a1a24);

    let ch  = &state.ch_view;
    let abr = &state.abr_view;

    let label = format!(
        "CH: t={:.4}s  span={:.3}ms    |    ABR: t={:.4}s  span={:.3}ms    |    \
         [S] sync-cursor:{}  [F] focus:{}  [ESC] quit",
        ch.center_t,
        ch.span_t * 1000.0,
        abr.center_t,
        abr.span_t * 1000.0,
        if state.sync_cursor { "ON " } else { "OFF" },
        if state.focus == Focus::Channel { "CH" } else { "ABR" },
    );
    render_text(buf, &label, 12, 14, stride, 0xeeeeee, 1.5);

    let hint = "L-Drag pan  R-Click cursor  Scroll zoom  Tab link-filter  \
                Click panel to focus  drag divider to resize";
    render_text(buf, hint, 12, 34, stride, 0x667788, 1.3);

    // ── Zoom button ───────────────────────────────────────────────────────────
    
   // ── Zoom button ───────────────────────────────────────────────────────────
    let reset_x = stride.saturating_sub(RESET_BTN_W + HUD_BTN_PAD);
    let zoom_x  = stride.saturating_sub(RESET_BTN_W + ZOOM_BTN_W + HUD_BTN_PAD * 2);

    if zoom_x > 10 {
        let zoom_bg     = if state.zoom_mode { 0x1a4d1a } else { 0x1e1e2e };
        let zoom_border = if state.zoom_mode { 0x44dd44 } else { 0x445566 };
        fill_rect(buf, stride, zoom_x, HUD_BTN_Y, ZOOM_BTN_W, HUD_BTN_H, zoom_bg);
        for x in zoom_x..zoom_x + ZOOM_BTN_W {
            let ti = HUD_BTN_Y * stride + x;
            let bi = (HUD_BTN_Y + HUD_BTN_H - 1) * stride + x;
            if ti < buf.len() { buf[ti] = zoom_border; }
            if bi < buf.len() { buf[bi] = zoom_border; }
        }
        for y in HUD_BTN_Y..HUD_BTN_Y + HUD_BTN_H {
            let li = y * stride + zoom_x;
            let ri = y * stride + zoom_x + ZOOM_BTN_W - 1;
            if li < buf.len() { buf[li] = zoom_border; }
            if ri < buf.len() { buf[ri] = zoom_border; }
        }
        let zoom_text_col = if state.zoom_mode { 0x88ff88 } else { 0x8899aa };
        draw_magnifier(buf, stride, (zoom_x + 16) as i32,
                    (HUD_BTN_Y + HUD_BTN_H / 2) as i32, zoom_text_col);
        let text_x = (zoom_x + 30).min(zoom_x + ZOOM_BTN_W.saturating_sub(4));
        render_text(buf, if state.zoom_mode { "ZOOM ON" } else { "ZOOM" },
                    text_x, HUD_BTN_Y + 11, stride, zoom_text_col, 1.4);

        // Reset button
        fill_rect(buf, stride, reset_x, HUD_BTN_Y, RESET_BTN_W, HUD_BTN_H, 0x1e1e2e);
        let reset_border = 0x554433;
        for x in reset_x..reset_x + RESET_BTN_W {
            let ti = HUD_BTN_Y * stride + x;
            let bi = (HUD_BTN_Y + HUD_BTN_H - 1) * stride + x;
            if ti < buf.len() { buf[ti] = reset_border; }
            if bi < buf.len() { buf[bi] = reset_border; }
        }
        for y in HUD_BTN_Y..HUD_BTN_Y + HUD_BTN_H {
            let li = y * stride + reset_x;
            let ri = y * stride + reset_x + RESET_BTN_W - 1;
            if li < buf.len() { buf[li] = reset_border; }
            if ri < buf.len() { buf[ri] = reset_border; }
        }
        let reset_text_x = (reset_x + 14).min(reset_x + RESET_BTN_W.saturating_sub(4));
        render_text(buf, "RESET", reset_text_x, HUD_BTN_Y + 11, stride, 0xffcc88, 1.4);
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
    let y0 = (drag.y0.min(drag.y1) as usize).max(HUD_H);
    let y1 = (drag.y0.max(drag.y1) as usize).min(h - TIME_AXIS);

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
    if step > 0.0 {
        let mut t = (t0 / step).ceil() * step;
        while t <= t1 {
            let x = x_of(t, ch, ch_px, ch_pw);
            draw_vline(buf, stride, x, y as i32, (y + 5) as i32, 0x555566);
            render_text(buf, &format!("{:.2}s", t), x as usize + 2, y + 4, stride, 0x778899, 1.3);
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
    if step_a > 0.0 {
        let mut t = (t0a / step_a).ceil() * step_a;
        while t <= t1a {
            let x = abr_x_of(t, abr, abr_px, abr_pw);
            if x >= abr_px as i32 && x < (abr_px + abr_pw) as i32 {
                abr_draw_vline(buf, stride, x, y as i32, (y + 5) as i32, 0x554455);
                render_text(buf, &format!("{:.2}s", t), x as usize + 2, y + 4, stride, 0x997788, 1.3);
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
    splitter_x:  usize,
    num_links:   u8,
) {
    let (mx, my) = window.get_mouse_pos(MouseMode::Discard).unwrap_or((0.0, 0.0));
    let mx_u     = mx as usize;

    // ── Global keys ──────────────────────────────────────────────────────────

    if window.is_key_pressed(Key::S, minifb::KeyRepeat::No) {
        state.sync_cursor = !state.sync_cursor;
    }

    // Click inside a panel transfers keyboard focus to it (and lets scroll work)
    if window.get_mouse_down(MouseButton::Left) {
        if mx_u < splitter_x                         { state.focus = Focus::Channel; }
        else if mx_u >= splitter_x + SPLITTER_W      { state.focus = Focus::Abr;     }
    }

    // ── Splitter drag ─────────────────────────────────────────────────────────

    let in_splitter = mx_u >= splitter_x && mx_u < splitter_x + SPLITTER_W;

    if window.get_mouse_down(MouseButton::Left) && (in_splitter || state.splitter_drag.is_some()) {
        if let Some(ref drag) = state.splitter_drag {
            let dx = (mx - drag.start_mx) as f32;
            state.split_frac = (drag.start_split + dx / w as f32).clamp(0.20, 0.80);
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

    let reset_x = w.saturating_sub(RESET_BTN_W + HUD_BTN_PAD);
    let zoom_x  = w.saturating_sub(RESET_BTN_W + ZOOM_BTN_W + HUD_BTN_PAD * 2);

    // ── Button clicks — use `clicked`, not `left_down` ───────────────────────
    if clicked
        && (my as usize) >= HUD_BTN_Y
        && (my as usize) < HUD_BTN_Y + HUD_BTN_H
    {
        let mx_u = mx as usize;
        if mx_u >= zoom_x && mx_u < zoom_x + ZOOM_BTN_W {
            state.zoom_mode = !state.zoom_mode;
            state.zoom_drag = None;
            return;
        }
        if mx_u >= reset_x && mx_u < reset_x + RESET_BTN_W {
            let ch_full  = ch_idx.t_max  - ch_idx.t_min;
            let abr_full = abr_idx.t_max - abr_idx.t_min;
            state.ch_view.center_t  = (ch_idx.t_min  + ch_idx.t_max)  * 0.5;
            state.ch_view.span_t    = ch_full;
            state.abr_view.center_t = (abr_idx.t_min + abr_idx.t_max) * 0.5;
            state.abr_view.span_t   = abr_full;
            return;
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
    // ── Zoom-box drag (overrides normal left-drag when zoom_mode active) ───────
    if state.zoom_mode {
        if window.get_mouse_down(MouseButton::Left) {
            if let Some(ref mut drag) = state.zoom_drag {
                drag.x1 = mx;
                drag.y1 = my;
            } else {
                // Determine which panel the drag started in
                let panel = if (mx as usize) < splitter_x { Focus::Channel } else { Focus::Abr };
                state.zoom_drag = Some(ZoomDrag { x0: mx, y0: my, x1: mx, y1: my, panel });
            }
        } else if let Some(drag) = state.zoom_drag.take() {
            // Mouse released — apply zoom to the relevant panel
            let t_from_x = |x: f32, px: usize, pw: usize, v_center: f64, v_span: f64| -> f64 {
                let n = (x as f64 - px as f64) / pw as f64;
                v_center - v_span * 0.5 + v_span * n
            };

            let x_lo = drag.x0.min(drag.x1);
            let x_hi = drag.x0.max(drag.x1);
            if (x_hi - x_lo) > 4.0 {   // ignore tiny accidental clicks
                match drag.panel {
                    Focus::Channel => {
                        let t0 = t_from_x(x_lo, ch_panel_x, ch_panel_w,
                                          state.ch_view.center_t, state.ch_view.span_t);
                        let t1 = t_from_x(x_hi, ch_panel_x, ch_panel_w,
                                          state.ch_view.center_t, state.ch_view.span_t);
                        state.ch_view.span_t    = (t1 - t0).max(1e-4);
                        state.ch_view.center_t  = (t0 + t1) * 0.5;
                        if state.sync_cursor {
                            state.abr_view.span_t   = state.ch_view.span_t;
                            state.abr_view.center_t = state.ch_view.center_t;
                        }
                    }
                    Focus::Abr => {
                        let t0 = t_from_x(x_lo, abr_panel_x, abr_panel_w,
                                          state.abr_view.center_t, state.abr_view.span_t);
                        let t1 = t_from_x(x_hi, abr_panel_x, abr_panel_w,
                                          state.abr_view.center_t, state.abr_view.span_t);
                        state.abr_view.span_t   = (t1 - t0).max(1e-4);
                        state.abr_view.center_t = (t0 + t1) * 0.5;
                        if state.sync_cursor {
                            state.ch_view.span_t   = state.abr_view.span_t;
                            state.ch_view.center_t = state.abr_view.center_t;
                        }
                    }
                }
            }
            state.zoom_mode = false;   // auto-exit zoom mode after selection
        }
        return;   // don't run normal pan/scroll while zoom_mode is active
    }

    // ── Route scroll / right-click to the panel under the mouse ──────────────

    let mouse_in_ch  = mx_u >= ch_panel_x  && mx_u < ch_panel_x  + ch_panel_w;
    let mouse_in_abr = mx_u >= abr_panel_x && mx_u < abr_panel_x + abr_panel_w;
    
    // Shared right-click cursor
    if window.get_mouse_down(MouseButton::Right) {
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

    // Scroll zoom — route by mouse position
    if let Some((_, scroll_y)) = window.get_scroll_wheel() {
        if scroll_y.abs() > 0.0 {
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

    if window.get_mouse_down(MouseButton::Left) {
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
            // Global: Space always toggles playback regardless of focus
            if window.is_key_pressed(Key::Space, minifb::KeyRepeat::No) {
                state.playing    = !state.playing;
                state.last_frame = Instant::now();
            }
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
    v.span_t = v.span_t.clamp(1e-3, full);
    let lo = idx.t_min + v.span_t * 0.5;
    let hi = idx.t_max - v.span_t * 0.5;
    // Guard: FP rounding can make lo > hi when span_t ≈ full
    v.center_t = if lo <= hi {
        v.center_t.clamp(lo, hi)
    } else {
        (idx.t_min + idx.t_max) * 0.5
    };
}


fn draw_ap_icon(buf: &mut [u32], stride: usize,
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
    let mut window = Window::new(
        "WLAN Sim + ABR Viewer",
        WIN_W, WIN_H,
        WindowOptions {
            resize:     true,
            scale_mode: minifb::ScaleMode::Stretch,
            ..WindowOptions::default()
        },
    ).expect("Failed to open unified viewer");

    let mut buf   = vec![0u32; WIN_W * WIN_H];
    let mut state = UnifiedState::new(&viz_idx, &abr_idx);

    // Pre-build ABR strip descriptors (doesn't change frame-to-frame)
    let abr_strips  = make_strips(&abr_idx);
    let n_abr       = abr_strips.len();
    let num_links   = link_configs.len() as u8;

    let mut screenshot_taken = false;

    while window.is_open() && !window.is_key_down(Key::Escape) {

        // ── Live dimensions ───────────────────────────────────────────────────
        let (w, h) = window.get_size();
        if buf.len() != w * h { buf.resize(w * h, 0); }

        // ── Derive panel geometry from split_frac ─────────────────────────────
        //
        // Total horizontal space after HUD/time-axis is handled vertically.
        // Left (channel) panel content area:
        //   x: CH_SIDEBAR_W .. split_px
        // Right (ABR) panel content area:
        //   x: split_px + SPLITTER_W + ABR_SIDEBAR_W .. w
        //
        // We keep the sidebars at the far edges; the splitter is between them.

        let split_px       = ((w as f32 * state.split_frac) as usize).clamp(200, w.saturating_sub(400));
        let splitter_x     = split_px;                                    // left edge of splitter bar

        // Channel viewer occupies [0 .. split_px]
        const CH_SIDEBAR_W: usize = 240;
        let ch_panel_x = CH_SIDEBAR_W;
        let ch_panel_w = split_px.saturating_sub(CH_SIDEBAR_W + 4);

        // ABR viewer occupies [split_px + SPLITTER_W .. w]
        const ABR_SIDEBAR_W: usize = 280;
        let abr_left   = split_px + SPLITTER_W;
        let abr_panel_x = abr_left + ABR_SIDEBAR_W;
        let abr_panel_w = w.saturating_sub(abr_left + ABR_SIDEBAR_W + 4);

        // let content_top    = HUD_H;
        let content_top = HUD_H + CONTROLS_H;
        let content_bottom = h.saturating_sub(TIME_AXIS);
        let content_h      = content_bottom - content_top;

        // ABR strips vertical layout
        const PANEL_PAD: usize = 10;
        let abr_usable_h = content_h.saturating_sub(PANEL_PAD);
        let abr_strip_h  = (abr_usable_h / n_abr).max(1);

        // ── Input ─────────────────────────────────────────────────────────────
        handle_unified_input(
            &window, &mut state,
            &viz_idx, &abr_idx,
            w, h,
            ch_panel_x, ch_panel_w,
            abr_panel_x, abr_panel_w,
            splitter_x,
            num_links,
        );
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

                
        // ── 1. Channel panel (left half) ─────────────────────────────────────────────
        {
            // Layout values that don't depend on lane_y
            let rows_bottom = content_bottom.saturating_sub(200 + CW_H + 12);
            let cw_panel_y  = rows_bottom + 4;

            // Highlight — computed once, used by every render call below
            let ch_highlight = {
                let (mx, my) = window.get_mouse_pos(MouseMode::Discard).unwrap_or((0.0, 0.0));
                hit_test_qdepth_legend(
                    mx, my, content_bottom - 180, 160, ch_panel_x, &state.ch_view, &viz_idx,
                )
                .or_else(|| hit_test_cw_legend(
                    mx, my, cw_panel_y, CW_H, ch_panel_x, &state.ch_view, &viz_idx,
                ))
            };

            // Lane loop — lane_y advances here
            let mut lane_y = content_top + 10;
            let lane_h     = 50usize;
            for (link_id_usize, link_cfg) in link_configs.iter().enumerate() {
                render_link_lane(
                    &mut buf, w, lane_y, lane_h,
                    ch_panel_x, ch_panel_w,
                    &state.ch_view, &viz_idx,
                    link_id_usize as u8, link_cfg.bandwidth_mhz,
                    &ch_highlight,
                );
                lane_y += lane_h + 5;
            }

            // rows_top is only valid AFTER the loop
            let rows_top = lane_y + 10;

            render_mackey_rows(
                &mut buf, w, rows_top, rows_bottom,
                ch_panel_x, ch_panel_w,
                &state.ch_view, &viz_idx,
                &ch_highlight,
            );
            render_cw_panel(
                &mut buf, w, cw_panel_y, CW_H,
                ch_panel_x, ch_panel_w,
                &state.ch_view, &viz_idx,
                &ch_highlight,
            );
            render_qdepth_panel(
                &mut buf, w, content_bottom - 180, 160,
                ch_panel_x, ch_panel_w,
                &state.ch_view, &viz_idx,
                &ch_highlight,
            );

            // Channel cursor line
            let cx = x_of(state.ch_view.cursor_t, &state.ch_view, ch_panel_x, ch_panel_w);
            if cx >= ch_panel_x as i32 && cx < (ch_panel_x + ch_panel_w) as i32 {
                draw_vline(&mut buf, w, cx, content_top as i32, content_bottom as i32, 0xffff66);
            }
        }

        // 2. ABR panel (right half)
        {
            let (mx, my) = window.get_mouse_pos(MouseMode::Discard).unwrap_or((0.0, 0.0));
            // Translate mouse x into sidebar-local coordinates for the right panel
            let abr_mx = mx - abr_left as f32;
            let highlight_ip = hit_test_abr_legend(abr_mx, my, &abr_idx);

            let minimap_y = content_bottom.saturating_sub(STA_MINIMAP_H);
            // render_abr_sidebar_clipped(&mut buf, w, h, abr_left, &abr_idx, &state.abr_view, highlight_ip, minimap_y);
            render_abr_sidebar(&mut buf, w, h, abr_left,
                   content_top + 4,          // ← 102 + 4 = 106
                   &abr_idx, &state.abr_view, highlight_ip, Some(minimap_y));
            for (si, strip) in abr_strips.iter().enumerate() {
                
                
                let strip_y = content_top + PANEL_PAD + si * abr_strip_h;
                render_abr_strip(&mut buf, w, strip, strip_y, abr_strip_h,
                                abr_panel_x, abr_panel_w, &state.abr_view, &abr_idx, highlight_ip,
                                Some(&viz_idx));
            }


            // ABR cursor line
            let cx = abr_x_of(state.abr_view.cursor_t, &state.abr_view, abr_panel_x, abr_panel_w);
            if cx >= abr_panel_x as i32 && cx < (abr_panel_x + abr_panel_w) as i32 {
                abr_draw_vline(&mut buf, w, cx, content_top as i32, content_bottom as i32, 0xffff66);
            }

            // ── STA minimap in lower sidebar ─────────────────────────────────────────
            if !abr_idx.by_ip_sta.is_empty() {
                let minimap_y = content_bottom.saturating_sub(STA_MINIMAP_H);
                render_sta_sidebar_minimap(
                    &mut buf, w,
                    abr_left,            // left edge of the full right panel
                    ABR_SIDEBAR_W,
                    minimap_y,
                    STA_MINIMAP_H,
                    &abr_idx,
                    state.abr_view.cursor_t,
                    AP_X as f32,
                    AP_Y as f32, 
                    highlight_ip,  
                );
            }
        }

        if let Some(ref drag) = state.zoom_drag {
            render_zoom_overlay(&mut buf, w, drag, h);
        }

        // 3. Splitter bar
        render_splitter(&mut buf, w, h, splitter_x);
        // 4. Shared HUD (draws over both halves)
        render_unified_hud(&mut buf, w, &state, &viz_idx, &abr_idx);
        // 5. Shared time axis
        render_controls_bar(&mut buf, w, &state);  
        render_unified_time_axis(
            &mut buf, w, w, h,
            &state,
            ch_panel_x,  ch_panel_w,
            abr_panel_x, abr_panel_w,
        );

        // 6. Focus indicator — thin coloured border on the active panel
        let focus_color = 0x334488_u32;
        let (fx, fw) = if state.focus == Focus::Channel {
            (0usize, split_px)
        } else {
            (split_px + SPLITTER_W, w.saturating_sub(split_px + SPLITTER_W))
        };
        for x in fx..(fx + fw).min(w) {
            let top_i = HUD_H * w + x;
            let bot_i = (content_bottom - 1) * w + x;
            if top_i < buf.len() { buf[top_i] = focus_color; }
            if bot_i < buf.len() { buf[bot_i] = focus_color; }
        }

        window.update_with_buffer(&buf, w, h).unwrap();

        if !screenshot_taken {
            save_screenshot(&buf, w, h, &sim_tag, "unified");
            screenshot_taken = true;
        }
    }
}

// ── Helper: render ABR sidebar offset to the right half ──────────────────────
// The original render_abr_sidebar draws at x=0; we need it offset to `x_off`.
// Rather than refactoring the original, we render into a small temp buffer and
// blit it across.  This keeps both viewers' internals unchanged.


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
        if sta_id == -1 {
            ac_color(ac)
        } else {
            [0xFF4444, 0xFF8822, 0xFFCC33, 0xFF55AA,
             0x44AAFF, 0x44FF88, 0x8844FF, 0x44FFEE]
                [(sta_id.unsigned_abs() as usize) % 8]
        }
    };

    // dash pattern: AP → solid, STA → dashed (same as qdepth)
    let pattern_for = |(sta_id, _, _): MacKey| -> usize {
        if sta_id == -1 { 0 } else { 2 }
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
            let lbl = if key.0 == -1 {
                format!("AP   {:?} L{}", key.1, key.2)
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


