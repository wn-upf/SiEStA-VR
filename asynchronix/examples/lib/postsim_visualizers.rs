use std::collections::HashMap;
use std::net::IpAddr;
use minifb::{Key, MouseButton, MouseMode, Window, WindowOptions};
use crate::lib::models_XR::AbrEvent;
use crate::lib::{render_text, ac_prio,} ;
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

        // ── STA spatial extents ──────────────────────────────────────────────
        let (mut sta_x_min, mut sta_x_max) = (f32::MAX, f32::MIN);
        let (mut sta_y_min, mut sta_y_max) = (f32::MAX, f32::MIN);

        for (i, ev) in events.iter().enumerate() {
            match ev {
                AbrEvent::FrameMetrics { ip_server, peak_throughput_mbps, rtt_ms, .. } => {
                    max_rtt = max_rtt.max(*rtt_ms);
                    max_tp  = max_tp .max(*peak_throughput_mbps);
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
            ceil:     idx.ceil_bitrate_mbps,
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
fn abr_fill_rect(buf: &mut [u32], stride: usize,
                 x: usize, y: usize, w: usize, h: usize, color: u32) {
    for row in y..(y + h) {
        for col in x..(x + w) {
            let i = row * stride + col;
            if i < buf.len() { buf[i] = color; }
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
        let trail = dim_color(base, 4);

        let Some(indices) = idx.by_ip_sta.get(ip) else { continue };

        // binary-search to find how many samples are ≤ cursor_t
        let end = indices.partition_point(|&i| abr_event_t(&idx.events[i]) <= cursor_t);
        if end == 0 { continue; }

        let mut prev_px: Option<(i32, i32)> = None;

        for &ei in &indices[..end] {
            if let AbrEvent::StaLocation { x, z, .. } = &idx.events[ei] {
                let (px, py) = to_px(*x, *z);
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
) {
    let t_lo = view.center_t - view.span_t * 0.5;
    let t_hi = view.center_t + view.span_t * 0.5;

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
    let ceil = if visible_max > 0.0 {
        (visible_max * 1.15).max(1e-3)
    } else {
        strip.ceil          // global fallback when no data in view
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
   
    abr_draw_hline(buf, stride,
        panel_x as i32, (panel_x + panel_w) as i32,
        (strip_y + strip_h - 1) as i32, 0x2a2a3a);
}

// ─────────────────────────────────────────────────────────────────────────────
// Sidebar: legend + cursor readout
// ─────────────────────────────────────────────────────────────────────────────
fn render_abr_sidebar(
    buf:    &mut [u32],
    stride: usize,
    height: usize,
    idx:    &AbrVizIndex,
    view:   &AbrViewState,
    highlight_ip: Option<usize>, 
    max_y: Option<usize>, 
) {
    abr_fill_rect(buf, stride, 0, 0, SIDEBAR_W, height, 0x14141c);
    render_text(buf, "ABR METRICS", 8, 8, stride, 0xffffff, TEXT_SIZE_ABR * 2.0); // f32 * f32

    let mut ly = 52usize;
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
        abr_fill_rect(buf, stride, 8, ly, 18, 10, color);
        render_text(buf, &format!("{}", ip), 30, ly,      stride, text_col,  TEXT_SIZE_ABR);
        render_text(buf, mode,               30, ly + 20, stride, 0x888899, TEXT_SIZE_ABR);
        
        if let Some(limit) = max_y{
            if ly + 44 > limit { break; }
        }

        ly += 44;
    }

    ly += 18;
    render_text(buf, &format!("t = {:.3}s", view.cursor_t), 8, ly, stride, 0xaaaaaa, TEXT_SIZE_ABR);
    ly += 22;

    for (ip_idx, ip) in idx.ip_order.iter().enumerate() {
        let color = user_color(ip_idx);

        // ── latest FrameMetrics at cursor ────────────────────────────────
        let (rtt_ms, flr, tp_mbps) = idx.by_ip_frame.get(ip)
            .and_then(|indices| {
                let pos = indices.partition_point(|&i| abr_event_t(&idx.events[i]) <= view.cursor_t);
                if pos == 0 { return None; }
                match &idx.events[indices[pos - 1]] {
                    AbrEvent::FrameMetrics { rtt_ms, flr, peak_throughput_mbps, .. } =>
                        Some((*rtt_ms, *flr, *peak_throughput_mbps)), // * needed: matching on &AbrEvent
                    _ => None,
                }
            })
            .unwrap_or((0.0, 0.0, 0.0));

        // ── latest BitrateUpdate at cursor ───────────────────────────────
        let bitrate_mbps = idx.by_ip_bitrate.get(ip)
            .and_then(|indices| {
                let pos = indices.partition_point(|&i| abr_event_t(&idx.events[i]) <= view.cursor_t);
                if pos == 0 { return None; }
                match &idx.events[indices[pos - 1]] {
                    AbrEvent::BitrateUpdate { new_bitrate_mbps, .. } => Some(*new_bitrate_mbps),
                    _ => None,
                }
            })
            .unwrap_or(0.0);

        let line = format!(
            "{:.0}M  RTT{:.0}ms  FLR{:.2}  TP{:.0}M",
            bitrate_mbps, rtt_ms, flr, tp_mbps,
        );
        render_text(buf, &line, 8, ly, stride, color, 1.4);
        ly += 22;
    }
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

pub fn run_abr_viewer(idx: AbrVizIndex) {
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

    while window.is_open() && !window.is_key_down(Key::Escape) {
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
                panel_x, panel_w, &view, &idx, highlight_ip,
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

        render_abr_sidebar(&mut buf, w, h, &idx, &view, highlight_ip, None );

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
pub fn run_viewer(idx: VizIndex, link_configs: &[LinkConfig]) {
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
    }
}


fn is_uplink(dest_id: i32) -> bool {
    dest_id > PREFIX_ID_DOWNLINK && dest_id < PREFIX_ID_UPLINK   // AP is always STA-id 0; adjust if your topology differs
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
fn build_qdepth_series(
    view: &ViewState,
    idx: &VizIndex,
) -> Vec<((i32, EdcaAc), Vec<(f64, usize)>)> {
    let mut per_link_map: HashMap<(i32, EdcaAc, u8), Vec<usize>> = HashMap::new();
    for (key, indices) in &idx.qdepth_by_key {
        for &ii in indices {
            if let VizEvent::QueueDepth { .. } = &idx.all[ii] {
                per_link_map.entry((key.0, key.1, key.2)).or_default().push(ii);
            }
        }
    }
    for v in per_link_map.values_mut() {
        v.sort_by(|&a, &b| event_t(&idx.all[a]).partial_cmp(&event_t(&idx.all[b])).unwrap());
    }

    let flow_keys: HashSet<(i32, EdcaAc)> = per_link_map
        .keys()
        .map(|&(s, a, _)| (s, a))
        .collect();

    let mut agg_map: HashMap<(i32, EdcaAc), Vec<(f64, usize)>> = HashMap::new();
    for (sta_id, ac) in &flow_keys {
        let mut all_events: Vec<(f64, u8, usize)> = per_link_map
            .iter()
            .filter(|(&(s, a, _), _)| s == *sta_id && a == *ac)
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
        let series = all_events
            .into_iter()
            .map(|(t, lid, depth)| {
                link_depths.insert(lid, depth);
                (t, link_depths.values().sum::<usize>())
            })
            .collect();
        agg_map.insert((*sta_id, *ac), series);
    }

    let mut keys: Vec<_> = agg_map.keys().copied().collect();
    keys.sort_by_key(|k| (ac_prio(k.1), k.0));
    keys.reverse();
    keys.into_iter().map(|k| (k, agg_map.remove(&k).unwrap())).collect()
}

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

    for ((sta_id, ac), series) in &series_list {
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

// Add this helper alongside your other color utilities:
#[inline]
fn blend_screen(dst: u32, src: u32) -> u32 {
    // Screen blend: result = 1 - (1-dst)(1-src)  — never clips to white for typical colors
    let ch = |d: u32, s: u32| -> u32 {
        let df = d as f32 / 255.0;
        let sf = s as f32 / 255.0;
        ((1.0 - (1.0 - df) * (1.0 - sf)) * 255.0) as u32
    };
    let r = ch((dst >> 16) & 0xFF, (src >> 16) & 0xFF);
    let g = ch((dst >>  8) & 0xFF, (src >>  8) & 0xFF);
    let b = ch( dst        & 0xFF,  src        & 0xFF);
    (r << 16) | (g << 8) | b
}
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
                            if should_draw_pixel(fill_x, pattern_type) {
                                // 2px outline instead of 3 — less mud at overlaps
                                for ty in 0..2usize {
                                    let oi = (py as usize + ty) * stride + (fill_x as usize);
                                    if oi < buf.len() { 
                                        buf[oi] = blend_screen(buf[oi], outline_color);
                                    }
                                }
                            }
                        }
                    }
                }
                if (prev_depth > 0 || depth > 0) && x >= panel_x as i32 && x < (panel_x + panel_w) as i32 {
                    draw_vline(buf, stride, x, py, y, outline_color);
                }
            }
            prev = Some((x, y, depth));
        }

        // Extend outline to right edge
        if let Some((px, py, prev_depth)) = prev {
            if prev_depth > 0 {
                let end_x = (panel_x + panel_w) as i32;
                for fill_x in px..end_x {
                    if fill_x >= panel_x as i32 && fill_x < end_x {
                        if should_draw_pixel(fill_x, pattern_type) {
                            for ty in 0..2usize {
                                let oi = (py as usize + ty) * stride + (fill_x as usize);
                                if oi < buf.len() { 
                                    buf[oi] = blend_screen(buf[oi], outline_color);
                                }
                            }
                        }
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
    pub mac_keys_sorted: Vec<MacKey>,
    pub t_min: f64,
    pub t_max: f64,
}
 
impl VizIndex {
    pub fn build(mut events: Vec<VizEvent>) -> Self {
        events.sort_by(|a, b| event_t(a).partial_cmp(&event_t(b)).unwrap());
        let mut idx = VizIndex {
            t_min: events.first().map(event_t).unwrap_or(0.0),
            t_max: events.last().map(|e| event_end(e)).unwrap_or(1.0),
            all: events,
            txops_by_link: HashMap::new(),
            collisions_by_link: HashMap::new(),
            backoff_by_key: HashMap::new(),
            qdepth_by_key: HashMap::new(),
            mac_keys_sorted: Vec::new(),
        };
        let mut keys = HashSet::new();
        for (i, ev) in idx.all.iter().enumerate() {
            match ev {
                VizEvent::TxopStart { link_id, owner, .. } => {
                    idx.txops_by_link.entry(*link_id).or_default().push(i);
                    keys.insert(*owner);
                }
                VizEvent::Collision { link_id, contenders, .. } => {
                    idx.collisions_by_link.entry(*link_id).or_default().push(i);
                    for k in contenders { keys.insert(*k); }
                }
                VizEvent::BackoffSnap { mac_key, .. } => {
                    idx.backoff_by_key.entry(*mac_key).or_default().push(i);
                    keys.insert(*mac_key);
                }
                VizEvent::QueueDepth { mac_key, .. } => {
                    idx.qdepth_by_key.entry(*mac_key).or_default().push(i);
                    keys.insert(*mac_key);
                }
            }
        }
        idx.mac_keys_sorted = keys.into_iter().collect();
        idx.mac_keys_sorted.sort_by_key(|k| (k.2, k.0 != -1, k.0, ac_prio(k.1)));
        idx
    }
}
 
fn latest_at<'a>(indices: &'a [usize], all: &'a [VizEvent], t_cursor: f64)
    -> Option<&'a VizEvent>
{
    let pos = indices.partition_point(|&i| event_t(&all[i]) <= t_cursor);
    if pos == 0 { None } else { Some(&all[indices[pos - 1]]) }
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
 
fn fill_rect(buf: &mut [u32], stride: usize, x: usize, y: usize, w: usize, h: usize, c: u32) {
    let h_buf = buf.len() / stride;
    for yy in y..(y + h).min(h_buf) {
        let row = yy * stride;
        for xx in x..(x + w).min(stride) { buf[row + xx] = c; }
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
        VizEvent::TxopStart   { t, .. } => *t,
        VizEvent::Collision   { t, .. } => *t,
        VizEvent::BackoffSnap { t, .. } => *t,
        VizEvent::QueueDepth  { t, .. } => *t,
    }
}
 
pub fn event_end(ev: &VizEvent) -> f64 {
    match ev {
        VizEvent::TxopStart { end, .. } => *end,
        VizEvent::Collision { end, .. } => *end,
        VizEvent::BackoffSnap { t, .. } => *t,
        VizEvent::QueueDepth  { t, .. } => *t,
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
        }
    }
}

// ── Shared HUD ────────────────────────────────────────────────────────────────

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
            if window.is_key_pressed(Key::Space,minifb::KeyRepeat::No) { v.paused = !v.paused; }
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

/// Mirror of channel viewer's clamp_view for the ABR state.
/// Mirror of channel viewer's clamp_view for the ABR state.
fn clamp_abr_view(v: &mut AbrViewState, idx: &AbrVizIndex) {
    let full = (idx.t_max - idx.t_min).max(1e-6);
    
    // 1. Clamp the zoom level so it cannot exceed the total duration of the data
    v.span_t = v.span_t.clamp(1e-3, full);
    
    // 2. Clamp the panning so the left and right edges never go past t_min and t_max.
    // When zoomed fully out (span_t == full), the min and max of this clamp 
    // evaluate to the exact center, perfectly locking the view in place.
    v.center_t = v.center_t.clamp(
        idx.t_min + v.span_t * 0.5,
        idx.t_max - v.span_t * 0.5,
    );
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
    ap_y:      f32,   // ← was ap_z
) {
    const PAD: usize = 6;
    let map_x = x_origin + PAD;
    let map_w = sidebar_w.saturating_sub(PAD * 2);
    let map_y = y_top + 18;
    let map_h = height.saturating_sub(22 + PAD);

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
    let (x_min, x_max) = idx.sta_x_range;
    let (y_min, y_max) = idx.sta_y_range;   // ← was sta_z_range
    let x_span = (x_max - x_min).max(1e-6);
    let y_span = (y_max - y_min).max(1e-6); // ← was z_span

    let to_px = |x: f32, y: f32| -> (i32, i32) {  // ← was (x, z)
        let nx =  ((x - x_min) / x_span) as f64;
        let ny = 1.0 - ((y - y_min) / y_span) as f64;  // ← was nz / z_span; +y = up
        (
            map_x as i32 + (nx * map_w as f64) as i32,
            map_y as i32 + (ny * map_h as f64) as i32,
        )
    };

    // ── AP marker ────────────────────────────────────────────────────────────
    {
        let (ax, ay) = to_px(ap_x, ap_y);   // ← was ap_z
        const ARM: i32 = 7;

        abr_draw_hline(buf, stride, ax - ARM, ax + ARM + 1, ay,      0xffffff);
        abr_draw_vline(buf, stride, ax,       ay - ARM,     ay + ARM, 0xffffff);

        for dy in -1i32..=1 {
            for dx in -1i32..=1 {
                let fx = ax + dx;
                let fy = ay + dy;
                if fx >= 0 && fy >= 0 {
                    let i = fy as usize * stride + fx as usize;
                    if i < buf.len() { buf[i] = 0xffff88; }
                }
            }
        }

        render_text(buf, &format!("AP ({:.1},{:.1})", ap_x, ap_y),  // ← was ap_z
                    (ax + 5).max(0) as usize, (ay - ARM - 14).max(0) as usize,
                    stride, 0xccccaa, 1.3);
    }

    // ── Per-STA: trail + dot + live coordinate label ──────────────────────────
    for (si, ip) in idx.ip_sta_order.iter().enumerate() {
        let base  = user_color(si);
        let trail = dim_color(base, 4);

        let Some(indices) = idx.by_ip_sta.get(ip) else { continue };
        let end = indices.partition_point(|&i| abr_event_t(&idx.events[i]) <= cursor_t);
        if end == 0 { continue; }

        let mut prev:       Option<(i32, i32)> = None;
        let mut last_world: Option<(f32, f32)> = None;

        for &ei in &indices[..end] {
            if let AbrEvent::StaLocation { x, y, .. } = &idx.events[ei] {  // ← was z
                let (px, py) = to_px(*x, *y);  // ← was *z
                if let Some((ppx, ppy)) = prev {
                    abr_draw_line(buf, stride, ppx, ppy, px, py, trail);
                }
                prev       = Some((px, py));
                last_world = Some((*x, *y));    // ← was *z
            }
        }

        if let Some((px, py)) = prev {
            let bright = brighten_color(base);
            for dy in -3i32..=3 {
                for dx in -3i32..=3 {
                    if dx * dx + dy * dy <= 9 {
                        let fx = px + dx;
                        let fy = py + dy;
                        if fx >= 0 && fy >= 0 {
                            let i = fy as usize * stride + fx as usize;
                            if i < buf.len() { buf[i] = bright; }
                        }
                    }
                }
            }

            if let Some((wx, wy)) = last_world {  // ← was wz
                let label     = format!("X:{:.1} Y:{:.1}", wx, wy);  // ← was Z:
                let label_x   = if px + 60 < (map_x + map_w) as i32 { px + 6 } else { px - 52 };
                let label_y   = (py - 7).max(map_y as i32);
                render_text(buf, &label,
                            label_x.max(map_x as i32) as usize,
                            label_y as usize,
                            stride, bright, 1.3);
            }
        }
    }

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

        let content_top    = HUD_H;
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

        // ── Render ────────────────────────────────────────────────────────────
        buf.fill(0x0d0d12);

        // 1. Channel panel (left half)
        {
            // Channel HUD (left portion only) — the shared HUD overwrites the
            // full top strip afterwards, so we render channel row-state here.
            let ch_highlight = {
                let (mx, my) = window.get_mouse_pos(MouseMode::Discard).unwrap_or((0.0, 0.0));
                hit_test_qdepth_legend(mx, my, content_bottom - 180, 160, ch_panel_x, &state.ch_view, &viz_idx)
            };

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

            let rows_top    = lane_y + 10;
            // let rows_bottom = content_bottom - 200;
            let rows_bottom = content_bottom.saturating_sub(200 + CW_H + 12);
            render_mackey_rows(
                &mut buf, w, rows_top, rows_bottom,
                ch_panel_x, ch_panel_w,
                &state.ch_view, &viz_idx,
                &None,
            );

            render_cw_panel(
                &mut buf, w,
                rows_bottom + 4, CW_H,
                ch_panel_x, ch_panel_w,
                &state.ch_view, &viz_idx,
                &ch_highlight,
            );

            render_qdepth_panel(
                &mut buf, w,
                content_bottom - 180, 160,
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

            // Replace the existing render_abr_sidebar_clipped call with:
            let minimap_y = content_bottom.saturating_sub(STA_MINIMAP_H);
            render_abr_sidebar_clipped(&mut buf, w, h, abr_left, &abr_idx, &state.abr_view, highlight_ip, minimap_y);
            for (si, strip) in abr_strips.iter().enumerate() {
                
                
                let strip_y = content_top + PANEL_PAD + si * abr_strip_h;
                render_abr_strip(&mut buf, w, strip, strip_y, abr_strip_h,
                                abr_panel_x, abr_panel_w, &state.abr_view, &abr_idx, highlight_ip);
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

                );
            }
        }
        // 3. Splitter bar
        render_splitter(&mut buf, w, h, splitter_x);
        // 4. Shared HUD (draws over both halves)
        render_unified_hud(&mut buf, w, &state, &viz_idx, &abr_idx);
        // 5. Shared time axis
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
    }
}

// ── Helper: render ABR sidebar offset to the right half ──────────────────────
// The original render_abr_sidebar draws at x=0; we need it offset to `x_off`.
// Rather than refactoring the original, we render into a small temp buffer and
// blit it across.  This keeps both viewers' internals unchanged.

fn render_abr_sidebar_clipped(
    buf:     &mut [u32],
    stride:  usize,
    h:       usize,
    x_off:   usize,
    idx:     &AbrVizIndex,
    view:    &AbrViewState,
    highlight_ip: Option<usize>, 
    max_y: usize, 
    ) {
    let sidebar_w = SIDEBAR_W.min(stride.saturating_sub(x_off));

    // Temp buffer — same height, sidebar width only
    let mut tmp = vec![0u32; sidebar_w * h];
    render_abr_sidebar(&mut tmp, sidebar_w, h, idx, view, highlight_ip, Some(max_y));

    // Blit into the main buffer at x_off
    for row in 0..h {
        let dst_row = row * stride + x_off;
        let src_row = row * sidebar_w;
        let copy_w  = sidebar_w.min(stride.saturating_sub(x_off));
        if dst_row + copy_w <= buf.len() {
            buf[dst_row..dst_row + copy_w].copy_from_slice(&tmp[src_row..src_row + copy_w]);
        }
    }
}


fn render_cw_panel(
    buf:     &mut [u32], stride: usize,
    panel_y: usize, panel_h: usize,
    panel_x: usize, panel_w: usize,
    view:    &ViewState,
    idx:     &VizIndex,
    highlight: &Option<ActiveHighlights>,
) {
    fill_rect(buf, stride, panel_x, panel_y, panel_w, panel_h, 0x10101a);
    fill_rect(buf, stride, 0,       panel_y, panel_x, panel_h, 0x14141c);
    render_text(buf, "CW", 8, panel_y + 6, stride, 0xcccccc, 2);

    let t_lo = view.center_t - view.span_t * 0.5;
    let t_hi = view.center_t + view.span_t * 0.5;

    // ── 1. Build one (t, cw) step series per MacKey ───────────────────────────
    let mut series_map: HashMap<MacKey, Vec<(f64, u32, bool)>> = HashMap::new();
    let mut max_cw = 4u32;   // was u16



    
    for (key, indices) in &idx.backoff_by_key {
        let mut series: Vec<(f64, u32, bool)> = indices
            .iter()
            .filter_map(|&ii| {
                if let VizEvent::BackoffSnap { t, cw, frozen, .. } = &idx.all[ii] {
                    Some((*t, *cw, *frozen))
                } else {
                    None
                }
            })
            .collect();
        series.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        if !series.is_empty() {
            series_map.insert(*key, series);
        }
    }

    // ── 2. Find visible maximum for scale ────────────────────────────────────
    let mut max_cw = 4u32;
    for series in series_map.values() {
        let s = series.partition_point(|(t, _, _)| *t < t_lo);
        if s > 0 {
            max_cw = max_cw.max(series[s - 1].1);
        }
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

        // Step-function outline
        // let mut prev: Option<(i32, i32, u16, bool)> = None;
        let mut prev: Option<(i32, i32, u32, bool)> = None;
        if s > 0 {
            let (_, cw, frozen) = series[s - 1];
            prev = Some((x_of(t_lo, view, panel_x, panel_w), calc_y(cw), cw, frozen));
        }

        for &(t, cw, frozen) in &series[s..] {
            if t > t_hi { break; }
            let x = x_of(t, view, panel_x, panel_w);
            let y = calc_y(cw);

            if let Some((px, py, prev_cw, prev_frozen)) = prev {
                // Horizontal segment — frozen periods rendered slightly dimmer
                let seg_color = if prev_frozen {
                    dim_color(outline_c, 2)
                } else {
                    outline_c
                };
                if prev_cw > 0 {
                    for fx in px..x {
                        if fx >= panel_x as i32 && fx < (panel_x + panel_w) as i32
                            && should_draw_pixel(fx, pattern_type)
                        {
                            for ty in 0..2usize {
                                let oi = (py as usize + ty) * stride + fx as usize;
                                if oi < buf.len() {
                                    buf[oi] = blend_screen(buf[oi], seg_color);
                                }
                            }
                        }
                    }
                }
                // Vertical step at transition
                if (prev_cw > 0 || cw > 0)
                    && x >= panel_x as i32 && x < (panel_x + panel_w) as i32
                {
                    draw_vline(buf, stride, x, py, y, outline_c);
                }
            }
            prev = Some((x, y, cw, frozen));
        }

        // Extend to right edge
        if let Some((px, py, prev_cw, prev_frozen)) = prev {
            if prev_cw > 0 {
                let seg_color = if prev_frozen { dim_color(outline_c, 2) } else { outline_c };
                for fx in px..(panel_x + panel_w) as i32 {
                    if fx >= panel_x as i32 && should_draw_pixel(fx, pattern_type) {
                        for ty in 0..2usize {
                            let oi = (py as usize + ty) * stride + fx as usize;
                            if oi < buf.len() {
                                buf[oi] = blend_screen(buf[oi], seg_color);
                            }
                        }
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


