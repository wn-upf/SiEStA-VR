//! The station map of the post-simulation viewer (`render_minimap`): the AP, every station at its position with its
//! trail, each headset's server, and arrows for the transmissions at the cursor or under the mouse. `MultiBss` holds the
//! run's one view (its events, ABR events and links) and what the map needs besides (the station ids' addresses).
//! (The name comes from the multi-BSS viewer this one is cut from; this build runs one BSS.)

use super::*;

/// Collision (map arrows and X; the lanes use a striped red).
const COLL_BSS: u32 = 0xff3344;
/// BSS colours: none is the lanes' DL blue / UL amber (the map arrows) or the collision red.
const BSS_PALETTE: [u32; 5] = [0xb39ddb, 0x4db6ac, 0xf48fb1, 0xaed581, 0xbcaaa4];

pub fn bss_color(b: u8) -> u32 {
    BSS_PALETTE[b as usize % BSS_PALETTE.len()]
}

/// What the viewer shows.
pub struct BssView {
    pub idx: VizIndex,
    pub abr: AbrVizIndex,
    /// Lane configurations, by lane id.
    pub links: Vec<LinkConfig>,
    /// Centre of the STA minimap.
    pub ap_xy: (f64, f64),
}

pub struct MultiBss {
    /// The run's one view.
    pub views: Vec<BssView>,
    pub n_bss: usize,
    /// AP position, and the BSS of every session / station address (none: BSS 0), for the map.
    ap_xy: Vec<(f64, f64)>,
    ip_bss: HashMap<IpAddr, u8>,
    sta_xy: HashMap<IpAddr, (f64, f64)>,
    sta_ip: HashMap<i32, IpAddr>,
    /// Positions of the moving stations over time (t, x, y), by address (none in this build: the map takes the
    /// headsets' positions from their location reports).
    net_pos: HashMap<IpAddr, Vec<(f64, f32, f32)>>,
    /// Background traffic direction (`is_ul_bg_traffic`: 0 DL, 1 UL, 2 both), for the map labels.
    bg_dir: Vec<u8>,
}

impl MultiBss {
    /// The run's view.
    pub fn single(idx: VizIndex, abr: AbrVizIndex, links: Vec<LinkConfig>) -> Self {
        let view = BssView { idx, abr, links, ap_xy: (AP_X as f64, AP_Y as f64) };
        let ap_xy = vec![view.ap_xy];
        MultiBss { views: vec![view], n_bss: 1, ap_xy, ip_bss: HashMap::new(), sta_xy: HashMap::new(), sta_ip: HashMap::new(),
                   net_pos: HashMap::new(), bg_dir: Vec::new() }
    }

    /// Sets the address of each station id (a one-BSS run's `run_sim` knows them), which the map's TXOP arrows need.
    pub fn with_sta_ip(mut self, sta_ip: HashMap<i32, IpAddr>) -> Self {
        self.sta_ip = sta_ip;
        self
    }

    /// Sets the background traffic direction of each BSS (`is_ul_bg_traffic`), shown next to its background stations.
    pub fn with_bg_dir(mut self, bg_dir: Vec<u8>) -> Self {
        self.bg_dir = bg_dir;
        self
    }
}

/// On opening: no link selected, rows scrolled to the top, nothing hovered.
pub fn on_switch(v: &mut ViewState, _m: &MultiBss, _active: usize) {
    v.selected_link = None;
    v.row_scroll = 0;
    v.q_scroll = 0;
    v.cw_scroll = 0;
    v.hover_t = None;
    v.hover_txop = None;
    v.hover_coll = None;
    v.hover_leg = None;
}

#[cfg(test)]
mod map_stable; // postsim_visualizers/obss/map_stable.rs: the map's fitted view does not follow the cursor

/// What the station map draws arrows for: the TXOP under the mouse in a link lane (else whatever is on the air at the
/// cursor), or one collision there; `leg`, an A-MPDU of that TXOP, has its station's arrow stressed and the others dimmed.
#[derive(Clone, Copy, Default)]
pub struct MapHover {
    pub txop: Option<(u8, f64)>,
    pub coll: Option<(u8, f64)>,
    pub leg: Option<usize>,
}

/// The palette index of station `sta` (its colour in the ABR legend, plots and map), for lighting up the
/// session of an A-MPDU under the mouse.
pub fn sta_highlight(m: &MultiBss, active: usize, sta: i32) -> Option<usize> {
    let ip = m.sta_ip.get(&sta)?;
    m.views.get(active)?.abr.ip_sta_order.iter().position(|x| x == ip)
}

/// Zoom and pan of the STA map (`render_minimap`); the wheel zooms about the mouse, a left drag pans, a right click or
/// the RESET button goes back to the fitted view.
pub struct MapView {
    zoom: f32,
    /// Offset of the view centre from the fitted one, m.
    pan: (f32, f32),
    /// Mouse and pan where the drag started.
    drag: Option<(f32, f32, (f32, f32))>,
    /// The map area (x, y, w, h) and its scale (m per px), as drawn last frame.
    rect: (usize, usize, usize, usize),
    m_per_px: f32,
    reset_btn: (usize, usize, usize, usize),
    prev_left: bool,
    prev_right: bool,
    /// Whole-run layout facts that depend on the run and the map size only (key: tab, map w, map h, stations), so they are
    /// worked out once and the fitted view cannot follow the cursor: whether two stations are ever at one spot, and the
    /// shift that makes room for the rows of boxes at the worst moment.
    grouped_run: Option<(MapKey, bool)>,
    shift_run: Option<(MapKey, f32)>,
}

type MapKey = (usize, usize, usize, usize);

impl Default for MapView {
    fn default() -> Self {
        MapView { zoom: 1.0, pan: (0.0, 0.0), drag: None, rect: (0, 0, 0, 0), m_per_px: 1.0, reset_btn: (0, 0, 0, 0),
                  prev_left: false, prev_right: false, grouped_run: None, shift_run: None }
    }
}

/// Mouse on the STA map: wheel zoom about the pointer, left-drag pan, right click or RESET back to the fitted view.
pub fn map_input(window: &Window, mv: &mut MapView) {
    let (mx, my) = super::ui_scale::mouse_pos(window).unwrap_or((-1.0, -1.0));
    let (left, right) = (window.get_mouse_down(MouseButton::Left), window.get_mouse_down(MouseButton::Right));
    let (x, y, w, h) = mv.rect;
    let inside = |r: (usize, usize, usize, usize)| mx >= r.0 as f32 && mx < (r.0 + r.2) as f32 && my >= r.1 as f32 && my < (r.1 + r.3) as f32;
    let in_map = w > 0 && inside((x, y, w, h));
    if (left && !mv.prev_left && inside(mv.reset_btn)) || (right && !mv.prev_right && in_map) {
        mv.zoom = 1.0;
        mv.pan = (0.0, 0.0);
        mv.drag = None;
    } else if in_map {
        if let Some((_, dy)) = window.get_scroll_wheel() {
            if dy != 0.0 {
                let f = if dy > 0.0 { 1.2f32 } else { 1.0 / 1.2 };
                let new_zoom = (mv.zoom * f).clamp(0.3, 25.0);
                let f = new_zoom / mv.zoom;
                // keep the point under the mouse where it is
                let (rx, ry) = (mx - (x + w / 2) as f32, my - (y + h / 2) as f32);
                mv.pan.0 += rx * mv.m_per_px * (1.0 - 1.0 / f);
                mv.pan.1 -= ry * mv.m_per_px * (1.0 - 1.0 / f);
                mv.zoom = new_zoom;
            }
        }
        if left && !mv.prev_left {
            mv.drag = Some((mx, my, mv.pan));
        }
    }
    if let (true, Some((x0, y0, p0))) = (left, mv.drag) {
        mv.pan = (p0.0 - (mx - x0) * mv.m_per_px, p0.1 + (my - y0) * mv.m_per_px);
    }
    if !left {
        mv.drag = None;
    }
    mv.prev_left = left;
    mv.prev_right = right;
}

/// The STA map: the AP at its position and the stations with their trails (a headset; a ring labelled BG DL / UL / ULDL
/// for a background station), stacked in a column where several share a spot; only the one highlighted in the ABR legend
/// gets its coordinates. Each headset's server is a laptop wired to the AP, in a column left of it. Arrows join each
/// transmitter to its receiver (DL blue, UL amber) for the TXOP under the mouse in a link lane, or else for every TXOP
/// running at the cursor. Sprites grow when the zoom leaves room for them.
pub fn render_minimap(
    buf: &mut [u32], stride: usize, x_origin: usize, sidebar_w: usize, y_top: usize, height: usize,
    m: &MultiBss, active: usize, cursor_t: f64, ch_cursor_t: f64, highlight_ip: Option<usize>,
    hover: MapHover, mv: &mut MapView,
) {
    const PAD: usize = 6;
    let (map_x, map_w, map_y) = (x_origin + PAD, sidebar_w.saturating_sub(2 * PAD), y_top + 18);
    let map_h = height.saturating_sub(22 + PAD);
    if map_w < 20 || map_h < 20 {
        return;
    }
    abr_fill_rect(buf, stride, x_origin, y_top, sidebar_w, height, 0x10101e);
    render_text(buf, "STA POSITIONS", x_origin + PAD, y_top + 3, stride, 0x888899, TEXT_SIZE_ABR);
    // the reset button, right of the title
    let btn = ((x_origin + sidebar_w).saturating_sub(PAD + 46), y_top + 2, 46, 13);
    let zoomed = mv.zoom != 1.0 || mv.pan != (0.0, 0.0);
    outline_rect_clip(buf, stride, btn.0 as i32, btn.1 as i32, (btn.0 + btn.2) as i32, (btn.1 + btn.3) as i32, if zoomed { 0x8888bb } else { 0x33334a });
    render_text(buf, "RESET", btn.0 + 8, btn.1 + 3, stride, if zoomed { 0xccccee } else { 0x555566 }, 1.0);
    mv.reset_btn = btn;
    mv.rect = (map_x, map_y, map_w, map_h);

    let idx = &m.views[active].abr;
    let all_tab = active >= m.n_bss;

    // every station, at its first report until the cursor passes it: (palette index, BSS, background device, address,
    // trail). A station with a known network position sits there; a headset moves from it by its head's horizontal (x, z)
    // displacement since its first pose.
    let mut stas: Vec<(usize, u8, bool, IpAddr, Vec<(f32, f32)>)> = Vec::new();
    // the whole run's path of each station of `stas` (t, x, y), for the layout facts that must not follow the cursor
    let mut paths: Vec<Vec<(f64, f32, f32)>> = Vec::new();
    // a station whose position the MAC updates during the run (random walk, scripted movement); only the others get boxes
    let mut mobile: Vec<bool> = Vec::new();
    // every position a station of this tab takes during the run, for the fitted box
    let mut extent: Vec<(f32, f32)> = Vec::new();
    for (si, ip) in idx.ip_sta_order.iter().enumerate() {
        // a moving station: where the MAC had it, up to the cursor
        if let Some(ps) = m.net_pos.get(ip).filter(|ps| !ps.is_empty()) {
            let end = ps.partition_point(|p| p.0 <= cursor_t).max(1);
            let bg = idx.by_ip_sta.get(ip).and_then(|ev| ev.first())
                .is_some_and(|&i| matches!(idx.events[i], AbrEvent::StaLocation { is_bg: true, .. }));
            extent.extend(ps.iter().map(|p| (p.1, p.2)));
            stas.push((si, m.ip_bss.get(ip).copied().unwrap_or(0), bg, *ip, ps[..end].iter().map(|p| (p.1, p.2)).collect()));
            paths.push(ps.clone());
            mobile.push(ps.len() > 1);
            continue;
        }
        let Some(ev) = idx.by_ip_sta.get(ip) else { continue };
        let end = ev.partition_point(|&i| abr_event_t(&idx.events[i]) <= cursor_t).max(1);
        let mut trail = Vec::new();
        let mut path_pts: Vec<(f64, f32, f32)> = Vec::new();
        let mut bg = false;
        let mut pose0: Option<(f32, f32)> = None;
        for (n, &i) in ev.iter().enumerate() {
            if let AbrEvent::StaLocation { x, y, z, is_bg, .. } = &idx.events[i] {
                let p = match m.sta_xy.get(ip) {
                    Some(&(nx, ny)) if *is_bg => (nx as f32, ny as f32),
                    Some(&(nx, ny)) => {
                        let (x0, z0) = *pose0.get_or_insert((*x, *z));
                        (nx as f32 + (x - x0), ny as f32 - (z - z0))
                    }
                    None => (*x, *y),
                };
                if path_pts.last().map(|q| (q.1, q.2)) != Some(p) { path_pts.push((abr_event_t(&idx.events[i]), p.0, p.1)); }
                if n < end {
                    if trail.last() != Some(&p) { trail.push(p); }
                    bg = *is_bg;
                }
            }
        }
        if !trail.is_empty() {
            match m.sta_xy.get(ip) {
                Some(&(x, y)) => extent.push((x as f32, y as f32)),
                None => extent.extend(path_pts.iter().map(|q| (q.1, q.2))),
            }
            stas.push((si, m.ip_bss.get(ip).copied().unwrap_or(0), bg, *ip, trail));
            paths.push(path_pts);
            mobile.push(false);
        }
    }

    // the fitted box: every AP and every position a station takes in the run (so it holds still while playing), with
    // room for the icons at its edges, at the map's aspect ratio; then the zoom and pan
    let pts = m.ap_xy.iter().map(|&(x, y)| (x as f32, y as f32)).chain(extent.iter().copied());
    let (mut x0, mut x1, mut y0, mut y1) = (f32::MAX, f32::MIN, f32::MAX, f32::MIN);
    for (x, y) in pts {
        x0 = x0.min(x); x1 = x1.max(x); y0 = y0.min(y); y1 = y1.max(y);
    }
    let mut hx = ((x1 - x0) / 2.0).max(3.0) + 0.5;
    let mut hy = ((y1 - y0) / 2.0).max(3.0) + 0.5;
    // stations at one position (within 0.5 m) form a group with a box; this depends on the run, not on the zoom, so boxes
    // never appear or vanish while zooming. A moving station is never boxed: walkers that meet only overlap, as sprites.
    let cluster = |pos: &[(f32, f32)]| -> Vec<Vec<usize>> {
        let mut clusters: Vec<Vec<usize>> = Vec::new();
        for k in 0..pos.len() {
            let near = |a: usize| !mobile[a] && !mobile[k] && (pos[a].0 - pos[k].0).abs() < 0.5 && (pos[a].1 - pos[k].1).abs() < 0.5;
            match clusters.iter_mut().find(|c| near(c[0])) {
                Some(c) => c.push(k),
                None => clusters.push(vec![k]),
            }
        }
        clusters
    };
    // where every station is at time t (before its first report, at it)
    let pos_at = |t: f64| -> Vec<(f32, f32)> {
        paths.iter().map(|p| { let i = p.partition_point(|q| q.0 <= t).max(1) - 1; (p[i].1, p[i].2) }).collect()
    };
    let cur: Vec<(f32, f32)> = stas.iter().map(|s| *s.4.last().unwrap()).collect();
    let clusters = cluster(&cur);
    // moments of the run that the layout is made to fit, so that it does not change with the cursor
    let sample_ts: Vec<f64> = {
        let (lo, hi) = paths.iter().fold((f64::MAX, f64::MIN), |a, p| (a.0.min(p[0].0), a.1.max(p[p.len() - 1].0)));
        const N: usize = 400;
        if hi > lo { (0..=N).map(|i| lo + (hi - lo) * i as f64 / N as f64).collect() } else { vec![lo] }
    };
    let key: MapKey = (active, map_w, map_h, stas.len());
    // Whether two stations are ever at one spot in the run (or now): decides the side margins, which set the zoom, so it must
    // not follow the cursor.
    let grouped_run = match mv.grouped_run {
        Some((k, g)) if k == key => g,
        _ => {
            let g = sample_ts.iter().any(|&t| cluster(&pos_at(t)).iter().any(|c| c.len() >= 2));
            mv.grouped_run = Some((key, g));
            g
        }
    };
    let grouped = grouped_run || clusters.iter().any(|c| c.len() >= 2);
    // the id a station transmits under (a pair has a DL and a UL id; the UL one is the station's own), as the queue panel names it
    let mut id_of: HashMap<IpAddr, i32> = HashMap::new();
    for (&id, &ip) in &m.sta_ip {
        let e = id_of.entry(ip).or_insert(id);
        *e = (*e).max(id);
    }
    // A group's box: a header (position, distance to the nearest AP), a rule, then a column of its stations. The sprite scale
    // is 1 whenever there is a group, so the box has one size at every zoom.
    const BOX_STEP: i32 = 13;
    const BOX_RING_R: i32 = 4;
    let box_hs = HEADSET_SCALE.max((2 * BOX_RING_R + 1) as f32 / 7.0);
    let bg_label = |b: u8| format!("BG{}", match m.bg_dir.get(b as usize) { Some(0) => " DL", Some(1) => " UL", Some(2) => " ULDL", _ => "" });
    let box_tag = |k: usize| -> String {
        let id = id_of.get(&stas[k].3).map(|i| format!("s{} ", i)).unwrap_or_default();
        if stas[k].2 { format!("{}{}", id, bg_label(stas[k].1)) } else { format!("{}VR", id) }
    };
    let box_icon_w = |k: usize| if stas[k].2 { 2 * BOX_RING_R + 1 } else { (26.0 * box_hs).round() as i32 };
    let box_row_w = |k: usize| box_icon_w(k) + 4 + 6 * box_tag(k).len() as i32;
    struct Group { members: Vec<usize>, l1: String, l2: String, half: i32, up: i32, down: i32, wx: f32, wy: f32 }
    let build_groups = |clusters: &[Vec<usize>], pos: &[(f32, f32)]| -> Vec<Group> { clusters.iter().filter(|c| c.len() >= 2).map(|c| {
        let mut members = c.clone();
        members.sort_by_key(|&k| (stas[k].1, stas[k].2, id_of.get(&stas[k].3).copied().unwrap_or(0)));
        let (wx, wy) = pos[members[0]];
        let (ai, d) = m.ap_xy.iter().enumerate()
            .map(|(i, a)| (i, ((a.0 as f32 - wx).powi(2) + (a.1 as f32 - wy).powi(2)).sqrt()))
            .fold((0, f32::MAX), |acc, x| if x.1 < acc.1 { x } else { acc });
        let (l1, l2) = (format!("({:.1}, {:.1})", wx, wy), format!("{:.1} m AP{}", d, ai));
        let text_w = 6 * l1.len().max(l2.len()) as i32;
        let half = (text_w.max(members.iter().map(|&k| box_row_w(k)).max().unwrap()) + 1) / 2 + 9;
        let n = members.len() as i32;
        Group { members, l1, l2, half, up: n * BOX_STEP / 2 + 30, down: n * BOX_STEP / 2 + 3, wx, wy }
    }).collect() };
    let groups = build_groups(&clusters, &cur);
    // wider on the left for the VR servers' laptop column, on the right for the background stations' labels
    let mgx_l = if stas.iter().any(|s| !s.2) { 56.0f32 } else { 34.0 }.max(if grouped { 64.0 } else { 0.0 });
    let mgx_r = if grouped { 64.0f32 } else if stas.iter().any(|s| s.2) { 60.0 } else { 34.0 };
    let mgy = 26.0f32;
    let (in_w, in_h) = ((map_w as f32 - mgx_l - mgx_r).max(10.0), (map_h as f32 - 2.0 * mgy).max(10.0));
    let aspect = in_w / in_h;
    if hx / hy > aspect { hy = hx / aspect } else { hx = hy * aspect }
    let m_base = 2.0 * hx / in_w;
    let m_per_px = m_base / mv.zoom;
    mv.m_per_px = m_per_px;

    // Which row each box goes in, and how far it sits from its spot, is decided once at the default view (zoom 1, no pan):
    // left to right, each box goes in the first row where it fits between the map's edges beside the boxes already there.
    // So the boxes fit what is seen after a right click, and the rows do not change while zooming.
    const M: i32 = 64;
    let map_wi = map_w as i32;
    let base_cx = (x0 + x1) / 2.0 - (mgx_l - mgx_r) / 2.0 * m_base;
    let base_px = |wx: f32| M + map_wi / 2 + ((wx - base_cx) / m_base).round() as i32;
    let (edge_l, edge_r) = (M + 6, M + map_wi - 6);
    let pack = |groups: &[Group]| -> (Vec<usize>, Vec<Vec<usize>>) {
        let mut order: Vec<usize> = (0..groups.len()).collect();
        order.sort_by_key(|&i| base_px(groups[i].wx));
        let wide = |i: usize| 2 * groups[i].half;
        // rows: each box in the first row it still fits in, side by side with 8 px between
        let mut rows: Vec<Vec<usize>> = Vec::new();
        for &i in &order {
            let used = |r: &Vec<usize>| r.iter().map(|&j| wide(j) + 8).sum::<i32>();
            match rows.iter_mut().find(|r| used(r) + wide(i) <= edge_r - edge_l) {
                Some(r) => r.push(i),
                None => rows.push(vec![i]),
            }
        }
        (order, rows)
    };
    let (order, rows) = pack(&groups);
    let n_rows = rows.len();
    let wide = |i: usize| 2 * groups[i].half;
    // positions in a row: as near each spot as the boxes allow (they are pushed together, not stacked on one side), by pooling
    // neighbours that would overlap, then kept between the map's edges
    let mut slot: Vec<(usize, i32)> = vec![(0, 0); groups.len()];
    for (r, row) in rows.iter().enumerate() {
        let prefix: Vec<i32> = row.iter().scan(0, |acc, &i| { let p = *acc; *acc += wide(i) + 8; Some(p) }).collect();
        let total = row.iter().map(|&i| wide(i) + 8).sum::<i32>() - 8;
        let mut blocks: Vec<(f32, usize)> = Vec::new(); // (sum of desired left edges less prefixes, count)
        for (k, &i) in row.iter().enumerate() {
            blocks.push(((base_px(groups[i].wx) - groups[i].half - prefix[k]) as f32, 1));
            while blocks.len() >= 2 {
                let (a, b) = (blocks[blocks.len() - 2], blocks[blocks.len() - 1]);
                if a.0 / a.1 as f32 <= b.0 / b.1 as f32 { break; }
                blocks.truncate(blocks.len() - 2);
                blocks.push((a.0 + b.0, a.1 + b.1));
            }
        }
        let mut k = 0;
        for (sum, count) in blocks {
            let t = ((sum / count as f32).round() as i32).clamp(edge_l, (edge_r - total).max(edge_l));
            for _ in 0..count {
                let i = row[k];
                slot[i] = (r, t + prefix[k] + groups[i].half - base_px(groups[i].wx));
                k += 1;
            }
        }
    }
    // Room under the lowest AP for the rows of boxes, taken from above by moving the content up (not by zooming out). It is the
    // room the worst moment of the run needs, so the content holds still while the groups come and go.
    let shift = match mv.shift_run {
        Some((k, v)) if k == key => v,
        _ => {
            let floor_off = 20.max((0..m.ap_xy.len().max(1) as u8).map(|b| servers_half_h(stas.iter().filter(|s| !s.2 && s.1 == b).count(), 1)).max().unwrap_or(0) + 6);
            let lowest_ap = (((y0 + y1) / 2.0 - y0) / m_base) as i32;
            let mut below = 0;
            for t in &sample_ts {
                let pos = pos_at(*t);
                let cl = cluster(&pos);
                if !cl.iter().any(|c| c.len() >= 2) { continue; }
                let gs = build_groups(&cl, &pos);
                let n_rows = pack(&gs).1.len() as i32;
                let box_h = gs.iter().map(|g| g.up + g.down).max().unwrap_or(0);
                below = below.max(floor_off + 28 + n_rows * box_h + 8 * (n_rows - 1) + 16);
            }
            let v = if below > 0 { ((below + lowest_ap - map_h as i32 / 2).max(0) as f32).min((map_h as f32 / 2.0 - 40.0).max(0.0)) } else { 0.0 };
            mv.shift_run = Some((key, v));
            v
        }
    };
    let (cx, cy) = ((x0 + x1) / 2.0 - (mgx_l - mgx_r) / 2.0 * m_per_px + mv.pan.0, (y0 + y1) / 2.0 - shift * m_per_px + mv.pan.1);

    // drawn offscreen, with a margin so whatever pokes out of the map is cut at its edge, then copied in
    let (ow, oh) = (map_w as i32 + 2 * M, map_h as i32 + 2 * M);
    let mut ob = vec![0x10101eu32; (ow * oh) as usize];
    let os = ow as usize;
    let to_px = |x: f32, y: f32| -> (i32, i32) {
        (M + map_w as i32 / 2 + ((x - cx) / m_per_px).round() as i32, M + map_h as i32 / 2 - ((y - cy) / m_per_px).round() as i32)
    };
    let near = |p: (i32, i32)| p.0 > 8 && p.0 < ow - 8 && p.1 > 8 && p.1 < oh - 8;
    let mut line = |ob: &mut Vec<u32>, a: (i32, i32), b: (i32, i32), c: u32| {
        if let Some((a, b)) = clip_line(a, b, (2, 2, ow - 3, oh - 3)) {
            abr_draw_line(ob, os, a.0, a.1, b.0, b.1, c);
        }
    };

    // grid: 5 m, or coarser so lines stay >= 12 px apart
    let mut step = 5.0f32;
    while step / m_per_px < 12.0 { step *= 2.0; }
    let (wx0, wy0) = (cx - (ow as f32 / 2.0) * m_per_px, cy - (oh as f32 / 2.0) * m_per_px);
    let mut g = (wx0 / step).ceil() * step;
    while g < wx0 + ow as f32 * m_per_px {
        let x = to_px(g, 0.0).0;
        fill_rect_clip(&mut ob, os, x, 0, x + 1, oh, 0x181828);
        g += step;
    }
    let mut g = (wy0 / step).ceil() * step;
    while g < wy0 + oh as f32 * m_per_px {
        let y = to_px(0.0, g).1;
        fill_rect_clip(&mut ob, os, 0, y, ow, y + 1, 0x181828);
        g += step;
    }

    // trails
    for (si, b, _, _, trail) in &stas {
        let lit = highlight_ip.map_or(true, |h| h == *si);
        let c = dim_color(bss_color(*b), if lit { 2 } else { 6 });
        for w in trail.windows(2) {
            line(&mut ob, to_px(w[0].0, w[0].1), to_px(w[1].0, w[1].1), c);
        }
    }

    // a group at one position is a spot of its own; other stations sharing a spot (within 6 px) stack in a column centred on it
    let mut spots: Vec<((i32, i32), Vec<usize>)> = Vec::new();
    for c in clusters.iter().filter(|c| c.len() >= 2) {
        let &(wx, wy) = stas[c[0]].4.last().unwrap();
        spots.push((to_px(wx, wy), c.clone()));
    }
    let first_single = spots.len();
    for c in clusters.iter().filter(|c| c.len() == 1) {
        let k = c[0];
        let &(wx, wy) = stas[k].4.last().unwrap();
        let p = to_px(wx, wy);
        match spots[first_single..].iter_mut().find(|s| (s.0 .0 - p.0).abs() < 6 && (s.0 .1 - p.1).abs() < 6) {
            Some(s) => s.1.push(k),
            None => spots.push((p, vec![k])),
        }
    }
    let aps_px: Vec<(i32, i32)> = m.ap_xy.iter().map(|&(x, y)| to_px(x as f32, y as f32)).collect();
    // sprite size: as large as the gap between a station's nearest approach to an AP in the run and that AP allows (1x-2x), capped
    // for dense maps; it follows the zoom, not the cursor
    let gap = extent.iter().flat_map(|e| m.ap_xy.iter().map(move |a| ((a.0 as f32 - e.0).powi(2) + (a.1 as f32 - e.1).powi(2)).sqrt()))
        .fold(f32::MAX, f32::min) / m_per_px;
    let sz: i32 = if gap >= 60.0 && !grouped { 2 } else { 1 };

    // The AP grows with the station sprites (box 19 px a step, headset visor 15), so zooming keeps their proportions; it shrinks
    // (to at most 0.45x) when a group's dot would fall on it, so the dot stays visible; all APs share the smallest size at its true position.
    let ap_scale: Vec<f32> = aps_px.iter().map(|&(ax, ay)| {
        let full = 1.2 * sz as f32;
        let touches = |s: f32| spots[..groups.len()].iter().any(|((px, py), _)| {
            let (dx, dy) = (px - ax, py - ay);
            dx.abs() <= (8.0 * s).ceil() as i32 + 5 && dy >= -((12.0 * s).ceil() as i32 + 5) && dy <= (3.0 * s).ceil() as i32 + 5
        });
        let mut s = full;
        while s > 0.45 * full && touches(s) { s -= 0.05 * full; }
        s
    }).collect();
    // one size for every AP: the smallest any of them needs
    let ap_scale: Vec<f32> = vec![ap_scale.iter().copied().fold(f32::MAX, f32::min); ap_scale.len()];

    // APs, the active tab's brightest
    for (b, &p) in aps_px.iter().enumerate() {
        if !near(p) { continue; }
        let c = if all_tab || b == active { bss_color(b as u8) } else { dim_color(bss_color(b as u8), 3) };
        let ap_s = ap_scale[b];
        draw_ap_icon(&mut ob, os, p.0, p.1, c, ap_s);
        render_text(&mut ob, &format!("AP{}", b), (p.0 - 8).max(0) as usize, (p.1 + 4 + (3.0 * ap_s).ceil() as i32) as usize, os, c, 1.0);
    }

    let mut label: Option<(i32, i32, String, u32)> = None;
    let mut icon_at: HashMap<IpAddr, (i32, i32)> = HashMap::new();
    // VR servers: one laptop per headset, in a column left of its AP, wired to it; lit with its headset
    let mut servers: HashMap<u8, Vec<usize>> = HashMap::new();
    for (si, b, ..) in stas.iter().filter(|s| !s.2) {
        servers.entry(*b).or_default().push(*si);
    }
    // Boxes at this zoom: each in its row (decided at the default view), centred under its spot plus its offset there, slid right
    // only as far as needed to clear the box before it; a row sits a margin below the one above, the first below every AP (its
    // label and servers) and its spot. All continuous in the zoom, so nothing jumps.
    let ap_floor = aps_px.iter().enumerate().map(|(b, &(_, ay))| {
        ay + (20 * sz).max(servers_half_h(servers.get(&(b as u8)).map_or(0, |v| v.len()), sz) + 6 * sz)
    }).max().unwrap_or(i32::MIN / 2);
    let mut lay: Vec<(i32, i32)> = vec![(0, 0); groups.len()]; // box centre x, y of its column's centre
    let mut above_bottom = i32::MIN / 2;
    for r in 0..n_rows {
        let mut right = i32::MIN / 2;
        let mut bottom = above_bottom;
        for &i in order.iter().filter(|&&i| slot[i].0 == r) {
            let g = &groups[i];
            let (px, py) = spots[i].0;
            let cx = (px + slot[i].1).max(right + 8 + g.half);
            let top = if r == 0 { (py + 10).max(ap_floor + 28) } else { above_bottom + 8 };
            lay[i] = (cx, top + g.up);
            right = cx + g.half;
            bottom = bottom.max(top + g.up + g.down);
        }
        above_bottom = bottom;
    }
    // colours of a group: its BSS (grey if mixed), dimmed unless lit by the ABR legend
    let group_colors = |g: &Group| {
        let bc = if g.members.iter().all(|&k| stas[k].1 == stas[g.members[0]].1) { bss_color(stas[g.members[0]].1) } else { 0x9999bb };
        let lit = highlight_ip.map_or(true, |h| g.members.iter().any(|&k| stas[k].0 == h));
        (dim_color(bc, if lit { 3 } else { 6 }), dim_color(bc, if lit { 1 } else { 4 }))
    };
    // VR servers: one laptop per headset, in a column beside its AP, wired to it by splines; lit with its headset. A column takes
    // the first of a few fixed slots (left of the AP, right of it, then further out, then shifted down or up) that touches no dot,
    // leader, box, station, other AP or earlier column; so it keeps its usual place unless something is there.
    type R4 = (i32, i32, i32, i32);
    let hit = |a: R4, b: R4| a.0 < b.2 && b.0 < a.2 && a.1 < b.3 && b.1 < a.3;
    let mut blocks: Vec<R4> = Vec::new();
    let mut leaders: Vec<((i32, i32), (i32, i32))> = Vec::new();
    for (i, g) in groups.iter().enumerate() {
        let ((px, py), (cx, cy)) = (spots[i].0, lay[i]);
        let r = (cx - g.half, cy - g.up, cx + g.half, cy + g.down);
        blocks.push((r.0 - 4, r.1 - 4, r.2 + 4, r.3 + 4));
        blocks.push((px - 8, py - 8, px + 8, py + 8));
        leaders.push(((px, py), (px.clamp(r.0, r.2), py.clamp(r.1, r.3))));
    }
    for ((px, py), mem) in spots.iter().skip(groups.len()) {
        let (w, h) = (if mem.iter().any(|&k| stas[k].2) { 60 } else { 24 }, 8 + 5 * mem.len() as i32);
        blocks.push((px - 14, py - h, px + w, py + h));
    }
    let ap_boxes: Vec<[R4; 2]> = aps_px.iter().map(|&(ax, ay)| {
        [(ax - (10 * sz + 2), ay - 8 * sz, ax + 10 * sz + 2, ay + 20 * sz), (ax - 16 * sz, ay - 52 * sz, ax + 16 * sz, ay - 8 * sz)]
    }).collect();
    let mut server_bss: Vec<u8> = servers.keys().copied().collect();
    // boxed ones first (their places are fixed, which the columns then avoid)
    server_bss.sort_by_key(|b| (servers[b].len() < SERVERS_BOXED_FROM, *b));
    for b in server_bss {
        let sis = &servers[&b];
        let Some(&(ax, ay)) = aps_px.get(b as usize) else { continue };
        if sis.len() >= SERVERS_BOXED_FROM {
            // Many servers: one box of small laptops at a fixed offset from its AP's sprite, left of it; its size does not follow
            // the zoom, and it moves with the AP.
            let n = sis.len() as i32;
            // sized by the AP sprite (which steps with the station sprites) and, more gently, by the zoom
            let bs = ap_scale[b as usize] / 1.2 * mv.zoom.sqrt().clamp(0.7, 2.2);
            let px = |v: f32| (v * bs).round() as i32;
            let (cols, bw, bh) = server_box(sis.len(), bs);
            let ap_edge = ax - px(10.0);
            let rect = (ap_edge - px(BOXED_CABLE_LEN) - bw, ay - bh / 2, ap_edge - px(BOXED_CABLE_LEN), ay - bh / 2 + bh);
            let (cw, ch, pad) = (11.0 * bs, 9.0 * bs, 4.0 * bs);
            let any_lit = highlight_ip.map_or(true, |h| sis.contains(&h));
            let bc = bss_color(b);
            // one thick cable from the box's middle to the AP, in a neutral grey so it is not taken for a station's thin,
            // BSS-coloured trail
            let wire = if any_lit { 0x8a90a8 } else { 0x3c4050 };
            let (y, x0, x1) = (ay, rect.2, ap_edge);
            for dy in -(bs.round() as i32).max(1)..=(bs.round() as i32).max(1) {
                line(&mut ob, (x0, y + dy), (x1, y + dy), wire);
            }
            fill_rect_clip(&mut ob, os, rect.0, rect.1, rect.2, rect.3, 0x181826);
            outline_rect_clip(&mut ob, os, rect.0, rect.1, rect.2, rect.3, dim_color(bc, if any_lit { 3 } else { 6 }));
            for (j, si) in sis.iter().enumerate() {
                let (c, r) = (j % cols, j / cols);
                let centre = (rect.0 + (pad + c as f32 * cw + cw / 2.0).round() as i32, rect.1 + (pad + r as f32 * ch + ch / 2.0).round() as i32);
                let lit = highlight_ip.map_or(true, |h| h == *si);
                draw_sprite_f(&mut ob, os, centre.0, centre.1, &LAPTOP, if lit { bc } else { dim_color(bc, 4) }, BOXED_LAPTOP_SCALE * bs);
            }
            blocks.push((rect.0 - 2, rect.1 - 2, rect.2 + 2, rect.3 + 2));
            continue;
        }
        let (n, step) = (sis.len() as i32, 14 * sz);
        // a slot: side of the AP (-1 left, 1 right), how far out, vertical shift -> laptop centres, their wire ends, and the
        // rectangles it occupies (the laptops, the wires)
        let slot = |side: i32, out: i32, dy: i32| {
            let ix = ax + side * (10 + 20 * sz + out * 18 * sz);
            let rows: Vec<((i32, i32), (i32, i32))> = (0..n).map(|j| {
                let iy = ay + dy + (2 * j - (n - 1)) * step / 2;
                let end = (ax + side * 10 * sz, ay + ((j * 2 - (n - 1)) * 3 * sz / 2).clamp(-4 * sz, 4 * sz));
                ((ix, iy), end)
            }).collect();
            let span = |f: fn(&((i32, i32), (i32, i32)), i32) -> (i32, i32)| {
                rows.iter().map(|r| f(r, sz)).fold((i32::MAX, i32::MIN), |a, v| (a.0.min(v.0), a.1.max(v.1)))
            };
            let centres = span(|r, _| (r.0 .1, r.0 .1));
            let lap = (ix - 8 * sz, centres.0 - 6 * sz, ix + 8 * sz, centres.1 + 6 * sz);
            let (x_in, x_out) = (ix - side * 8 * sz, ax + side * 10 * sz);
            let ends = span(|r, sz| ((r.0 .1 + 3 * sz).min(r.1 .1), (r.0 .1 + 3 * sz).max(r.1 .1)));
            let wire = (x_in.min(x_out), ends.0 - 1, x_in.max(x_out), ends.1 + 1);
            (rows, lap, wire)
        };
        let cost = |lap: R4, wire: R4| -> usize {
            [lap, wire].iter().map(|&r| {
                blocks.iter().filter(|&&q| hit(r, q)).count()
                    + leaders.iter().filter(|&&(a, c)| clip_line(a, c, r).is_some()).count()
                    + ap_boxes.iter().enumerate().filter(|(o, _)| *o != b as usize).map(|(_, bx)| bx.iter().filter(|&&q| hit(r, q)).count()).sum::<usize>()
                    + usize::from(hit(r, (ap_boxes[b as usize][1].0, ap_boxes[b as usize][1].1, ap_boxes[b as usize][1].2, ay - 16 * sz)))
            }).sum()
        };
        let mut best: Option<(usize, (Vec<((i32, i32), (i32, i32))>, R4, R4))> = None;
        'slots: for out in 0..2 {
            for dy in [0, 22 * sz, -22 * sz] {
                for side in [-1, 1] {
                    let sl = slot(side, out, dy);
                    let c = cost(sl.1, sl.2);
                    if best.as_ref().map_or(true, |bs| c < bs.0) { best = Some((c, sl)); }
                    if c == 0 { break 'slots; }
                }
            }
        }
        let (_, (rows, lap, wire_r)) = best.unwrap();
        blocks.push(lap);
        blocks.push(wire_r);
        let side = if rows[0].1 .0 < rows[0].0 .0 { -1 } else { 1 };
        for (j, si) in sis.iter().enumerate() {
            let ((ix, iy), end) = rows[j];
            let lit = highlight_ip.map_or(true, |h| h == *si);
            let c = if lit { bss_color(b) } else { dim_color(bss_color(b), 4) };
            // the wire: a cubic spline with level ends, from the laptop to its own spot on the AP's edge; the spots keep the
            // laptops' order, so the wires never cross
            let start = (ix - side * 8 * sz, iy + 3 * sz);
            let mid = (start.0 + end.0) as f32 / 2.0;
            let at = |t: f32| {
                let u = 1.0 - t;
                let x = u * u * u * start.0 as f32 + 3.0 * u * u * t * mid + 3.0 * u * t * t * mid + t * t * t * end.0 as f32;
                let y = u * u * u * start.1 as f32 + 3.0 * u * u * t * start.1 as f32 + 3.0 * u * t * t * end.1 as f32 + t * t * t * end.1 as f32;
                (x.round() as i32, y.round() as i32)
            };
            let wire = dim_color(c, 2);
            let mut prev = start;
            for k in 1..=16 {
                let p = at(k as f32 / 16.0);
                line(&mut ob, prev, p, wire);
                prev = p;
            }
            if near((ix, iy)) {
                draw_sprite(&mut ob, os, ix, iy, &LAPTOP, c, sz);
            }
        }
    }
    // leaders first (a box drawn later covers any that pass behind it): a round dot at the group's true position, a line to its box
    for (i, g) in groups.iter().enumerate() {
        let ((px, py), (cx, cy)) = (spots[i].0, lay[i]);
        let (left, top, right, bottom) = (cx - g.half, cy - g.up, cx + g.half, cy + g.down);
        if !(near((left, top)) || near((right, bottom)) || near((px, py))) { continue; }
        let (_, tc) = group_colors(g);
        let (tx, ty) = (px.clamp(left, right), py.clamp(top, bottom));
        let off = if (tx - px).abs() >= (ty - py).abs() { (0, 1) } else { (1, 0) };
        for k in 0..2 {
            line(&mut ob, (px + k * off.0, py + k * off.1), (tx + k * off.0, ty + k * off.1), tc);
        }
        for dy in -3i32..=3 {
            for dx in -3i32..=3 {
                if dx * dx + dy * dy <= 10 { fill_rect_clip(&mut ob, os, px + dx, py + dy, px + dx + 1, py + dy + 1, tc); }
            }
        }
    }
    // What is on the air at the cursor (or under the mouse in a link lane): per station whether the AP sends to it (dl) or it
    // sends to the AP, in the TXOP's colour, and the same for collision contenders. The arrows come after the sprites; the
    // boxes tint the rows of the stations involved, so the arrow can stop at a box's edge and the row says which station.
    let view = &m.views[active];
    let txops: Vec<(u8, &TxopGroup)> = match hover.txop {
        // a hovered collision is drawn alone
        _ if hover.coll.is_some() => Vec::new(),
        Some((link, t)) => view.idx.txop_groups_by_link.get(&link)
            .and_then(|gs| gs.iter().find(|g| g.t == t)).map(|g| vec![(link, g)]).unwrap_or_default(),
        None => view.idx.txop_groups_by_link.iter().filter_map(|(&link, gs)| {
            let i = gs.partition_point(|g| g.end <= ch_cursor_t);
            gs.get(i).filter(|g| g.t <= ch_cursor_t).map(|g| (link, g))
        }).collect(),
    };
    let mut txop_hits: Vec<(IpAddr, bool, u32)> = Vec::new();
    // the hovered A-MPDU's station and direction
    let mut stressed: Option<(IpAddr, bool)> = None;
    for (_, g) in txops {
        for &e in &g.legs {
            let VizEvent::TxopStart { sender, dest_id, .. } = &view.idx.all[e] else { continue };
            // the AP sends downlink to the station; a station sends uplink to its AP (its `dest_id` may be the wired
            // peer behind the AP)
            let (dl, sta) = if sender.0 < 0 { (true, *dest_id) } else { (false, sender.0) };
            let Some(&ip) = m.sta_ip.get(&sta) else { continue };
            let hit = (ip, dl, if dl { DL_BE_COLOR } else { UL_BE_COLOR });
            if !txop_hits.contains(&hit) { txop_hits.push(hit); }
            if hover.leg == Some(e) { stressed = Some((ip, dl)); }
        }
    }
    // with one A-MPDU of the TXOP hovered, the TXOP's other users fade
    if let Some(st) = stressed {
        for h in txop_hits.iter_mut().filter(|h| (h.0, h.1) != st) {
            h.2 = dim_color(h.2, 3);
        }
    }
    // collisions at the cursor: an arrow for each contender's attempted transmission (a station to its AP; the AP to the
    // station of its next downlink TXOP on the lane), red within a BSS, magenta with another BSS, each marked with an X
    let mut coll_hits: Vec<(IpAddr, bool, u32)> = Vec::new();
    for (&link, idxs) in &view.idx.collisions_by_link {
        // the collisions at the cursor, or only the hovered one
        let at = hover.coll.map_or(ch_cursor_t, |(_, t)| t);
        if hover.coll.is_some_and(|(l, _)| l != link) { continue; }
        let i = idxs.partition_point(|&k| event_t(&view.idx.all[k]) <= at);
        for &k in idxs[..i].iter().rev().take(6) {
            let VizEvent::Collision { end, contenders, .. } = &view.idx.all[k] else { continue };
            let keep = match hover.coll { Some((_, t)) => event_t(&view.idx.all[k]) == t, None => ch_cursor_t < *end };
            if !keep { continue; }
            let c = COLL_BSS;
            for key in contenders {
                let hit = if key.0 < 0 {
                    // the AP: toward the destination of its next downlink TXOP on this lane
                    let t_coll = event_t(&view.idx.all[k]);
                    view.idx.txop_groups_by_link.get(&link).and_then(|gs| {
                        let from = gs.partition_point(|g| g.t < t_coll);
                        gs[from..].iter().take(8).find_map(|g| g.legs.iter().find_map(|&e| match &view.idx.all[e] {
                            VizEvent::TxopStart { sender, dest_id, .. } if sender.0 < 0 => Some(*dest_id),
                            _ => None,
                        }))
                    }).and_then(|d| m.sta_ip.get(&d)).map(|&ip| (ip, true, c))
                } else {
                    m.sta_ip.get(&key.0).map(|&ip| (ip, false, c))
                };
                if let Some(h) = hit { if !coll_hits.contains(&h) { coll_hits.push(h); } }
            }
        }
    }
    // a station in a box: its row's tint, a colour per direction
    let mut tint: HashMap<IpAddr, [Option<u32>; 2]> = HashMap::new();
    for &(ip, dl, c) in txop_hits.iter().chain(coll_hits.iter()) {
        let e = tint.entry(ip).or_insert([None, None]);
        e[if dl { 0 } else { 1 }].get_or_insert(c);
    }
    let mut box_of: HashMap<IpAddr, R4> = HashMap::new();

    for (si_idx, ((px, py), spot_members)) in spots.iter().enumerate() {
        let boxed = si_idx < groups.len();
        let members: Vec<usize> = if boxed { groups[si_idx].members.clone() } else { spot_members.clone() };
        let members = &members;
        let n = members.len() as i32;
        let hs = HEADSET_SCALE * sz as f32;
        let ring_r = 2 * sz + 2;
        // in a box the headset is as tall as a ring is wide, so the two kinds of station weigh the same
        let hsb = if boxed { box_hs } else { hs };
        // the text after a station's icon: its id and role in a box; a lone background station only its role
        let tag = |k: usize| -> String {
            match (stas[k].2, boxed) {
                (_, true) => box_tag(k),
                (true, false) => bg_label(stas[k].1),
                (false, false) => String::new(),
            }
        };
        let icon_w = |k: usize| if stas[k].2 { 2 * ring_r + 1 } else { (26.0 * hsb).round() as i32 };
        let row_w = |k: usize| icon_w(k) + match tag(k).len() { 0 => 0, l => 4 + 6 * l as i32 };
        // a group gets a roomy column; a lone station keeps the tight spacing
        let step = if boxed { BOX_STEP } else if members.iter().any(|&k| stas[k].2) { (9.0 * hs).round().max((4 * sz + 8) as f32) as i32 } else { (9.0 * hs).round() as i32 };
        // where the column of stations is centred: the spot, or its box's centre
        let (cx, cy) = if boxed { lay[si_idx] } else { (*px, *py) };
        if boxed {
            let g = &groups[si_idx];
            let (left, top, right, bottom) = (cx - g.half, cy - g.up, cx + g.half, cy + g.down);
            let (c, tc) = group_colors(g);
            if near((left, top)) || near((right, bottom)) {
                // opaque, so the leaders and grid behind it do not show through; a two pixel edge
                fill_rect_clip(&mut ob, os, left, top, right + 1, bottom + 1, 0x10101e);
                outline_rect_clip(&mut ob, os, left, top, right, bottom, c);
                outline_rect_clip(&mut ob, os, left + 1, top + 1, right - 1, bottom - 1, c);
                fill_rect_clip(&mut ob, os, left + 5, top + 26, right - 4, top + 27, dim_color(c, 2));
                for (row, text) in [&g.l1, &g.l2].into_iter().enumerate() {
                    let x = cx - 6 * text.len() as i32 / 2;
                    render_text(&mut ob, text, x.max(0) as usize, (top + 5 + 10 * row as i32).max(0) as usize, os, tc, 1.0);
                }
            }
        }
        for (j, &k) in members.iter().enumerate() {
            let (si, b, bg, ip, trail) = &stas[k];
            let iy = cy + (2 * j as i32 - (n - 1)) * step / 2;
            let row_left = cx - row_w(k) / 2;
            let visor = (HEADSET_VISOR_DX * hsb).round() as i32;
            // where the station is: the ring's centre, or the headset's visor (arrows point here)
            let ix = match (boxed, *bg) {
                (false, _) => *px,
                (true, true) => row_left + ring_r,
                (true, false) => row_left + icon_w(k) / 2 - visor,
            };
            icon_at.insert(*ip, (ix, iy));
            if boxed {
                let g = &groups[si_idx];
                box_of.insert(*ip, (cx - g.half, cy - g.up, cx + g.half, cy + g.down));
            }
            if !near((ix, iy)) { continue; }
            if let (true, Some(t)) = (boxed, tint.get(ip)) {
                // the station is on the air: its row lit in the arrow's colour (half and half for both directions)
                let g = &groups[si_idx];
                let (x0, x1, y0, y1) = (cx - g.half + 3, cx + g.half - 2, iy - step / 2 + 1, iy + step / 2);
                match (t[0], t[1]) {
                    (Some(d), Some(u)) => {
                        fill_rect_clip(&mut ob, os, x0, y0, x1, (y0 + y1) / 2, dim_color(d, 2));
                        fill_rect_clip(&mut ob, os, x0, (y0 + y1) / 2, x1, y1, dim_color(u, 2));
                    }
                    (Some(c), None) | (None, Some(c)) => fill_rect_clip(&mut ob, os, x0, y0, x1, y1, dim_color(c, 2)),
                    _ => {}
                }
            }
            let lit = highlight_ip.map_or(true, |h| h == *si);
            let c = if lit { bss_color(*b) } else { dim_color(bss_color(*b), 4) };
            let text_x = if boxed { row_left + icon_w(k) + 4 } else { ix + ring_r + 3 };
            if *bg {
                for k in 0..8 * ring_r {
                    let a = k as f32 / (8 * ring_r) as f32 * 2.0 * std::f32::consts::PI;
                    let (x, y) = (ix + (ring_r as f32 * a.cos()).round() as i32, iy + (ring_r as f32 * a.sin()).round() as i32);
                    if x >= 0 && y >= 0 && x < ow && y < oh {
                        ob[y as usize * os + x as usize] = c;
                    }
                }
            } else {
                draw_sprite_f(&mut ob, os, ix + visor, iy, &HEADSET, c, hsb);
            }
            let text = tag(k);
            if !text.is_empty() {
                render_text(&mut ob, &text, text_x.max(0) as usize, (iy - 3).max(0) as usize, os, c, 1.0);
            }
            if highlight_ip == Some(*si) {
                let &(wx, wy) = trail.last().unwrap();
                label = Some((ix, iy - 4 * sz, format!("X:{:.1} Y:{:.1}", wx, wy), brighten_color(bss_color(*b))));
            }
        }
    }

    // Where arrows meet an AP: one anchor per station on a rectangle just outside the AP's sprite and label, so the arrows
    // of different stations do not run together near the AP. Each anchor lies in its station's direction, and neighbouring
    // directions are spread to at least 0.3 rad apart (neighbours pooled, as for the boxes). Depends only on where the
    // stations are, so the anchors hold still while the arrows come and go.
    let mut anchor: HashMap<IpAddr, (i32, i32)> = HashMap::new();
    for (b, &(ax, ay)) in aps_px.iter().enumerate() {
        let mut v: Vec<(f32, IpAddr)> = stas.iter().filter(|s| s.1 as usize == b)
            .filter_map(|s| icon_at.get(&s.3).map(|&(x, y)| (((y - ay) as f32).atan2((x - ax) as f32), s.3))).collect();
        if v.is_empty() { continue; }
        v.sort_by(|p, q| p.0.partial_cmp(&q.0).unwrap());
        let k = v.len();
        let tau = 2.0 * std::f32::consts::PI;
        let gap = |i: usize| ((v[(i + 1) % k].0 - v[i].0).rem_euclid(tau)).max(if k == 1 { tau } else { 0.0 });
        // unwrap the angles in order, starting after the widest empty gap
        let first = (0..k).max_by(|&i, &j| gap(i).partial_cmp(&gap(j)).unwrap()).map_or(0, |i| (i + 1) % k);
        let mut ang: Vec<(f32, IpAddr)> = Vec::new();
        let mut a = v[first].0;
        for n in 0..k {
            let i = (first + n) % k;
            if n > 0 { a += (v[i].0 - v[(first + n - 1) % k].0).rem_euclid(tau); }
            ang.push((a, v[i].1));
        }
        const SEP: f32 = 0.3;
        let mut blocks: Vec<(f32, usize)> = Vec::new();
        for (i, &(a, _)) in ang.iter().enumerate() {
            blocks.push((a - SEP * i as f32, 1));
            while blocks.len() >= 2 {
                let (p, q) = (blocks[blocks.len() - 2], blocks[blocks.len() - 1]);
                if p.0 / p.1 as f32 <= q.0 / q.1 as f32 { break; }
                blocks.truncate(blocks.len() - 2);
                blocks.push((p.0 + q.0, p.1 + q.1));
            }
        }
        let k = ap_scale[b] / (1.2 * sz as f32);
        let (hx_a, up_a, down_a) = ((12.0 * sz as f32 * k).max(10.0) + 2.0, 6.0 * sz as f32 * k + 2.0, (20.0 * sz as f32 * k).max(16.0 + (3.0 * ap_scale[b]).ceil()));
        let mut i = 0;
        for (sum, count) in blocks {
            for _ in 0..count {
                let phi = sum / count as f32 + SEP * i as f32;
                let (dx, dy) = (phi.cos(), phi.sin());
                let t = (hx_a / dx.abs().max(1e-3)).min(if dy > 0.0 { down_a } else { up_a } / dy.abs().max(1e-3));
                anchor.insert(ang[i].1, (ax + (t * dx).round() as i32, ay + (t * dy).round() as i32));
                i += 1;
            }
        }
    }
    // arrows: where a station is in a box, the arrow stops at the box's edge (the tinted row names the station)
    let head = 4 + 3 * sz;
    let end_at = |ip: &IpAddr, dl: bool, c: u32| -> Option<((i32, i32), (i32, i32), u32)> {
        let &sta_px = icon_at.get(ip)?;
        let ap = anchor.get(ip).copied().or_else(|| stas.iter().find(|s| s.3 == *ip).and_then(|s| aps_px.get(s.1 as usize).copied()))?;
        let (dx, dy) = ((sta_px.0 - ap.0) as f32, (sta_px.1 - ap.1) as f32);
        let n = (dx * dx + dy * dy).sqrt().max(1.0);
        // the far end: a box's edge (the tinted row names the station), else the icon; stopping short of it by two pixels, or by
        // the head's length so the icon stays visible
        let (far, back) = match box_of.get(ip).and_then(|&r| clip_line(ap, sta_px, r)) {
            Some((e, _)) => (e, 2.0),
            None => (sta_px, head as f32),
        };
        let far = (far.0 - (dx / n * back).round() as i32, far.1 - (dy / n * back).round() as i32);
        Some(if dl { (ap, far, c) } else { (far, ap, c) })
    };
    for (ip, dl, c) in &txop_hits {
        if let Some((from, to, c)) = end_at(ip, *dl, *c) {
            if let Some((a, b)) = clip_line(from, to, (2, 2, ow - 3, oh - 3)) {
                draw_arrow(&mut ob, os, a, b, c, head);
                // the hovered A-MPDU's arrow: twice as thick, with a bigger head
                if stressed == Some((*ip, *dl)) {
                    draw_arrow(&mut ob, os, (a.0 + 1, a.1 + 1), (b.0 + 1, b.1 + 1), brighten_color(c), head + 2);
                }
            }
        }
    }

    // the X sits on the arrow's midpoint (free space between the stations), on a dark plate so it reads over sprites
    let arm = 3 * sz + 3;
    let mut marks: Vec<((i32, i32), u32)> = Vec::new();
    let mut coll_arrows: Vec<((i32, i32), (i32, i32), u32)> = Vec::new();
    for (ip, dl, c) in &coll_hits {
        if let Some(a) = end_at(ip, *dl, *c) { if !coll_arrows.contains(&a) { coll_arrows.push(a); } }
    }
    for &(from, to, c) in &coll_arrows {
        if let Some((a, b)) = clip_line(from, to, (2, 2, ow - 3, oh - 3)) {
            draw_arrow(&mut ob, os, a, b, c, head);
            let mid = ((a.0 + b.0) / 2, (a.1 + b.1) / 2);
            // a station and its AP often collide on one arrow pair: keep one X per spot
            if !marks.iter().any(|&(m, _)| (m.0 - mid.0).abs() < arm && (m.1 - mid.1).abs() < arm) { marks.push((mid, c)); }
        }
    }
    for ((cx, cy), c) in marks {
        fill_rect_clip(&mut ob, os, cx - arm - 2, cy - arm - 2, cx + arm + 3, cy + arm + 3, 0x05050a);
        for (p0, p1) in [((cx - arm, cy - arm), (cx + arm, cy + arm)), ((cx - arm, cy + arm), (cx + arm, cy - arm))] {
            for dx in [0, 1] { // two pixels thick
                abr_draw_line(&mut ob, os, p0.0 + dx, p0.1, p1.0 + dx, p1.1, c);
            }
        }
    }

    if let Some((x, y, text, c)) = label {
        let tw = text_w(&text, 1.0);
        let lx = if x + 8 + tw < M + map_w as i32 { x + 8 } else { (x - 8 - tw).max(M) };
        fill_rect_clip(&mut ob, os, lx - 2, y - 12, lx + tw + 2, y - 2, 0x05050a);
        render_text(&mut ob, &text, lx.max(0) as usize, (y - 11).max(0) as usize, os, c, 1.0);
    }

    // copy the map area in
    for row in 0..map_h {
        let src = (row + M as usize) * os + M as usize;
        let dst = (map_y + row) * stride + map_x;
        if dst + map_w <= buf.len() && map_x + map_w <= stride {
            buf[dst..dst + map_w].copy_from_slice(&ob[src..src + map_w]);
        }
    }

    render_text(buf, "wheel zoom, drag, R-click reset", map_x + 5, map_y + map_h - 11, stride, 0x555566, 1.0);

    // scale bar: a round length near 40 px
    let target = 40.0 * m_per_px;
    let mag = 10f32.powf(target.log10().floor());
    let len = [1.0, 2.0, 5.0, 10.0].iter().map(|f| f * mag).find(|l| *l >= target * 0.7).unwrap_or(mag * 10.0);
    let bar = (len / m_per_px) as i32;
    // top right, clear of the boxes under the APs
    let (bx, by) = ((map_x + map_w) as i32 - bar - 8, map_y as i32 + 20);
    abr_draw_hline(buf, stride, bx, bx + bar, by, 0x888899);
    abr_draw_vline(buf, stride, bx, by - 3, by, 0x888899);
    abr_draw_vline(buf, stride, bx + bar, by - 3, by, 0x888899);
    render_text(buf, &format!("{}m", len), bx.max(0) as usize, (by - 13).max(0) as usize, stride, 0x888899, 1.0);

    outline_rect_clip(buf, stride, map_x as i32, map_y as i32, (map_x + map_w) as i32, (map_y + map_h) as i32, 0x2a2a3e);
}

/// The part of segment `a`-`b` inside the box (x0, y0, x1, y1) (Liang-Barsky).
fn clip_line(a: (i32, i32), b: (i32, i32), (x0, y0, x1, y1): (i32, i32, i32, i32)) -> Option<((i32, i32), (i32, i32))> {
    let (dx, dy) = ((b.0 - a.0) as f32, (b.1 - a.1) as f32);
    let (mut t0, mut t1) = (0.0f32, 1.0f32);
    for (p, q) in [(-dx, (a.0 - x0) as f32), (dx, (x1 - a.0) as f32), (-dy, (a.1 - y0) as f32), (dy, (y1 - a.1) as f32)] {
        if p == 0.0 {
            if q < 0.0 { return None; }
        } else {
            let r = q / p;
            if p < 0.0 { t0 = t0.max(r) } else { t1 = t1.min(r) }
        }
    }
    (t0 <= t1).then(|| ((a.0 + (t0 * dx) as i32, a.1 + (t0 * dy) as i32), (a.0 + (t1 * dx) as i32, a.1 + (t1 * dy) as i32)))
}

/// A 2 px arrow from `a` to `b` with a head of up to `head` px at `b` (shorter on a short arrow, so it still shows).
fn draw_arrow(buf: &mut [u32], stride: usize, a: (i32, i32), b: (i32, i32), c: u32, head: i32) {
    let (dx, dy) = ((b.0 - a.0) as f32, (b.1 - a.1) as f32);
    let len = (dx * dx + dy * dy).sqrt();
    if len < 4.0 {
        return;
    }
    let gap = (head as f32).min(len / 2.0);
    let (ux, uy) = (dx / len, dy / len);
    let tip = b;
    for (ox, oy) in [(0, 0), (1, 0), (0, 1)] {
        abr_draw_line(buf, stride, a.0 + ox, a.1 + oy, tip.0 + ox, tip.1 + oy, c);
    }
    for s in [-1.0f32, 1.0] {
        // the head: two strokes at +-28 degrees from the shaft
        let (cs, sn) = (0.883f32, 0.469f32 * s);
        let (hx, hy) = (-(ux * cs - uy * sn) * gap, -(ux * sn + uy * cs) * gap);
        for (ox, oy) in [(0, 0), (1, 0), (0, 1)] {
            abr_draw_line(buf, stride, tip.0 + ox, tip.1 + oy, tip.0 + hx as i32 + ox, tip.1 + hy as i32 + oy, c);
        }
    }
}

/// Map sprites: `#` frame (BSS colour), `-` strap (darker), `o` glass / screen, `w` glint, `~` Wi-Fi sign (lighter),
/// `.` transparent. The headset is a front view in the style of a Vision Pro (dark glass in a thin frame that dips into
/// a nose cutout); stations carry a Wi-Fi sign at their top right.
pub(crate) const HEADSET: [&str; 7] = [
    "..###############...~~~~~.",
    ".#ooooooooooooooo#.~.....~",
    "-#owwoooooooooooo#-..~~~..",
    "-#oowoooooooooooo#-...~...",
    ".#ooooooooooooooo#........",
    "..#ooooo###ooooo#.........",
    "...#####...#####..........",
];
/// More VR servers than this on one AP are drawn together, smaller, in a box.
const SERVERS_BOXED_FROM: usize = 6;
/// The small laptops' size relative to a lone one.
const BOXED_LAPTOP_SCALE: f32 = 0.6;
/// Length of the cable from a laptop box to its AP at base size, px.
const BOXED_CABLE_LEN: f32 = 26.0;

/// The box of `n` boxed laptops drawn at `s` times the base size: columns, and width and height in px.
fn server_box(n: usize, s: f32) -> (usize, i32, i32) {
    let cols = (n as f32).sqrt().ceil() as usize;
    let rows = n.div_ceil(cols);
    let px = |v: f32| (v * s).round() as i32;
    (cols, px(cols as f32 * 11.0 + 8.0), px(rows as f32 * 9.0 + 8.0))
}

/// Half the height a BSS's servers take beside their AP: a column of laptops; a box sits in the map's corner instead.
fn servers_half_h(n: usize, sz: i32) -> i32 {
    if n >= SERVERS_BOXED_FROM { 0 } else { n as i32 * 7 * sz }
}

/// A VR server's laptop: wired, so no Wi-Fi sign; drawn next to its AP.
const LAPTOP: [&str; 11] = [
    "..###########..",
    ".#ooooooooooo#.",
    ".#owwoooooooo#.",
    ".#owooooooooo#.",
    ".#ooooooooooo#.",
    ".#ooooooooooo#.",
    "..###########..",
    ".#############.",
    "#-------------#",
    "#-----###-----#",
    "###############",
];

/// Headsets are drawn this much smaller than the other sprites.
const HEADSET_SCALE: f32 = 0.85;
/// The headset's visor centre lies this many sprite pixels left of the sprite's centre (the Wi-Fi sign hangs off its
/// right); the visor, not the sprite, sits on the station's spot, where the arrows point.
pub(crate) const HEADSET_VISOR_DX: f32 = 4.0;

fn sprite_color(ch: u8, body: u32) -> Option<u32> {
    Some(match ch {
        b'#' => body,
        b'-' => dim_color(body, 2),
        b'o' => 0x161624,
        b'w' => 0xffffff,
        b'~' => brighten_color(brighten_color(body)),
        _ => return None,
    })
}

/// Draws `rows` centred on (cx, cy), each sprite pixel `sz` x `sz` screen pixels.
fn draw_sprite(buf: &mut [u32], stride: usize, cx: i32, cy: i32, rows: &[&str], body: u32, sz: i32) {
    let (h, w) = (rows.len() as i32 * sz, rows[0].len() as i32 * sz);
    for (r, row) in rows.iter().enumerate() {
        for (c, ch) in row.bytes().enumerate() {
            let Some(color) = sprite_color(ch, body) else { continue };
            let (x, y) = (cx - w / 2 + c as i32 * sz, cy - h / 2 + r as i32 * sz);
            fill_rect_clip(buf, stride, x, y, x + sz, y + sz, color);
        }
    }
}

/// `draw_sprite` at a fractional pixel size `s`: each screen pixel mixes the sprite colours it covers by area, over
/// what is already drawn there.
pub(crate) fn draw_sprite_f(buf: &mut [u32], stride: usize, cx: i32, cy: i32, rows: &[&str], body: u32, s: f32) {
    if s.fract() == 0.0 {
        return draw_sprite(buf, stride, cx, cy, rows, body, s as i32);
    }
    let (sh, sw) = (rows.len(), rows[0].len());
    let (x0, y0) = (cx as f32 - sw as f32 * s / 2.0, cy as f32 - sh as f32 * s / 2.0);
    let h_buf = (buf.len() / stride) as i32;
    let rgb = |c: u32| [(c >> 16 & 0xff) as f32, (c >> 8 & 0xff) as f32, (c & 0xff) as f32];
    for py in (y0.floor() as i32).max(0)..((y0 + sh as f32 * s).ceil() as i32).min(h_buf) {
        // the pixel's extent in sprite pixels
        let (v0, v1) = ((py as f32 - y0) / s, (py as f32 + 1.0 - y0) / s);
        for px in (x0.floor() as i32).max(0)..((x0 + sw as f32 * s).ceil() as i32).min(stride as i32) {
            let (u0, u1) = ((px as f32 - x0) / s, (px as f32 + 1.0 - x0) / s);
            let (mut acc, mut cov) = ([0.0f32; 3], 0.0f32);
            for r in (v0.floor().max(0.0) as usize)..(v1.ceil() as usize).min(sh) {
                let fy = v1.min(r as f32 + 1.0) - v0.max(r as f32);
                for (c, &ch) in rows[r].as_bytes().iter().enumerate().take((u1.ceil() as usize).min(sw)).skip(u0.floor().max(0.0) as usize) {
                    let Some(color) = sprite_color(ch, body) else { continue };
                    let a = (fy * (u1.min(c as f32 + 1.0) - u0.max(c as f32)) * s * s).max(0.0);
                    let k = rgb(color);
                    for i in 0..3 { acc[i] += a * k[i]; }
                    cov += a;
                }
            }
            if cov <= 0.0 { continue; }
            let i = py as usize * stride + px as usize;
            let bg = rgb(buf[i]);
            let ch = |j: usize| ((acc[j] + (1.0 - cov.min(1.0)) * bg[j]).round().clamp(0.0, 255.0)) as u32;
            buf[i] = ch(0) << 16 | ch(1) << 8 | ch(2);
        }
    }
}
