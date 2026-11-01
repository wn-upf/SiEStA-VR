//! What the mouse points at in a link lane beyond a whole TXOP: a collision (its stripes), or one A-MPDU (its tile). A
//! hovered collision lights its contenders' rows, gets an outline and a tooltip, and the station map draws only its arrows;
//! a hovered tile makes the map stress that one station's arrow.

use std::collections::HashSet;
use super::{event_t, leg_rects, x_of, ActiveHighlights, MacKey, VizEvent, VizIndex, ViewState};

/// A collision under the mouse.
pub struct CollHover {
    pub link: u8,
    pub t: f64,
    pub end: f64,
    pub contenders: Vec<MacKey>,
}

/// The lane (y, height, link) under `my` and the time under `mx`, with the time a pixel spans.
fn lane_time(mx: f32, my: f32, lanes: &[(usize, usize, u8)], panel_x: usize, panel_w: usize, view: &ViewState)
    -> Option<((usize, usize, u8), f64, f64)> {
    let (mxi, myi) = (mx as i32, my as i32);
    if panel_w == 0 || mxi < panel_x as i32 || mxi >= (panel_x + panel_w) as i32 {
        return None;
    }
    let &lane = lanes.iter().find(|(y, h, _)| myi >= *y as i32 && myi < (*y + *h) as i32)?;
    let t_px = view.span_t / panel_w as f64;
    Some((lane, view.center_t - view.span_t * 0.5 + (mx as f64 - panel_x as f64) * t_px, t_px))
}

/// The collision under the mouse in a link lane: its stripes cover the lane between 6 px from its top and bottom.
pub fn collision_at(mx: f32, my: f32, lanes: &[(usize, usize, u8)], panel_x: usize, panel_w: usize, view: &ViewState, idx: &VizIndex)
    -> Option<CollHover> {
    let ((lane_y, lane_h, link), t_m, t_px) = lane_time(mx, my, lanes, panel_x, panel_w, view)?;
    let myi = my as usize;
    if myi < lane_y + 6 || myi + 6 >= lane_y + lane_h {
        return None;
    }
    let cs = idx.collisions_by_link.get(&link)?;
    // a collision a pixel wide still catches the mouse
    let i = cs.partition_point(|&k| event_t(&idx.all[k]) <= t_m + t_px);
    cs[..i].iter().rev().take(16).find_map(|&k| match &idx.all[k] {
        VizEvent::Collision { t, end, contenders, .. } if t_m <= end.max(t + t_px) => Some(CollHover {
            link, t: *t, end: *end, contenders: contenders.to_vec(),
        }),
        _ => None,
    })
}

/// The A-MPDU (its `TxopStart` index) whose tile is under the mouse.
pub fn leg_at(mx: f32, my: f32, lanes: &[(usize, usize, u8)], panel_x: usize, panel_w: usize, view: &ViewState, idx: &VizIndex)
    -> Option<usize> {
    let ((lane_y, lane_h, link), t_m, t_px) = lane_time(mx, my, lanes, panel_x, panel_w, view)?;
    let groups = idx.txop_groups_by_link.get(&link)?;
    let (top, h) = ((lane_y + 2) as i32, lane_h as i32 - 4);
    let (mxi, myi) = (mx as i32, my as i32);
    let start = groups.partition_point(|g| g.end < t_m - 2.0 * t_px);
    groups[start..].iter().take_while(|g| g.t <= t_m + 2.0 * t_px).find_map(|g| {
        leg_rects(g, idx, view, panel_x, panel_w, top, h).into_iter()
            .find(|r| mxi >= r.x0 && mxi < r.x1 && myi >= r.y0 && myi < r.y1)
            .map(|r| r.ev)
    })
}

fn who(k: &MacKey) -> String {
    if k.0 < 0 { super::ap_name(k.0) } else { format!("s{}", k.0) }
}

pub fn tooltip(c: &CollHover) -> Vec<String> {
    let names: Vec<String> = c.contenders.iter().map(|k| format!("{} {:?}", who(k), k.1)).collect();
    vec![
        format!("Collision on link {}", c.link),
        format!("contenders: {}", names.join(", ")),
        format!("{:.1} us of airtime lost @ {:.6} s", (c.end - c.t) * 1e6, c.t),
        "their backoff reached 0 in the same slot; the map shows each attempt with an X".to_string(),
    ]
}

/// The contenders' queues, lit across the lanes, the rows and the backoff and queue panels.
pub fn highlights(c: &CollHover) -> ActiveHighlights {
    let keys: HashSet<MacKey> = c.contenders.iter().copied().collect();
    let txops = keys.iter().map(|k| (*k, -1)).collect();
    ActiveHighlights { keys, txops }
}

/// A white frame around the hovered collision in its lane.
pub fn draw_outline(buf: &mut [u32], stride: usize, c: &CollHover, lanes: &[(usize, usize, u8)], panel_x: usize, panel_w: usize, view: &ViewState) {
    let Some(&(y, h, _)) = lanes.iter().find(|l| l.2 == c.link) else { return };
    let (px0, px1) = (panel_x as i32, (panel_x + panel_w) as i32);
    let x0 = x_of(c.t, view, panel_x, panel_w).clamp(px0, px1);
    let x1 = x_of(c.end, view, panel_x, panel_w).clamp(x0 + 2, px1.max(x0 + 2));
    super::outline_rect_clip(buf, stride, x0 - 1, (y + 4) as i32, x1, (y + h).saturating_sub(5) as i32, 0xffffff);
}
