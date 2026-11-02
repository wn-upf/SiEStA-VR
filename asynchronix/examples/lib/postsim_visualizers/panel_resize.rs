//! Drag-resizable layout of the unified viewer's channel side. Three edges: the bottom of the link lanes, the top of the
//! backoff (CW) panel and the top of the queue panel. Dragging one moves only that edge: the other two keep their
//! place. The backoff panel is glued to the lanes (its top rides with the lanes' bottom, so lanes that shrink give it the
//! room); its own edge adjusts the gap between them. A grip marks each edge, lit under the pointer, which turns into
//! resize arrows; a double-click on an edge gives it back its default.

use std::time::{Duration, Instant};
use minifb::{CursorStyle, MouseButton, Window};

const GRAB: f32 = 6.0;
pub const MIN_CW_H: i64 = 48;
pub const MIN_Q_H: i64 = 60;
/// A lane body never goes below this many px (or its default, if that is smaller).
const MIN_BODY: usize = 28;
const MAX_F: f32 = 2.5;
/// Room kept between the lanes' block and the CW panel.
const GAP: i64 = 6;
/// Layout constants of the channel side: below the queue panel; between CW and queue panels.
const BELOW_Q: i64 = 20;
const BETWEEN: i64 = 48;
const DOUBLE_CLICK: Duration = Duration::from_millis(400);
const Q_DEFAULT: i64 = 160;

#[derive(Clone, Copy, PartialEq)]
pub enum Edge {
    Lanes,
    Cw,
    Q,
}

/// What the layout is made of this frame (logical px).
pub struct Geometry {
    /// The channel side's width (label column and plots).
    pub right: usize,
    pub bottom: usize,
    /// y of the rows below the lanes minus the lanes' bodies: `top(f)` is that plus the scaled bodies.
    pub base_const: usize,
    /// Default body height of each lane.
    pub bodies: Vec<usize>,
    /// From the lanes' bottom edge to `top`: the gap.
    pub below_lanes: usize,
}

impl Geometry {
    pub fn body(&self, i: usize, f: f32) -> usize {
        let b = self.bodies[i];
        ((b as f32 * f).round() as usize).max(MIN_BODY.min(b))
    }
    /// First y below the lanes for lane scale `f`.
    pub fn top(&self, f: f32) -> i64 {
        (self.base_const + (0..self.bodies.len()).map(|i| self.body(i, f)).sum::<usize>()) as i64
    }
    fn f_min(&self) -> f32 {
        self.bodies.iter().map(|&b| (MIN_BODY.min(b) as f32 / b.max(1) as f32)).fold(0.2, f32::max)
    }
}

/// The sizes of this frame.
#[derive(Clone, Copy)]
pub struct Sizes {
    /// Lane scale.
    pub f: f32,
    pub cw_h: usize,
    pub q_h: usize,
    pub lanes_end: i64,
    pub cw_y: i64,
    pub q_y: i64,
}

struct Drag {
    edge: Edge,
    y0: f32,
    f0: f32,
    shift0: i64,
    q0: i64,
    t0: i64,
}

#[derive(Default)]
pub struct PanelResize {
    lane_f: Option<f32>,
    /// How far the CW edge sits above its glued place, px.
    cw_shift: i64,
    q_h: Option<i64>,
    drag: Option<Drag>,
    last_press: Option<(Instant, Edge)>,
    prev_left: bool,
    /// Edges of panels the View menu hides: neither drawn nor grabbed (lanes, CW, queue).
    hidden: [bool; 3],
}

/// The CW height that results from a lane scale, a shift and a queue height, given the defaults.
fn cw_of(g: &Geometry, cwd: i64, f: f32, shift: i64, q: i64) -> i64 {
    cwd + g.top(1.0) - g.top(f) - (q - Q_DEFAULT) + shift
}

impl PanelResize {
    /// Which edges to leave out (lanes, CW, queue), set every frame from what is shown.
    pub fn set_hidden(&mut self, hidden: [bool; 3]) {
        self.hidden = hidden;
        if self.drag.as_ref().is_some_and(|d| self.is_hidden(d.edge)) {
            self.drag = None;
        }
    }

    fn is_hidden(&self, e: Edge) -> bool {
        self.hidden[match e { Edge::Lanes => 0, Edge::Cw => 1, Edge::Q => 2 }]
    }

    /// The lane scale whose lanes end at `target`, for lanes that fill the side when both panels below
    /// them are hidden.
    pub fn fill_scale(g: &Geometry, target: i64) -> f32 {
        Self::f_for_top(g, target, MAX_F)
    }

    pub fn sizes(&self, cwd: usize, g: &Geometry) -> Sizes {
        let cwd = cwd as i64;
        let f = self.lane_f.unwrap_or(1.0).clamp(g.f_min(), MAX_F);
        let q = self.q_h.unwrap_or(Q_DEFAULT).max(MIN_Q_H);
        let cw = cw_of(g, cwd, f, self.cw_shift, q).max(MIN_CW_H);
        let cw_y = g.bottom as i64 - q - cw - BETWEEN;
        Sizes {
            f, cw_h: cw as usize, q_h: q as usize,
            lanes_end: g.top(f) - g.below_lanes as i64,
            cw_y, q_y: g.bottom as i64 - q - BELOW_Q,
        }
    }

    fn edge_y(s: &Sizes, e: Edge) -> f32 {
        (match e { Edge::Lanes => s.lanes_end + 8, Edge::Cw => s.cw_y, Edge::Q => s.q_y }) as f32
    }

    fn edge_at(&self, (mx, my): (f32, f32), g: &Geometry, s: &Sizes) -> Option<Edge> {
        if mx < 0.0 || mx >= g.right as f32 { return None; }
        // the lanes' edge is grabbed just under the lanes; the panels' edges just above them
        let near = |e: Edge| {
            let y = Self::edge_y(s, e);
            let (lo, hi) = if e == Edge::Lanes { (y - 8.0, y) } else { (y - GRAB, y + 1.0) };
            (my >= lo && my < hi).then(|| (my - y).abs())
        };
        [Edge::Q, Edge::Cw, Edge::Lanes].into_iter()
            .filter(|&e| !self.is_hidden(e))
            .filter_map(|e| near(e).map(|d| (e, d)))
            .min_by(|a, b| a.1.total_cmp(&b.1))
            .map(|(e, _)| e)
    }

    /// Largest lane scale whose lanes end no lower than `target` (the lanes' first-free y).
    fn f_for_top(g: &Geometry, target: i64, cap: f32) -> f32 {
        let (mut lo, mut hi) = (g.f_min(), cap.max(g.f_min()));
        if g.top(hi) <= target { return hi; }
        for _ in 0..24 {
            let mid = 0.5 * (lo + hi);
            if g.top(mid) <= target { lo = mid } else { hi = mid }
        }
        lo
    }

    /// Mouse input on the edges, given this frame's sizes. True while an edge is held: the caller then drops the pans
    /// and zoom box the same press would start in the panels.
    pub fn input(&mut self, window: &Window, g: &Geometry, cwd: usize, s: &Sizes) -> bool {
        let mouse = super::ui_scale::mouse_pos(window);
        let left = window.get_mouse_down(MouseButton::Left);
        let pressed = left && !self.prev_left;
        self.prev_left = left;
        if !left {
            self.drag = None;
            return false;
        }
        let Some((mx, my)) = mouse else { return self.drag.is_some() };
        let cwd = cwd as i64;
        if pressed {
            let Some(edge) = self.edge_at((mx, my), g, s) else { return false };
            let double = self.last_press.is_some_and(|(t, e)| e == edge && t.elapsed() < DOUBLE_CLICK);
            if double {
                match edge {
                    Edge::Lanes => self.lane_f = None,
                    Edge::Cw => self.cw_shift = 0,
                    Edge::Q => self.q_h = None,
                }
                self.last_press = None;
                self.drag = None;
            } else {
                self.last_press = Some((Instant::now(), edge));
                self.drag = Some(Drag { edge, y0: my, f0: s.f, shift0: self.cw_shift, q0: s.q_h as i64, t0: g.top(s.f) });
            }
            return true;
        }
        let Some(d) = &self.drag else { return false };
        // dragging an edge up by `up` px; every limit keeps the CW panel at least MIN_CW_H tall
        let up = (d.y0 - my) as i64;
        let (f, shift, q) = (s.f, self.cw_shift, s.q_h as i64);
        match d.edge {
            Edge::Q => {
                let q_max = Q_DEFAULT + cwd + g.top(1.0) - g.top(f) + shift - MIN_CW_H;
                self.q_h = Some((d.q0 + up).clamp(MIN_Q_H, q_max.max(MIN_Q_H)));
            }
            Edge::Cw => {
                let hi = (cw_glue_room(g, cwd)).max(0);
                let lo = MIN_CW_H - cwd - g.top(1.0) + g.top(f) + (q - Q_DEFAULT);
                self.cw_shift = (d.shift0 + up).clamp(lo.min(hi), hi);
            }
            Edge::Lanes => {
                let t_max = cwd + g.top(1.0) - (q - Q_DEFAULT) + shift - MIN_CW_H;
                let target = (d.t0 - up).min(t_max);
                self.lane_f = Some(Self::f_for_top(g, target, MAX_F));
            }
        }
        true
    }

    fn lit(&self, window: &Window, g: &Geometry, s: &Sizes) -> Option<Edge> {
        self.drag.as_ref().map(|d| d.edge)
            .or_else(|| super::ui_scale::mouse_pos(window).and_then(|m| self.edge_at(m, g, s)))
    }

    /// The pointer over an edge or while dragging one.
    pub fn cursor(&self, window: &Window, g: &Geometry, s: &Sizes) -> Option<CursorStyle> {
        self.lit(window, g, s).map(|_| CursorStyle::ResizeUpDown)
    }

    /// A grip on each edge (a short dotted bar at the middle), and a full-width line on the one under the pointer.
    pub fn draw(&self, buf: &mut [u32], stride: usize, window: &Window, g: &Geometry, s: &Sizes) {
        use super::fill_rect;
        let lit = self.lit(window, g, s);
        let mid = g.right / 2;
        for e in [Edge::Lanes, Edge::Cw, Edge::Q] {
            if self.is_hidden(e) { continue; }
            let on = lit == Some(e);
            // the lanes' grip sits in the gap under them, the panels' just above their top
            let y = (match e { Edge::Lanes => s.lanes_end + 3, Edge::Cw => s.cw_y - 4, Edge::Q => s.q_y - 4 }).max(0) as usize;
            if on {
                fill_rect(buf, stride, 0, y, g.right, 2, if self.drag.is_some() { 0xffcc66 } else { 0x6a9cff });
            }
            let col = if on { 0xffffff } else { 0x56627c };
            for k in 0..9usize {
                fill_rect(buf, stride, mid.saturating_sub(27) + k * 6, y, 3, 2, col);
            }
        }
    }
}

/// Most the CW edge may sit above its glued place: the gap the defaults leave under the lanes.
fn cw_glue_room(g: &Geometry, cwd: i64) -> i64 {
    let y_c_def = g.bottom as i64 - Q_DEFAULT - cwd - BETWEEN;
    y_c_def - g.top(1.0) - GAP
}
