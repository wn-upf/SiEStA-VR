//! Drag-resizable ABR sidebar of the unified viewer. Its right edge sets the sidebar's width (legend and station map
//! together) and the station map's top edge sets the map's height. The pointer turns into resize arrows over either edge,
//! as over the splitter, and a double-click on an edge gives it back its default size.

use std::time::{Duration, Instant};
use minifb::{CursorStyle, MouseButton, Window};

/// Grab band around the sidebar's right edge, logical px each side.
const GRAB_X: f32 = 4.0;
/// Grab band around the map's top edge: mostly above it, clear of the map's title row and RESET button.
const GRAB_ABOVE: f32 = 5.0;
const GRAB_BELOW: f32 = 2.0;
const MIN_SIDEBAR_W: usize = 150;
/// The ABR plots keep at least this width beside the sidebar.
const MIN_PLOTS_W: usize = 250;
const MIN_MAP_H: usize = 90;
/// The legend above the map keeps at least this height (less if the legend itself is shorter).
const MIN_LEGEND_H: usize = 120;
const DOUBLE_CLICK: Duration = Duration::from_millis(400);

#[derive(Clone, Copy, PartialEq)]
enum Edge {
    Side,
    Map,
}

/// Where the sidebar sits this frame (logical px).
pub struct Geometry {
    /// Left edge of the ABR panel (the sidebar's left edge, or the plots' when `on_right`).
    pub left: usize,
    /// Width of the ABR panel, sidebar and plots together.
    pub room_w: usize,
    pub top: usize,
    pub bottom: usize,
    /// Whether the station map is drawn (a run without stations has none).
    pub has_map: bool,
    /// Height of the legend and cursor table at the top of the sidebar, which the map may cover only down to
    /// `MIN_LEGEND_H` of.
    pub legend_h: usize,
    /// The sidebar sits at the panel's right edge (Layout menu): its left edge is the one that drags.
    pub on_right: bool,
}

impl Geometry {
    /// Left edge of a sidebar `w` wide.
    pub fn sidebar_x(&self, w: usize) -> usize {
        if self.on_right { (self.left + self.room_w).saturating_sub(w) } else { self.left }
    }
}

#[derive(Default)]
pub struct SidebarResize {
    width: Option<usize>,
    map_h: Option<usize>,
    /// The edge being dragged, the mouse coordinate along the drag and the size at the press.
    drag: Option<(Edge, f32, usize)>,
    last_press: Option<(Instant, Edge)>,
    prev_left: bool,
}

impl SidebarResize {
    /// The sidebar width and map height: the dragged sizes (the defaults until dragged), clamped to the room there is.
    pub fn sizes(&self, default_w: usize, default_h: usize, g: &Geometry) -> (usize, usize) {
        let max_w = g.room_w.saturating_sub(MIN_PLOTS_W).max(MIN_SIDEBAR_W);
        let max_h = g.bottom.saturating_sub(g.top).saturating_sub(g.legend_h.min(MIN_LEGEND_H)).max(MIN_MAP_H);
        (self.width.unwrap_or(default_w).clamp(MIN_SIDEBAR_W, max_w), self.map_h.unwrap_or(default_h).clamp(MIN_MAP_H, max_h))
    }

    fn edge_at(&self, (mx, my): (f32, f32), g: &Geometry, w: usize, h: usize) -> Option<Edge> {
        let x0 = g.sidebar_x(w);
        let edge_x = if g.on_right { x0 } else { x0 + w } as f32;
        let map_y = g.bottom.saturating_sub(h) as f32;
        if (mx - edge_x).abs() < GRAB_X && my >= g.top as f32 && my < g.bottom as f32 {
            Some(Edge::Side)
        } else if g.has_map && my >= map_y - GRAB_ABOVE && my < map_y + GRAB_BELOW && mx >= x0 as f32 && mx < (x0 + w) as f32 {
            Some(Edge::Map)
        } else {
            None
        }
    }

    /// Mouse input on the edges, given this frame's sizes. True while an edge is held: the caller then drops the pans
    /// and zoom box the same press would start in the panels.
    pub fn input(&mut self, window: &Window, g: &Geometry, w: usize, h: usize) -> bool {
        let mouse = super::ui_scale::mouse_pos(window);
        let left = window.get_mouse_down(MouseButton::Left);
        let pressed = left && !self.prev_left;
        self.prev_left = left;
        if !left {
            self.drag = None;
            return false;
        }
        let Some((mx, my)) = mouse else { return self.drag.is_some() };
        if pressed {
            let Some(edge) = self.edge_at((mx, my), g, w, h) else { return false };
            let double = self.last_press.is_some_and(|(t, e)| e == edge && t.elapsed() < DOUBLE_CLICK);
            if double {
                match edge {
                    Edge::Side => self.width = None,
                    Edge::Map => self.map_h = None,
                }
                self.last_press = None;
                self.drag = None;
            } else {
                self.last_press = Some((Instant::now(), edge));
                self.drag = Some(match edge {
                    Edge::Side => (edge, mx, w),
                    Edge::Map => (edge, my, h),
                });
            }
            return true;
        }
        match self.drag {
            // on the right, dragging its left edge leftwards makes the sidebar wider
            Some((Edge::Side, x0, w0)) => {
                let dx = if g.on_right { x0 - mx } else { mx - x0 };
                self.width = Some((w0 as f32 + dx).max(0.0) as usize)
            }
            // dragging the map's top edge up makes the map taller
            Some((Edge::Map, y0, h0)) => self.map_h = Some((h0 as f32 - (my - y0)).max(0.0) as usize),
            None => return false,
        }
        true
    }

    /// The pointer over an edge or while dragging one.
    pub fn cursor(&self, window: &Window, g: &Geometry, w: usize, h: usize) -> Option<CursorStyle> {
        let edge = self.drag.map(|d| d.0).or_else(|| super::ui_scale::mouse_pos(window).and_then(|m| self.edge_at(m, g, w, h)))?;
        Some(match edge {
            Edge::Side => CursorStyle::ResizeLeftRight,
            Edge::Map => CursorStyle::ResizeUpDown,
        })
    }
}
