//! The ABR plots' order and which of them are shown (ABR menu). A plot's title drags: drop it between two other plots to
//! move it there. The pointer turns into a hand over a title, and a bar marks where the plot will land.

use minifb::{CursorStyle, MouseButton, Window};
use crate::lib::render_text;
use super::{fill_rect, outline_rect_clip, text_w, TEXT_SIZE_ABR};

/// Height of a plot's title band, the part that drags (logical px).
const TITLE_H: usize = 20;
/// A press must move this far before it counts as a drag (logical px).
const DRAG_START: f32 = 4.0;

pub struct StripOrder {
    /// Plot indices (into `make_strips`) in display order.
    pub order: Vec<usize>,
    /// Whether each plot is shown, by plot index.
    pub shown: Vec<bool>,
    /// Display position (among the shown plots) of the title pressed, the press y, and whether it has moved enough to be
    /// a drag.
    drag: Option<(usize, f32, bool)>,
    prev_left: bool,
}

/// Where the shown plots are this frame (logical px).
pub struct Geometry {
    pub x: usize,
    pub w: usize,
    pub top: usize,
    pub strip_h: usize,
    /// Each shown plot's name, in display order.
    pub names: Vec<String>,
}

impl Geometry {
    fn n(&self) -> usize {
        self.names.len()
    }
    /// The title band of the plot shown at position `i`: (x0, y0, x1, y1).
    fn title_rect(&self, i: usize) -> (usize, usize, usize, usize) {
        let y = self.top + i * self.strip_h;
        let tw = (text_w(&self.names[i], TEXT_SIZE_ABR) as usize + 16).min(self.w);
        (self.x, y, self.x + tw, y + TITLE_H.min(self.strip_h))
    }
    fn title_at(&self, mx: f32, my: f32) -> Option<usize> {
        if mx < 0.0 || my < 0.0 {
            return None;
        }
        let (mx, my) = (mx as usize, my as usize);
        (0..self.n()).find(|&i| {
            let (x0, y0, x1, y1) = self.title_rect(i);
            mx >= x0 && mx < x1 && my >= y0 && my < y1
        })
    }
    /// The gap between plots nearest to `my`: 0 above the first, `n` below the last.
    fn slot_at(&self, my: f32) -> usize {
        if self.strip_h == 0 {
            return 0;
        }
        let rel = (my - self.top as f32) / self.strip_h as f32;
        (rel.round().max(0.0) as usize).min(self.n())
    }
}

/// `order` with the plot shown at position `from` of `visible` moved to gap `slot` of `visible` (0: before the first shown
/// plot; `visible.len()`: after the last). Hidden plots keep their place relative to the shown ones before them.
pub fn moved(order: &[usize], visible: &[usize], from: usize, slot: usize) -> Vec<usize> {
    let Some(&id) = visible.get(from) else { return order.to_vec() };
    let mut out: Vec<usize> = order.iter().copied().filter(|&i| i != id).collect();
    let at = if let Some(&before) = visible.get(slot) {
        if before == id {
            return order.to_vec();
        }
        out.iter().position(|&i| i == before).unwrap_or(out.len())
    } else {
        visible.iter().rev().find(|&&v| v != id)
            .and_then(|&last| out.iter().position(|&i| i == last))
            .map_or(out.len(), |p| p + 1)
    };
    out.insert(at, id);
    out
}

impl StripOrder {
    pub fn new(n: usize) -> Self {
        StripOrder { order: (0..n).collect(), shown: vec![true; n], drag: None, prev_left: false }
    }

    /// The shown plots, in display order.
    pub fn visible(&self) -> Vec<usize> {
        self.order.iter().copied().filter(|&i| self.shown.get(i).copied().unwrap_or(true)).collect()
    }

    pub fn reset(&mut self) {
        *self = StripOrder::new(self.shown.len());
    }

    pub fn is_default_order(&self) -> bool {
        self.order.iter().enumerate().all(|(k, &i)| k == i)
    }

    /// Mouse input on the titles. True while a title is held: the caller then drops the pan and zoom box the same press
    /// would start in the plots.
    pub fn input(&mut self, window: &Window, g: &Geometry) -> bool {
        let left = window.get_mouse_down(MouseButton::Left);
        let pressed = left && !self.prev_left;
        self.prev_left = left;
        let Some((mx, my)) = super::ui_scale::mouse_pos(window) else { return left && self.drag.is_some() };
        if pressed {
            self.drag = g.title_at(mx, my).map(|i| (i, my, false));
            return self.drag.is_some();
        }
        let Some((from, y0, moving)) = self.drag else { return false };
        if left {
            self.drag = Some((from, y0, moving || (my - y0).abs() >= DRAG_START));
            return true;
        }
        // released: drop it
        self.drag = None;
        if moving {
            let visible = self.visible();
            self.order = moved(&self.order, &visible, from, g.slot_at(my));
        }
        false
    }

    pub fn cursor(&self, window: &Window, g: &Geometry) -> Option<CursorStyle> {
        if self.drag.is_some() {
            return Some(CursorStyle::ClosedHand);
        }
        let (mx, my) = super::ui_scale::mouse_pos(window)?;
        g.title_at(mx, my).map(|_| CursorStyle::OpenHand)
    }

    /// The title under the pointer underlined; while dragging, the plot's title outlined, a bar at the gap it will land in
    /// and its name beside the pointer.
    pub fn draw(&self, buf: &mut [u32], stride: usize, window: &Window, g: &Geometry) {
        let Some((mx, my)) = super::ui_scale::mouse_pos(window) else { return };
        match self.drag {
            Some((from, _, true)) if from < g.n() => {
                let (x0, y0, x1, y1) = g.title_rect(from);
                outline_rect_clip(buf, stride, x0 as i32, y0 as i32, x1 as i32, y1 as i32, 0xffcc66);
                let slot = g.slot_at(my);
                let y = (g.top + slot * g.strip_h).saturating_sub(2);
                fill_rect(buf, stride, g.x, y, g.w, 3, 0xffcc66);
                let name = &g.names[from];
                let (bx, by) = (mx as usize + 14, (my as usize).saturating_sub(8));
                let bw = text_w(name, 1.5) as usize + 12;
                fill_rect(buf, stride, bx, by, bw, 18, 0x262636);
                outline_rect_clip(buf, stride, bx as i32, by as i32, (bx + bw) as i32, (by + 18) as i32, 0xffcc66);
                render_text(buf, name, bx + 6, by + 4, stride, 0xffeecc, 1.5);
            }
            Some(_) => {}
            None => {
                if let Some(i) = g.title_at(mx, my) {
                    let (x0, _, x1, y1) = g.title_rect(i);
                    fill_rect(buf, stride, x0 + 4, y1.saturating_sub(2), (x1 - x0).saturating_sub(8), 1, 0x6a9cff);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn moving_a_plot() {
        let all = [0, 1, 2, 3, 4];
        // down: the first plot below the third
        assert_eq!(moved(&all, &all, 0, 3), vec![1, 2, 0, 3, 4]);
        // up: the last plot to the top
        assert_eq!(moved(&all, &all, 4, 0), vec![4, 0, 1, 2, 3]);
        // to the end
        assert_eq!(moved(&all, &all, 1, 5), vec![0, 2, 3, 4, 1]);
        // dropped where it was
        assert_eq!(moved(&all, &all, 2, 2), all.to_vec());
        assert_eq!(moved(&all, &all, 2, 3), all.to_vec());
        // a hidden plot (2) stays after the shown plot before it
        let vis = [0, 1, 3, 4];
        assert_eq!(moved(&all, &vis, 3, 0), vec![4, 0, 1, 2, 3]);
        assert_eq!(moved(&all, &vis, 0, 4), vec![1, 2, 3, 4, 0]);
        // out of range does nothing
        assert_eq!(moved(&all, &all, 9, 0), all.to_vec());
    }

    #[test]
    fn visible_follows_order_and_shown() {
        let mut s = StripOrder::new(4);
        s.order = vec![3, 1, 0, 2];
        s.shown[1] = false;
        assert_eq!(s.visible(), vec![3, 0, 2]);
        assert!(!s.is_default_order());
        s.reset();
        assert_eq!(s.visible(), vec![0, 1, 2, 3]);
    }
}
