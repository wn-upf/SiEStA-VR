//! Windows-style menu bar: a row of titles across the top of the window. A click on a title opens its menu; moving over
//! another title while one is open switches to it; a click on an item runs it and closes the menu; a click anywhere else,
//! or Esc, closes it. The bar keeps the mouse from a press it took until the button is released, so that click never also
//! pans, zooms or moves the cursor in the panels underneath.
//!
//! A drop-down taller than the room below the bar is laid out in side-by-side columns, split only between sections (see
//! `layout`); one that does not fit even so (a window too narrow for the columns) scrolls with the wheel.

use minifb::{Key, KeyRepeat, MouseButton};
use super::ui_input::UiInput;
use crate::lib::render_text;
use super::{fill_rect, outline_rect_clip, text_w};

/// Height of the bar (logical px).
pub const BAR_H: usize = 24;
const TITLE_SCALE: f32 = 1.5;
const ITEM_SCALE: f32 = 1.5;
const HEADER_SCALE: f32 = 1.0;
const ITEM_H: usize = 22;
const HEADER_H: usize = 18;
const SEP_H: usize = 9;
const TITLE_PAD: usize = 10;
/// Room left of an item's label for its tick or dot.
const GUTTER: usize = 26;

const BAR_BG: u32 = 0x1a1a24;
const MENU_BG: u32 = 0x1b1b28;
const HOVER_BG: u32 = 0x2c3a66;
const BORDER: u32 = 0x5566aa;
const INK: u32 = 0xe6e9f0;
const INK_OFF: u32 = 0x5a5f70;
const KEY_INK: u32 = 0x8899aa;
const TICK: u32 = 0x88dd88;

pub enum Kind {
    Action,
    /// A switch, ticked when on.
    Check(bool),
    /// One of a group of choices, dotted when chosen.
    Radio(bool),
    /// A small caption over the items after it.
    Header,
    Separator,
}

pub struct Item<C> {
    pub label: String,
    /// Its keyboard shortcut, shown right-aligned ("" for none).
    pub key: &'static str,
    pub kind: Kind,
    pub enabled: bool,
    pub cmd: Option<C>,
}

impl<C> Item<C> {
    pub fn action(label: impl Into<String>, key: &'static str, cmd: C) -> Self {
        Item { label: label.into(), key, kind: Kind::Action, enabled: true, cmd: Some(cmd) }
    }
    pub fn check(label: impl Into<String>, key: &'static str, on: bool, cmd: C) -> Self {
        Item { label: label.into(), key, kind: Kind::Check(on), enabled: true, cmd: Some(cmd) }
    }
    pub fn radio(label: impl Into<String>, key: &'static str, on: bool, cmd: C) -> Self {
        Item { label: label.into(), key, kind: Kind::Radio(on), enabled: true, cmd: Some(cmd) }
    }
    pub fn header(label: impl Into<String>) -> Self {
        Item { label: label.into(), key: "", kind: Kind::Header, enabled: false, cmd: None }
    }
    pub fn sep() -> Self {
        Item { label: String::new(), key: "", kind: Kind::Separator, enabled: false, cmd: None }
    }
    /// Greyed out and inert unless `on`.
    pub fn enabled(mut self, on: bool) -> Self {
        self.enabled = on;
        self
    }
    fn height(&self) -> usize {
        match self.kind {
            Kind::Separator => SEP_H,
            Kind::Header => HEADER_H,
            _ => ITEM_H,
        }
    }
    fn active(&self) -> bool {
        self.enabled && self.cmd.is_some()
    }
}

pub struct Menu<C> {
    pub title: &'static str,
    pub items: Vec<Item<C>>,
}

/// What the bar did with this frame's input.
pub struct Output<C> {
    /// The item clicked.
    pub cmd: Option<C>,
    /// The mouse and keyboard belong to the bar this frame (a menu is open or the press it took is still held): the
    /// panels must not see them.
    pub captured: bool,
    /// Esc closed a menu (so it must not also quit).
    pub esc_used: bool,
}

#[derive(Default)]
pub struct MenuBar {
    open: Option<usize>,
    holding: bool,
    prev_left: bool,
    /// Rows of the frame last drawn (0 before the first): `input` lays the drop-down out for the frame on screen.
    rows: std::cell::Cell<usize>,
    /// The first item shown of a drop-down that scrolls.
    first: usize,
}

/// (x0, x1) of each title.
fn title_rects<C>(menus: &[Menu<C>]) -> Vec<(usize, usize)> {
    let mut x = 4;
    menus.iter().map(|m| {
        let w = text_w(m.title, TITLE_SCALE) as usize + 2 * TITLE_PAD;
        let r = (x, x + w);
        x += w;
        r
    }).collect()
}

/// (x, y, w, h) in logical px.
type Rect = (usize, usize, usize, usize);

fn inside((x, y, w, h): Rect, mx: f32, my: f32) -> bool {
    mx >= x as f32 && mx < (x + w) as f32 && my >= y as f32 && my < (y + h) as f32
}

/// Where an open drop-down and each of its items go. Drawing and hit-testing both read it, so a click always finds what
/// was drawn under it.
#[derive(Debug)]
struct DropLayout {
    frame: Rect,
    /// Each item's rectangle; `None` for one not drawn (a separator that ends a column, an item scrolled out of view).
    items: Vec<Option<Rect>>,
    /// x of the line between each pair of side-by-side columns.
    dividers: Vec<usize>,
    /// Set when the list scrolls.
    scroll: Option<Scroll>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Scroll {
    /// The first item shown, and the first one when scrolled to the end.
    first: usize,
    last_first: usize,
    /// Height of all the items, and of those shown.
    total_h: usize,
    shown_h: usize,
}

impl DropLayout {
    fn contains(&self, mx: f32, my: f32) -> bool {
        inside(self.frame, mx, my)
    }

    /// The item under the mouse.
    fn item_at(&self, mx: f32, my: f32) -> Option<usize> {
        if !self.contains(mx, my) {
            return None;
        }
        self.items.iter().position(|r| r.is_some_and(|r| inside(r, mx, my)))
    }
}

/// Items a wheel notch scrolls.
const WHEEL_ITEMS: usize = 3;

/// The width a one-column drop-down has always had (every label measured at the item scale, a caption's too).
fn one_column_w<C>(items: &[Item<C>]) -> usize {
    items.iter().map(|it| {
        let key = if it.key.is_empty() { 0 } else { text_w(it.key, ITEM_SCALE) as usize + 28 };
        GUTTER + text_w(&it.label, ITEM_SCALE) as usize + key + 16
    }).max().unwrap_or(120).max(180)
}

/// The width of a column: its widest item as drawn (a caption at its own smaller scale, with room for its rule).
fn column_w<C>(items: &[Item<C>]) -> usize {
    items.iter().map(|it| match it.kind {
        Kind::Separator => 0,
        Kind::Header => 16 + text_w(&it.label, HEADER_SCALE) as usize + 24,
        _ => {
            let key = if it.key.is_empty() { 0 } else { text_w(it.key, ITEM_SCALE) as usize + 28 };
            GUTTER + text_w(&it.label, ITEM_SCALE) as usize + key + 16
        }
    }).max().unwrap_or(120).max(180)
}

/// Splits a drop-down's items into the fewest columns no taller than `cap`, breaking only between sections (before a
/// caption or after a separator), as evenly as the sections allow. Each column is a range of item indices; a separator
/// that ends a column is not counted (it is not drawn). `None` when one section alone is taller than `cap`.
fn columns<C>(items: &[Item<C>], cap: usize) -> Option<Vec<std::ops::Range<usize>>> {
    let n = items.len();
    let is_sep = |k: usize| matches!(items[k].kind, Kind::Separator);
    // where each section starts, then the end
    let mut starts: Vec<usize> = (0..n).filter(|&k| k == 0 || matches!(items[k].kind, Kind::Header) || is_sep(k - 1)).collect();
    starts.push(n);
    let sections = starts.len() - 1;
    let heights: Vec<usize> = items.iter().map(Item::height).collect();
    // a column holding sections a..b
    let col_h = |a: usize, b: usize| {
        let (s, e) = (starts[a], starts[b]);
        heights[s..e].iter().sum::<usize>() - if is_sep(e - 1) { SEP_H } else { 0 }
    };
    // left to right, each column taking sections while it stays within `cap`
    let fill = |cap: usize| -> Option<Vec<(usize, usize)>> {
        let mut cols = Vec::new();
        let mut a = 0;
        while a < sections {
            if col_h(a, a + 1) > cap {
                return None;
            }
            let mut b = a + 1;
            while b < sections && col_h(a, b + 1) <= cap {
                b += 1;
            }
            cols.push((a, b));
            a = b;
        }
        Some(cols)
    };
    let fewest = fill(cap)?;
    // the most even split: the lowest height any run of sections has that still needs no more columns
    let mut tried: Vec<usize> = Vec::new();
    for a in 0..sections {
        for b in a + 1..=sections {
            if col_h(a, b) <= cap {
                tried.push(col_h(a, b));
            }
        }
    }
    tried.sort_unstable();
    tried.dedup();
    let even = tried.into_iter().find_map(|h| fill(h).filter(|c| c.len() <= fewest.len())).unwrap_or(fewest);
    Some(even.into_iter().map(|(a, b)| starts[a]..starts[b]).collect())
}

/// The layout of menu `i`'s drop-down in a window `stride` wide and `rows` high (0: unknown, no limit), with the list
/// scrolled to item `first` if it scrolls.
///
/// A drop-down that fits below the bar is one column, as it always was. A taller one is laid out in side-by-side
/// columns, broken only between sections (before a caption or after a separator), the fewest that fit, as even as the
/// sections allow and each as wide as the widest item; it moves left when the columns would run past the right edge. A
/// list that fits neither way (a window too narrow for the columns, or one section taller than the window) is one column
/// cut at the window's bottom that scrolls with the wheel.
fn layout<C>(menus: &[Menu<C>], i: usize, stride: usize, rows: usize, first: usize) -> DropLayout {
    let items = &menus[i].items;
    let x0 = title_rects(menus)[i].0;
    // kept inside the window
    let place = |w: usize| x0.min(stride.saturating_sub(w + 2));
    let room = if rows == 0 { usize::MAX } else { rows.saturating_sub(BAR_H) };
    let heights: Vec<usize> = items.iter().map(Item::height).collect();
    let total_h: usize = heights.iter().sum();
    let top = BAR_H + 4;
    if items.is_empty() || total_h + 8 <= room {
        let (w, mut iy) = (one_column_w(items), top);
        let x = place(w);
        let rects = heights.iter().map(|&h| {
            iy += h;
            Some((x, iy - h, w, h))
        }).collect();
        return DropLayout { frame: (x, BAR_H, w, total_h + 8), items: rects, dividers: Vec::new(), scroll: None };
    }
    let col_w = column_w(items);
    let cap = room.saturating_sub(8);
    if let Some(cols) = columns(items, cap).filter(|c| c.len() * col_w + 2 <= stride) {
        let w = cols.len() * col_w;
        let x = place(w);
        let mut rects = vec![None; items.len()];
        let mut tallest = 0;
        for (c, range) in cols.iter().enumerate() {
            let (cx, mut iy) = (x + c * col_w, top);
            for k in range.clone() {
                if k + 1 == range.end && matches!(items[k].kind, Kind::Separator) {
                    continue;
                }
                rects[k] = Some((cx, iy, col_w, heights[k]));
                iy += heights[k];
            }
            tallest = tallest.max(iy - top);
        }
        let dividers = (1..cols.len()).map(|c| x + c * col_w).collect();
        return DropLayout { frame: (x, BAR_H, w, tallest + 8), items: rects, dividers, scroll: None };
    }
    // the last resort: one column down to the window's bottom, scrolled
    let x = place(col_w);
    let mut last_first = items.len();
    let mut tail = 0;
    while last_first > 0 && tail + heights[last_first - 1] <= cap {
        last_first -= 1;
        tail += heights[last_first];
    }
    let last_first = last_first.min(items.len() - 1);
    let first = first.min(last_first);
    let mut rects = vec![None; items.len()];
    let mut iy = top;
    for k in first..items.len() {
        if iy + heights[k] > top + cap {
            break;
        }
        rects[k] = Some((x, iy, col_w, heights[k]));
        iy += heights[k];
    }
    let scroll = Scroll { first, last_first, total_h, shown_h: iy - top };
    DropLayout { frame: (x, BAR_H, col_w, room), items: rects, dividers: Vec::new(), scroll: Some(scroll) }
}

impl MenuBar {
    pub fn is_open(&self) -> bool {
        self.open.is_some()
    }

    pub fn close(&mut self) {
        self.open = None;
    }

    /// Opens menu `i` (none: closes it), as a click on its title would (the screenshots of `XRSIM_VIZ_TOUR=all`).
    pub fn open_at(&mut self, i: Option<usize>) {
        self.open = i;
        self.first = 0;
    }

    pub fn input<C: Copy, W: UiInput + ?Sized>(&mut self, window: &W, menus: &[Menu<C>], stride: usize) -> Output<C> {
        let (mx, my) = super::ui_scale::mouse_pos(window).unwrap_or((-1.0, -1.0));
        let left = window.get_mouse_down(MouseButton::Left);
        let pressed = left && !self.prev_left;
        self.prev_left = left;
        if self.open.is_some_and(|o| o >= menus.len()) {
            self.open = None;
        }
        let was_open = self.open;
        let titles = title_rects(menus);
        let over_title = if my >= 0.0 && my < BAR_H as f32 {
            titles.iter().position(|&(a, b)| mx >= a as f32 && mx < b as f32)
        } else {
            None
        };
        let mut out = Output { cmd: None, captured: false, esc_used: false };
        if self.open.is_some() && window.is_key_pressed(Key::Escape, KeyRepeat::No) {
            self.open = None;
            out.esc_used = true;
        }
        // as in Windows: with a menu open, the pointer moving onto another title opens that one
        if let (Some(o), Some(t)) = (self.open, over_title) {
            if o != t {
                self.open = Some(t);
            }
        }
        if pressed {
            if let Some(t) = over_title {
                self.open = if self.open == Some(t) { None } else { Some(t) };
                self.holding = true;
            } else if let Some(o) = self.open {
                let drop = layout(menus, o, stride, self.rows.get(), self.first);
                match drop.item_at(mx, my) {
                    Some(k) if menus[o].items[k].active() => {
                        out.cmd = menus[o].items[k].cmd;
                        self.open = None;
                    }
                    // a caption, a separator or a greyed item keeps the menu open
                    Some(_) => {}
                    // so does the drop-down's margin, or the space under a shorter column
                    None if drop.contains(mx, my) => {}
                    None => self.open = None,
                }
                self.holding = true;
            } else if my >= 0.0 && my < BAR_H as f32 {
                self.holding = true;
            }
        }
        if self.open != was_open {
            self.first = 0;
        } else if let Some(o) = self.open {
            // a drop-down too tall for the window even in columns scrolls with the wheel over it
            let drop = layout(menus, o, stride, self.rows.get(), self.first);
            if let (Some(s), Some((_, dy))) = (drop.scroll, window.get_scroll_wheel()) {
                if drop.contains(mx, my) && dy != 0.0 {
                    self.first = if dy > 0.0 { s.first.saturating_sub(WHEEL_ITEMS) } else { (s.first + WHEEL_ITEMS).min(s.last_first) };
                }
            }
        }
        if !left {
            self.holding = false;
        }
        out.captured = self.holding || self.open.is_some();
        out
    }

    /// The bar: titles on the left (the open one lit, the one under the pointer shaded), `status` right-aligned when it
    /// fits.
    pub fn draw_bar<C, W: UiInput + ?Sized>(&self, buf: &mut [u32], stride: usize, window: &W, menus: &[Menu<C>], status: &str) {
        self.rows.set(buf.len() / stride.max(1));
        fill_rect(buf, stride, 0, 0, stride, BAR_H, BAR_BG);
        fill_rect(buf, stride, 0, BAR_H - 1, stride, 1, 0x2a2a40);
        let (mx, my) = super::ui_scale::mouse_pos(window).unwrap_or((-1.0, -1.0));
        let titles = title_rects(menus);
        let text_h = (7.0 * TITLE_SCALE).round() as usize;
        for (i, (m, &(x0, x1))) in menus.iter().zip(&titles).enumerate() {
            let hover = my >= 0.0 && my < BAR_H as f32 && mx >= x0 as f32 && mx < x1 as f32;
            if self.open == Some(i) {
                fill_rect(buf, stride, x0, 2, x1 - x0, BAR_H - 3, HOVER_BG);
            } else if hover {
                fill_rect(buf, stride, x0, 2, x1 - x0, BAR_H - 3, 0x262638);
            }
            render_text(buf, m.title, x0 + TITLE_PAD, (BAR_H - text_h) / 2, stride, INK, TITLE_SCALE);
        }
        let end = titles.last().map_or(0, |r| r.1) + 24;
        let sw = text_w(status, 1.0) as usize;
        if !status.is_empty() && end + sw + 10 <= stride {
            render_text(buf, status, stride - sw - 10, (BAR_H - 7) / 2, stride, 0xb8c0cc, 1.0);
        }
    }

    /// The open menu's drop-down, over everything else.
    pub fn draw_menu<C, W: UiInput + ?Sized>(&self, buf: &mut [u32], stride: usize, window: &W, menus: &[Menu<C>]) {
        self.rows.set(buf.len() / stride.max(1));
        let Some(o) = self.open.filter(|&o| o < menus.len()) else { return };
        let drop = layout(menus, o, stride, self.rows.get(), self.first);
        let (x, y, w, h) = drop.frame;
        let (mx, my) = super::ui_scale::mouse_pos(window).unwrap_or((-1.0, -1.0));
        let hovered = drop.item_at(mx, my);
        // a soft shadow right and below
        shade(buf, stride, x + 4, y + h, w, 4);
        shade(buf, stride, x + w, y + 4, 4, h);
        fill_rect(buf, stride, x, y, w, h, MENU_BG);
        outline_rect_clip(buf, stride, x as i32, y as i32, (x + w) as i32, (y + h) as i32, BORDER);
        for &dx in &drop.dividers {
            fill_rect(buf, stride, dx, y + 6, 1, h.saturating_sub(12), 0x34344c);
        }
        let text_h = (7.0 * ITEM_SCALE).round() as usize;
        for (k, (it, r)) in menus[o].items.iter().zip(&drop.items).enumerate() {
            // each item in its own rectangle: its column's x and width
            let Some((x, iy, w, ih)) = *r else { continue };
            match it.kind {
                Kind::Separator => fill_rect(buf, stride, x + 8, iy + SEP_H / 2, w - 16, 1, 0x34344c),
                Kind::Header => {
                    render_text(buf, &it.label, x + 10, iy + 6, stride, 0x7f8cb0, HEADER_SCALE);
                    let tx = x + 16 + text_w(&it.label, HEADER_SCALE) as usize;
                    fill_rect(buf, stride, tx, iy + 9, (x + w).saturating_sub(tx + 10), 1, 0x2a2a40);
                }
                Kind::Action | Kind::Check(_) | Kind::Radio(_) => {
                    if hovered == Some(k) && it.active() {
                        fill_rect(buf, stride, x + 2, iy, w - 4, ih, HOVER_BG);
                    }
                    let ink = if it.enabled { INK } else { INK_OFF };
                    let ty = iy + (ih - text_h) / 2;
                    match it.kind {
                        Kind::Check(on) => draw_check(buf, stride, x + 9, iy + (ih - 11) / 2, on, it.enabled),
                        Kind::Radio(on) => draw_radio(buf, stride, x + 9, iy + (ih - 11) / 2, on, it.enabled),
                        _ => {}
                    }
                    render_text(buf, &it.label, x + GUTTER, ty, stride, ink, ITEM_SCALE);
                    if !it.key.is_empty() {
                        let kw = text_w(it.key, ITEM_SCALE) as usize;
                        render_text(buf, it.key, x + w - kw - 12, ty, stride, if it.enabled { KEY_INK } else { INK_OFF }, ITEM_SCALE);
                    }
                }
            }
        }
        // a scrolled list: a bar on its right edge, its thumb the part shown
        if let Some(s) = drop.scroll {
            let track = h.saturating_sub(8);
            let thumb = (track * s.shown_h / s.total_h.max(1)).max(12).min(track);
            let ty = y + 4 + if s.last_first == 0 { 0 } else { (track - thumb) * s.first / s.last_first };
            fill_rect(buf, stride, x + w - 6, y + 4, 3, track, 0x262638);
            fill_rect(buf, stride, x + w - 6, ty, 3, thumb, BORDER);
        }
    }
}

/// Darkens a rectangle (a drop shadow).
fn shade(buf: &mut [u32], stride: usize, x: usize, y: usize, w: usize, h: usize) {
    let rows = buf.len() / stride.max(1);
    for yy in y..(y + h).min(rows) {
        for xx in x..(x + w).min(stride) {
            let c = &mut buf[yy * stride + xx];
            *c = (*c >> 1) & 0x7f7f7f;
        }
    }
}

/// An 11 x 11 box, ticked when `on`.
fn draw_check(buf: &mut [u32], stride: usize, x: usize, y: usize, on: bool, enabled: bool) {
    let frame = if enabled { 0x66708a } else { 0x3a3e4c };
    outline_rect_clip(buf, stride, x as i32, y as i32, x as i32 + 10, y as i32 + 10, frame);
    if on {
        let c = if enabled { TICK } else { INK_OFF };
        // a tick: down from (2,5) to (4,8), then up to (8,2), two px thick
        for (dx, dy) in [(2, 5), (3, 6), (4, 7), (5, 6), (6, 5), (7, 4), (8, 3)] {
            fill_rect(buf, stride, x + dx, y + dy, 1, 2, c);
        }
    }
}

/// An 11 x 11 ring, dotted when `on`.
fn draw_radio(buf: &mut [u32], stride: usize, x: usize, y: usize, on: bool, enabled: bool) {
    let frame = if enabled { 0x66708a } else { 0x3a3e4c };
    let (cx, cy) = (x as i32 + 5, y as i32 + 5);
    for dy in -5i32..=5 {
        for dx in -5i32..=5 {
            let d2 = dx * dx + dy * dy;
            let c = if (17..=30).contains(&d2) { Some(frame) } else if on && d2 <= 5 { Some(if enabled { TICK } else { INK_OFF }) } else { None };
            if let Some(c) = c {
                fill_rect(buf, stride, (cx + dx) as usize, (cy + dy) as usize, 1, 1, c);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn menus() -> Vec<Menu<u8>> {
        vec![
            Menu { title: "File", items: vec![Item::action("Quit", "Esc", 1)] },
            Menu { title: "View", items: vec![Item::header("Sides"), Item::check("Channel", "", true, 2), Item::sep(),
                                              Item::check("Packet arrivals", "I", false, 3).enabled(false)] },
        ]
    }

    #[test]
    fn titles_follow_each_other() {
        let m = menus();
        let t = title_rects(&m);
        assert_eq!(t.len(), 2);
        assert!(t[0].1 == t[1].0 && t[0].0 < t[0].1);
    }

    #[test]
    fn items_are_found_by_height_and_kept_in_the_window() {
        let m = menus();
        let at = |stride: usize, mx: f32, my: f32| layout(&m, 1, stride, 1000, 0).item_at(mx, my);
        let (x, y, w, h) = layout(&m, 1, 2000, 1000, 0).frame;
        assert_eq!(y, BAR_H);
        assert_eq!(h, HEADER_H + ITEM_H + SEP_H + ITEM_H + 8);
        let px = (x + 20) as f32;
        assert_eq!(at(2000, px, (y + 4 + 2) as f32), Some(0));
        assert_eq!(at(2000, px, (y + 4 + HEADER_H + 1) as f32), Some(1));
        assert_eq!(at(2000, px, (y + 4 + HEADER_H + ITEM_H + SEP_H + 1) as f32), Some(3));
        assert_eq!(at(2000, (x + w + 1) as f32, (y + 30) as f32), None);
        assert!(!m[1].items[3].active() && !m[1].items[0].active() && m[1].items[1].active());
        // a narrow window pulls the drop-down left, never past the right edge
        let (x, _, w, _) = layout(&m, 1, 150, 1000, 0).frame;
        assert!(x + w <= 150 || x == 0);
    }

    /// `sections` sections, each a caption over `per` choices.
    fn long_menu(sections: usize, per: usize) -> Vec<Menu<u8>> {
        let mut items = Vec::new();
        for s in 0..sections {
            items.push(Item::header(format!("Section {s}, with a caption longer than its items")));
            for k in 0..per {
                items.push(Item::radio(format!("Choice {k} of section {s}"), "", k == 0, (s * per + k) as u8));
            }
        }
        vec![Menu { title: "File", items: vec![Item::action("Quit", "Esc", 0)] }, Menu { title: "Codec", items }]
    }

    /// Every drawn item lies inside the window and the drop-down, and a click on its centre finds it.
    fn check_drawn_items_are_found(d: &DropLayout, stride: usize, rows: usize) {
        let (fx, fy, fw, fh) = d.frame;
        assert!(fx + fw <= stride && fy + fh <= rows, "drop-down {:?} outside {stride} x {rows}", d.frame);
        for (k, r) in d.items.iter().enumerate() {
            let Some((x, y, w, h)) = *r else { continue };
            assert!(x >= fx && x + w <= fx + fw && y >= fy && y + h <= fy + fh, "item {k} at {:?} outside {:?}", r, d.frame);
            let (cx, cy) = (x as f32 + w as f32 / 2.0, y as f32 + h as f32 / 2.0);
            assert_eq!(d.item_at(cx, cy), Some(k), "item {k} at {:?}", r);
        }
    }

    #[test]
    fn a_long_menu_is_split_into_even_columns_inside_the_window() {
        // 8 captions and 32 choices (40 items, 848 px) under a 600 px window's bar (576 px)
        let m = long_menu(8, 4);
        let (stride, rows) = (1600, 600);
        let d = layout(&m, 1, stride, rows, 0);
        assert_eq!(m[1].items.len(), 40);
        assert!(d.scroll.is_none());
        assert!(!d.dividers.is_empty(), "one column: {:?}", d.frame);
        assert!(d.items.iter().all(Option::is_some));
        check_drawn_items_are_found(&d, stride, rows);
        // each column starts with a caption (no section is split), and the columns are as even as the sections allow
        let mut xs: Vec<usize> = d.items.iter().flatten().map(|r| r.0).collect();
        xs.dedup();
        assert_eq!(xs.len(), 2);
        for &cx in &xs {
            let k = d.items.iter().position(|r| r.is_some_and(|r| r.0 == cx)).unwrap();
            assert!(matches!(m[1].items[k].kind, Kind::Header));
        }
        let col_h = |cx: usize| d.items.iter().flatten().filter(|r| r.0 == cx).map(|r| r.3).sum::<usize>();
        assert_eq!(col_h(xs[0]), 4 * (HEADER_H + 4 * ITEM_H));
        assert_eq!(col_h(xs[0]), col_h(xs[1]));
        // each column as wide as the widest item, side by side
        assert_eq!(xs[1] - xs[0], column_w(&m[1].items));
        assert_eq!(d.dividers, vec![xs[1]]);
        // the top margin holds no item but is part of the drop-down (a click there keeps it open)
        assert_eq!(d.item_at(d.frame.0 as f32 + 1.0, (BAR_H + 2) as f32), None);
        assert!(d.contains(d.frame.0 as f32 + 1.0, (BAR_H + 2) as f32));
    }

    #[test]
    fn columns_move_left_to_stay_inside_the_window() {
        let mut m = long_menu(8, 4);
        // a menu far to the right on the bar
        m.insert(0, Menu { title: "A rather long title that pushes the next one right", items: vec![Item::action("x", "", 0)] });
        let (stride, rows) = (1100, 600);
        let d = layout(&m, 2, stride, rows, 0);
        assert!(d.scroll.is_none() && d.dividers.len() == 1);
        assert!(d.frame.0 < title_rects(&m)[2].0);
        check_drawn_items_are_found(&d, stride, rows);
    }

    #[test]
    fn a_column_breaks_after_a_separator_and_leaves_it_out() {
        let mut items = Vec::new();
        for s in 0..4u8 {
            for k in 0..8u8 {
                items.push(Item::action(format!("Action {k}"), "", s * 8 + k));
            }
            items.push(Item::sep());
        }
        let m = vec![Menu { title: "Edit", items }];
        // 4 x (8 x 22 + 9) = 740 px; 400 px under the bar
        let d = layout(&m, 0, 1600, 424, 0);
        assert!(d.scroll.is_none() && d.dividers.len() == 1);
        check_drawn_items_are_found(&d, 1600, 424);
        for (k, r) in d.items.iter().enumerate() {
            // the separators that end a column are not drawn; the others are
            assert_eq!(r.is_none(), k == 17 || k == 35, "item {k}");
        }
    }

    #[test]
    fn a_short_menu_is_one_column_as_before() {
        let m = menus();
        let it = &m[1].items;
        let w = it.iter().map(|it| {
            let key = if it.key.is_empty() { 0 } else { text_w(it.key, ITEM_SCALE) as usize + 28 };
            GUTTER + text_w(&it.label, ITEM_SCALE) as usize + key + 16
        }).max().unwrap().max(180);
        for rows in [0, 120, 600, 2000] {
            let d = layout(&m, 1, 2000, rows, 0);
            assert_eq!(d.frame, (title_rects(&m)[1].0, BAR_H, w, HEADER_H + ITEM_H + SEP_H + ITEM_H + 8));
            assert!(d.dividers.is_empty() && d.scroll.is_none());
            let mut y = BAR_H + 4;
            for (r, item) in d.items.iter().zip(it) {
                assert_eq!(*r, Some((d.frame.0, y, w, item.height())));
                y += item.height();
            }
        }
    }

    #[test]
    fn a_window_too_narrow_for_columns_scrolls_the_list() {
        let m = long_menu(8, 4);
        let (stride, rows) = (420, 600);
        let n = m[1].items.len();
        let d = layout(&m, 1, stride, rows, 0);
        let s = d.scroll.expect("scrolls");
        assert_eq!((s.first, d.frame.1 + d.frame.3), (0, rows));
        assert!(d.items[0].is_some() && d.items[n - 1].is_none());
        check_drawn_items_are_found(&d, stride, rows);
        // scrolled past the end: held at the end, the last item shown
        let d = layout(&m, 1, stride, rows, 1000);
        let s = d.scroll.unwrap();
        assert!(s.first == s.last_first && s.first > 0);
        assert!(d.items[n - 1].is_some() && d.items[s.first - 1].is_none());
        check_drawn_items_are_found(&d, stride, rows);
        // one section taller than the window scrolls too, however wide it is
        let d = layout(&long_menu(2, 30), 1, 3000, rows, 0);
        assert!(d.scroll.is_some());
        check_drawn_items_are_found(&d, 3000, rows);
    }
}
