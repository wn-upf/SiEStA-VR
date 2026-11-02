//! Where the unified viewer's two sides go: the channel side and the ABR side, in either order or one alone (View and
//! Layout menus), with the splitter between them when both are shown; and, inside the ABR side, the sidebar (legend and
//! station map) left or right of the plots, or none.

use super::{split_limits, SPLITTER_W};

/// The sides' x extents this frame (logical px). A hidden side has width 0.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Sides {
    pub ch_x0: usize,
    pub ch_w: usize,
    pub abr_x0: usize,
    pub abr_w: usize,
    /// Left edge of the splitter, when both sides are shown.
    pub splitter: Option<usize>,
    /// Where the splitter may go.
    pub limits: (usize, usize),
}

impl Sides {
    pub fn in_ch(&self, x: usize) -> bool {
        self.ch_w > 0 && x >= self.ch_x0 && x < self.ch_x0 + self.ch_w
    }
    pub fn in_abr(&self, x: usize) -> bool {
        self.abr_w > 0 && x >= self.abr_x0 && x < self.abr_x0 + self.abr_w
    }
}

/// The sides of a window `w` wide. `split_frac` places the splitter (the left side's share), held to the limits that keep
/// each side usable; `swapped` puts the ABR side on the left. At least one side is always shown.
pub fn sides(w: usize, split_frac: f32, show_ch: bool, show_abr: bool, swapped: bool) -> Sides {
    let limits = split_limits(w);
    if !show_abr {
        return Sides { ch_x0: 0, ch_w: w, abr_x0: w, abr_w: 0, splitter: None, limits };
    }
    if !show_ch {
        return Sides { ch_x0: w, ch_w: 0, abr_x0: 0, abr_w: w, splitter: None, limits };
    }
    let split = ((w as f32 * split_frac) as usize).clamp(limits.0, limits.1);
    let (right_x0, right_w) = (split + SPLITTER_W, w.saturating_sub(split + SPLITTER_W));
    if swapped {
        Sides { ch_x0: right_x0, ch_w: right_w, abr_x0: 0, abr_w: split, splitter: Some(split), limits }
    } else {
        Sides { ch_x0: 0, ch_w: split, abr_x0: right_x0, abr_w: right_w, splitter: Some(split), limits }
    }
}

/// `split_frac` after swapping the sides, so each side keeps its width.
pub fn mirrored_split(w: usize, split_frac: f32) -> f32 {
    if w == 0 {
        return split_frac;
    }
    let split = w as f32 * split_frac;
    ((w as f32 - split - SPLITTER_W as f32) / w as f32).clamp(0.0, 1.0)
}

/// The ABR side's sidebar and plots (logical px).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AbrInner {
    pub sidebar_x: usize,
    pub sidebar_w: usize,
    pub panel_x: usize,
    pub panel_w: usize,
}

/// The sidebar (`sidebar_w` wide, 0 when hidden) and the plots inside an ABR side at `x0`, `w` wide.
pub fn abr_inner(x0: usize, w: usize, sidebar_w: usize, on_right: bool) -> AbrInner {
    let sw = sidebar_w.min(w);
    if sw == 0 {
        AbrInner { sidebar_x: x0, sidebar_w: 0, panel_x: x0 + 4, panel_w: w.saturating_sub(8) }
    } else if on_right {
        AbrInner { sidebar_x: x0 + w - sw, sidebar_w: sw, panel_x: x0 + 4, panel_w: w.saturating_sub(sw + 8) }
    } else {
        // as before the menus: the plots run to 4 px short of the window's right edge
        AbrInner { sidebar_x: x0, sidebar_w: sw, panel_x: x0 + sw, panel_w: w.saturating_sub(sw + 4) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sides_fit_the_window_and_never_overlap() {
        for w in [880usize, 1000, 1920, 3840] {
            for frac in [0.0f32, 0.2, 0.5, 0.8, 1.0] {
                for (ch, abr) in [(true, true), (true, false), (false, true), (false, false)] {
                    for swapped in [false, true] {
                        let s = sides(w, frac, ch, abr, swapped);
                        assert!(s.ch_x0 + s.ch_w <= w && s.abr_x0 + s.abr_w <= w, "{w} {frac} {ch} {abr} {swapped}: {s:?}");
                        assert!(s.ch_w > 0 || s.abr_w > 0, "both sides hidden");
                        for x in 0..w {
                            assert!(!(s.in_ch(x) && s.in_abr(x)), "x={x} in both: {s:?}");
                        }
                        if let Some(sp) = s.splitter {
                            assert!(!s.in_ch(sp) && !s.in_abr(sp));
                            assert!(s.ch_w.min(s.abr_w) >= 400 - SPLITTER_W, "{s:?}");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn default_layout_is_the_old_one() {
        // what the viewer did before the menus: channel [0, split), splitter, ABR to the right edge
        let w = 1850;
        let s = sides(w, 0.5, true, true, false);
        let split = ((w as f32 * 0.5) as usize).clamp(split_limits(w).0, split_limits(w).1);
        assert_eq!(s, Sides { ch_x0: 0, ch_w: split, abr_x0: split + SPLITTER_W, abr_w: w - split - SPLITTER_W,
                              splitter: Some(split), limits: split_limits(w) });
        let a = abr_inner(s.abr_x0, s.abr_w, 210, false);
        assert_eq!((a.sidebar_x, a.panel_x, a.panel_w), (s.abr_x0, s.abr_x0 + 210, w - (s.abr_x0 + 210 + 4)));
    }

    #[test]
    fn swapping_keeps_each_side_its_width() {
        let w = 1850;
        let s = sides(w, 0.6, true, true, false);
        let t = sides(w, mirrored_split(w, 0.6), true, true, true);
        assert!(s.ch_w.abs_diff(t.ch_w) <= 1 && s.abr_w.abs_diff(t.abr_w) <= 1, "{s:?} {t:?}");
        assert_eq!(t.abr_x0, 0);
    }

    #[test]
    fn sidebar_inside_the_abr_side() {
        for (w, sw) in [(400usize, 210usize), (900, 360), (100, 300), (500, 0)] {
            for on_right in [false, true] {
                let a = abr_inner(50, w, sw, on_right);
                assert!(a.sidebar_x >= 50 && a.sidebar_x + a.sidebar_w <= 50 + w, "{a:?}");
                assert!(a.panel_x >= 50 && a.panel_x + a.panel_w <= 50 + w, "{a:?}");
                if a.sidebar_w > 0 && a.panel_w > 0 {
                    let (s0, s1, p0, p1) = (a.sidebar_x, a.sidebar_x + a.sidebar_w, a.panel_x, a.panel_x + a.panel_w);
                    assert!(s1 <= p0 || p1 <= s0, "sidebar and plots overlap: {a:?}");
                }
            }
        }
    }
}
