//! UI scale of the post-sim viewers. Each frame is drawn at a logical resolution (the window's pixels over the scale) and
//! minifb stretches it to the window, so text, map, sprites and layout grow together and the viewer looks the same on a
//! 1080p and a 4K monitor.
//!
//! The scale follows the monitor holding most of the window: its shorter side over 1080 px, rounded to a whole number
//! (minifb stretches nearest-neighbour, so whole numbers keep the bitmap font crisp): 1x on 1080p/1200p, 2x on 4K.
//! `XRSIM_VIZ_SCALE` overrides it for every monitor (`1.5`) or per xrandr monitor name (`HDMI-1:1.5,eDP-1:1`); `,` and `.`
//! change it live for the monitor the window is on, and `U` gives that monitor back its default.

use std::cell::Cell;
use std::time::{Duration, Instant};
use minifb::{Key, KeyRepeat, MouseMode};
use super::ui_input::UiInput;

/// A logical frame narrower or shorter than this squeezes the layout (panels overlap below ~760 px high), so the scale
/// gives way.
const MIN_LOGICAL_W: f32 = 900.0;
const MIN_LOGICAL_H: f32 = 760.0;
const STEP: f32 = 0.25;
const MAX_SCALE: f32 = 4.0;
/// How often the window's position is checked against the monitors (two X round trips each).
const PROBE_EVERY: Duration = Duration::from_millis(250);

thread_local! {
    // each viewer runs on its own thread, so each keeps its own scale for `mouse_pos`
    static SCALE: Cell<f32> = Cell::new(1.0);
    static SOURCE: Cell<&'static str> = Cell::new("auto");
    // left edge of the panel being handled, for a panel drawn at x = 0 and shown further right (the channel side when the
    // sides are swapped): `mouse_pos` reports x from there
    static ORIGIN_X: Cell<f32> = Cell::new(0.0);
}

/// The scale of the current frame.
pub fn current() -> f32 {
    SCALE.with(|s| s.get())
}

/// The scale and where it came from, for the HUD: `UI 2x auto`, `UI 1.5x env`, `UI 1.25x set`.
pub fn label() -> String {
    let s = current();
    let num = if s.fract() == 0.0 { format!("{}", s as u32) } else { format!("{s}") };
    format!("UI {num}x {}", SOURCE.with(|c| c.get()))
}

/// Sets the x that `mouse_pos` and `mouse_pos_any` count from (0: the window's left edge).
pub fn set_origin_x(x: f32) {
    ORIGIN_X.with(|o| o.set(x));
}

/// minifb's mouse position, in the logical pixels the frame is drawn in.
pub fn mouse_pos<W: UiInput + ?Sized>(window: &W) -> Option<(f32, f32)> {
    let (s, o) = (current(), ORIGIN_X.with(|o| o.get()));
    window.get_mouse_pos(MouseMode::Discard).map(|(x, y)| (x / s - o, y / s))
}

/// Like `mouse_pos`, but still reports the pointer while a drag takes it outside the window (possibly negative or past the
/// edge), so a drag can be clamped instead of losing the mouse.
pub fn mouse_pos_any<W: UiInput + ?Sized>(window: &W) -> Option<(f32, f32)> {
    let (s, o) = (current(), ORIGIN_X.with(|o| o.get()));
    window.get_mouse_pos(MouseMode::Pass).map(|(x, y)| (x / s - o, y / s))
}

/// The wanted scale `s` held to what a `ww` x `wh` window can show: whole steps down to 1x; a window smaller than the layout
/// at 1x is drawn at the layout's minimum size and shrunk to fit (a fractional scale), so it never goes blank.
fn fit_scale(s: f32, ww: usize, wh: usize) -> f32 {
    let cap = (ww as f32 / MIN_LOGICAL_W).min(wh as f32 / MIN_LOGICAL_H);
    if cap < 1.0 { cap.max(0.05) } else { s.min((cap / STEP).floor() * STEP).max(1.0) }
}

#[derive(Debug, Clone, PartialEq)]
struct Monitor {
    name: String,
    x: i32,
    y: i32,
    w: i32,
    h: i32,
}

impl Monitor {
    /// Pixels of the rectangle (x, y, w, h) on this monitor.
    fn overlap(&self, (x, y, w, h): (i32, i32, i32, i32)) -> i64 {
        let ow = ((x + w).min(self.x + self.w) - x.max(self.x)).max(0) as i64;
        let oh = ((y + h).min(self.y + self.h) - y.max(self.y)).max(0) as i64;
        ow * oh
    }
    fn auto_scale(&self) -> f32 {
        (self.w.min(self.h) as f32 / 1080.0).round().max(1.0)
    }
}

/// One line of `xrandr --listmonitors`: ` 0: +*eDP-1 1920/344x1200/215+0+960  eDP-1`.
fn parse_monitor(line: &str) -> Option<Monitor> {
    let mut it = line.split_whitespace();
    it.next()?.strip_suffix(':')?;
    it.next()?; // +*eDP-1
    let geom = it.next()?;
    let name = it.next()?.to_string();
    let (size, pos) = geom.split_once('+')?;
    let (ws, hs) = size.split_once('x')?;
    let (xs, ys) = pos.split_once('+')?;
    Some(Monitor {
        name,
        x: xs.parse().ok()?,
        y: ys.parse().ok()?,
        w: ws.split('/').next()?.parse().ok()?,
        h: hs.split('/').next()?.parse().ok()?,
    })
}

/// The monitors as X sees them; none off X11 (Wayland, no xrandr), where the scale stays at the override or 1.
fn list_monitors() -> Vec<Monitor> {
    let Ok(out) = std::process::Command::new("xrandr").arg("--listmonitors").output() else { return Vec::new() };
    String::from_utf8_lossy(&out.stdout).lines().filter_map(parse_monitor).collect()
}

/// `XRSIM_VIZ_SCALE`: a bare number for every monitor, `name:number` for one; bad entries are ignored.
fn parse_overrides(spec: &str) -> (Option<f32>, Vec<(String, f32)>) {
    let ok = |v: &str| v.trim().parse::<f32>().ok().filter(|s| s.is_finite() && *s >= 1.0).map(|s| s.min(MAX_SCALE));
    let (mut all, mut named) = (None, Vec::new());
    for entry in spec.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        match entry.rsplit_once(':') {
            Some((name, v)) => if let Some(s) = ok(v) { named.push((name.trim().to_string(), s)) },
            None => if let Some(s) = ok(entry) { all = Some(s) },
        }
    }
    (all, named)
}

/// Per-window scale state; call `frame` once per frame, before laying out.
pub struct UiScale {
    monitors: Vec<Monitor>,
    env_all: Option<f32>,
    env_named: Vec<(String, f32)>,
    /// `,`/`.` choices, per monitor name ("" when the monitor is unknown)
    live: Vec<(String, f32)>,
    monitor: Option<usize>,
    last_probe: Option<Instant>,
    /// A step (+1 / -1) or a return to the default asked for from the menu, applied by the next `frame`.
    menu_step: i32,
    menu_default: bool,
}

impl UiScale {
    pub fn new() -> Self {
        let (env_all, env_named) = parse_overrides(&std::env::var("XRSIM_VIZ_SCALE").unwrap_or_default());
        UiScale { monitors: list_monitors(), env_all, env_named, live: Vec::new(), monitor: None, last_probe: None,
                  menu_step: 0, menu_default: false }
    }

    /// Reads the scale keys, finds the monitor under the window, and returns the logical frame size to draw (and to
    /// hand to `update_with_buffer`) for the window's current size.
    pub fn frame<W: UiInput + ?Sized>(&mut self, window: &W) -> (usize, usize) {
        let (ww, wh) = window.get_size();
        if self.last_probe.map_or(true, |t| t.elapsed() >= PROBE_EVERY) {
            self.last_probe = Some(Instant::now());
            let (px, py) = window.get_position();
            let rect = (px as i32, py as i32, ww as i32, wh as i32);
            // the monitor holding most of the window (its centre can sit off-screen); the last one while it is on none
            let best = self.monitors.iter().enumerate().map(|(i, m)| (i, m.overlap(rect))).filter(|&(_, a)| a > 0)
                .max_by_key(|&(_, a)| a).map(|(i, _)| i);
            if best.is_some() { self.monitor = best; }
        }
        let name = self.monitor.map_or(String::new(), |i| self.monitors[i].name.clone());

        let (mut s, mut source) = self.requested(&name);
        let menu_step = std::mem::take(&mut self.menu_step);
        let step = if window.is_key_pressed(Key::Period, KeyRepeat::Yes) || menu_step > 0 { STEP }
                   else if window.is_key_pressed(Key::Comma, KeyRepeat::Yes) || menu_step < 0 { -STEP }
                   else { 0.0 };
        if step != 0.0 {
            // step from the scale on screen, so a press right after the size guard kicked in is not swallowed
            let next = ((current().min(s) + step) / STEP).round() * STEP;
            s = next.clamp(1.0, MAX_SCALE);
            self.live.retain(|(n, _)| *n != name);
            self.live.push((name.clone(), s));
            source = "set";
        }
        if window.is_key_pressed(Key::U, KeyRepeat::No) || std::mem::take(&mut self.menu_default) {
            self.live.retain(|(n, _)| *n != name);
            (s, source) = self.requested(&name);
        }

        let s = fit_scale(s, ww, wh);
        SCALE.with(|c| c.set(s));
        SOURCE.with(|c| c.set(source));
        (((ww as f32 / s).round() as usize).max(1), ((wh as f32 / s).round() as usize).max(1))
    }

    /// One step larger (`up`) or smaller, as `.` and `,` do, from the next frame on.
    pub fn menu_step(&mut self, up: bool) {
        self.menu_step = if up { 1 } else { -1 };
    }

    /// Back to the monitor's default scale, as `U` does, from the next frame on.
    pub fn menu_default(&mut self) {
        self.menu_default = true;
    }

    /// The scale asked for on monitor `name`: a live choice, then the env (by name, then for all), then the default.
    fn requested(&self, name: &str) -> (f32, &'static str) {
        if let Some(&(_, s)) = self.live.iter().find(|(n, _)| n == name) { return (s, "set"); }
        if let Some(&(_, s)) = self.env_named.iter().find(|(n, _)| n == name) { return (s, "env"); }
        if let Some(s) = self.env_all { return (s, "env"); }
        (self.monitor.map_or(1.0, |i| self.monitors[i].auto_scale()), "auto")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_xrandr_monitors() {
        let m = parse_monitor(" 1: +HDMI-1 3840/621x2160/341+1920+0  HDMI-1").unwrap();
        assert_eq!(m, Monitor { name: "HDMI-1".into(), x: 1920, y: 0, w: 3840, h: 2160 });
        assert_eq!(m.auto_scale(), 2.0);
        let l = parse_monitor(" 0: +*eDP-1 1920/344x1200/215+0+960  eDP-1").unwrap();
        assert_eq!((l.x, l.y, l.auto_scale()), (0, 960, 1.0));
        // the window seen above: centre below both monitors, mostly on the 4K one
        let win = (1668, 1533, 3272, 1517);
        assert!(m.overlap(win) > l.overlap(win) && l.overlap(win) > 0);
        assert_eq!(l.overlap((0, 0, 100, 900)), 0);
        assert!(parse_monitor("Monitors: 2").is_none());
    }

    #[test]
    fn small_windows_keep_a_minimum_logical_frame() {
        for (ww, wh) in [(0usize, 0usize), (1, 1), (300, 200), (899, 2000), (2000, 300), (900, 760), (3840, 2160)] {
            let s = fit_scale(2.0, ww, wh);
            assert!(s.is_finite() && s > 0.0);
            let (lw, lh) = ((ww as f32 / s).round(), (wh as f32 / s).round());
            if ww >= 45 && wh >= 38 {
                assert!(lw >= 880.0 && lh >= 380.0, "{ww}x{wh} -> {lw}x{lh}");
            }
        }
        assert_eq!(fit_scale(2.0, 3840, 2160), 2.0);
    }

    #[test]
    fn parses_overrides() {
        assert_eq!(parse_overrides("1.5"), (Some(1.5), vec![]));
        assert_eq!(parse_overrides("HDMI-1:2, eDP-1:1 ,junk,0.5"), (None, vec![("HDMI-1".into(), 2.0), ("eDP-1".into(), 1.0)]));
        assert_eq!(parse_overrides(""), (None, vec![]));
    }
}
