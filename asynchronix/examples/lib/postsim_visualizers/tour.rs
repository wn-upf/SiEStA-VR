//! Help overlays of the unified viewer: a quick tour (F1) that dims the window but for one part and explains it in a box
//! beside it, step by step, zooming the channel view into real transmissions of the run (`tour_examples`); the keyboard
//! and mouse sheet (H); and a short About box. Each is modal: while it is open the panels get no input.
//!
//! The steps' text has a little markup: a paragraph per line, `- ` starts a bullet, `` `words` `` are highlighted (keys,
//! names on screen), and `{rrggbb}` is a colour swatch, kept on one line with the word after it.

use minifb::{Key, KeyRepeat, MouseButton, Window};
use crate::lib::render_text;
use super::tour_examples::{Example, Kind};
use super::{fill_rect, outline_rect_clip, text_w};

/// (x, y, w, h), logical px.
pub type Rect = (usize, usize, usize, usize);

/// The parts of the window as drawn this frame; `None` for a part that is hidden or absent from the run.
pub struct Regions {
    pub w: usize,
    pub h: usize,
    pub menu: Rect,
    pub controls: Rect,
    pub lanes: Option<Rect>,
    pub bottom: Option<Rect>,
    pub abr_plots: Option<Rect>,
    pub legend: Option<Rect>,
    pub map: Option<Rect>,
    pub time_axis: Rect,
    /// The backoff (CW) panel alone.
    pub cw: Option<Rect>,
    /// The current step's example (its TXOPs or collision on its lanes), as drawn this frame.
    pub example: Option<Rect>,
}

/// A part a step points at.
#[derive(Clone, Copy)]
enum Spot {
    /// A part of the window (left out when it is not shown).
    Part(fn(&Regions) -> Option<Rect>),
    /// The step's example, as drawn.
    Example,
}

struct Step {
    title: &'static str,
    body: String,
    /// What it points at, the first one most; none: the box in the middle.
    spots: Vec<Spot>,
    /// The example whose view the channel side shows during the step (index into the examples).
    example: Option<usize>,
}

/// The spotlights of a step this frame.
fn resolve(spots: &[Spot], r: &Regions) -> Vec<Rect> {
    spots.iter().filter_map(|s| match s {
        Spot::Part(f) => f(r),
        Spot::Example => r.example.or(r.lanes),
    }).filter(|&(_, _, w, h)| w > 0 && h > 0).collect()
}

const WELCOME: &str = "This viewer replays a finished simulation.\n\
    - The `channel side` shows the Wi-Fi medium: who transmits when, backoff and queues.\n\
    - The `ABR side` shows the video sessions: the bitrate the ABR chose, throughput, delay and losses.\n\
    `Space` or `Next` moves on (`Back` or the left arrow goes back), zooming into real transmissions of this run: point \
    at them for their details, use the wheel to zoom and drag to pan (each step starts from its own view). `Esc` closes \
    the tour and puts your view back.";

const MENU: &str = "- `View` shows or hides a whole side.\n\
    - `Channel` and `ABR` switch single panels on and off; each item shows its key.\n\
    - `Layout` swaps the sides, moves the ABR sidebar and resets the sizes.\n\
    - `Help` brings back this tour (`F1`) and the list of keys (`H`).\n\
    The right end reads each side's time window: `t` is its centre, `span` its width.";

const PLAYBACK: &str = "`PLAY` (`Space`) moves the yellow cursor through the run at the `SPEED` on the slider: simulated time \
    per real second, on a log scale from 1 us/s to 1 s/s.\n\
    `REWIND` sends it back to the start; `CURSOR` is its time.";

const LANES: &str = "One lane per link; time runs left to right. Each box is an A-MPDU, coloured by access category:\n\
    - {ff66cc} voice   {66ccff} video   {aaaaaa} background\n\
    - {4fc3f7} best effort downlink   {ffb74d} best effort uplink\n\
    - {ff0000} frame-loss notice   {ff3344} collision (stripes)\n\
    Hover any box for its sender, MCS, frames and timing. The next steps zoom into examples from this run.";

const SU_DL: &str = "The AP sends one A-MPDU to one headset over the whole channel. The box spans the TXOP, from the \
    moment the AP takes the medium to the end of the exchange.\n\
    The label reads AC, sender -> receiver (a negative `STA` id is an AP) and MCS, then the frames and MPDUs. On the \
    station map an arrow runs from the AP to the receiver.";

const SU_UL: &str = "A headset wins the channel itself, after its own AIFS and backoff, and sends one A-MPDU to the AP. The \
    colour is still the AC, with {ffb74d} amber for best effort uplink. On the station map the arrow points at the AP.";

const COLLISION: &str = "Two or more stations' backoff reached zero in the same slot and they sent at once.\n\
    - {ff3344} red stripes cover the airtime lost\n\
    The losers double their contention window: see the BACKOFF panel. On the station map each attempt is an arrow with \
    an X; hover the stripes for the contenders.";

const MLO: &str = "With MLO the AP and the headsets are multi-link devices: each lane is one link, on its own channel, and \
    the links work at the same time.\n\
    Each link contends on its own: the backoff and queue rows carry the link (`L0`, `L1`, ...). `Tab` or Channel > Rows \
    shows the rows of one link.";

const BOTTOM: &str = "- `BACKOFF`: each EDCA queue's contention window over time. `B` swaps it for a grid of backoff \
    counters (counter/CW, * frozen, amber = AIFS).\n\
    - `QUEUES`: each queue's depth. `Q` changes the style (lines, strips, heatmap, stacked), `W` puts all on one \
    scale, `I` marks the arrivals.\n\
    Hover a TXOP and the queues in it light up. The wheel over the names scrolls the rows; drag the dotted grips to \
    resize.";

const ABR: &str = "One line per video session, in the legend's colours:\n\
    - `Bitrate`: what the ABR chose, on a fixed 0-120 Mb/s scale\n\
    - `Peak` and `Instant throughput`, `RTT`, `Frame-loss ratio`: these rescale to what is in view\n\
    A dashed purple line on Bitrate traces an emulated bandwidth cap, when the run has one. Drag a plot's title up or \
    down to move it; the ABR menu hides plots.";

const LEGEND: &str = "One entry per video server: its address and ABR.\n\
    - hover an entry to pick out that session in every plot and on the map\n\
    - the table gives each session's bitrate, RTT and frame-loss ratio at the cursor";

const MAP: &str = "Headsets and other stations around the AP at the cursor time, with their trails.\n\
    - the wheel zooms, a drag pans, a right click or `RESET` fits it again\n\
    - drag the map's top edge or the sidebar's edge to resize; double-click an edge to restore it";

const TIME: &str = "Over either side:\n\
    - the wheel zooms time about the pointer; a left drag pans\n\
    - a right click places the cursor (`S` keeps both sides' cursors together)\n\
    - click a side to give it the keyboard: arrows pan, `+` `-` zoom, `Home` `End` jump\n\
    - `ZOOM` (`Z`) zooms by box: drag a box, click in, right-click out, double-click for the whole run. `RESET` shows \
    the whole run.\n\
    Drag the divider between the sides to resize them.";

const END: &str = "- `H` (Help > Keyboard and mouse) lists every key\n\
    - `F1` starts this tour again\n\
    - Layout > `Reset layout` brings back every panel at its default size\n\
    Your view is back where it was.";

/// The tour's steps for this frame's window and the run's examples: the parts that are not on screen are skipped.
fn steps(r: &Regions, ex: &[Example]) -> Vec<Step> {
    let mut v: Vec<Step> = Vec::new();
    let part = |title, body: &str, f: fn(&Regions) -> Option<Rect>| Step { title, body: body.to_string(), spots: vec![Spot::Part(f)], example: None };
    let map = Spot::Part(|r| r.map);
    v.push(Step { title: "Welcome", body: WELCOME.into(), spots: Vec::new(), example: None });
    v.push(part("Menu bar", MENU, |r| Some(r.menu)));
    v.push(part("Playback", PLAYBACK, |r| Some(r.controls)));
    if r.lanes.is_some() {
        v.push(part("Link lanes", LANES, |r| r.lanes));
    }
    let find = |k: Kind| ex.iter().position(|e| e.kind == k);
    let lanes_on = r.lanes.is_some();
    let example_step = |v: &mut Vec<Step>, k: Kind, title: &'static str, body: String, also: Option<Spot>| {
        if let Some(i) = find(k).filter(|_| lanes_on) {
            let detail = ex[i].detail.replacen("This one:", "`This one:`", 1);
            let spots = std::iter::once(Spot::Example).chain(also).collect();
            v.push(Step { title, body: format!("{}\n{}", body, detail), spots, example: Some(i) });
        }
    };
    example_step(&mut v, Kind::SuDl, "Single-user downlink", SU_DL.into(), Some(map));
    example_step(&mut v, Kind::SuUl, "Single-user uplink", SU_UL.into(), Some(map));
    example_step(&mut v, Kind::Collision, "Collision", COLLISION.into(), Some(map));
    example_step(&mut v, Kind::Mlo, "Several links (MLO)", MLO.into(), None);
    for (title, body, f) in [
        ("Backoff and queues", BOTTOM, (|r: &Regions| r.bottom) as fn(&Regions) -> Option<Rect>),
        ("ABR plots", ABR, |r| r.abr_plots),
        ("Legend and readout", LEGEND, |r| r.legend),
        ("Station map", MAP, |r| r.map),
        ("Moving through time", TIME, |r| Some(r.time_axis)),
    ] {
        if f(r).is_some() {
            v.push(part(title, body, f));
        }
    }
    v.push(Step { title: "That's all", body: END.into(), spots: Vec::new(), example: None });
    v
}

// ── Text with markup ─────────────────────────────────────────────────────────

const BODY_SCALE: f32 = 1.5;
const LINE_H: usize = 19;
const PARA_GAP: usize = 7;
const BULLET_INDENT: usize = 16;
const INK: u32 = 0xdde2ea;
const HILITE: u32 = 0xffd27a;

/// One word: its text, whether it is highlighted, and a colour swatch in front of it.
#[derive(Debug, PartialEq)]
struct Word {
    text: String,
    hl: bool,
    swatch: Option<u32>,
}

#[derive(Debug, PartialEq)]
struct Para {
    bullet: bool,
    words: Vec<Word>,
}

fn parse(body: &str) -> Vec<Para> {
    body.split('\n').map(|p| {
        let (bullet, p) = match p.strip_prefix("- ") { Some(rest) => (true, rest), None => (false, p) };
        let mut words = Vec::new();
        let mut hl = false;
        let mut swatch = None;
        for raw in p.split_whitespace() {
            if let Some(hex) = raw.strip_prefix('{').and_then(|r| r.strip_suffix('}')) {
                if let Ok(c) = u32::from_str_radix(hex, 16) {
                    swatch = Some(c);
                    continue;
                }
            }
            // a backtick opens or closes a highlight; the word is highlighted if any of it is
            let mut text = String::new();
            let mut word_hl = hl;
            for ch in raw.chars() {
                if ch == '`' {
                    hl = !hl;
                    word_hl |= hl;
                } else {
                    text.push(ch);
                }
            }
            if !text.is_empty() {
                words.push(Word { text, hl: word_hl, swatch: swatch.take() });
            }
        }
        Para { bullet, words }
    }).collect()
}

/// Width of a word as drawn (with its swatch).
fn word_w(w: &Word) -> usize {
    text_w(&w.text, BODY_SCALE) as usize + if w.swatch.is_some() { 16 } else { 0 }
}

/// A laid-out line: y offset, x offset (bullet indent), whether it carries a bullet, and its words.
struct Line<'a> {
    y: usize,
    x: usize,
    bullet: bool,
    words: Vec<&'a Word>,
}

/// `paras` broken into lines no wider than `max_w`; returns the lines and the total height.
fn layout(paras: &[Para], max_w: usize) -> (Vec<Line<'_>>, usize) {
    let space = text_w(" ", BODY_SCALE) as usize;
    let mut lines = Vec::new();
    let mut y = 0;
    for (pi, p) in paras.iter().enumerate() {
        if pi > 0 {
            y += PARA_GAP;
        }
        let x0 = if p.bullet { BULLET_INDENT } else { 0 };
        let mut cur: Vec<&Word> = Vec::new();
        let mut w = 0;
        let mut first = true;
        for word in &p.words {
            let ww = word_w(word);
            if !cur.is_empty() && x0 + w + space + ww > max_w {
                lines.push(Line { y, x: x0, bullet: p.bullet && first, words: std::mem::take(&mut cur) });
                y += LINE_H;
                w = 0;
                first = false;
            }
            if !cur.is_empty() {
                w += space;
            }
            w += ww;
            cur.push(word);
        }
        lines.push(Line { y, x: x0, bullet: p.bullet && first, words: cur });
        y += LINE_H;
    }
    // the last line needs only its glyphs' height
    (lines, y.saturating_sub(LINE_H - 12))
}

fn draw_text(buf: &mut [u32], stride: usize, x: usize, y: usize, lines: &[Line]) {
    let space = text_w(" ", BODY_SCALE) as usize;
    for l in lines {
        if l.bullet {
            fill_rect(buf, stride, x + 4, y + l.y + 4, 4, 4, 0x8899cc);
        }
        let mut cx = x + l.x;
        for w in &l.words {
            if let Some(c) = w.swatch {
                fill_rect(buf, stride, cx, y + l.y, 11, 11, c);
                outline_rect_clip(buf, stride, cx as i32, (y + l.y) as i32, cx as i32 + 11, (y + l.y) as i32 + 11, 0x666677);
                cx += 16;
            }
            render_text(buf, &w.text, cx, y + l.y, stride, if w.hl { HILITE } else { INK }, BODY_SCALE);
            cx += text_w(&w.text, BODY_SCALE) as usize + space;
        }
    }
}

// ── The overlays ─────────────────────────────────────────────────────────────

/// (keys, what they do); a row with empty keys is a section caption.
const KEYS: &[(&str, &str)] = &[
    ("", "MOUSE"),
    ("Wheel", "zoom time about the pointer"),
    ("Left drag", "pan"),
    ("Right click", "place the cursor"),
    ("Hover", "a box: its details"),
    ("Hover legend", "pick out one session"),
    ("Wheel on names", "scroll the CW / queue rows"),
    ("Drag edges", "resize (double-click: default)"),
    ("Drag plot title", "move an ABR plot"),
    ("Map", "wheel zoom, drag pan, right click fit"),
    ("", "PLAYBACK AND TIME"),
    ("Space", "play / pause"),
    ("Left Right, A D", "pan the focused side"),
    ("+ -", "zoom the focused side"),
    ("Home End", "jump to the start / end"),
    ("S", "keep both cursors together"),
    ("Z", "box zoom (right click out, double-click all)"),
    ("", "CHANNEL"),
    ("Q", "queue style"),
    ("W", "one scale for all queues"),
    ("B", "backoff grid / CW history"),
    ("I", "packet arrivals"),
    ("Tab", "rows of one link"),
    ("PgUp PgDn", "scroll the station rows"),
    ("", "WINDOW"),
    (". ,", "larger / smaller UI"),
    ("U", "default UI scale"),
    ("F1", "quick tour"),
    ("H", "this list"),
    ("Esc", "close a menu or this; else quit"),
];

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Overlay {
    /// The tour, at this step of the steps shown.
    Tour(usize),
    Keys,
    About,
}

#[derive(Clone, Copy, PartialEq)]
enum Btn {
    Back,
    Next,
    Close,
}

#[derive(Default)]
pub struct Help {
    pub overlay: Option<Overlay>,
    /// Buttons as drawn last frame.
    buttons: Vec<(Btn, Rect)>,
    prev_left: bool,
    /// A press taken while open is still held (also after it closed the overlay): the panels must not see it.
    holding: bool,
    /// The tour's text box as drawn last frame: the mouse over it is the tour's, elsewhere it may point at the panels.
    box_rect: Option<Rect>,
}

const BOX_W: usize = 540;
const PAD: usize = 18;
const TITLE_H: usize = 14;
const BTN_H: usize = 24;

/// Area two rectangles share.
fn overlap(a: Rect, b: Rect) -> usize {
    let w = (a.0 + a.2).min(b.0 + b.2).saturating_sub(a.0.max(b.0));
    let h = (a.1 + a.3).min(b.1 + b.3).saturating_sub(a.1.max(b.1));
    w * h
}

/// Where a box `bw` x `bh` goes in a `w` x `h` window so it covers none of `spots`: beside the first one (right, below,
/// left, above), else beside another, else in a corner; when nothing is free, where it covers least.
fn place_clear(spots: &[Rect], bw: usize, bh: usize, w: usize, h: usize) -> (usize, usize) {
    const GAP: usize = 16;
    let fit = |x: usize, y: usize| (x.min(w.saturating_sub(bw + 4)).max(4), y.min(h.saturating_sub(bh + 4)).max(4));
    let mut cands: Vec<(usize, usize)> = Vec::new();
    for &(sx, sy, sw, sh) in spots {
        cands.push(fit(sx + sw + GAP, sy));
        cands.push(fit(sx + 24, sy + sh + GAP));
        cands.push(fit(sx.saturating_sub(bw + GAP), sy));
        cands.push(fit(sx + 24, sy.saturating_sub(bh + GAP)));
    }
    cands.extend([fit(w, 76), fit(4, h), fit(w, h), fit(4, 76), fit(w.saturating_sub(bw) / 2, h.saturating_sub(bh) / 2)]);
    let covered = |&(x, y): &(usize, usize)| spots.iter().map(|&s| overlap((x, y, bw, bh), s)).sum::<usize>();
    cands.iter().copied().find(|c| covered(c) == 0)
        .or_else(|| cands.iter().copied().min_by_key(covered))
        .unwrap_or((4, 4))
}

impl Help {
    pub fn is_open(&self) -> bool {
        self.overlay.is_some()
    }

    /// The tour is open (its steps let the mouse point at the panels for tooltips and highlights, but take its clicks).
    pub fn touring(&self) -> bool {
        matches!(self.overlay, Some(Overlay::Tour(_)))
    }

    /// The tour's step (as shown, possibly past the last one).
    pub fn tour_step(&self) -> Option<usize> {
        match self.overlay {
            Some(Overlay::Tour(i)) => Some(i),
            _ => None,
        }
    }

    /// The mouse at (`mx`, `my`) is over the tour's text box.
    pub fn over_box(&self, mx: f32, my: f32) -> bool {
        self.box_rect.is_some_and(|(x, y, w, h)| mx >= x as f32 && mx < (x + w) as f32 && my >= y as f32 && my < (y + h) as f32)
    }

    /// The overlay has the mouse and keyboard: it is open, or the press that closed it is still held.
    pub fn captures(&self) -> bool {
        self.overlay.is_some() || self.holding
    }

    pub fn toggle(&mut self, o: Overlay) {
        let same = match (self.overlay, o) {
            (Some(Overlay::Tour(_)), Overlay::Tour(_)) => true,
            (a, b) => a == Some(b),
        };
        self.overlay = if same { None } else { Some(o) };
        self.buttons.clear();
    }

    /// Number of steps of the tour in this window.
    pub fn tour_len(r: &Regions, ex: &[Example]) -> usize {
        steps(r, ex).len()
    }

    /// The example whose view the channel side must show at the tour's current step.
    pub fn example(&self, r: &Regions, ex: &[Example]) -> Option<usize> {
        let Some(Overlay::Tour(i)) = self.overlay else { return None };
        let st = steps(r, ex);
        st.get(i.min(st.len().saturating_sub(1))).and_then(|s| s.example)
    }

    /// Keys and clicks of the open overlay. Returns whether Esc closed it (so it must not also quit).
    pub fn input(&mut self, window: &Window, r: &Regions, ex: &[Example]) -> bool {
        let left = window.get_mouse_down(MouseButton::Left);
        let pressed = left && !self.prev_left;
        self.prev_left = left;
        if !left {
            self.holding = false;
        }
        if pressed && self.overlay.is_some() {
            self.holding = true;
        }
        let Some(o) = self.overlay else { return false };
        let key = |k: Key| window.is_key_pressed(k, KeyRepeat::No);
        if key(Key::Escape) {
            self.overlay = None;
            return true;
        }
        let mut btn = None;
        if pressed {
            let (mx, my) = super::ui_scale::mouse_pos(window).unwrap_or((-1.0, -1.0));
            btn = self.buttons.iter().find(|(_, (x, y, w, h))| {
                mx >= *x as f32 && mx < (x + w) as f32 && my >= *y as f32 && my < (y + h) as f32
            }).map(|b| b.0);
        }
        match o {
            Overlay::Tour(i) => {
                let n = steps(r, ex).len();
                let next = btn == Some(Btn::Next) || key(Key::Space) || key(Key::Right) || key(Key::Enter) || key(Key::NumPadEnter)
                    || key(Key::N);
                let back = btn == Some(Btn::Back) || key(Key::Left) || key(Key::Backspace) || key(Key::P);
                if btn == Some(Btn::Close) || key(Key::F1) {
                    self.overlay = None;
                } else if next {
                    self.overlay = if i + 1 >= n { None } else { Some(Overlay::Tour(i + 1)) };
                } else if back {
                    self.overlay = Some(Overlay::Tour(i.saturating_sub(1)));
                } else if i >= n {
                    self.overlay = Some(Overlay::Tour(n.saturating_sub(1)));
                }
            }
            Overlay::Keys | Overlay::About => {
                if pressed || key(Key::H) || key(Key::Enter) || key(Key::F1) {
                    self.overlay = None;
                }
            }
        }
        false
    }

    /// Draws the open overlay; `mouse` lights the button under it.
    pub fn draw(&mut self, buf: &mut [u32], stride: usize, mouse: (f32, f32), r: &Regions, ex: &[Example], about: &[String]) {
        self.buttons.clear();
        self.box_rect = None;
        let Some(o) = self.overlay else { return };
        match o {
            Overlay::Tour(i) => {
                let st = steps(r, ex);
                let i = i.min(st.len().saturating_sub(1));
                let Some(step) = st.get(i) else { return };
                let spots = resolve(&step.spots, r);
                dim(buf, stride, &spots);
                for &(x, y, w, h) in &spots {
                    let (x0, y0) = (x as i32, y as i32);
                    let (x1, y1) = ((x + w).min(stride).saturating_sub(1) as i32, (y + h).saturating_sub(1) as i32);
                    outline_rect_clip(buf, stride, x0, y0, x1, y1, 0xffcc66);
                    outline_rect_clip(buf, stride, x0 + 1, y0 + 1, x1 - 1, y1 - 1, 0xffcc66);
                }
                let paras = parse(&step.body);
                let (lines, text_h) = layout(&paras, BOX_W - 2 * PAD);
                let bh = PAD + TITLE_H + 16 + text_h + 20 + BTN_H + PAD;
                let (bx, by) = if spots.is_empty() {
                    (r.w.saturating_sub(BOX_W) / 2, r.h.saturating_sub(bh) / 2)
                } else {
                    place_clear(&spots, BOX_W, bh, r.w, r.h)
                };
                panel(buf, stride, bx, by, BOX_W, bh);
                self.box_rect = Some((bx, by, BOX_W, bh));
                render_text(buf, step.title, bx + PAD, by + PAD, stride, 0xffeecc, 2.0);
                let count = format!("{} / {}", i + 1, st.len());
                render_text(buf, &count, bx + BOX_W - PAD - text_w(&count, 1.0) as usize, by + PAD + 4, stride, 0x8899aa, 1.0);
                fill_rect(buf, stride, bx + PAD, by + PAD + TITLE_H + 7, BOX_W - 2 * PAD, 1, 0x2e3450);
                draw_text(buf, stride, bx + PAD, by + PAD + TITLE_H + 16, &lines);
                let by_btn = by + bh - PAD - BTN_H;
                let last = i + 1 >= st.len();
                let mut bxr = bx + BOX_W - PAD;
                for (b, label, primary) in [(Btn::Next, if last { "Done" } else { "Next >" }, true), (Btn::Back, "< Back", false)] {
                    if b == Btn::Back && i == 0 { continue; }
                    let bw = text_w(label, 1.5) as usize + 24;
                    bxr -= bw;
                    self.button(buf, stride, (bxr, by_btn, bw, BTN_H), b, label, primary, mouse);
                    bxr -= 8;
                }
                if !last {
                    self.button(buf, stride, (bx + PAD, by_btn, text_w("Close", 1.5) as usize + 24, BTN_H), Btn::Close, "Close", false, mouse);
                }
            }
            Overlay::Keys => {
                dim(buf, stride, &[]);
                let half = KEYS.len().div_ceil(2);
                // split at the caption nearest the middle, so no section runs over two columns
                let split = (1..KEYS.len()).filter(|&k| KEYS[k].0.is_empty()).min_by_key(|&k| k.abs_diff(half)).unwrap_or(half);
                let cols = [&KEYS[..split], &KEYS[split..]];
                const ROW: usize = 18;
                let key_w = KEYS.iter().map(|k| text_w(k.0, 1.5) as usize).max().unwrap_or(0) + 16;
                let col_w = key_w + KEYS.iter().map(|k| text_w(k.1, 1.5) as usize).max().unwrap_or(0) + 28;
                let rows = cols.iter().map(|c| c.len()).max().unwrap_or(0);
                let (bw, bh) = (2 * col_w + 2 * PAD, PAD + TITLE_H + 18 + rows * ROW + 30);
                let (bx, by) = (r.w.saturating_sub(bw) / 2, r.h.saturating_sub(bh) / 2);
                panel(buf, stride, bx, by, bw, bh);
                render_text(buf, "Keyboard and mouse", bx + PAD, by + PAD, stride, 0xffeecc, 2.0);
                for (c, col) in cols.iter().enumerate() {
                    let cx = bx + PAD + c * col_w;
                    let mut y = by + PAD + TITLE_H + 18;
                    for (k, what) in col.iter() {
                        if k.is_empty() {
                            render_text(buf, what, cx, y + 5, stride, 0x7f8cb0, 1.0);
                        } else {
                            render_text(buf, k, cx, y, stride, HILITE, 1.5);
                            render_text(buf, what, cx + key_w, y, stride, INK, 1.5);
                        }
                        y += ROW;
                    }
                }
                let foot = "Keys act on the side last clicked. Click or press H to close.";
                render_text(buf, foot, bx + PAD, by + bh - 20, stride, 0x8899aa, 1.0);
            }
            Overlay::About => {
                dim(buf, stride, &[]);
                let body = about.join("\n");
                let paras = parse(&body);
                let (lines, text_h) = layout(&paras, BOX_W - 2 * PAD);
                let bh = PAD + TITLE_H + 16 + text_h + 32;
                let (bx, by) = (r.w.saturating_sub(BOX_W) / 2, r.h.saturating_sub(bh) / 2);
                panel(buf, stride, bx, by, BOX_W, bh);
                render_text(buf, "About this viewer", bx + PAD, by + PAD, stride, 0xffeecc, 2.0);
                draw_text(buf, stride, bx + PAD, by + PAD + TITLE_H + 16, &lines);
                render_text(buf, "Click to close.", bx + PAD, by + bh - 20, stride, 0x8899aa, 1.0);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn button(&mut self, buf: &mut [u32], stride: usize, (x, y, w, h): Rect, b: Btn, label: &str, primary: bool, (mx, my): (f32, f32)) {
        let hover = mx >= x as f32 && mx < (x + w) as f32 && my >= y as f32 && my < (y + h) as f32;
        let bg = match (primary, hover) {
            (true, true) => 0x3a5aa8,
            (true, false) => 0x2c4682,
            (false, true) => 0x34344c,
            (false, false) => 0x24243a,
        };
        fill_rect(buf, stride, x, y, w, h, bg);
        outline_rect_clip(buf, stride, x as i32, y as i32, (x + w) as i32, (y + h) as i32, if primary { 0x6a8cdd } else { 0x4a4a66 });
        render_text(buf, label, x + (w - text_w(label, 1.5) as usize) / 2, y + (h - 11) / 2, stride, 0xf0f2f8, 1.5);
        self.buttons.push((b, (x, y, w, h)));
    }
}

/// A help box: dark, framed, with a shadow.
fn panel(buf: &mut [u32], stride: usize, x: usize, y: usize, w: usize, h: usize) {
    let rows = buf.len() / stride.max(1);
    for yy in (y + 5)..(y + h + 5).min(rows) {
        for xx in (x + 5)..(x + w + 5).min(stride) {
            let c = &mut buf[yy * stride + xx];
            *c = (*c >> 1) & 0x7f7f7f;
        }
    }
    fill_rect(buf, stride, x, y, w, h, 0x161622);
    outline_rect_clip(buf, stride, x as i32, y as i32, (x + w) as i32, (y + h) as i32, 0x6a7cc0);
}

/// Darkens the whole frame but `spots`.
fn dim(buf: &mut [u32], stride: usize, spots: &[Rect]) {
    let rows = buf.len() / stride.max(1);
    for yy in 0..rows {
        // the spots' x ranges on this row
        let keep: Vec<(usize, usize)> = spots.iter().filter(|s| yy >= s.1 && yy < s.1 + s.3).map(|s| (s.0, s.0 + s.2)).collect();
        for xx in 0..stride {
            if keep.iter().any(|&(a, b)| xx >= a && xx < b) {
                continue;
            }
            let c = &mut buf[yy * stride + xx];
            *c = ((*c >> 2) & 0x3f3f3f) + ((*c >> 3) & 0x1f1f1f);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn regions(full: bool) -> Regions {
        let opt = |r: Rect| if full { Some(r) } else { None };
        Regions {
            w: 2200, h: 950, menu: (0, 0, 2200, 24), controls: (0, 24, 2200, 44),
            lanes: Some((0, 72, 1100, 110)), bottom: Some((0, 540, 1100, 380)),
            abr_plots: opt((1320, 68, 880, 860)), legend: opt((1106, 68, 210, 300)), map: opt((1106, 330, 210, 600)),
            time_axis: (0, 928, 2200, 22), cw: Some((0, 540, 1100, 150)), example: opt((300, 72, 200, 110)),
        }
    }

    const KINDS: [Kind; 4] = [Kind::SuDl, Kind::SuUl, Kind::Collision, Kind::Mlo];

    fn examples() -> Vec<Example> {
        KINDS.iter().map(|&kind| Example {
            kind, lanes: (0, 0), t0: 1.0, t1: 1.001, detail: "This one: a test.".into(), ..Default::default()
        }).collect()
    }

    #[test]
    fn steps_skip_parts_not_on_screen() {
        let all = steps(&regions(true), &examples());
        let none = steps(&regions(false), &[]);
        // three parts (ABR plots, legend, map) and four examples come and go
        assert_eq!(all.len(), none.len() + 3 + KINDS.len());
        for s in &none {
            assert!(!["ABR plots", "Legend and readout", "Station map", "Collision"].contains(&s.title));
        }
        // without the lanes (channel side hidden) no example is shown
        let r = Regions { lanes: None, example: None, ..regions(true) };
        assert!(steps(&r, &examples()).iter().all(|s| s.example.is_none()));
    }

    #[test]
    fn every_combination_of_hidden_parts_works() {
        let ex = examples();
        for mask in 0..64u32 {
            let on = |b: u32, r: Rect| if mask & (1 << b) != 0 { Some(r) } else { None };
            let full = regions(true);
            let r = Regions {
                lanes: on(0, full.lanes.unwrap()), bottom: on(2, full.bottom.unwrap()),
                abr_plots: on(3, full.abr_plots.unwrap()), map: on(4, full.map.unwrap()), cw: on(5, full.cw.unwrap()),
                ..full
            };
            for exs in [&ex[..], &[]] {
                let st = steps(&r, exs);
                assert!(st.len() >= 5 && st.first().unwrap().title == "Welcome" && st.last().unwrap().title == "That's all");
                let mut buf = vec![0u32; r.w * r.h];
                for i in 0..st.len() + 2 {
                    // a step past the end (a part hidden since) draws the last one
                    let mut help = Help { overlay: Some(Overlay::Tour(i)), ..Default::default() };
                    help.draw(&mut buf, r.w, (10.0, 10.0), &r, exs, &[]);
                    assert!(help.example(&r, exs).map_or(true, |k| k < exs.len()));
                }
            }
        }
    }

    #[test]
    fn tiny_and_odd_windows_draw() {
        for (w, h) in [(880usize, 400usize), (900, 760), (1, 1), (3840, 2160)] {
            let r = Regions { w, h, ..regions(true) };
            let mut buf = vec![0u32; w * h];
            for o in [Overlay::Tour(0), Overlay::Tour(5), Overlay::Tour(99), Overlay::Keys, Overlay::About] {
                let mut help = Help { overlay: Some(o), ..Default::default() };
                help.draw(&mut buf, w, (-1.0, -1.0), &r, &examples(), &["About".into()]);
            }
        }
    }

    #[test]
    fn markup_parses() {
        let p = parse("- {ff0000} red `Esc` and `two words` here\nplain");
        assert!(p[0].bullet && !p[1].bullet);
        let w = &p[0].words;
        assert_eq!((w[0].text.as_str(), w[0].swatch, w[0].hl), ("red", Some(0xff0000), false));
        assert_eq!((w[1].text.as_str(), w[1].hl), ("Esc", true));
        assert_eq!((w[2].text.as_str(), w[2].hl), ("and", false));
        assert!(w[3].hl && w[4].hl && !w[5].hl, "{w:?}");
        // a word wider than the line still gets a line of its own
        let long = parse("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa b");
        let (lines, _) = layout(&long, 100);
        assert_eq!(lines.len(), 2);
        assert!(layout(&parse(""), 100).0.len() == 1);
    }

    #[test]
    fn every_step_fits_its_box() {
        let ex = examples();
        for s in steps(&regions(true), &ex) {
            let paras = parse(&s.body);
            // every {..} became a swatch
            assert!(paras.iter().flat_map(|p| &p.words).all(|w| !w.text.contains('{')), "bad swatch in {}", s.title);
            let (lines, h) = layout(&paras, BOX_W - 2 * PAD);
            let space = text_w(" ", BODY_SCALE) as usize;
            for l in &lines {
                let w: usize = l.words.iter().map(|w| word_w(w)).sum::<usize>() + space * l.words.len().saturating_sub(1);
                assert!(l.x + w <= BOX_W - 2 * PAD, "{}: line too wide", s.title);
            }
            // the box fits the smallest logical frame (760 px tall)
            assert!(PAD + TITLE_H + 16 + h + 20 + BTN_H + PAD <= 700, "{}: {} px of text", s.title, h);
        }
    }

    #[test]
    fn boxes_stay_in_the_window_and_off_the_spots() {
        let r = regions(true);
        let (lanes, map, bottom) = (r.lanes.unwrap(), r.map.unwrap(), r.bottom.unwrap());
        for spots in [vec![r.menu], vec![lanes, map], vec![r.example.unwrap(), map], vec![bottom], vec![r.time_axis],
                      vec![(0, 0, 2200, 950)], vec![(2150, 900, 40, 40)], vec![(300, 72, 200, 110), r.cw.unwrap()]] {
            for bh in [260usize, 420] {
                let (x, y) = place_clear(&spots, BOX_W, bh, r.w, r.h);
                assert!(x + BOX_W <= r.w && y + bh <= r.h, "{spots:?} -> {x},{y}");
                // free room exists for all but the whole-window spot
                if spots[0] != (0, 0, 2200, 950) {
                    let cover: usize = spots.iter().map(|&s| overlap((x, y, BOX_W, bh), s)).sum();
                    assert_eq!(cover, 0, "{spots:?} -> {x},{y}");
                }
            }
        }
        // a window smaller than the box: still placed, at the top left
        assert_eq!(place_clear(&[(0, 0, 10, 10)], BOX_W, 300, 200, 100), (4, 4));
    }

    /// Renders every slide and the keyboard sheet over a viewer screenshot, for looking at the text:
    /// `TOUR_BG=<screenshot.png> TOUR_OUT=<dir> cargo test --release --example XR_sim -- --ignored render_slides`.
    #[test]
    #[ignore]
    fn render_slides() {
        let (Ok(bg), Ok(out)) = (std::env::var("TOUR_BG"), std::env::var("TOUR_OUT")) else { return };
        let img = image::open(bg).expect("background").to_rgb8();
        let (w, h) = (img.width() as usize, img.height() as usize);
        let base: Vec<u32> = img.pixels().map(|p| (p[0] as u32) << 16 | (p[1] as u32) << 8 | p[2] as u32).collect();
        let r = Regions { w, h, ..regions(true) };
        let mut ex = examples();
        ex[0].detail = "This one: the AP sends 56 MPDUs of Video to s205 at MCS 8, 1450 us.".into();
        let n = steps(&r, &ex).len();
        let overlays: Vec<Overlay> = (0..n).map(Overlay::Tour).chain([Overlay::Keys, Overlay::About]).collect();
        for (k, o) in overlays.into_iter().enumerate() {
            let mut buf = base.clone();
            let mut help = Help { overlay: Some(o), ..Default::default() };
            help.draw(&mut buf, w, (-1.0, -1.0), &r, &ex, &["SiEStA-VR post-simulation viewer.".into(), "Run: test".into()]);
            let mut png = image::RgbImage::new(w as u32, h as u32);
            for (i, px) in buf.iter().enumerate() {
                png.put_pixel((i % w) as u32, (i / w) as u32, image::Rgb([(px >> 16) as u8, (px >> 8) as u8, *px as u8]));
            }
            png.save(format!("{out}/slide_{k:02}.png")).expect("save");
        }
    }

    #[test]
    fn every_key_on_the_sheet_fits_its_column() {
        assert!(KEYS.iter().all(|(k, w)| k.chars().count() <= 16 && w.chars().count() <= 46));
    }
}
