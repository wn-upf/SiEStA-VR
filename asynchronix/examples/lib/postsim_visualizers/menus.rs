//! The unified viewer's menus: what they hold, built from the viewer's state every frame so the ticks always match the
//! screen, and what each item does. A switch that also has a key does exactly what the key does.

use std::time::Instant;
use super::menu_bar::{Item, Menu};
use super::{panel_resize, sidebar_resize, strip_order::StripOrder, tour, ui_scale, Focus, UnifiedState};

/// Which panels are shown and where (View, Channel, ABR and Layout menus). The panel switches that have a key live in
/// the viewer's state as before.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Prefs {
    pub show_ch: bool,
    pub show_abr: bool,
    /// ABR side on the left, channel side on the right.
    pub swap_sides: bool,
    /// The ABR sidebar (legend and station map) right of the plots.
    pub sidebar_right: bool,
    /// The backoff (CW) and queue panels under the link lanes.
    pub show_cw: bool,
    pub show_q: bool,
    pub show_legend: bool,
    pub show_map: bool,
}

impl Default for Prefs {
    fn default() -> Self {
        Prefs {
            show_ch: true, show_abr: true, swap_sides: false, sidebar_right: false,
            show_cw: true, show_q: true, show_legend: true, show_map: true,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Cmd {
    Screenshot,
    Quit,
    ShowCh,
    ShowAbr,
    Sync,
    Zoom,
    WholeRun,
    PlayPause,
    Rewind,
    UiLarger,
    UiSmaller,
    UiDefault,
    Overhead,
    CwPanel,
    BoGrid,
    QPanel,
    QStyle(u8),
    QShared,
    Arrivals,
    Rows(Option<u8>),
    Strip(usize),
    Legend,
    Map,
    StripOrder,
    Swap,
    SidebarRight,
    DefaultSizes,
    ResetLayout,
    Tour,
    Keys,
    About,
}

/// What the run has, which decides the items that apply.
pub struct Facts<'a> {
    pub num_links: u8,
    pub has_map: bool,
    /// The ABR plots' names, by plot index.
    pub strip_names: &'a [&'static str],
}

const Q_STYLES: [&str; 4] = ["Lines, with station rows", "One strip per queue", "Heatmap", "Stacked area"];

pub fn build(state: &UnifiedState, prefs: &Prefs, strips: &StripOrder, f: &Facts) -> Vec<Menu<Cmd>> {
    let v = &state.ch_view;
    let mut channel = vec![
        Item::header("Lanes"),
        Item::check("AIFS, backoff and preamble", "O", v.show_overhead, Cmd::Overhead),
        Item::header("Backoff panel"),
        Item::check("Backoff panel", "", prefs.show_cw, Cmd::CwPanel),
        Item::check("Counter grid instead of CW", "B", v.show_bo_matrix, Cmd::BoGrid).enabled(prefs.show_cw && v.q_mode != 0),
        Item::header("Queue panel (Q: next style)"),
        Item::check("Queue panel", "", prefs.show_q, Cmd::QPanel),
    ];
    for (m, name) in Q_STYLES.iter().enumerate() {
        channel.push(Item::radio(*name, "", v.q_mode == m as u8, Cmd::QStyle(m as u8)));
    }
    channel.push(Item::check("One scale for all queues", "W", v.q_shared, Cmd::QShared));
    channel.push(Item::check("Packet arrivals", "I", v.show_arrivals, Cmd::Arrivals));
    channel.push(Item::header("Rows below the lanes (Tab)"));
    channel.push(Item::radio("All links", "", v.selected_link.is_none(), Cmd::Rows(None)));
    for l in 0..f.num_links {
        channel.push(Item::radio(format!("Link {l} only"), "", v.selected_link == Some(l), Cmd::Rows(Some(l))));
    }

    let mut abr = vec![Item::header("Plots (drag a title to move one)")];
    for (i, name) in f.strip_names.iter().enumerate() {
        abr.push(Item::check(*name, "", strips.shown.get(i).copied().unwrap_or(true), Cmd::Strip(i)));
    }
    abr.push(Item::action("Default plot order", "", Cmd::StripOrder).enabled(!strips.is_default_order()));
    abr.push(Item::header("Sidebar"));
    abr.push(Item::check("Legend and readout", "", prefs.show_legend, Cmd::Legend));
    abr.push(Item::check("Station map", "", prefs.show_map, Cmd::Map).enabled(f.has_map));

    vec![
        Menu { title: "File", items: vec![
            Item::action("Save screenshot", "", Cmd::Screenshot),
            Item::sep(),
            Item::action("Quit", "Esc", Cmd::Quit),
        ] },
        Menu { title: "View", items: vec![
            Item::header("Sides"),
            // the last side shown cannot be hidden
            Item::check("Channel side", "", prefs.show_ch, Cmd::ShowCh).enabled(prefs.show_abr),
            Item::check("ABR side", "", prefs.show_abr, Cmd::ShowAbr).enabled(prefs.show_ch),
            Item::header("Time"),
            Item::action(if state.playing { "Pause" } else { "Play" }, "Space", Cmd::PlayPause),
            Item::action("Rewind", "", Cmd::Rewind),
            Item::check("Keep both cursors together", "S", state.sync_cursor, Cmd::Sync),
            Item::check("Box zoom", "Z", state.zoom_mode, Cmd::Zoom),
            Item::action("Show the whole run", "", Cmd::WholeRun),
            Item::header(format!("Text and UI size ({})", ui_scale::label())),
            Item::action("Larger", ".", Cmd::UiLarger),
            Item::action("Smaller", ",", Cmd::UiSmaller),
            Item::action("Monitor default", "U", Cmd::UiDefault),
        ] },
        Menu { title: "Channel", items: channel },
        Menu { title: "ABR", items: abr },
        Menu { title: "Layout", items: vec![
            Item::check("Swap sides", "", prefs.swap_sides, Cmd::Swap).enabled(prefs.show_ch && prefs.show_abr),
            Item::check("ABR sidebar on the right", "", prefs.sidebar_right, Cmd::SidebarRight),
            Item::sep(),
            Item::action("Default sizes", "", Cmd::DefaultSizes),
            Item::action("Reset layout", "", Cmd::ResetLayout),
        ] },
        Menu { title: "Help", items: vec![
            Item::action("Quick tour", "F1", Cmd::Tour),
            Item::action("Keyboard and mouse", "H", Cmd::Keys),
            Item::sep(),
            Item::action("About", "", Cmd::About),
        ] },
    ]
}

/// What the main loop must do after an item.
#[derive(Debug, PartialEq)]
pub enum After {
    Nothing,
    Quit,
    Screenshot,
}

/// Everything an item can change.
pub struct Targets<'a> {
    pub state: &'a mut UnifiedState,
    pub prefs: &'a mut Prefs,
    pub strips: &'a mut StripOrder,
    pub ui: &'a mut ui_scale::UiScale,
    pub help: &'a mut tour::Help,
    pub sidebar: &'a mut sidebar_resize::SidebarResize,
    pub bottom: &'a mut panel_resize::PanelResize,
    /// The channel and ABR views' whole time ranges.
    pub ch_range: (f64, f64),
    pub abr_range: (f64, f64),
    /// Window width (logical px).
    pub w: usize,
}

pub fn apply(cmd: Cmd, t: Targets) -> After {
    let s = t.state;
    let v = &mut s.ch_view;
    match cmd {
        Cmd::Screenshot => return After::Screenshot,
        Cmd::Quit => return After::Quit,
        Cmd::ShowCh => {
            t.prefs.show_ch = !t.prefs.show_ch || !t.prefs.show_abr;
            if !t.prefs.show_ch { s.focus = Focus::Abr; }
        }
        Cmd::ShowAbr => {
            t.prefs.show_abr = !t.prefs.show_abr || !t.prefs.show_ch;
            if !t.prefs.show_abr { s.focus = Focus::Channel; }
        }
        Cmd::Sync => s.sync_cursor = !s.sync_cursor,
        Cmd::Zoom => {
            s.zoom_mode = !s.zoom_mode;
            s.zoom_drag = None;
        }
        Cmd::WholeRun => {
            let ((c0, c1), (a0, a1)) = (t.ch_range, t.abr_range);
            (s.ch_view.center_t, s.ch_view.span_t) = ((c0 + c1) * 0.5, c1 - c0);
            (s.abr_view.center_t, s.abr_view.span_t) = ((a0 + a1) * 0.5, a1 - a0);
        }
        // as the PLAY and REWIND buttons
        Cmd::PlayPause => {
            s.playing = !s.playing;
            s.last_frame = Instant::now();
        }
        Cmd::Rewind => {
            s.ch_view.cursor_t = t.ch_range.0;
            s.abr_view.cursor_t = t.ch_range.0;
            s.playing = false;
        }
        Cmd::UiLarger => t.ui.menu_step(true),
        Cmd::UiSmaller => t.ui.menu_step(false),
        Cmd::UiDefault => t.ui.menu_default(),
        Cmd::Overhead => v.show_overhead = !v.show_overhead,
        Cmd::CwPanel => t.prefs.show_cw = !t.prefs.show_cw,
        Cmd::BoGrid => v.show_bo_matrix = !v.show_bo_matrix,
        Cmd::QPanel => t.prefs.show_q = !t.prefs.show_q,
        Cmd::QStyle(m) => v.q_mode = m % 4,
        Cmd::QShared => v.q_shared = !v.q_shared,
        Cmd::Arrivals => v.show_arrivals = !v.show_arrivals,
        Cmd::Rows(l) => v.selected_link = l,
        Cmd::Strip(i) => {
            if let Some(on) = t.strips.shown.get_mut(i) { *on = !*on; }
        }
        Cmd::Legend => t.prefs.show_legend = !t.prefs.show_legend,
        Cmd::Map => t.prefs.show_map = !t.prefs.show_map,
        Cmd::StripOrder => t.strips.order = (0..t.strips.shown.len()).collect(),
        Cmd::Swap => {
            t.prefs.swap_sides = !t.prefs.swap_sides;
            s.split_frac = super::layout::mirrored_split(t.w, s.split_frac);
        }
        Cmd::SidebarRight => t.prefs.sidebar_right = !t.prefs.sidebar_right,
        Cmd::DefaultSizes => default_sizes(s, t.sidebar, t.bottom),
        Cmd::ResetLayout => {
            *t.prefs = Prefs::default();
            t.strips.reset();
            default_sizes(s, t.sidebar, t.bottom);
        }
        Cmd::Tour => t.help.toggle(tour::Overlay::Tour(0)),
        Cmd::Keys => t.help.toggle(tour::Overlay::Keys),
        Cmd::About => t.help.toggle(tour::Overlay::About),
    }
    After::Nothing
}

fn default_sizes(s: &mut UnifiedState, sidebar: &mut sidebar_resize::SidebarResize, bottom: &mut panel_resize::PanelResize) {
    s.split_frac = 0.5;
    *sidebar = Default::default();
    *bottom = Default::default();
}

/// The menu bar's right end: each shown side's time window, the cursor sync and which side has the keyboard.
pub fn status(state: &UnifiedState, prefs: &Prefs) -> String {
    let mut parts = Vec::new();
    if prefs.show_ch || !prefs.show_abr {
        parts.push(format!("CH t={:.4}s span={:.3}ms", state.ch_view.center_t, state.ch_view.span_t * 1e3));
    }
    if prefs.show_abr {
        parts.push(format!("ABR t={:.4}s span={:.3}ms", state.abr_view.center_t, state.abr_view.span_t * 1e3));
    }
    parts.push(format!("cursors {}", if state.sync_cursor { "synced" } else { "apart" }));
    if prefs.show_abr && (prefs.show_ch || !prefs.show_abr) {
        parts.push(format!("keys: {}", if state.focus == Focus::Channel { "CH" } else { "ABR" }));
    }
    parts.push("F1 tour".into());
    parts.join("   |   ")
}
