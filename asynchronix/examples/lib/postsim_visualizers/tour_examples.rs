//! Real transmissions of the run for the quick tour to zoom into: one of each kind the run has (single-user downlink and
//! uplink, a collision, two links busy at once). Each is taken from the second half of the run when it has one, where the
//! sessions are past their start.

use super::{access_window, is_uplink, leg_user, VizEvent, VizIndex};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    SuDl,
    SuUl,
    Collision,
    Mlo,
}

#[derive(Clone, Debug)]
pub struct Example {
    pub kind: Kind,
    /// First and last lane it spans.
    pub lanes: (u8, u8),
    pub t0: f64,
    pub t1: f64,
    /// What the tour says about this one in particular.
    pub detail: String,
    /// What the station map draws during the step: this TXOP (link, start) or this collision; neither, what is on the air
    /// at the cursor.
    pub txop: Option<(u8, f64)>,
    pub coll: Option<(u8, f64)>,
    /// Cursor positions the step steps through, one every `ANIM_STEP` s, over and over (none of this build's examples).
    pub anim: Vec<f64>,
}

/// Real seconds per animation step.
pub const ANIM_STEP: f64 = 0.55;

impl Default for Example {
    fn default() -> Self {
        Example { kind: Kind::SuDl, lanes: (0, 0), t0: 0.0, t1: 0.0, detail: String::new(), txop: None, coll: None,
                  anim: Vec::new() }
    }
}

/// Narrowest window the tour zooms to: a few slots and a preamble stay readable.
const MIN_SPAN: f64 = 300e-6;

impl Example {
    /// The channel view that shows it: centred, three times as wide as it is (at least `MIN_SPAN`), held to the run's
    /// range, with the cursor on it.
    pub fn view(&self, t_min: f64, t_max: f64) -> (f64, f64, f64) {
        let full = (t_max - t_min).max(1e-9);
        let (factor, least) = (3.0, MIN_SPAN);
        let span = ((self.t1 - self.t0) * factor).max(least).min(full);
        let mid = 0.5 * (self.t0 + self.t1);
        let center = mid.clamp(t_min + span * 0.5, (t_max - span * 0.5).max(t_min + span * 0.5));
        (center, span, mid.clamp(t_min, t_max))
    }

    /// The cursor `elapsed` real seconds into an animated step.
    pub fn cursor_at(&self, elapsed: f64) -> Option<f64> {
        if self.anim.is_empty() || !elapsed.is_finite() || elapsed < 0.0 {
            return None;
        }
        Some(self.anim[(elapsed / ANIM_STEP) as usize % self.anim.len()])
    }
}

/// Indices `0..n` starting at the first one at or after `mid` (by `t_of`), then the ones before it.
fn from_mid(n: usize, t_of: impl Fn(usize) -> f64, mid: f64) -> impl Iterator<Item = usize> {
    let k = (0..n).position(|i| t_of(i) >= mid).unwrap_or(n);
    (k..n).chain(0..k)
}

fn who(id: i32) -> String {
    if id < 0 { "the AP".into() } else { format!("s{id}") }
}

pub fn find(idx: &VizIndex) -> Vec<Example> {
    let mid = 0.5 * (idx.t_min + idx.t_max);
    let mut out: Vec<Example> = Vec::new();
    let mut links: Vec<u8> = idx.txop_groups_by_link.keys().copied().collect();
    links.sort_unstable();

    // single-user TXOPs: the first of each direction from the middle of the run, on the lowest link that has one
    for (kind, want_ap) in [(Kind::SuDl, true), (Kind::SuUl, false)] {
        'links: for &l in &links {
            let gs = &idx.txop_groups_by_link[&l];
            for i in from_mid(gs.len(), |i| gs[i].t, mid) {
                let g = &gs[i];
                if !g.is_plain_su() {
                    continue;
                }
                let VizEvent::TxopStart { sender, dest_id, mcs, ampdu_packets, .. } = &idx.all[g.legs[0]] else { continue };
                let ok = if want_ap { sender.0 < 0 && !is_uplink(*dest_id) } else { sender.0 >= 0 && is_uplink(*dest_id) };
                if !ok {
                    continue;
                }
                let detail = if want_ap {
                    format!("This one: the AP sends {} MPDUs of {:?} to s{} at MCS {}, {:.0} us.",
                            ampdu_packets, sender.1, leg_user(*sender, *dest_id), mcs, (g.end - g.t) * 1e6)
                } else {
                    format!("This one: s{} sends {} MPDUs of {:?} to the AP at MCS {}, {:.0} us.",
                            sender.0, ampdu_packets, sender.1, mcs, (g.end - g.t) * 1e6)
                };
                // from the start of its channel access (AIFS and backoff), which the step describes
                let t0 = access_window(&idx.all[g.legs[0]], g.t).map_or(g.t, |w| w.0.min(g.t));
                out.push(Example { kind, lanes: (l, l), t0, t1: g.end, detail, txop: Some((l, g.t)), ..Default::default() });
                break 'links;
            }
        }
    }

    // a collision
    let mut coll: Option<(u8, f64, f64, String)> = None;
    'coll: for &l in &links {
        let Some(cs) = idx.collisions_by_link.get(&l) else { continue };
        for i in from_mid(cs.len(), |i| super::event_t(&idx.all[cs[i]]), mid) {
            let VizEvent::Collision { t, end, contenders, .. } = &idx.all[cs[i]] else { continue };
            let names: Vec<String> = contenders.iter().map(|k| who(k.0)).collect();
            let verb = if names.len() == 1 { "collides" } else { "collide" };
            let detail = format!("This one: {} {}, {:.0} us lost.", names.join(" and "), verb, (end - t) * 1e6);
            coll = Some((l, *t, *end, detail));
            break 'coll;
        }
    }
    if let Some((l, t0, t1, detail)) = coll {
        out.push(Example { kind: Kind::Collision, lanes: (l, l), t0, t1, detail, coll: Some((l, t0)), ..Default::default() });
    }

    // MLO: two links busy at once
    'mlo: for (ai, &a) in links.iter().enumerate() {
        for &b in &links[ai + 1..] {
            let (Some(ga), Some(gb)) = (idx.txop_groups_by_link.get(&a), idx.txop_groups_by_link.get(&b)) else { continue };
            for i in from_mid(ga.len(), |i| ga[i].t, mid) {
                let x = &ga[i];
                let j = gb.partition_point(|g| g.end <= x.t);
                if let Some(y) = gb.get(j).filter(|y| y.t < x.end) {
                    let detail = format!("This one: links {} and {} both carry a TXOP at {:.6} s.", a, b, x.t.max(y.t));
                    out.push(Example { kind: Kind::Mlo, lanes: (a, b), t0: x.t.min(y.t), t1: x.end.max(y.end),
                                       detail, ..Default::default() });
                    break 'mlo;
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_centres_and_holds_to_the_run() {
        let ex = Example { kind: Kind::SuDl, lanes: (0, 0), t0: 1.0, t1: 1.001, ..Default::default() };
        let (c, s, cur) = ex.view(0.0, 10.0);
        assert!((s - 0.003).abs() < 1e-12 && (c - 1.0005).abs() < 1e-12 && (cur - 1.0005).abs() < 1e-12);
        // a short one gets the least span; one at the end of the run stays inside it
        let ex = Example { t0: 9.99999, t1: 10.0, ..ex };
        let (c, s, _) = ex.view(0.0, 10.0);
        assert!((s - MIN_SPAN).abs() < 1e-12 && c + s * 0.5 <= 10.0 + 1e-12);
    }

    #[test]
    fn animation_steps_and_holds() {
        let ex = Example { kind: Kind::SuDl, t0: 1.0, t1: 1.0001, anim: vec![1.0, 2.0, 3.0], ..Default::default() };
        assert_eq!(ex.cursor_at(0.0), Some(1.0));
        assert_eq!(ex.cursor_at(ANIM_STEP * 1.5), Some(2.0));
        assert_eq!(ex.cursor_at(ANIM_STEP * 3.2), Some(1.0), "it starts over");
        assert_eq!(ex.cursor_at(f64::NAN), None);
        assert_eq!(Example::default().cursor_at(1.0), None);
    }

    #[test]
    fn nothing_to_find_in_an_empty_run() {
        let idx = VizIndex::build(Vec::new());
        assert!(find(&idx).is_empty());
    }

    #[test]
    fn scanning_starts_at_the_middle() {
        let ts = [0.0, 1.0, 2.0, 3.0, 4.0];
        assert_eq!(from_mid(5, |i| ts[i], 2.5).collect::<Vec<_>>(), vec![3, 4, 0, 1, 2]);
        assert_eq!(from_mid(5, |i| ts[i], 9.0).collect::<Vec<_>>(), vec![0, 1, 2, 3, 4]);
        assert_eq!(from_mid(0, |_| 0.0, 1.0).count(), 0);
    }
}
