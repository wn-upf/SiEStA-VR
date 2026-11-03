//! Viz only (`XRSIM_VIZ=1`): where the TXOP holder's AIFS and backoff countdown ran, for the viewer's overhead overlay
//! (`O`). Nothing here is read by the model.
//!
//! This MAC checks the channel on a 9 us slot tick. At a tick where the link is idle and AIFS has passed since the medium
//! went idle (`DcfStats::medium_free_since`), an EDCAF with frames to send drops its counter by one, and contends at
//! zero (in the same tick). The countdown before a TXOP is therefore the decrements of the holder's current draw in the
//! idle period that ends with the TXOP; `VizCountdown` records them as they happen. A queue that runs out of frames
//! (with MLO, when another link sent them) stops counting until new ones arrive: only the last unbroken run of
//! decrements, one per tick up to the TXOP, is reported.

use std::time::Duration;

use tai_time::TaiTime;

use crate::lib::t_secs;

/// The last unbroken run of decrements of one EDCAF's current draw in the current idle period.
#[derive(Clone, Copy, Debug, Default, Hash)]
pub struct VizCountdown {
    /// `medium_free_since` of the idle period the decrements below belong to (None: none of this draw yet).
    period: Option<TaiTime<0>>,
    first: Option<TaiTime<0>>,
    last: Option<TaiTime<0>>,
    count: u32,
}

/// The holder's channel access before a TXOP, as the viewer's `TxopStart` carries it (seconds; -1 = unknown).
#[derive(Clone, Copy, Debug)]
pub struct VizAccess {
    /// When the medium last went idle on the link.
    pub idle_since: f64,
    pub aifs_s: f64,
    /// Back-off slots drawn for this attempt.
    pub bo_drawn: u32,
    /// End of AIFS (`idle_since + aifs_s`).
    pub bo_origin: f64,
    /// The countdown in this idle period: from `bo_start` (one slot before the first decrement of the last unbroken run,
    /// not before AIFS ended) to `bo_zero_at` (the decrement to zero), `bo_slots` decrements. No decrement (the counter was already zero):
    /// `bo_start = bo_zero_at = bo_origin`, `bo_slots = 0`.
    pub bo_start: f64,
    pub bo_zero_at: f64,
    pub bo_slots: i32,
}

impl VizCountdown {
    /// The counter dropped by one at `now`, in the idle period that began at `idle_since`; the previous decrement more
    /// than a slot tick ago (`slot`) ends the run.
    pub fn on_decrement(&mut self, now: TaiTime<0>, idle_since: TaiTime<0>, slot: Duration) {
        let unbroken = self.last.is_some_and(|last| now <= last + slot + Duration::from_nanos(1));
        if self.period != Some(idle_since) || !unbroken {
            *self = VizCountdown { period: Some(idle_since), first: Some(now), last: None, count: 0 };
        }
        self.count += 1;
        self.last = Some(now);
    }

    /// A new back-off was drawn: the next decrements are a new countdown.
    pub fn reset(&mut self) {
        *self = VizCountdown::default();
    }

    /// The access window of a TXOP its EDCAF starts now, in the idle period that began at `idle_since`.
    pub fn window(&self, idle_since: TaiTime<0>, aifs: Duration, slot: Duration, bo_drawn: u32) -> VizAccess {
        let origin = idle_since + aifs;
        let (bo_start, bo_zero_at, bo_slots) = match (self.period == Some(idle_since), self.first, self.last) {
            (true, Some(first), Some(last)) if self.count > 0 => {
                // the first decrement ends the slot before it; that slot may begin while AIFS still runs (the tick's phase)
                let start = if first >= origin + slot { t_secs(first) - slot.as_secs_f64() } else { t_secs(origin) };
                (start, t_secs(last), self.count as i32)
            }
            _ => (t_secs(origin), t_secs(origin), 0),
        };
        VizAccess {
            idle_since: t_secs(idle_since),
            aifs_s: aifs.as_secs_f64(),
            bo_drawn,
            bo_origin: t_secs(origin),
            bo_start,
            bo_zero_at,
            bo_slots,
        }
    }
}

impl super::QueueModule {
    /// Snapshot of `key`'s back-off right after the MAC redrew it outside its slot tick (after a TXOP, or a win with
    /// nothing to send), so the viewer's backoff grid shows the new draw. Viz only.
    pub(super) fn viz_backoff_snap(&self, key: &crate::lib::MacKey, now: TaiTime<0>) {
        if !crate::lib::visualizer_queues_enabled() {
            return;
        }
        let st = self.dcf(key);
        self.emit_visualization_event(super::VizEvent::BackoffSnap {
            t: t_secs(now),
            mac_key: *key,
            counter: st.backoff_counter,
            cw: st.cw,
            frozen: st.backoff_frozen,
            medium_free_since: t_secs(st.medium_free_since),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(us: u64) -> TaiTime<0> {
        TaiTime::EPOCH + Duration::from_micros(us)
    }

    #[test]
    fn countdown_of_the_current_idle_period_only() {
        let (aifs, slot) = (Duration::from_micros(34), Duration::from_micros(9));
        let mut c = VizCountdown::default();
        // two decrements in an earlier idle period, then three in the one that ends with the TXOP
        c.on_decrement(at(100), at(50), slot);
        c.on_decrement(at(109), at(50), slot);
        for us in [1036, 1045, 1054] {
            c.on_decrement(at(us), at(1000), slot);
        }
        let w = c.window(at(1000), aifs, slot, 7);
        assert_eq!(w.bo_slots, 3);
        assert!((w.bo_origin - 1034e-6).abs() < 1e-12);
        // the first slot would begin at 1027 us, inside AIFS: clipped to its end
        assert!((w.bo_start - 1034e-6).abs() < 1e-12 && (w.bo_zero_at - 1054e-6).abs() < 1e-12);
        assert_eq!(w.bo_drawn, 7);
        // a TXOP after an idle period with no decrement: zero slots at the end of AIFS
        let w = c.window(at(2000), aifs, slot, 0);
        assert_eq!((w.bo_slots, w.bo_start, w.bo_zero_at), (0, w.bo_origin, w.bo_origin));
        // a pause (no frames to send) breaks the run: the last one counts, from a slot before its first decrement
        c.on_decrement(at(1300), at(1000), slot);
        c.on_decrement(at(1309), at(1000), slot);
        let w = c.window(at(1000), aifs, slot, 7);
        assert_eq!(w.bo_slots, 2);
        assert!((w.bo_start - 1291e-6).abs() < 1e-12 && (w.bo_zero_at - 1309e-6).abs() < 1e-12);
        // a new draw forgets the old countdown
        c.reset();
        assert_eq!(c.window(at(1000), aifs, slot, 0).bo_slots, 0);
    }
}
