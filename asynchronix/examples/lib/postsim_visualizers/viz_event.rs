//! The viewer's own copy of the MAC's visualisation events (`models_mm1k::VizEvent`), built from them when the viewer
//! opens (`from_mac`). The MAC of this build reports a TXOP's start and end, holder, destination, MCS and A-MPDU contents,
//! collisions, back-off snapshots, queue depths and bandwidth-pattern changes. The viewer's other fields stay at their
//! "unknown" value, which every panel already handles: no channel-access window before a TXOP (`idle_since < 0`), no
//! PPDU split into preamble and data symbols (`ppdu_t1 <= ppdu_t0`: the whole TXOP is drawn as one block), no per-MPDU
//! queue arrivals in the tooltip (the queue panel takes the arrivals from the queue depths instead).

use std::net::IpAddr;

use crate::lib::models_mm1k::VizEvent as MacVizEvent;
use crate::lib::MacKey;

#[derive(Clone, Debug)]
pub enum VizEvent {
    /// Medium grabbed for a successful TXOP on this link.
    TxopStart {
        t: f64,
        end: f64,
        link_id: u8,
        /// (sta, ac, link) of the TXOP holder.
        owner: MacKey,
        dest_id: i32,
        ampdu_packets: u16,
        mcs: u8,
        alvr_stream_ids: Vec<u16>,
        alvr_frame_ids: Vec<u32>,
        alvr_frame_losses: Option<Vec<u32>>,
        /// Queue this A-MPDU came from (the holder's in this build).
        sender: MacKey,
        /// Data symbols of the PPDU (absolute s); `ppdu_t1 <= ppdu_t0`: not reported, the whole TXOP is drawn.
        ppdu_t0: f64,
        ppdu_t1: f64,
        /// PHY preamble before `ppdu_t0` (0: not reported).
        preamble_s: f64,
        /// Instant the medium last went idle before this TXOP (`-1.0`: not reported) and the holder's AIFS.
        idle_since: f64,
        aifs_s: f64,
        /// RTS, CTS and BlockAck durations (0: not reported).
        ctrl_tx_s: f64,
        ctrl_rx_s: f64,
        resp_s: f64,
        /// Back-off slots drawn and the countdown's instants (`-1.0`: not reported).
        bo_drawn: u32,
        bo_origin: f64,
        bo_start: f64,
        bo_zero_at: f64,
        /// When each MPDU entered its MAC queue (empty: not reported).
        mpdu_arrivals: Vec<f64>,
    },
    /// Collision interval on this link.
    Collision {
        t: f64,
        end: f64,
        link_id: u8,
        contenders: smallvec::SmallVec<[MacKey; 4]>,
    },
    /// Back-off / AIFS snapshot, emitted on change.
    BackoffSnap {
        t: f64,
        mac_key: MacKey,
        counter: u32,
        cw: u32,
        frozen: bool,
        medium_free_since: f64,
    },
    /// Queue depth for one virtual queue.
    QueueDepth {
        t: f64,
        mac_key: MacKey,
        depth: usize,
        sta_src: i32,
        sta_dest: i32,
    },
    /// Bandwidth-pattern regime change on an emulated link.
    BandwidthChange {
        t: f64,
        end: f64,
        ip: IpAddr,
        state_name: Option<&'static str>,
        mbps: f32,
    },
}

/// The viewer's event for one of the MAC's.
pub fn from_mac(ev: MacVizEvent) -> VizEvent {
    match ev {
        MacVizEvent::TxopStart { t, end, link_id, owner, dest_id, ampdu_packets, mcs, alvr_stream_ids, alvr_frame_ids, alvr_frame_losses } =>
            VizEvent::TxopStart {
                t, end, link_id, owner, dest_id, ampdu_packets, mcs, alvr_stream_ids, alvr_frame_ids, alvr_frame_losses,
                sender: owner,
                ppdu_t0: t, ppdu_t1: t, preamble_s: 0.0,
                idle_since: -1.0, aifs_s: 0.0,
                ctrl_tx_s: 0.0, ctrl_rx_s: 0.0, resp_s: 0.0,
                bo_drawn: 0, bo_origin: -1.0, bo_start: -1.0, bo_zero_at: -1.0,
                mpdu_arrivals: Vec::new(),
            },
        MacVizEvent::Collision { t, end, link_id, contenders } => VizEvent::Collision { t, end, link_id, contenders },
        MacVizEvent::BackoffSnap { t, mac_key, counter, cw, frozen, medium_free_since } =>
            VizEvent::BackoffSnap { t, mac_key, counter, cw, frozen, medium_free_since },
        MacVizEvent::QueueDepth { t, mac_key, depth, sta_src, sta_dest } => VizEvent::QueueDepth { t, mac_key, depth, sta_src, sta_dest },
        MacVizEvent::BandwidthChange { t, end, ip, state_name, mbps } => VizEvent::BandwidthChange { t, end, ip, state_name, mbps },
    }
}

/// The viewer's events for the MAC's, in the same order.
pub fn from_mac_all(evs: Vec<MacVizEvent>) -> Vec<VizEvent> {
    evs.into_iter().map(from_mac).collect()
}
