use crate::{
    debug_bgprint,
    debug_debug,
    print_brown,
    print_magenta,
    print_pink,
    print_prettyyyy,
    print_red,
    print_yellow, // print_blue, print_dblue, print_green, print_pretty,
    print_dblue,
};
use crossbeam::channel::{unbounded, Receiver, Sender};
use rand::distributions::WeightedIndex;
use rand::Rng;
use std::cmp::{self, max};
use std::collections::{HashMap, VecDeque};
use std::f64::consts::PI;
use std::future::Future;
use std::hash::Hash;
use std::net::IpAddr;
// use std::hash::Hash;
use asynchronix::model::{Context, Model};
use asynchronix::ports::Output;
use rand::rngs::StdRng;
use rand_distr::{Distribution, Exp, Normal};
use std::time::{Duration, Instant};
// use crate::lib::TESTS_RANDOM_PATTERNS;
use crate::{debug_print, format_elapsed, taitime_to_f64, lib::t_secs, };
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tai_time::TaiTime;
use regex::Regex;
use crate::lib::ParquetQueue;


use crate::lib::{
    airtime_ampdu, alvr_stream_socket::parse_shard_data, collision_delay, exponential, ac_prio,
    perStaLockStats, AmpduPacket, Coords, CsvType, CumulativeStats, DebugColor, MacKey, MpduPacket,
    WindowKey, DEBUG_PRINT_ENABLED, DEFAULT_TMAX_AGG, DOWNLINK_QUEUE_SIZE, NUMBER_OF_RANDOM_EVENTS,
    P_TX, UPLINK_QUEUE_SIZE, DEBUG_EDCA, DEBUG_MLO, VISUALIZER_QUEUES_ENABLED, 
};

use rand::SeedableRng;
////////////////////////// CONSTS/////////////////////
pub const ROOM_W: f64 = 24.0;
pub const ROOM_H: f64 = 12.0;

/// Access Point at room center
pub const AP_X: f64 = ROOM_W / 2.0;
pub const AP_Y: f64 = ROOM_H / 2.0;

pub const MAX_EMULATED_QUEUE_PACKETS: usize = 10000;
pub const CSV_PER_PACKET: bool = true; // To collect Queueing times, Service, queue state, collisions per-packet in QUEUE_STATS.csv

/// TEMP DEBUG: per-packet trace of every `try_send_bandwidth` decision (outcome, token level,
/// rate, elapsed wait) to `stdout`. Very verbose — meant to be flipped on only while diagnosing
/// the emulated-link burst/thinning issue, then set back to `false` (or deleted) once resolved.
pub const BANDWIDTH_TRACE_ENABLED: bool = false;

pub const STEP1_TBEGIN: f64 = 10.0;
pub const STEP1_TEND: f64 = 20.0;

pub const STEP2_TBEGIN: f64 = 30.0;
pub const STEP2_TEND: f64 = 40.0;

pub const STEP3_TBEGIN: f64 = 50.0;
pub const STEP3_TEND: f64 = 60.0;

pub const BANDWIDTH_LIMIT_S1: f64 = 100E6;
pub const BANDWIDTH_LIMIT_S2: f64 = 95E6;
pub const BANDWIDTH_LIMIT_S3: f64 = 90E6;

pub const MTU_EMULATED: f64 = 1500.0 * 8.0 * 10.0; // allow bursts of N MTUs

/// Lowest frame rate this burst sizing needs to cover — the *longer* the frame period, the
/// more bits accumulate into one frame at a given rate, so the lowest configured fps is the
/// worst case for how wide a burst window needs to be (see `BANDWIDTH_BURST_MS`).
pub const BANDWIDTH_BURST_MIN_FPS: f64 = 60.0;

/// How many average-sized frames the token bucket must be able to admit in one shot, at
/// `BANDWIDTH_BURST_MIN_FPS`. 20x is deliberately generous — real keyframes only run ~1.8-2x
/// the mean frame size (measured off the HEVC/AV1 frame-size CSVs) — so this comfortably
/// covers keyframe spikes with headroom to spare, rather than being tuned to the exact
/// measured ratio.
pub const BANDWIDTH_BURST_FRAMES: f64 = 20.0;

/// Token-bucket burst capacity for every `NetworkPattern::Bandwidth`, expressed as
/// milliseconds of data *at that pattern's `token_refill_rate`* — see `new_bandwidth` for why
/// this is derived from the refill rate rather than from a fixed bit count or from `max_bps`.
///
/// A bigger burst window only changes how much can be admitted *instantaneously* after an idle
/// gap — it does not raise the sustained rate a flow can achieve, because the bucket is still
/// hard-capped at this size and only ever refills at `token_refill_rate` (the real, unchanged
/// bandwidth limit). Over any window of time `T`, total admitted bits stays bounded by
/// `bucket_capacity + token_refill_rate * T`, so the long-run average converges to
/// `token_refill_rate` regardless of how large this constant is — widening it trades *how
/// bursty* admission is allowed to be for *how much* it can burst, not the throughput limit
/// itself.
///
/// `avg_frame_bits(rate) = rate / fps`, so requiring
/// `bucket_capacity(rate) = rate * BANDWIDTH_BURST_MS / 1000 >= BANDWIDTH_BURST_FRAMES *
/// avg_frame_bits(rate)` simplifies to a rate-independent `BANDWIDTH_BURST_MS` — one constant
/// covers every Markov regime, not just whichever rate it was tuned against.
pub const BANDWIDTH_BURST_MS: f64 = 1000.0 * BANDWIDTH_BURST_FRAMES / BANDWIDTH_BURST_MIN_FPS;

/// netem's default `limit` (queue depth) in packets when a `qdisc ... netem rate <X>`
/// command doesn't specify one explicitly (see iproute2 q_netem.c: `#define DEFAULT_LIMIT 1000`).
pub const NETEM_DEFAULT_LIMIT_PACKETS: f64 = 1000.0;
/// Reference packet size (bits) used to convert netem's packet-count `limit` into an
/// equivalent worst-case queueing delay for a given rate.
pub const NETEM_REFERENCE_PACKET_BITS: f64 = 1500.0 * 8.0;

/// Token-bucket burst window (ms) for the flat NIC-speed cap only (see
/// `EmulatedLink::new_with_bandwidth`) — deliberately much smaller than `BANDWIDTH_BURST_MS`.
/// Sized to ~2 MTUs' worth of transmission time at a 1 Gbps NIC rate, so the bucket only ever
/// banks a couple of packets' worth of credit and `try_send_bandwidth` keeps debiting it
/// packet-by-packet, forcing the drain loop to re-check (and effectively pace) each packet
/// instead of letting a whole burst (e.g. one video frame) through in a single instantaneous
/// pass — restoring per-packet serialization at the NIC rate without reintroducing the old
/// separate `link_free_time` deadline mechanism.
pub const NIC_CAP_BURST_MS: f64 = 2.0 * NETEM_REFERENCE_PACKET_BITS / 1e9 * 1000.0;


pub const STR_PLUS_MODE_MLO: bool = true; // Set to true for STR+ mode, running backoffs and assigning traffic to link in last moment.

pub const MCS_REPORT_PERIOD_F32: f32 = 0.1; //

// pub const DEBUG_SCHEDULING: bool = false;
// pub const SOFTMAX_POLICY: bool = false;
// pub const LYAPUNOV_POLICY: bool = false;
// pub const LYAPUNOV_V: f64 = 5E7; // Lyapunov optimization parameter

//////////////////////// MACROS ////////////////////////////////
#[macro_export]
macro_rules! debug_edca {
    ($fmt:expr, $($arg:tt)*) => {
        if DEBUG_EDCA {
            let msg = format!($fmt, $($arg)*);
             println!("{}", DebugColor::Blue.to_background_fn()(msg));
        }
    };
}
#[macro_export]
macro_rules! debug_edca_r {
    ($fmt:expr, $($arg:tt)*) => {
        if DEBUG_EDCA {
            let msg = format!($fmt, $($arg)*);
             println!("{}", DebugColor::Red.to_background_fn()(msg));
        }

    };
}

#[macro_export]
macro_rules! debug_schedule {
    ($fmt:expr, $($arg:tt)*) => {
        // if DEBUG_SCHEDULING == true {
            let msg = format!($fmt, $($arg)*);
            println!("{}", DebugColor::Navy.to_background_fn()(msg));
        // }
    };
}
#[macro_export]
macro_rules! log_mlo {
    ($now:expr, $($arg:tt)*) => {
        if DEBUG_MLO {
            print!("\x1b[38;5;141m{} [MLO] ", format_elapsed!($now));
            println!($($arg)*);
            print!("\x1b[0m");
        }
    };
}
#[macro_export]
macro_rules! log_link_selection {
    ($now:expr, $($arg:tt)*) => {
        if DEBUG_MLO {
            print!("\x1b[38;5;87m{} [LINK-SEL] ", format_elapsed!($now));
            println!($($arg)*);
            print!("\x1b[0m");
        }
    };
}
#[allow(unused)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LinkSelectionStrategy {
    // Choose the channel that is free using backoffs
    Opportunistic,
    /// Always prefer primary link (link 0)
    PrimaryFirst,
    /// Choose link with lowest current load
    LoadBalanced,
    /// Choose link with best channel conditions
    QualityBased,
    /// Round-robin between available links
    RoundRobin,
    /// Use link that has shortest expected transmission time
    LowestLatency,
    // Lyapunov
    LyapunovBackpressure,
}
use std::fmt;
// Implement the standard library's Display trait
impl fmt::Display for LinkSelectionStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Use a match expression to map each variant to its desired string
        match self {
            LinkSelectionStrategy::Opportunistic => write!(f, "Opportunistic"),
            LinkSelectionStrategy::PrimaryFirst => write!(f, "PrimaryFirst"),
            LinkSelectionStrategy::LoadBalanced => write!(f, "LoadBalanced"),
            LinkSelectionStrategy::QualityBased => write!(f, "QualityBased"),
            LinkSelectionStrategy::RoundRobin => write!(f, "RoundRobin"),
            LinkSelectionStrategy::LowestLatency => write!(f, "LowestLatency"),
            LinkSelectionStrategy::LyapunovBackpressure => write!(f, "Backpressure"),
        }
    }
}

#[allow(unused)]
#[derive(Clone, Debug)]
pub struct StaRateInfo {
    total_transmission_delay_single: f64,
    total_transmission_delay_fullampdu: f64,
    fullampdu_max_size: usize,
    packet_count: usize,
    weighted_rate_single: f64,
    weighted_rate_fullampdu: f64,
    per_packet_channel_access_efficiency: f64,
    expected_queue_delivery_ms: f64,
}
#[allow(unused)]
pub fn softmax_with_temperature(values: &[f64], temperature: f64) -> Vec<f64> {
    if temperature <= 0.0 {
        panic!("Temperature must be greater than 0");
    }

    let max_value = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max); // Prevent overflow
    let exp_values: Vec<f64> = values
        .iter()
        .map(|&v| ((v - max_value) / temperature).exp())
        .collect();
    let sum_exp: f64 = exp_values.iter().sum();
    exp_values.iter().map(|&v| v / sum_exp).collect()
}

pub struct PoissonSource {
    pub arrival_rate: f64,
    pub mean_length_packets: f64,

    pub output_port: Output<MpduPacket>,

    pub num_packets_sent: usize,

    pub rng_seed: StdRng,
}

#[allow(dead_code)]
impl PoissonSource {
    pub fn new(arrival_rate_bps: f64, mean_length: f64, rng_seed: u64) -> Self {
        let arrival_rate = arrival_rate_bps / mean_length;
        Self {
            arrival_rate: arrival_rate,
            mean_length_packets: mean_length,
            output_port: Default::default(),
            num_packets_sent: 0,
            rng_seed: StdRng::seed_from_u64(rng_seed),
        }
    }
    fn send_packet<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let mut packet = MpduPacket::new();

            let mut time_interarrival =
                Duration::from_secs_f64(exponential(1.0 / self.arrival_rate, &mut self.rng_seed));
            time_interarrival = max(time_interarrival, Duration::from_secs_f64(1E-9));

            let len_random =
                exponential(self.mean_length_packets as f64, &mut self.rng_seed) as usize;
            packet.length_packet_bits = cmp::max(1, len_random); // Set here because the packet is not generated by a VR STA, but a BG STA (of which the 'application' is this self-scheduled function)

            self.num_packets_sent += 1;
            packet.packet_id = self.num_packets_sent;
            self.output_port.send(packet).await;

            context
                .scheduler
                .schedule_event(time_interarrival, Self::send_packet, ())
                .unwrap();
        }
    }
}

impl Model for PoissonSource {}

#[allow(non_camel_case_types)]
pub struct STA_source {
    // extended class to PoissonGen
    pub output_port: Output<MpduPacket>,

    pub sta_id: i32,
    pub destination_id: i32,

    pub arrival_rate: f64,
    pub mean_length_packets: f64,
    pub num_packets_sent: usize,
    pub received_packet_counter: usize,

    pub sta_coordinates: Coords,
    pub does_sta_tx: bool,
    pub rng_seed: StdRng,
}
#[allow(unused)]
impl STA_source {
    pub fn new(
        arrival_rate_bps: f64,
        mean_length: f64,
        src: i32,
        dest: i32,
        coordinates: Coords,
        does_sta_transmit: bool,
        rate_service_bps: f64,
        rng_seed: u64,
    ) -> Self {
        let arrival_rate = arrival_rate_bps / mean_length;
        let effective_mu = rate_service_bps / mean_length;
        // println!("\n*************************************************");
        // println!("[DEBUG STA{}]\tCoordinates: {:?}\n\tDestination: STA{} | RATE_IN: {:.3} Mbps, Rate_service: {:.3} (packs/s),\n\t Arrival_rate (pack/s): {:.3}, Departure_rate: {:.3},  L = {}",
        //                     src, coordinates, dest,                     arrival_rate_bps/1E6, rate_service_bps / 1E6 , arrival_rate,effective_mu ,mean_length);

        Self {
            output_port: Default::default(),

            sta_id: src,
            destination_id: dest,
            arrival_rate: arrival_rate,
            mean_length_packets: mean_length,
            num_packets_sent: 0,
            sta_coordinates: coordinates,

            received_packet_counter: 0,
            does_sta_tx: does_sta_transmit,
            rng_seed: StdRng::seed_from_u64(rng_seed),
        }
    }

    pub fn move_coordinates(&mut self, distance_to_move: f64) {
        // brownian movement for STA

        // Generate a random angle in spherical coordinates to determine the direction of movement
        let theta = self.rng_seed.gen_range(0.0..2.0 * PI); // azimuthal angle for x and y
        let phi = self.rng_seed.gen_range(0.0..PI); // polar angle for z-axis

        // Decompose the distance into x, y, and z components
        let dx = distance_to_move * theta.cos() * phi.sin();
        let dy = distance_to_move * theta.sin() * phi.sin();
        let dz = distance_to_move * phi.cos();

        println!("[MOVE STA COORDS] Before: {:?}", self.sta_coordinates);

        // Update the coordinates
        self.sta_coordinates.x += dx;
        self.sta_coordinates.y += dy;
        self.sta_coordinates.z += dz;
        println!("                  After: {:?}", self.sta_coordinates);
    }

    pub async fn input(&mut self, ampdu_packet: AmpduPacket, context: &Context<Self>) {
        let elapsed = context.scheduler.time();
        for packet in ampdu_packet.mpdu_packets {
            debug_print!(
                DebugColor::Red,
                "{} [DBG STA SRC {} IN]  ---Packet {} arrived from STA{} into STA{}",
                format_elapsed!(elapsed),
                self.sta_id,
                packet.packet_id,
                packet.sta_src_id,
                packet.sta_dest_id,
            );

            self.received_packet_counter += 1;
        }
    }

    pub fn send_packet<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            if self.does_sta_tx {
                // if STA is "TX type"         (and not "RX only")

                let mut packet = MpduPacket::new();

                let mut time_interarrival = Duration::from_secs_f64(exponential(
                    1.0 / self.arrival_rate,
                    &mut self.rng_seed,
                ));

                time_interarrival = max(time_interarrival, Duration::from_nanos(1));

                let len_random =
                    exponential(self.mean_length_packets as f64, &mut self.rng_seed) as usize;

                // let len_random = self.mean_length_packets as usize;

                packet.length_packet_bits = cmp::max(1, len_random); // Set here because the packet is not generated by a VR STA, but a BG STA (of which the 'application' is this self-scheduled function)
                packet.packet_id = self.num_packets_sent;

                packet.sta_src_id = self.sta_id;
                packet.sta_dest_id = self.destination_id;

                packet.sta_src_coords = self.sta_coordinates;

                self.output_port.send(packet).await;
                self.num_packets_sent += 1;

                context // reschedule this function
                    .scheduler
                    .schedule_event(time_interarrival, Self::send_packet, ())
                    .unwrap();
            }
        }
    }
}
impl Model for STA_source {}

#[derive(Clone)]
pub struct QueueStats {
    pub waiting_time_cum: CumulativeStats,
    pub service_time_cum: CumulativeStats,
    pub queue_length_counter: usize,
    pub num_packets_dropped: usize,
    pub num_packets_rx: usize,
}
impl QueueStats {
    pub fn new() -> Self {
        Self {
            waiting_time_cum: CumulativeStats::new(),
            service_time_cum: CumulativeStats::new(),
            queue_length_counter: 0,
            num_packets_dropped: 0,
            num_packets_rx: 0,
        }
    }
    pub fn update_cumstats(
        &mut self,
        ts: f64,
        tq: f64,
        packet_drops: usize,
        packets_rx: usize,
        queue_length: usize,
    ) {
        self.waiting_time_cum.add(tq);
        self.service_time_cum.add(ts);
        self.num_packets_dropped = packet_drops;
        self.num_packets_rx = packets_rx;
        self.queue_length_counter += queue_length;
    }

    pub fn print_nicely(&self) {
        let width = 48; // Total width of the table
        let separator = format!("+{}+", "-".repeat(width));

        // Calculate blocking probability
        let p_k = if self.num_packets_rx > 0 {
            self.num_packets_dropped as f64 / self.num_packets_rx as f64
        } else {
            0.0
        };

        // Helper closure to format a row
        let format_row = |label: &str, value: f64| format!("| {:<30} | {:>14.6} |", label, value);

        // Print the header
        // println!("DEBUUUUG {} / {} = {}", self.queue_length_counter , self.num_packets_rx, self.queue_length_counter as f64 / self.num_packets_rx as f64  );

        println!("{}", separator);
        println!(
            "{:^2}",
            "| QUEUE MODULE                                   |"
        );
        println!("{}", separator);
        // Print statistics
        println!("{}", format_row("P_k (Blocking Probability)", p_k));
        println!(
            "{}",
            format_row(
                "E[N_q]",
                self.queue_length_counter as f64 / self.num_packets_rx as f64
            )
        );
        println!(
            "{}",
            format_row(
                "E[T] (queue + tx)",
                self.waiting_time_cum.get_average() + self.service_time_cum.get_average()
            )
        );
        println!(
            "{}",
            format_row("E[T_q]", self.waiting_time_cum.get_average())
        );
        println!(
            "{}",
            format_row("E[T_s]", self.service_time_cum.get_average())
        );
        println!(
            "{}",
            format_row(
                "CV of T_s",
                self.service_time_cum.get_coefficient_variation()
            )
        );
        println!(
            "{}",
            format_row("2nd Moment of T_s", self.service_time_cum.get_2nd_moment())
        );

        // Print the footer
        println!("{}", separator);
    }
}

// First, let's add a new enum for distribution types
// #[derive(Clone, Debug) ]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")] // optional: emits "gaussian"/"uniform" instead of Rust‑style names
pub enum JitterDistributionType {
    Gaussian,
    Uniform,
}

impl Default for JitterDistributionType {
    fn default() -> Self {
        JitterDistributionType::Uniform
    }
}

#[allow(unused, unused_variables)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NetworkPattern {
    Constant,

    OnOffPeriodic {
        on_duration: Duration,
        off_duration: Duration,
        current_state: bool,

        #[serde(with = "taitime_serde")] // ② apply custom (de)serializer
        last_state_change: TaiTime<0>,
    },

    ProbabilisticDrop {
        drop_probability: f64,

        #[serde(with = "taitime_serde")]
        valid_from: TaiTime<0>,

        #[serde(with = "taitime_serde")]
        valid_until: TaiTime<0>,
    },

    Bandwidth {
        max_bps: f64,
        current_tokens: f64,
        max_tokens: f64,
        token_refill_rate: f64,
        last_refill: f64,
        /// Packets that would need to wait longer than this to drain are dropped
        /// instead of queued, mirroring netem's packet-count `limit` (default 1000
        /// packets) converted to an equivalent worst-case queueing delay at this rate.
        max_latency: Duration,

        #[serde(with = "taitime_serde")]
        valid_from: TaiTime<0>,

        #[serde(with = "taitime_serde")]
        valid_until: TaiTime<0>,
    },

    Jitter {
        mean_delay: Duration,
        distribution_type: JitterDistributionType,
        variance: f64,
        correlation_pct: f64,
        last_delay: Duration,

        #[serde(with = "taitime_serde")]
        valid_from: TaiTime<0>,

        #[serde(with = "taitime_serde")]
        valid_until: TaiTime<0>,
    },
}

use crate::lib::{HeaderALVRStream, taitime_serde};

#[allow(unused)]
impl NetworkPattern {
    /// Create a new `NetworkPattern` of type Bandwidth.
    ///
    /// `max_bps` is purely a *reported* cap — external readers (the Oracle ABR's regime-change
    /// scheduling in `xr_entry::mod`, the trace CSV, the visualizer) read this field directly to
    /// know "what's the limit for this segment", so it must always be the real rate, whatever the
    /// caller intends that to mean. It is NOT used for the internal token-bucket sizing below —
    /// conflating the two (making the bucket's capacity `max_bps` bits) previously made the
    /// bucket's refill-from-empty time equal to a fixed 1 second regardless of rate, which for
    /// bursty/framed traffic (arriving faster than it drains) meant only the very first burst was
    /// admitted whole; every later one tail-trickled out one packet at a time once that one-time
    /// credit was spent, killing 802.11 aggregation and collapsing real goodput. The bucket is
    /// instead sized off `token_refill_rate` as `BANDWIDTH_BURST_MS` worth of data at that rate,
    /// so it fully refills between bursts arriving faster than that (e.g. video frames) — letting
    /// each burst through in one shot — independent of whatever `max_bps` is reporting.
    ///
    /// `max_latency` mirrors netem's packet-count `limit` (default 1000 packets when
    /// unspecified, e.g. `tc qdisc add ... netem rate <X>`), converted to an equivalent
    /// worst-case queueing delay at this rate using a 1500-byte reference packet:
    /// `NETEM_DEFAULT_LIMIT_PACKETS * NETEM_REFERENCE_PACKET_BITS / token_refill_rate`.
    /// Packets that would need to wait longer than this are dropped rather than queued.
    ///
    /// `current_tokens` starts at 0, not `max_tokens`: a real `tc qdisc add/change ... netem
    /// rate <X>` grants no banked burst credit at activation — the very next packet is paced
    /// at the new rate immediately. Pre-filling the bucket here previously let every regime
    /// transition (e.g. each dwell step in a bandwidth test) absorb a `BANDWIDTH_BURST_MS`-sized
    /// burst for free with zero queueing delay, which shows up as the sim's RTT staying flat for
    /// a beat *after* the real cap has already kicked in on ALVR, before finally catching up —
    /// a phase lag at regime onset that real netem does not have. `max_tokens` still stays at
    /// `bucket_capacity` so bursts accumulated from *within-regime* idle gaps (e.g. between
    /// video frames, once the regime is already up and running) are unaffected — only the
    /// one-time free credit at regime start is removed.
    pub fn new_bandwidth(
        max_bps: f64,
        token_refill_rate: f64,
        valid_from: TaiTime<0>,
        valid_until: TaiTime<0>,
    ) -> Self {
        Self::new_bandwidth_with_burst_ms(
            max_bps,
            token_refill_rate,
            valid_from,
            valid_until,
            BANDWIDTH_BURST_MS,
        )
    }

    /// Same as `new_bandwidth`, but with the token-bucket burst window (normally
    /// `BANDWIDTH_BURST_MS`, sized to admit a whole video frame at once) overridden — used by
    /// the flat NIC-speed cap (`EmulatedLink::new_with_bandwidth`), which needs a per-packet
    /// serialization budget instead: at NIC rates (e.g. 1 Gbps) `BANDWIDTH_BURST_MS` worth of
    /// tokens is tens of megabits, big enough to admit an entire burst (e.g. one video frame's
    /// packets) in the same `drain()` pass with zero inter-packet delay, instead of pacing them
    /// out at the NIC rate like a real link would.
    pub fn new_bandwidth_with_burst_ms(
        max_bps: f64,
        token_refill_rate: f64,
        valid_from: TaiTime<0>,
        valid_until: TaiTime<0>,
        burst_ms: f64,
    ) -> Self {
        let valid_s = valid_from.duration_since(TaiTime::EPOCH).as_secs_f64();
        let max_latency = Duration::from_secs_f64(
            NETEM_DEFAULT_LIMIT_PACKETS * NETEM_REFERENCE_PACKET_BITS / token_refill_rate,
        );
        let bucket_capacity = token_refill_rate * burst_ms / 1000.0;

        Self::Bandwidth {
            max_bps,
            current_tokens: 0.0,         // No banked burst credit at regime activation
            max_tokens: bucket_capacity, // Maximum bucket capacity, reachable via in-regime refill
            token_refill_rate,
            last_refill: valid_s,
            max_latency,
            valid_from,
            valid_until,
        }
    }

    pub fn csv_headers() -> &'static [&'static str] {
        &[
            "effect_type",
            // OnOffPeriodic
            "on_duration_s",
            "off_duration_s",
            "current_state",
            "last_change_s",
            // ProbabilisticDrop
            "drop_probability",
            "pd_valid_from_s",
            "pd_valid_until_s",
            // Bandwidth
            "bw_max_bps",
            "bw_current_tokens",
            "bw_max_tokens",
            "bw_token_refill_rate",
            "bw_last_refill_s", // Added this field, as it's an f64 time
            "bw_valid_from_s",
            "bw_valid_until_s",
            // Jitter
            "jit_mean_delay_s",
            "jit_distribution",
            "jit_variance",
            "jit_correlation_pct",
            "jit_last_delay_s",
            "jit_valid_from_s",
            "jit_valid_until_s",
        ]
    }

    pub fn to_csv_row(&self) -> Vec<String> {
        // convenience closures to convert time values to f64 strings
        let d2f = |d: &Duration| d.as_secs_f64().to_string();
        let t2f = |t: &TaiTime<0>| t.duration_since(TaiTime::EPOCH).as_secs_f64().to_string();
        let f2s = |f: &f64| f.to_string();

        // start with effect_type
        // let mut row = vec![format!("{:?}", self)
        //     .split('(')
        //     .next()
        //     .unwrap()
        //     .to_string()];
        let mut row = vec![match self {
            NetworkPattern::OnOffPeriodic { .. } => "OnOffPeriodic",
            NetworkPattern::ProbabilisticDrop { .. } => "ProbabilisticDrop",
            NetworkPattern::Bandwidth { .. } => "Bandwidth",
            NetworkPattern::Jitter { .. } => "Jitter",
            NetworkPattern::Constant => "Constant",
        }
        .to_string()];

        // now push _all_ possible columns in the same order as csv_headers()
        match self {
            NetworkPattern::OnOffPeriodic {
                on_duration,
                off_duration,
                current_state,
                last_state_change,
            } => {
                row.push(d2f(on_duration));
                row.push(d2f(off_duration));
                row.push(current_state.to_string());
                row.push(t2f(last_state_change));

                // fill the rest with empties
                row.extend(
                    std::iter::repeat(String::new()).take(Self::csv_headers().len() - row.len()),
                );
            }

            NetworkPattern::ProbabilisticDrop {
                drop_probability,
                valid_from,
                valid_until,
            } => {
                // push blanks for OnOffPeriodic (4 fields)
                row.extend((0..4).map(|_| String::new()));

                row.push(drop_probability.to_string());
                row.push(t2f(valid_from));
                row.push(t2f(valid_until));

                // fill the rest
                row.extend(
                    std::iter::repeat(String::new()).take(Self::csv_headers().len() - row.len()),
                );
            }

            NetworkPattern::Bandwidth {
                max_bps,
                current_tokens,
                max_tokens,
                token_refill_rate,
                last_refill,
                valid_from,
                valid_until,
                max_latency: _,
            } => {
                // blanks for OnOffPeriodic (4) + ProbabilisticDrop (3) = 7 fields
                row.extend((0..7).map(|_| String::new()));

                row.push(f2s(max_bps));
                row.push(f2s(current_tokens));
                row.push(f2s(max_tokens));
                row.push(f2s(token_refill_rate));
                row.push(f2s(last_refill)); // Pushing the f64 last_refill time
                row.push(t2f(valid_from));
                row.push(t2f(valid_until));

                // fill the rest
                row.extend(
                    std::iter::repeat(String::new()).take(Self::csv_headers().len() - row.len()),
                );
            }

            NetworkPattern::Jitter {
                mean_delay,
                distribution_type,
                variance,
                correlation_pct,
                last_delay,
                valid_from,
                valid_until,
            } => {
                // blanks for OnOff (4) + ProbDrop (3) + Bandwidth (7) = 14 fields
                row.extend((0..14).map(|_| String::new()));

                row.push(d2f(mean_delay));
                row.push(format!("{:?}", distribution_type));
                row.push(f2s(variance));
                row.push(correlation_pct.to_string());
                row.push(d2f(last_delay));
                row.push(t2f(valid_from));
                row.push(t2f(valid_until));

                // fill the rest (should be 0)
                row.extend(
                    std::iter::repeat(String::new()).take(Self::csv_headers().len() - row.len()),
                );
            }

            NetworkPattern::Constant => {
                // nothing else to push—just pad out the full width
                row.extend(std::iter::repeat(String::new()).take(Self::csv_headers().len() - 1));
            }
        }

        row
    }

    pub fn new_jitter_gaussian(
        mean_delay_ms: f64,
        std_dev_ms: f64,
        correlation_pct: f64,
        valid_from: TaiTime<0>,
        valid_until: TaiTime<0>,
    ) -> Self {
        Self::Jitter {
            mean_delay: Duration::from_secs_f64(mean_delay_ms / 1000.0),
            distribution_type: JitterDistributionType::Gaussian,
            variance: std_dev_ms / 1000.0, // Convert ms to seconds
            correlation_pct: correlation_pct.clamp(0.0, 100.0),
            last_delay: Duration::from_secs_f64(mean_delay_ms / 1000.0), // Initialize with mean
            valid_from,
            valid_until,
        }
    }

    pub fn new_jitter_uniform(
        mean_delay_ms: f64,
        half_width_ms: f64,
        correlation_pct: f64,
        valid_from: TaiTime<0>,
        valid_until: TaiTime<0>,
    ) -> Self {
        Self::Jitter {
            mean_delay: Duration::from_secs_f64(mean_delay_ms / 1000.0),
            distribution_type: JitterDistributionType::Uniform,
            variance: half_width_ms / 1000.0, // Convert ms to seconds
            correlation_pct: correlation_pct.clamp(0.0, 100.0),
            last_delay: Duration::from_secs_f64(mean_delay_ms / 1000.0), // Initialize with mean
            valid_from,
            valid_until,
        }
    }

    /// Placeholder for creating other network patterns
    pub fn new_constant() -> Self {
        Self::Constant
    }
}

pub struct EmulatedLink {
    /// Can be inserted between any STA output and the `QueueModule` input.
    pub output: Output<MpduPacket>,
    queue_mechanism: QueueMechanism,
    /// Optional bandwidth emulation in bits per second (e.g., 1_000_000_000 for 1 Gbps)
    bandwidth_bps: Option<u64>,
}
#[allow(unused)]
impl EmulatedLink {
    /// Create a new emulated link.
    /// - `max_queue_size`: maximum packets to buffer in the emulator
    /// - `now`: current simulator time as a `TaiTime`
    /// - `tests`: tuple flags `(bandwidth, jitter, packet_loss, random_events, markov)`

    pub fn get_network_patterns(&self) -> &[NetworkPattern] {
        &self.queue_mechanism.network_emulator.get_patterns()
    }

    pub fn new(
        max_queue_size: usize,
        now: TaiTime<0>,
        emulated_tests: Option<(bool, bool, bool, bool, bool)>,
        id_sta: IpAddr,
        sim_duration_secs: f64,
        viz_tx: Option<crossbeam::channel::Sender<VizEvent>>,
    ) -> Self {
        let queue_mechanism: QueueMechanism;

        if let Some(values_tests) = emulated_tests {
            queue_mechanism =
                QueueMechanism::new(max_queue_size, now, values_tests, id_sta, sim_duration_secs, viz_tx.as_ref());
        } else {
            print_yellow!("NO PATTERNS?",);
            queue_mechanism = QueueMechanism::new(
                MAX_EMULATED_QUEUE_PACKETS,
                TaiTime::EPOCH,
                (false, false, false, false, false),
                id_sta,
                0.0,
                viz_tx.as_ref(),
            );
        }

        EmulatedLink {
            output: Default::default(),
            queue_mechanism,
            bandwidth_bps: None, // Disabled by default for backward compatibility
        }
    }

    /// Create a new emulated link with bandwidth emulation (e.g., 1 Gbps).
    /// - `bandwidth_bps`: bandwidth in bits per second (e.g., 1_000_000_000 for 1 Gbps)
    ///
    /// Enforced as an ordinary `NetworkPattern::Bandwidth`, going through the same token-bucket
    /// accounting as Markov/CBR patterns — *not* the old per-packet `link_free_time`
    /// serialization deadline. That old approach computed a single-packet-wide deadline at
    /// arrival and made `drain()` block strictly on it, so even packets from the same
    /// same-instant burst (e.g. one video frame) each needed their own separately-scheduled
    /// drain — one packet released per event, regardless of how much headroom the *actual*
    /// (Markov) bandwidth limit had. Folding it into the same pattern list lets a fast NIC cap
    /// (1 Gbps is far above any realistic Markov/CBR rate) stay a non-binding formality instead
    /// of becoming the accidental bottleneck that serializes every burst.
    ///
    /// Uses `NIC_CAP_BURST_MS`, not the default `BANDWIDTH_BURST_MS`, for its token bucket: the
    /// default is deliberately sized to admit a whole video frame at once for slow Markov/CBR
    /// patterns, but at NIC rates that same window is tens of megabits of burst credit — enough
    /// to let every packet of a burst drain in the same pass with no inter-packet delay, instead
    /// of pacing them out at the NIC rate the way a real link would.
    pub fn new_with_bandwidth(
        max_queue_size: usize,
        now: TaiTime<0>,
        emulated_tests: Option<(bool, bool, bool, bool, bool)>,
        id_sta: IpAddr,
        bandwidth_bps: Option<u64>,
        sim_duration_secs: f64,
        viz_tx: Option<crossbeam::channel::Sender<VizEvent>>,
    ) -> Self {
        let mut link = Self::new(max_queue_size, now, emulated_tests, id_sta, sim_duration_secs, viz_tx);
        link.bandwidth_bps = bandwidth_bps;
        if let Some(bw) = bandwidth_bps {
            let valid_until = now
                .checked_add(Duration::from_secs_f64(sim_duration_secs.max(1.0)))
                .unwrap_or(now);
            link.queue_mechanism.network_emulator.add_pattern(NetworkPattern::new_bandwidth_with_burst_ms(
                bw as f64,
                bw as f64,
                now,
                valid_until,
                NIC_CAP_BURST_MS,
            ));
        }
        link
    }

    /// Convenience method to create a 1 Gbps emulated link
    pub fn new_1gbps(
        max_queue_size: usize,
        now: TaiTime<0>,
        emulated_tests: Option<(bool, bool, bool, bool, bool)>,
        id_sta: IpAddr,
        sim_duration_secs: f64,
    ) -> Self {
        Self::new_with_bandwidth(
            max_queue_size,
            now,
            emulated_tests,
            id_sta,
            Some(1_000_000_000),
            sim_duration_secs,
            None,
        )
    }

    /// Enable or disable bandwidth emulation
    pub fn set_bandwidth(&mut self, bandwidth_bps: Option<u64>) {
        self.bandwidth_bps = bandwidth_bps;
    }

    /// Handle packet arrival from a STA. Applies emulation logic, possibly queuing or dropping.
    ///
    /// The flat NIC-speed cap (`bandwidth_bps`), if enabled, is applied as an ordinary
    /// `NetworkPattern::Bandwidth` added once in `new_with_bandwidth` — not here — so it goes
    /// through the same batch-capable token-bucket accounting as Markov/CBR patterns instead of
    /// a separate per-packet serialization deadline.
    pub async fn input(&mut self, packet: MpduPacket, context: &Context<Self>) {
        let now = context.scheduler.time();

        match self.queue_mechanism.enqueue(packet, now) {
            EnqueueOutcome::Dropped => {}
            EnqueueOutcome::Queued => {
                // If a flush is already pending, it'll reach this packet in due course. Otherwise,
                // *schedule* the first drain rather than running it inline: siblings from the same
                // logical burst (e.g. the rest of one video frame) are typically sent via separate
                // sequential calls into `input` at this same simulated instant. Draining synchronously
                // here would dispatch this packet alone — with the queue empty again before the next
                // sibling even exists — so nothing downstream ever sees them as a batch. Deferring by
                // one scheduler tick lets every same-instant arrival enqueue first; `drain()` then
                // sees the whole batch at once and can release it together (or ration it fairly, if
                // there isn't bandwidth for all of it).
                if self.queue_mechanism.next_flush_scheduled.is_none() {
                    self.schedule_drain_at(now, now, context);
                }
            }
        }
    }

    /// Drains as many queued packets as currently possible, evaluated fresh against whatever
    /// Bandwidth pattern is active *right now* — not a deadline computed once when a packet was
    /// enqueued. This is what lets an improving link speed up an existing backlog: a packet
    /// that queued behind a slow (e.g. Congested) regime gets re-checked against whatever
    /// regime is active by the time we get to it, instead of draining at a schedule frozen
    /// under the regime that was active when it arrived.
    async fn drain(&mut self, context: &Context<Self>) {
        let now = context.scheduler.time();
        self.queue_mechanism.next_flush_scheduled = None;

        // TEMP DEBUG: sample the emulated-queue depth at most once per 10 ms, so a backlog
        // building up during a bandwidth drop (bufferbloat) is visible as a time series without
        // emitting one line per drain call.
        let should_log_drain = match self.queue_mechanism.last_drain_logged {
            None => true,
            Some(last) => now.duration_since(last) >= Duration::from_millis(10),
        };
        if should_log_drain {
            self.queue_mechanism.last_drain_logged = Some(now);
            let now_s = now.duration_since(TaiTime::EPOCH).as_secs_f64();
            // print_red!(
            //     "[DRAIN-DBG {:.6} {}] queue_len={}",
            //     now_s,
            //     self.queue_mechanism.id_sta,
            //     self.queue_mechanism.queue.len()
            // );
        }

        loop {
            let Some(front) = self.queue_mechanism.queue.front() else { break };

            if now < front.bandwidth_eligible_at {
                let wake_at = front.bandwidth_eligible_at;
                self.schedule_drain_at(wake_at, now, context);
                return;
            }

            let bits = front.packet.length_packet_bits as f64;
            let elapsed_wait = now.duration_since(front.enqueued_at);
            match self.queue_mechanism.network_emulator.try_send_bandwidth(now, bits, elapsed_wait) {
                BandwidthOutcome::Sent => {
                    let front = self.queue_mechanism.queue.pop_front().unwrap();
                    self.output.send(front.packet).await;
                }
                BandwidthOutcome::Wait(retry_in) => {
                    let wake_at = now.checked_add(retry_in).unwrap_or(now);
                    self.schedule_drain_at(wake_at, now, context);
                    return;
                }
                BandwidthOutcome::ExceedsLimit => {
                    // Would need to wait longer than the emulated netem `limit` — drop rather
                    // than let it (and everything behind it, since this is FIFO) sit forever.
                    self.queue_mechanism.queue.pop_front();
                }
            }
        }
    }

    /// Schedules the next `drain` wake-up. Only ever called once per `drain` invocation (each
    /// call path `return`s right after), and `drain` always resets `next_flush_scheduled` to
    /// `None` at the top of its own call, so there's no earlier-vs-later comparison to make —
    /// just record the one pending wake-up and schedule it.
    fn schedule_drain_at(&mut self, wake_at: TaiTime<0>, now: TaiTime<0>, context: &Context<Self>) {
        self.queue_mechanism.next_flush_scheduled = Some(wake_at);
        let delay = wake_at.duration_since(now).max(Duration::from_nanos(1));
        context.scheduler.schedule_event(delay, Self::flush_queue, ()).unwrap();
    }

    pub fn flush_queue<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move { self.drain(context).await; }
    }
}

impl Model for EmulatedLink {}

pub enum EnqueueOutcome {
    Queued,
    Dropped, // ProbabilisticDrop hit, or the emulated queue was already at `max_queue_size`
}

/// A packet waiting in `QueueMechanism`'s queue, plus the scheduling metadata that lets
/// bandwidth service be re-evaluated against whatever pattern is active *at drain time* (see
/// `EmulatedLink::drain`) instead of a deadline frozen at arrival.
#[derive(Clone, Debug)]
struct QueuedPacket {
    packet: MpduPacket,
    /// When this packet was originally enqueued — used to measure its *actual* cumulative
    /// time-in-queue (see `EmulatedLink::drain`), as opposed to `try_send_bandwidth`'s own
    /// per-attempt wait estimate, which never sees more than one packet's worth of shortfall
    /// at a time and so can't by itself detect a backlog that's grown well past the emulated
    /// netem `limit`.
    enqueued_at: TaiTime<0>,
    /// Not eligible for bandwidth accounting before this — the NIC-level (`bandwidth_bps`)
    /// serialization deadline plus this packet's one-shot Jitter delay, both decided once at
    /// arrival since neither depends on backlog or on a rate that might later change.
    bandwidth_eligible_at: TaiTime<0>,
}

#[derive(Clone)]
pub struct QueueMechanism {
    queue: VecDeque<QueuedPacket>,             // Packet queue
    network_emulator: NetworkPatternEmulator, // Bandwidth pattern
    max_queue_size: usize,
    next_flush_scheduled: Option<TaiTime<0>>,
    last_bw_pattern_logged: Option<(TaiTime<0>, usize)>, // (last_log_time, pattern_index)
    last_drain_logged: Option<TaiTime<0>>,               // TEMP DEBUG: throttle for drain-depth log
    id_sta: IpAddr,
}

impl QueueMechanism {
    pub fn new(
        max_emulated_queue_packets: usize,
        _now: TaiTime<0>,
        tests: (bool, bool, bool, bool, bool),
        id_sta: IpAddr,
        sim_duration_secs: f64,
        viz_tx: Option<&crossbeam::channel::Sender<VizEvent>>,
    ) -> Self {
        let mut network_emulator = NetworkPatternEmulator::new(id_sta);

        let valid_from: TaiTime<0> = TaiTime::EPOCH
            .checked_add(Duration::from_secs_f64(STEP1_TBEGIN))
            .unwrap();
        let valid_until: TaiTime<0> = TaiTime::EPOCH
            .checked_add(Duration::from_secs_f64(STEP1_TEND))
            .unwrap();

        let valid_from2: TaiTime<0> = TaiTime::EPOCH
            .checked_add(Duration::from_secs_f64(STEP2_TBEGIN))
            .unwrap();
        let valid_until2: TaiTime<0> = TaiTime::EPOCH
            .checked_add(Duration::from_secs_f64(STEP2_TEND))
            .unwrap();

        let valid_from3: TaiTime<0> = TaiTime::EPOCH
            .checked_add(Duration::from_secs_f64(STEP3_TBEGIN))
            .unwrap();
        let valid_until3: TaiTime<0> = TaiTime::EPOCH
            .checked_add(Duration::from_secs_f64(STEP3_TEND))
            .unwrap();

        let (test_bw, test_jitter, test_pl, test_random, test_markov) = tests;

        // Assume these time values are defined appropriately:
        let overall_start = _now.checked_add(Duration::from_secs(10)).unwrap();
        let overall_end = _now.checked_add(Duration::from_secs(65)).unwrap();

        // print_red!("EMU EFFECTS APPLIED! {:?}", tests);
        if test_random {
            // network_emulator.add_random_events(
            //     NUMBER_OF_RANDOM_EVENTS,                                  // count: add 5 events
            //     RandomEventKind::Jitter,            // type of event
            //     overall_start,                      // overall start time for events
            //     overall_end,                        // overall end time for events
            //     Duration::from_millis(50),         // min duration for each event
            //     Duration::from_millis(500),         // max duration for each event
            //     JitterDistributionType::Uniform,    // distribution for the event duration (and jitter)
            //     8.0,                                // mean delay in ms
            //     6.0,                                // half-width (for Uniform jitter) or std dev (if Gaussian)
            // );

            // network_emulator.add_random_events(
            //     NUMBER_OF_RANDOM_EVENTS,                                  // Number of events
            //     RandomEventKind::PacketLoss,        // Event type: Packet Loss
            //     overall_start,                      // Overall window start time
            //     overall_end,                        // Overall window end time
            //     Duration::from_millis(50),          // Minimum duration per event
            //     Duration::from_millis(500),         // Maximum duration per event
            //     JitterDistributionType::Uniform,    // Distribution for event duration
            //     0.01,                                // Drop probability (mean_value)
            //     0.01,                                // Variance (not used for packet loss events)
            // );

            network_emulator.add_random_events(
                NUMBER_OF_RANDOM_EVENTS,         // Number of events
                RandomEventKind::Bandwidth,      // Event type: Bandwidth limit
                overall_start,                   // Overall window start time
                overall_end,                     // Overall window end time
                Duration::from_millis(3000),     // Minimum duration per event
                Duration::from_millis(15000),    // Maximum duration per event
                JitterDistributionType::Uniform, // Distribution for event duration
                50e6,                            // Maximum bps (1Mbps) as mean_value
                40e6,                            // Variance
            );
        }

        if test_markov {
            // Covers the whole simulation (from _now, not the 10s->65s demo window the other
            // test modes use above) so trace richness scales automatically with sim_duration_secs
            // instead of needing a hand-tuned event count/window.
            let markov_end = _now
                .checked_add(Duration::from_secs_f64(sim_duration_secs))
                .unwrap();
            network_emulator.add_markov_modulated_bandwidth(
                &WIFI_CLOUD_VR_STATES,
                &WIFI_CLOUD_VR_TRANSITIONS,
                _now,
                markov_end,
                0, // start in "Excellent"
                viz_tx,
            );
        }

        if test_bw {
            print_red!("****BW PATTERNS ADDED*****",);

            network_emulator.add_pattern(NetworkPattern::new_bandwidth(
                MTU_EMULATED,
                BANDWIDTH_LIMIT_S1,
                valid_from,
                valid_until,
            ));
            network_emulator.add_pattern(NetworkPattern::new_bandwidth(
                MTU_EMULATED,
                BANDWIDTH_LIMIT_S2,
                valid_from2,
                valid_until2,
            ));
            network_emulator.add_pattern(NetworkPattern::new_bandwidth(
                MTU_EMULATED,
                BANDWIDTH_LIMIT_S3,
                valid_from3,
                valid_until3,
            ));
        }

        if test_pl {
            network_emulator.add_pattern(NetworkPattern::ProbabilisticDrop {
                drop_probability: (0.01),
                valid_from: valid_from,
                valid_until: valid_until,
            });
            network_emulator.add_pattern(NetworkPattern::ProbabilisticDrop {
                drop_probability: (0.005),
                valid_from: valid_from2,
                valid_until: valid_until2,
            });

            // network_emulator.add_pattern(NetworkPattern::ProbabilisticDrop {
            //     drop_probability: (0.02),
            //     valid_from: valid_from3,
            //     valid_until: valid_until3,
            // });
        }
        if test_jitter {
            network_emulator.add_pattern(NetworkPattern::new_jitter_uniform(
                3.0, // mean delay in ms
                3.0, // standard deviation in ms
                0.0, // 20% correlation with previous packet delay
                valid_from,
                valid_until,
            ));

            // Example 2: Uniform jitter with 15ms mean delay and 10ms half-width
            network_emulator.add_pattern(NetworkPattern::new_jitter_uniform(
                5.0, // mean delay in ms
                5.0, // half-width in ms
                0.0, // no correlation with previous packet
                valid_from2,
                valid_until2,
            ));

            // Example 3: Highly correlated gaussian jitter (simulates slow fluctuations)
            network_emulator.add_pattern(NetworkPattern::new_jitter_uniform(
                10.0, // mean delay in ms
                10.0, // standard deviation in ms
                0.0,  // 80% correlation with previous packet delay
                valid_from3,
                valid_until3,
            ));
        }

        Self {
            queue: VecDeque::new(),
            network_emulator,
            max_queue_size: max_emulated_queue_packets,
            next_flush_scheduled: None,
            last_bw_pattern_logged: None,
            last_drain_logged: None,
            id_sta,
        }
    }

    /// Applies one-shot arrival effects (ProbabilisticDrop, Jitter) and, unless dropped or the
    /// queue is already full, pushes the packet in. Bandwidth is *not* decided here — see
    /// `EmulatedLink::drain`, which re-evaluates it fresh against whatever pattern is active at
    /// actual drain time instead of a deadline computed once now.
    pub fn enqueue(&mut self, packet: MpduPacket, now: TaiTime<0>) -> EnqueueOutcome {
        self.log_active_bw_patterns(now, &packet);
        match self.network_emulator.apply_arrival_effects(&packet, now) {
            ArrivalOutcome::Drop => EnqueueOutcome::Dropped,
            ArrivalOutcome::Delay(jitter_extra) => {
                if self.queue.len() >= self.max_queue_size {
                    return EnqueueOutcome::Dropped;
                }
                // Stack onto any NIC-level (bandwidth_bps) serialization deadline already set
                // by EmulatedLink, instead of clobbering it — the two delay sources compound.
                let nic_floor = packet.emulated_added_delay_deadline.unwrap_or(now).max(now);
                let bandwidth_eligible_at = nic_floor.checked_add(jitter_extra).unwrap_or(nic_floor);
                self.queue.push_back(QueuedPacket { packet, enqueued_at: now, bandwidth_eligible_at });
                EnqueueOutcome::Queued
            }
        }
    }

    fn log_active_bw_patterns(&mut self, now: TaiTime<0>, packet: &MpduPacket) {
        // Log at most once per second to avoid spam
        let should_log = match self.last_bw_pattern_logged {
            None => true,
            Some((last_time, _)) => now.duration_since(last_time) >= Duration::from_secs(1),
        };

        if !should_log {
            return;
        }

        // Find active BW patterns
        for (idx, pattern) in self.network_emulator.patterns.iter().enumerate() {
            if let NetworkPattern::Bandwidth {
                max_bps,
                current_tokens,
                max_tokens,
                token_refill_rate,
                valid_from,
                valid_until,
                ..
            } = pattern
            {
                if now >= *valid_from && now <= *valid_until {
                    // Format the message with traffic direction
                    let direction = if packet.sta_src_id > packet.sta_dest_id {
                        format!("UL: STA{}→AP", packet.sta_src_id)
                    } else {
                        format!("DL: AP→STA{}", packet.sta_dest_id)
                    };

                    // crate::print_dblue!(
                    //         "{:.6}[BW EMU {} ({:.5}->{:.5})] | {} | Limit: {:.2} Mbps | Tokens: {:.0}/{:.0} Mbits ",
                    //         format_elapsed!(now),
                    //         self.id_sta,
                    //         format_elapsed!(valid_from),
                    //         format_elapsed!(valid_until),
                    //         direction,
                    //         max_bps / 1e6,
                    //         current_tokens / 1e6,
                    //         max_tokens / 1e6,
                    //         // token_refill_rate / 1e6
                    //     );

                    self.last_bw_pattern_logged = Some((now, idx));
                    return; // Only log one pattern per call
                }
            }
        }
    }
}

#[allow(unused)]
#[derive(Clone, Debug)]
pub enum RandomEventKind {
    PacketLoss,
    Jitter,
    Bandwidth,
}

/// One state of a semi-Markov bandwidth process: a bps range sampled once per visit (one
/// "regime", e.g. a stable MCS/contention level) and a dwell-time range sampled per visit
/// from an exponential distribution around `mean_dwell_secs` (clamped to the given range so
/// a single unlucky sample can't produce a degenerate near-zero or runaway-long segment).
#[derive(Clone, Copy, Debug)]
pub struct MarkovBandwidthState {
    pub name: &'static str,
    pub bps_range: (f64, f64),
    pub dwell_range_secs: (f64, f64),
    pub mean_dwell_secs: f64,
}

/// Cloud-VR-over-Wi-Fi preset: 5 steady-state regimes (bandwidth set by 802.11ac/ax MCS
/// level and airtime contention) plus a short transient `Fade` state for body/multipath
/// blocking events. Order must match `WIFI_CLOUD_VR_TRANSITIONS` below.
///
/// Dwell times give each visit enough length (mean 6-8s) for an ABR agent running on a
/// ~1s control loop to actually settle on a bitrate and reap sustained reward from matching
/// it, rather than the regime changing before it can react. Combined with the transition
/// matrix below (tuned for a near-uniform time-share across Excellent/Good/Fair/Poor, with
/// Congested a bit rarer and Fade rare/brief), this deliberately over-represents Poor/
/// Congested relative to a real, typical Wi-Fi deployment — trading realism for a training
/// curriculum that reliably visits the whole bandwidth range every episode instead of
/// mostly sitting in Excellent/Good. If you want stock-realistic time shares back, bias
/// the transition matrix's self-loops/edges toward the top states again.
pub const WIFI_CLOUD_VR_STATES: [MarkovBandwidthState; 6] = [
    MarkovBandwidthState {
        name: "Excellent",
        bps_range: (90e6, 125e6),
        dwell_range_secs: (3.0, 20.0),
        mean_dwell_secs: 8.0,
    },
    MarkovBandwidthState {
        name: "Good",
        bps_range: (80e6, 110e6),
        dwell_range_secs: (3.0, 20.0),
        mean_dwell_secs: 10.0,
    },
    MarkovBandwidthState {
        name: "Fair",
        bps_range: (40e6, 80e6),
        dwell_range_secs: (3.0, 20.0),
        mean_dwell_secs: 10.0,
    },
    MarkovBandwidthState {
        name: "Poor",
        bps_range: (35e6, 65e6),
        dwell_range_secs: (3.0, 20.0),
        mean_dwell_secs: 10.0,
    },
    MarkovBandwidthState {
        name: "Congested",
        bps_range: (35e6, 45e6),
        dwell_range_secs: (2.0, 15.0),
        mean_dwell_secs: 6.0,
    },
    MarkovBandwidthState {
        name: "Fade",
        bps_range: (15e6, 20e6),
        dwell_range_secs: (0.1, 2.0),
        mean_dwell_secs: 0.6,
    },
];

/// Row-stochastic transition matrix for `WIFI_CLOUD_VR_STATES` (state indices: 0=Excellent,
/// 1=Good, 2=Fair, 3=Poor, 4=Congested, 5=Fade). Each row sums to 1.0.
///
/// States 0-4 form a birth-death chain (only adjacent-severity moves, e.g. Excellent can't
/// jump straight to Congested) with *symmetric* edge weights (0.20 each way at every edge):
/// for a birth-death chain, adjacent-pair detailed balance means the embedded-chain
/// stationary distribution only depends on the ratio of paired edge weights, not on the
/// self-loop size — so equal edges alone give Excellent/Good/Fair/Poor a near-identical
/// long-run time share (~20% each) regardless of how sticky each self-loop is.
/// `Congested` reuses Excellent's "only one neighbor" shape, ending up slightly rarer
/// (~15-16%). Every state 0-4 leaks 6% probability to `Fade`, which is not sticky (no
/// self-loop) and returns uniformly to one of states 0-4 — since Fade's own dwell is tiny
/// (mean 0.6s) this keeps it a rare, brief excursion (<1% of total time) rather than a
/// persistent regime, which is the one place realism (fades are brief) is kept strictly.
pub const WIFI_CLOUD_VR_TRANSITIONS: [[f64; 6]; 6] = [
    // Excellent
    [0.74, 0.20, 0.00, 0.00, 0.00, 0.06],
    // Good
    [0.15, 0.54, 0.25, 0.00, 0.00, 0.06],
    // Fair
    [0.00, 0.20, 0.54, 0.20, 0.00, 0.06],
    // Poor
    [0.00, 0.00, 0.20, 0.54, 0.20, 0.06],
    // Congested
    [0.00, 0.00, 0.00, 0.20, 0.74, 0.06],
    // Fade (returns uniformly to any ambient regime; never self-loops)
    [0.20, 0.20, 0.20, 0.20, 0.20, 0.00],
];

#[allow(unused)]
#[derive(Clone, Debug)]
pub struct NetworkPatternEmulator {
    patterns: Vec<NetworkPattern>,
    ip_parent: IpAddr,
}
impl NetworkPatternEmulator {
    pub fn new(ip_parent: IpAddr) -> Self {
        Self {
            patterns: Vec::new(),
            ip_parent,
        }
    }

    pub fn get_patterns(&self) -> &[NetworkPattern] {
        &self.patterns
    }

    pub fn add_random_events(
        &mut self,
        count: usize,
        event_type: RandomEventKind,
        overall_start: TaiTime<0>,
        overall_end: TaiTime<0>,
        min_duration: Duration,
        max_duration: Duration,
        dist: JitterDistributionType,
        mean_value: f64,
        variance: f64,
    ) {
        if count == 0 {
            return; // Nothing to do
        }

        let overall_duration = overall_end.duration_since(overall_start);
        let overall_duration_secs = overall_duration.as_secs_f64();

        // --- New logic for non-overlapping, exponential gaps ---

        // 1. Estimate average event duration to calculate average gap time.
        //    This is a heuristic to parameterize the exponential distribution.
        let avg_duration_secs = (min_duration.as_secs_f64() + max_duration.as_secs_f64()) / 2.0;
        let total_avg_event_time_secs = avg_duration_secs * (count as f64);

        // 2. Calculate total time available for gaps.
        //    We use .max(1e-9) to avoid division by zero if events take all the time.
        let total_gap_time_secs = (overall_duration_secs - total_avg_event_time_secs).max(1e-9);

        // 3. Calculate the mean time for one gap. We plan for 'count' gaps
        //    (including the initial gap from overall_start).
        let mean_gap_secs = total_gap_time_secs / (count as f64);

        let min_gap_secs = 4.0;

        // 4. Create the exponential distribution for the gaps.
        //    The rate (lambda) is 1 / mean.
        let rate_lambda = 1.0 / mean_gap_secs;
        let gap_dist = Exp::new(rate_lambda)
            .expect("Failed to create exponential gap distribution. Check calculations.");

        let mut current_time = overall_start; // This tracks the end of the last event
        let mut events_added = 0;
        let mut rng = rand::thread_rng();
        // --- End of new logic ---

        // Modified loop: Use a while loop to add "as many as possible"
        while events_added < count {
            // --- New: Generate gap and event_start ---
            // Generate an exponentially distributed gap time.
            let gap_secs_sample = gap_dist.sample(&mut rng);
            let gap_secs = gap_secs_sample + min_gap_secs;

            let event_start = match current_time.checked_add(Duration::from_secs_f64(gap_secs)) {
                Some(time) => time,
                None => break, // Break if time addition overflows
            };

            // Determine event duration (existing logic)
            let duration_secs = match dist {
                JitterDistributionType::Uniform => {
                    let min = min_duration.as_secs_f64();
                    let max = max_duration.as_secs_f64();
                    rng.gen_range(min..max)
                }
                JitterDistributionType::Gaussian => {
                    let center = (min_duration.as_secs_f64() + max_duration.as_secs_f64()) / 2.0;
                    // Note: The 2nd param to Normal::new is std_dev, not variance.
                    // Assuming 'variance' param *means* std_dev for this case.
                    let normal = Normal::new(center, variance)
                        .unwrap_or_else(|_| Normal::new(center, 0.1).unwrap());
                    let sample = normal.sample(&mut rng);
                    sample
                        .max(min_duration.as_secs_f64())
                        .min(max_duration.as_secs_f64())
                }
            };
            let event_duration = Duration::from_secs_f64(duration_secs);
            let event_end = match event_start.checked_add(event_duration) {
                Some(time) => time,
                None => break, // Break if time addition overflows
            };

            // Check if the event fits within the overall window.
            if event_end > overall_end {
                // This event doesn't fit, and no subsequent events will either.
                break;
            }

            // Debug log (existing)
            print_prettyyyy!(
                DebugColor::Green,
                "Creating event: {:?}, start: {:?}, duration: {:?}, intensity: {}",
                event_type,
                event_start,
                event_duration,
                mean_value
            );

            let std_dev = variance.sqrt(); // Used for PacketLoss

            let normal = Normal::new(mean_value, std_dev).expect("Invalid distribution parameters");
            let mut drop_probability = normal.sample(&mut rng);
            drop_probability = drop_probability.clamp(0.0, 1.0);

            // Create the event based on its type. (existing logic)
            let pattern = match event_type {
                RandomEventKind::PacketLoss => NetworkPattern::ProbabilisticDrop {
                    drop_probability: drop_probability,
                    valid_from: event_start,
                    valid_until: event_end,
                },
                RandomEventKind::Jitter => {
                    match dist {
                        JitterDistributionType::Uniform => NetworkPattern::new_jitter_uniform(
                            mean_value,
                            variance,
                            0.0,
                            event_start,
                            event_end,
                        ),
                        JitterDistributionType::Gaussian => NetworkPattern::new_jitter_gaussian(
                            mean_value,
                            variance, // Note: param is likely std_dev, not variance
                            0.0,
                            event_start,
                            event_end,
                        ),
                    }
                }
                RandomEventKind::Bandwidth => {
                    let normal_bw = Normal::new(mean_value, variance)
                        .unwrap_or_else(|_| Normal::new(mean_value, 0.1 * mean_value).unwrap());

                    let z: f64 = normal_bw.sample(&mut rand::thread_rng()).max(15e6); // minimum 15 Mbps

                    crate::print_dblue!(
                        "[{}] Bandwidth period: {:.5} -> {:.5} | sampled {:.2} Mbps",
                        self.ip_parent,
                        format_elapsed!(event_start),
                        format_elapsed!(event_end),
                        z / 1e6
                    );
                    NetworkPattern::new_bandwidth(z, z, event_start, event_end)
                }
            };

            self.add_pattern(pattern);

            events_added += 1;
            current_time = event_end; // The next event's gap starts after this one ends
                                      // --- End of new logic ---
        }

        // --- New: Add log statement for shortfall ---
        if events_added < count {
            // TODO: Replace println! with your application's logger (e.g., log::warn!)
            println!(
                "WARN: Requested {} random events, but only {} could be added without overlap in the given time window.",
                count, events_added
            );
        }
    }

    /// Fills `[overall_start, overall_end)` with contiguous `Bandwidth` patterns driven by a
    /// semi-Markov chain over `states`/`transitions` (see `WIFI_CLOUD_VR_STATES` for a ready
    /// preset). Unlike `add_random_events`, richness is not tied to a manually-tuned event
    /// `count`: the walk simply keeps emitting segments until it reaches `overall_end`, so the
    /// same state/transition table produces equally rich traces for a 1-minute or 1-hour run.
    ///
    /// Each visit to a state samples one bandwidth value (a "regime", e.g. one MCS/contention
    /// level) for its whole dwell, and one dwell duration (exponential around
    /// `mean_dwell_secs`, clamped to `dwell_range_secs`) before transitioning to the next
    /// state per `transitions[current_state]`. Because states cover the entire window with no
    /// gaps, there's no "default/unconstrained" bandwidth to fall back into between segments,
    /// unlike the sparse overlay events from `add_random_events`.
    pub fn add_markov_modulated_bandwidth<const N: usize>(
        &mut self,
        states: &[MarkovBandwidthState; N],
        transitions: &[[f64; N]; N],
        overall_start: TaiTime<0>,
        overall_end: TaiTime<0>,
        initial_state: usize,
        viz_tx: Option<&crossbeam::channel::Sender<VizEvent>>,
    ) {
        let mut rng = rand::thread_rng();
        let mut current_state = initial_state;
        let mut current_time: TaiTime<0> = overall_start;

        while current_time < overall_end {
            let state = &states[current_state];

            let (min_dwell, max_dwell) = state.dwell_range_secs;
            let dwell_dist = Exp::new(1.0 / state.mean_dwell_secs)
                .expect("mean_dwell_secs must be positive");
            let dwell_secs = dwell_dist.sample(&mut rng).clamp(min_dwell, max_dwell);

            let seg_end = match current_time.checked_add(Duration::from_secs_f64(dwell_secs)) {
                Some(t) => t.min(overall_end),
                None => overall_end,
            };

            // .min()/.max() guards against a state whose bps_range got entered/edited backwards
            // — gen_range panics on an inverted (empty) range instead of just swapping it.
            let (bw_min, bw_max) = state.bps_range;
            let bps = rng.gen_range(bw_min.min(bw_max)..=bw_min.max(bw_max));

            crate::print_dblue!(
                "[{}] Markov state {} : {:.5} -> {:.5} | {:.2} Mbps",
                self.ip_parent,
                state.name,
                format_elapsed!(current_time),
                format_elapsed!(seg_end),
                bps / 1e6
            );

            self.add_pattern(NetworkPattern::new_bandwidth(
                bps,
                bps,
                current_time,
                seg_end,
            ));

            if VISUALIZER_QUEUES_ENABLED {
                if let Some(tx) = viz_tx {
                    let _ = tx.try_send(VizEvent::BandwidthChange {
                        t: current_time.duration_since(TaiTime::EPOCH).as_secs_f64(),
                        end: seg_end.duration_since(TaiTime::EPOCH).as_secs_f64(),
                        ip: self.ip_parent,
                        state_name: Some(state.name),
                        mbps: (bps / 1e6) as f32,
                    });
                }
            }

            current_time = seg_end;

            let weights = &transitions[current_state];
            current_state = WeightedIndex::new(weights)
                .expect("each transition row must have a positive weight sum")
                .sample(&mut rng);
        }
    }

    pub fn add_pattern(&mut self, pattern: NetworkPattern) {
        self.patterns.push(pattern);
    }

    /// One-shot, per-packet effects decided at arrival: ProbabilisticDrop (may drop outright)
    /// and Jitter (an extra fixed delay). Unlike Bandwidth, neither depends on queue backlog or
    /// on a rate that might change while this packet later waits, so there's no correctness
    /// reason to defer them to drain time — see `try_send_bandwidth` for that.
    pub fn apply_arrival_effects(&mut self, packet: &MpduPacket, now: TaiTime<0>) -> ArrivalOutcome {
        self.apply_arrival_effects_labeled(now, &|| packet.print(DebugColor::Red))
    }

    /// Same as `apply_arrival_effects`, for callers with no `MpduPacket` (e.g. real-network
    /// mode's raw shard bytes) — the drop-log line falls back to a generic label instead of
    /// the packet's own debug print.
    pub fn apply_arrival_effects_bits(&mut self, now: TaiTime<0>) -> ArrivalOutcome {
        self.apply_arrival_effects_labeled(now, &|| "<real-net shard>".to_string())
    }

    fn apply_arrival_effects_labeled(
        &mut self,
        now: TaiTime<0>,
        label: &dyn Fn() -> String,
    ) -> ArrivalOutcome {
        let mut total_delay = Duration::ZERO;

        for pattern in self.patterns.iter_mut() {
            match pattern {
                NetworkPattern::ProbabilisticDrop { drop_probability, valid_from, valid_until } => {
                    if now < *valid_from || now > *valid_until {
                        continue;
                    }
                    let mut rng = rand::thread_rng();
                    let rand_value: f64 = rng.gen();

                    if rand_value < *drop_probability {
                        print_red!(
                            "[{} | RANDOM LOSS ( {:.5} -> {:.5} )]  prob= {:.4}! {:?}",
                            format_elapsed!(now),
                            format_elapsed!(*valid_from),
                            format_elapsed!(*valid_until),
                            *drop_probability,
                            label()
                        );
                        return ArrivalOutcome::Drop;
                    }
                }

                NetworkPattern::Jitter {
                    mean_delay,
                    distribution_type,
                    variance,
                    correlation_pct,
                    last_delay,
                    valid_from,
                    valid_until,
                } => {
                    if now < *valid_from || now > *valid_until {
                        continue;
                    }
                    let mut rng = rand::thread_rng();

                    let random_component = match distribution_type {
                        JitterDistributionType::Gaussian => {
                            let normal = rand_distr::Normal::new(0.0, *variance).unwrap();
                            rng.sample(normal)
                        }
                        // gen_range panics on an empty range, which -variance..variance
                        // is whenever variance == 0.0 (a fixed, non-random delay -- a
                        // valid and common config, not an edge case to reject).
                        JitterDistributionType::Uniform if *variance > 0.0 => {
                            rng.gen_range(-*variance..*variance)
                        }
                        JitterDistributionType::Uniform => 0.0,
                    };

                    let correlated_offset = if *correlation_pct > 0.0 {
                        let last_deviation = last_delay.as_secs_f64() - mean_delay.as_secs_f64();
                        last_deviation * (*correlation_pct / 100.0)
                    } else {
                        0.0
                    };

                    let new_delay_secs =
                        (mean_delay.as_secs_f64() + random_component + correlated_offset).max(0.0);
                    *last_delay = Duration::from_secs_f64(new_delay_secs);

                    total_delay += Duration::from_secs_f64(new_delay_secs);
                }

                NetworkPattern::Bandwidth { .. }
                | NetworkPattern::Constant
                | NetworkPattern::OnOffPeriodic { .. } => continue,
            }
        }

        ArrivalOutcome::Delay(total_delay)
    }

    /// Attempts to account `bits` against every Bandwidth pattern active *right now*, refilling
    /// each for elapsed time first. Nothing is debited unless *all* currently-active patterns
    /// have enough tokens (a two-phase peek-then-commit, so a packet that fails on one pattern
    /// never partially double-charges another). This is re-evaluated fresh on every call — a
    /// packet that couldn't be sent against a slow (e.g. Congested) regime a moment ago gets
    /// re-checked against whatever regime is active *now*, instead of a one-shot deadline
    /// computed once and left to go stale. Compare to a real qdisc/token bucket, which
    /// re-checks token availability at actual dequeue time rather than precomputing a future
    /// send time when a packet is enqueued — an improving link speeds up an existing backlog
    /// instead of ignoring it.
    ///
    /// Returns `Sent` if accounted for now, `Wait(estimate)` if not enough tokens yet (nothing
    /// debited — `estimate` is just a scheduling hint, re-derived at the next attempt), or
    /// `ExceedsLimit` if that estimated wait already exceeds the most restrictive active
    /// pattern's `max_latency` (the emulated netem `limit`) — checked fresh each attempt rather
    /// than frozen at arrival, so a packet isn't condemned by a since-improved worst case.
    /// `elapsed_wait` is how long this packet has *already* sat in the queue (since it was
    /// originally enqueued), not just what this one attempt needs — see the `ExceedsLimit`
    /// check below for why the per-attempt wait alone can't detect a backlog that's grown past
    /// the emulated netem `limit`.
    pub fn try_send_bandwidth(&mut self, now: TaiTime<0>, bits: f64, elapsed_wait: Duration) -> BandwidthOutcome {
        let now_s = now.duration_since(TaiTime::EPOCH).as_secs_f64();

        let mut any_active = false;
        let mut all_sufficient = true;
        let mut max_wait = Duration::ZERO;
        let mut min_limit: Option<Duration> = None;

        // TEMP DEBUG (BANDWIDTH_TRACE_ENABLED): snapshot of the last active pattern touched
        // this call, purely for the trace print below — remove once the burst/thinning issue
        // is root-caused.
        let mut trace_tokens = 0.0_f64;
        let mut trace_max_tokens = 0.0_f64;
        let mut trace_rate = 0.0_f64;

        for pattern in self.patterns.iter_mut() {
            let NetworkPattern::Bandwidth {
                valid_from,
                valid_until,
                max_latency,
                current_tokens,
                max_tokens,
                token_refill_rate,
                last_refill,
                ..
            } = pattern
            else {
                continue;
            };
            if now < *valid_from || now > *valid_until {
                continue;
            }
            any_active = true;

            let dt = now_s - *last_refill;
            *current_tokens = (*current_tokens + dt * *token_refill_rate).min(*max_tokens);
            *last_refill = now_s;

            if *current_tokens < bits {
                all_sufficient = false;
                let need = bits - *current_tokens;
                max_wait = max_wait.max(Duration::from_secs_f64(need / *token_refill_rate));

                // Only a pattern that's actually short on tokens (i.e. genuinely constraining
                // this packet) contributes its netem `limit` to the drop threshold. A
                // non-binding pattern — notably the fast flat NIC cap (1 Gbps), whose bucket is
                // never empty at realistic loads — must NOT: its `max_latency` is tiny (1000
                // packets drain in ~12ms at 1 Gbps) and, if folded in via `min`, would collapse
                // the effective drop threshold far below the real (slow) bottleneck's limit,
                // dropping packets — and thus whole video frames — that the real bandwidth limit
                // would have queued and delivered fine.
                min_limit = Some(min_limit.map_or(*max_latency, |m: Duration| m.min(*max_latency)));
            }

            trace_tokens = *current_tokens;
            trace_max_tokens = *max_tokens;
            trace_rate = *token_refill_rate;
        }

        if !any_active {
            return BandwidthOutcome::Sent;
        }
        if all_sufficient {
            for pattern in self.patterns.iter_mut() {
                let NetworkPattern::Bandwidth { valid_from, valid_until, current_tokens, .. } = pattern else {
                    continue;
                };
                if now >= *valid_from && now <= *valid_until {
                    *current_tokens -= bits;
                }
            }
            if BANDWIDTH_TRACE_ENABLED {
                print_dblue!(
                    "[BW-TRACE {:.6} {}] Sent    bits={:.0} tokens={:.0}/{:.0} rate={:.2}Mbps elapsed_wait={:.3}ms",
                    now_s, self.ip_parent, bits, trace_tokens, trace_max_tokens, trace_rate / 1e6,
                    elapsed_wait.as_secs_f64() * 1000.0
                );
            }
            return BandwidthOutcome::Sent;
        }
        if elapsed_wait + max_wait > min_limit.unwrap() {
            if BANDWIDTH_TRACE_ENABLED {
                print_red!(
                    "[BW-TRACE {:.6} {}] ExceedsLimit bits={:.0} tokens={:.0}/{:.0} rate={:.2}Mbps elapsed_wait={:.3}ms max_wait={:.3}ms limit={:.3}ms",
                    now_s, self.ip_parent, bits, trace_tokens, trace_max_tokens, trace_rate / 1e6,
                    elapsed_wait.as_secs_f64() * 1000.0, max_wait.as_secs_f64() * 1000.0,
                    min_limit.unwrap().as_secs_f64() * 1000.0
                );
            }
            return BandwidthOutcome::ExceedsLimit;
        }
        if BANDWIDTH_TRACE_ENABLED {
            print_yellow!(
                "[BW-TRACE {:.6} {}] Wait    bits={:.0} tokens={:.0}/{:.0} rate={:.2}Mbps elapsed_wait={:.3}ms max_wait={:.3}ms",
                now_s, self.ip_parent, bits, trace_tokens, trace_max_tokens, trace_rate / 1e6,
                elapsed_wait.as_secs_f64() * 1000.0, max_wait.as_secs_f64() * 1000.0
            );
        }
        BandwidthOutcome::Wait(max_wait)
    }
}

/// Convenience wrapper around `add_markov_modulated_bandwidth` pinned to the
/// `WIFI_CLOUD_VR_STATES`/`WIFI_CLOUD_VR_TRANSITIONS` preset -- used by real-network mode's
/// `net_emu_writer` so it doesn't need to know the preset's type parameters, only that it
/// wants "the Markov Wi-Fi trace".
pub fn add_markov_wifi_cloud_vr_bandwidth(
    emulator: &mut NetworkPatternEmulator,
    overall_start: TaiTime<0>,
    overall_end: TaiTime<0>,
    initial_state: usize,
    viz_tx: Option<&crossbeam::channel::Sender<VizEvent>>,
) {
    emulator.add_markov_modulated_bandwidth(
        &WIFI_CLOUD_VR_STATES,
        &WIFI_CLOUD_VR_TRANSITIONS,
        overall_start,
        overall_end,
        initial_state,
        viz_tx,
    );
}

pub enum ArrivalOutcome {
    Drop,
    Delay(Duration),
}

pub enum BandwidthOutcome {
    Sent,
    Wait(Duration),
    ExceedsLimit,
}

#[derive(Debug, Default)]
pub struct StatsUpdate {
    pub T_s: f64,
    pub T_q: f64,
    pub blocked_packet_counter: usize,
    pub arrived_packet_counter: usize,
    pub queue_length_when_out: usize,
    pub sta_src_id: i32,
    pub sta_dest_id: i32,
    pub packet_id: i32,
    pub now: tai_time::TaiTime<0>,
    pub length_packet: usize,
    pub edca_ac: EdcaAc,

    pub ampdu_id: u32,

    pub is_collision: bool,
    pub collision_backoff: f64,
    pub link_id: u8,
    pub alvr_header:HeaderALVRStream,  
}
#[derive(Clone, Copy, Debug, Hash)]
pub struct EdcaParam {
    pub cw_min: u32,
    pub cw_max: u32,
    pub aifsn: u8,          // slots added to DIFS
    pub txop_limit_us: u16, // 0 = no TXOP
}
const SIFS_US: u64 = 16; // 16 µs
const SLOT_TIME_US: u64 = 9; // 9 µs for OFDM

fn aifs(p: EdcaParam) -> Duration {
    Duration::from_micros(SIFS_US + p.aifsn as u64 * SLOT_TIME_US)
}

pub const EDCA_TABLE: [EdcaParam; 4] = [
    /* VO */
    EdcaParam {
        cw_min: 3,
        cw_max: 7,
        aifsn: 2,
        txop_limit_us: 1504,
    },
    /* VI */
    EdcaParam {
        cw_min: 7,
        cw_max: 15,
        aifsn: 2,
        txop_limit_us: 3008,
    },
    /* BE */
    EdcaParam {
        cw_min: 15,
        cw_max: 1023,
        aifsn: 3,
        txop_limit_us: 0,
    },
    /* BK */
    EdcaParam {
        cw_min: 15,
        cw_max: 1023,
        aifsn: 7,
        txop_limit_us: 0,
    },
];

#[derive(Hash, Clone, Debug)]
pub struct DcfStats {
    pub mac_key: MacKey, 
    pub cw: u32,
    pub backoff_counter: u32,
    pub retry_count: u8,
    pub param: EdcaParam,              // <-- NEW (static per AC)
    pub medium_free_since: TaiTime<0>, // NEW –
    pub backoff_frozen: bool,          // NEW
    pub edca_ac_str: String,
    pub last_backoff_drawn_logs: u32, // Only for logging to CSV the 'drawn' BO value.
                                      // pub slot_timer_event,
}

pub const MAX_RETRIES_MAC: u8 = 10;
use crate::lib::EdcaAc;

impl DcfStats {
    pub fn new(ac: EdcaAc, mac_key: MacKey, ) -> Self {
        let p = EDCA_TABLE[ac as usize];

        let edca_ac_str = match ac {
            EdcaAc::Voice => "AC_VO",
            EdcaAc::Video => "AC_VI",
            EdcaAc::Background => "AC_BK",
            EdcaAc::BestEffort => "AC_BE",
        };

        let drawn_randomly_bo = rand::thread_rng().gen_range(0..=p.cw_min);

        Self {
            mac_key, 
            cw: p.cw_min,
            backoff_counter: drawn_randomly_bo,
            retry_count: 0,
            param: p,
            medium_free_since: TaiTime::EPOCH,
            backoff_frozen: false,
            edca_ac_str: edca_ac_str.to_string(),
            last_backoff_drawn_logs: drawn_randomly_bo,
        }
    }

    /// Draw a new random back-off inside the current CW.
    pub fn reload_backoff(&mut self) {
        let randomly_drawn = rand::thread_rng().gen_range(0..=self.cw);

        self.backoff_counter = randomly_drawn.clone(); //this one 'ticks down'
        self.last_backoff_drawn_logs = randomly_drawn.clone(); // this one logs the drawn value for each tx.
    }

    /// Call after a **successful** transmission.
    pub fn on_success(&mut self, cw_min: u32) {
        self.cw = cw_min;
        self.retry_count = 0;
        self.reload_backoff();
    }

    /// Call after a **collision or PHY-error** that requires a retry.
    pub fn on_failure(&mut self) -> bool {
        // print_red!("PHY collision or err: {:#?}", self);
        self.retry_count += 1;
        if self.retry_count > MAX_RETRIES_MAC {
            // drop MSDU – tell caller to flush the head-of-line
            self.cw = self.param.cw_min;
            self.retry_count = 0;
            self.reload_backoff();
            return false; // “give up”
        }
        self.cw = ((self.cw + 1) * 2) - 1; // CW = 2·(CW+1) − 1

        if self.cw > self.param.cw_max {
            self.cw = self.param.cw_max;
        }
        self.reload_backoff();
        true // keep packet
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Medium {
    busy_until: TaiTime<0>,          // actual airtime occupied
    nav_until: TaiTime<0>,           // virtual carrier sense (from Duration fields)
    tx_owner: Option<MacKey>,        // who currently holds TXOP (only while busy)
    last_txop_owner: Option<MacKey>, // who last held a TXOP (sticky for logging)
    last_txop_end: TaiTime<0>,       // when that TXOP ended}

    max_busy_reached: TaiTime<0>, // High-water mark to prevent double-counting overlapping busy periods
    accumulated_busy_s: f32,      // Total seconds the medium has spent busy
}
impl Medium {
    #[inline]
    pub fn is_idle(&self, now: TaiTime<0>) -> bool {
        now >= self.busy_until && now >= self.nav_until
    }
    #[inline]
    pub fn start_txop(&mut self, now: TaiTime<0>, t: TaiTime<0>, owner: MacKey) {

        self.integrate_busy_time(now, t);
        self.busy_until = t;
        self.tx_owner = Some(owner)
    }
    /// Busy due to collision/backoff/NAV (no owner)
    #[inline]
    pub fn occupy_collision(&mut self, now: TaiTime<0>, until: TaiTime<0>) {
        self.integrate_busy_time(now, until);
        self.tx_owner = None;
        self.busy_until = until;
    }
    #[inline]
    pub fn set_nav_until(&mut self, now: TaiTime<0>, t: TaiTime<0>) {
        self.integrate_busy_time(now, t);
        self.nav_until = t;
    }

    /// Release an owned TXOP at `now` (called exactly when TX completes)
    #[inline]
    pub fn release_txop(&mut self, now: TaiTime<0>) {
        if let Some(owner) = self.tx_owner {
            self.last_txop_owner = Some(owner);
            self.last_txop_end = now;
        }
        self.tx_owner = None;
        // keep busy_until as-is; caller may immediately schedule contention next
    }

    /// Clear stale owner if medium is idle (safety net)
    #[inline]
    pub fn clear_if_idle(&mut self, now: TaiTime<0>) {
        if self.is_idle(now) {
            self.tx_owner = None;
        }
    }

    #[inline]
    pub fn _current_owner(&self) -> Option<MacKey> {
        if let Some(owner) = self.tx_owner {
            Some(owner)
        } else {
            None
        }
    }
    #[inline]
    pub fn _last_owner(&self) -> Option<MacKey> {
        if let Some(owner) = self.last_txop_owner {
            Some(owner)
        } else {
            None
        }    
    }
    #[inline]
    pub fn _last_end(&self) -> TaiTime<0> {
        self.last_txop_end
    }

    #[inline]
    fn integrate_busy_time(&mut self, now: TaiTime<0>, until: TaiTime<0>) {
        // Start counting from either `now` or our highest recorded busy time, whichever is later.
        // (This prevents counting time that has already passed, or double-counting overlaps)
        let start = if now > self.max_busy_reached { now } else { self.max_busy_reached };

        if until > start {
            // Assuming TaiTime can be converted to f32 seconds.
            // Adjust this subtraction to match your specific TaiTime API if needed.
            let added_duration = until.duration_since(TaiTime::EPOCH).as_secs_f32() - start.duration_since(TaiTime::EPOCH).as_secs_f32();
            self.accumulated_busy_s += added_duration;
            self.max_busy_reached = until;
        }
    }

    #[inline]
    pub fn get_accumulated_busy_s(&self) -> f32 {
        self.accumulated_busy_s
    }

    /// Calculates overall utilization from the start of the simulation [0.0 to 1.0]
    #[inline]
    pub fn overall_utilization(&self, opt_sim_start_time: Option<TaiTime<0>>, now: TaiTime<0>) -> f32 {
        let sim_start_time = if let Some(tai_time) = opt_sim_start_time {
            tai_time
        } else {
            TaiTime::EPOCH
        };
        let total_time = now.duration_since(TaiTime::EPOCH).as_secs_f32() - sim_start_time.duration_since(TaiTime::EPOCH).as_secs_f32();
        if total_time <= 0.0 { return 0.0; }
        (self.accumulated_busy_s / total_time).clamp(0.0, 1.0)
    }

}

#[derive(Clone, Debug)]
pub struct StaCapabilities {
    pub _is_str_capable: bool,
    pub links: Vec<u8>,
}
#[derive(Clone, Debug)]
pub struct LinkConfig {
    pub link_id: u8,
    pub _frequency_ghz: f64, // 5 or 6
    pub bandwidth_mhz: u16,  // 80, 160, 320
}
// Helper for the fallback case
fn default_mloconfig(config: &str) -> Vec<LinkConfig> {
    print_red!("WARNING WRONG MLO STRING ({config}) || DEFAULTING TO SLO80!!", );
    vec![LinkConfig {
        link_id: 0,
        _frequency_ghz: 5.0,
        bandwidth_mhz: 80,
    }]
}

pub fn create_mlo_config(config: &str) -> Vec<LinkConfig> {
    // 1. Identify the prefix and the "values" part
    // Matches MLO or SLO followed by a sequence of digits and hyphens
    let re = Regex::new(r"^(SLO|MLO)([\d\-]+)$").unwrap();

    let caps = match re.captures(config) {
        Some(c) => c,
        None => return vec![], // Or handle error appropriately
    };

    let mode = &caps[1];
    let values_str = &caps[2];

    // 2. Split the values by '-' and parse them into integers
    let bandwidths: Vec<u16> = values_str
        .split('-')
        .filter_map(|s| s.parse::<u16>().ok())
        .collect();

    match mode {
        "SLO" => {
            let bw = bandwidths.first().cloned().unwrap_or(80);
            vec![LinkConfig {
                link_id: 0,
                _frequency_ghz: 5.0,
                bandwidth_mhz: bw,
            }]
        }
        "MLO" => {
            bandwidths
                .into_iter()
                .enumerate()
                .map(|(i, bw)| LinkConfig {
                    link_id: i as u8,
                    // Example logic for frequency mapping
                    _frequency_ghz: if i == 0 { 5.0 } else { 6.0 },
                    bandwidth_mhz: bw,
                })
                .collect()
        }
        _ => vec![],
    }
}

#[derive(Clone, Debug)]
pub struct WindowMetricReport {
    pub timestamp: f64,
    pub window_key: WindowKey, // (STA_ID, LINK_ID)
    pub avg_mcs: f64,
    pub total_util: f64,
    pub sta_util: f64,
}

#[derive(Clone, Default, Debug)]
pub struct WindowMetrics {
    pub sta_id: WindowKey,
    pub window_start: f64,
    pub window_duration: Duration,

    // Utilization Accumulators
    pub total_busy_time: f64,    // Total channel busy time (all STAs)
    pub sta_tx_time: f64,        // Time this STA spent transmitting
    pub sta_collision_time: f64, // Time wasted in collisions for this STA

    // MCS Accumulators
    pub mcs_sum: f64,        // Sum of MCS indices (for average)
    pub packet_count: usize, // Number of packets to compute average
}

impl WindowMetrics {
    pub fn new(duration_window_f32: f32, sta_id: WindowKey) -> Self {
        Self {
            sta_id,
            window_duration: Duration::from_secs_f32(duration_window_f32),
            ..Default::default()
        }
    }
    pub fn reset(&mut self, now: f64) {
        *self = Self {
            window_start: now,
            sta_id: self.sta_id,
            window_duration: self.window_duration,
            ..Default::default()
        };
    }

    #[inline]
    pub fn update_metrics_mcs_util(&mut self, mcs: Option<u8>, duration: f64, success: bool) {
        if success {
            if let Some(emecese) = mcs {
                self.sta_tx_time += duration;
                self.total_busy_time += duration; // Assuming successful TX contributes to total busy time
                self.packet_count += 1; // Only count succesful transmissions for MCS.
                self.mcs_sum += emecese as f64;

                // print_pink!("UPDATE! Metrics: tx_time: {}, busy_time: {}, packet_count: {}, mcs_avg: {}",
                //                      self.sta_tx_time, self.total_busy_time, self.packet_count, self.mcs_sum / self.packet_count as f64);
            }
        } else {
            self.sta_collision_time += duration;
            self.total_busy_time += duration; // Collisions also consume channel time
        }
    }

    // Returns (Avg MCS, Total Util, STA Util)
    pub fn compute_and_reset(&mut self, now: f64) -> (WindowKey, f64, f64, f64) {
        let duration = now - self.window_start;
        if duration <= 0.0 {
            return (self.sta_id, 0.0, 0.0, 0.0);
        }

        let avg_mcs = if self.packet_count > 0 {
            self.mcs_sum / self.packet_count as f64
        } else {
            0.0
        };
        // Relative time spent in transmissions + collisions
        let sta_util = (self.sta_tx_time + self.sta_collision_time) / duration;
        let total_util = self.total_busy_time / duration;

        self.reset(now);
        (self.sta_id, avg_mcs, total_util, sta_util)
    }
}


#[derive(Clone, Debug)]
pub enum VizEvent { // Visualization events for the medium state, emitted for logging/plotting purposes
    /// Medium grabbed for a successful TXOP on this link.
    TxopStart {
        t: f64,
        end: f64,
        link_id: u8,
        owner: MacKey,         // (sta, ac, link)
        dest_id: i32, 
        ampdu_packets: u16,
        mcs: u8,
        alvr_stream_ids: Vec<u16>, // Hold ALVR StreamIDs contained in AMPDU
        alvr_frame_ids: Vec<u32>,  // Added
        alvr_frame_losses: Option<Vec<u32>>, 
        
    },
    /// Collision interval on this link.
    Collision {
        t: f64,
        end: f64,
        link_id: u8,
        contenders: smallvec::SmallVec<[MacKey; 4]>,
    },
    /// Backoff/AIFS snapshot — emit ONLY on change, not every slot.
    BackoffSnap {
        t: f64,
        mac_key: MacKey,
        counter: u32,
        cw: u32,
        frozen: bool,
        medium_free_since: f64,  // for the AIFS bar
    },
    /// Queue depth for one virtual queue.
    QueueDepth {
        t: f64,
        mac_key: MacKey,
        depth: usize,
        sta_src: i32, //new field for per-device queue
        sta_dest: i32, //new field for per-device queue
    },
    /// Bandwidth-pattern regime change on an emulated link — emitted once per segment as a
    /// semi-Markov bandwidth chain (see `add_markov_modulated_bandwidth`) transitions into a
    /// new state, so post-sim viewers can overlay the applied bandwidth trace.
    BandwidthChange {
        t: f64,
        end: f64,
        ip: IpAddr,
        state_name: Option<&'static str>,
        mbps: f32,
    },
}

// #[allow(unused)]
// #[derive(Clone)]
pub struct QueueModule {
    // pub output_port_sta1: Output<AmpduPacket>,
    pub link_outputs: HashMap<u8, Output<AmpduPacket>>, // AP's Tx ports (key=link_id)
    // pub queue: VecDeque<MpduPacket>,
    // pub queue: Vec<MpduPacket>,
    pub per_flow_queues: Vec<VecDeque<MpduPacket>>,

    pub queue_maxsize_dl: usize,
    pub service_timer: Duration,
    // pub aux_packet_serviced: MpduPacket,
    pub aux_ampdu_serviced: AmpduPacket,

    // pub packet_being_served: bool,
    pub link_is_transmitting: HashMap<u8, bool>,

    pub blocked_packet_counter: usize,
    pub arrived_packet_counter: usize,
    pub queue_length_counter: usize,
    // pub arrival_rate: f64,
    pub service_rate: f64,
    // pub t0_time: Instant,
    // pub csv_metrics: CsvType,
    pub csv_metrics_network: Option<CsvType>,
    pub parquet_metrics_network: Option<ParquetQueue>,

    pub coords_queue: Coords,
    pub p_tx: f64,

    pub STA_coords_grid: Vec<Coords>,

    pub STA_coords_map: HashMap<usize, Coords>,

    pub cumulative_stats_queue: Arc<Mutex<QueueStats>>,

    pub stats_tx: Option<Sender<StatsUpdate>>,
    pub stats_rx: Option<Receiver<StatsUpdate>>,

    pub array_stas_stats: Arc<Mutex<HashMap<usize, perStaLockStats>>>,

    // pub array_dcf_values: Arc<Mutex<HashMap<MacKey, DcfStats>>>,
    pub sta_stats_cache: HashMap<(i32, i32), StaRateInfo>, // for caching per-sta stats, performance optimization
    pub PL_probability: f64,

    // pub queue_network_emulator: QueueMechanism,
    pub ul_capacity_queue_device: usize,
    pub ampdu_id: u32,

    // pub shared_medium: Medium,
    pub link_mediums: HashMap<u8, Medium>, // State for each link (key=link_id)
    pub link_channel_widths: HashMap<u8, usize>, // Store channel width per link
    pub sta_capabilities: HashMap<i32, StaCapabilities>, // (key=sta_id)
    pub link_queue_depths: HashMap<u8, usize>, // Holds packets assigned to each link with MLO: Required for optimization to stop iterating O(n) over queue
    // pub mac_queue_depths: HashMap<MacKey, usize>, // Holds every EDCA_AC/STA_ID queue sizes for logging. 
    // pub mac_queue_depths: Vec<usize>, // flat, no mutex needed if single-threaded DES, indexed by mac_key_index
    // pub array_dcf_values: Arc<Mutex<HashMap<MacKey, DcfStats>>>,
    pub dcf_values: Vec<DcfStats>,  // flat, no mutex needed if single-threaded DES
    pub mac_key_index: HashMap<MacKey, usize>,  // built once at init, never mutated
    pub num_links: usize, 


    pub mlo_linkselection_strat: LinkSelectionStrategy,
    pub packs_per_ampdu: usize,
    pub window_metrics_mcs_util: Arc<Mutex<HashMap<WindowKey, WindowMetrics>>>, //  K: STA_ID, V: Metrics on temporal window, Arc<Mutex<>> for accessibility
    pub output_metrics: Output<WindowMetricReport>, // Just to report MCS and utilization in real time, for each STA.
    pub viz_tx: Option<crossbeam::channel::Sender<VizEvent>>,
    pub active_mac_key_counts: HashMap<(i32, EdcaAc), usize>,

    pub slot_tick_pending: bool,
}
#[allow(unused)]
impl QueueModule {
    pub fn new(
        num_stas: usize,
        queue_size: usize,
        PL_prob: f64,
        vec_ids: Vec<i32>,
        folder_dir: String,
        emulated_tests: Option<(bool, bool, bool, bool, bool)>,
        link_configs: Vec<LinkConfig>, // NEW: Configure available links
        mlo_linkselection_strat: LinkSelectionStrategy,
        packs_per_ampdu: usize,
        results_path: &str, 
        viz_tx: Option<crossbeam::channel::Sender<VizEvent>>,
    ) -> Self {

        let mut stats_vec: HashMap<usize, perStaLockStats> = HashMap::new();
        let mut windows_vec = HashMap::new();

        let (stats_tx, stats_rx) = unbounded();
        let num_links = link_configs.len();

        let total_entries = (num_stas + 1) * num_links * 4;     // index = sta_idx * (num_links * 4 ACs) + link_idx * 4 ACs + ac_idx
        let mut dcf_values: Vec<DcfStats> = Vec::with_capacity(total_entries);
        let mut mac_key_index: HashMap<MacKey, usize> = HashMap::with_capacity(total_entries);


        // let mut mac_queue_depths = vec![0usize; total_entries];

        let sta_idx_of = |sta_id: i32| -> usize {
            if sta_id == -1 {
                num_stas
            } else {
                vec_ids.iter().position(|&id| id == sta_id)
                    .expect("sta_id not in vec_ids")
            }
        };

        let dummy_key = (-2, EdcaAc::BestEffort, 0);
        dcf_values.resize_with(total_entries, || DcfStats::new(EdcaAc::BestEffort, dummy_key));


        // Initialize per-STA stats
        for i in 0..num_stas {
            let sta_stats = perStaLockStats::new();

            if let Ok(mut stats) = sta_stats.data.clone().lock() {
                stats.sta_id = vec_ids[i] as i32;
                stats_vec.insert(stats.sta_id.clone() as usize, sta_stats.clone());
            }
        }
        for (link_idx, link_config) in link_configs.iter().enumerate() {
            let link_id = link_config.link_id;

            // AP: sta_id = -1
            for ac in [EdcaAc::Voice, EdcaAc::Video, EdcaAc::BestEffort, EdcaAc::Background] {
                let wind_key = (-1, link_id);
                let mac_key: MacKey = (-1, ac, link_id);
                
                let idx = num_stas * (num_links * 4) + link_idx * 4 + Self::ac_idx(ac);
                
                // 2. NOW this works, because len() is equal to total_entries
                dcf_values[idx] = DcfStats::new(ac, mac_key);
                
                mac_key_index.insert(mac_key, idx);
                windows_vec.insert(
                    wind_key,
                    WindowMetrics::new(MCS_REPORT_PERIOD_F32, wind_key),
                );
            }

            // STAs
            for (s_idx, &sta_id) in vec_ids.iter().enumerate() {
                for ac in [EdcaAc::Voice, EdcaAc::Video, EdcaAc::BestEffort, EdcaAc::Background] {
                    let mac_key: MacKey = (sta_id, ac, link_id);
                    let idx = s_idx * (num_links * 4) + link_idx * 4 + Self::ac_idx(ac);
                    
                    // This overwrites the dummy data safely
                    dcf_values[idx] = DcfStats::new(ac, mac_key);
                    mac_key_index.insert(mac_key, idx);
                    
                    let wind_key: (i32, u8) = (sta_id, link_id);
                     windows_vec.insert(
                        wind_key,
                        WindowMetrics::new(MCS_REPORT_PERIOD_F32, wind_key),
                    );
                }
            }
        }
        
        // Initialize link mediums
        let mut link_mediums = HashMap::new();
        let mut link_outputs = HashMap::new();
        let mut link_channel_widths = HashMap::new();
        let mut link_queue_depths = HashMap::new(); // <-- ADD THIS

        let mut being_served = HashMap::new();

        for link_config in &link_configs {
            link_queue_depths.insert(link_config.link_id, 0usize); // ← add this
            link_mediums.insert(link_config.link_id, Medium::default());
            link_channel_widths.insert(link_config.link_id, link_config.bandwidth_mhz as usize);
            link_outputs.insert(link_config.link_id, Output::default());
            being_served.insert(link_config.link_id, false);
        }
        let csv_metrics_network = if crate::lib::NETWORK_CSV_LOGGING{Some(CsvType::new(&folder_dir, results_path).expect("?? CSVTYPE"))} else{None}; 
        let parquet_metrics_network: Option<ParquetQueue> = if crate::lib::NETWORK_PARQUET_LOGGING{Some(ParquetQueue::new(&folder_dir, results_path).expect("?? PARQUETTYPE"))} else{None};

        Self {
            // queue: VecDeque::with_capacity(queue_size),
            // queue: Vec::with_capacity(queue_size),
            per_flow_queues: vec![VecDeque::new(); total_entries],
            // queue_maxsize_k:        queue_size,
            queue_maxsize_dl: DOWNLINK_QUEUE_SIZE,
            ul_capacity_queue_device: UPLINK_QUEUE_SIZE,
            link_outputs,
            service_timer: Duration::ZERO,
            aux_ampdu_serviced: AmpduPacket::new(),
            link_is_transmitting: being_served,
            blocked_packet_counter: 0,
            arrived_packet_counter: 0,
            queue_length_counter: 0,
            service_rate: 0.0,
            // t0_time: Instant::now(),
            csv_metrics_network,
            parquet_metrics_network, 
            coords_queue: Coords::with_coords(AP_X, AP_Y, 0.0), // To test.
            p_tx: P_TX,
            STA_coords_grid: Vec::new(),
            STA_coords_map: HashMap::new(),
            cumulative_stats_queue: Arc::new(Mutex::new(QueueStats::new())),
            array_stas_stats: Arc::new(Mutex::new(stats_vec)),
            // array_dcf_values: Arc::new(Mutex::new(dcf_stats_vec)),
            dcf_values, 
            mac_key_index, 
            num_links, 

            sta_stats_cache: HashMap::new(),
            stats_tx: Some(stats_tx),
            stats_rx: Some(stats_rx),
            PL_probability: PL_prob,
            ampdu_id: 0,
            link_mediums,
            link_channel_widths,
            link_queue_depths,
            // mac_queue_depths,
            // mac_queue_depths: HashMap::new(), 
            sta_capabilities: HashMap::new(),
            mlo_linkselection_strat,
            packs_per_ampdu,

            window_metrics_mcs_util: Arc::new(Mutex::new(windows_vec)),
            output_metrics: Output::default(),
            viz_tx, 
            active_mac_key_counts: HashMap::new(), 
            slot_tick_pending: false,
        }
    }

    #[inline]
    pub fn get_queue_stats_handle(&self) -> Arc<Mutex<QueueStats>> {
        self.cumulative_stats_queue.clone()
    }
    #[inline]
    pub fn get_stas_stats_handle(&self) -> Arc<Mutex<HashMap<usize, perStaLockStats>>> {
        self.array_stas_stats.clone()
    }
    

    #[inline]
    fn dcf(&self, key: &MacKey) -> &DcfStats { // Helper: Obtains DCF MAC params for particular STA/EDCA_AC/LinkID
        &self.dcf_values[self.mac_key_index[key]]
    }

    #[inline]
    fn dcf_mut(&mut self, key: &MacKey) -> &mut DcfStats {  // Helper: Obtains DCF MAC params as mutable for particular STA/EDCA_AC/LinkID
        let idx = self.mac_key_index[key];
        &mut self.dcf_values[idx]
    }

    #[inline]
    fn emit_visualization_event(&self, ev: VizEvent) {
        if !VISUALIZER_QUEUES_ENABLED{return;}
        else{
            if let Some(tx) = &self.viz_tx {
            let _ = tx.try_send(ev);  // never block the sim
            }
        }
    }

    fn queue_depth(&self, key: &MacKey) -> usize {
        self.per_flow_queues[self.mac_key_index[key]].len()
    }

    #[inline]
    pub fn update_window_stats(
        &mut self,
        wind_key: WindowKey,
        mcs: Option<u8>,
        duration: f64,
        success: bool,
        now: f64,
    ) {
        let mut windows_map: std::sync::MutexGuard<'_, HashMap<(i32, u8), WindowMetrics>> = self
            .window_metrics_mcs_util
            .lock()
            .expect("Window stats mutex poisoned");

        let metrics = windows_map.entry(wind_key).or_insert_with(|| {
            // Fallback default if STA missing: 0.1s window
            WindowMetrics::new(MCS_REPORT_PERIOD_F32, wind_key)
        });

        // 3. CHECK & REPORT: Has the window duration passed?
        // We check this BEFORE adding the new packet to keep the window boundary clean.
        // println!("now: {}, start: {} -> Elapsed = {}", now, metrics.window_start, elapsed);
        let elapsed = now - metrics.window_start;
        if elapsed >= metrics.window_duration.as_secs_f64() {
            // A. Compute the averages for the finishing window

            let sta_util = (metrics.sta_tx_time + metrics.sta_collision_time) / elapsed; // Redundant, for testing only
            let total_util = metrics.total_busy_time / elapsed; // Redundant, for testing only

            // if metrics.packet_count > 0 {
            //      print_magenta!("{} ELAPSED = {:.4}, DURATION: {:.4}\nMETRICS_WINDOW STA {} LINK {}: MCS={:.2}, Util={:.6}, Relative_util_STA={:.6}",
            //          now,elapsed, metrics.window_duration.as_secs_f32(),  wind_key.0, wind_key.1, metrics.mcs_sum / metrics.packet_count as f64, total_util, sta_util
            //      );
            // } else {
            //      // Optional: Print zeros even if idle to keep charts continuous
            //      print_brown!("METRICS_WINDOW,{},{:?},0.0,0.0,0.0", now, wind_key);
            // }

            let (window_key, avg_mcs, total_util, sta_util) = metrics.compute_and_reset(now);

            let report = WindowMetricReport {
                timestamp: now,
                window_key: wind_key,
                avg_mcs,
                total_util,
                sta_util,
            };
            // println!("inside! metrics: {:?}\nreport:{:?}", metrics, report);
            self.output_metrics.send(report);

            // B. Output to CSV/Console
            // Format: [Time, STA_ID, Avg_MCS, Total_Chan_Util, STA_Relative_Util]
            // You can append this to your self.csv_metrics or just print it.
        }
        // 4. UPDATE: Add the current packet's stats to the (possibly new) window
        metrics.update_metrics_mcs_util(mcs, duration, success);
    }

   #[inline]
    pub fn tick_backoff(&mut self, now: TaiTime<0>) -> HashMap<u8, Vec<MacKey>> {
        // Clear idle links
        for medium in self.link_mediums.values_mut() {
            medium.clear_if_idle(now);
        }

        // Precompute which MacKeys have packets (per link)
        // let mut present_keys: HashSet<(i32, EdcaAc)> = HashSet::new();
        // for p in self.queue.iter() {
        //     let is_ul = p.sta_src_id > p.sta_dest_id;
        //     let key = if is_ul {
        //         (p.sta_src_id, p.edca_ac)
        //     } else {
        //         (-1, p.edca_ac)
        //     };
        //     present_keys.insert(key);
        // }

        let mut ready_per_link: HashMap<u8, Vec<MacKey>> = HashMap::new();

        // st is ALREADY a mutable reference (&mut DcfStats) thanks to this loop
        for st in &mut self.dcf_values {
            let key = st.mac_key;
            let (sta_id, ac, link_id) = key;

            // Skip if no packets for this STA/AC
            // if !present_keys.contains(&(sta_id, ac)) {
            //     continue;
            // }
            if self.active_mac_key_counts.get(&(sta_id, ac)).copied().unwrap_or(0) == 0 {
                continue;
            }
            let prev = (st.backoff_counter, st.backoff_frozen, st.cw); // Snapshot before mutation, to compare later

            // Get medium state for this link. 
            // Rust allows this because `link_mediums` is a different field than `dcf_values`.
            let medium = self.link_mediums.get(&link_id).unwrap();
            let idle_slot = medium.is_idle(now);
            
            if !idle_slot {
                debug_edca!(
                    "{} [LINK {}] BUSY until {}, owned by {:?} | Previous owner: {:?}",
                    format_elapsed!(now),
                    link_id,
                    format_elapsed!(medium.busy_until),
                    medium._current_owner(),
                    medium._last_owner(), 
                );
            }
            // AIFS gating
            let aifs_until = st.medium_free_since + aifs(st.param);
            let aifs_satisfied = idle_slot && aifs_until <= now;

            if st.backoff_frozen && aifs_satisfied {
                let aifs_duration_us = aifs(st.param).as_micros();
                debug_edca!(
                    "{} \t[EDCA] L-{} ({}, {:?}): AIFS satisfied ({}µs). UNFREEZING.",
                    format_elapsed!(now),
                    link_id,
                    sta_id,
                    ac,
                    aifs_duration_us
                );
            }

            // Update backoff state directly
            if aifs_satisfied {
                st.backoff_frozen = false;
            }

            // Backoff countdown
            if idle_slot && !st.backoff_frozen && st.backoff_counter > 0 {
                debug_edca!(
                    "{} [EDCA] L-{} ({}, {:?}): COUNTDOWN {} -> {}",
                    format_elapsed!(now),
                    link_id,
                    sta_id,
                    ac,
                    st.backoff_counter,
                    st.backoff_counter - 1
                );
                st.backoff_counter -= 1;
            }
            // Check if ready
            if st.backoff_counter == 0 && !st.backoff_frozen {
                ready_per_link
                    .entry(link_id)
                    .or_insert_with(Vec::new)
                    .push(key);
            }
            let curr = (st.backoff_counter, st.backoff_frozen, st.cw);
            if curr != prev {
                // Borrow-checker note: we can't call `&self` methods here because `st` holds &mut.
                // Push directly through the sender.
                    if let Some(tx) = &self.viz_tx {
                        if VISUALIZER_QUEUES_ENABLED{
                            let _ = tx.try_send(VizEvent::BackoffSnap {
                                t: t_secs(now),
                                mac_key: key,
                                counter: st.backoff_counter,
                                cw: st.cw,
                                frozen: st.backoff_frozen,
                                medium_free_since: t_secs(st.medium_free_since),
                        });

                        }
                    }
                }


        }

        // NOTE: `summary` is only ever read by `debug_edca!`, which compiles away when
        // DEBUG_EDCA is false. Building it unconditionally cost a String + one format!
        // per contender on every 9 µs slot tick, so it is gated on the same const.
        if DEBUG_EDCA && !ready_per_link.is_empty() {
            // Build a short summary string
            let mut summary = String::new();
            for (link_id, contenders) in &ready_per_link {
                summary.push_str(&format!("L-{}: {} | ", link_id, contenders.len()));
            }

            debug_edca!(
                "{} [TICK] Ready contenders: {}",
                format_elapsed!(now),
                summary
            );
        }
        ready_per_link
    }


    #[inline]
    fn txop_cap_secs(&self, key: &MacKey) -> f64 {
        // let p = self.array_dcf_values.lock().unwrap()[key].param;
        let p = self.dcf(key).param;          // no lock, no unwrap

        if p.txop_limit_us == 0 {
            f64::INFINITY
        } else {
            p.txop_limit_us as f64 * 1e-6
        }
    }
    
    #[inline]
    fn ac_idx(ac: EdcaAc) -> usize {
        match ac {
            EdcaAc::Voice      => 0,
            EdcaAc::Video      => 1,
            EdcaAc::BestEffort => 2,
            EdcaAc::Background => 3,
        }
    }
    #[inline]
    fn resolve_virtual_collision(&mut self, mut ready: Vec<MacKey>) -> Vec<MacKey> {
        // Collisions when same STA has several ACs winning backoff

        let mut winner = HashMap::<i32, MacKey>::new(); // sta_id → winning AC

        ready.sort_by_key(|k| ac_prio(k.1));
        for key in ready {
            // iterate lowest to highest value
            let sta = key.0; // -1 for AP
            if !winner.contains_key(&sta) {
                winner.insert(sta, key); // first (=highest-prio) wins
            } else {
                // “virtual collision” for this lower-prio AC
                // if let Some(st) = self.array_dcf_values.lock().unwrap().get_mut(&key) {
                        // st.on_failure();
                // }
                self.dcf_mut(&key).on_failure(); 
      
            }
        }
        winner.into_values().collect()
    }

    #[inline]
    fn select_link_for_packet(&mut self, pkt: &MpduPacket, now: TaiTime<0>) -> Option<u8> {
        /// Select the best link for a given packet based on strategy and STA capabilities
        let sta_id = if pkt.sta_src_id > pkt.sta_dest_id {
            pkt.sta_src_id // Uplink
        } else {
            pkt.sta_dest_id // Downlink
        };

        // Get STA capabilities
        let cap = match self.sta_capabilities.get(&sta_id) {
            Some(c) => c,
            None => {
                log_link_selection!(
                    now,
                    "⚠️  STA {} has no registered capabilities, using default (single link)",
                    sta_id
                );
                // Assume legacy single-link device
                return Some(0);
            }
        };

        if cap.links.is_empty() {
            log_link_selection!(now, "⚠️  STA {} has no available links!", sta_id);
            return None;
        }

        // Single-link device - trivial case
        if cap.links.len() == 1 {
            let link_id = cap.links[0];
            // log_link_selection!(
            //     now,
            //     "STA {} is single-link (LINK-{})",
            //     sta_id,
            //     link_id
            // );
            return Some(link_id);
        }

        // Multi-link device - apply selection strategy
        // log_link_selection!(
        //     now,
        //     "STA {} is MLO-capable: links={:?}, STR={}",
        //     sta_id,
        //     cap.links,
        //     cap.is_str_capable
        // );

        let selected = match self.mlo_linkselection_strat {
            LinkSelectionStrategy::PrimaryFirst => {
                self.select_mlo_channel_primary_first(&cap.links, now)
            }
            LinkSelectionStrategy::Opportunistic => {
                self.select_mlo_channel_opportunistic(&cap.links, now)
            }
            LinkSelectionStrategy::LyapunovBackpressure => {
                self.select_mlo_channel_lyapunov_backpressure(&cap.links, now)
            }
            _ => {
                println!(
                    "Mode {} UNIMPLEMENTED --> DEFAULT TO OPPORTUNISTIC",
                    self.mlo_linkselection_strat.to_string()
                );
                self.select_mlo_channel_opportunistic(&cap.links, now)
            }
        };

        if let Some(link_id) = selected {
            if STR_PLUS_MODE_MLO == false {
                log_link_selection!(
                now,
                "✓ Selected LINK-{} for STA {} (strat: {:?})",
                link_id,
                sta_id,
                self.mlo_linkselection_strat,
            );
        } 
            // pkt.print(DebugColor::Blue);
        }
        selected
    }

    fn get_link_queue_depth(&self, link_id: u8) -> usize {
        // Now an O(1) lookup
        self.link_queue_depths.get(&link_id).copied().unwrap_or(0)
    }

    #[inline]
    fn select_mlo_channel_lyapunov_backpressure(
        &self,
        available_links: &[u8],
        now: TaiTime<0>,
    ) -> Option<u8> {
        // 1. Handle edge cases
        if available_links.is_empty() {
            return None;
        }
        // If only one link, there's no choice to make
        if available_links.len() == 1 {
            return Some(available_links[0]);
        }

        // 2. Find the link with the minimum queue depth
        let mut min_depth = usize::MAX;
        let mut selected_link = available_links[0]; // Default to the first link

        for &link_id in available_links {
            // Use your existing O(1) lookup
            let depth = self.get_link_queue_depth(link_id);

            // Optional logging to see the decision process
            // log_link_selection!(
            //     now,
            //     "Lyapunov: Checking LINK-{} -> Q_depth = {}",
            //     link_id,
            //     depth
            // );

            if depth < min_depth {
                min_depth = depth;
                selected_link = link_id;
            }
        }

        // log_link_selection!(
        //     now,
        //     "Lyapunov: Selected LINK-{} (min_depth = {})",
        //     selected_link,
        //     min_depth
        // );

        // 3. Return the link with the shortest queue
        Some(selected_link)
    }

    #[inline]
    fn select_mlo_channel_opportunistic(
        &self,
        available_links: &[u8],
        now: TaiTime<0>,
    ) -> Option<u8> {
        // 1. Handle edge cases (no links or only one link)
        if available_links.is_empty() {
            return None;
        }
        if available_links.len() == 1 {
            return Some(available_links[0]);
        }

        // 2. We are comparing the first two available links
        let link_a_id = available_links[0];
        let link_b_id = available_links[1];

        // 3. Get the idle state for both, defaulting to `false` (busy)
        //    if the medium doesn't exist in the map.
        let a_is_idle = self
            .link_mediums
            .get(&link_a_id)
            .map_or(false, |m| m.is_idle(now));

        let b_is_idle = self
            .link_mediums
            .get(&link_b_id)
            .map_or(false, |m| m.is_idle(now));

        // 4. Implement the selection logic using a match statement
        match (a_is_idle, b_is_idle) {
            (true, true) => {
                // Both are idle: Choose one at random.
                let chosen_link = if rand::thread_rng().gen_bool(0.5) {
                    link_a_id
                } else {
                    link_b_id
                };

                // // This log might be too noisy, but matches the style
                // log_link_selection!(
                //     now,
                //     "Opportunistic: Both links idle, randomly chose LINK-{}",
                //     chosen_link
                // );
                Some(chosen_link)
            }
            (true, false) => {
                // Only A is idle: Choose A.
                // log_link_selection!(
                //     now,
                //     "Opportunistic: Link {} busy, selecting idle LINK-{}",
                //     link_b_id,
                //     link_a_id
                // );
                Some(link_a_id)
            }
            (false, true) => {
                // Only B is idle: Choose B.
                // log_link_selection!(
                //     now,
                //     "Opportunistic: Link {} busy, selecting idle LINK-{}",
                //     link_a_id,
                //     link_b_id
                // );
                Some(link_b_id)
            }
            (false, false) => {
                // Both are busy: Choice doesn't matter. Fall back to A.
                // log_link_selection!(
                //     now,
                //     "Opportunistic: Both links busy, defaulting to LINK-{}",
                //     link_a_id
                // );
                // Some(link_a_id) // WHOOPSIE! Should be RANDOM.

                let random_choice = if rand::thread_rng().gen_bool(0.5) {
                    link_a_id
                } else {
                    link_b_id
                };
                Some(random_choice)
            }
        }
    }
    fn select_mlo_channel_primary_first(
        &self,
        available_links: &[u8],
        now: TaiTime<0>,
    ) -> Option<u8> {
        let primary_link = available_links[0];

        // Check if primary is idle
        if let Some(medium) = self.link_mediums.get(&primary_link) {
            if medium.is_idle(now) {
                return Some(primary_link);
            }
        }

        // Primary busy - check queue depth
        let primary_depth = self.get_link_queue_depth(primary_link);

        if primary_depth < 10 {
            // Threshold for "acceptable" load
            return Some(primary_link);
        }

        // Primary overloaded - try secondary links
        for &link_id in &available_links[1..] {
            if let Some(medium) = self.link_mediums.get(&link_id) {
                if medium.is_idle(now) {
                    log_link_selection!(
                        now,
                        "⚠️  Primary link {} overloaded ({} pkts), using backup LINK-{}",
                        primary_link,
                        primary_depth,
                        link_id
                    );
                    return Some(link_id);
                }
            }
        }

        Some(primary_link) // Fall back to primary
    }

    #[inline]
    pub async fn cache_input_packet(&mut self, length_packet_bits: usize,
        sta_src_id: i32,
        sta_dest_id: i32,
        sta_src_coords: Coords,   // Copy
        edca_ac: EdcaAc,      // Copy
        link_id: u8
        ) {
            let key = (sta_src_id, sta_dest_id);

            // Do all immutable reading from `self` *before* the mutable borrow.
            let is_ul = sta_src_id > sta_dest_id;
            let mac_key_edca = if is_ul {
                (sta_src_id, edca_ac, link_id)
            } else {
                (-1, edca_ac, link_id)
            };

            let channel_width = self.link_channel_widths.get(&link_id).copied().unwrap();
            // All immutable borrows happen here and end immediately
            let cap_s_edca = self.txop_cap_secs(&mac_key_edca);
            let coords_queue = self.coords_queue; // Assuming Coords is Copy
            let p_tx = self.p_tx; // f64 is Copy

            let entry = self.sta_stats_cache.entry(key).or_insert_with(|| {
                // Calculate transmission delay for a single packet
                let resultz = airtime_ampdu(
                    length_packet_bits as f64,
                    1,
                    coords_queue,
                    sta_src_coords,
                    p_tx,
                    channel_width,
                );

                // Binary search
                let mut low = 1;
                let mut high = self.packs_per_ampdu as i32;
                let mut optimal_n_packets = 0;
                let mut resultz_full_ampdu = airtime_ampdu(
                    length_packet_bits as f64 * high as f64,
                    high,
                    coords_queue,
                    sta_src_coords,
                    p_tx,
                    channel_width,
                );

                while low <= high {
                    let mid = (low + high) / 2;
                    let test_resultz = airtime_ampdu(
                        length_packet_bits as f64 * mid as f64,
                        mid,
                        coords_queue,
                        sta_src_coords,
                        p_tx,
                        channel_width,
                    );

                    // Use the pre-calculated cap_s_edca variable
                    if test_resultz.0 <= DEFAULT_TMAX_AGG || test_resultz.0 <= cap_s_edca {
                        optimal_n_packets = mid;
                        resultz_full_ampdu = test_resultz;
                        low = mid + 1;
                    } else {
                        high = mid - 1;
                    }
                }
                // Note: packet_count starts at 0, will be incremented below
                StaRateInfo {
                    total_transmission_delay_single: resultz.0,
                    total_transmission_delay_fullampdu: resultz_full_ampdu.0,
                    fullampdu_max_size: optimal_n_packets as usize,
                    packet_count: 0,
                    weighted_rate_single: resultz.0, // First value for EWMA
                    weighted_rate_fullampdu: resultz_full_ampdu.0, // First value for EWMA
                    // Avoid division by zero if optimal_n_packets is 0
                    per_packet_channel_access_efficiency: resultz_full_ampdu.0
                        / (optimal_n_packets.max(1) as f64),
                    expected_queue_delivery_ms: 0.0,
                }
            }); // <-- Mutable borrow of self.sta_stats_cache ends here

            entry.packet_count += 1;

            // Re-calculate the expected delivery time based on the new count
            entry.expected_queue_delivery_ms =
                entry.per_packet_channel_access_efficiency * entry.packet_count as f64 * 1000.0;
        }

    #[inline]
    pub async fn input(&mut self, mut pkt: MpduPacket, ctx: &Context<Self>) {
        let now = ctx.scheduler.time();
        pkt.queue_in_instant = now;
        self.arrived_packet_counter += 1;

        let is_ul = pkt.sta_src_id > pkt.sta_dest_id;

        // Select best link for this packet
        let selected_link: Option<u8> = self.select_link_for_packet(&pkt, now);

        if !is_ul {
            // Extra guard to prevent wrong routing.
            match selected_link {
                Some(link_id) => {

                    // Cache the selected link in packet metadata
                    pkt.assign_link(link_id);

                    let mac_key_dl: MacKey = (-1, pkt.edca_ac, link_id);
                    pkt.mac_key_cached = Some(mac_key_dl);

                    let cur_depth = self.queue_depth(&mac_key_dl);
                    if cur_depth < self.queue_maxsize_dl {
                 

                        self.cache_input_packet( pkt.length_packet_bits ,pkt.sta_src_id, pkt.sta_dest_id, pkt.sta_src_coords, pkt.edca_ac, link_id).await;

                        // *self.mac_queue_depths.entry(mac_key_dl).or_insert(0) += 1;
                        // *self.queue_depth_mut(&mac_key_dl) += 1;
                        *self.link_queue_depths
                            .entry(link_id)
                            .or_insert(0) += 1;                        
                        // Trigger scheduling if medium is idle
                        // [STR+] Trigger scheduling if ANY link is idle (start race on all links), else just check the link_id of the current packet.
                        // Instead of checking only the assigned 'link_id', we check if any link is free to start the backoff process.
                        let any_link_idle = if STR_PLUS_MODE_MLO {
                            self.link_is_transmitting.values().any(|&tx| !tx)
                        } else {
                            *self.link_is_transmitting.get(&link_id).unwrap() == false
                        };
                        let new_depth = cur_depth + 1;
                        log_mlo!(
                                now,
                                "📥 DL Packet {} (STA {} → {}) assigned to LINK-{}: Q_size={}",
                                pkt.packet_id,
                                pkt.sta_src_id,
                                pkt.sta_dest_id,
                                link_id,
                                new_depth
                            );
                        // *self.active_mac_key_counts
                        //     .entry((pkt.sta_src_id, pkt.edca_ac))
                        //     .or_insert(0) += 1;
                        *self.active_mac_key_counts
                            .entry((-1i32, pkt.edca_ac))            // ← matches tick_backoff logic
                            .or_insert(0) += 1;

                        if VISUALIZER_QUEUES_ENABLED {
                            self.emit_visualization_event(VizEvent::QueueDepth {
                                t: t_secs(now),
                                mac_key: mac_key_dl,
                                depth: new_depth,
                                sta_src: pkt.sta_src_id,
                                sta_dest: pkt.sta_dest_id,
                            });
                        }

                        let idx = self.mac_key_index[&mac_key_dl];
                        self.per_flow_queues[idx].push_back(pkt);

                        if new_depth == 1 && any_link_idle {
                            self.schedule_tick(ctx);
                        }
  
                    } else {
                        self.blocked_packet_counter += 1;
                        log_mlo!(now, "❌ QUEUE FULL - dropped packet {}", pkt.packet_id);
                    }
                }
                None => {
                    self.blocked_packet_counter += 1;
                    log_mlo!(
                        now,
                        "❌ No available link for STA {} (not MLO-capable or all links busy)",
                        pkt.sta_src_id
                    );
                }
            }
        } else { // guard against BG DL+UL traffic being sent to both inputs.

            // println!("[Input DL Discard] NOT DL (SRC: {} > DST: {} ) ", pkt.sta_src_id , pkt.sta_dest_id, );
        }
    }

    /// Uplink input function (from STAs)
    #[inline]
    pub async fn input_UL(&mut self, mut packet: MpduPacket, context: &Context<Self>) {
        self.arrived_packet_counter += 1;

        self.queue_length_counter += self.per_flow_queues.iter().map(|dq| dq.len()).sum::<usize>();

        let now = context.scheduler.time();
        let is_ul = packet.sta_src_id > packet.sta_dest_id;

        // Select link for uplink packet
        let selected_link = self.select_link_for_packet(&packet, now);
        
        if is_ul {
            match selected_link {
                Some(link_id) => {

                    packet.assign_link(link_id);
                    packet.mac_key_cached = Some(if is_ul {
                        (packet.sta_src_id, packet.edca_ac, link_id)
                    } else {
                        (-1, packet.edca_ac, link_id)
                    });
                    let mac_key_ul = packet.mac_key_cached.unwrap();

                    // let current_depth = self.mac_queue_depths.get(&mac_key_ul).copied().unwrap_or(0);
                    let current_depth = self.queue_depth(&mac_key_ul);
                    if current_depth <self.ul_capacity_queue_device {

                        packet.queue_in_instant = now; // UL packets always go in queue, later they're dropped if they exceed max of STA/EDCA_AC virtual queue.
                        self.cache_input_packet( packet.length_packet_bits ,packet.sta_src_id, packet.sta_dest_id, packet.sta_src_coords, packet.edca_ac, link_id).await;
                        
                        let idx = self.mac_key_index[&mac_key_ul];
                        let new_depth: usize = current_depth + 1;

                        log_mlo!(
                            now,
                            "📥 UL Packet {} from STA{} → STA{} on LINK-{}, Q_size = {}",
                            packet.packet_id,
                            packet.sta_src_id,
                            packet.sta_dest_id,
                            link_id,
                            new_depth
                        );
                        if VISUALIZER_QUEUES_ENABLED {
                            self.emit_visualization_event(VizEvent::QueueDepth {
                                t: t_secs(now),
                                mac_key: mac_key_ul,
                                depth: new_depth,
                                sta_src: packet.sta_src_id,
                                sta_dest: packet.sta_dest_id,
                            });
                        }
                  
                        *self.active_mac_key_counts
                            .entry((packet.sta_src_id, packet.edca_ac))
                            .or_insert(0) += 1;
                        
                        self.per_flow_queues[idx].push_back(packet);
                        *self.link_queue_depths
                            .entry(link_id)
                            .or_insert(0) += 1;
            
                        let any_link_idle = if STR_PLUS_MODE_MLO {
                            self.link_is_transmitting.values().any(|&tx| !tx) // get any link that is not busy
                        } else {
                            *self.link_is_transmitting.get(&link_id).unwrap() == false
                        };

                        if new_depth == 1 && any_link_idle {
                            self.schedule_tick(context);
                        }
                    }
                        else {
                            log_mlo!(
                                now,
                                "❌ UL Queue for STA{} (MAC_KEY={:?}) exceeded capacity ({} pkts), dropping packet {}",
                                packet.sta_src_id,
                                mac_key_ul,
                                current_depth,
                                packet.packet_id
                            );
                        }
                    }
                None => {
                    self.blocked_packet_counter += 1;
                    log_mlo!(
                        now,
                        "❌ No available link for UL packet from STA{}",
                        packet.sta_src_id
                    );
                }
            }
        } else { // guard against BG DL+UL traffic being sent to both inputs.
             // println!("[Input UL Discard] NOT UL (SRC: {} !> DST: {} ) ", packet.sta_src_id , packet.sta_dest_id, );
        }
    }

    /// Keeps `STA_coords_map` in sync with a STA's live position (e.g. random-walk or scripted
    /// distance test movement), since DL airtime/rate lookups read from this map rather than
    /// from per-packet coordinates.
    #[inline]
    pub async fn input_coords_update(&mut self, update: (i32, Coords), _context: &Context<Self>) {
        self.STA_coords_map.insert(update.0 as usize, update.1);
    }

    fn get_ampdu_utility(
        &self,
        n_mpdus: usize,
        first_packet: &MpduPacket, // To get coordinates, length, etc.
        mac_key: &MacKey,
    ) -> (f64, f64) {
        if n_mpdus == 0 {
            return (0.0, 0.0);
        }

        let (sta_id, ac, link_id) = *mac_key;
        let channel_width = self.link_channel_widths.get(&link_id).copied().unwrap();
        let packet_bits = first_packet.length_packet_bits as f64;
        let total_bits = packet_bits * n_mpdus as f64;
        let is_ul = first_packet.sta_src_id > first_packet.sta_dest_id;

        // Use the same logic as build_new_ampdu to get coords
        let (src_coords, dest_coords) = if is_ul {
            (first_packet.sta_src_coords, self.coords_queue)
        } else {
            let dest_coords = self
                .STA_coords_map
                .get(&(first_packet.sta_dest_id as usize))
                .unwrap()
                .clone();
            (self.coords_queue, dest_coords)
        };

        let airtime_secs = airtime_ampdu(
            total_bits,
            n_mpdus as i32,
            src_coords,
            dest_coords,
            P_TX, // This is a global, ok
            channel_width,
        )
        .0;

        if airtime_secs == 0.0 {
            return (0.0, 0.0); // Avoid division by zero
        }

        let utility = n_mpdus as f64 / airtime_secs;
        (utility, airtime_secs)
    }

    #[inline]
    fn select_next_sta(&self) -> &HashMap<(i32, i32), StaRateInfo> {
        // The entire slow loop is gone, O(1) lookup.

        &self.sta_stats_cache
    }

    #[inline]
    pub async fn send_ampdu(&mut self, AMPDU_sent: AmpduPacket, context: &Context<Self>) {
        let now = context.scheduler.time();
        let elapsed = context.scheduler.time();
        // self.shared_medium.release_txop(elapsed);
        let link_id = AMPDU_sent.link_id;

        if let Some(medium) = self.link_mediums.get_mut(&link_id) {
            medium.release_txop(elapsed);
        }
        // crate::print_brown!("{} | [TXOP END] last_owner={:?}", format_elapsed!(elapsed), self.shared_medium.last_owner());
        debug_debug!(
            DebugColor::Red,
            "{} [DBG TXOP]    --AMPDU sent to STA {} with {} packets inside, L = {}, AMPDU_size: {}",
            format_elapsed!(elapsed),
            AMPDU_sent.sta_dest_id,
            AMPDU_sent.mpdu_packets.len(),
            AMPDU_sent.total_length,
            AMPDU_sent.size
        );
        // AMPDU_sent.print();
        self.ampdu_id += 1; // increment the AMPDU counter for logging.

        self.link_is_transmitting.insert(link_id, false);

        if VISUALIZER_QUEUES_ENABLED {
            let mac_key = AMPDU_sent.mac_key;
            
            // Reconstruct the per-deque depths at actual TXOP completion
            let mut depth_by_key: HashMap<MacKey, usize> = HashMap::new();
            for packet in &AMPDU_sent.mpdu_packets {
                let lid = packet.assigned_link_id.unwrap_or(mac_key.2);
                let is_ul = packet.sta_src_id > packet.sta_dest_id;
                let correct_key: MacKey = (
                    if is_ul { packet.sta_src_id } else { -1 },
                    packet.edca_ac,
                    lid,
                );
                // Record so we emit one final depth snapshot per deque
                depth_by_key.entry(correct_key).or_insert_with(|| {
                    self.mac_key_index.get(&correct_key)
                        .map(|&idx| self.per_flow_queues[idx].len())
                        .unwrap_or(0)
                });
            }

            for (correct_key, depth) in depth_by_key {
                self.emit_visualization_event(VizEvent::QueueDepth {
                    t: t_secs(now),  // ← TXOP end, not build time
                    mac_key: correct_key,
                    depth,
                    sta_src: if correct_key.0 == -1 { AMPDU_sent.sta_src_id } 
                            else { correct_key.0 },
                    sta_dest: AMPDU_sent.sta_dest_id,
                });
            }
        }



        let mac_key = AMPDU_sent.mac_key;
        let wind_key: WindowKey = (mac_key.0, mac_key.2);
        // crate::print_dblue!("[DBG SEND AMPDU] KEY: {:?}", wind_key );

        let now_elapsed = elapsed.duration_since(TaiTime::EPOCH).as_secs_f64();
        let mcs_assigned = AMPDU_sent.mcs_assigned;
        



        // Send to appropriate output port
        if let Some(output) = self.link_outputs.get_mut(&link_id) {
            output.send(AMPDU_sent).await;
        }

        // if self.per_flow_queues.iter().any(|dq| !dq.is_empty()) {
        //     self.deque_schedule_service((), context).await;
        // }
        if self.per_flow_queues.iter().any(|dq| !dq.is_empty()) {
            self.schedule_tick(context);
        }

        

        let dcf_data = {
            // Lock happens here
            // let mut guard = self.array_dcf_values.lock().unwrap();
            let st= self.dcf_mut(&mac_key); 
            
            // Extract the values needed for CSV/Logging later
            let prev_retries = st.retry_count;
            let prev_cw_val = st.cw;
            let prev_drawn_bo = st.last_backoff_drawn_logs;
            // Clone the string now so we don't need 'st' later
            let ac_str = st.edca_ac_str.clone();

            // Update 'st' immediately while we have the lock
            st.on_success(st.param.cw_min);

            // Return the extracted data
            Some((prev_retries, prev_cw_val, prev_drawn_bo, ac_str))
        }; 

        
        if let Some((prev_retries, prev_cw_val, prev_drawn_bo, ac_str)) = dcf_data {
            let mut drained = smallvec::SmallVec::<[StatsUpdate; 64]>::new();
            if let Some(rx) = self.stats_rx.as_mut() {
                while let Ok(up) = rx.try_recv() {
                    drained.push(up);
                }
            }
            if !drained.is_empty() {
                if let Ok(mut qstats) = self.cumulative_stats_queue.lock() {
                    for u in &drained {
                        qstats.update_cumstats(
                            u.T_s,
                            u.T_q,
                            u.blocked_packet_counter,
                            u.arrived_packet_counter,
                            u.queue_length_when_out,
                        );
                    }
                }

                // println!("{:.6} {:?} Updating with mcs: {}, dur:{}, transmit!",
                //     format_elapsed!(context.scheduler.time()), wind_key,  mcs_assigned, drained[0].T_s );

                self.update_window_stats(
                    wind_key,
                    Some(mcs_assigned),
                    drained[0].T_s,
                    true,
                    now_elapsed
                );

                if CSV_PER_PACKET {
                    for u in drained {
                        let alvr_header = u.alvr_header; 
                        // CSV write outside the lock
                        let row_ac = u.edca_ac.to_string();   // per-row, not per-AMPDU
                        if let Some(metrics) = &self.csv_metrics_network{
                            metrics.update_stats(
                            u.now,
                            u.packet_id as usize,
                            u.queue_length_when_out,
                            u.T_s,
                            u.T_q,
                            u.length_packet,
                            u.sta_src_id,
                            u.sta_dest_id,
                            u.ampdu_id,
                            u.is_collision,
                            u.collision_backoff,
                            u.link_id as usize,
                            prev_cw_val as usize,
                            prev_retries,
                            prev_drawn_bo,
                            row_ac.clone(),
                            alvr_header.clone(), 
                        );
                        }
                        if let Some(parquet_metrics) = &self.parquet_metrics_network {
                            parquet_metrics.update_stats(
                                u.now,
                                u.packet_id as usize,
                                u.queue_length_when_out,
                                u.T_s,
                                u.T_q,
                                u.length_packet,
                                u.sta_src_id,
                                u.sta_dest_id,
                                u.ampdu_id,
                                u.is_collision,
                                u.collision_backoff,
                                u.link_id as usize,
                                prev_cw_val as usize,
                                prev_retries,
                                prev_drawn_bo,
                                row_ac,
                                alvr_header, 
                            );
                        }
                    }
                }
            }
        }
    }

    #[inline]
    fn build_new_ampdu<'a>(
        &mut self,
        first_packet: &MpduPacket,
        link_id: u8,
        now: TaiTime<0>,
    ) -> (AmpduPacket, Duration) {

        let (sta_src_id, sta_dest_id) = (first_packet.sta_src_id, first_packet.sta_dest_id);

        let channel_width = self.link_channel_widths.get(&link_id).copied().unwrap();

        self.aux_ampdu_serviced.reset();
        self.aux_ampdu_serviced.sta_dest_id = first_packet.sta_dest_id;
        self.aux_ampdu_serviced.sta_src_id = first_packet.sta_src_id;
        self.aux_ampdu_serviced.coordinates = first_packet.sta_src_coords.clone();
        self.aux_ampdu_serviced.link_id = link_id; // Tag AMPDU with link

        if first_packet.sta_src_id <= 0 || first_packet.sta_dest_id <= 0 {
            panic!(
                "Invalid STA IDs in first packet: src_id={}, dest_id={}",
                first_packet.sta_src_id, first_packet.sta_dest_id
            );
        }
        let is_ul = first_packet.sta_src_id > first_packet.sta_dest_id;
        let target_dest_id = first_packet.sta_dest_id;
        // println!("IS_UL = {} ( {} > {})", is_ul, first_packet.sta_src_id, first_packet.sta_dest_id);

        let mac_key: MacKey = if is_ul {
            (first_packet.sta_src_id, first_packet.edca_ac, link_id)
        } else {
            (-1, first_packet.edca_ac, link_id)
        };

        self.aux_ampdu_serviced.mac_key = mac_key;
        let txop_us: f64 = self.dcf(&mac_key).param.txop_limit_us as f64;

        let mut last_service_duration = Duration::default();
        // let mut packet_index = 0;
        let mut resultz = 0.0;

        log_mlo!(
            now,
            "🔨 Building AMPDU on LINK-{} ({} Mhz) for STA {} → {} (AC: {:?})",
            link_id,
            channel_width,
            sta_src_id,
            sta_dest_id,
            first_packet.edca_ac
        );
        
        let mut flow_backlog = self.queue_depth(&mac_key);

        let mut deque_sources: Vec<(usize, usize)> = Vec::new();

        let mut taken_snapshot: Vec<(usize, MpduPacket)> = Vec::new();

        let primary_idx = self.mac_key_index[&mac_key];
        let sta_id_for_deque: i32 = if is_ul { sta_src_id } else { -1 };
        let mut source_deque_idxs: Vec<usize> = vec![primary_idx];

        if STR_PLUS_MODE_MLO {
            let mut other_links: Vec<u8> = self.link_mediums.keys().copied()
                .filter(|&lid| lid != link_id)
                .collect();
            other_links.sort_unstable();
            for alt_link_id in other_links {
                let alt_mac_key: MacKey = (sta_id_for_deque, first_packet.edca_ac, alt_link_id);
                if let Some(&alt_idx) = self.mac_key_index.get(&alt_mac_key) {
                    if !self.per_flow_queues[alt_idx].is_empty() {
                        source_deque_idxs.push(alt_idx);
                    }
                }
            }
        }
        
        // ========== Aggregate packets for this flow on this link ==========
        'agg: for &deq_idx in &source_deque_idxs {
            let mut taken = 0usize;

            for pos in 0..self.per_flow_queues[deq_idx].len() {
                let current_packet = match self.per_flow_queues[deq_idx].get(pos) {
                    Some(p) => p,
                    None => break,
                };
                if !is_ul && current_packet.sta_dest_id != target_dest_id {
                    continue;
                }

                let new_total_length =
                    self.aux_ampdu_serviced.total_length + current_packet.length_packet_bits;
                let new_size = self.aux_ampdu_serviced.size + 1;

                if is_ul {
                    let ampdu_airtime_mcs = airtime_ampdu(
                        new_total_length as f64,
                        new_size,
                        current_packet.sta_src_coords.clone(),
                        self.coords_queue,
                        P_TX,
                        channel_width,
                    );
                    resultz = ampdu_airtime_mcs.0;
                    self.aux_ampdu_serviced.mcs_assigned = ampdu_airtime_mcs.1;
                } else {
                    let dest_coords = self
                        .STA_coords_map
                        .get(&(current_packet.sta_dest_id as usize))
                        .unwrap_or_else(|| panic!("no coordinates for STA {}", current_packet.sta_dest_id))
                        .clone();
                    let ampdu_airtime_mcs = airtime_ampdu(
                        new_total_length as f64,
                        new_size,
                        self.coords_queue,
                        dest_coords,
                        P_TX,
                        channel_width,
                    );
                    resultz = ampdu_airtime_mcs.0;
                    self.aux_ampdu_serviced.mcs_assigned = ampdu_airtime_mcs.1;
                }

                let cap_s_edca = self.txop_cap_secs(&mac_key);
                if resultz >= DEFAULT_TMAX_AGG
                    || new_size > self.packs_per_ampdu as i32
                    || resultz >= cap_s_edca
                {
                    log_mlo!(
                        now,
                        "  AMPDU limit reached: size={}/{}, airtime={:.3}ms/{:.3}ms",
                        new_size,
                        self.packs_per_ampdu,
                        resultz * 1000.0,
                        f64::min(DEFAULT_TMAX_AGG * 1000.0, cap_s_edca * 1000.0)
                    );
                     if taken > 0 {
                        deque_sources.push((deq_idx, taken)); // ← record partial deque FIRST
                    }
                    break 'agg;
                }

                let mut cloned_packet = current_packet.clone();
                cloned_packet.queue_length_when_out = flow_backlog;
                if flow_backlog > 0 { flow_backlog -= 1; }
                cloned_packet.queue_out_instant = now;
                cloned_packet.T_q = now.duration_since(cloned_packet.queue_in_instant);
                cloned_packet.mac_key_cached = Some(mac_key);

                // Record in snapshot BEFORE pushing into mpdu_packets
                taken_snapshot.push((deq_idx, cloned_packet.clone()));

                self.aux_ampdu_serviced.mpdu_packets.push(cloned_packet);
                self.aux_ampdu_serviced.total_length = new_total_length;
                self.aux_ampdu_serviced.size = new_size;
                last_service_duration = Duration::from_secs_f64(resultz);
                taken += 1;
            }

            if taken > 0 {
                deque_sources.push((deq_idx, taken));
            }
        }
      
        log_mlo!(
            now,
            "  Aggregated {} packets, total_length={}, MCS={},  airtime={:.3}ms",
            self.aux_ampdu_serviced.mpdu_packets.len(),
            self.aux_ampdu_serviced.total_length,
            self.aux_ampdu_serviced.mcs_assigned,
            last_service_duration.as_secs_f64() * 1000.0
        );

        // ========== Simulate transmission errors (MAC-layer packet loss) ==========
        let mut new_ampdu_packets: Vec<MpduPacket> = Vec::new();
        let mut rng = rand::thread_rng();
        let mut failed_count = 0;

        for mut packet in self.aux_ampdu_serviced.mpdu_packets.drain(..) {
            packet.T_s = Duration::from_secs_f64(resultz);
            let random_value: f64 = rng.gen();

            if random_value <= self.PL_probability {
                // Packet loss - leave original in queue for retransmission
                failed_count += 1;
                debug_bgprint!(
                    DebugColor::DarkRed,
                    "❌ Packet {} failed (PL={:.3}), will be retransmitted",
                    packet.header_alvr,
                    self.PL_probability
                );
                self.blocked_packet_counter += 1;
            } else {
                // Successful transmission, update stats. 
                if let Some(stats_tx) = &self.stats_tx {
                    let stats_update = StatsUpdate {
                        T_s: resultz,
                        T_q: packet.T_q.as_secs_f64(), 
                        blocked_packet_counter: self.blocked_packet_counter,
                        arrived_packet_counter: self.arrived_packet_counter,
                        queue_length_when_out: packet.queue_length_when_out,
                        sta_src_id: packet.sta_src_id,
                        sta_dest_id: packet.sta_dest_id,
                        packet_id: packet.packet_id as i32,
                        now,
                        length_packet: packet.length_packet_bits,
                        ampdu_id: self.ampdu_id,
                        is_collision: false,
                        collision_backoff: 0.0,
                        link_id: link_id,
                        alvr_header: packet.header_alvr.clone(), 
                        edca_ac: packet.edca_ac,
                    };

                    stats_tx
                        .send(stats_update)
                        .expect("Failed to send stats update");
                }
                new_ampdu_packets.push(packet);
            }
        }

        if failed_count > 0 {
            log_mlo!(
                now,
                "  ⚠️  {} packets failed transmission on LINK-{}",
                failed_count,
                link_id
            );
        }

        self.aux_ampdu_serviced.mpdu_packets = new_ampdu_packets;

        // ========== Remove successfully transmitted packets from queue ==========
        // ── Pop aggregated packets from their deques ─────────────────────────────────
        if is_ul {
            // UL queues are per-flow: every packet in the deque belongs to the same
            // (src, dest) pair, so the taken packets are always contiguous at the front.
            for (deq_idx, count) in &deque_sources {
                    for _ in 0..*count {
                        self.per_flow_queues[*deq_idx].pop_front();
                    }
                }
            } else {
                // DL queue (-1, ac, link_id) is shared across all destination STAs.
                // Packets for other destinations may be interleaved between the ones we
                // just aggregated, so we remove exactly the packets that entered taken_snapshot.
                let taken_ids: HashSet<usize> = taken_snapshot
                    .iter()
                    .map(|(_, p)| p.packet_id)
                    .collect();
                let involved_deques: HashSet<usize> = taken_snapshot
                    .iter()
                    .map(|(idx, _)| *idx)
                    .collect();
                for deq_idx in involved_deques {
                    self.per_flow_queues[deq_idx]
                        .retain(|p| !taken_ids.contains(&p.packet_id));
                }
            }

        if VISUALIZER_QUEUES_ENABLED {
            for (deq_idx, _count) in &deque_sources {
                // Reverse-lookup: find the MacKey for this deq_idx
                if let Some((&correct_key, _)) = self.mac_key_index.iter()
                    .find(|(_, &idx)| idx == *deq_idx)
                {
                    let remaining_depth = self.per_flow_queues[*deq_idx].len();
                    let is_ul = correct_key.0 != -1;
                    self.emit_visualization_event(VizEvent::QueueDepth {
                        t: t_secs(now),          // ← build time, not TXOP end
                        mac_key: correct_key,
                        depth: remaining_depth,  // ← 0 for fully drained deques
                        sta_src: if is_ul { correct_key.0 } else { first_packet.sta_src_id },
                        sta_dest: first_packet.sta_dest_id,
                    });
                }
            }
        }

        // ── Push failed (PL) packets back to the front for retransmission ────────────
        // Build set of packet_ids that survived PL
        let successful_ids: std::collections::HashSet<usize> = self
            .aux_ampdu_serviced.mpdu_packets
            .iter()
            .map(|p| p.packet_id)
            .collect();

        // Group failed packets by deque, preserving FIFO order
        let mut failed_by_deque: HashMap<usize, Vec<MpduPacket>> = HashMap::new();
        for (deq_idx, pkt) in taken_snapshot {
            if !successful_ids.contains(&pkt.packet_id) {
                failed_by_deque.entry(deq_idx).or_default().push(pkt);
            }
        }
        // Reverse before push_front so the original head ends up at front again
        for (deq_idx, mut failed_pkts) in failed_by_deque {
            failed_pkts.reverse();
            for pkt in failed_pkts {
                self.per_flow_queues[deq_idx].push_front(pkt);
            }
        }

        // ── Update link_queue_depths and active_mac_key_counts for successful packets ─
        let mut packets_removed_by_key: HashMap<(i32, i32), usize> = HashMap::new();
        for packet in &self.aux_ampdu_serviced.mpdu_packets {
            *packets_removed_by_key
                .entry((packet.sta_src_id, packet.sta_dest_id))
                .or_insert(0) += 1;

            if let Some(assigned_lid) = packet.assigned_link_id {
                if let Some(count) = self.link_queue_depths.get_mut(&assigned_lid) {
                    *count = count.saturating_sub(1);
                }
            }

            let is_ul_pkt = packet.sta_src_id > packet.sta_dest_id;
            let ac_key = if is_ul_pkt { (packet.sta_src_id, packet.edca_ac) }
                        else         { (-1i32, packet.edca_ac) };
            if let Some(c) = self.active_mac_key_counts.get_mut(&ac_key) {
                *c = c.saturating_sub(1);
            }

        }

        // ── Update sta_stats_cache ────────────────────────────────────────────────────
        for (key, count_removed) in packets_removed_by_key {
            if let Some(entry) = self.sta_stats_cache.get_mut(&key) {
                entry.packet_count = entry.packet_count.saturating_sub(count_removed);
                entry.expected_queue_delivery_ms =
                    entry.per_packet_channel_access_efficiency * entry.packet_count as f64 * 1000.0;
                if count_removed != 0 {
                    log_mlo!(
                        now,
                        "  Updated cache for flow ({}, {}): {} packets remaining",
                        key.0, key.1, entry.packet_count
                    );
                }
            }
        }
        

        if DEBUG_PRINT_ENABLED {
            print_yellow!(
                "{} [DBG AMPDU] LINK-{} --Dequeueing AMPDU, serviced at {}",
                format_elapsed!(now),
                link_id,
                format_elapsed!(now + last_service_duration)
            );
            self.aux_ampdu_serviced.print();
        }

        self.link_is_transmitting.insert(link_id, true);
        let ampdu_to_send = std::mem::replace(&mut self.aux_ampdu_serviced, AmpduPacket::new());

        log_mlo!(
            now,
            "✅ AMPDU built on LINK-{}: {} packets, {:.3}ms airtime | Txend = {:.8}",
            link_id,
            ampdu_to_send.mpdu_packets.len(),
            last_service_duration.as_secs_f64() * 1000.0,
            taitime_to_f64!(now + last_service_duration),
        );

        (ampdu_to_send, last_service_duration)
    }
    
    #[inline]
    fn schedule_tick(&mut self, context: &Context<Self>) {
        if !self.slot_tick_pending {
            self.slot_tick_pending = true;
            context.scheduler
                .schedule_event(
                    Duration::from_micros(SLOT_TIME_US),
                    Self::deque_schedule_service,
                    (),
                )
                .unwrap();
        }
    }

    #[inline]
    fn deque_schedule_service<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            self.slot_tick_pending = false;
            let now = context.scheduler.time();

            // UL capacity check (same as before)
            // let overflowing_flows: HashMap<(i32,i32), usize> = self.sta_stats_cache
            //     .iter()
            //     .filter_map(|((src, dest), info)| {
            //         let is_ul = src > dest;
            //         let limit = if is_ul { self.ul_capacity_queue_device } else { self.queue_maxsize_dl };
            //         if info.packet_count > limit {
            //             Some(((*src, *dest), info.packet_count - limit))
            //         } else {
            //             None
            //         }
            //     })
            //     .collect();

            // Optimized: // Only pay for the collection when there's actually something overflowing
            let has_overflow = self.sta_stats_cache.iter().any(|((src, dest), info)| {
                let limit = if src > dest {
                    self.ul_capacity_queue_device
                } else {
                    self.queue_maxsize_dl
                };
                info.packet_count > limit
            });

            if has_overflow {
                // now do the collect — same logic, just not always
                let overflowing_flows: HashMap<(i32,i32), usize> = self.sta_stats_cache
                    .iter()
                    .filter_map(|((src, dest), info)| {
                        let is_ul = src > dest;
                        let limit = if is_ul { self.ul_capacity_queue_device } else { self.queue_maxsize_dl };
                        if info.packet_count > limit {
                            Some(((*src, *dest), info.packet_count - limit))
                        } else {
                            None
                        }
                    })
                    .collect();

                    for ((src, dest), excess_count) in overflowing_flows {
                    let is_ul_flow = src > dest;
                    let sta_for_key: i32 = if is_ul_flow { src } else { -1 };

                    // Try every AC × link combination for this flow direction.
                    for ac in [EdcaAc::Voice, EdcaAc::Video, EdcaAc::BestEffort, EdcaAc::Background] {
                        let mut remaining_drops = excess_count;
                        // let link_ids: Vec<u8> = self.link_mediums.keys().copied().collect();
                        // let link_ids: SmallVec<[u8; 4]> = self.link_mediums.keys().copied().collect();

                        for &lid in self.link_mediums.keys() {
                            if remaining_drops == 0 { break; }
                            let overflow_key: MacKey = (sta_for_key, ac, lid);
                            if let Some(&idx) = self.mac_key_index.get(&overflow_key) {
                               if is_ul_flow {
                                    // UL: queue is per-flow, pop_back is safe
                                    let drop_count = remaining_drops.min(self.per_flow_queues[idx].len());
                                    for _ in 0..drop_count {
                                        if let Some(dropped) = self.per_flow_queues[idx].pop_back() {
                                            self.blocked_packet_counter += 1;
                                            if let Some(count) = self.link_queue_depths.get_mut(&lid) {
                                                *count = count.saturating_sub(1);
                                            }
                                            if let Some(c) = self.active_mac_key_counts.get_mut(&(src, ac)) {
                                                *c = c.saturating_sub(1);
                                            }
                                            if let Some(entry) = self.sta_stats_cache
                                                .get_mut(&(dropped.sta_src_id, dropped.sta_dest_id))
                                            {
                                                entry.packet_count = entry.packet_count.saturating_sub(1);
                                                entry.expected_queue_delivery_ms =
                                                    entry.per_packet_channel_access_efficiency
                                                    * entry.packet_count as f64 * 1000.0;
                                            }
                                        }
                                    }
                                    remaining_drops -= drop_count;
                                } else {
                                    // DL: shared queue — target only packets for this specific destination
                                    let target_dest = dest as i32;
                                    let mut dropped_count = 0usize;
                                    self.per_flow_queues[idx].retain(|p| {
                                        if dropped_count < remaining_drops && p.sta_dest_id == target_dest {
                                            dropped_count += 1;
                                            false
                                        } else {
                                            true
                                        }
                                    });
                                    for _ in 0..dropped_count {
                                        self.blocked_packet_counter += 1;
                                        if let Some(count) = self.link_queue_depths.get_mut(&lid) {
                                            *count = count.saturating_sub(1);
                                        }
                                        if let Some(c) = self.active_mac_key_counts.get_mut(&(-1i32, ac)) {
                                            *c = c.saturating_sub(1);
                                        }
                                    }
                                    if let Some(entry) = self.sta_stats_cache.get_mut(&(src, dest)) {
                                        entry.packet_count = entry.packet_count.saturating_sub(dropped_count);
                                        entry.expected_queue_delivery_ms =
                                            entry.per_packet_channel_access_efficiency
                                            * entry.packet_count as f64 * 1000.0;
                                    }
                                    remaining_drops -= dropped_count;
                                }
                            }
                        }
                    }
                }
            }

            // Get ready contenders per link
            let ready_per_link: HashMap<u8, Vec<MacKey>> = self.tick_backoff(now);

            // Process each link independently
            let mut transmissions_scheduled = false;

            for (link_id, mut pre_contenders) in ready_per_link {
                if pre_contenders.is_empty() {
                    continue;
                }

                // Resolve virtual collisions (same STA, different ACs)
                let contenders = self.resolve_virtual_collision(pre_contenders);

                if contenders.is_empty() {
                    continue;
                }

                // Check for physical collision (different STAs)
                let collision_now = contenders.len() > 1;

                if collision_now {
                    // Handle collision on this link
                    let T_col = collision_delay();
                    let T_col_dur = Duration::from_secs_f32(T_col);
                    // print_yellow!("{:.5} [Channel {} collision!] T_col:{:.5}| contenders: {:?} ", taitime_to_f64!(now), link_id, T_col, contenders );
                    if let Some(medium) = self.link_mediums.get_mut(&link_id) {
                        medium.occupy_collision(now, now + T_col_dur);
                        
                        if VISUALIZER_QUEUES_ENABLED {
                            self.emit_visualization_event(VizEvent::Collision {
                                t: t_secs(now),
                                end: t_secs(now + T_col_dur),
                                link_id,
                                contenders: contenders.iter().copied().collect(),
                            });
                        }
                    }

                    let first_contender_key = contenders.first().unwrap();

                    
                    for key in contenders.clone() {
                        let st = self.dcf_mut(&key); 
                        let old_cw = st.cw;
                        st.on_failure();
                        debug_edca_r!(
                            " \t\t ↳ ({}, {:?}, L-{}): CW {} → {}, backoff={}",
                            key.0,
                            key.1,
                            key.2,
                            old_cw,
                            st.cw,
                            st.backoff_counter
                        );
                        
                    }
  
                    // Freeze all MACs on this link
                    for st in &mut self.dcf_values {
                        if st.mac_key.2 == link_id {
                            st.backoff_frozen = true;
                            st.medium_free_since = now + T_col_dur;
                        }
                    }

                    for key in contenders.iter() {
                        let contender_key: WindowKey = (key.0, key.2); // this could be cleaner :D
                        self.update_window_stats(
                            contender_key,
                            None,
                            T_col as f64,
                            false,
                            now.duration_since(TaiTime::EPOCH).as_secs_f64()
                        );

                        let st = self.dcf(key);

                        if let Some(tx) = &self.viz_tx {
                            if VISUALIZER_QUEUES_ENABLED{
                                let _ = tx.try_send(VizEvent::BackoffSnap {
                                    t: t_secs(now),
                                    mac_key: *key,
                                    counter: st.backoff_counter,
                                    cw: st.cw,
                                    frozen: st.backoff_frozen,
                                    medium_free_since: t_secs(st.medium_free_since),
                                    });
                            }
                        }

                    }

                    if let Some(stats_tx) = &self.stats_tx {
                        // Deconstruct key. Assuming key is (sta_id, ac, link_id) based on your debug print
                        for key in &contenders {
                            let (sta_id, ac, winner_link_id) = *key;

                            // Try to find the packet at the head of the queue for this specific collider
                            // to extract real SRC/DEST and packet metrics.
                           
                            // NEW — O(1)
                            let collision_mac_key: MacKey = (sta_id, ac, winner_link_id);
                            let colliding_packet = self.mac_key_index
                                .get(&collision_mac_key)
                                .and_then(|&idx| self.per_flow_queues[idx].front());

                            // let ac_queue_length_when_out = *self.mac_queue_depths
                            //     .get(&key)
                            //     .unwrap_or(&0);

                            let stats_update = if let Some(p) = colliding_packet {
                                // We found the actual packet that collided!
                                StatsUpdate {
                                    now,
                                    is_collision: true,
                                    collision_backoff: T_col as f64,
                                    sta_src_id: p.sta_src_id,
                                    sta_dest_id: p.sta_dest_id,
                                    link_id: link_id,
                                    packet_id: p.packet_id as i32,
                                    length_packet: p.length_packet_bits,
                                    T_q: now.duration_since(p.queue_in_instant).as_secs_f64(),
                                    alvr_header: p.header_alvr.clone(),
                                    T_s: 0.0, // Transmission didn't succeed
                                    blocked_packet_counter: self.blocked_packet_counter,
                                    arrived_packet_counter: self.arrived_packet_counter,
                                    queue_length_when_out: 0, 
                                    ampdu_id: self.ampdu_id,
                                    edca_ac: p.edca_ac,
                                }
                            } else {
                                // Fallback: If no packet found (shouldn't happen if DCF is active), 
                                // use the key info with dummy dest.
                                StatsUpdate {
                                    now,
                                    is_collision: true,
                                    collision_backoff: T_col as f64,
                                    sta_src_id: sta_id,
                                    sta_dest_id: if sta_id == -1 { 1 } else { -1 },
                                    link_id: link_id,
                                    ..StatsUpdate::default() // Assuming you have Default or fill manually
                                }
                            };

                            stats_tx.send(stats_update).expect("Failed to send collision stats");
                        }
                    }

                    // Schedule wake-up after collision resolves
                    context
                        .scheduler
                        .schedule_event(T_col_dur, Self::deque_schedule_service, ())
                        .unwrap();

                    // Mark that we scheduled something
                    self.slot_tick_pending = true; // ← ADD — collision owns the next wake-up
                    transmissions_scheduled = true;

                    continue;
                }

                // Single winner on this link
                let winner_key = contenders[0];
                let (sta_id, ac, winner_link_id) = winner_key;

                // NEW — O(1) primary; O(num_links) STR+ fallback
                let winner_flow_idx = self.mac_key_index[&winner_key];
                let first_packet: MpduPacket = {
                    // Try primary deque first.
                    let primary_front = self.per_flow_queues[winner_flow_idx].front().cloned();

                    let found = if primary_front.is_some() {
                        primary_front
                    } else if STR_PLUS_MODE_MLO {
                        // Primary deque empty: check other links' deques for same (STA, AC).
                        let sta_for_deque: i32 = if sta_id == -1 { -1 } else { sta_id };
                        let mut other_links: Vec<u8> = self.link_mediums.keys().copied()
                            .filter(|&lid| lid != winner_link_id)
                            .collect();
                        other_links.sort_unstable();
                        let mut alt_found = None;
                        for alt_lid in other_links {
                            let alt_key: MacKey = (sta_for_deque, ac, alt_lid);
                            if let Some(&alt_idx) = self.mac_key_index.get(&alt_key) {
                                if let Some(p) = self.per_flow_queues[alt_idx].front() {
                                    alt_found = Some(p.clone());
                                    break;
                                }
                            }
                        }
                        alt_found
                    } else {
                        None
                    };

                    match found {
                        Some(p) => p,
                        None => {
                            let st = self.dcf_mut(&winner_key);
                            st.on_success(st.param.cw_min);
                            continue;
                        }
                    }
                };

                // Build AMPDU for this link
                let (mut ampdu_to_send, ampdu_airtime) =
                    self.build_new_ampdu(&first_packet, link_id, now);
                ampdu_to_send.link_id = link_id; // Tag AMPDU with link

                if ampdu_to_send.mpdu_packets.is_empty() || ampdu_airtime == Duration::ZERO {
                    log_mlo!(
                        now,
                        "⚠️  LINK-{}: Empty AMPDU or zero airtime, skipping transmission",
                        link_id
                    );
                    self.link_is_transmitting.insert(link_id, false); 
                    // Reset backoff for this MAC to try again
                    let st = self.dcf_mut(&winner_key); 
                    st.on_failure(); // Increase CW and redraw backoff

                    // schedule next tick to allow backoff countdown (no deadlock)
                    self.schedule_tick(context); 
                    transmissions_scheduled = true; // Prevent duplicate scheduling

                    continue;
                }

                // Occupy medium on this link
                if let Some(medium) = self.link_mediums.get_mut(&link_id) {
                    medium.start_txop(now, now + ampdu_airtime, winner_key);
                    let unique_ids: Vec<u32> = ampdu_to_send.mpdu_packets
                        .iter()
                        .map(|p| p.header_alvr.next_packet_index)
                        .collect::<HashSet<_>>() // Filters duplicates automatically
                        .into_iter()
                        .collect();

                    let unique_stream_ids: Vec<u16> = ampdu_to_send.mpdu_packets
                        .iter()
                        .map(|p| p.header_alvr.stream_id)
                        .collect::<HashSet<_>>()
                        .into_iter()
                        .collect();

                    if VISUALIZER_QUEUES_ENABLED {
                        self.emit_visualization_event(VizEvent::TxopStart {
                            t: t_secs(now),
                            end: t_secs(now + ampdu_airtime),
                            link_id,
                            owner: winner_key,
                            dest_id: first_packet.sta_dest_id,
                            ampdu_packets: ampdu_to_send.mpdu_packets.len() as u16,
                            mcs: ampdu_to_send.mcs_assigned,
                            alvr_frame_ids: unique_ids, 
                            alvr_stream_ids: unique_stream_ids,
                            alvr_frame_losses: first_packet.header_alvr.frame_losses, 
                        });
                    }
                }

                // Freeze all MACs on this link during TXOP
                for st in &mut self.dcf_values {
                    if st.mac_key.2 == link_id {
                        st.backoff_frozen = true;
                        st.medium_free_since = now + ampdu_airtime;
                    }
                }
                
                // if let Ok(mut map) = self.array_dcf_values.lock() {
                //     for ((_, _, lid), st) in map.iter_mut() {
                //         if *lid == link_id {
                //             st.backoff_frozen = true;
                //             st.medium_free_since = now + ampdu_airtime;
                //         }
                //     }
                // }   

                // Schedule TX completion
                context
                    .scheduler
                    .schedule_event(ampdu_airtime, Self::send_ampdu, ampdu_to_send)
                    .unwrap();

                transmissions_scheduled = true;
            }

            // Schedule next slot check if needed
            
            let mut need_next_slot = false;

            // if self.per_flow_queues.iter().any(|dq| !dq.is_empty()) {
            if self.link_queue_depths.values().any(|&d| d > 0) {
                'outer: for st in &self.dcf_values {
                    let (sta_id, ac, link_id) = st.mac_key;

                    // Skip mid-TXOP links — send_ampdu will reschedule at completion
                    if *self.link_is_transmitting.get(&link_id).unwrap_or(&false) {
                        continue;
                    }

                    // Any MAC on an idle link that has work needs a next slot tick,
                    // whether it's frozen, counting down, OR already at zero (ready).
                    if self.active_mac_key_counts.get(&(sta_id, ac)).copied().unwrap_or(0) > 0 {
                        need_next_slot = true;
                        break 'outer;
                    }
                }
            }

            if need_next_slot {
                self.schedule_tick(context);
            }
        
        
        }
    }
}

impl Model for QueueModule {}

#[derive(Clone, Default)]
#[allow(unused)]
pub struct DataSink {
    pub system_time: f64,
    pub av_l: f64,
    pub last_time: f64,
    pub rx_packets_counter: usize,
}
#[allow(unused)]
impl DataSink {
    pub fn new() -> Self {
        Self {
            system_time: 0.0,
            av_l: 0.0,
            last_time: 0.0,
            rx_packets_counter: 0,
        }
    }
    pub fn print_nicely(&self) {
        let width = 48; // Total width of the table
        let separator = format!("+{}+", "-".repeat(width));

        // Helper closure to format a row
        let format_row = |label: &str, value: f64| format!("| {:<30} | {:>14.6} |", label, value);

        // Print the header
        println!("{}", separator);
        println!(
            "{:^50}",
            "| SINK                                           |"
        );
        println!("{}", separator);

        // Print statistics
        println!(
            "{}",
            format_row(
                "Average System Time",
                self.system_time / self.rx_packets_counter as f64
            )
        );
        println!(
            "{}",
            format_row(
                "Avg Received Throughput[Mbps]",
                (self.av_l / self.last_time) / 1E6
            )
        );

        // Print the footer
        println!("{}", separator);
    }
}

#[derive(Default)]
pub struct Sink {
    // pub input: Input <MpduPacket>,
    pub received_packet_counter: usize,
    pub mutex_data: Arc<Mutex<DataSink>>,
}
#[allow(unused)]
impl Sink {
    pub fn new() -> Self {
        Self {
            received_packet_counter: 0,
            mutex_data: Arc::new(Mutex::new(DataSink::new())),
        }
    }

    pub fn get_data_handle(&self) -> Arc<Mutex<DataSink>> {
        Arc::clone(&self.mutex_data)
    }

    pub async fn input(&mut self, ampdu_packet: AmpduPacket, context: &Context<Self>) {
        let now = context.scheduler.time();

        for mut packet in ampdu_packet.mpdu_packets {
            // packet.T_s = now.duration_since(packet.queue_out_instant);

            // debug_print!(
            //     DebugColor::Magenta,
            //     "{} [DBG SINK ] ---Packet {} arrived from STA{} into Sink (STA{}, T_s = {})",
            //     format_elapsed!(now),
            //     packet.packet_id,
            //     packet.sta_src_id,
            //     packet.sta_dest_id,
            //     packet.T_s.as_secs_f64(),

            // );
            if packet.data_inner.len() >= 100 {
                if let Ok((
                    packet_length,
                    stream_id,
                    next_packet_index,
                    shards_count,
                    shard_index,
                    tx_r_instant,
                )) = parse_shard_data(&packet.data_inner[..100])
                {
                    let str_id = match stream_id {
                        0 => "Tracking",
                        1 => "Haptics",
                        2 => "Audio",
                        3 => "Video",
                        4 => "Statistics",
                        _ => "?? IDK",
                    };

                    debug_print!(
                        DebugColor::DarkRed,
                        "\nPacket length: {}| Stream ID: {}| Next packet index: {}| Shards count: {} | Shard index: {} | Transmit-receive instant: {} |\n--------------------------------------------------------------------------------------------------------------------------------------------------------------------------__",
                        packet_length,
                        str_id,
                        next_packet_index,
                        shards_count,
                        shard_index,
                        tx_r_instant
                    );
                }
            }

            // println!("{} - Packet received!!", format_duration(elapsed));
            // packet.print();

            let packet_total_time = now.duration_since(packet.queue_in_instant);

            if let Ok(mut data) = self.mutex_data.lock() {
                data.system_time += packet_total_time.as_secs_f64();
                data.av_l += packet.length_packet_bits as f64;
                data.rx_packets_counter += 1;
                data.last_time = taitime_to_f64!(now);
            }

            self.received_packet_counter += 1;
        }
    }
}

impl Model for Sink {}

#[cfg(test)]
mod markov_scratch_tests {
    use super::*;
    #[test]
    fn markov_time_share_check() {
        // Non-overlapping placeholder ranges, same dwell/transitions as the real preset,
        // purely so this check can attribute each generated segment to a state unambiguously
        // (the real WIFI_CLOUD_VR_STATES ranges legitimately overlap between Excellent/Good).
        let mut test_states = WIFI_CLOUD_VR_STATES;
        test_states[0].bps_range = (1000.0, 1099.0); // Excellent
        test_states[1].bps_range = (900.0, 999.0);   // Good
        test_states[2].bps_range = (800.0, 899.0);   // Fair
        test_states[3].bps_range = (700.0, 799.0);   // Poor
        test_states[4].bps_range = (600.0, 699.0);   // Congested
        test_states[5].bps_range = (500.0, 599.0);   // Fade

        let mut totals = [0.0f64; 6];
        let names = ["Excellent","Good","Fair","Poor","Congested","Fade"];
        for _ in 0..20000 {
            let mut emu = NetworkPatternEmulator::new(IpAddr::V4(std::net::Ipv4Addr::new(127,0,0,1)));
            let start = TaiTime::<0>::EPOCH;
            let end = start.checked_add(Duration::from_secs_f64(300.0)).unwrap();
            emu.add_markov_modulated_bandwidth(&test_states, &WIFI_CLOUD_VR_TRANSITIONS, start, end, 0, None);
            for p in emu.get_patterns() {
                if let NetworkPattern::Bandwidth { valid_from, valid_until, max_bps, .. } = p {
                    let dur = valid_until.duration_since(*valid_from).as_secs_f64();
                    let idx = test_states.iter().position(|s| {
                        *max_bps >= s.bps_range.0 && *max_bps <= s.bps_range.1
                    }).unwrap();
                    totals[idx] += dur;
                }
            }
        }
        let grand_total: f64 = totals.iter().sum();
        for (n, t) in names.iter().zip(totals.iter()) {
            println!("{:10} time_frac={:.4}", n, t/grand_total);
        }
    }
}
