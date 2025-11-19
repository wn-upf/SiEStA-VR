

use crate::{
    debug_bgprint, debug_debug, print_prettyyyy, print_red, print_yellow
    // print_blue, print_dblue, print_green, print_pretty,
};
use crossbeam::channel::{unbounded, Receiver, Sender};
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
use std::time::{Duration, Instant};
use rand_distr::{Normal, Distribution, Exp};
use crate::lib::alvr_stream_socket::parse_shard_data;
use crate::lib::{ DOWNLINK_QUEUE_SIZE, UPLINK_QUEUE_SIZE,  MacKey, SLOT};
use crate::lib::DebugColor; 
use rand::rngs::StdRng;
// use crate::lib::TESTS_RANDOM_PATTERNS;
use std::sync::{Arc, Mutex};
use tai_time::TaiTime;
use serde::{Serialize, Deserialize};
use std::collections::HashSet;
use crate::{debug_print, format_elapsed, taitime_to_f64};

use crate::lib::{
    collision_delay, exponential,  airtime_ampdu, perStaLockStats, AmpduPacket, Coords,
    CsvType, CumulativeStats, MpduPacket, DEBUG_PRINT_ENABLED, DEFAULT_TMAX_AGG, MAX_AMPDU_SIZE, NUMBER_OF_RANDOM_EVENTS,
    P_TX,
};

use rand::{SeedableRng};

////////////////////////// CONSTS/////////////////////
pub const ROOM_W: f64 = 24.0;
pub const ROOM_H: f64 = 12.0;

pub const AP_X: f64 = ROOM_W / 2.0;  /// Access Point at room center 
pub const AP_Y: f64 = ROOM_H / 2.0;


pub const MAX_EMULATED_QUEUE_PACKETS: usize = 10000;
pub const CSV_PER_PACKET: bool = true; // To collect Queueing times, Service, queue state, collisions per-packet in QUEUE_STATS.csv

pub const STEP1_TBEGIN: f64 = 10.0;
pub const STEP1_TEND: f64 =   20.0;

pub const STEP2_TBEGIN: f64 = 30.0;
pub const STEP2_TEND: f64 =   40.0;

pub const STEP3_TBEGIN: f64 = 50.0;
pub const STEP3_TEND: f64 =   60.0;

pub const BANDWIDTH_LIMIT_S1: f64 = 100E6;
pub const BANDWIDTH_LIMIT_S2: f64 = 95E6;
pub const BANDWIDTH_LIMIT_S3: f64 = 90E6;

pub const REFILL_INTERVAL: Duration = Duration::from_micros(5);
pub const MTU_EMULATED: f64 = 1500.0 * 8.0 * 10.0 ; // allow bursts of N MTUs 
pub const DEBUG_EDCA: bool =    false; 
pub const DEBUG_MLO: bool = false;



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

macro_rules! log_mlo {
    ($now:expr, $($arg:tt)*) => {
        if DEBUG_MLO {
            print!("\x1b[38;5;141m{} [MLO] ", format_elapsed!($now));
            println!($($arg)*);
            print!("\x1b[0m");
        }
    };
}

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

    pub rng_seed: StdRng
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

            let len_random = exponential(self.mean_length_packets as f64, &mut self.rng_seed) as usize;
            packet.length_packet_bits = cmp::max(1, len_random);

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

                let mut time_interarrival =
                    Duration::from_secs_f64(exponential(1.0 / self.arrival_rate, &mut self.rng_seed));

                time_interarrival = max(time_interarrival, Duration::from_nanos(1));

                let len_random = exponential(self.mean_length_packets as f64, &mut self.rng_seed) as usize;

                // let len_random = self.mean_length_packets as usize;

                packet.length_packet_bits = cmp::max(1, len_random);
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
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")] // optional: emits "gaussian"/"uniform" instead of Rust‑style names
pub enum JitterDistributionType {
    Gaussian,
    Uniform,
}

#[allow(unused, unused_variables)]
#[derive(Clone, Debug, Serialize, Deserialize )]
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

use crate::lib::taitime_serde; 


#[allow(unused)]
impl NetworkPattern {
    /// Create a new `NetworkPattern` of type Bandwidth
    pub fn new_bandwidth(
        max_bps: f64,
        token_refill_rate: f64,
        valid_from: TaiTime<0>,
        valid_until: TaiTime<0>,
    ) -> Self {

        let valid_s = valid_from.duration_since(TaiTime::EPOCH).as_secs_f64(); 

        Self::Bandwidth {
            max_bps,
            current_tokens: max_bps, // Initialize tokens to maximum
            max_tokens: max_bps,     // Maximum bucket capacity
            token_refill_rate,
            last_refill: valid_s, 
            valid_from,
            valid_until,
        }
    }
    fn bandwidth_account(
        &mut self,
        now: TaiTime<0>,
        packet_bits: Option<f64>,
    ) -> (bool /*can_send*/, Duration /*delay if not*/) {
        let Self::Bandwidth {
            current_tokens,
            max_tokens,
            token_refill_rate,
            last_refill,
            ..
        } = self else { unreachable!() };

        let mut now = now.duration_since(TaiTime::EPOCH).as_secs_f64(); 
        // Refill
        let dt = now - *last_refill; 
        *current_tokens = (*current_tokens + dt * *token_refill_rate).min(*max_tokens);
        *last_refill = now;

        if let Some(bits) = packet_bits {
            if *current_tokens >= bits {
                *current_tokens -= bits;           // send – netem path
                return (true, Duration::ZERO);
            }
            let need = bits - *current_tokens;     // queue – netem path
            *current_tokens -= bits;               // go negative – keep the deficit            
            let delay = need / *token_refill_rate;
            return (false, Duration::from_secs_f64(delay));
        }
        (false, Duration::ZERO) // called as pure refill
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
        }.to_string()];


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
                row.extend(std::iter::repeat(String::new())
                    .take(Self::csv_headers().len() - row.len()));
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
                row.extend(std::iter::repeat(String::new())
                    .take(Self::csv_headers().len() - row.len()));
            }

            NetworkPattern::Bandwidth {
                max_bps,
                current_tokens,
                max_tokens,
                token_refill_rate,
                last_refill,
                valid_from,
                valid_until,
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
                row.extend(std::iter::repeat(String::new())
                    .take(Self::csv_headers().len() - row.len()));
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
                row.extend(std::iter::repeat(String::new())
                    .take(Self::csv_headers().len() - row.len()));
            }

            NetworkPattern::Constant => {
                // nothing else to push—just pad out the full width
                row.extend(std::iter::repeat(String::new())
                    .take(Self::csv_headers().len() - 1));
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

    /// Consumes tokens from the bucket and returns `true` if sufficient tokens exist
    pub fn consume_tokens(&mut self, packet_size: usize) -> bool {
        match self {
            Self::Bandwidth {
                current_tokens,
                max_tokens,
                token_refill_rate,
                ..
            } => {
                let packet_tokens = (packet_size * 8) as f64; // Convert packet size to bits
                let refilled_tokens = token_refill_rate.min(*max_tokens - *current_tokens);
                *current_tokens += refilled_tokens; // Refill tokens
                if *current_tokens >= packet_tokens {
                    *current_tokens -= packet_tokens; // Consume tokens
                    true
                } else {
                    false
                }
            }
            // Other patterns could return `true` or implement specific logic
            _ => true, // Default to always allowing transmission
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
    /// Time when the link will be free (last packet finishes transmission)
    link_free_time: TaiTime<0>,
}
#[allow(unused)]
impl EmulatedLink {
    /// Create a new emulated link.
    /// - `max_queue_size`: maximum packets to buffer in the emulator
    /// - `now`: current simulator time as a `TaiTime`
    /// - `tests`: tuple flags `(bandwidth, jitter, packet_loss, random_events)`
    
    pub fn get_network_patterns(&self) -> &[NetworkPattern] {
        &self.queue_mechanism.network_emulator.get_patterns()
    }
    
    pub fn new(
        max_queue_size: usize,
        now: TaiTime<0>,
        emulated_tests: Option<(bool, bool, bool, bool)>,
        id_sta: IpAddr, 
    ) -> Self {
        let queue_mechanism: QueueMechanism;

        if let Some(values_tests) = emulated_tests {
            queue_mechanism =
                QueueMechanism::new(max_queue_size, now, values_tests, id_sta);
        } else {
            print_yellow!("NO PATTERNS?", ); 
            queue_mechanism = QueueMechanism::new(
                MAX_EMULATED_QUEUE_PACKETS,
                TaiTime::EPOCH,
                (false, false, false, false),
                id_sta
            );
        }

        EmulatedLink {
            output: Default::default(),
            queue_mechanism,
            bandwidth_bps: None, // Disabled by default for backward compatibility
            link_free_time: now,
        }
    }

    /// Create a new emulated link with bandwidth emulation (e.g., 1 Gbps).
    /// - `bandwidth_bps`: bandwidth in bits per second (e.g., 1_000_000_000 for 1 Gbps)
    pub fn new_with_bandwidth(
        max_queue_size: usize,
        now: TaiTime<0>,
        emulated_tests: Option<(bool, bool, bool, bool)>,
        id_sta: IpAddr,
        bandwidth_bps: u64,
    ) -> Self {
        let mut link = Self::new(max_queue_size, now, emulated_tests, id_sta);
        link.bandwidth_bps = Some(bandwidth_bps);
        link
    }

    /// Convenience method to create a 1 Gbps emulated link
    pub fn new_1gbps(
        max_queue_size: usize,
        now: TaiTime<0>,
        emulated_tests: Option<(bool, bool, bool, bool)>,
        id_sta: IpAddr,
    ) -> Self {
        Self::new_with_bandwidth(max_queue_size, now, emulated_tests, id_sta, 1_000_000_000)
    }

    /// Enable or disable bandwidth emulation
    pub fn set_bandwidth(&mut self, bandwidth_bps: Option<u64>) {
        self.bandwidth_bps = bandwidth_bps;
    }

    /// Calculate transmission delay for a packet based on its size and configured bandwidth
    fn calculate_transmission_delay(&self, packet_size_bytes: usize) -> Option<Duration> {
        self.bandwidth_bps.map(|bw| {
            // Transmission time = (packet_size_bits) / (bandwidth_bps)
            let packet_size_bits = (packet_size_bytes as u64) * 8;
            let delay_nanos = (packet_size_bits * 1_000_000_000) / bw;
            Duration::from_nanos(delay_nanos.max(1))
        })
    }

    /// Handle packet arrival from a STA. Applies emulation logic, possibly queuing or dropping.
    pub async fn input(&mut self, mut packet: MpduPacket, context: &Context<Self>) {
        let now = context.scheduler.time();
        
        // Apply bandwidth emulation if enabled
        if let Some(transmission_delay) = self.calculate_transmission_delay(packet.length_packet_bits * 8) {
            // Calculate when this packet can start transmission
            let transmission_start = if now >= self.link_free_time {
                now // Link is free, start immediately
            } else {
                self.link_free_time // Link is busy, queue behind previous packet
            };
            
            // Update when the link will be free
            self.link_free_time = transmission_start + transmission_delay;
            
            // Set the packet's deadline
            packet.emulated_added_delay_deadline = Some(self.link_free_time);
        }
        
        // Delegate to the queue mechanism
        match self.queue_mechanism.enqueue_or_transmit(packet, context) {
            EnqueueResult::Transmitted(pkt) => {
                // No emulation delay: forward immediately
                self.output.send(pkt).await;
            }
            EnqueueResult::Queued(queued_pkt) => {
                // Scheduled for delayed transmission
                if let Some(deadline) = queued_pkt.emulated_added_delay_deadline {
                    let delay = deadline
                        .duration_since(now)
                        .max(Duration::from_nanos(1));
                    // Schedule a flush event
                    
                    self.queue_mechanism.next_flush_scheduled = Some(deadline); 
                    context
                        .scheduler
                        .schedule_event(delay, Self::flush_queue, ())
                        .unwrap();
                }
            }
            EnqueueResult::Dropped => {
                // Packet dropped by network pattern or overflow
                // Reset link time if bandwidth emulation is enabled
                if self.bandwidth_bps.is_some() {
                    self.link_free_time = now;
                }
            }
        }
    }

    pub fn flush_queue<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            self.queue_mechanism.next_flush_scheduled = None;
            // This now efficiently gets only the ready packets
            let ready = self.queue_mechanism.process_emu_queued_packets(context);
            for pkt in ready {
                self.output.send(pkt).await;
            }

            // *** OPTIMIZATION ***
            // Efficiently schedule the next flush based on the *new* front packet.
            if let Some(next_pkt) = self.queue_mechanism.queue.front() {
                if let Some(next_deadline) = next_pkt.emulated_added_delay_deadline {
                    let now = context.scheduler.time();
                    let delay = next_deadline.duration_since(now).max(Duration::from_nanos(1));
                    self.queue_mechanism.next_flush_scheduled = Some(next_deadline);
                    
                    context.scheduler.schedule_event(delay, Self::flush_queue, ()).unwrap();
                }
            }
        }
    }

}

impl Model for EmulatedLink {}

#[derive(Debug)]
pub enum EnqueueResult {
    Transmitted(MpduPacket), // Packet was immediately transmitted
    Queued(MpduPacket),      // Packet was added to the queue
    Dropped,                 // Packet was dropped due to queue overflow
}
#[derive(Clone)]
pub struct QueueMechanism {
    queue: VecDeque<MpduPacket>,              // Packet queue
    network_emulator: NetworkPatternEmulator, // Bandwidth pattern
    max_queue_size: usize,
    next_flush_scheduled: Option<TaiTime<0>>, 
    last_bw_pattern_logged: Option<(TaiTime<0>, usize)>, // (last_log_time, pattern_index)    
    id_sta: IpAddr, 
}

impl QueueMechanism {
    pub fn new(
        max_emulated_queue_packets: usize,
        _now: TaiTime<0>,
        tests: (bool, bool, bool, bool),
        id_sta: IpAddr, 
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

        let (test_bw, test_jitter, test_pl, test_random) = tests;

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
                NUMBER_OF_RANDOM_EVENTS,                                  // Number of events
                RandomEventKind::Bandwidth,         // Event type: Bandwidth limit
                overall_start,                      // Overall window start time
                overall_end,                        // Overall window end time
                Duration::from_millis(3000),         // Minimum duration per event
                Duration::from_millis(15000),        // Maximum duration per event
                JitterDistributionType::Uniform,    // Distribution for event duration
                50e6,                                // Maximum bps (1Mbps) as mean_value
                40e6,                                // Variance 
            );
        }


        if test_bw {

            print_red!("****BW PATTERNS ADDED*****", ); 
            
        
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
                drop_probability: (0.02),
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
            id_sta, 
        }
    }


    pub fn enqueue_or_transmit(
        &mut self,
        mut packet: MpduPacket,
        context: &Context<EmulatedLink>,
    ) -> EnqueueResult {
        let now = context.scheduler.time();
        // print_yellow!("{} Enqueue or transmit? ", format_elapsed!(now)); 

        // let mut dbg_reason = "no‑pattern";
        // let mut _dbg_delay  = Duration::ZERO;
        // let _dbg_packet = packet.clone(); 

        if self.network_emulator.last_update_time + REFILL_INTERVAL <= now {
            self.network_emulator.refill_all_buckets(now);
        }
        self.log_active_bw_patterns(now, &packet);
        if packet.length_packet_bits == 0 {
            packet.length_packet_bits = packet.data_inner.len();   // fallback for early traffic
        }          
        // Get the potential delay for the packet
        let reason = match self.network_emulator.should_transmit_with_delay(
            &mut packet,
            now,
        ) {
            Some(delay) if delay == Duration::ZERO => {
                // Immediate transmission possible
                // dbg_reason = "immediate";
                EnqueueResult::Transmitted(packet)
            }
            Some(delay) => {

                // Add the delay to the packet's queue_in_instant
                let mut delayed_packet = packet.clone();
                delayed_packet.emulated_added_delay_deadline = now.checked_add(delay);

                // Enqueue the packet
                if self.queue.len() < self.max_queue_size {

                    let needs_flush = match self.next_flush_scheduled {
                        None => true,
                        Some(scheduled_time) => {
                            // Only reschedule if this packet would be ready sooner
                            delayed_packet.emulated_added_delay_deadline.unwrap() < scheduled_time
                        }
                    }; 

                    self.queue.push_back(delayed_packet.clone());
                    if needs_flush {
                        EnqueueResult::Queued(delayed_packet)
                    } else {
                        // Don't trigger new scheduling
                        EnqueueResult::Queued(MpduPacket { 
                            emulated_added_delay_deadline: None, 
                            ..delayed_packet 
                        })
                    }
                    
                } else {
                    // print_red!(
                    //     "[NETEM FULL queue] Packet DROPPED (ALVR Stream: {} | Frame_id: {} , {} / {})",
                    //     // delayed_packet.packet_id,
                    //     delayed_packet.header_alvr.stream_id, 
                    //     delayed_packet.header_alvr.next_packet_index,
                    //     delayed_packet.header_alvr.shard_index,
                    //     delayed_packet.header_alvr.shards_count
                    // );
                    EnqueueResult::Dropped
                }
            }
            None => { 
                     // dbg_reason = "drop";
                      EnqueueResult::Dropped
                    },
        }; 

        // db_debug_bgprint!(
        //     DebugColor::Cyan,
        //     "[NETEM] t={:.6}  decision={}  delay={:.6}  pkt_len={} ",
        //     format_elapsed!(now),
        //     dbg_reason,
        //     dbg_delay.as_secs_f64(),
        //     dbg_packet.length_packet,
        //     // active_patterns
        //         // .first()
        //         // .map(|p| if let NetworkPattern::Bandwidth { current_tokens, .. } = p { *current_tokens } else { 0. })
        // );
        reason


    }
    pub fn process_emu_queued_packets(
        &mut self,
        context: &Context<EmulatedLink>,
    ) -> Vec<MpduPacket> {
        let now = context.scheduler.time();
        let mut ready: Vec<MpduPacket> = Vec::new();

        // *** OPTIMIZATION ***
        // Efficiently process only the ready packets from the front.
        // This loop stops as soon as it finds a packet that is not ready.
        while let Some(p) = self.queue.front() {
            if let Some(deadline) = p.emulated_added_delay_deadline {
                if now >= deadline {
                    // Packet is ready, pop it and add to the ready list
                    ready.push(self.queue.pop_front().unwrap()); // We know it's Some
                } else {
                    // The front packet is not ready, so no subsequent packet can be.
                    break;
                }
            } else {
                // Packet has no deadline, treat as dropped (matches original logic)
                self.queue.pop_front();
            }
        }
        ready
    }
    
    fn log_active_bw_patterns(&mut self, now: TaiTime<0>, packet: &MpduPacket) {
            // Log at most once per second to avoid spam
            let should_log = match self.last_bw_pattern_logged {
                None => true,
                Some((last_time, _)) => {
                    now.duration_since(last_time) >= Duration::from_secs(1)
                }
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

                        crate::print_dblue!(
                            "{:.6}[BW EMU {} ({:.5}->{:.5})] | {} | Limit: {:.2} Mbps | Tokens: {:.0}/{:.0} Mbits ",
                            format_elapsed!(now),
                            self.id_sta, 
                            format_elapsed!(valid_from),
                            format_elapsed!(valid_until), 
                            direction,
                            max_bps / 1e6,
                            current_tokens / 1e6,
                            max_tokens / 1e6,
                            // token_refill_rate / 1e6
                        );

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

#[allow(unused)]
#[derive(Clone, Debug)]
pub struct NetworkPatternEmulator {
    patterns: Vec<NetworkPattern>,
    last_update_time: TaiTime<0>,
    last_update_only_DBG_NETEM: TaiTime<0>,
    debug_counter: usize, // Counter to track the calls
    ip_parent: IpAddr, 
}
impl NetworkPatternEmulator {
    pub fn new(ip_parent: IpAddr) -> Self {
        Self {
            patterns: Vec::new(),
            last_update_time: TaiTime::default(),
            last_update_only_DBG_NETEM: TaiTime::default(),
            debug_counter: 0,
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
                    let normal = Normal::new(center, variance).unwrap_or_else(|_| Normal::new(center, 0.1).unwrap());
                    let sample = normal.sample(&mut rng);
                    sample.max(min_duration.as_secs_f64()).min(max_duration.as_secs_f64())
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
                event_type, event_start, event_duration, mean_value
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
                },
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
                    NetworkPattern::new_bandwidth(
                        z,
                        z,
                        event_start,
                        event_end,
                    )
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
    

    pub fn add_pattern(&mut self, pattern: NetworkPattern) {
        self.patterns.push(pattern);
    }

    pub fn check_active_patterns(&self, current_time: TaiTime<0>) -> (bool, bool) {
        let mut any_active = false;
        let mut just_ended = false;

        for pattern in &self.patterns {
            if let NetworkPattern::Bandwidth {
                valid_from,
                valid_until,
                ..
            } = pattern
            {
                // Check if pattern is active
                if current_time >= *valid_from && current_time <= *valid_until {
                    any_active = true;
                }
                // Check if pattern just ended (within last 100ms)
                let end_window = valid_until
                    .checked_add(Duration::from_millis(100))
                    .unwrap_or(*valid_until);
                if current_time > *valid_until && current_time <= end_window {
                    just_ended = true;
                }
            }
        }
        (any_active, just_ended)
    }

     pub fn refill_all_buckets(&mut self, now: TaiTime<0>) {
        for pattern in &mut self.patterns {
            if let NetworkPattern::Bandwidth { .. } = pattern {
                // Refill without packet accounting
                pattern.bandwidth_account(now, None);
            }
        }
    }


    #[allow(unused_assignments)]
    pub fn should_transmit_with_delay(
        &mut self,
        packet: &mut MpduPacket,
        current_time: TaiTime<0>,
        // bandwidth_limit_bps_parent: f64,
    ) -> Option<Duration> {



        // Update time-based patterns
        let alvr_header = packet.header_alvr.clone();
        if self.last_update_time == TaiTime::default() {
            self.last_update_time = current_time;
        }

        if packet.has_consumed_emu_tokens == true {
            return Some(Duration::ZERO);
        }
        

        let _time_delta = current_time.duration_since(self.last_update_time);
        let _time_delta_dbg = current_time.duration_since(self.last_update_only_DBG_NETEM);

        self.last_update_only_DBG_NETEM = current_time;
        self.last_update_time = current_time;

        let (_has_active, just_ended) = self.check_active_patterns(current_time);
        // / If a pattern just ended, signal to purge the queue
        if just_ended {
            // print_green!(
            //     "{} [PATTERN TRANSITION] Bandwidth pattern just ended, need to purge queue",
            //     format_elapsed!(current_time)
            // );
            // return Some(Duration::from_micros(16)); 
            return None; // Signal to drop the packet, was causing excessive drops
        }

        // Find all active bandwidth patterns at the current time
        let mut active_patterns: Vec<_> = self
            .patterns
            .iter_mut()
            .filter_map(|pattern| {
                if let NetworkPattern::Bandwidth {
                    valid_from,
                    valid_until,
                    ..
                } = pattern
                {
                    if current_time >= *valid_from && current_time <= *valid_until {
                        Some(pattern)
                    } else {
                        None
                    }
                
                } else if let NetworkPattern::ProbabilisticDrop {
                    valid_from,
                    valid_until,
                    drop_probability: _,
                } = pattern
                {
                    if current_time >= *valid_from && current_time <= *valid_until {
                        Some(pattern)
                    } else {
                        None
                    }
                } else if let NetworkPattern::Jitter {
                    mean_delay: _,
                    distribution_type: _,
                    variance: _, // Standard deviation for Gaussian, half-width for Uniform
                    correlation_pct: _, // Correlation with previous delay (0-100%)
                    last_delay: _, // Stores previous delay for correlation
                    valid_from,
                    valid_until,
                } = pattern
                {
                    if current_time >= *valid_from && current_time <= *valid_until {
                        Some(pattern)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        // Warn if multiple active patterns
        if active_patterns.len() > 1 {
            debug_bgprint!(
                DebugColor::Lavender,
                "WARNING: Multiple active bandwidth patterns detected at {:4.9}",
                format_elapsed!(current_time)
            );
        }

        match active_patterns.first_mut() {
            Some(pattern) => match pattern {
                NetworkPattern::ProbabilisticDrop {
                    drop_probability,
                    valid_from,
                    valid_until,
                } => {
                    let mut rng = rand::thread_rng(); // Create a random number generator
                    let rand_value: f64 = rng.gen(); // Generate a random value between 0 and 1

                    if rand_value < *drop_probability {
                        print_red!(
                            "[{} | RANDOM LOSS ( {:.5} -> {:.5} )]  prob= {:.4}! {:?}",
                            format_elapsed!(current_time),
                            format_elapsed!(valid_from),
                            format_elapsed!(valid_until),
                            *drop_probability,
                            packet.print(DebugColor::Red)
                        );
                        return None; // Drop the packet
                    } else {
                        return Some(Duration::ZERO); // transmit inmediatelyOM
                    }
                }

                NetworkPattern::Bandwidth {
                    // current_tokens,
                    // max_tokens,
                    // token_refill_rate,
                    // valid_from,
                    // valid_until,
                    ..
                } => {
                        let pkt_bits = (packet.length_packet_bits * 8) as f64;
                        let (can_send, delay) = pattern.bandwidth_account(current_time, Some(pkt_bits));
                    
                        if can_send {
                            packet.has_consumed_emu_tokens = true;
                            return Some(Duration::ZERO);
                        }
                        else {
                            packet.has_consumed_emu_tokens = true;   // we already debited the bucket
                            return Some(delay);
                        }

                        // return Some(delay);          // queued, tokens NOT deducted yet  
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
                    let mut rng = rand::thread_rng();

                    // Calculate the new random delay
                    let random_component = match distribution_type {
                        JitterDistributionType::Gaussian => {
                            // Using a normal distribution
                            let normal = rand_distr::Normal::new(0.0, *variance).unwrap();
                            rng.sample(normal)
                        }
                        JitterDistributionType::Uniform => {
                            // Using a uniform distribution centered on 0 with width 2*variance
                            rng.gen_range(-*variance..*variance)
                        }
                    };

                    // Apply correlation with previous delay if correlation_pct > 0
                    let correlated_offset = if *correlation_pct > 0.0 {
                        // Calculate deviation from mean of last delay
                        let last_deviation = last_delay.as_secs_f64() - mean_delay.as_secs_f64();

                        // Apply correlation factor
                        let correlation_factor = *correlation_pct / 100.0;
                        last_deviation * correlation_factor
                    } else {
                        0.0
                    };

                    // Combine mean delay, random component, and correlation
                    let new_delay_secs =
                        mean_delay.as_secs_f64() + random_component + correlated_offset;

                    // Ensure delay is not negative
                    let new_delay_secs = new_delay_secs.max(0.0);

                    // Update last_delay for next packet
                    *last_delay = Duration::from_secs_f64(new_delay_secs);
                    self.debug_counter += 1;

                    if self.debug_counter >= 128 {
                        print_red!("{:4.9} [DBG JITTER ({:.5} -> {:.5})] Delay: {:.3} ms | Mean: {:.3} ms, Rand: {:.3} ms, Corr: {:.3} ms | (ALVR F_id: {} - {}/{})",
                            format_elapsed!(current_time),
                            format_elapsed!(valid_from),
                            format_elapsed!(valid_until),
                            new_delay_secs * 1000.0,
                            mean_delay.as_secs_f64() * 1000.0,
                            random_component * 1000.0,
                            correlated_offset * 1000.0,
                            alvr_header.next_packet_index,
                            alvr_header.shard_index,
                            alvr_header.shards_count - 1
                        );
                    }

                    return Some(Duration::from_secs_f64(new_delay_secs));
                }
                _ => return Some(Duration::ZERO),
            },
            None => Some(Duration::ZERO), // No active pattern
        }
    }
}

#[derive(Debug)]
pub struct StatsUpdate {
    pub T_s: f64,
    pub T_q: f64,
    pub blocked_packet_counter: usize,
    pub arrived_packet_counter: usize,
    pub queue_length_when_out: usize,
    pub sta_src_id: usize,
    pub sta_dest_id: usize,
    pub packet_id: i32,
    pub now: tai_time::TaiTime<0>,
    pub length_packet: usize,

    pub ampdu_id: u32, 

    pub is_collision: bool, 
    pub collision_backoff: f64, 
    pub link_id: u8, 

}
#[derive(Clone, Copy, Debug, Hash)]
pub struct EdcaParam {
    pub cw_min: i32,
    pub cw_max: i32,
    pub aifsn: u8,            // slots added to DIFS
    pub txop_limit_us: u16,   // 0 = no TXOP
}
const SIFS_US: u64 = 16;  // 16 µs
const SLOT_TIME_US: u64 = 9;  // 9 µs for OFDM

fn aifs(p: EdcaParam) -> Duration {
    Duration::from_micros(SIFS_US + p.aifsn as u64 * SLOT_TIME_US)
}

pub const EDCA_TABLE: [EdcaParam; 4] = [
    /* VO */ EdcaParam { cw_min:  3,   cw_max:   7,  aifsn: 2,  txop_limit_us:  1504 },
    /* VI */ EdcaParam { cw_min:  7,   cw_max:  15,  aifsn: 2,  txop_limit_us:  3008 },
    /* BE */ EdcaParam { cw_min: 15,  cw_max: 1023,  aifsn: 3,  txop_limit_us:  0    },
    /* BK */ EdcaParam { cw_min: 15,  cw_max: 1023,  aifsn: 7,  txop_limit_us:  0    },
];

#[derive(Hash, Clone, Debug)]
pub struct DcfStats {
    pub cw: i32,
    pub backoff_counter: i32, 
    pub retry_count: u8, 
    pub param: EdcaParam,          // <-- NEW (static per AC)
    pub medium_free_since: TaiTime<0>,   // NEW – 
    pub backoff_frozen: bool,            // NEW
    pub edca_ac_str: String, 
    pub last_backoff_drawn_logs: i32, // Only for logging to CSV the 'drawn' BO value.
    // pub slot_timer_event,
}
 
pub const MAX_RETRIES_MAC: u8 = 10; 
use crate::lib::EdcaAc;

impl DcfStats {
     pub fn new(ac: EdcaAc) -> Self {
        let p = EDCA_TABLE[ac as usize];

        let edca_ac_str = match ac {
            EdcaAc::Voice =>      "AC_VO",
            EdcaAc::Video =>      "AC_VI",
            EdcaAc::Background => "AC_BK", 
            EdcaAc::BestEffort=>  "AC_BE", 
        }; 

        let drawn_randomly_bo = rand::thread_rng().gen_range(0..=p.cw_min); 

        Self {
            cw: p.cw_min,
            backoff_counter: drawn_randomly_bo, 
            retry_count: 0,
            param: p,
            medium_free_since: TaiTime::EPOCH, 
            backoff_frozen: false,
            edca_ac_str: edca_ac_str.to_string(),   
            last_backoff_drawn_logs: drawn_randomly_bo , 
        }
    }

    /// Draw a new random back-off inside the current CW.
    pub fn reload_backoff(&mut self) {

        let randomly_drawn = rand::thread_rng().gen_range(0..=self.cw);

        self.backoff_counter         = randomly_drawn.clone();  //this one 'ticks down' 
        self.last_backoff_drawn_logs = randomly_drawn.clone();  // this one logs the drawn value for each tx. 
    }

    /// Call after a **successful** transmission.
    pub fn on_success(&mut self, cw_min: i32) {
        self.cw          = cw_min;
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
            return false;           // “give up”
        }
        self.cw = ((self.cw + 1) * 2 ) - 1;          // CW = 2·(CW+1) − 1
        
        if self.cw > self.param.cw_max{
            self.cw = self.param.cw_max;
        }
        self.reload_backoff();
        true                                           // keep packet
    }
}



#[derive(Clone, Copy, Debug, Default)]
pub struct Medium {
    busy_until: TaiTime<0>,     // actual airtime occupied
    nav_until: TaiTime<0>,      // virtual carrier sense (from Duration fields)
    tx_owner: Option<MacKey>,     // who currently holds TXOP (only while busy)
    last_txop_owner: Option<MacKey>, // who last held a TXOP (sticky for logging)
    last_txop_end: TaiTime<0>,    // when that TXOP ended}
   }
impl Medium {
    #[inline] pub fn is_idle(&self, now: TaiTime<0>) -> bool {
        now >= self.busy_until && now >= self.nav_until
    }
    #[inline] pub fn start_txop(&mut self, t: TaiTime<0>, owner: MacKey) { 
        self.busy_until = t;
        self.tx_owner = Some(owner)
     }
     /// Busy due to collision/backoff/NAV (no owner)
    #[inline] pub fn occupy_collision(&mut self, until: TaiTime<0>) {
        self.tx_owner = None;
        self.busy_until = until;
    }
    #[inline] pub fn _set_nav_until(&mut self, t: TaiTime<0>) { self.nav_until = t; }

    /// Release an owned TXOP at `now` (called exactly when TX completes)
    #[inline] pub fn release_txop(&mut self, now: TaiTime<0>) {
        if let Some(owner) = self.tx_owner {
            self.last_txop_owner = Some(owner);
            self.last_txop_end = now;
        }
        self.tx_owner = None;
        // keep busy_until as-is; caller may immediately schedule contention next
    }

    /// Clear stale owner if medium is idle (safety net)
    #[inline] pub fn clear_if_idle(&mut self, now: TaiTime<0>) {
        if self.is_idle(now) { self.tx_owner = None; }
    }

    #[inline] pub fn _current_owner(&self) -> Option<MacKey> { self.tx_owner }
    #[inline] pub fn _last_owner(&self) -> Option<MacKey> { self.last_txop_owner }
    #[inline] pub fn _last_end(&self) -> TaiTime<0> { self.last_txop_end }
}

#[derive(Clone, Debug)]
pub struct StaCapabilities {
    pub _is_str_capable: bool,
    pub links: Vec<u8>,
}
#[derive(Clone, Debug)]
pub struct LinkConfig {
    pub link_id: u8,
    pub _frequency_ghz: f64,  // 5 or 6
    pub bandwidth_mhz: u16,   // 80, 160, 320
}

// Helper function to initialize typical MLO setup
pub fn create_mlo_config(config: &str) -> Vec<LinkConfig> {

    let a = match config{
        "MLO0" => {             // SLO 
            vec![
                LinkConfig {
                    link_id: 0,
                    _frequency_ghz: 5.0,
                    bandwidth_mhz: 80,
                },
            ]
        }
        "MLO1" => {           // MLO 80_80 MHz channels 
             vec![
            LinkConfig {
                link_id: 0,
                _frequency_ghz: 5.0,
                bandwidth_mhz: 80,
            },
            LinkConfig {
                link_id: 1,
                _frequency_ghz: 6.0,
                bandwidth_mhz: 80,
            },
            ]
        },
        "MLO2" => {           // MLO 80_160 MHz channels 
             vec![
            LinkConfig {
                link_id: 0,
                _frequency_ghz: 5.0,
                bandwidth_mhz: 80,
            },
            LinkConfig {
                link_id: 1,
                _frequency_ghz: 6.0,
                bandwidth_mhz: 160,
            },
            ]
        },
        "MLO3" => {           // MLO 80_320 MHz channels 
             vec![
            LinkConfig {
                link_id: 0,
                _frequency_ghz: 5.0,
                bandwidth_mhz: 80,
            },
            LinkConfig {
                link_id: 1,
                _frequency_ghz: 6.0,
                bandwidth_mhz: 320,
            },
            ]
        },

        _ => {

            print_red!("WARNING WRONG MLO STRING ({config}) || DEFAULTING TO SLO!!",  ); 
            vec![
                LinkConfig {
                    link_id: 0,
                    _frequency_ghz: 5.0,
                    bandwidth_mhz: 80,
                },
            ]

        }
    }; 
    println!("Creating MLO Config!\n{:#?}", a); 
    a
}



#[allow(unused)]
#[derive(Clone)]
pub struct QueueModule {
    // pub output_port_sta1: Output<AmpduPacket>,

    pub link_outputs: HashMap<u8, Output<AmpduPacket>>, // AP's Tx ports (key=link_id)
    // pub queue: VecDeque<MpduPacket>,
    pub queue: Vec<MpduPacket>, 

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
    pub t0_time: Instant,

    pub csv_metrics: CsvType,

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
    pub link_channel_widths: HashMap<u8, usize>,  // Store channel width per link
    pub sta_capabilities: HashMap<i32, StaCapabilities>, // (key=sta_id)
    pub link_queue_depths: HashMap<u8, usize>, // Required for optimization to stop iterating O(n) over queue
    pub array_dcf_values: Arc<Mutex<HashMap<MacKey, DcfStats>>>,

    pub mlo_linkselection_strat: LinkSelectionStrategy, 

}
#[allow(unused)]
impl QueueModule {
     pub fn new(
        num_stas: usize,
        queue_size: usize,
        PL_prob: f64,
        vec_ids: Vec<i32>,
        folder_dir: String,
        emulated_tests: Option<(bool, bool, bool, bool)>,
        link_configs: Vec<LinkConfig>, // NEW: Configure available links
        mlo_linkselection_strat: LinkSelectionStrategy, 
    ) -> Self {
        let mut stats_vec: HashMap<usize, perStaLockStats> = HashMap::new();
        let mut dcf_stats_vec = HashMap::new();
        let (stats_tx, stats_rx) = unbounded();

        // Initialize per-STA stats
        for i in 0..num_stas {
            let sta_stats = perStaLockStats::new();
            if let Ok(mut stats) = sta_stats.data.clone().lock() {
                stats.sta_id = vec_ids[i] as i32;
                stats_vec.insert(stats.sta_id.clone() as usize, sta_stats.clone());
            }
        }

        // Create BEB queues per link and AC
        for link_id in link_configs.iter().map(|lc| lc.link_id) {
            // AP gets four virtual MACs per link (one per AC)
            for ac in [EdcaAc::Voice, EdcaAc::Video, EdcaAc::BestEffort, EdcaAc::Background] {
                dcf_stats_vec.insert((-1, ac, link_id), DcfStats::new(ac));
            }

            // Every uplink STA keeps one MAC per link per AC
            for sta_id in &vec_ids {
                for ac in [EdcaAc::Voice, EdcaAc::Video, EdcaAc::BestEffort, EdcaAc::Background] {
                    dcf_stats_vec.insert((*sta_id, ac, link_id), DcfStats::new(ac));
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
            link_mediums.insert(link_config.link_id, Medium::default());
            link_channel_widths.insert(link_config.link_id, link_config.bandwidth_mhz as usize); 
            link_outputs.insert(link_config.link_id, Output::default()); 
            being_served.insert(link_config.link_id, false); 
        }

        Self {
            // queue: VecDeque::with_capacity(queue_size),
            queue: Vec::with_capacity(queue_size),
            // queue_maxsize_k:        queue_size, 
            queue_maxsize_dl:         DOWNLINK_QUEUE_SIZE,
            ul_capacity_queue_device: UPLINK_QUEUE_SIZE,
            link_outputs,
            service_timer: Duration::ZERO,
            aux_ampdu_serviced: AmpduPacket::new(),
            link_is_transmitting: being_served,
            blocked_packet_counter: 0,
            arrived_packet_counter: 0,
            queue_length_counter: 0,
            service_rate: 0.0,
            t0_time: Instant::now(),
            csv_metrics: CsvType::new(&folder_dir).expect("?? CSVTYPE"),
            coords_queue: Coords::with_coords(AP_X, AP_Y, 0.0),  // To test.
            p_tx: P_TX,
            STA_coords_grid: Vec::new(),
            STA_coords_map: HashMap::new(),
            cumulative_stats_queue: Arc::new(Mutex::new(QueueStats::new())),
            array_stas_stats: Arc::new(Mutex::new(stats_vec)),
            array_dcf_values: Arc::new(Mutex::new(dcf_stats_vec)),
            sta_stats_cache: HashMap::new(),
            stats_tx: Some(stats_tx),
            stats_rx: Some(stats_rx),
            PL_probability: PL_prob,
            ampdu_id: 0,
            link_mediums,
            link_channel_widths, 
            link_queue_depths, 
            sta_capabilities: HashMap::new(),
            mlo_linkselection_strat, 
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
    pub fn tick_backoff(&mut self, now: TaiTime<0>) -> HashMap<u8, Vec<MacKey>> {
        // Clear idle links
        for medium in self.link_mediums.values_mut() {
            medium.clear_if_idle(now);
        }

        // Precompute which MacKeys have packets (per link)
        let mut present_keys: HashSet<(i32, EdcaAc)> = HashSet::new();
        for p in self.queue.iter() {
            let is_ul = p.sta_src_id > p.sta_dest_id;
            let key = if is_ul { (p.sta_src_id, p.edca_ac) } else { (-1, p.edca_ac) };
            present_keys.insert(key);
        }


        let mut ready_per_link: HashMap<u8, Vec<MacKey>> = HashMap::new();
        
        // Lock ONCE and do all updates in a single pass
        let mut map = self.array_dcf_values.lock().unwrap();
        for (key, st) in map.iter_mut() {
            let (sta_id, ac, link_id) = *key;
            
            // debug_edca!(
            //     "{} [TICK] Checking KEY ({}, {:?}, L-{}): state=({} slots, frozen={})",
            //     format_elapsed!(now),
            //     sta_id,
            //     ac,
            //     link_id,
            //     st.backoff_counter,
            //     st.backoff_frozen
            // );


            // Skip if no packets for this STA/AC
            if !present_keys.contains(&(sta_id, ac)) {
    
                continue;
            }

            // Get medium state for this link
            let medium = self.link_mediums.get(&link_id).unwrap();
            let idle_slot = medium.is_idle(now);
            if !idle_slot {
                debug_edca!(
                    "{} [MEDIUM] L-{} BUSY until {}",
                    format_elapsed!(now),
                    link_id,
                    format_elapsed!(medium.busy_until)  // You'll need to expose this
                );
            }
            // AIFS gating
            let aifs_until = st.medium_free_since + aifs(st.param);
            let aifs_satisfied = idle_slot && aifs_until <= now;
            
            if st.backoff_frozen && aifs_satisfied {
                let aifs_duration_us = aifs(st.param).as_micros();
                debug_edca!(
                    "{} \t[EDCA] L-{} ({}, {:?}): AIFS satisfied ({}µs). UNFREEZING.",
                    format_elapsed!(now), link_id, sta_id, ac, aifs_duration_us
                );
            }

            // Update backoff state directly (st is already mutable)
            if aifs_satisfied {
                st.backoff_frozen = false;
            }

            // Backoff countdown
            if idle_slot && !st.backoff_frozen && st.backoff_counter > 0 {
                debug_edca!(
                   "{} [EDCA] L-{} ({}, {:?}): COUNTDOWN {} -> {}",
                   format_elapsed!(now), link_id, sta_id, ac,
                   st.backoff_counter, st.backoff_counter - 1 
               );
                st.backoff_counter -= 1;
            }

            // Check if ready
            if st.backoff_counter == 0 && !st.backoff_frozen {
                ready_per_link.entry(link_id).or_insert_with(Vec::new).push(*key);
            }
            }
        if !ready_per_link.is_empty() {
            // Build a short summary string
            let mut summary = String::new();
            for (link_id, contenders) in &ready_per_link {
                // You can customize the detail level here.
                // For just counts:
                summary.push_str(&format!("L-{}: {} | ", link_id, contenders.len()));
                
                // For full key details (might be verbose again, but informative):
                // summary.push_str(&format!("L-{}: {:?} | ", link_id, contenders));
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
        let p = self.array_dcf_values.lock().unwrap()[key].param;
        if p.txop_limit_us == 0 { f64::INFINITY } else { p.txop_limit_us as f64 * 1e-6 }
    }
    #[inline]
    fn ac_prio(&mut self, ac: EdcaAc) -> u8 { match ac {
        EdcaAc::Voice => 0, EdcaAc::Video => 1, EdcaAc::BestEffort => 2, EdcaAc::Background => 3
    }}
    #[inline]
    fn resolve_virtual_collision(&mut self, mut ready: Vec<MacKey>) -> Vec<MacKey> {  // Collisions when same STA has several ACs winning backoff  

        let mut winner = HashMap::<i32, MacKey>::new();  // sta_id → winning AC

        ready.sort_by_key(|k| self.ac_prio(k.1));
        for key in ready {                               // iterate lowest to highest value
            let sta = key.0;                             // -1 for AP
            if !winner.contains_key(&sta) {
                winner.insert(sta, key);                 // first (=highest-prio) wins
            } else {
                // “virtual collision” for this lower-prio AC
                if let Some(st) = self.array_dcf_values.lock().unwrap().get_mut(&key) {
                    st.on_failure();
                }
            }
        }
        winner.into_values().collect()
    }

    /// ## A snippet from previous code that optimized a similar problem: 
    ///  let mut selected_sta = None;
            // if LYAPUNOV_POLICY == true {
            //     let mut min_priority = f64::MAX;
            //     debug_schedule!(
            //         "T {:.5} LYAPUNOV Drift-plus-Penalty scheduling policy:",
            //         format_elapsed!(now)
            //     );
            //     for (key, info) in sta_packets.iter() {
            //         let lhs = LYAPUNOV_V * info.per_packet_channel_access_efficiency;
            //         let rhs = info.expected_queue_delivery_ms;
            //         let priority: f64 = if info.packet_count >= MAX_AMPDU_SIZE as usize {
            //             lhs - rhs
            //         } else {
            //             1E12 as f64
            //         };
            //         let is_ul = if key.0 > key.1 {1} else {0};
            //         debug_schedule!(
            //             "Q_{:.0} = {} -> Priority STA{:.0} = ({:.3}) == {} - {} | is_ul = {} (src: {} dest: {}) ",
            //             key.0,
            //             info.packet_count,
            //             key.0,
            //             priority,
            //             lhs,
            //             rhs,
            //             is_ul,
            //             key.0,
            //             key.1,
            //         );
            //         if priority < min_priority {
            //             min_priority = priority;
            //             selected_sta = Some(*key);
            //         }
            //     }
            // } else if SOFTMAX_POLICY == true {
            //     debug_schedule!("SOFTMAX POLICY", );
            //     let mut softmax_values: Vec<f64> = Vec::new();
            //     let mut softmax_keys: Vec<(i32, i32)> = Vec::new();
            //     for ((sta_src, sta_dest), packets) in sta_packets.iter() {
            //         let _is_ul = if sta_src > sta_dest {1} else {0};
            //         debug_schedule!(
            //             "IS_UL = {} | ( src: {}, dest: {} )",
            //             _is_ul,
            //             sta_src,
            //             sta_dest
            //         );
            //         softmax_values.push(packets.expected_queue_delivery_ms);
            //         softmax_keys.push((*sta_src, *sta_dest));
            //     }
            //     pub const SOFTMAX_TEMP: f64 = 1000.0;
            //     let softmax_probs = softmax_with_temperature(&softmax_values, SOFTMAX_TEMP);
            //     let mut rng = rand::thread_rng();
            //     let selected_index = softmax_probs
            //         .iter()
            //         .position(|&p| (1.0 - p) > rng.gen::<f64>())
            //         .unwrap_or(softmax_probs.len() - 1);
            //     let selected_key = softmax_keys[selected_index];
            //     selected_sta = Some(selected_key);
            // } else {
            //     // NORMAL POLICY: FIFO
            // }
    

    #[inline]
    fn select_link_for_packet(&mut self, pkt: &MpduPacket, now: TaiTime<0>) -> Option<u8> {     /// Select the best link for a given packet based on strategy and STA capabilities

        let sta_id = if pkt.sta_src_id > pkt.sta_dest_id {
            pkt.sta_src_id  // Uplink
        } else {
            pkt.sta_dest_id  // Downlink
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
                self.select_primary_first(&cap.links, now)
            }
            LinkSelectionStrategy::Opportunistic => {
                self.select_opportunistic(&cap.links, now)
            }
            LinkSelectionStrategy::LyapunovBackpressure => {
                self.select_lyapunov_backpressure(&cap.links, now)
            }
            _ => {
                println!("Mode {} UNIMPLEMENTED --> DEFAULT TO OPPORTUNISTIC", self.mlo_linkselection_strat.to_string()); 
                self.select_opportunistic(&cap.links, now)
            }
        };
        
        if let Some(link_id) = selected {
            log_link_selection!(
                now,
                "✓ Selected LINK-{} for STA {} (strat: {:?})",
                link_id,
                sta_id,
                self.mlo_linkselection_strat,
            );
        // pkt.print(DebugColor::Blue); 
        }
        selected
    }


    fn get_link_queue_depth(&self, link_id: u8) -> usize {
    // Now an O(1) lookup
        self.link_queue_depths.get(&link_id).copied().unwrap_or(0)
    }

    #[inline]
    fn select_lyapunov_backpressure(&self, available_links: &[u8], now: TaiTime<0>) -> Option<u8> {
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
    fn select_opportunistic(&self, available_links: &[u8], now: TaiTime<0>) -> Option<u8> {
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
            let a_is_idle = self.link_mediums.get(&link_a_id)
                .map_or(false, |m| m.is_idle(now));
            
            let b_is_idle = self.link_mediums.get(&link_b_id)
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
                },
                (true, false) => {
                    // Only A is idle: Choose A.
                    // log_link_selection!(
                    //     now,
                    //     "Opportunistic: Link {} busy, selecting idle LINK-{}",
                    //     link_b_id,
                    //     link_a_id
                    // );
                    Some(link_a_id)
                },
                (false, true) => {
                    // Only B is idle: Choose B.
                    // log_link_selection!(
                    //     now,
                    //     "Opportunistic: Link {} busy, selecting idle LINK-{}",
                    //     link_a_id,
                    //     link_b_id
                    // );
                    Some(link_b_id)
                },
                (false, false) => {
                    // Both are busy: Choice doesn't matter. Fall back to A.
                    // log_link_selection!(
                    //     now,
                    //     "Opportunistic: Both links busy, defaulting to LINK-{}",
                    //     link_a_id
                    // );
                    Some(link_a_id)
                }
            }
        }
    fn select_primary_first(&self, available_links: &[u8], now: TaiTime<0>) -> Option<u8> {
        let primary_link = available_links[0];
        
        // Check if primary is idle
        if let Some(medium) = self.link_mediums.get(&primary_link) {
            if medium.is_idle(now) {
                return Some(primary_link);
            }
        }
        
        // Primary busy - check queue depth
        let primary_depth = self.get_link_queue_depth(primary_link);
        
        if primary_depth < 10 {  // Threshold for "acceptable" load
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
        
        Some(primary_link)  // Fall back to primary
    }
    
    #[inline]
    pub async fn cache_input_packet(&mut self, packet: MpduPacket, link_id: u8, ) {
        let key = (packet.sta_src_id, packet.sta_dest_id);

        // Do all immutable reading from `self` *before* the mutable borrow.
        let is_ul = packet.sta_src_id > packet.sta_dest_id;
        let mac_key_edca = if is_ul {
            (packet.sta_src_id, packet.edca_ac, link_id)
        } else {
            (-1, packet.edca_ac, link_id)
        };

        let channel_width = self.link_channel_widths.get(&link_id).copied().unwrap(); 

        // All immutable borrows happen here and end immediately
        let cap_s_edca = self.txop_cap_secs(&mac_key_edca);
        let coords_queue = self.coords_queue; // Assuming Coords is Copy
        let p_tx = self.p_tx;                 // f64 is Copy

        let entry = self.sta_stats_cache.entry(key).or_insert_with(|| {

            // Calculate transmission delay for a single packet
            let resultz = airtime_ampdu(
                packet.length_packet_bits as f64,
                1,
                coords_queue, 
                packet.sta_src_coords,
                p_tx,         
                channel_width,
            );

            // Binary search
            let mut low = 1;
            let mut high = MAX_AMPDU_SIZE;
            let mut optimal_n_packets = 0;
            let mut resultz_full_ampdu = airtime_ampdu(
                packet.length_packet_bits as f64 * high as f64,
                high,
                coords_queue,
                packet.sta_src_coords,
                p_tx,         
                channel_width,
            );

            while low <= high {
                let mid = (low + high) / 2;
                let test_resultz = airtime_ampdu(
                    packet.length_packet_bits as f64 * mid as f64,
                    mid,
                    coords_queue, 
                    packet.sta_src_coords,
                    p_tx,         
                    channel_width,

                );

                // Use the pre-calculated cap_s_edca variable
                if test_resultz <= DEFAULT_TMAX_AGG || test_resultz <= cap_s_edca {
                    optimal_n_packets = mid;
                    resultz_full_ampdu = test_resultz;
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
            // Note: packet_count starts at 0, will be incremented below
            StaRateInfo {
                total_transmission_delay_single: resultz,
                total_transmission_delay_fullampdu: resultz_full_ampdu,
                fullampdu_max_size: optimal_n_packets as usize,
                packet_count: 0,
                weighted_rate_single: resultz, // First value for EWMA
                weighted_rate_fullampdu: resultz_full_ampdu, // First value for EWMA
                // Avoid division by zero if optimal_n_packets is 0
                per_packet_channel_access_efficiency: resultz_full_ampdu / (optimal_n_packets.max(1) as f64),
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
        
        
        
        if !is_ul{ // Extra guard to prevent wrong routing. 
            match selected_link {
                Some(link_id) => {
                    log_mlo!(
                        now,
                        "📥 DL Packet {} (STA {} → {}) assigned to LINK-{}: Q_size={}",
                        pkt.packet_id,
                        pkt.sta_src_id,
                        pkt.sta_dest_id,
                        link_id, 
                        self.queue.len(), 
                    );
                    
                    // Cache the selected link in packet metadata
                    pkt.assign_link(link_id);
                    
                    // Create MacKey with link_id
                    pkt.mac_key_cached = Some(if is_ul {
                        (pkt.sta_src_id, pkt.edca_ac, link_id)
                    } else {
                        (-1, pkt.edca_ac, link_id)
                    });
                    
                    if self.queue.len() < self.queue_maxsize_dl {
                        self.cache_input_packet(pkt.clone(), link_id).await;
                        self.queue.push(pkt);

                        self.link_queue_depths.entry(link_id).and_modify(|c| *c += 1); // add to lookup hashmap, per link 
                        
                        // Trigger scheduling if medium is idle
                        
                        if self.queue.len() == 1 && *self.link_is_transmitting.get(&link_id).unwrap() == false {
                            // We schedule it one slot time in the future.
                            // This prevents the immediate call bug and starts the
                            // 9µs timer loop correctly.
                            ctx.scheduler
                                .schedule_event(
                                    Duration::from_secs_f64(SLOT), // SLOT = 9e-6
                                    Self::deque_schedule_service, 
                                    ()
                                )
                                .unwrap();
                        }
                        
                        // if !self.packet_being_served {
                        //     if let Some(medium) = self.link_mediums.get(&link_id) {
                        //         if medium.is_idle(now) {
                        //             self.deque_schedule_service((), ctx).await;
                        //         }
                        //     }
                        // }
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
          }
        else{ // guard against BG DL+UL traffic being sent to both inputs. 

            // println!("[Input DL Discard] NOT DL (SRC: {} > DST: {} ) ", pkt.sta_src_id , pkt.sta_dest_id, ); 
        }

    }

    /// Uplink input function (from STAs)
    #[inline]
    pub async fn input_UL(&mut self, mut packet: MpduPacket, context: &Context<Self>) {
        self.arrived_packet_counter += 1;
        self.queue_length_counter += self.queue.len();

        let now = context.scheduler.time();
        let is_ul = packet.sta_src_id > packet.sta_dest_id;
        
        // Select link for uplink packet
        let selected_link = self.select_link_for_packet(&packet, now);    

        if is_ul{
            match selected_link {
                Some(link_id) => {
                    packet.assign_link(link_id);
                    packet.mac_key_cached = Some(if is_ul {
                        (packet.sta_src_id, packet.edca_ac, link_id)
                    } else {
                        (-1, packet.edca_ac, link_id)
                    });
                    
                    packet.queue_in_instant = now; // UL packets always go in queue, later they're dropped if they exceed max of STA/EDCA_AC virtual queue. 
                    self.cache_input_packet(packet.clone(), link_id).await;
                    self.queue.push(packet.clone());
                    self.link_queue_depths.entry(link_id).and_modify(|c| *c += 1);

                    log_mlo!(
                        now,
                        "📥 UL Packet {} from STA{} → STA{} on LINK-{}, Q_size = {}",
                        packet.packet_id,
                        packet.sta_src_id,
                        packet.sta_dest_id,
                        link_id,
                        self.queue.len()
                    );

                    if self.queue.len() == 1 && *self.link_is_transmitting.get(&link_id).unwrap() == false {                        
                        context.scheduler
                            .schedule_event(
                                Duration::from_secs_f64(SLOT), // SLOT = 9e-6
                                Self::deque_schedule_service, 
                                ()
                            )
                            .unwrap();
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
            
        }
        else{ // guard against BG DL+UL traffic being sent to both inputs. 
            // println!("[Input UL Discard] NOT UL (SRC: {} !> DST: {} ) ", packet.sta_src_id , packet.sta_dest_id, ); 
        }
    
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
            let dest_coords = self.STA_coords_map
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
        );

        if airtime_secs == 0.0 {
            return (0.0, 0.0); // Avoid division by zero
        }
        
        let utility = n_mpdus as f64 / airtime_secs;
        (utility, airtime_secs)
    }


    #[inline]
    fn select_next_sta(&self) -> &HashMap<(i32, i32), StaRateInfo> {    // The entire slow loop is gone, O(1) lookup. 

        &self.sta_stats_cache
    }
    
    #[inline]
    pub async fn send_ampdu(&mut self, AMPDU_sent: AmpduPacket, context: &Context<Self>) {
        let elapsed = context.scheduler.time();
        // self.shared_medium.release_txop(elapsed);
        let link_id = AMPDU_sent.link_id;
  
        if let Some(medium) = self.link_mediums.get_mut(&link_id) {
            medium.release_txop(elapsed);
        }
        // crate::print_brown!("{} | [TXOP END] last_owner={:?}", format_elapsed!(elapsed), self.shared_medium.last_owner());
        debug_debug!(
            DebugColor::Red,
            "{} [DBG TXOP]    --AMPDU sent to STA {} with {} packets inside, Q_size = {}, L = {}, AMPDU_size: {}",
            format_elapsed!(elapsed),
            AMPDU_sent.sta_dest_id,
            AMPDU_sent.mpdu_packets.len(),
            self.queue.len(),
            AMPDU_sent.total_length,
            AMPDU_sent.size - 1
        );
        // AMPDU_sent.print();
        self.ampdu_id += 1; // increment the AMPDU counter for logging. 

        self.link_is_transmitting.insert(link_id, false);

        let mac_key = AMPDU_sent.mac_key;   
        
        // Send to appropriate output port
        if let Some(output) = self.link_outputs.get_mut(&link_id) {
            output.send(AMPDU_sent).await;
        }

        if self.queue.len() > 0 {
            self.deque_schedule_service((), context).await;
            // context.scheduler.schedule_event(Duration::from_nanos(10), Self::deque_schedule_service, ()).unwrap();
        }

 

        if let Some(st) = self.array_dcf_values.lock().unwrap().get_mut(&mac_key) { 
            
            let prev_retries = st.retry_count; 
            let prev_cw_val = st.cw;
            let prev_drawn_bo=st.last_backoff_drawn_logs; 

            
            st.on_success(st.param.cw_min); 



            let mut drained = smallvec::SmallVec::<[StatsUpdate; 64]>::new();
            if let Some(rx) = self.stats_rx.as_mut() {
                while let Ok(up) = rx.try_recv() { drained.push(up); }
            }
            if !drained.is_empty() {
                if let Ok(mut qstats) = self.cumulative_stats_queue.lock() {
                    for u in &drained {
                        qstats.update_cumstats(u.T_s, u.T_q, u.blocked_packet_counter, u.arrived_packet_counter, u.queue_length_when_out);
                    }
                }

                if CSV_PER_PACKET{
                    for u in drained { // CSV write outside the lock
                        self.csv_metrics.update_stats(
                            u.now, u.packet_id as usize, u.queue_length_when_out, u.T_s, u.T_q,
                            u.length_packet, u.sta_src_id, u.sta_dest_id, u.ampdu_id, u.is_collision,
                            u.collision_backoff, u.link_id as usize, prev_cw_val as usize, prev_retries, prev_drawn_bo, st.edca_ac_str.clone(), 
                        );
                    }
                }
            }
        }
        else{
            print_red!("NO MAC_KEY VALUE in array DCF vals??? {:?}", mac_key); 
        }
    }
    #[inline]
    fn build_new_ampdu<'a>(
        &mut self,
        first_packet: &MpduPacket,
        now: TaiTime<0>,
    ) -> (AmpduPacket, Duration) {
        let mut success_indices: Vec<usize> = Vec::new();
        let (sta_src_id, sta_dest_id) = (first_packet.sta_src_id, first_packet.sta_dest_id);
        // Get the link_id from the first packet
        let link_id = first_packet.assigned_link_id
            .expect("Packet must have assigned_link_id before building AMPDU");

        let channel_width = self.link_channel_widths.get(&link_id).copied().unwrap(); 
        
        self.aux_ampdu_serviced.reset();
        self.aux_ampdu_serviced.sta_dest_id = first_packet.sta_dest_id;
        self.aux_ampdu_serviced.sta_src_id = first_packet.sta_src_id;
        self.aux_ampdu_serviced.coordinates = first_packet.sta_src_coords.clone();
        self.aux_ampdu_serviced.link_id = link_id;  // Tag AMPDU with link

        let is_ul = first_packet.sta_src_id > first_packet.sta_dest_id;

        // println!("IS_UL = {} ( {} > {})", is_ul, first_packet.sta_src_id, first_packet.sta_dest_id); 
        
        let mac_key: MacKey = if is_ul {
            (first_packet.sta_src_id, first_packet.edca_ac, link_id)
        } else {
            (-1, first_packet.edca_ac, link_id)
        };

        self.aux_ampdu_serviced.mac_key = mac_key;

        let txop_us: f64 = self.array_dcf_values
            .lock()
            .unwrap()
            .get(&mac_key)
            .map(|st| st.param.txop_limit_us as f64)
            .unwrap_or(0.0);

        let mut last_service_duration = Duration::default();
        let mut packet_index = 0;
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

        // ========== Aggregate packets for this flow on this link ==========
        while packet_index < self.queue.len() {
            if let Some(current_packet) = self.queue.get(packet_index) {
                //  Only aggregate packets that:
                // 1. Match the same flow (src/dest)
                // 2. Are assigned to the SAME link
                if current_packet.sta_dest_id != self.aux_ampdu_serviced.sta_dest_id
                    || current_packet.sta_src_id != self.aux_ampdu_serviced.sta_src_id
                    || current_packet.assigned_link_id != Some(link_id)  
                {
                    packet_index += 1;
                    continue;
                }

                let new_total_length =
                    self.aux_ampdu_serviced.total_length + current_packet.length_packet_bits;
                let new_size = self.aux_ampdu_serviced.size + 1;

                // Calculate airtime for the new AMPDU size
                // NOTE: we assume links are simmetrical, src and dest are mixed up for DL/UL, 
                //       but we compute distance correctly and rest of parameters follow. 

                if is_ul {

                    resultz = airtime_ampdu(
                        new_total_length as f64,
                        new_size,
                        current_packet.sta_src_coords.clone(),
                        self.coords_queue,
                        P_TX,
                        channel_width, 
                    );
                    // println!("Computing UL AMPDU airtime: src: {:?}, dest: {:?}", current_packet.sta_src_coords.clone(), self.coords_queue, ); 

                } else {
                    let dest_coords = self  
                        .STA_coords_map
                        .get(&(current_packet.sta_dest_id as usize))
                        .unwrap_or_else(|| {
                            panic!("no coordinates for STA {}", current_packet.sta_dest_id)
                        })
                        .clone();
                    
                        
                    // println!("Computing DL AMPDU airtime: src: {:?}, dest: {:?}", self.coords_queue, dest_coords); 
                    resultz = airtime_ampdu(
                        new_total_length as f64,
                        new_size,
                        self.coords_queue,
                        dest_coords,
                        P_TX,
                        channel_width, 
                    );
                }
                let cap_s_edca = self.txop_cap_secs(&mac_key);

                // Check if adding this packet would exceed limits
                if resultz >= DEFAULT_TMAX_AGG || new_size > MAX_AMPDU_SIZE || resultz >= cap_s_edca {
                    log_mlo!(
                        now,
                        "  AMPDU limit reached: size={}/{}, airtime={:.3}ms/{:.3}ms",
                        new_size,
                        MAX_AMPDU_SIZE,
                        resultz * 1000.0,
                        f64::min(DEFAULT_TMAX_AGG * 1000.0, cap_s_edca * 1000.0)
                    );
                    break;
                }

                // Clone the packet and add to AMPDU
                let mut cloned_packet = current_packet.clone();
                cloned_packet.original_index = packet_index;
                cloned_packet.queue_length_when_out = self.queue.len();
                cloned_packet.queue_out_instant = now;
                cloned_packet.T_q = now.duration_since(cloned_packet.queue_in_instant);

                // Update stats before moving packet
                if let Some(stats_tx) = &self.stats_tx {
                    let stats_update = StatsUpdate {
                        T_s: resultz,
                        T_q: now
                            .duration_since(cloned_packet.queue_in_instant)
                            .as_secs_f64(),
                        blocked_packet_counter: self.blocked_packet_counter,
                        arrived_packet_counter: self.arrived_packet_counter,
                        queue_length_when_out: cloned_packet.queue_length_when_out,
                        sta_src_id: cloned_packet.sta_src_id as usize,
                        sta_dest_id: cloned_packet.sta_dest_id as usize,
                        packet_id: cloned_packet.packet_id as i32,
                        now,
                        length_packet: cloned_packet.length_packet_bits,
                        ampdu_id: self.ampdu_id,
                        is_collision: false,
                        collision_backoff: 0.0,
                        link_id: link_id, 
                    };

                    stats_tx
                        .send(stats_update)
                        .expect("Failed to send stats update");
                }

                // Add the cloned packet to the AMPDU
                self.aux_ampdu_serviced.mpdu_packets.push(cloned_packet);
                self.aux_ampdu_serviced.total_length = new_total_length;
                self.aux_ampdu_serviced.size = new_size;
                last_service_duration = Duration::from_secs_f64(resultz);
            }
            packet_index += 1;
        }

        log_mlo!(
            now,
            "  Aggregated {} packets, total_length={}, airtime={:.3}ms",
            self.aux_ampdu_serviced.mpdu_packets.len(),
            self.aux_ampdu_serviced.total_length,
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
                log_mlo!(
                    now,
                    "  ❌ Packet {} failed (PL={:.3}), will be retransmitted",
                    packet.packet_id,
                    self.PL_probability
                );
                self.blocked_packet_counter += 1;
            } else {
                // Successful transmission
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
        for packet in &self.aux_ampdu_serviced.mpdu_packets {
            success_indices.push(packet.original_index);
        }

        // Track removals per flow for cache update
        let mut packets_removed_by_key: HashMap<(i32, i32), usize> = HashMap::new();

        // Remove in descending order to avoid index shifts
        success_indices.sort_unstable_by(|a, b| b.cmp(a));

        let success_indices_set: std::collections::HashSet<usize> = success_indices.into_iter().collect();

        // 2. Use retain to remove all successful packets in a single O(N) pass.
        // We must also count removals and update link depths *during* this pass.
        let mut i = 0;
        self.queue.retain(|packet| {
            let current_idx = i;
            i += 1; // Increment index for the next packet

            if success_indices_set.contains(&current_idx) {
                // This packet was successful, so remove it (return false)
                
                // --- Update Tally/Cache Logic ---
                let key = (packet.sta_src_id, packet.sta_dest_id);
                *packets_removed_by_key.entry(key).or_insert(0) += 1;

                // --- Decrement Per-Link Queue Depth ---
                if let Some(link_id) = packet.assigned_link_id {
                    // Check if link_id is in the map before modifying
                    if let Some(count) = self.link_queue_depths.get_mut(&link_id) {
                        *count = count.saturating_sub(1);
                    }
                }   
                return false; // Remove from queue
            }
            true // Keep in queue
        });

        // ========== Update cache for affected flows ==========
        for (key, count_removed) in packets_removed_by_key {
            if let Some(entry) = self.sta_stats_cache.get_mut(&key) {
                entry.packet_count = entry.packet_count.saturating_sub(count_removed);
                entry.expected_queue_delivery_ms =
                    entry.per_packet_channel_access_efficiency * entry.packet_count as f64 * 1000.0;

                if count_removed != 0{
                    log_mlo!(
                    now,
                    "  Updated cache for flow ({}, {}): {} packets remaining",
                    key.0,
                    key.1,
                    entry.packet_count
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
        let ampdu_to_send =
            std::mem::replace(&mut self.aux_ampdu_serviced, AmpduPacket::new());

        log_mlo!(
            now,
            "✅ AMPDU built on LINK-{}: {} packets, {:.3}ms airtime | Txend = {:.8}",
            link_id,
            ampdu_to_send.mpdu_packets.len(),
            last_service_duration.as_secs_f64() * 1000.0, 
            taitime_to_f64!(now + last_service_duration ),  

        );

        (ampdu_to_send, last_service_duration)
    }

    #[inline]
    fn deque_schedule_service<'a>(
    &'a mut self,
    _: (),
    context: &'a Context<Self>,
) -> impl Future<Output = ()> + Send + 'a {
    async move {
        let now = context.scheduler.time();

        // UL capacity check (same as before)
        let sta_packets: HashMap<(i32, i32), StaRateInfo> = self.select_next_sta().clone();
      // 1. Find all overflowing UL flows and how many packets to drop
        let mut overflowing_flows = HashMap::new(); // Key: (src, dest), Val: excess_count
        for ((sta_src, sta_dest), packets) in sta_packets.iter() {
            let is_ul = sta_src > sta_dest;
            if is_ul && packets.packet_count > self.ul_capacity_queue_device {
                let excess_count: usize = packets.packet_count - self.ul_capacity_queue_device;
                overflowing_flows.insert((*sta_src, *sta_dest), excess_count);
                
                print_red!(
                    "{} [UL CAPACITY EXCEEDED] STA {} -> AP {}: {} packets (max: {}), dropping {}",
                    format_elapsed!(now),
                    sta_src,
                    sta_dest,
                    packets.packet_count,
                    self.ul_capacity_queue_device,
                    excess_count
                );
            }
            if !is_ul && packets.packet_count > self.queue_maxsize_dl {

                let excess_count_dl: usize = packets.packet_count - self.queue_maxsize_dl;
                overflowing_flows.insert((*sta_src, *sta_dest), excess_count_dl);
                print_red!(
                    "{} [DL CAPACITY EXCEEDED] STA {} -> AP {}: {} packets (max: {}), dropping {}",
                    format_elapsed!(now),
                    sta_src,
                    sta_dest,
                    packets.packet_count,
                    self.ul_capacity_queue_device,
                    excess_count_dl
                );
            }
        }
        let mut global_indices_to_drop = std::collections::HashSet::new();
        
        if !overflowing_flows.is_empty() {
            // 2. Find all indices for *these specific* overflowing flows
            // We build a map: (src, dest) -> [idx1, idx8, idx22]
            let mut flow_indices_map: HashMap<(i32, i32), Vec<usize>> = HashMap::new();
            for (i, packet) in self.queue.iter().enumerate() {
                let key = (packet.sta_src_id, packet.sta_dest_id);
                if overflowing_flows.contains_key(&key) {
                    flow_indices_map.entry(key).or_default().push(i);
                }
            }

            // 3. For each overflowing flow, get the newest (last) indices to drop
            for (key, excess_count) in overflowing_flows {
                if let Some(indices) = flow_indices_map.get_mut(&key) {
                    // `indices` is already sorted (from iter().enumerate()). We want the last `excess_count`.
                    let start_index = indices.len().saturating_sub(excess_count);
                    for i in start_index..indices.len() {
                        global_indices_to_drop.insert(indices[i]);
                    }
                }
            }
            // 4. Run retain ONCE to remove all marked packets
            let mut i = 0;
            self.queue.retain(|packet| {
                let current_idx = i;
                i += 1; // Increment index for the next packet

                if global_indices_to_drop.contains(&current_idx) {
                    // This is a packet to drop
                    self.blocked_packet_counter += 1;
                    
                    // Decrement per-link queue depth
                    if let Some(link_id) = packet.assigned_link_id {
                        if let Some(count) = self.link_queue_depths.get_mut(&link_id) {
                            *count = count.saturating_sub(1);
                        }
                    }

                    return false; // Drop from queue
                }
                return true; // Keep in queue
            });
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
                    medium.occupy_collision(now + T_col_dur);
                }
                if let Ok(mut map) = self.array_dcf_values.lock() {
                        // Apply backoff to all contenders on this link
                    for key in contenders.clone() {
                        if let Some(st) = map.get_mut(&key) {
                            let old_cw = st.cw;
                            st.on_failure();
                            debug_edca_r!(
                                " \t\t ↳ ({}, {:?}, L-{}): CW {} → {}, backoff={}",
                                key.0, key.1, key.2,
                                old_cw,
                                st.cw,
                                st.backoff_counter
                            );
                        }
                    }
                    debug_edca_r!(
                            "{} [COLLISION] LINK-{}: {} contenders collided, T_col={:.3}ms",
                            format_elapsed!(now),
                            link_id,
                            contenders.len(),
                            T_col * 1000.0
                        );
                                            
                    // Freeze all MACs on this link
                        for ((_, _, lid), st) in map.iter_mut() {
                            if *lid == link_id {
                                st.backoff_frozen = true;
                                st.medium_free_since = now + T_col_dur;
                            }
                        }
                }
                
                // CRITICAL FIX: Schedule wake-up after collision resolves
                context.scheduler
                    .schedule_event(T_col_dur, Self::deque_schedule_service, ())
                    .unwrap();
                
                // Mark that we scheduled something
                transmissions_scheduled = true;

                continue;
            }
            
            // Single winner on this link
            let winner_key = contenders[0];
            let (sta_id, ac, winner_link_id) = winner_key;
            
            // Find first packet that matches this STA/AC
            let first_ix = match self.queue.iter().position(|p| {
                let is_ul = p.sta_src_id > p.sta_dest_id;
                let p_sta = if is_ul { p.sta_src_id } else { -1 };
                p_sta == sta_id
                 && p.edca_ac == ac
                 && p.assigned_link_id == Some(winner_link_id)
            }) {
                Some(ix) => ix,
                None => {
                    // Reload backoff for next round
                    if let Some(st) = self.array_dcf_values.lock().unwrap().get_mut(&winner_key) {
                        st.on_success(st.param.cw_min);
                    }
                    continue;
                }
            };
            
            let first_packet = self.queue.get(first_ix).cloned().unwrap();
            
            // Build AMPDU for this link
            let (mut ampdu_to_send, ampdu_airtime) = self.build_new_ampdu(&first_packet, now);
            ampdu_to_send.link_id = link_id; // Tag AMPDU with link
            
            if ampdu_to_send.mpdu_packets.is_empty() || ampdu_airtime == Duration::ZERO {
                log_mlo!(
                    now,
                    "⚠️  LINK-{}: Empty AMPDU or zero airtime, skipping transmission",
                    link_id
                );
                
                // Reset backoff for this MAC to try again
                if let Some(st) = self.array_dcf_values.lock().unwrap().get_mut(&winner_key) {
                    st.on_failure(); // Increase CW and redraw backoff
                }
                // schedule next tick to allow backoff countdown (no deadlock)
                context.scheduler
                    .schedule_event(Duration::from_secs_f64(SLOT), Self::deque_schedule_service, ())
                    .unwrap();

                transmissions_scheduled = true; // Prevent duplicate scheduling
                
                continue;
            }

            // Occupy medium on this link
            if let Some(medium) = self.link_mediums.get_mut(&link_id) {
                medium.start_txop(now + ampdu_airtime, winner_key);
            }
            
            // Freeze all MACs on this link during TXOP
            if let Ok(mut map) = self.array_dcf_values.lock() {
                for ((_, _, lid), st) in map.iter_mut() {
                    if *lid == link_id {
                        st.backoff_frozen = true;
                        st.medium_free_since = now + ampdu_airtime;
                    }
                }
            }
            
            // Schedule TX completion
            context.scheduler
                .schedule_event(ampdu_airtime, Self::send_ampdu, ampdu_to_send)
                .unwrap();
            
            transmissions_scheduled = true;
        }
        
        // Schedule next slot check if needed
        if !transmissions_scheduled {
            let mut need_next_slot = false;
            
            // Check if ANY MAC has work to do (including frozen MACs)
            if let Ok(map) = self.array_dcf_values.lock() {
                for (key, st) in map.iter() {
                    // Has packets for this flow?
                    let (sta_id, ac, link_id) = *key;
                    let has_packets = self.queue.iter().any(|p| {
                        let p_sta = if p.sta_src_id > p.sta_dest_id { p.sta_src_id } else { -1 };
                        p_sta == sta_id && p.edca_ac == ac && p.assigned_link_id == Some(link_id)
                    });
                    
                    if !has_packets {
                        continue;
                    }
                    
                    // Frozen MAC will unfreeze in the future
                    if st.backoff_frozen {
                        need_next_slot = true;
                        break;
                    }
                    
                    // MAC has non-zero backoff
                    if st.backoff_counter > 0 {
                        need_next_slot = true;
                        break;
                    }
                }
            }
            
            if need_next_slot {
                context.scheduler
                    .schedule_event(Duration::from_secs_f64(SLOT), Self::deque_schedule_service, ())
                    .unwrap();
            }
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
