use crate::{debug_bgprint, debug_debug, print_blue, print_green, print_pretty, print_prettyyyy, print_red, print_yellow};
use crossbeam::channel::{unbounded, Receiver, Sender};
use rand::Rng;
use std::cmp::{self, max};
use std::collections::{HashMap, VecDeque};
use std::f64::consts::PI;
use std::future::Future;
// use std::hash::Hash;
use asynchronix::model::{Context, Model};
use asynchronix::ports::Output;
use std::time::{Duration, Instant};
use rand_distr::{Normal, Distribution};
use crate::lib::alvr_stream_socket::parse_shard_data;
use crate::lib::{ SLOT, MacKey};
use crate::lib::DebugColor; 
use rand::rngs::StdRng;
// use crate::lib::TESTS_RANDOM_PATTERNS;
use std::sync::{Arc, Mutex};
use tai_time::TaiTime;
use serde::{Serialize, Deserialize};

// use crate::db_debug_bgprint;

use crate::lib::{
    collision_delay, exponential,  airtime_ampdu, perStaLockStats, AmpduPacket, Coords,
    CsvType, CumulativeStats, MpduPacket, DEBUG_PRINT_ENABLED, DEFAULT_TMAX_AGG, MAX_AMPDU_SIZE, NUMBER_OF_RANDOM_EVENTS,
    P_TX,
};
use std::collections::HashSet;
use crate::{debug_print, format_elapsed, taitime_to_f64};

use rand::{SeedableRng};

//////////// CONST DEFINES ///////////

// #[macro_export]
// macro_rules! debug_schedule {
//     ($fmt:expr,$($arg:tt)*) => {
//         if DEBUG_SCHEDULING == true {
//             println!($fmt, $($arg)*);
//         }
//     }
// }

const DEBUG_EDCA: bool = false; 

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
macro_rules! debug_schedule {
    ($fmt:expr, $($arg:tt)*) => {
        // if DEBUG_SCHEDULING == true {
            let msg = format!($fmt, $($arg)*);
            println!("{}", DebugColor::Navy.to_background_fn()(msg));
        // }

    };
}

// pub const DEBUG_SCHEDULING: bool = false;

pub const SOFTMAX_POLICY: bool = false;
pub const LYAPUNOV_POLICY: bool = false;
pub const LYAPUNOV_V: f64 = 5E7; // Lyapunov optimization parameter

struct StaRateInfo {
    total_transmission_delay_single: f64,
    total_transmission_delay_fullampdu: f64,
    fullampdu_max_size: usize,
    packet_count: usize,
    weighted_rate_single: f64,
    weighted_rate_fullampdu: f64,
    per_packet_channel_access_efficiency: f64,
    expected_queue_delivery_ms: f64,
}

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
pub const MAX_EMULATED_QUEUE_PACKETS: usize = 10000;


pub const STEP1_TBEGIN: f64 = 25.0;
pub const STEP1_TEND: f64 =   35.0;

pub const STEP2_TBEGIN: f64 = 45.0;
pub const STEP2_TEND: f64 =   55.0;

pub const STEP3_TBEGIN: f64 = 65.0;
pub const STEP3_TEND: f64 =   75.0;

pub const BANDWIDTH_LIMIT_S1: f64 = 90E6;
pub const BANDWIDTH_LIMIT_S2: f64 = 70E6;
pub const BANDWIDTH_LIMIT_S3: f64 = 50E6;

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
            packet.length_packet = cmp::max(1, len_random);

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
        println!("\n*************************************************");
        println!("[DEBUG STA{}]\tCoordinates: {:?}\n\tDestination: STA{} | RATE_IN: {:.3} Mbps, Rate_service: {:.3} (packs/s),\n\t Arrival_rate (pack/s): {:.3}, Departure_rate: {:.3},  L = {}",
                            src, coordinates, dest,                     arrival_rate_bps/1E6, rate_service_bps / 1E6 , arrival_rate,effective_mu ,mean_length);

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

                packet.length_packet = cmp::max(1, len_random);
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

#[allow(unused)]
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
            "on_duration_secs",   "on_duration_nanos",
            "off_duration_secs",  "off_duration_nanos",
            "current_state",
            "last_change_secs",   "last_change_nanos",

            // ProbabilisticDrop
            "drop_probability",
            "pd_valid_from_secs", "pd_valid_from_nanos",
            "pd_valid_until_secs","pd_valid_until_nanos",

            // Bandwidth
            "bw_max_bps",         "bw_current_tokens",
            "bw_max_tokens",      "bw_token_refill_rate",
            "bw_valid_from_secs", "bw_valid_from_nanos",
            "bw_valid_until_secs","bw_valid_until_nanos",

            // Jitter
            "jit_mean_delay_secs","jit_mean_delay_nanos",
            "jit_distribution",   "jit_variance",
            "jit_correlation_pct","jit_last_delay_secs",
            "jit_last_delay_nanos",
            "jit_valid_from_secs","jit_valid_from_nanos",
            "jit_valid_until_secs","jit_valid_until_nanos",
        ]
    }



    /// Turn *this* variant into one row of Strings, matching exactly the above headers.
    pub fn to_csv_row(&self) -> Vec<String> {
        // convenience closures
        let d2s = |d: &Duration| d.as_secs().to_string();
        let d2n = |d: &Duration| d.subsec_nanos().to_string();
        let t2s = |t: &TaiTime<0>| t.as_secs().to_string();
        let t2n = |t: &TaiTime<0>| t.subsec_nanos().to_string();

        // start with effect_type
        let mut row = vec![format!("{:?}", self)  // but we'll overwrite below
            .split('(').next().unwrap().to_string()
        ];

        // now push _all_ possible columns in the same order as csv_headers()
        match self {
            NetworkPattern::OnOffPeriodic {
                on_duration,
                off_duration,
                current_state,
                last_state_change,
            } => {
                row.push(d2s(on_duration));
                row.push(d2n(on_duration));
                row.push(d2s(off_duration));
                row.push(d2n(off_duration));
                row.push(current_state.to_string());
                row.push(t2s(last_state_change));
                row.push(t2n(last_state_change));

                // fill the rest with empties
                row.extend(std::iter::repeat(String::new()).take(
                    NetworkPattern::csv_headers().len() - row.len(),
                ));
            }

            NetworkPattern::ProbabilisticDrop {
                drop_probability,
                valid_from,
                valid_until,
            } => {
                // push blanks for OnOffPeriodic
                row.extend((0..7).map(|_| String::new()));

                row.push(drop_probability.to_string());
                row.push(t2s(valid_from));
                row.push(t2n(valid_from));
                row.push(t2s(valid_until));
                row.push(t2n(valid_until));

                // fill the rest
                row.extend(std::iter::repeat(String::new())
                    .take(NetworkPattern::csv_headers().len() - row.len()));
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
                // blanks for OnOffPeriodic + ProbabilisticDrop
                row.extend((0..12).map(|_| String::new()));

                row.push(max_bps.to_string());
                row.push(current_tokens.to_string());
                row.push(max_tokens.to_string());
                row.push(token_refill_rate.to_string());
                row.push(t2s(valid_from));
                row.push(t2n(valid_from));
                row.push(t2s(valid_until));
                row.push(t2n(valid_until));

                row.extend(std::iter::repeat(String::new())
                    .take(NetworkPattern::csv_headers().len() - row.len()));
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
                // blanks for the first three variants
                row.extend((0..20).map(|_| String::new()));

                row.push(d2s(mean_delay));
                row.push(d2n(mean_delay));
                row.push(format!("{:?}", distribution_type));
                row.push(variance.to_string());
                row.push(correlation_pct.to_string());
                row.push(d2s(last_delay));
                row.push(d2n(last_delay));
                row.push(t2s(valid_from));
                row.push(t2n(valid_from));
                row.push(t2s(valid_until));
                row.push(t2n(valid_until));
            }

            NetworkPattern::Constant => {
                // nothing else to push—just pad out the full width
                row.extend(std::iter::repeat(String::new())
                    .take(NetworkPattern::csv_headers().len() - 1));
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




pub struct EmulatedLink { /// Can be inserted between any STA output and the `QueueModule` input.

    pub output: Output<MpduPacket>,
    queue_mechanism: QueueMechanism,
}

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
    ) -> Self {

        let queue_mechanism: QueueMechanism;

        if let Some(values_tests) = emulated_tests {
            queue_mechanism =
                QueueMechanism::new(max_queue_size, now, values_tests);
        } else {
            print_yellow!("NO PATTERNS?", ); 
            queue_mechanism = QueueMechanism::new(
                MAX_EMULATED_QUEUE_PACKETS,
                TaiTime::EPOCH,
                (false, false, false, false),
            );
        }

        EmulatedLink {
            output: Default::default(),
            queue_mechanism,
            
        }
    }

    /// Handle packet arrival from a STA. Applies emulation logic, possibly queuing or dropping.
    pub async fn input(&mut self, packet: MpduPacket, context: &Context<Self>) {
        // Delegate to the queue mechanism
        match self.queue_mechanism.enqueue_or_transmit(packet, context) {
            EnqueueResult::Transmitted(pkt) => {
                // No emulation delay: forward immediately
                self.output.send(pkt).await;
            }
            EnqueueResult::Queued(queued_pkt) => {
                // Scheduled for delayed transmission
                if let Some(deadline) = queued_pkt.emulated_added_delay_deadline {
                    let now = context.scheduler.time();
                    let delay = deadline
                        .duration_since(now)
                        .max(Duration::from_nanos(1));
                    // Schedule a flush event
                    context
                        .scheduler
                        .schedule_event(delay, Self::flush_queue, ())
                        .unwrap();
                }
            }
            EnqueueResult::Dropped => {
                // Packet dropped by network pattern or overflow. Logging can go here.
                print_red!(
                    // crate::lib::DebugColor::Red,
                    "[EMULATED LINK] Packet dropped by network emulator", 
                );
            }
        }
    }

    /// Scheduled event: attempt to emit all ready packets from the internal queue
    pub fn flush_queue<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            // Process all delayed packets that have reached their deadline
            let ready = self.queue_mechanism.process_emu_queued_packets(context);
            for pkt in ready {
                self.output.send(pkt).await;
            }

            if !self.queue_mechanism.queue.is_empty() {
                if let Some(next_deadline) = self.queue_mechanism
                                      .queue
                                      .iter()
                                      .filter_map(|p| p.emulated_added_delay_deadline)
                                      .min()
                {   
                    let now   = context.scheduler.time();
                    let delay = next_deadline.duration_since(now)
                                            .max(Duration::from_nanos(1));
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
}

impl QueueMechanism {
    pub fn new(
        max_emulated_queue_packets: usize,
        _now: TaiTime<0>,
        tests: (bool, bool, bool, bool),
    ) -> Self {
        let mut network_emulator = NetworkPatternEmulator::new();

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
            network_emulator.add_random_events(
                NUMBER_OF_RANDOM_EVENTS,                                  // count: add 5 events
                RandomEventKind::Jitter,            // type of event
                overall_start,                      // overall start time for events
                overall_end,                        // overall end time for events
                Duration::from_millis(50),         // min duration for each event
                Duration::from_millis(500),         // max duration for each event
                JitterDistributionType::Uniform,    // distribution for the event duration (and jitter)
                8.0,                                // mean delay in ms
                6.0,                                // half-width (for Uniform jitter) or std dev (if Gaussian)
            );
    
            network_emulator.add_random_events(
                NUMBER_OF_RANDOM_EVENTS,                                  // Number of events
                RandomEventKind::PacketLoss,        // Event type: Packet Loss
                overall_start,                      // Overall window start time
                overall_end,                        // Overall window end time
                Duration::from_millis(50),          // Minimum duration per event
                Duration::from_millis(500),         // Maximum duration per event
                JitterDistributionType::Uniform,    // Distribution for event duration
                0.01,                                // Drop probability (mean_value)
                0.01,                                // Variance (not used for packet loss events)
            );
    
            network_emulator.add_random_events(
                NUMBER_OF_RANDOM_EVENTS,                                  // Number of events
                RandomEventKind::Bandwidth,         // Event type: Bandwidth limit
                overall_start,                      // Overall window start time
                overall_end,                        // Overall window end time
                Duration::from_millis(50),         // Minimum duration per event
                Duration::from_millis(500),        // Maximum duration per event
                JitterDistributionType::Uniform,    // Distribution for event duration
                50e6,                                // Maximum bps (1Mbps) as mean_value
                0.0,                                // Variance (not used for bandwidth events)
            );
        }


        if test_bw {

            print_red!("****BW PATTERNS ADDED*****", ); 
            let mtu = 1500.0 * 8.0; 
            network_emulator.add_pattern(NetworkPattern::new_bandwidth(
                mtu,
                BANDWIDTH_LIMIT_S1,
                valid_from,
                valid_until,
            ));
            network_emulator.add_pattern(NetworkPattern::new_bandwidth(
                mtu, 
                BANDWIDTH_LIMIT_S2,
                valid_from2,
                valid_until2,
            ));
            network_emulator.add_pattern(NetworkPattern::new_bandwidth(
                mtu, 
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
        }
    }

    // pub fn should_purge_queue(&self, current_time: TaiTime<0>) -> bool {
    //     // Check each bandwidth pattern's end time
    //     for pattern in &self.network_emulator.patterns {
    //         if let NetworkPattern::Bandwidth { valid_until, .. } = pattern {
    //             // If we've just passed the end time of a pattern, purge the queue
    //             if current_time >= *valid_until
    //                 && current_time
    //                     <= valid_until
    //                         .checked_add(Duration::from_millis(200))
    //                         .unwrap_or(*valid_until)
    //             {
    //                 return true;
    //             }
    //         }
    //     }
    //     false
    // }

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

        if packet.length_packet == 0 {
            packet.length_packet = packet.data_inner.len();   // fallback for early traffic
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

                // dbg_reason = "queued"; 
                // _dbg_delay = delay; 

                // db_debug_bgprint!(DebugColor::Chocolate, "[DBG Queue NETEM] Q_length: {} | ENQUEUED packet {} - delayed by {:.6} seconds (ALVR: frame {} shard {:4.0}/{:4.0})", 
                //             self.queue.len(),
                //             packet.packet_id,
                //             delay.as_secs_f64(),
                //             packet.header_alvr.next_packet_index,
                //             packet.header_alvr.shard_index,
                //             packet.header_alvr.shards_count - 1,
                //         );

                // Add the delay to the packet's queue_in_instant
                let mut delayed_packet = packet.clone();
                delayed_packet.emulated_added_delay_deadline = now.checked_add(delay);

                // Enqueue the packet
                if self.queue.len() < self.max_queue_size {
                    self.queue.push_back(delayed_packet.clone());
                    EnqueueResult::Queued(delayed_packet)
                } else {
                    print_red!(
                        "[NETEM FULL queue] Packet {} DROPPED (ALVR: F_id: {} , {} / {})",
                        delayed_packet.packet_id,
                        delayed_packet.header_alvr.next_packet_index,
                        delayed_packet.header_alvr.shard_index,
                        delayed_packet.header_alvr.shards_count
                    );
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
        let mut transmitted_packets: Vec<MpduPacket> = Vec::new();
        let mut index = 0;
        // print_red!("****PROCESSING ENQUED PACKETS*****\n Len of self queue: {}", self.queue.len()); 


        
        let mut indexes_to_remove = vec![];
        while index < self.queue.len() {
            if let Some(packet) = self.queue.get_mut(index) {
                match packet.emulated_added_delay_deadline {
                    Some(delay) if delay == TaiTime::EPOCH => {
                        // Remove and process the packet
                        debug_bgprint!(DebugColor::DarkGreen, "[DBG EMU QUEUE PROCESS] Delay ZERO Packet ALVR: {}. Now = {} | (F_index: {} , {} / {} )", 
                        format_elapsed!(delay), now.duration_since(TaiTime::EPOCH).as_secs_f32(), packet.header_alvr.next_packet_index, packet.header_alvr.shard_index, packet.header_alvr.shards_count );
                        // let packet = self.queue.remove(index).unwrap();
                        indexes_to_remove.push(index);
                        transmitted_packets.push(packet.clone());
                        // Don't increment index as we've removed the current element
                    }
                    Some(delay) => {
                        const EPSILON: f32 = 1e-9;

                        // Compare with a small tolerance
                        if (now.duration_since(TaiTime::EPOCH).as_secs_f32()
                            - delay.duration_since(TaiTime::EPOCH).as_secs_f32())
                        .abs()
                            < EPSILON
                        {
                            transmitted_packets.push(packet.clone());

                            self.queue.remove(index);

                            // continue; // skip incrementing index
                        } else {
                            // debug_bgprint!(DebugColor::Mint, "[DBG EMU QUEUE] Delay of Packet ALVR: {}. Now = {:.9}, deadline = {:.9} | (F_index: {} , {}/{} )",
                            // format_elapsed!(delay), now.duration_since(TaiTime::EPOCH).as_secs_f32() ,packet.emulated_added_delay_deadline.unwrap().duration_since(TaiTime::EPOCH).as_secs_f32() ,packet.header_alvr.next_packet_index, packet.header_alvr.shard_index, packet.header_alvr.shards_count );
                            index += 1;
                        }

                        // Packet still needs to wait
                    }
                    None => {
                        // Packet dropped
                        self.queue.remove(index);
                        print_pretty!(
                            DebugColor::Red,
                            "[EMU QUEUE DROP] Dropped packet ID {}",
                            index
                        );
                    }
                }
            } else {
                break;
            }
        }
        indexes_to_remove.sort_unstable();
        indexes_to_remove.reverse();

        for &index in &indexes_to_remove {
            print!("actually Removed: ");
            let packet = self.queue.remove(index).unwrap();
            packet.print(DebugColor::LightBlue);
        }
        transmitted_packets
    }
}


#[derive(Clone, Debug)]
pub enum RandomEventKind {
    PacketLoss,
    Jitter,
    Bandwidth,
}


#[derive(Clone, Debug)]
pub struct NetworkPatternEmulator {
    patterns: Vec<NetworkPattern>,
    last_update_time: TaiTime<0>,
    last_update_only_DBG_NETEM: TaiTime<0>,
    debug_counter: usize, // Counter to track the calls
}
impl NetworkPatternEmulator {
    pub fn new() -> Self {
        Self {
            patterns: Vec::new(),
            last_update_time: TaiTime::default(),
            last_update_only_DBG_NETEM: TaiTime::default(),
            debug_counter: 0,
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
        let overall_duration = overall_end.duration_since(overall_start);
        let max_offset_secs = overall_duration
            .as_secs_f64()- (max_duration.as_secs_f64());

        let mut rng = rand::thread_rng();

        for _ in 0..count {
            // Randomly choose a start time within the overall window, leaving room for a full event duration.
            let offset_secs = rng.gen_range(0.0..max_offset_secs);
            let event_start = overall_start
                .checked_add(Duration::from_secs_f64(offset_secs))
                .expect("Time addition failed");

            // Determine event duration based on chosen distribution
            let duration_secs = match dist {
                JitterDistributionType::Uniform => {
                    let min = min_duration.as_secs_f64();
                    let max = max_duration.as_secs_f64();
                    rng.gen_range(min..max)
                }
                JitterDistributionType::Gaussian => {
                    // For Gaussian, we use a Normal distribution centered at the midpoint.
                    let center = (min_duration.as_secs_f64() + max_duration.as_secs_f64()) / 2.0;
                    // Create a normal distribution; if variance <= 0, fallback to center.
                    let normal = Normal::new(center, variance).unwrap_or_else(|_| Normal::new(center, 0.1).unwrap());
                    // Sample and then clamp the duration between min and max.
                    let sample = normal.sample(&mut rng);
                    sample.max(min_duration.as_secs_f64()).min(max_duration.as_secs_f64())
                }
            };
            let event_duration = Duration::from_secs_f64(duration_secs);
            let event_end = event_start
                .checked_add(event_duration)
                .expect("Time addition failed");
                // Debug log: print event details before creation.
            print_prettyyyy!(
                DebugColor::Green, 
                "Creating event: {:?}, start: {:?}, duration: {:?}, intensity: {}",
                event_type, event_start, event_duration, mean_value
            ); 

            let std_dev = variance.sqrt();
            let normal = Normal::new(mean_value, std_dev).expect("Invalid distribution parameters");
            let mut drop_probability = normal.sample(&mut rng);
            drop_probability = drop_probability.clamp(0.0, 1.0);
                            
            // Create the event based on its type.
            let pattern = match event_type {
                RandomEventKind::PacketLoss => NetworkPattern::ProbabilisticDrop {
                    drop_probability: drop_probability, // e.g. 0.8 for intense loss
                    valid_from: event_start,
                    valid_until: event_end,
                },
                RandomEventKind::Jitter => {
                    // Use the distribution type to pick between jitter constructors.
                    match dist {
                        JitterDistributionType::Uniform => NetworkPattern::new_jitter_uniform(
                            mean_value, // mean delay in ms
                            variance,   // half-width in ms
                            0.0,        // no correlation by default
                            event_start,
                            event_end,
                        ),
                        JitterDistributionType::Gaussian => NetworkPattern::new_jitter_gaussian(
                            mean_value, // mean delay in ms
                        variance,   // standard deviation in ms
                        0.0,        // no correlation by default
                        event_start,
                        event_end,
                    ),
                }
            }, 
            RandomEventKind::Bandwidth => {
                // For bandwidth events, mean_value represents the max_bps limit.
                NetworkPattern::new_bandwidth(
                    mean_value, // max_bps for the event
                    mean_value, // here we use the same value for the token refill rate
                    event_start,
                    event_end,
                )
            }
        };

        // Finally, add the generated pattern to the emulator.
        self.add_pattern(pattern);
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
            print_green!(
                "{} [PATTERN TRANSITION] Bandwidth pattern just ended, need to purge queue",
                format_elapsed!(current_time)
            );

            return None; // Signal to drop the packet (and potentially purge queue)
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
                        let pkt_bits = (packet.length_packet * 8) as f64;
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
    /* VO */ EdcaParam { cw_min:  3,  cw_max:  7,  aifsn: 2, txop_limit_us:  1504 },
    /* VI */ EdcaParam { cw_min:  7,  cw_max: 15, aifsn: 2,   txop_limit_us:  3008 },
    /* BE */ EdcaParam { cw_min: 15,  cw_max: 1023, aifsn: 3, txop_limit_us:    0 },
    /* BK */ EdcaParam { cw_min: 15,  cw_max: 1023, aifsn: 7, txop_limit_us:    0 },
];

#[derive(Hash, Clone, Copy, Debug)]
pub struct DcfStats {
    pub cw: i32,
    pub backoff_counter: i32, 
    pub retry_count: u8, 
    pub param: EdcaParam,          // <-- NEW (static per AC)
    pub medium_free_since: TaiTime<0>,   // NEW – 
    pub backoff_frozen: bool,            // NEW
    // pub slot_timer_event,
}
 
pub const MAX_RETRIES_MAC: u8 = 10; 
use crate::lib::EdcaAc;

impl DcfStats {
     pub fn new(ac: EdcaAc) -> Self {
        let p = EDCA_TABLE[ac as usize];
        Self {
            cw: p.cw_min,
            backoff_counter: rand::thread_rng().gen_range(0..=p.cw_min),
            retry_count: 0,
            param: p,
            medium_free_since: TaiTime::EPOCH, 
            backoff_frozen: false, 
        }
    }

    /// Draw a new random back-off inside the current CW.
    pub fn reload_backoff(&mut self) {
        self.backoff_counter = rand::thread_rng().gen_range(0..=self.cw);
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
        if self.cw > self.param.cw_max { self.cw = self.param.cw_max; }
        self.reload_backoff();
        true                                           // keep packet
    }
}



#[inline]
fn ac_short(ac: EdcaAc) -> &'static str {
    match ac {
        EdcaAc::Voice => "VO",
        EdcaAc::Video => "VI",
        EdcaAc::BestEffort => "BE",
        EdcaAc::Background => "BK",
    }
}

#[inline]
fn fmt_key(key: &MacKey) -> String {
    // key: (sta_id, EdcaAc)
    let sta = key.0;
    let ac  = ac_short(key.1);
    // AP uses -1 in your code — keep it visible:
    format!("STA={:>2} AC={}", sta, ac)
}

#[inline]
fn log_edca(key: &MacKey, msg: &str) {
    // Keep it short and grep-friendly
    print_blue!("\t\t-----contenders: {} | {}", fmt_key(key), msg);
}



fn maps_to((id, ac): &MacKey, p: &MpduPacket) -> bool {
    // AP (downlink) contends with id = -1; UL STA contends with its own id
    let mac_id = if p.sta_src_id > p.sta_dest_id { p.sta_src_id } else { -1 };
    (mac_id, p.edca_ac) == (*id, *ac)
}


fn ac_needs_tick(
    key:  &MacKey,
    st:   &DcfStats,
    q:    &VecDeque<MpduPacket>,
    now:  TaiTime<0>,
) -> bool {
    // Does the queue hold a packet that maps to this virtual MAC?
    let has_pkts = q.iter().any(|p| maps_to(key, p));
    if !has_pkts { return false; }

    // Still inside AIFS window or back-off not yet zero?
    let aifs_done = st.medium_free_since + aifs(st.param) <= now;
    !aifs_done || st.backoff_counter > 0
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
    #[inline] pub fn set_nav_until(&mut self, t: TaiTime<0>) { self.nav_until = t; }

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

    #[inline] pub fn current_owner(&self) -> Option<MacKey> { self.tx_owner }
    #[inline] pub fn last_owner(&self) -> Option<MacKey> { self.last_txop_owner }
    #[inline] pub fn last_end(&self) -> TaiTime<0> { self.last_txop_end }


}

#[allow(unused)]
#[derive(Clone)]
pub struct QueueModule {
    pub output_port_sta1: Output<AmpduPacket>,
    pub output_port_sta2: Output<AmpduPacket>,

    pub queue: VecDeque<MpduPacket>,
    pub queue_maxsize: usize,
    pub service_timer: Duration,
    // pub aux_packet_serviced: MpduPacket,
    pub aux_ampdu_serviced: AmpduPacket,

    pub packet_being_served: bool,

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

    pub array_dcf_values: Arc<Mutex<HashMap<MacKey, DcfStats>>>, 

    pub PL_probability: f64,

    // pub queue_network_emulator: QueueMechanism,
    pub ul_capacity_queue_device: usize,

    pub ampdu_id: u32, 

    pub shared_medium: Medium, 
}


#[allow(unused)]
impl QueueModule {
    pub fn get_queue_stats_handle(&self) -> Arc<Mutex<QueueStats>> {
        self.cumulative_stats_queue.clone()
    }
    pub fn get_stas_stats_handle(&self) -> Arc<Mutex<HashMap<usize, perStaLockStats>>> {
        self.array_stas_stats.clone()
    }
    pub fn new(
        num_stas: usize,
        queue_size: usize,
        PL_prob: f64,
        vec_ids: Vec<i32>,
        folder_dir: String,
        ul_size: usize,
        emulated_tests: Option<(bool, bool, bool, bool)>, // BW, Jitter, PL
    ) -> Self {
        // Create a vector of perStaLockStats with initialized sta_ids
        let mut stats_vec: HashMap<usize, perStaLockStats> = HashMap::new();
        let mut dcf_stats_vec = HashMap::new(); 

        let (stats_tx, stats_rx) = unbounded();

        for i in 0..num_stas {
            let sta_stats = perStaLockStats::new();
            // We need to lock the mutex to modify the sta_id
            if let Ok(mut stats) = sta_stats.data.clone().lock() {
                stats.sta_id = vec_ids[i] as i32;
                println!("iter: {}, stats id : {:?}", i, stats.sta_id);
                stats_vec.insert(stats.sta_id.clone() as usize, sta_stats.clone());
        
            }    
        }

        ///////////////////// CREATE BEB QUEUES /////////////////////
   
        // AP gets four virtual MACs (one per AC)
        for ac in [EdcaAc::Voice, EdcaAc::Video, EdcaAc::BestEffort, EdcaAc::Background] {
            dcf_stats_vec.insert((-1, ac), DcfStats::new(ac));
        }

        // Every uplink STA keeps **one** MAC – attach its “default” AC_BE
        for sta_id in vec_ids {
            for ac in [EdcaAc::Voice, EdcaAc::Video, EdcaAc::BestEffort, EdcaAc::Background] {
                dcf_stats_vec.insert((sta_id, ac), DcfStats::new(ac));
            }
        }

        Self {
            queue: VecDeque::new(),
            queue_maxsize: queue_size,
            output_port_sta1: Default::default(),
            output_port_sta2: Default::default(),
            service_timer: Duration::ZERO,
            aux_ampdu_serviced: AmpduPacket::new(),
            packet_being_served: false,
            blocked_packet_counter: 0,
            arrived_packet_counter: 0,
            queue_length_counter: 0,
            service_rate: 0.0,
            t0_time: Instant::now(),

            csv_metrics: CsvType::new(&folder_dir).expect("?? CSVTYPE"),

            coords_queue: Coords::new(),
            p_tx: P_TX,
            STA_coords_grid: Vec::new(),
            STA_coords_map: HashMap::new(),

            cumulative_stats_queue: Arc::new(Mutex::new(QueueStats::new())),
            array_stas_stats: Arc::new(Mutex::new(stats_vec)),

            array_dcf_values: Arc::new(Mutex::new(dcf_stats_vec)),  

            stats_tx: Some(stats_tx),
            stats_rx: Some(stats_rx),

            PL_probability: PL_prob,
            // queue_network_emulator: queue_mechanism,
            ul_capacity_queue_device: ul_size,
            ampdu_id: 0, 

            shared_medium: Medium::default(), 
        }
    }



   fn tick_backoff(&mut self, now: TaiTime<0>) -> Vec<MacKey> {
        
        self.shared_medium.clear_if_idle(now);
        
        let mut ready = Vec::new();
        let idle_slot = self.shared_medium.is_idle(now);

        // crate::print_pink!("states array dcf: ",); 
        // for state in self.array_dcf_values.lock().unwrap().keys(){
        //     crate::print_pink!("{:?}", state); 
        // }


        debug_edca!(
            "{} | [EDCA] medium_idle={} |  q_size={} | ac_states={}",
            format_elapsed!(now),
            idle_slot,
            self.queue.len(),
            self.array_dcf_values.lock().unwrap().len()
        );
        for (key, st) in self.array_dcf_values.lock().unwrap().iter_mut() {
            if !self.queue.iter().any(|p| maps_to(key, p)) { 
                // log_edca(now, key, "no_pkts_for_AC -> skip");
                continue; 
            }

            // AIFS gating
            if idle_slot && st.medium_free_since + aifs(st.param) <= now {
                debug_edca!("\t[{} AIFS satisfied] -> unfreeze", key.0);
                st.backoff_frozen = false;
            }
            else{
                debug_edca!("\t[{} WAIT AIFS] {} < {}",key.0, format_elapsed!(now)  , format_elapsed!(now + aifs(st.param))); 
            }
            // else if !idle_slot {
                // st.medium_free_since = now;
                // st.backoff_frozen = true;
            // }

            // Backoff countdown (one slot per call of deque_schedule_service)
            if idle_slot && !st.backoff_frozen && st.backoff_counter > 0 {

                let prev = st.backoff_counter;
                st.backoff_counter -= 1;
                if DEBUG_EDCA {
                    log_edca(
                        key,
                        &format!(" countdown {} -> {}", prev, st.backoff_counter)
                );
                }
            }

            if st.backoff_counter == 0 && !st.backoff_frozen {
                if DEBUG_EDCA{print_green!("{} [ {} -> AC {:?}] READY (backoff==0 & unfrozen)", format_elapsed!(now), key.0, key.1);}
                ready.push(*key);
            }
        }
        ready
    }

    fn txop_cap_secs(&self, key: &MacKey) -> f64 {
        let p = self.array_dcf_values.lock().unwrap()[key].param;
        if p.txop_limit_us == 0 { f64::INFINITY } else { p.txop_limit_us as f64 * 1e-6 }
    }

    fn ac_needs_tick(key: &MacKey, st: &DcfStats, q: &VecDeque<MpduPacket>, now: TaiTime<0>) -> bool {
        let has_pkts = q.iter().any(|p| maps_to(key, p));
        if !has_pkts { return false; }
        let aifs_done = st.medium_free_since + aifs(st.param) <= now;
        !aifs_done || st.backoff_counter > 0
    }

    fn ac_prio(&mut self, ac: EdcaAc) -> u8 { match ac {
        EdcaAc::Voice => 0, EdcaAc::Video => 1, EdcaAc::BestEffort => 2, EdcaAc::Background => 3
    }}

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


    // fn backoff_tick(&self) -> Vec<MacKey> {
    //     let mut ready = Vec::new();
    //     if let Ok(mut map) = self.array_dcf_values.lock() {
    //         for (key, stats) in map.iter_mut() {
    //             if stats.backoff_counter > 0 { stats.backoff_counter -= 1; }
    //             if stats.backoff_counter == 0 {
    //                 // For the AP we must check both src_id and AC match packets in queue
    //                 if self.queue.iter().any(|p| {
    //                     let tid = if p.sta_src_id > p.sta_dest_id { p.sta_src_id } else { -1 };
    //                     (tid, p.edca_ac) == *key
    //                 }) {
    //                     ready.push(*key);
    //                 }
    //             }
    //         }
    //     }
    //     ready
    // }

    pub async fn input(&mut self, mut pkt: MpduPacket, ctx: &Context<Self>) {
        let now = ctx.scheduler.time();
        pkt.queue_in_instant = now;
        self.arrived_packet_counter += 1;


        if self.queue.len() < self.queue_maxsize {
            self.queue.push_back(pkt);
            if !self.packet_being_served && self.shared_medium.is_idle(now) {
                self.deque_schedule_service((), ctx).await;
            }
        } else {
            self.blocked_packet_counter += 1;
            debug_bgprint!(DebugColor::Green, "[QUEUE FULL] dropping packet {}", pkt.packet_id);
        }
    }

    pub async fn input_UL(&mut self, mut packet: MpduPacket, context: &Context<Self>) {
        self.arrived_packet_counter += 1;
        self.queue_length_counter += self.queue.len();

        let now = context.scheduler.time();
        if self.queue.len() < self.queue_maxsize {
            packet.queue_in_instant = now;
            self.queue.push_back(packet.clone());

            debug_print!(
                DebugColor::Green,
                "{} [DBG QUEUE UL] -Packet {} arrives from STA{} destined to STA{}, Q_size = {}",
                format_elapsed!(now),
                packet.packet_id,
                packet.sta_src_id,
                packet.sta_dest_id,
                self.queue.len()
            );

            if self.queue.len() == 1 && !self.packet_being_served && self.shared_medium.is_idle(now){
                self.deque_schedule_service((), context).await;
            }
        } else {
            self.blocked_packet_counter += 1;
            print_red!(
                // DebugColor::Red,
                "{} [DBG FULL QUEUE] Packet {} (S: {}, D: {})DROPPED from QUEUEMODULE!! , Q_size = {}",
                format_elapsed!(now),
                packet.packet_id,
                packet.sta_src_id,
                packet.sta_dest_id, 
                self.queue.len()
            );
        }
    }

    pub async fn send_ampdu(&mut self, AMPDU_sent: AmpduPacket, context: &Context<Self>) {
        let elapsed = context.scheduler.time();
        self.shared_medium.release_txop(elapsed);

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

        self.packet_being_served = false;

        let mac_key = AMPDU_sent.mac_key; 
        self.output_port_sta1.send(AMPDU_sent).await;

        if self.queue.len() > 0 {
            self.deque_schedule_service((), context).await;
            // context.scheduler.schedule_event(Duration::from_nanos(10), Self::deque_schedule_service, ()).unwrap();
        }
        if let Some(st) = self.array_dcf_values.lock().unwrap().get_mut(&mac_key) { st.on_success(st.param.cw_min); }

        if let Some(stats_rx) = self.stats_rx.as_mut() {
            // print!("OK1,");
            if let Ok(mut queue_stats) = self.cumulative_stats_queue.lock() {
                // print!("OK2,");

                if let Ok(_array_STAs_stats) = self.array_stas_stats.lock() {
                    // print!("OK3,");

                    while let Ok(stats_update) = stats_rx.try_recv() {
                        // println!("OK CUM");
                        queue_stats.update_cumstats(
                            stats_update.T_s,
                            stats_update.T_q,
                            stats_update.blocked_packet_counter,
                            stats_update.arrived_packet_counter,
                            stats_update.queue_length_when_out,
                        );

                        // println!("OK STATS");
                        self.csv_metrics.update_stats(
                            stats_update.now,
                            stats_update.packet_id as usize,
                            stats_update.queue_length_when_out,
                            stats_update.T_s,
                            stats_update.T_q,
                            stats_update.length_packet,
                            stats_update.sta_src_id,
                            stats_update.sta_dest_id,
                            stats_update.ampdu_id, 
                            stats_update.is_collision, 
                            stats_update.collision_backoff, 
                        );
                    }
                }
            }
        }
    }

    fn select_next_sta(&self) -> HashMap<(i32, i32), StaRateInfo> {
        let mut sta_packets: HashMap<(i32, i32), StaRateInfo> = HashMap::new();

        // Iterate over packets in the queue to compute STA metrics
        for packet in self.queue.iter() {
            let key = (packet.sta_src_id, packet.sta_dest_id);
            
            let is_ul = packet.sta_src_id > packet.sta_dest_id; 
            let mac_key_edca = if is_ul {
                (packet.sta_src_id, packet.edca_ac)
            } else {
                (-1, packet.edca_ac)
            };

            // Calculate transmission delay for a single packet
            let resultz = airtime_ampdu(
                packet.length_packet as f64,
                1,
                self.coords_queue,
                packet.sta_src_coords,
                self.p_tx,
            );
            
            let cap_s_edca = self.txop_cap_secs(&mac_key_edca);

            // Binary search to find the maximum number of packets that fit within T_MAX_AGG
            let mut low = 1;
            let mut high = MAX_AMPDU_SIZE;
            let mut optimal_n_packets = 0;
            let mut resultz_full_ampdu = airtime_ampdu(
                packet.length_packet as f64 * high as f64,
                high,
                self.coords_queue,
                packet.sta_src_coords,
                self.p_tx,
            );

            while low <= high {
                let mid = (low + high) / 2;
                let test_resultz = airtime_ampdu(
                    packet.length_packet as f64 * mid as f64,
                    mid,
                    self.coords_queue,
                    packet.sta_src_coords,
                    self.p_tx,
                );

                if test_resultz <= DEFAULT_TMAX_AGG || test_resultz <= cap_s_edca {
                    optimal_n_packets = mid;
                    resultz_full_ampdu = test_resultz;
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }

            // Update STA rate information
            let entry = sta_packets.entry(key).or_insert(StaRateInfo {
                total_transmission_delay_single: 0.0,
                total_transmission_delay_fullampdu: 0.0,
                fullampdu_max_size: 0,
                packet_count: 0,
                weighted_rate_single: 0.0,
                weighted_rate_fullampdu: 0.0,
                per_packet_channel_access_efficiency: 0.0,
                expected_queue_delivery_ms: 0.0,
            });

            entry.total_transmission_delay_single = resultz;
            entry.total_transmission_delay_fullampdu = resultz_full_ampdu;
            entry.fullampdu_max_size = optimal_n_packets as usize;
            entry.packet_count += 1;
            entry.weighted_rate_single =
                0.2 * resultz + 0.8 * entry.weighted_rate_single;
            entry.weighted_rate_fullampdu =
                0.2 * resultz_full_ampdu+ 0.8 * entry.weighted_rate_fullampdu;
            entry.per_packet_channel_access_efficiency =
                entry.total_transmission_delay_fullampdu / entry.fullampdu_max_size as f64;
            entry.expected_queue_delivery_ms =
                entry.per_packet_channel_access_efficiency * entry.packet_count as f64 * 1000.0;
        }
        sta_packets
    }

    fn build_new_ampdu<'a>(
        &mut self,
        first_packet: &MpduPacket,
        now: TaiTime<0>,
    ) -> (AmpduPacket, Duration)  {

            let mut success_indices: Vec<usize> = Vec::new(); // Original indices of packets successfully transmitted.

            let (sta_src_id, sta_dest_id ) = (first_packet.sta_src_id, first_packet.sta_dest_id) ; 

            self.aux_ampdu_serviced.reset();
            self.aux_ampdu_serviced.sta_dest_id = first_packet.sta_dest_id;
            self.aux_ampdu_serviced.sta_src_id = first_packet.sta_src_id;
            self.aux_ampdu_serviced.coordinates = first_packet.sta_src_coords.clone();


            let is_ul = first_packet.sta_src_id > first_packet.sta_dest_id; 
            let mac_key: MacKey = if is_ul {
                (first_packet.sta_src_id, first_packet.edca_ac)
            } else {
                (-1, first_packet.edca_ac)
            };
            self.aux_ampdu_serviced.mac_key = mac_key;
            
            // let key      = (first_packet.sta_src_id, first_packet.edca_ac);
            if DEBUG_EDCA{print_red!("Building AMPDU for key = {:?}", mac_key);} 

            let txop_us: f64 = self.array_dcf_values
                .lock().unwrap()
                .get(&mac_key)
                .map(|st| st.param.txop_limit_us as f64)
                .unwrap_or(0.0);

            let mut last_service_duration = Duration::default();
            let mut packet_index = 0;
            let mut resultz = 0.0;

            // ********** Modification: Duplicate packets instead of removing them **********
            while packet_index < self.queue.len() {
                if let Some(current_packet) = self.queue.get(packet_index) {
                    if current_packet.sta_dest_id != self.aux_ampdu_serviced.sta_dest_id
                        || current_packet.sta_src_id != self.aux_ampdu_serviced.sta_src_id
                    {
                        packet_index += 1;
                        continue;
                    }
                    let new_total_length =
                        self.aux_ampdu_serviced.total_length + current_packet.length_packet;
                    let new_size = self.aux_ampdu_serviced.size + 1;
                    let is_ul = current_packet.sta_src_id > current_packet.sta_dest_id; // 
                    
                    if is_ul{
                        resultz = airtime_ampdu(
                        new_total_length as f64,
                        new_size,
                        self.coords_queue,
                        current_packet.sta_src_coords.clone(),
                        P_TX,
                        );
                    }
                    else{
                        let dest_coords = self
                            .STA_coords_map
                            .get(&(current_packet.sta_dest_id as usize))
                            .unwrap_or_else(|| panic!(
                                "no coordinates for STA {}",
                                current_packet.sta_dest_id
                            ))
                            .clone();

                        // println!("DOWNLINK so coords are {:?}", dest_coords.x); 
                        resultz = airtime_ampdu(
                            new_total_length as f64,
                            new_size,
                            self.coords_queue,
                            dest_coords,
                            P_TX,
                        );
                    }
                    let cap_s_edca = self.txop_cap_secs(&mac_key);

                    if resultz >= DEFAULT_TMAX_AGG || new_size > MAX_AMPDU_SIZE || resultz >= cap_s_edca {
                        // print_red!(
                        //     // DebugColor::DarkRed,
                        //     "AMPDU full ({} / {}) or delay too high: {:.3} out of {:.3} ms (EDCA_AC: {:?} )",
                        //     new_size,
                        //     MAX_AMPDU_SIZE,
                        //     resultz * 1000.0,
                        //     f64::min(DEFAULT_TMAX_AGG * 1000.0, cap_s_edca * 1000.0), 
                        //     mac_key.1, 
                        // );
                        break;
                    }

                    // Instead of removing the packet, clone it and update clone metadata.
                    let mut cloned_packet = current_packet.clone();
                    cloned_packet.original_index = packet_index; // record original index
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
                            length_packet: cloned_packet.length_packet,
                            ampdu_id: self.ampdu_id, 

                            is_collision: false,
                            collision_backoff: 0.0, 
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
            // **********************************************************************************

            // Process AMPDU packets: simulate transmission errors
            let mut new_ampdu_packets: Vec<MpduPacket> = Vec::new();
            let mut rng = rand::thread_rng();
            for mut packet in self.aux_ampdu_serviced.mpdu_packets.drain(..) {

                packet.T_s = Duration::from_secs_f64(resultz);   // Assign transmission delay of full AMPDU                       
                let random_value: f64 = rng.gen();

                if random_value <= self.PL_probability {

                    debug_debug!( DebugColor::SaddleBrown, 
                        "{:.6} [DBG QUEUE] - packet from {} to {} with errors in MAC layer: Packet_ID: {}| ALVR S: {}/{} F: {}| Index in Q: {}",
                        format_elapsed!(now),
                        packet.sta_src_id,
                        packet.sta_dest_id,
                        packet.packet_id,

                        packet.header_alvr.shard_index,
                        packet.header_alvr.shards_count,
                        packet.header_alvr.next_packet_index, 
                        packet.original_index
                    );
                    self.blocked_packet_counter += 1;
                    // Packet encountered an error, so we leave its original copy in the queue.
                    // Optionally, we could log it or mark it.
                } else {
                    new_ampdu_packets.push(packet);
                    
                }
            }
            self.aux_ampdu_serviced.mpdu_packets = new_ampdu_packets;
                
            // Now remove from the main queue the packets that were transmitted successfully.
            // Gather all original indices from the AMPDU (successful ones).
            for packet in &self.aux_ampdu_serviced.mpdu_packets {
                success_indices.push(packet.original_index);
            }
            // Remove indices in descending order to avoid index shift.
            success_indices.sort_unstable_by(|a, b| b.cmp(a));
            for idx in success_indices {
                // Safety: ensure index is valid.
                if idx < self.queue.len() {
                    self.queue.remove(idx);
                }
            }

            if DEBUG_PRINT_ENABLED {
                print_yellow!(
                    "{} [DBG AMPDU] --Dequeueing AMPDU, serviced at {}",
                    format_elapsed!(now),
                    format_elapsed!(now + last_service_duration)
                );
                self.aux_ampdu_serviced.print();
            }


            self.packet_being_served = true;
            let ampdu_to_send =
                std::mem::replace(&mut self.aux_ampdu_serviced, AmpduPacket::new()); // clean up self, retrieve obtained ampdu
            (ampdu_to_send, last_service_duration) 
        }


    fn deque_schedule_service<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let now = context.scheduler.time();
            let mut lost_packets: Vec<(usize, MpduPacket)> = Vec::new(); // Unused now; kept for debug if needed.
            // Idea: Given arbitrary random traffic patterns that might lead to queue bufferbloat on some STAs, 
            // select first packet fairly to ensure channel access with reduced backlog for each user.
            let sta_packets: HashMap<(i32, i32), StaRateInfo> = self.select_next_sta();
    
            // debug_schedule!(
            //     // DebugColor::Cyan, 
            //     "{} | ***************** SCHEDULING *******************",
            //     format_elapsed!(now)
            // );
    
            let mut ul_stas = HashSet::new();
            let mut is_dl : bool = false; 

        
            for ((sta_src, sta_dest), packets) in sta_packets.iter() {
               
                let is_ul = sta_src > sta_dest;
                if is_ul {
                    ul_stas.insert(*sta_src);        // count each UL-STA once
                } else {
                    is_dl = true; 
                }
                
                debug_debug!(
                    DebugColor::Cyan, 
                    "src: {}, dest: {} | UL_FLOW: {} |queue_packets: {} | N_max_ampdu={}, T_s_full = {:.3} ms, EWMA(T_s_full) = {:.3} ms ",
                    sta_src, sta_dest, is_ul, packets.packet_count,
                    packets.fullampdu_max_size,
                    packets.total_transmission_delay_fullampdu * 1000.0,
                    packets.weighted_rate_fullampdu * 1000.0
                );
    
                if is_ul && packets.packet_count > self.ul_capacity_queue_device {
                    print_red!(
                        // DebugColor::Red,
                        "{} [UL CAPACITY EXCEEDED] STA {} -> AP {}: {} packets (max: {})",
                        format_elapsed!(now),
                        sta_src,
                        sta_dest,
                        packets.packet_count,
                        self.ul_capacity_queue_device
                    );
                    let excess_count = packets.packet_count - self.ul_capacity_queue_device;
                    let mut packets_to_remove = excess_count;
                    let mut indices_to_remove = Vec::new();
                    // Scan from the back of the queue (newest packets first)
                    for i in (0..self.queue.len()).rev() {
                        if let Some(packet) = self.queue.get(i) {
                            if packet.sta_src_id == *sta_src && packet.sta_dest_id == *sta_dest {
                                indices_to_remove.push(i);
                                packets_to_remove -= 1;
                                if packets_to_remove == 0 {
                                    break;
                                }
                            }
                        }
                    }

                    
                    // Remove identified packets (from back to front to avoid index issues)
                    for idx in indices_to_remove {
                        if let Some(removed_packet) = self.queue.remove(idx) {
                            print_red!(
                                // DebugColor::Red,
                                "{} [UL PACKET DROPPED] Packet_ID: {}, SRC: {}, DST: {}",
                                format_elapsed!(now),
                                removed_packet.packet_id,
                                removed_packet.sta_src_id,
                                removed_packet.sta_dest_id, 
                            );
                            self.blocked_packet_counter += 1;
                        }
                    }
                }
            }
            
            let mut pre_contenders: Vec<MacKey> = self.tick_backoff(now);

            // First resolve virtual collisions (same STA different ACs)
            let contenders = self.resolve_virtual_collision(pre_contenders);
            
            if contenders.is_empty() {
                // Only tick per-slot if the medium is idle (otherwise we already scheduled at busy end)
                if self.shared_medium.is_idle(now) {
                    let mut need_next_slot = false;
                    if let Ok(map) = self.array_dcf_values.lock() {
                        for (key, st) in map.iter() {
                            if ac_needs_tick(key, st, &self.queue, now) { need_next_slot = true; break; }
                        }
                    }
                    if need_next_slot {
                        context.scheduler
                            .schedule_event(Duration::from_secs_f64(SLOT), Self::deque_schedule_service, ())
                            .unwrap();
                    }
                }
                return;
            }


            // Now handle physical collisions between different STAs
            let collision_now = contenders.len() > 1;
                            
            let mut selected_sta = None;
            if LYAPUNOV_POLICY == true {
                let mut min_priority = f64::MAX;
                debug_schedule!(
                    "T {:.5} LYAPUNOV Drift-plus-Penalty scheduling policy:",
                    format_elapsed!(now)
                );
                for (key, info) in sta_packets.iter() {
                    let lhs = LYAPUNOV_V * info.per_packet_channel_access_efficiency;
                    let rhs = info.expected_queue_delivery_ms;
                    let priority: f64 = if info.packet_count >= MAX_AMPDU_SIZE as usize {
                        lhs - rhs
                    } else {
                        1E12 as f64
                    };
                    let is_ul = if key.0 > key.1 {1} else {0};
                    debug_schedule!(
                        "Q_{:.0} = {} -> Priority STA{:.0} = ({:.3}) == {} - {} | is_ul = {} (src: {} dest: {}) ",
                        key.0,
                        info.packet_count,
                        key.0,
                        priority,
                        lhs,
                        rhs,
                        is_ul,
                        key.0,
                        key.1,
                    );
                    if priority < min_priority {
                        min_priority = priority;
                        selected_sta = Some(*key);
                    }
                }
            } else if SOFTMAX_POLICY == true {
                debug_schedule!("SOFTMAX POLICY", );
                let mut softmax_values: Vec<f64> = Vec::new();
                let mut softmax_keys: Vec<(i32, i32)> = Vec::new();
                for ((sta_src, sta_dest), packets) in sta_packets.iter() {
                    let _is_ul = if sta_src > sta_dest {1} else {0};
                    debug_schedule!(
                        "IS_UL = {} | ( src: {}, dest: {} )",
                        _is_ul,
                        sta_src,
                        sta_dest
                    );
                    softmax_values.push(packets.expected_queue_delivery_ms);
                    softmax_keys.push((*sta_src, *sta_dest));
                }
                pub const SOFTMAX_TEMP: f64 = 1000.0;
                let softmax_probs = softmax_with_temperature(&softmax_values, SOFTMAX_TEMP);
                let mut rng = rand::thread_rng();
                let selected_index = softmax_probs
                    .iter()
                    .position(|&p| (1.0 - p) > rng.gen::<f64>())
                    .unwrap_or(softmax_probs.len() - 1);
                let selected_key = softmax_keys[selected_index];
                selected_sta = Some(selected_key);
            } else {
                // NORMAL POLICY: FIFO
            }

            if collision_now {
                if let Some(key) = selected_sta {
                    selected_sta = Some(key);
                } else if !self.queue.is_empty() {
                    let first = self.queue.front().unwrap();
                    selected_sta = Some((first.sta_src_id, first.sta_dest_id));
                }
                if let Some(_sta_key) = selected_sta {

                    // In collision, we do not remove or duplicate any packets.
                    // Instead, we simply schedule a retransmission backoff.
                    let T_col: f32 = collision_delay(); 
                    let T_col_dur = Duration::from_secs_f32(T_col);

                    self.shared_medium.occupy_collision(now + T_col_dur);
                  
                    // print_red!("{} | **************[COLLISION]**********\ncontenders={} -> busy_until={}, owner=None",
                    //     format_elapsed!(now),
                    //     contenders.len(),
                    //     format_elapsed!(now + T_col_dur));
                  
                    for key in contenders {
                       if let Some(st) = self.array_dcf_values.lock().unwrap().get_mut(&key) {
                           st.on_failure(); // increases CW and redraws backoff
                       }
                    }       

                    if let Some(stats_tx) = &self.stats_tx {
                            let stats_update = StatsUpdate {
                                T_s: 0.0,
                                T_q: 0.0,
                                blocked_packet_counter: self.blocked_packet_counter,
                                arrived_packet_counter: self.arrived_packet_counter,
                                queue_length_when_out: 0,
                                sta_src_id: _sta_key.0 as usize,
                                sta_dest_id: _sta_key.1 as usize,
                                packet_id: 0,
                                now,
                                length_packet: 0,
                                ampdu_id: self.ampdu_id, 

                                is_collision: true,
                                collision_backoff: T_col as f64, 
                            };

                            stats_tx
                                .send(stats_update)
                                .expect("Failed to send stats update");                    
                        }
                    
                    if let Ok(mut map) = self.array_dcf_values.lock() {
                        for (_, st) in map.iter_mut() {
                            st.backoff_frozen    = true;
                            st.medium_free_since = now + T_col_dur;   // AIFS will start from here
                        }
                    }


                    context
                        .scheduler
                        .schedule_event(T_col_dur, Self::deque_schedule_service, ())
                        .unwrap();
                    return; 
                }
            } else {

                let winner_key = contenders[0];
                let first_ix = match self.queue.iter().position(|p| maps_to(&winner_key, p)) {
                    Some(ix) => ix,
                    None => {
                        // Defensive: redraw backoff and try next slot
                        if let Some(st) = self.array_dcf_values.lock().unwrap().get_mut(&winner_key) {
                            st.on_success(st.param.cw_min); // post-backoff reload
                        }
                        print_red!("Not supposed to happn!!!!!", ); 
                        context.scheduler
                            .schedule_event(Duration::from_secs_f64(SLOT), Self::deque_schedule_service, ())
                            .unwrap();
                        return;
                    }
                };
                let first_packet = self.queue.get(first_ix).cloned().unwrap();
                // Build AMPDU **only** from packets that map to the same MacKey
                let (ampdu_to_send, ampdu_airtime) = self.build_new_ampdu(&first_packet, now);

                // Occupy the medium for the TXOP airtime
                // self.shared_medium.occupy_until(now + ampdu_airtime);

                self.shared_medium.start_txop( now + ampdu_airtime, winner_key);
                // crate::print_green!("{} | [TXOP START] owner={:?} until={}", format_elapsed!(now), winner_key, format_elapsed!(now + ampdu_airtime));

                // Freeze everyone while TXOP is in progress and record when medium will stop being busy
                if let Ok(mut map) = self.array_dcf_values.lock() {
                    for (_, st) in map.iter_mut() {
                        st.backoff_frozen = true;
                        st.medium_free_since = now + ampdu_airtime;
                    }
                }

                                // Schedule the TX completion and the next contention exactly at TX end
                context.scheduler
                    .schedule_event(ampdu_airtime, Self::send_ampdu, ampdu_to_send)
                    .unwrap();

                return; 

            }
        
            let mut need_next_slot = false;
            if let Ok(map) = self.array_dcf_values.lock() {
                for (key, st) in map.iter() {
                    if ac_needs_tick(key, st, &self.queue, now) {
                        need_next_slot = true;
                        break;
                    }
                }
            }

            if need_next_slot {
                context.scheduler
                    .schedule_event(Duration::from_secs_f64(SLOT),
                                    Self::deque_schedule_service,
                                    ())
                    .unwrap();
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
                data.av_l += packet.length_packet as f64;
                data.rx_packets_counter += 1;
                data.last_time = taitime_to_f64!(now);
            }

            self.received_packet_counter += 1;
        }
    }
}

impl Model for Sink {}
