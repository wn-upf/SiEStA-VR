// !```text
// !                     ┌────────────────────────────────────────────────────┐
// !                     │                                                    │
// !                     │                   Packet Flow                      │
// !                     │   ┌──────────────┐                ┌──────────────┐ │
// !    MpduPacket   ●──►│──►│ PoissonSource├───────────────►│ QueueModule  ├──► AmpduPacket
// !                     │   │              │    output_port │              │ │    output_port
// !                     │   └──────────────┘                └──────────────┘ │
// !                     │                                                    │
// !                     └────────────────────────────────────────────────────┘
// !```
use asynchronix::model::{Context, Model};
use asynchronix::ports::Output;
use asynchronix::simulation::{Mailbox, SimInit};
use asynchronix::time::MonotonicTime;
use colored::*;
use rand::Rng;
use std::cmp::{self};
use std::cmp::{max, min};
use std::collections::VecDeque;
use std::env;
use std::f64::consts::PI;
use std::future::Future;
use std::time::{Duration, Instant};
use tracing_subscriber::registry::Data;

use std::sync::{Arc, Mutex};

mod libs; // for calling m own local library
use crate::libs::{
    compute_mm1k_metrics, exponential, frametransmission_delay, perStaLockStats, perStaStats,
    write_all_sta_csvs, Coords, CsvType, CumulativeStats, ResultsFrameTXDelay, DEFAULT_TMAX_AGG,
    MAX_AMPDU_SIZE, P_TX,
};

use crate::libs::{AmpduPacket, MpduPacket};

// Define a constant to control debugging
const DEBUG_PRINT_ENABLED: bool = true; // Change to false to disable

#[macro_export]
macro_rules! debug_print {
    ($color:expr, $fmt:expr, $($arg:tt)*) => {
        // Check if debugging is enabled
        if DEBUG_PRINT_ENABLED {
            let msg = format!($fmt, $($arg)*);
            println!("{}", $color.to_color_fn()(msg));
        }
    };
}
#[macro_export]
macro_rules! format_elapsed {
    ($elapsed:expr) => {{
        let total_seconds =
            $elapsed.as_secs() as f64 + ($elapsed.subsec_nanos() as f64 / 1_000_000_000.0);
        format!("{:.9}", total_seconds)
    }};
}
#[macro_export]
macro_rules! taitime_to_f64 {
    ($tai:expr) => {{
        let secs = $tai.as_secs() as f64;
        let nanos = $tai.subsec_nanos() as f64;
        secs + (nanos / 1_000_000_000.0)
    }};
}

pub enum DebugColor {
    Red,
    Green,
    Blue,
    Yellow,
    Magenta,
}

impl DebugColor {
    fn to_color_fn(&self) -> fn(String) -> colored::ColoredString {
        match self {
            DebugColor::Red => |s| s.red(),
            DebugColor::Green => |s| s.green(),
            DebugColor::Blue => |s| s.blue(),
            DebugColor::Yellow => |s| s.yellow(),
            DebugColor::Magenta => |s| s.magenta(),
        }
    }
}

pub trait DebugPrint {
    fn print_debug(&self, color: DebugColor, prefix: &str);
}

impl DebugPrint for MpduPacket {
    fn print_debug(&self, color: DebugColor, prefix: &str) {
        debug_print!(
            color,
            "[{}] Packet ID: {}, Length: {}",
            prefix,
            self.packet_id,
            self.length_packet
        );
    }
}
pub struct PoissonSource {
    pub arrival_rate: f64,
    pub mean_length_packets: f64,

    pub output_port: Output<MpduPacket>,

    pub num_packets_sent: usize,
}

impl PoissonSource {
    pub fn new(arrival_rate_bps: f64, mean_length: f64) -> Self {
        let arrival_rate = arrival_rate_bps / mean_length;
        Self {
            arrival_rate: arrival_rate,
            mean_length_packets: mean_length,
            output_port: Default::default(),
            num_packets_sent: 0,
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
                Duration::from_secs_f64(exponential(1.0 / self.arrival_rate));
            time_interarrival = max(time_interarrival, Duration::from_nanos(10));

            let len_random = exponential(self.mean_length_packets as f64) as usize;
            packet.length_packet = cmp::max(1, len_random);

            self.num_packets_sent += 1;
            packet.packet_id = self.num_packets_sent;
            self.output_port.send(packet.clone()).await;

            context
                .scheduler
                .schedule_event(time_interarrival, Self::send_packet, ())
                .unwrap();
        }
    }
}

impl Model for PoissonSource {}

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
}

impl STA_source {
    pub fn new(
        arrival_rate_bps: f64,
        mean_length: f64,
        src: i32,
        dest: i32,
        coordinates: Coords,
        does_STA_transmit: bool,
    ) -> Self {
        let arrival_rate = arrival_rate_bps / mean_length;

        Self {
            output_port: Default::default(),

            sta_id: src,
            destination_id: dest,
            arrival_rate: arrival_rate,
            mean_length_packets: mean_length,
            num_packets_sent: 0,
            sta_coordinates: coordinates,

            received_packet_counter: 0,
            does_sta_tx: does_STA_transmit,
        }
    }

    pub fn move_coordinates(&mut self, distance_to_move: f64) {
        // brownian movement for STA
        let mut rng = rand::thread_rng();

        // Generate a random angle in spherical coordinates to determine the direction of movement
        let theta = rng.gen_range(0.0..2.0 * PI); // azimuthal angle for x and y
        let phi = rng.gen_range(0.0..PI); // polar angle for z-axis

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
                "{} [DBG STA{} IN]  ---Packet {} arrived from STA{} into STA{}",
                format_elapsed!(elapsed),
                self.sta_id,
                packet.packet_id,
                packet.sta_src_id,
                packet.sta_dest_id,
            );

            self.received_packet_counter += 1;
        }
    }

    fn send_packet<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            if self.does_sta_tx {
                // if STA is "TX type"         (and not "RX only")

                let mut packet = MpduPacket::new();

                let mut time_interarrival =
                    Duration::from_secs_f64(exponential(1.0 / self.arrival_rate));
                time_interarrival = max(time_interarrival, Duration::from_nanos(10));

                let len_random = exponential(self.mean_length_packets as f64) as usize;
                packet.length_packet = cmp::max(1, len_random);
                packet.packet_id = self.num_packets_sent;

                packet.sta_src_id = self.sta_id;
                packet.sta_dest_id = self.destination_id;

                self.output_port.send(packet.clone()).await;
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
    waiting_time_cum: CumulativeStats,
    service_time_cum: CumulativeStats,

    num_packets_dropped: i32,
    num_packets_rx: i32,
}
impl QueueStats {
    pub fn new() -> Self {
        Self {
            waiting_time_cum: CumulativeStats::new(),
            service_time_cum: CumulativeStats::new(),
            num_packets_dropped: 0,
            num_packets_rx: 0,
        }
    }
    pub fn update_cumstats(&mut self, ts: f64, tq: f64, packet_drops: i32, packets_rx: i32) {
        self.waiting_time_cum.add(tq);
        self.service_time_cum.add(ts);
        self.num_packets_dropped = packet_drops;
        self.num_packets_rx = packets_rx;
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
        println!("{}", separator);
        println!(
            "{:^2}",
            "| QUEUE MODULE                                   |"
        );
        println!("{}", separator);

        // Print statistics
        println!("{}", format_row("P_k (Blocking Probability)", p_k));
        println!("{}", format_row("E[N_q]", 0.0)); // Placeholder - needs implementation
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

#[derive(Clone)]
pub struct QueueModule {
    pub output_port: Output<AmpduPacket>,

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
    pub rate_departures_bps: f64,
    pub t0_time: Instant,

    pub csv_metrics: CsvType,

    pub coords_queue: Coords,
    pub p_tx: f64,

    pub STA_coords_grid: Vec<Coords>,

    pub cumulative_stats_queue: Arc<Mutex<QueueStats>>,
    pub array_stas_stats: Arc<Mutex<Vec<perStaLockStats>>>,
}

impl QueueModule {
    pub fn get_queue_stats_handle(&self) -> Arc<Mutex<QueueStats>> {
        self.cumulative_stats_queue.clone()
    }

    pub fn get_stas_stats_handle(&self) -> Arc<Mutex<Vec<perStaLockStats>>> {
        self.array_stas_stats.clone()
    }

    pub fn new(num_stas: usize, queue_size: usize, rate_departures_bps: f64) -> Self {
        // Create a vector of perStaLockStats with initialized sta_ids
        let mut stats_vec = Vec::with_capacity(num_stas);
        for i in 0..num_stas {
            let mut sta_stats = perStaLockStats::new();
            // We need to lock the mutex to modify the sta_id
            if let Ok(mut stats) = sta_stats.data.lock() {
                stats.sta_id = i as i32;
            }
            stats_vec.push(sta_stats);
        }

        Self {
            queue: VecDeque::new(),
            queue_maxsize: queue_size,
            output_port: Default::default(),
            service_timer: Duration::ZERO,
            aux_ampdu_serviced: AmpduPacket::new(),
            packet_being_served: false,
            blocked_packet_counter: 0,
            arrived_packet_counter: 0,
            queue_length_counter: 0,
            service_rate: 0.0,
            rate_departures_bps,
            t0_time: Instant::now(),

            csv_metrics: CsvType::new(),

            coords_queue: Coords::new(),
            p_tx: 20.0,
            STA_coords_grid: Vec::new(),
            cumulative_stats_queue: Arc::new(Mutex::new(QueueStats::new())),
            array_stas_stats: Arc::new(Mutex::new(stats_vec)),
        }
    }

    pub async fn input(&mut self, mut packet: MpduPacket, context: &Context<Self>) {
        self.arrived_packet_counter += 1;
        self.queue_length_counter += self.queue.len();

        let now = context.scheduler.time();
        if self.queue.len() < self.queue_maxsize {
            packet.queue_in_instant = now;
            self.queue.push_back(packet);

            debug_print!(
                DebugColor::Blue,
                "{} [DBG QUEUE] -Packet {} arrives from STA{} destined to STA{}, Q_size = {}",
                format_elapsed!(now),
                packet.packet_id,
                packet.sta_src_id,
                packet.sta_dest_id,
                self.queue.len()
            );

            if self.queue.len() == 1 && !self.packet_being_served {
                self.deque_schedule_service((), context).await;
            }
        } else {
            self.blocked_packet_counter += 1;
            let elapsed = context.scheduler.time();
            debug_print!(
                DebugColor::Red,
                "{} [DBG FULL QUEUE] Packet {} DROPPED!! , Q_size = {}",
                format_elapsed!(elapsed),
                packet.packet_id,
                self.queue.len()
            );
        }
    }

    pub async fn send_ampdu(&mut self, AMPDU_sent: AmpduPacket, context: &Context<Self>) {
        let elapsed = context.scheduler.time();
        debug_print!(
            DebugColor::Magenta,
            "{} [DBG SERVE] --AMPDU sent to STA {} with {} packets inside, Q_size = {}",
            format_elapsed!(elapsed),
            AMPDU_sent.sta_id,
            AMPDU_sent.mpdu_packets.len(),
            self.queue.len()
        );
        // AMPDU_sent.print();
        self.packet_being_served = false;
        self.output_port.send(AMPDU_sent).await;
        self.aux_ampdu_serviced.reset();

        if self.queue.len() > 0 {
            self.deque_schedule_service((), context).await;
        }
    }

    fn deque_schedule_service<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            if let Some(first_packet) = self.queue.front() {
                let now: tai_time::TaiTime<0> = context.scheduler.time();

                // Initialize AMPDU with first packet's info (but don't remove it yet)
                self.aux_ampdu_serviced.reset();
                self.aux_ampdu_serviced.sta_id = first_packet.sta_dest_id;
                self.aux_ampdu_serviced.coordinates = first_packet.sta_dest_coords.clone();
                self.aux_ampdu_serviced.size = 1; // start at 1

                let mut index = 0;
                let mut last_service_duration = Duration::default();

                while index < self.queue.len() {
                    // Get packet info before any modifications
                    let (matches_sta_id, packet_length) =
                        if let Some(current_packet) = self.queue.get(index) {
                            (
                                current_packet.sta_dest_id == self.aux_ampdu_serviced.sta_id,
                                current_packet.length_packet,
                            )
                        } else {
                            break;
                        };

                    if !matches_sta_id {
                        // Skip packets not matching AMPDU's STA_ID
                        println!("skip {}", index);
                        index += 1;
                        continue;
                    }

                    // Check AMPDU constraints before adding packet
                    let resulting_delays = frametransmission_delay(
                        self.aux_ampdu_serviced.total_length as f64,
                        self.aux_ampdu_serviced.size,
                        self.coords_queue,
                        self.aux_ampdu_serviced.coordinates,
                        self.p_tx,
                    );

                    if resulting_delays.service_delay >= DEFAULT_TMAX_AGG
                        || self.aux_ampdu_serviced.size >= MAX_AMPDU_SIZE as i32
                    {
                        debug_print!(
                            DebugColor::Yellow,
                            "{} [DBG AMPDU END] T_s = {} / {} ; SIZE = {} / {}",
                            format_elapsed!(now),
                            resulting_delays.service_delay,
                            DEFAULT_TMAX_AGG,
                            self.aux_ampdu_serviced.size,
                            MAX_AMPDU_SIZE
                        );
                        break;
                    }

                    // Remove packet and add to AMPDU
                    if let Some(mut packet) = self.queue.remove(index) {
                        packet.queue_out_instant = now;

                        debug_print!(
                            DebugColor::Yellow,
                            "{} [DBG DEQUE] --Packet {} (STA{}) dequed and put in AMPDU, Iter index: {}, Q_size = {}",
                            format_elapsed!(now),
                            packet.packet_id,
                            packet.sta_dest_id,
                            index + 1,
                            self.queue.len(),
                        );

                        self.aux_ampdu_serviced.mpdu_packets.push(packet);
                        self.aux_ampdu_serviced.total_length += packet_length;
                        self.aux_ampdu_serviced.size += 1;

                        last_service_duration =
                            Duration::from_secs_f64(resulting_delays.service_delay);

                        // Don't increment index since we removed a packet
                    } else {
                        index += 1;
                    }
                }

                // Update all packets with the final service duration
                for packet in self.aux_ampdu_serviced.mpdu_packets.iter_mut() {
                    let packet_queue_time = packet
                        .queue_out_instant
                        .duration_since(packet.queue_in_instant);

                    packet.T_q = packet_queue_time;
                    packet.expected_T_s = last_service_duration;

                    let T_s_f64 = packet.expected_T_s.as_secs_f64();
                    let T_q_f64 = packet.T_q.as_secs_f64();

                    // UPDATE STATS
                    if let Ok(mut queue_stats) = self.cumulative_stats_queue.lock() {
                        // println!("[DEBUGDEBUGDEBU]!!!! T_s : {}, T_q : {} !", T_s_f64, T_q_f64);
                        queue_stats.update_cumstats(
                            T_s_f64,
                            T_q_f64,
                            self.blocked_packet_counter as i32,
                            self.arrived_packet_counter as i32,
                        );
                    }

                    if let Ok(array_STAs_stats) = self.array_stas_stats.lock() {
                        if let Some(stats) = array_STAs_stats.get(packet.sta_src_id as usize) {
                            if let Ok(mut stats_data) = stats.data.lock() {
                                // println!("DEBUG STA{} ", packet.sta_src_id);
                                stats_data.update_stats_per_sta(
                                    now,
                                    packet.packet_id,
                                    self.queue.len(),
                                    T_s_f64,
                                    T_q_f64,
                                    packet.length_packet,
                                );
                            }
                        }
                    }

                    self.csv_metrics.update_stats(
                        now,
                        packet.packet_id,
                        self.queue.len(),
                        packet.expected_T_s.as_secs_f64(),
                        packet_queue_time.as_secs_f64(),
                        packet.length_packet,
                    );
                }

                if !self.aux_ampdu_serviced.mpdu_packets.is_empty() {
                    debug_print!(
                        DebugColor::Yellow,
                        "{} [DBG AMPDU] --Dequeueing AMPDU, serviced at {}",
                        format_elapsed!(now),
                        format_elapsed!(now + last_service_duration),
                    );
                    self.packet_being_served = true;

                    context
                        .scheduler
                        .schedule_event(
                            last_service_duration,
                            Self::send_ampdu,
                            (self.aux_ampdu_serviced.clone()),
                        )
                        .unwrap();
                } else {
                    println!("?????????");
                }
            }
        }
    }
}

impl Model for QueueModule {}

#[derive(Clone, Default)]
pub struct DataSink {
    pub system_time: f64,
    pub av_l: f64,
    pub last_time: f64,
    pub rx_packets_counter: usize,
}
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
            let format_row =
                |label: &str, value: f64| format!("| {:<30} | {:>14.6} |", label, value);

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
                format_row("Avg Received Throughput[Mbps]", self.av_l / self.last_time)
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

impl Sink {
    pub fn new() -> Self {
        Self {
            received_packet_counter: 0,
            mutex_data: Arc::new(Mutex::new(DataSink::new())),
        }
    }

    pub fn get_data_handle(&self) -> Arc<Mutex<DataSink>>{
        Arc::clone(&self.mutex_data)
    }


    pub async fn input(&mut self, ampdu_packet: AmpduPacket, context: &Context<Self>) {
        let now = context.scheduler.time();

        for packet in ampdu_packet.mpdu_packets {
            debug_print!(
                DebugColor::Red,
                "{} [DBG SINK IN]  ---Packet {} arrived from STA{} into STA{}",
                format_elapsed!(now),
                packet.packet_id,
                packet.sta_src_id,
                packet.sta_dest_id,
            );
            // println!("{} - Packet received!!", format_duration(elapsed));
            // packet.print();

            let packet_total_time = now.duration_since(packet.queue_in_instant);

            if let Ok(mut data) = self.mutex_data.lock() {
                data.system_time += packet_total_time.as_secs_f64();
                data.av_l += packet.length_packet as f64;
                data.rx_packets_counter += 1; 
                data.last_time = taitime_to_f64!(context.scheduler.time()); 

                // println!(
                //     "dbgggggggggggg st: {}, av_l : {}, rx_c: {}, last_t: {}",
                //     data.system_time, data.av_l, data.rx_packets_counter, data.last_time
                // );
            }

            self.received_packet_counter += 1;
        }
    }
}

impl Model for Sink {}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////// SIMULATION ////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// fn simple_MM1K(
//     stoptime: f64,
//     mean_length: f64,
//     k_queue: usize,
//     rate_bps_in: f64,
//     rate_queue_bps: f64,
//     distance: f64,
// ) {

//     let num_STAs = 1;
//     let coords_sta = Coords {
//         x: distance,
//         y: 0.0,
//         z: 0.0,
//     };
//     let results = frametransmission_delay(
//         mean_length as f64,
//         MAX_AMPDU_SIZE,
//         Coords::new(),
//         coords_sta,
//         P_TX,
//     );

//     let effective_rate = mean_length / results.service_delay;

//     let LT = compute_mm1k_metrics(rate_bps_in, mean_length, effective_rate, k_queue);

//     let mut source: STA_source = STA_source::new(rate_bps_in, mean_length, 0, 2, coords_sta, true); // STAs 0
//     let mut queue: QueueModule = QueueModule::new(num_STAs ,k_queue - 1 as usize, rate_queue_bps);
//     let sink = Sink::new();

//     // mutex data handles to be able to access simulator variables, as csv vecs or CumulativeStats
//     let csv_data_handle: Arc<Mutex<libs::CsvData>> = queue.csv_metrics.get_data_handle();
//     let queuestats_data_handle= queue.get_queue_stats_handle();
//     let stats_sta_data_handle: Arc<Mutex<Vec<perStaLockStats>>> = queue.get_stas_stats_handle();

//     let mbox_src = Mailbox::new();
//     let mbox_src_address = mbox_src.address();

//     let mbox_queue = Mailbox::new();
//     let queue_address = mbox_queue.address();

//     let sink_mbox = Mailbox::new();
//     let sink_mbox_address = sink_mbox.address();

//     // CONNECT COMPONENTS
//     // source.output_port.connect(Sink::input, &sink_mbox);

//     source.output_port.connect(QueueModule::input, &mbox_queue);
//     queue.output_port.connect(Sink::input, &sink_mbox);

//     let t0 = MonotonicTime::EPOCH;

//     let mut simu = SimInit::new()
//         .add_model(source, mbox_src, "STA BG")
//         .add_model(queue, mbox_queue, "Queue")
//         .add_model(sink, sink_mbox, "Sink")
//         .init(t0);

//     let scheduler = simu.scheduler();

//     // ----------
//     // Simulation.
//     // ----------

//     // Check initial conditions.

//     let t = t0;

//     assert_eq!(simu.time(), t);

//     // START WITH FIRST EVENT
//     scheduler
//         .schedule_event(
//             Duration::from_millis(1),
//             STA_source::send_packet,
//             (),
//             &mbox_src_address,
//         )
//         .unwrap();

//     simu.step_by(Duration::from_secs_f64(stoptime)); //works

//     // After simulation, write the CSV data
//     if let Ok(data) = csv_data_handle.lock() {
//         if let Err(e) = data.write_to_csv() {
//             eprintln!("Failed to write CSV file: {}", e);
//         }
//     }

//     if let Ok(data) = stats_sta_data_handle.lock() {

//         if let Err(e) = write_all_sta_csvs(&data) {
//             eprintln!("Error writing STA CSV files: {}", e);
//         }
//     }

//     // println!("************ END RESULTS ***********\n LT: {:#?}", LT);
//     LT.print_results();

//     if let Ok(queue_stats) = queuestats_data_handle.lock() {
//         println!("Waiting time mean: {}", queue_stats.waiting_time_cum.get_average());
//         println!("Waiting time std dev: {}", queue_stats.waiting_time_cum.get_std_dev());
//         println!("Service time mean: {}", queue_stats.service_time_cum.get_average());
//         println!("Service time std dev: {}", queue_stats.service_time_cum.get_std_dev());
//     } else {
//         eprintln!("Failed to lock queue stats");
//     };

// }

// SCENARIO 2: TWO STAS AS BG TRAFFIC, 1 STA AS SINK
fn multiple_STA_sim(
    num_STAs: usize,
    stoptime: f64,
    mean_length: f64,
    k_queue: usize,
    rate_bps_in: f64,
    rate_queue_bps: f64,
    distance: f64,
) {
    let v_distance = vec![1.0, distance, distance]; // just some random values

    let coords_sta1 = Coords {
        x: v_distance[0],
        y: 0.0,
        z: 0.0,
    };
    let coords_sta2 = Coords {
        x: v_distance[1],
        y: 0.0,
        z: 0.0,
    };
    let coords_sta3 = Coords {
        x: v_distance[2],
        y: 0.0,
        z: 0.0,
    };

    let vec_coords = vec![coords_sta1, coords_sta2, coords_sta3];
    println!("vec_coords: {:?}", vec_coords);

    let results1 = frametransmission_delay(
        mean_length * MAX_AMPDU_SIZE as f64,
        MAX_AMPDU_SIZE,
        Coords::new(),
        coords_sta1,
        P_TX,
    );
    let results2 = frametransmission_delay(
        mean_length * MAX_AMPDU_SIZE as f64,
        MAX_AMPDU_SIZE,
        Coords::new(),
        coords_sta2,
        P_TX,
    );

    let effective_rate1 = mean_length / results1.service_delay;
    let effective_rate2 = mean_length / results2.service_delay;
    let effective_rate = (effective_rate1 + effective_rate2) / 2.0;

    println!("*******************************************************************"); 
    println!("Inputs--> rate: {}, l_mean :{}, effective_rate: {}, k: {}", rate_bps_in, mean_length, effective_rate, k_queue); 

    let LT = compute_mm1k_metrics(rate_bps_in, mean_length as f64, effective_rate, k_queue);

    let mut sta1_bg: STA_source =
        STA_source::new(rate_bps_in, mean_length, 0, 2, coords_sta1, true); // STAs 0 and 1 send traffic to 5 through AP
    let mut sta2_bg: STA_source =
        STA_source::new(rate_bps_in, mean_length, 1, 2, coords_sta2, true);

    // let sta5_ul: STA_source = STA_source::new(rate_bps_in, mean_length, 2, 7, coords_sta3, false); // RX STA, acts as sink with coordinates
    let sink: Sink = Sink::new();
    let mbox_sink: Mailbox<Sink> = Mailbox::new();

    let mut queue: QueueModule = QueueModule::new(num_STAs, k_queue - 1 as usize, rate_queue_bps);

    // mutex data handles to be able to access simulator variables, as csv vecs or CumulativeStats

    queue.STA_coords_grid.resize(num_STAs, Coords::new());
    for i in 0..num_STAs {
        queue.STA_coords_grid[i] = vec_coords[i];
    }

    let csv_data_handle = queue.csv_metrics.get_data_handle();
    let queuestats_data_handle: Arc<Mutex<QueueStats>> = queue.get_queue_stats_handle();
    let stats_sta_data_handle: Arc<Mutex<Vec<perStaLockStats>>> = queue.get_stas_stats_handle();
    let sinkstats_data_handle = sink.get_data_handle(); 



    let mbox_sta1 = Mailbox::new();
    let mbox_sta2 = Mailbox::new();

    let sta1_address = mbox_sta1.address();
    let sta2_address = mbox_sta2.address();
    let sta3_address = mbox_sink.address();

    let mbox_queue = Mailbox::new();
    // let queue_address = mbox_queue.address();

    // let sink_mbox = Mailbox::new();
    // let sink_mbox_address = sink_mbox.address();

    // CONNECT COMPONENTS
    // source.output_port.connect(Sink::input, &sink_mbox);

    sta1_bg.output_port.connect(QueueModule::input, &mbox_queue); // Two DL STAs send
    sta2_bg.output_port.connect(QueueModule::input, &mbox_queue);
    queue.output_port.connect(Sink::input, &mbox_sink);

    let t0 = MonotonicTime::EPOCH;

    let mut simu: asynchronix::simulation::Simulation = SimInit::new()
        .add_model(sta1_bg, mbox_sta1, "STA1 (BG)")
        .add_model(sta2_bg, mbox_sta2, "STA2 (BG)")
        .add_model(queue, mbox_queue, "Queue")
        .add_model(sink, mbox_sink, "SINK")
        .init(t0);

    let scheduler = simu.scheduler();
    // ----------
    // Simulation.
    // ----------
    // Check initial conditions.

    let t = t0;

    assert_eq!(simu.time(), t);

    // START WITH FIRST EVENT
    scheduler
        .schedule_event(
            Duration::from_millis(1), //STA 1 starts in 1 millisecond
            STA_source::send_packet,
            (),
            &sta1_address,
        )
        .unwrap();

    scheduler
        .schedule_event(
            Duration::from_secs(5), // STA 2 will start in 5 seconds
            STA_source::send_packet,
            (),
            &sta2_address,
        )
        .unwrap();

    simu.step_by(Duration::from_secs_f64(stoptime)); //works

    // After simulation, write the CSV data
    if let Ok(data) = csv_data_handle.lock() {
        if let Err(e) = data.write_to_csv() {
            eprintln!("Failed to write CSV file: {}", e);
        }
    }
    if let Ok(stats_vec) = stats_sta_data_handle.lock() {
        // Now stats_vec is a MutexGuard<Vec<perStaLockStats>>
        for sta_stats in stats_vec.iter() {
            if let Ok(sta_data) = sta_stats.data.lock() {
                sta_data.print_nicely();
            }
        }

        if let Err(e) = write_all_sta_csvs(&stats_vec) {
            eprintln!("Error writing STA CSV files: {}", e);
        }
    }

    if let Ok(queue_stats) = queuestats_data_handle.lock() {
        // println!("[DEBUGDEBUGDEBU]!!!! T_s : {}, T_q : {} !", T_s_f64, T_q_f64);
        queue_stats.print_nicely();
    }

    if let Ok(sink_stats) = sinkstats_data_handle.lock(){
        sink_stats.print_nicely(); 
    }

    // println!("************ END RESULTS STAS***********\n LT: ");

    println!("*******************************************************************"); 
    println!("Inputs--> rate: {}, l_mean :{}, effective_rate: {}, k: {}", rate_bps_in, mean_length, effective_rate, k_queue); 

    LT.print_results();
}

fn main() {
    env::set_var("RUST_BACKTRACE", "1"); // for debug backtrace!

    // READ COMMAND-LINE ARGUMENTS
    let args: Vec<String> = env::args().collect();
    if args.len() != 7 {
        eprintln!(
            "Usage: {} <mean_length> <k_queue> <rate_bps> <rate_queue_bps> <distance>",
            args[0]
        );
        return;
    }
    let stoptime: f64 = args[1].parse().expect("Invalid T_END");
    let mean_length: f64 = args[2].parse().expect("Invalid mean_length");
    let k_queue: usize = args[3].parse().expect("Invalid k_queue");
    let rate_bps_in: f64 = args[4].parse().expect("Invalid rate_bps_in");
    let rate_queue_bps: f64 = args[5].parse().expect("Invalid rate_queue_bps");
    let distance: f64 = args[6].parse().expect("Invalid STA distance");

    // /// SCENARIO 1: MM1K WITH POISSON, QUEUE, SINK
    // simple_MM1K(
    //     stoptime,
    //     mean_length,
    //     k_queue,
    //     rate_bps_in,
    //     rate_queue_bps,
    //     distance,
    // );

    multiple_STA_sim(
        3,
        stoptime,
        mean_length,
        k_queue,
        rate_bps_in,
        rate_queue_bps,
        distance,
    );
}
