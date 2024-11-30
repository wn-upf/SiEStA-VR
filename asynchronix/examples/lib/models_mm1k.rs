use crossbeam::channel::{unbounded, Receiver, Sender};
use rand::Rng;
use std::cmp::{self, max};
use std::collections::{HashMap, VecDeque};
use std::f64::consts::PI;
use std::future::Future;
use std::result;

use asynchronix::model::{Context, Model};
use asynchronix::ports::Output;
use std::time::{Duration, Instant};

use crate::lib::alvr_stream_socket::parse_shard_data;
use crate::DebugColor;
use std::sync::{Arc, Mutex};

use crate::lib::{
    exponential, frametransmission_delay, perStaLockStats, AmpduPacket, Coords, CsvType,
    CumulativeStats, MpduPacket, DEBUG_PRINT_ENABLED, DEFAULT_TMAX_AGG, MAX_AMPDU_SIZE, P_TX,
};
use crate::{debug_print, format_elapsed, taitime_to_f64};

use super::ResultsFrameTXDelay;

pub struct PoissonSource {
    pub arrival_rate: f64,
    pub mean_length_packets: f64,

    pub output_port: Output<MpduPacket>,

    pub num_packets_sent: usize,
}
#[allow(dead_code)]
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
            time_interarrival = max(time_interarrival, Duration::from_secs_f64(1E-9));

            let len_random = exponential(self.mean_length_packets as f64) as usize;
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
                    Duration::from_secs_f64(exponential(1.0 / self.arrival_rate));

                time_interarrival = max(time_interarrival, Duration::from_nanos(1));

                let len_random = exponential(self.mean_length_packets as f64) as usize;

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

    pub PL_probability: f64,
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
}
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
    ) -> Self {
        // Create a vector of perStaLockStats with initialized sta_ids
        let mut stats_vec = HashMap::new();

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

            csv_metrics: CsvType::new(),

            coords_queue: Coords::new(),
            p_tx: 20.0,
            STA_coords_grid: Vec::new(),
            STA_coords_map: HashMap::new(),

            cumulative_stats_queue: Arc::new(Mutex::new(QueueStats::new())),
            array_stas_stats: Arc::new(Mutex::new(stats_vec)),

            stats_tx: Some(stats_tx),
            stats_rx: Some(stats_rx),

            PL_probability: PL_prob,
        }
    }

    pub async fn input(&mut self, mut packet: MpduPacket, context: &Context<Self>) {
        self.arrived_packet_counter += 1;
        self.queue_length_counter += self.queue.len();

        let now = context.scheduler.time();
        if self.queue.len() < self.queue_maxsize {
            packet.queue_in_instant = now;
            self.queue.push_back(packet.clone());

            debug_print!(
                DebugColor::Green,
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
            debug_print!(
                DebugColor::Red,
                "{} [DBG FULL QUEUE] Packet {} DROPPED!! , Q_size = {}",
                format_elapsed!(now),
                packet.packet_id,
                self.queue.len()
            );
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

            if self.queue.len() == 1 && !self.packet_being_served {
                self.deque_schedule_service((), context).await;
            }
        } else {
            self.blocked_packet_counter += 1;
            debug_print!(
                DebugColor::Red,
                "{} [DBG FULL QUEUE] Packet {} DROPPED!! , Q_size = {}",
                format_elapsed!(now),
                packet.packet_id,
                self.queue.len()
            );
        }
    }

    pub async fn send_ampdu(&mut self, AMPDU_sent: AmpduPacket, context: &Context<Self>) {
        let elapsed = context.scheduler.time();

        debug_print!(
            DebugColor::Red,
            "{} [DBG TX]    --AMPDU sent to STA {} with {} packets inside, Q_size = {}, L = {}, AMPDU_size: {}",
            format_elapsed!(elapsed),
            AMPDU_sent.sta_dest_id,
            AMPDU_sent.mpdu_packets.len(),
            self.queue.len(),
            AMPDU_sent.total_length,
            AMPDU_sent.size - 1
        );
        // AMPDU_sent.print();
        self.packet_being_served = false;

        match AMPDU_sent.sta_dest_id {
            0 => {
                self.output_port_sta1.send(AMPDU_sent).await;
            }
            1 => {
                self.output_port_sta1.send(AMPDU_sent).await;
            }
            2 => {
                self.output_port_sta1.send(AMPDU_sent).await;
            }
            12 => {
                self.output_port_sta1.send(AMPDU_sent).await;
            }

            _ => {
                println!("ERROR!!!! ERROR!!! UNEXPECTED STA ID QUEUE");
            }
        }

        if self.queue.len() > 0 {
            self.deque_schedule_service((), context).await;
            // context.scheduler.schedule_event(Duration::from_nanos(10), Self::deque_schedule_service, ()).unwrap();
        }

        if let Some(stats_rx) = self.stats_rx.as_mut() {
            // print!("OK1,");
            if let Ok(mut queue_stats) = self.cumulative_stats_queue.lock() {
                // print!("OK2,");

                if let Ok(array_STAs_stats) = self.array_stas_stats.lock() {
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

                        if let Some(stats) = array_STAs_stats.get(&stats_update.sta_src_id) {
                            // println!("OK PER STA");

                            if let Ok(mut stats_data) = stats.data.lock() {
                                stats_data.update_stats_per_sta(
                                    stats_update.now,
                                    stats_update.packet_id as usize,
                                    stats_update.queue_length_when_out,
                                    stats_update.T_s,
                                    stats_update.T_q,
                                    stats_update.length_packet,
                                    stats_update.sta_src_id,
                                    stats_update.sta_dest_id,
                                );
                            }
                        } else {
                            println!(
                                "ERROR: No stats found for station {}. Total stations: {}",
                                stats_update.sta_src_id,
                                array_STAs_stats.len()
                            );
                        }
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
                        );
                    }
                }
            }
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

                // Initialize AMPDU with first packet's info
                self.aux_ampdu_serviced.reset();
                self.aux_ampdu_serviced.sta_dest_id = first_packet.sta_dest_id;
                self.aux_ampdu_serviced.sta_src_id = first_packet.sta_src_id;
                self.aux_ampdu_serviced.coordinates = first_packet.sta_src_coords.clone();

                let mut last_service_duration = Duration::default();
                let mut packet_index = 0;

                let mut resultz =  ResultsFrameTXDelay::new(); 

                // Process packets that match the AMPDU destination (and source?)
                while packet_index < self.queue.len() {
                    if let Some(current_packet) = self.queue.get(packet_index) {
                        if current_packet.sta_dest_id != self.aux_ampdu_serviced.sta_dest_id
                            || current_packet.sta_src_id != self.aux_ampdu_serviced.sta_src_id
                        {
                            // make sure we select packets at a single interface (sta)
                            packet_index += 1;
                            continue;
                        }

                        let new_total_length =
                            self.aux_ampdu_serviced.total_length + current_packet.length_packet;
                        let new_size = self.aux_ampdu_serviced.size + 1;

                        resultz = frametransmission_delay(
                            new_total_length as f64,
                            new_size,
                            self.coords_queue,
                            current_packet.sta_src_coords,
                            P_TX,
                        );

                        if resultz.service_delay >= DEFAULT_TMAX_AGG || new_size >= MAX_AMPDU_SIZE {
                            break;
                        }

                        // Remove packet and update AMPDU
                        if let Some(mut packet_rmvd) = self.queue.remove(packet_index) {
                            packet_rmvd.queue_length_when_out = self.queue.len();
                            packet_rmvd.queue_out_instant = now;

                            packet_rmvd.T_q = now.duration_since(packet_rmvd.queue_in_instant);
                        

                            // Move packet into AMPDU without cloning
                            self.aux_ampdu_serviced.mpdu_packets.push(packet_rmvd);
                            self.aux_ampdu_serviced.total_length = new_total_length;
                            self.aux_ampdu_serviced.size = new_size;
                            last_service_duration = Duration::from_secs_f64(resultz.service_delay);
                        }
                    }
                }

                for packet in self.aux_ampdu_serviced.mpdu_packets.iter_mut(){
                    packet.T_s = Duration::from_secs_f64( resultz.service_delay); 

                     // Update stats before moving packet
                     if let Some(stats_tx) = &self.stats_tx {
                        let stats_update = StatsUpdate {
                            T_s: packet.T_s.as_secs_f64(), 
                            T_q: now
                                .duration_since(packet.queue_in_instant)
                                .as_secs_f64(),
                            blocked_packet_counter: self.blocked_packet_counter,
                            arrived_packet_counter: self.arrived_packet_counter,
                            queue_length_when_out: packet.queue_length_when_out,
                            sta_src_id: packet.sta_src_id as usize,
                            sta_dest_id: packet.sta_dest_id as usize,

                            packet_id: packet.packet_id as i32,
                            now,
                            length_packet: packet.length_packet,
                        };
                        stats_tx
                            .send(stats_update)
                            .expect("Failed to send stats update");
                    }
                }

                if !self.aux_ampdu_serviced.mpdu_packets.is_empty() {
                    debug_print!(
                        DebugColor::Yellow,
                        "{} [DBG AMPDU] --Dequeueing AMPDU, serviced at {}",
                        format_elapsed!(now),
                        format_elapsed!(now + last_service_duration),
                    );
                    
                    if DEBUG_PRINT_ENABLED == true {
                        self.aux_ampdu_serviced.print();
                    }

                    self.packet_being_served = true;

                    let mut rng = rand::thread_rng();

                    self.aux_ampdu_serviced.mpdu_packets.retain(|packet| {
                        let random_value: f64 = rng.gen();

                        if random_value <= self.PL_probability {
                            debug_print!(
                                DebugColor::Purple,
                                "{} [DBG TX] --packet {:?} dropped due to loss probability",
                                format_elapsed!(now),
                                packet.header_alvr,
                            );
                            self.blocked_packet_counter += 1;
                            false // Do not retain the packet
                        } else {
                            true // Retain the packet
                        }
                    });

                    // Move AMPDU to scheduled event instead of cloning
                    let ampdu_to_send =
                        std::mem::replace(&mut self.aux_ampdu_serviced, AmpduPacket::new());

                    context
                        .scheduler
                        .schedule_event(last_service_duration, Self::send_ampdu, ampdu_to_send)
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

            debug_print!(
                DebugColor::Magenta,
                "{} [DBG SINK ] ---Packet {} arrived from STA{} into Sink (STA{}, T_s = {})",
                format_elapsed!(now),
                packet.packet_id,
                packet.sta_src_id,
                packet.sta_dest_id,
                packet.T_s.as_secs_f64(),

            );
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
