









////////////////////////////////////// XR SIMULATOR ////////////////////////////
/// 
///  Mixing up connection.rs and bitratemanager to simplify the process of generating frames.
///     * Will try to stay accurate to packet latencies in all parts of the pipeline ( for now, linear terms with maybe some randomness) 
/// 
/// TODO:   
///     * XRServer sending packets , rate corresponding to FPS and bitrate (90 fps, 100 Mbps) to sink, with correct headers. 
//      * Decoder queue of XRClient

/// 
use asynchronix::simulation::{Mailbox, Scheduler, SimInit};
use asynchronix::time::MonotonicTime;


use std::time::{Duration, Instant};

use std::sync::{Arc, Mutex};
use serde::{Deserialize, Serialize};
use std::net::IpAddr; 


mod lib; // for calling m own local library
use crate::lib::{
    compute_mm1k_metrics, exponential, frametransmission_delay, perStaLockStats,
    write_all_sta_csvs, Coords, CsvType, CumulativeStats, DEFAULT_TMAX_AGG,
    MAX_AMPDU_SIZE, P_TX, MpduPacket, AmpduPacket, DebugColor, SlidingWindowAverage, 
};

mod statistics_manager; 
use statistics_manager::*;

const RETRY_CONNECT_MIN_INTERVAL: Duration = Duration::from_secs(1);
const HANDSHAKE_ACTION_TIMEOUT: Duration = Duration::from_secs(2);
const STREAMING_RECV_TIMEOUT: Duration = Duration::from_millis(500);


// use crate::lib::{AmpduPacket, MpduPacket, exponential, Coords, CumulativeStats, CsvType};
// use crate::{debug_print, format_elapsed, format_timestamp}; 


use rand::Rng;
use std::cmp::{max, self};
use std::collections::VecDeque;
use std::env;
use std::f64::consts::PI;
use std::future::Future;

use asynchronix::model::{Context, Model};
use asynchronix::ports::Output;

use crate::lib::DEBUG_PRINT_ENABLED;

const SHARD_PREFIX_SIZE: usize = mem::size_of::<u32>() // packet length - field itself (4 bytes)
    + mem::size_of::<u16>() // stream ID
    + mem::size_of::<u32>() // packet index
    + mem::size_of::<u32>() // shards count
    + mem::size_of::<u32>() // shards index
    + mem::size_of::<f32>(); // tx relative timestamp

mod stream_socket; 

use crate::stream_socket::*; 



#[derive(Clone)]
pub struct BitrateManager{  

    last_frame_instant: Instant, 
    last_update_instant: Instant,

    frame_index: usize, 

    frame_interval_average: SlidingWindowAverage<Duration>, 
    encoder_latency_average: SlidingWindowAverage<Duration>,
    network_latency_average: SlidingWindowAverage<Duration>, 

    bitrate_average_mbps: SlidingWindowAverage<f32>,

    last_target_bitrate_mbps: f32, 
    update_interval_s: Duration, 
    
    rtt_average: SlidingWindowAverage<Duration>,
    peak_throughput_average: SlidingWindowAverage<f32>,
    frame_interarrival_average: SlidingWindowAverage<f32>,
}

static BITRATE_MANAGER: Lazy<Mutex<BitrateManager>> =
    Lazy::new(|| Mutex::new(BitrateManager::new(256, 60.0, 30.0)));

static STATISTICS_MANAGER: OptLazy<StatisticsManager> = alvr_common::lazy_mut_none();

impl BitrateManager{ // TODO: Add method for CBR
    pub fn new( max_history_size: usize, initial_framerate: f32, initial_bitrate: f32) -> Self {

        Self{
            last_frame_instant: Instant::now(), 
            last_update_instant: Instant::now(), 

            frame_index: 0, 
    
            frame_interval_average:  SlidingWindowAverage::new(Duration::ZERO , max_history_size), 
            encoder_latency_average: SlidingWindowAverage::new(Duration::ZERO , max_history_size), 
            network_latency_average: SlidingWindowAverage::new(Duration::ZERO , max_history_size), 
            
            bitrate_average_mbps: SlidingWindowAverage::new(initial_bitrate * 1E6, max_history_size), 
            last_target_bitrate_mbps: initial_bitrate * 1E6,  
            update_interval_s: UPDATE_BITRATE_INTERVAL, 

            rtt_average: SlidingWindowAverage::new(Duration::from_millis(5), max_history_size),
            peak_throughput_average: SlidingWindowAverage::new(300E6, max_history_size),
            frame_interarrival_average: SlidingWindowAverage::new(
                1. / initial_framerate,
                max_history_size,
            ),
        }
    }

    pub fn adjust_bitrate(&mut self, network_conditions: &NetworkConditions) { 
        
            todo!("TODO: ABR!! "); 
            /* Bitrate adjustment logic */ 
        }

}

pub struct XRServer{

    pub bitrate_manager: BitrateManager, 

    pub sender_video: StreamSender<H>,
    pub sender_audio: StreamSender<H>,
    pub sender_haptics: StreamSender<H>, 

    pub output_video: Output<MpduPacket>,
    pub output_audio: Output<MpduPacket>,
    pub output_haptics: Output<MpduPacket>,  

    pub is_streaming: bool, 

}

impl XRServer{
    pub fn new() -> Self {
        let arrival_rate = arrival_rate_bps / mean_length;
        let effective_mu = rate_service_bps /mean_length; 
        println!("\n*************************************************"); 
        println!("[DEBUG STA{}]\tCoordinates: {:?}\n\tDestination: STA{} | RATE_IN: {:.3} Mbps, Rate_service: {:.3} (packs/s),\n\t Arrival_rate (pack/s): {:.3}, Departure_rate: {:.3},  L = {}",
                            src, coordinates, dest,                     arrival_rate_bps/1E6, rate_service_bps / 1E6 , arrival_rate,effective_mu ,mean_length);

        Self {
            bitrate_manager: BitrateManager::new(MAX_HISTORY_SIZE, INITIAL_FRAMERATE_FPS, INITIAL_BITRATE_MBPS), 

            sender_video: StreamSender::new(VIDEO), 
            sender_audio: StreamSender::new(AUDIO), 
            sender_haptics: StreamSender::new(HAPTICS), 

            output_video: Output::default(), 
            output_audio: Output::default(), 
            output_haptics: Output::default(), 
            is_streaming: false, 
        }
    }


    pub fn connection_pipeline(&mut self) {

        

        *BITRATE_MANAGER.lock() =
            BitrateManager::new(settings.video.bitrate.history_size, fps, initial_bitrate);



        let mut stream_socket = StreamSocketBuilder::connect_to_client(timeout, client_ip, port, protocol, dscp, send_buffer_bytes, recv_buffer_bytes, max_packet_size)
        
        // do the rest of code for initiating connection




        
        
        while self.is_streaming == true {






        }

    }
    // TODO : More functions to process inputs, handle ABR, etc. 
}

impl Model for XRServer{}

pub struct XRClient{

    pub decoder_queue: VecDeque<VideoFrame>, 

    pub output_statistics: Output<MpduPacket>,
    pub output_tracking: Output<MpduPacket>,

    pub coordinates: Coords,
    pub is_streaming: bool, 
    
    pub frames_dropped_counter: usize, 


}

impl XRClient{
    fn new() -> Self{
        Self{
            decoder_queue: VecDeque::new(),
            output_statistics: Output::default(), 
            output_tracking: Output::default(), 

            coordinates: Coords::new(), 
            is_streaming: false, 
            frames_dropped_counter: 0, 
        }
    }

    fn input_packets(packet: MpduPacket){

        // TODO: Based on the stream type (VIDEO, AUDIO, etc.) call one function or the other for the same packet



    }

    fn recv_video( &mut self, data: ReceiverData<VideoPacketHeader>) // inside the thread::spawn(move) of connection_pipeline
    {
                let packet_stats = NetworkStatisticsPacket{
                    frame_index: data.get_frame_index() as i32,                 // index of the current frame
                    frame_span: data.get_frame_span(),                          // duration of the current frame

                    bytes_in_frame: data.get_bytes_in_frame(),                  // bytes received for the current frame, including both prefixes and network headers
                    bytes_in_frame_app: data.get_bytes_in_frame_app(),          // bytes received for the current frame, excluding both prefixes and network headers

                    // Interval specific metrics
                    frame_interarrival: data.get_frame_interarrival(),              // time interval between consecutive frames

                    interarrival_jitter: data.get_interarrival_jitter(),        // measure of the variability in the time between the reception of consecutive video shards
                    ow_delay: data.get_ow_delay(),                              // one-way delay of the received video shards
                    filtered_ow_delay: data.get_filtered_ow_delay(),            // kalman filtered one-way delay of the received video shards, as GCC does

                    frames_skipped: data.get_frames_skipped(),                  // number of frames skipped

                    rx_bytes: data.get_rx_bytes(),                              // bytes received in the interval between the consecutive frames, including any prefixes and network headers

                    rx_shard_counter: data.get_rx_shard_counter(),              // non-duplicated video shards received during the interval between consecutive frames
                    duplicated_shard_counter: data.get_duplicated_shard_counter(), // duplicated video shards received during the interval between consecutive frames

                    highest_rx_frame_index: data.get_highest_rx_frame_index(), // index of the highest video FRAME received during the interval between consecutive frames
                    highest_rx_shard_index: data.get_highest_rx_shard_index(), // index of the highest video SHARD received ... 
                }; 
        
        // TODO: Make the function to actually schedule sending this packet! 
        let Ok((header, nal)) = data.get(); 

        if !push_frame_decoder(header.timestamp, nal){

            
            report_video_packet_dropped(data.get_frame_index()); // report in HistoryFrame the lost packet, but we're not doing HistoryFrame (?) 
            self.frames_dropped_counter += 1; 
        }
        else{
            // TODO:  
            // stats.report_video_packet_data(header.timestamp, data.get_frame_index(), frames_dropped); //

        }

        // if header.is_idr {}
        
        // if data.had_packet_loss(){


        // }
    }
    // fn send_statistics_packet(){
    //     // TODO! 
    // } 

    fn recv_audio(data: ReceiverData<>){

        // actually we do nothing on this, just consume the packet 
        
    }

    fn receive_control_packet() {

    }



}


impl Model for XRClient{}

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
            self.output_port.send(packet.clone()).await;

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

impl STA_source {
    pub fn new(
        arrival_rate_bps: f64,
        mean_length: f64,
        src: i32,
        dest: i32,
        coordinates: Coords,
        does_sta_transmit: bool,
        rate_service_bps: f64
    ) -> Self {
        let arrival_rate = arrival_rate_bps / mean_length;
        let effective_mu = rate_service_bps /mean_length; 
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

                time_interarrival = max(time_interarrival, Duration::from_nanos(1));

                let len_random = exponential(self.mean_length_packets as f64) as usize;
                
                // let len_random = self.mean_length_packets as usize; 

                packet.length_packet = cmp::max(1, len_random);
                packet.packet_id = self.num_packets_sent;

                packet.sta_src_id = self.sta_id;
                packet.sta_dest_id = self.destination_id;

                packet.sta_dest_coords = self.sta_coordinates; 

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
    queue_length_counter: usize, 

    num_packets_dropped: usize,
    num_packets_rx: usize,
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
    pub fn update_cumstats(&mut self, ts: f64, tq: f64, packet_drops: usize, packets_rx: usize, queue_length: usize) {
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
        println!("{}", format_row("E[N_q]", self.queue_length_counter as f64 / self.num_packets_rx as f64)); 
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
            let sta_stats = perStaLockStats::new();
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
            DebugColor::Red,
            "{} [DBG TX]    --AMPDU sent to STA {} with {} packets inside, Q_size = {}, L = {}, AMPDU_size: {}",
            format_elapsed!(elapsed),
            AMPDU_sent.sta_id,
            AMPDU_sent.mpdu_packets.len(),
            self.queue.len(),
            AMPDU_sent.total_length,
            AMPDU_sent.size - 1, 
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


                let mut last_service_duration = Duration::default();

                for packet_index_loop in 0..self.queue.len(){
                    
                    if let Some(current_packet) = self.queue.get(packet_index_loop as usize){

                        if current_packet.sta_dest_id != self.aux_ampdu_serviced.sta_id {
                            continue; 
                        }
                        self.aux_ampdu_serviced.total_length += current_packet.length_packet; 
                        self.aux_ampdu_serviced.size += 1;

                        let resultz = frametransmission_delay(self.aux_ampdu_serviced.total_length as f64, self.aux_ampdu_serviced.size, self.coords_queue, current_packet.sta_dest_coords, P_TX); 

                        if resultz.service_delay >= DEFAULT_TMAX_AGG || self.aux_ampdu_serviced.size >= MAX_AMPDU_SIZE {
                            debug_print!(DebugColor::Blue,"[DBG DEQUE] \t\t finished early! | T_s: {:.3} of {:.3}, AMPDU_SIZE : {} of {}", 
                                        resultz.service_delay, DEFAULT_TMAX_AGG, self.aux_ampdu_serviced.size, MAX_AMPDU_SIZE); 
                            break; 
                        }

                        if let Some(mut packet_rmvd) = self.queue.remove(packet_index_loop){
                            
                            // packet_index_loop -= 1; 
                            packet_rmvd.queue_length_when_out = self.queue.len(); 
                            packet_rmvd.queue_out_instant = now; 

                            self.aux_ampdu_serviced.mpdu_packets.push(packet_rmvd);

                            debug_print!(
                                DebugColor::Blue,
                                "{} [DBG DEQUE] --Packet {} (STA{}) dequed and put in AMPDU, Iter index: {}, Q_size = {}",
                                format_elapsed!(now),
                                packet_rmvd.packet_id,
                                packet_rmvd.sta_dest_id,
                                packet_index_loop, 
                                self.queue.len(),
                            );

                            last_service_duration = Duration::from_secs_f64(resultz.service_delay); //use the last service delay
                        }
                    }
                }

                // while index < self.queue.len() {
                //     // Get packet info before any modifications
                //     let (matches_sta_id, packet_length) =
                //         if let Some(current_packet) = self.queue.get(index) {
                //             (
                //                 current_packet.sta_dest_id == self.aux_ampdu_serviced.sta_id,
                //                 current_packet.length_packet,
                //             )
                //         } else {
                //             break;
                //         };

                //     if !matches_sta_id {
                //         // Skip packets not matching AMPDU's STA_ID
                //         println!("skip {}", index);
                //         index += 1;
                //         continue;
                //     }

                //     // Check AMPDU constraints before adding packet
                //     let resulting_delays = frametransmission_delay(
                //         self.aux_ampdu_serviced.total_length as f64,
                //         self.aux_ampdu_serviced.size,
                //         self.coords_queue,
                //         self.aux_ampdu_serviced.coordinates,
                //         self.p_tx,
                //     );

                //     if resulting_delays.service_delay >= DEFAULT_TMAX_AGG
                //         || self.aux_ampdu_serviced.size >= MAX_AMPDU_SIZE as i32
                //     {
                //         debug_print!(
                //             DebugColor::Yellow,
                //             "{} [DBG AMPDU END] T_s = {} / {} ; SIZE = {} / {}",
                //             format_elapsed!(now),
                //             resulting_delays.service_delay,
                //             DEFAULT_TMAX_AGG,
                //             self.aux_ampdu_serviced.size,
                //             MAX_AMPDU_SIZE
                //         );
                //         break;
                //     }

                //     // Remove packet and add to AMPDU
                //     if let Some(mut packet) = self.queue.remove(index) {
                //         packet.queue_out_instant = now;

                //         debug_print!(
                //             DebugColor::Blue,
                //             "{} [DBG DEQUE] --Packet {} (STA{}) dequed and put in AMPDU, Iter index: {}, Q_size = {}",
                //             format_elapsed!(now),
                //             packet.packet_id,
                //             packet.sta_dest_id,
                //             index + 1,
                //             self.queue.len(),
                //         );


                //         self.aux_ampdu_serviced.mpdu_packets.push(packet);
                //         self.aux_ampdu_serviced.total_length += packet.length_packet;
                //         self.aux_ampdu_serviced.size += 1;


                //         debug_print!(DebugColor::Blue, "\t\t aux_ampdu_size: {} L_in: {}", self.aux_ampdu_serviced.size, self.aux_ampdu_serviced.total_length); 
                //         packet.queue_length_when_out = self.queue.len().clone(); 

                        
                //         last_service_duration =
                //             Duration::from_secs_f64(resulting_delays.service_delay);

                //         // Don't increment index since we removed a packet
                //     } else {
                //         index += 1;
                //     }
                // }

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
                            self.blocked_packet_counter,
                            self.arrived_packet_counter,
                            packet.queue_length_when_out,
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
                    
                    if DEBUG_PRINT_ENABLED{
                        self.aux_ampdu_serviced.print();
                    }

                    self.packet_being_served = true;

                    context
                        .scheduler
                        .schedule_event(
                            last_service_duration,
                            Self::send_ampdu,
                            self.aux_ampdu_serviced.clone(),
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
                format_row("Avg Received Throughput[Mbps]", (self.av_l / self.last_time ) / 1E6)
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

        for mut packet in ampdu_packet.mpdu_packets {

            packet.T_s = now.duration_since(packet.queue_out_instant); 

            debug_print!(
                DebugColor::Magenta,
                "{} [DBG SINK ] ---Packet {} arrived from STA{} into Sink (STA{}, T_s = {}, E[T_s] = {})",
                format_elapsed!(now),
                packet.packet_id,
                packet.sta_src_id,
                packet.sta_dest_id,
                packet.T_s.as_secs_f64(),
                packet.expected_T_s.as_secs_f64(), 

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

    const NUM_STAS_UL: usize = 1; //for now 
    
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
    println!("vec_coords: {:?}\n", vec_coords);

    let results1 = frametransmission_delay(
        mean_length * MAX_AMPDU_SIZE as f64, // optimistic assumption of max throughput
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

    let aggregated_rate_in = (num_STAs - NUM_STAS_UL) as f64 * rate_bps_in; 

    println!("*******************************************************************"); 
    println!("Inputs--> rate: {}, l_mean :{}, effective_rate: {}, k: {}", aggregated_rate_in, mean_length, effective_rate, k_queue); 

    let LT = compute_mm1k_metrics(aggregated_rate_in, mean_length as f64, effective_rate, k_queue);

    let mut sta1_bg: STA_source =
        STA_source::new(rate_bps_in, mean_length, 0, 2, coords_sta1, true, effective_rate1); // STAs 0 and 1 send traffic to 5 through AP
    let mut sta2_bg: STA_source =
        STA_source::new(rate_bps_in, mean_length, 1, 2, coords_sta2, true, effective_rate2);



    println!("STA1 PathLoss: {:.2}, P_rx : {:.2}, T_total: {:.3} ms, T_s(data): {:.3} ms , rate_total: {:.2} \n\n",
    results1.pathloss, results1.p_rx, results1.service_delay * 1000.0, 
    results1.data_service_delay * 1000.0, (1.0 / results1.service_delay) * mean_length); 

    println!("STA2 PathLoss: {:.2}, P_rx : {:.2}, T_total: {:.3} ms, T_s(data): {:.3} ms , rate_total: {:.2} \n\n",
    results2.pathloss, results2.p_rx, results2.service_delay * 1000.0, 
    results2.data_service_delay * 1000.0, (1.0 / results2.service_delay) * mean_length); 



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
    // let sta3_address = mbox_sink.address();

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

    let mut simu: asynchronix::simulation::Simulation = SimInit::with_num_threads(64)
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
    let epsilon1 = Duration::from_secs_f64(exponential(0.9)); 
    let epsilon2 = Duration::from_secs_f64(exponential(0.9)); 

    let duration_scheduled1= Duration::from_secs(10) + epsilon1; 
    let duration_scheduled2 = Duration::from_secs(10) + epsilon2 ; 

    scheduler
        .schedule_event(
            duration_scheduled1,
            STA_source::send_packet,
            (),
            &sta1_address,
        )
        .unwrap();

    scheduler
        .schedule_event(
            duration_scheduled2,
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
    println!("Inputs--> rate: {}, l_mean :{}, effective_rate: {}, k: {}", aggregated_rate_in, mean_length, effective_rate, k_queue); 

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
