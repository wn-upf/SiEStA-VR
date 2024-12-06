use asynchronix::time;
use crossbeam::channel::{unbounded, Receiver, Sender};
use crossbeam::queue;
use rand::Rng;
use core::net;
use std::cmp::{self, max};
use std::collections::VecDeque;
use std::f64::consts::PI;
use std::future::Future;
use std::ops::Deref;
use crate::debug_bgprint;

use asynchronix::model::{Context, Model};
use asynchronix::ports::Output;
use std::mem::replace; 
use std::time::{Duration, Instant};
use crate::lib::ResultsFrameTXDelay; 
use crate::lib::alvr_stream_socket::parse_shard_data;
use crate::DebugColor;
use std::sync::{Arc, Mutex};
use tai_time::TaiTime; 

use crate::lib::{
    exponential, frametransmission_delay, perStaLockStats, AmpduPacket, Coords, CsvType,
    CumulativeStats, MpduPacket, DEBUG_PRINT_ENABLED, DEFAULT_TMAX_AGG, MAX_AMPDU_SIZE, P_TX,
};
use crate::{debug_print, format_elapsed, taitime_to_f64};

pub struct PoissonSource {
    pub arrival_rate: f64,
    pub mean_length_packets: f64,

    pub output_port: Output<MpduPacket>,

    pub num_packets_sent: usize,
}

pub const MAX_EMULATED_QUEUE_PACKETS: usize =  1000; 
// pub const BANDWIDTH_LIMIT: f64 = 25.01E6; 

// Steps of emulated bandwidth 
pub const STEP1_TBEGIN :u64 = 30; 
pub const STEP1_TEND   :u64 = 150; 

pub const STEP2_TBEGIN :u64 = 60; 
pub const STEP2_TEND   :u64 = 80;

pub const STEP3_TBEGIN :u64 = 90; 
pub const STEP3_TEND   :u64 = 110; 

pub const BANDWIDTH_LIMIT_S1: f64 = 30.0E6; 
pub const BANDWIDTH_LIMIT_S2: f64 = 25.0E6; 
pub const BANDWIDTH_LIMIT_S3: f64 = 20.0E6; 




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


#[derive(Clone, Debug)]
pub enum NetworkPattern {
    Constant,
    OnOffPeriodic {
        on_duration: Duration,
        off_duration: Duration,
        current_state: bool,
        last_state_change: TaiTime<0>,
    },
    ProbabilisticDrop {
        drop_probability: f64,
    },
    Bandwidth {
        max_bps: f64,
        current_tokens: f64,
        max_tokens: f64,
        token_refill_rate: f64,
        valid_from: TaiTime<0>, 
        valid_until: TaiTime<0>, 

    },
}



impl NetworkPattern {
    /// Create a new `NetworkPattern` of type Bandwidth
    pub fn new_bandwidth(max_bps: f64, token_refill_rate: f64, valid_from: TaiTime<0>, valid_until: TaiTime<0> ) -> Self {
        Self::Bandwidth {
            max_bps,
            current_tokens: max_bps, // Initialize tokens to maximum
            max_tokens: max_bps,     // Maximum bucket capacity
            token_refill_rate,
            valid_from, 
            valid_until
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

#[derive(Debug)]
pub enum EnqueueResult {
    Transmitted(MpduPacket),  // Packet was immediately transmitted
    Queued,                   // Packet was added to the queue
    Dropped,                  // Packet was dropped due to queue overflow
}
#[derive(Clone)]
pub struct QueueMechanism {
    queue: VecDeque<MpduPacket>, // Packet queue
    network_emulator: NetworkPatternEmulator, // Bandwidth pattern
    max_queue_size: usize, 
    queue_delay_timer: Duration, 
}

impl QueueMechanism {
    pub fn new(max_emulated_queue_packets: usize, now: TaiTime<0>) -> Self {
        let mut network_emulator = NetworkPatternEmulator::new();

        let valid_from = TaiTime::EPOCH.checked_add(Duration::from_secs(STEP1_TBEGIN)).unwrap(); 
        let valid_until = TaiTime::EPOCH.checked_add(Duration::from_secs(STEP1_TEND)).unwrap(); 
        // let valid_from2 = TaiTime::EPOCH.checked_add(Duration::from_secs(STEP2_TBEGIN)).unwrap(); 
        // let valid_until2 = TaiTime::EPOCH.checked_add(Duration::from_secs(STEP2_TEND)).unwrap(); 

        // let valid_from3= TaiTime::EPOCH.checked_add(Duration::from_secs(STEP3_TBEGIN)).unwrap(); 
        // let valid_until3 = TaiTime::EPOCH.checked_add(Duration::from_secs(STEP3_TEND)).unwrap(); 


        network_emulator.add_pattern(NetworkPattern::new_bandwidth(BANDWIDTH_LIMIT_S1, BANDWIDTH_LIMIT_S1, valid_from, valid_until));
        // network_emulator.add_pattern(NetworkPattern::new_bandwidth(BANDWIDTH_LIMIT_S2, BANDWIDTH_LIMIT_S2, valid_from2, valid_until2));
        // network_emulator.add_pattern(NetworkPattern::new_bandwidth(BANDWIDTH_LIMIT_S3, BANDWIDTH_LIMIT_S3, valid_from3, valid_until3));


        Self {
            queue: VecDeque::new(),
            network_emulator,
            max_queue_size: max_emulated_queue_packets , 
            queue_delay_timer: Duration::ZERO, 
        }
    }
    // fn calculate_queue_delay(&self, packet: &MpduPacket) -> Duration {
    //     // Calculate how long the packet needs to wait based on current tokens
    //     let current_tokens = self.network_emulator.get_current_tokens();
    //     let packet_tokens = (packet.length_packet * 8) as f64;
        
    //     if current_tokens >= packet_tokens {
    //         Duration::ZERO
    //     } else {
    //         // Calculate time needed to refill tokens
    //         let tokens_needed = packet_tokens - current_tokens;
    //         let refill_rate = self.network_emulator.get_token_refill_rate();
    //         Duration::from_secs_f64(tokens_needed / refill_rate)
    //     }
    // }
   
    pub fn enqueue_or_transmit(
        &mut self,
        mut packet: MpduPacket,
        context: &Context<QueueModule>,
    ) -> EnqueueResult {
        let now = context.scheduler.time();
        
        // Get the potential delay for the packet
        match self.network_emulator.should_transmit_with_delay(&mut packet, now) {
            Some(delay) if delay == Duration::ZERO => {
                // Immediate transmission possible
                EnqueueResult::Transmitted(packet)
            },
            Some(delay) => {
            //     // Packet needs to be delayed
            //     debug_bgprint!(
            //         DebugColor::Chocolate, 
            //         "{:9.5}[DBG NETEM] Packet {} delayed by {:.6} seconds", 
            //         format_elapsed!(now), 
            //         packet.packet_id, 
            //         delay.as_secs_f64()
            //     );
                debug_bgprint!(DebugColor::Chocolate, "[DBG Queue_NETEM] Q_length: {} | ENQUEUED packet {} - delayed by {:.6} seconds (ALVR: frame {} shard {:4.0}/{:4.0})", 
                            self.queue.len(), 
                            packet.packet_id,
                            delay.as_secs_f64(),
                            packet.header_alvr.next_packet_index,
                            packet.header_alvr.shard_index,
                            packet.header_alvr.shards_count,
                        ); 
                
                // Add the delay to the packet's queue_in_instant
                let mut delayed_packet = packet;

                delayed_packet.emulated_delay = Some(delay.as_secs_f64());                 
                // Enqueue the packet
                if self.queue.len() < self.max_queue_size {
                    self.queue.push_back(delayed_packet);
                }
                else{
                    debug_bgprint!(DebugColor::Red, "[NETEM FULL queue] Packet {} DROPPED (ALVR: F_id: {} , {} / {})",delayed_packet.packet_id, delayed_packet.header_alvr.next_packet_index, delayed_packet.header_alvr.shard_index, delayed_packet.header_alvr.shards_count ); 
                    return EnqueueResult::Dropped; 
                }
                EnqueueResult::Queued
            },
            None => EnqueueResult::Dropped,
        }
    }

    pub fn process_emu_queued_packets(
        &mut self,
        context: &Context<QueueModule>,
    ) -> Vec<MpduPacket> {
        let now = context.scheduler.time();
        let mut transmitted_packets = Vec::new();
        let mut index = 0;

        while index < self.queue.len() {
            if let Some(packet) = self.queue.get_mut(index){

                match self.network_emulator.should_transmit_with_delay(packet, now) {
                    Some(delay) if delay == Duration::ZERO => {
                        // Remove and process the packet
                        let packet = self.queue.remove(index).unwrap();
                        transmitted_packets.push(packet);
                        // Don't increment index as we've removed the current element
                    },
                    Some(delay) => {
                        // Packet still needs to wait
                        index += 1;
                    },
                    None => {
                        // Packet dropped
                        self.queue.remove(index);
                    }
                }
            } else {
                break;
            }
        }

        transmitted_packets
    }

    // pub fn enqueue_or_transmit(
    //     &mut self,
    //     packet: MpduPacket,
    //     context: &Context<QueueModule>,
    // ) -> EnqueueResult {
    //     let now = context.scheduler.time(); 
    //     if self.queue.len() >= self.max_queue_size {
    //         return EnqueueResult::Dropped; // explicitly show packet was dropped
    //     }
        
    //     if self
    //         .network_emulator
    //         .should_transmit(&packet, context.scheduler.time())
    //     {
    //         // Immediate transmission
    //         EnqueueResult::Transmitted(packet)
    //     } else {
    //         let wait_time = self.calculate_queue_delay(&packet); 

    //         debug_bgprint!(DebugColor::Chocolate, "[DBG Queue_NETEM] Q_length: {} | ENQUEUED packet {} (ALVR: frame {} shard {:4.0}/{:4.0})", 
    //             self.queue.len(), 
    //             packet.packet_id,
    //             packet.header_alvr.next_packet_index,
    //             packet.header_alvr.shard_index,
    //             packet.header_alvr.shards_count
    //         ); 
            
    //         // Add to queue
    //         self.queue.push_back(packet);
    //         EnqueueResult::Queued
    //     }
    // }

    // pub fn process_queued_packets(
    //     &mut self,
    //     context: &Context<QueueModule>,
    // ) -> Vec<MpduPacket> {
    //     let mut transmitted_packets = Vec::new();
    //     let mut index = 0;

    //     while index < self.queue.len() {
    //         if let Some(packet) = self.queue.get(index) {
    //             if self.network_emulator.should_transmit(packet, context.scheduler.time()) {
    //                 // Remove and process the packet
    //                 let packet = self.queue.remove(index).unwrap();
    //                 transmitted_packets.push(packet);
    //                 // Don't increment index as we've removed the current element
    //             } else {
    //                 // Move to next packet
    //                 index += 1;
    //             }
    //         } else {
    //             break;
    //         }
    //     }

    //     transmitted_packets
    // }
}
    

#[derive(Clone, Debug)]
pub struct NetworkPatternEmulator {
    patterns: Vec<NetworkPattern>,
    last_update_time: TaiTime<0>,
    debug_counter: usize, // Counter to track the calls
}
impl NetworkPatternEmulator {
    pub fn new() -> Self {
        Self {
            patterns: Vec::new(),
            last_update_time: TaiTime::default(),
            debug_counter: 0, 
        }
    }
        pub fn get_current_tokens(&self) -> f64 {
            self.patterns.iter()
                .filter_map(|pattern| match pattern {
                    NetworkPattern::Bandwidth { current_tokens, .. } => Some(*current_tokens),
                    _ => None
                })
                .next()
                .unwrap_or(0.0)
        }
    
        pub fn get_token_refill_rate(&self) -> f64 {
            self.patterns.iter()
                .filter_map(|pattern| match pattern {
                    NetworkPattern::Bandwidth { token_refill_rate, .. } => Some(*token_refill_rate),
                    _ => None
                })
                .next()
                .unwrap_or(0.0)
        }
    

    pub fn add_pattern(&mut self, pattern: NetworkPattern) {
        self.patterns.push(pattern);
    }

    pub fn should_transmit_with_delay(&mut self, packet: &mut MpduPacket, current_time: TaiTime<0>) -> Option<Duration> {
        // Update time-based patterns
        let alvr_header = packet.header_alvr.clone(); 
        if self.last_update_time == TaiTime::default() {
            self.last_update_time = current_time;
        }

        if packet.has_consumed_emu_tokens == true{
            return Some(Duration::ZERO);
        }

        let time_delta = current_time.duration_since(self.last_update_time);
        self.last_update_time = current_time;

                // Find all active bandwidth patterns at the current time
        let mut active_patterns: Vec<_> = self.patterns.iter_mut()
        .filter_map(|pattern| {
            if let NetworkPattern::Bandwidth { 
                valid_from,
                valid_until,
                ..
            } = pattern {
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
                DebugColor::Red, 
                "WARNING: Multiple active bandwidth patterns detected at {:4.9}",
                format_elapsed!(current_time)
            );
        }

        match active_patterns.first_mut() {
            Some(pattern) => match pattern {
                NetworkPattern::Bandwidth { 
                    current_tokens, 
                    max_tokens,
                    token_refill_rate,
                    valid_from,
                    valid_until,
                    ..
                } => {
                    // Token bucket algorithm
                    let refilled_tokens = *token_refill_rate * time_delta.as_secs_f64();
                    let new_tokens = (*current_tokens + refilled_tokens).min(*max_tokens);
                    let packet_tokens = (packet.length_packet * 8) as f64;
                    self.debug_counter += 1; 

                    if self.debug_counter >= 100{
                        debug_bgprint!(DebugColor::DarkBlue, "{:4.9} [DBG NETEM ({:4.4} -> {:4.4})] BW bucket -> refill: {}, available_tokens: {:.3} Mbps, packet_tokens: {:.3} Mb (ALVR F_id: {} -  {}/{})" , format_elapsed!(current_time),format_elapsed!(valid_from), format_elapsed!(valid_until),  refilled_tokens/1e6,  new_tokens / 1e6, packet_tokens/1e6, alvr_header.next_packet_index, alvr_header.shard_index, alvr_header.shards_count - 1 ); 
                        self.debug_counter = 0; 
                    }

                    if new_tokens >= packet_tokens {
                        // Packet can be transmitted
                        *current_tokens = new_tokens - packet_tokens;
                        packet.has_consumed_emu_tokens = true; // Mark tokens as consumed
                        return Some(Duration::ZERO)
                    } else {
                        // Calculate delay needed to accumulate enough tokens
                        let tokens_needed = packet_tokens - new_tokens;
                        let delay_seconds = tokens_needed / *token_refill_rate;
                        return Some(Duration::from_secs_f64(delay_seconds))
                    }
                },
                _ => return Some(Duration::ZERO)
            },
            None => Some(Duration::ZERO) // No active pattern
        }
    }


    pub fn should_transmit(&mut self, packet: &MpduPacket, current_time: TaiTime<0>) -> bool {
        // Update time-based patterns
        if self.last_update_time == TaiTime::default() {
            self.last_update_time = current_time;
        }

        let time_delta = current_time.duration_since(self.last_update_time);
        self.last_update_time = current_time;

        self.patterns.iter_mut().all(|pattern| match pattern {
            NetworkPattern::Constant => true,
            
            NetworkPattern::OnOffPeriodic { 
                on_duration, 
                off_duration, 
                current_state, 
                last_state_change 
            } => {
                let target_duration = if *current_state { *on_duration } else { *off_duration };
                let state_duration = current_time.duration_since(*last_state_change);
                
                if state_duration >= target_duration {
                    // Toggle state
                    *current_state = !*current_state;
                    *last_state_change = current_time;
                }

                *current_state
            },
            
            NetworkPattern::ProbabilisticDrop { drop_probability } => {
                let mut rng = rand::thread_rng();
                rng.gen::<f64>() > *drop_probability
            },
            
            NetworkPattern::Bandwidth { 
                max_bps, 
                current_tokens, 
                max_tokens,
                token_refill_rate ,
                valid_from,
                valid_until 
            } => {
                // Token bucket algorithm
                let refilled_tokens = *token_refill_rate * time_delta.as_secs_f64();
                // println!(" *token_refill: ({}) * time_delta ({}) = {}", *token_refill_rate, time_delta.as_secs_f64(), refilled_tokens); 

                let new_tokens = (*current_tokens + refilled_tokens).min(*max_tokens);
                
                let packet_tokens = (packet.length_packet * 8) as f64;
                self.debug_counter += 1; 
                if self.debug_counter >= 1000{
                    debug_bgprint!(DebugColor::DarkRed, "{:4.9} [DBG NETEM ({:4.4} -> {:4.4})] BW bucket -> refill: {}, available_tokens: {:.3} Mbps, packet_tokens: {:.3} Mb" , format_elapsed!(current_time), format_elapsed!(valid_from), format_elapsed!(valid_until), refilled_tokens/1e6,  new_tokens / 1e6, packet_tokens/1e6 ); 
                    self.debug_counter = 0; 
                }
                
                if new_tokens >= packet_tokens {
                    *current_tokens = new_tokens - packet_tokens;
                    true
                } else {
                    false
                }
            }
        })
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
    pub packet_id: i32,
    pub now: tai_time::TaiTime<0>,
    pub length_packet: usize,
}
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
    pub rate_departures_bps: f64,
    pub t0_time: Instant,

    pub csv_metrics: CsvType,

    pub coords_queue: Coords,
    pub p_tx: f64,

    pub STA_coords_grid: Vec<Coords>,

    pub cumulative_stats_queue: Arc<Mutex<QueueStats>>,

    pub stats_tx : Option<Sender<StatsUpdate>>,
    pub stats_rx : Option<Receiver<StatsUpdate>>,

    pub array_stas_stats: Arc<Mutex<Vec<perStaLockStats>>>,

    pub PL_probability: f64, 
    pub network_emulator: NetworkPatternEmulator, 
    pub queue_network_emulator: QueueMechanism, 
}

impl QueueModule {
    pub fn get_queue_stats_handle(&self) -> Arc<Mutex<QueueStats>> {
        self.cumulative_stats_queue.clone()
    }

    pub fn get_stas_stats_handle(&self) -> Arc<Mutex<Vec<perStaLockStats>>> {
        self.array_stas_stats.clone()
    }

    pub fn new(num_stas: usize, queue_size: usize, rate_departures_bps: f64, PL_prob: f64) -> Self {
        // Create a vector of perStaLockStats with initialized sta_ids
        let mut stats_vec = Vec::with_capacity(num_stas);
        
        let (stats_tx, stats_rx) = unbounded(); 
        for i in 0..num_stas {
            let sta_stats = perStaLockStats::new();
            // We need to lock the mutex to modify the sta_id
            if let Ok(mut stats) = sta_stats.data.lock() {
                stats.sta_id = i as i32;
            }
            stats_vec.push(sta_stats);
        }
        let network_emulator = NetworkPatternEmulator::new();



        // network_emulator.add_pattern(NetworkPattern::Bandwidth {
        //     max_bps: BANDWIDTH_LIMIT,
        //     current_tokens: BANDWIDTH_LIMIT,
        //     max_tokens: BANDWIDTH_LIMIT,
        //     token_refill_rate: BANDWIDTH_LIMIT, // Tokens per second
        // });
        let queue_mechanism = QueueMechanism::new( MAX_EMULATED_QUEUE_PACKETS, TaiTime::EPOCH); 

         // Example: Add probabilistic drop
        // network_emulator.add_pattern(NetworkPattern::ProbabilisticDrop {
        //     drop_probability: 0.001, // 
        // });

        
        // network_emulator.add_pattern(NetworkPattern::OnOffPeriodic {
        //     on_duration: Duration::from_secs(5),
        //     off_duration: Duration::from_secs(2),
        //     current_state: true,
        //     last_state_change: TaiTime::default(),
        // });

        // Example: Bandwidth limitation
       
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
            rate_departures_bps,
            t0_time: Instant::now(),

            csv_metrics: CsvType::new(),

            coords_queue: Coords::new(),
            p_tx: 20.0,
            STA_coords_grid: Vec::new(),
            cumulative_stats_queue: Arc::new(Mutex::new(QueueStats::new())),
            array_stas_stats: Arc::new(Mutex::new(stats_vec)),

            stats_tx : Some(stats_tx),
            stats_rx: Some(stats_rx),  

            PL_probability: PL_prob, 
            network_emulator: network_emulator,
            queue_network_emulator: queue_mechanism, 
        }
    }


    pub async fn input(&mut self, mut packet_arg: MpduPacket, context: &Context<Self>) {
        let now = context.scheduler.time();

        let id = packet_arg.packet_id.clone(); 
        packet_arg.queue_in_instant = now;
        let cloned_dbg = packet_arg.clone(); 

        if self.network_emulator.should_transmit(&packet_arg, now) {
            match self.queue_network_emulator.enqueue_or_transmit(packet_arg, &context) {
                EnqueueResult::Transmitted(packet) => {
                    self.arrived_packet_counter += 1;
                    self.queue_length_counter += self.queue.len();
        
                    if self.queue.len() < self.queue_maxsize {
                        self.queue.push_back(packet.clone());
        
                        if self.queue.len() == 1 && !self.packet_being_served {
                            self.deque_schedule_service((), context).await;
                        }
                    } else {
                        self.blocked_packet_counter += 1;
                        debug_bgprint!(
                            DebugColor::Red,
                            "{} [DBG FULL QUEUE] Packet {} DROPPED!! , Q_size = {:2.0}",
                            format_elapsed!(now),
                            packet.packet_id,
                            self.queue.len()
                        );
                    }
                },
                EnqueueResult::Queued => {
                    self.arrived_packet_counter += 1;
                    self.queue_length_counter += self.queue.len();
                },
                EnqueueResult::Dropped => {
                    // Packet dropped by network pattern
                    self.blocked_packet_counter += 1;
                    debug_bgprint!(
                        DebugColor::Red,
                        "Packet {} dropped by network pattern (ALVR: frame {} shard {:4.0}/{:4.0})", 
                        id, // Assuming packet_arg has a packet_id
                        cloned_dbg.header_alvr.next_packet_index,
                        cloned_dbg.header_alvr.shard_index,
                        cloned_dbg.header_alvr.shards_count, 

                    );
                }
            }
        }
        let processed_packets = self.queue_network_emulator.process_emu_queued_packets(context);
        if !processed_packets.is_empty(){
            {debug_bgprint!(DebugColor::Azure, "{}[DBG PROCESS NETEM] Processed packets:", format_elapsed!(now));}
        
            for mut packet in processed_packets {
                debug_bgprint!(DebugColor::Azure, "[DBG PROCESS NETEM] \t\t Packet:  ID: {} (ALVR: frame {} shard: {}/{})", packet.packet_id,  packet.header_alvr.next_packet_index, packet.header_alvr.shard_index, packet.header_alvr.shards_count -1 );
                self.process_transmitted_packet(packet, context).await;
            }
        }
    }

        // New helper method to process transmitted packets
    async fn process_transmitted_packet(&mut self, packet: MpduPacket, context: &Context<Self>) {
        self.arrived_packet_counter += 1;
        self.queue_length_counter += self.queue.len();
        let id = packet.packet_id.clone(); 
        let packet_arg = packet.clone(); 
        debug_print!(DebugColor::DarkBlue, "Processing packet from netem", ); 
        // If an AMPDU is being prepared and has space

        // If can't add to AMPDU, add to queue
        if self.queue.len() < self.queue_maxsize {
            self.queue.push_back(packet_arg.clone());
            debug_print!(DebugColor::DarkBlue, "Processing netem. Pushing into queue frame alvr {}, shard {}/{}", packet_arg.header_alvr.next_packet_index, packet_arg.header_alvr.shard_index, packet_arg.header_alvr.shards_count - 1 ); 

            // If this is the first packet and no packet is being served, start service
            if self.queue.len() == 1 && !self.packet_being_served {
                self.deque_schedule_service((), context).await;
            }
        } else {
            self.blocked_packet_counter += 1;
            debug_bgprint!(
                DebugColor::Red,
                "{} [DBG FULL QUEUE] Packet {} DROPPED!! , Q_size = {:2.0}",
                format_elapsed!(context.scheduler.time()),
                id, 
                self.queue.len()
            );
        }
    }

    // New method to try adding packet to current AMPDU
    async fn try_add_to_ampdu(&mut self, packet: MpduPacket, context: &Context<Self>) -> bool {
        let now = context.scheduler.time();

        // Initial AMPDU setup if empty
        if self.aux_ampdu_serviced.mpdu_packets.is_empty() {
            self.aux_ampdu_serviced.reset();
            self.aux_ampdu_serviced.sta_dest_id = packet.sta_dest_id;
            self.aux_ampdu_serviced.coordinates = packet.sta_src_coords.clone();
        }

        // Calculate potential new AMPDU length and service time
        let new_total_length = 
            self.aux_ampdu_serviced.total_length + packet.length_packet;
        let new_size = self.aux_ampdu_serviced.size + 1;

        let resultz = frametransmission_delay(
            new_total_length as f64,
            new_size,
            self.coords_queue,
            packet.sta_src_coords,
            P_TX,
        );

        // Check if adding packet would exceed AMPDU constraints
        if resultz.service_delay >= DEFAULT_TMAX_AGG || new_size > MAX_AMPDU_SIZE {
            return false;
        }

        // Add packet to AMPDU
        let mut processed_packet = packet;
        processed_packet.queue_length_when_out = self.queue.len();
        processed_packet.queue_out_instant = now;
        processed_packet.T_q = now.duration_since(processed_packet.queue_in_instant);
        processed_packet.expected_T_s = Duration::from_secs_f64(resultz.service_delay);

        // Send stats if needed
        if let Some(stats_tx) = &self.stats_tx {
            let stats_update = StatsUpdate {
                T_s: resultz.service_delay,
                T_q: processed_packet.T_q.as_secs_f64(),
                blocked_packet_counter: self.blocked_packet_counter,
                arrived_packet_counter: self.arrived_packet_counter,
                queue_length_when_out: processed_packet.queue_length_when_out,
                sta_src_id: processed_packet.sta_src_id as usize,
                packet_id: processed_packet.packet_id as i32,
                now,
                length_packet: processed_packet.length_packet,
            };
            stats_tx.send(stats_update).expect("Failed to send stats update");
        }

        // Add to AMPDU
        self.aux_ampdu_serviced.mpdu_packets.push(processed_packet);
        self.aux_ampdu_serviced.total_length = new_total_length;
        self.aux_ampdu_serviced.size = new_size;

        true
    }

    // Helper method to handle dropped packets
    fn handle_dropped_packet(&mut self, packet: MpduPacket, id: i32) {
        self.blocked_packet_counter += 1;
        debug_bgprint!(
            DebugColor::Red,
            "Packet {} dropped by network pattern (ALVR: frame {} shard {:4.0}/{:4.0})", 
            id, 
            packet.header_alvr.next_packet_index,
            packet.header_alvr.shard_index,
            packet.header_alvr.shards_count, 
        );
    }



 

    pub async fn input_UL(&mut self, mut packet: MpduPacket, context: &Context<Self>) {
        self.arrived_packet_counter += 1;
        self.queue_length_counter += self.queue.len();

        let now = context.scheduler.time();
        if self.queue.len() < self.queue_maxsize {
            packet.queue_in_instant = now;
            self.queue.push_back(packet.clone());

            // debug_print!(
            //     DebugColor::Green,
            //     "{} [DBG QUEUE] -Packet {} arrives from STA{} destined to STA{}, Q_size = {:2.0}",
            //     format_elapsed!(now),
            //     packet.packet_id,
            //     packet.sta_src_id,
            //     packet.sta_dest_id,
            //     self.queue.len()
            // );

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
        
        self.packet_being_served = false;


        match AMPDU_sent.sta_dest_id{
         0 =>    {self.output_port_sta1.send(AMPDU_sent).await;}
         2 =>    {self.output_port_sta2.send(AMPDU_sent).await;}

         _ =>    {println!("ERROR!!!! ERROR!!! UNEXPECTED STA ID QUEUE"); }
        }
        
        if self.queue.len() > 0 {
            self.deque_schedule_service((), context).await; 
            // context.scheduler.schedule_event(Duration::from_nanos(10), Self::deque_schedule_service, ()).unwrap();
        }

        if let Some(stats_rx) = self.stats_rx.as_mut() {
            if let Ok(mut queue_stats) = self.cumulative_stats_queue.lock() {
                if let Ok(array_STAs_stats) = self.array_stas_stats.lock() {
                    while let Ok(stats_update) = stats_rx.try_recv() {
                        queue_stats.update_cumstats(
                            stats_update.T_s,
                            stats_update.T_q,
                            stats_update.blocked_packet_counter,
                            stats_update.arrived_packet_counter,
                            stats_update.queue_length_when_out,
                        );

                        if let Some(stats) = array_STAs_stats.get(stats_update.sta_src_id as usize) {
                            if let Ok(mut stats_data) = stats.data.lock() {
                                stats_data.update_stats_per_sta(
                                    stats_update.now,
                                    stats_update.packet_id as usize,
                                    stats_update.queue_length_when_out,
                                    stats_update.T_s,
                                    stats_update.T_q,
                                    stats_update.length_packet,
                                );
                            }
                        }

                        self.csv_metrics.update_stats(
                            stats_update.now,
                            stats_update.packet_id as usize,
                            stats_update.queue_length_when_out,
                            stats_update.T_s,
                            stats_update.T_q,
                            stats_update.length_packet,
                        );
                    }
                }
            }
        }
       

    }

    // Separated stats processing into its own method
    async fn process_stats(&mut self, stats_rx: &Receiver<StatsUpdate>) {
        if let Ok(mut queue_stats) = self.cumulative_stats_queue.lock() {
            if let Ok(array_STAs_stats) = self.array_stas_stats.lock() {
                while let Ok(stats_update) = stats_rx.try_recv() {
                    queue_stats.update_cumstats(
                        stats_update.T_s,
                        stats_update.T_q,
                        stats_update.blocked_packet_counter,
                        stats_update.arrived_packet_counter,
                        stats_update.queue_length_when_out,
                    );

                    if let Some(stats) = array_STAs_stats.get(stats_update.sta_src_id as usize) {
                        if let Ok(mut stats_data) = stats.data.lock() {
                            stats_data.update_stats_per_sta(
                                stats_update.now,
                                stats_update.packet_id as usize,
                                stats_update.queue_length_when_out,
                                stats_update.T_s,
                                stats_update.T_q,
                                stats_update.length_packet,
                            );
                        }
                    }

                    self.csv_metrics.update_stats(
                        stats_update.now,
                        stats_update.packet_id as usize,
                        stats_update.queue_length_when_out,
                        stats_update.T_s,
                        stats_update.T_q,
                        stats_update.length_packet,
                    );
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
                self.aux_ampdu_serviced.coordinates = first_packet.sta_src_coords.clone();

                let mut last_service_duration = Duration::default();
                let mut packet_index = 0;
                let mut resultz = ResultsFrameTXDelay::new(); 

                // Process packets that match the AMPDU destination
                while packet_index < self.queue.len() {
                    if let Some(current_packet) = self.queue.get(packet_index) {
                        if current_packet.sta_dest_id != self.aux_ampdu_serviced.sta_dest_id {
                            packet_index += 1;
                            continue;
                        }

                        let new_total_length = 
                            self.aux_ampdu_serviced.total_length + current_packet.length_packet;
                        let new_size = self.aux_ampdu_serviced.size + 1;

                        let resultz = frametransmission_delay(
                            new_total_length as f64,
                            new_size,
                            self.coords_queue,
                            current_packet.sta_src_coords,
                            P_TX,
                        );

                        if resultz.service_delay >= DEFAULT_TMAX_AGG || new_size > MAX_AMPDU_SIZE {
                            break;
                        }

                        // Remove packet and update AMPDU
                        if let Some(mut packet_rmvd) = self.queue.remove(packet_index) {
                            packet_rmvd.queue_length_when_out = self.queue.len();
                            packet_rmvd.queue_out_instant = now;
                            
                            // Update stats before moving packet
                            if let Some(stats_tx) = &self.stats_tx {
                                let stats_update = StatsUpdate {
                                    T_s: resultz.service_delay,
                                    T_q: now.duration_since(packet_rmvd.queue_in_instant).as_secs_f64(),
                                    blocked_packet_counter: self.blocked_packet_counter,
                                    arrived_packet_counter: self.arrived_packet_counter,
                                    queue_length_when_out: packet_rmvd.queue_length_when_out,
                                    sta_src_id: packet_rmvd.sta_src_id as usize,
                                    packet_id: packet_rmvd.packet_id as i32,
                                    now,
                                    length_packet: packet_rmvd.length_packet,
                                };
                                stats_tx.send(stats_update).expect("Failed to send stats update");
                            }

                            packet_rmvd.T_q = now.duration_since(packet_rmvd.queue_in_instant);
                            packet_rmvd.expected_T_s = Duration::from_secs_f64(resultz.service_delay);

                            // Move packet into AMPDU without cloning
                            self.aux_ampdu_serviced.mpdu_packets.push(packet_rmvd);
                            self.aux_ampdu_serviced.total_length = new_total_length;
                            self.aux_ampdu_serviced.size = new_size;
                            last_service_duration = Duration::from_secs_f64(resultz.service_delay);
                        }
                    }
                }

                if !self.aux_ampdu_serviced.mpdu_packets.is_empty() {
                   
                    for packet in self.aux_ampdu_serviced.mpdu_packets.iter_mut(){
                        packet.T_s = Duration::from_secs_f64(resultz.service_delay); 
                    }
                    debug_print!(
                        DebugColor::Yellow,
                        "{} [DBG AMPDU] --Dequeueing AMPDU, serviced at {}",
                        format_elapsed!(now),
                        format_elapsed!(now + last_service_duration),
                    );

                    if DEBUG_PRINT_ENABLED {
                        self.aux_ampdu_serviced.print();
                    }

                    self.packet_being_served = true;
                    

                    let mut rng = rand::thread_rng();

                    self.aux_ampdu_serviced.mpdu_packets.retain(|packet| {
                        let random_value: f64 = rng.gen();
                    
                        if random_value <= self.PL_probability {
                            debug_bgprint!(
                                DebugColor::Red,
                                "{} [DBG QUEUE TX] --packet lost: Packet_ID: {}\t ALVR: {}/{} in frame {} due to probability {}/{}", 
                                format_elapsed!(now),
                                packet.packet_id, 
                                packet.header_alvr.shard_index,
                                packet.header_alvr.shards_count,
                                packet.header_alvr.next_packet_index, 
                                random_value, 
                                self.PL_probability,
                            );
                            self.blocked_packet_counter += 1;
                            false // Do not retain the packet
                        } else {
                            true // Retain the packet
                        }
                    });

                    // Move AMPDU to scheduled event instead of cloning
                    let ampdu_to_send = std::mem::replace(&mut self.aux_ampdu_serviced, AmpduPacket::new());
                    
                    context.scheduler
                        .schedule_event(
                            last_service_duration,
                            Self::send_ampdu,
                            ampdu_to_send,
                        )
                        .unwrap();
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
