use std::future::Future;

use asynchronix::model::{Context, Model};
use asynchronix::ports::Output;
use asynchronix::simulation::{Mailbox, SimInit};
use asynchronix::time::MonotonicTime;

use rand::thread_rng;
use rand_distr::{Distribution, Exp};
use std::cmp::{self};

use std::time::{Duration, Instant};

use std::collections::VecDeque;
use std::cmp::{min, max}; 
mod libs; // for callign local library
use crate::libs::{
    compute_mm1k_metrics, frametransmission_delay, Coords, CsvType, ResultsFrameTXDelay,
};

use crate::libs::{AmpduPacket, MpduPacket};

use colored::*;

const DEFAULT_TMAX_AGG: f64 = 4.85E-3;
const MAX_AMPDU_SIZE: i32 = 64;

#[macro_export]
macro_rules! format_elapsed {
    ($elapsed:expr) => {{
        let total_seconds =
            $elapsed.as_secs() as f64 + ($elapsed.subsec_nanos() as f64 / 1_000_000_000.0);
        format!("{:.9}", total_seconds)
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

#[macro_export]
macro_rules! debug_print {
    ($color:expr, $fmt:expr, $($arg:tt)*) => {
        let msg = format!($fmt, $($arg)*);
        println!("{}", $color.to_color_fn()(msg));
    };
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

pub fn exponential(mean: f64) -> f64 {
    let mut rng = thread_rng();
    let exp = Exp::new(1.0 / mean).unwrap();
    let value = exp.sample(&mut rng);
    value
}

pub struct PoissonSource {
    pub arrival_rate: f64,
    pub mean_length_packets: f64,

    pub output_port: Output<MpduPacket>,

    pub num_packets_sent: usize,
}

impl PoissonSource {
    pub fn new(arrival_rate: f64, mean_length: f64) -> Self {
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

            let mut time_interarrival = Duration::from_secs_f64(exponential(1.0 / self.arrival_rate));
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
    pub arrival_rate: f64,
    pub service_rate: f64,
    pub rate_departures_bps: f64,
    pub t0_time: Instant,

    pub csv_metrics: CsvType,

    pub coords_queue: Coords,
    pub p_tx: f64,
}

impl QueueModule {
    pub fn new(queue_size: usize, rate_departures_bps: f64) -> Self {
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
            arrival_rate: 0.0,
            service_rate: 0.0,
            rate_departures_bps,
            t0_time: Instant::now(),

            csv_metrics: CsvType::new(),

            coords_queue: Coords::new(),
            p_tx: 20.0,
        }
    }

    pub async fn input(&mut self, mut packet: MpduPacket, context: &Context<Self>) {
        self.arrived_packet_counter += 1;
        self.queue_length_counter += self.queue.len();

        if self.queue.len() < self.queue_maxsize {
            packet.queue_in_instant = context.scheduler.time();
            self.queue.push_back(packet);
            let elapsed = context.scheduler.time();

            debug_print!(
                DebugColor::Blue,
                "{} [DBG QUEUE] Packet {} arrives, Q_size = {}",
                format_elapsed!(elapsed),
                packet.packet_id,
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

    fn deque_schedule_service<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            if self.packet_being_served == true {
                let elapsed = context.scheduler.time();
                debug_print!(
                    DebugColor::Magenta,
                    "{} [DBG SERVE] --AMPDU {} sent, Q_size = {}",
                    format_elapsed!(elapsed),
                    self.aux_ampdu_serviced.sta_id,
                    self.queue.len()
                );
                // self.aux_packet_serviced.print();
                self.output_port.send(self.aux_ampdu_serviced.clone()).await;
                self.aux_ampdu_serviced.reset();
                self.packet_being_served = false;
            }

            if let Some(first_packet) = self.queue.pop_front() {
                // DEQUE PACKET IF ANY IN QUEUE

                
                let mut first_packet_mut = first_packet.clone();
                //ampdu code
                let now: tai_time::TaiTime<0> = context.scheduler.time();
                self.aux_ampdu_serviced.sta_id = first_packet_mut.sta_dest_id;
                self.aux_ampdu_serviced.coordinates = first_packet_mut.sta_coords.clone();

                first_packet_mut.queue_out_instant = now;

                self.aux_ampdu_serviced.mpdu_packets.push(first_packet_mut); // put packet in AMPDU 
                self.aux_ampdu_serviced.total_length += first_packet_mut.length_packet;
                self.aux_ampdu_serviced.size += 1;

                let mut packets_to_remove = Vec::new();
                let mut resulting_delays = frametransmission_delay(first_packet_mut.length_packet as f64, MAX_AMPDU_SIZE, self.coords_queue, first_packet_mut.sta_coords, self.p_tx); 
                let mut service_duration =  Duration::from_secs_f64(resulting_delays.service_delay) ; 

                for (index, packet) in self.queue.iter().enumerate() {
                    if packet.sta_dest_id != self.aux_ampdu_serviced.sta_id {
                        continue;
                    }

                    resulting_delays = frametransmission_delay(
                        self.aux_ampdu_serviced.total_length as f64,
                        MAX_AMPDU_SIZE as i32,
                        self.coords_queue,
                        self.aux_ampdu_serviced.coordinates,
                        self.p_tx,
                    );

                    if resulting_delays.service_delay >= DEFAULT_TMAX_AGG
                        || self.aux_ampdu_serviced.size >= MAX_AMPDU_SIZE as i32
                    {
                        break;
                    }

                    // self.queue.remove(packet_index_loop );

                    packets_to_remove.push(index);
                    service_duration = Duration::from_secs_f64(resulting_delays.service_delay);
                }

                for &index in packets_to_remove.iter().rev() {
                    // remove packets from queue
                    if let Some(mut packet) = self.queue.remove(index) {
                        packet.queue_out_instant = now;

                        debug_print!(
                            DebugColor::Yellow,
                            "{} [DBG DEQUE] -Packet {} dequeued and put in AMPDU, Q_size = {}",
                            format_elapsed!(now),
                            packet.packet_id,
                            self.queue.len(),
                        );

                        self.aux_ampdu_serviced.mpdu_packets.push(packet);
                        self.aux_ampdu_serviced.total_length += packet.length_packet;
                        self.aux_ampdu_serviced.size += 1;
                    }
                }

                for packet in self.aux_ampdu_serviced.mpdu_packets.iter_mut() {
                    // update metrics for all packets to be serviced in AMPDU
                    packet.expected_T_s = service_duration;

                    let packet_queue_time = packet
                        .queue_out_instant
                        .duration_since(packet.queue_in_instant);

                    self.csv_metrics.update_stats(
                        now,
                        packet.packet_id,
                        self.queue.len(),
                        packet.expected_T_s.as_secs_f64(),
                        packet_queue_time.as_secs_f64(),
                        packet.length_packet,
                    );
                }

                debug_print!(
                    DebugColor::Yellow,
                    "{} [DBG AMPDU] --Dequeueing AMPDU, serviced at {}",
                    format_elapsed!(now),
                    format_elapsed!(now + service_duration),
                );
                self.packet_being_served = true; 
                context
                    .scheduler
                    .schedule_event(service_duration, Self::deque_schedule_service, ())
                    .unwrap();
            } // queuesize>= 1
        } // async mv
    }
}

impl Model for QueueModule {}

// #[derive(Default)]
pub struct Sink {
    // pub input: Input <MpduPacket>,
    pub received_packet_counter: usize,
    pub t0_sink: Instant,
    // pub packet_length_counter: usize,

    // pub system_time_counter : f64,
    // pub queue_time_counter:     f64,
    // pub service_time_counter:   f64,
    // pub total_time_q_tx :       f64,

    // pub subsampling_counter: usize,
    // pub subsampling_const:  usize,
}

impl Sink {
    pub fn new() -> Self {
        Self {
            received_packet_counter: 0,
            t0_sink: Instant::now(),
        }
    }

    pub async fn input(&mut self, ampdu_packet: AmpduPacket, context: &Context<Self>) {
        let elapsed = context.scheduler.time();

        for packet in ampdu_packet.mpdu_packets {
            debug_print!(
                DebugColor::Red,
                "{} [DBG SINK]  ---Packet {} at sink",
                format_elapsed!(elapsed),
                packet.packet_id,
            );
            // println!("{} - Packet received!!", format_duration(elapsed));
            // packet.print();
            self.received_packet_counter += 1;
        }
    }
}

impl Model for Sink {}

fn main() {
    // DEFINE SIM PARAMS
    let mean_length: f64 = 1000.0;

    let k_queue: usize = 100;
    let rate_bps = 200000.0;

    let rate_queue_bps: f64 = 20000.0;

    let LT = compute_mm1k_metrics(rate_bps, mean_length, rate_queue_bps, k_queue);

    //// DEFINE COMPONENTS
    let mut source = PoissonSource::new(rate_bps, mean_length);
    let mut queue: QueueModule = QueueModule::new(k_queue - 1 as usize, rate_queue_bps);
    let mut sink = Sink::new();

    let mbox_src = Mailbox::new();
    let mbox_src_address = mbox_src.address();

    let mbox_queue = Mailbox::new();
    let queue_address = mbox_queue.address();

    let sink_mbox = Mailbox::new();
    let sink_mbox_address = sink_mbox.address();

    // CONNECT COMPONENTS
    // source.output_port.connect(Sink::input, &sink_mbox);

    source.output_port.connect(QueueModule::input, &mbox_queue);
    queue.output_port.connect(Sink::input, &sink_mbox);

    let t0 = MonotonicTime::EPOCH;

    let mut simu = SimInit::with_num_threads(1)
        .add_model(source, mbox_src, "Poisson")
        .add_model(queue, mbox_queue, "Queue")
        .add_model(sink, sink_mbox, "Sink")
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
            Duration::from_secs(1),
            PoissonSource::send_packet,
            (),
            &mbox_src_address,
        )
        .unwrap();

    let stoptime = 1E3;
    simu.step_by(Duration::from_secs_f64(stoptime)); //works

    // // for i in 0..stoptime{                          //also works
    // //     simu.step();
    // // }

    println!("************ END RESULTS ***********\n LT: {:#?}", LT);


}
