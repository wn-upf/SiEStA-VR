use std::future::Future;

use asynchronix::model::{Context, Model};
use asynchronix::ports::Output;
use asynchronix::simulation::{Mailbox, SimInit};
use asynchronix::time::MonotonicTime;
use tai_time::TaiTime;

use rand::thread_rng;
use rand_distr::{Distribution, Exp};
use std::cmp::{self};

use std::time::{Duration, Instant};

use std::collections::VecDeque;

mod libs; // for callign local library
use crate::libs::{compute_mm1k_metrics, CsvType};
use colored::*;

use std::cmp::max;

const DEBUG: bool = true; // Set to `false` to disable `debug_print!`

#[macro_export]
macro_rules! debug_print {
    ($color:expr, $fmt:expr, $($arg:tt)*) => {
        if DEBUG {
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

pub fn exponential(mean: f64) -> f64 {
    let mut rng = thread_rng();
    let exp = Exp::new(1.0 / mean).unwrap();
    let value = exp.sample(&mut rng);
    value
}

#[derive(Debug, Clone, Copy)]
pub struct MpduPacket {
    pub packet_id: usize,
    pub length_packet: usize,
    pub queue_in_instant: TaiTime<0>,
    pub queue_out_instant: TaiTime<0>,
    pub sink_in_instant: Instant,
    pub T_q: Duration,
    pub T_s: Duration,
    pub expected_T_s: Duration,
}

impl MpduPacket {
    pub fn new() -> Self {
        Self {
            packet_id: 0,
            length_packet: 0,
            queue_in_instant: TaiTime::default(),
            queue_out_instant: TaiTime::default(),
            sink_in_instant: Instant::now(),
            T_q: Duration::ZERO,
            T_s: Duration::ZERO,
            expected_T_s: Duration::ZERO,
        }
    }

    pub fn print(&self) {
        println!("Packet ID: {}, L: {}", self.packet_id, self.length_packet);
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
            let mut time_interarrival_bf =
                Duration::from_secs_f64(exponential(1.0 / self.arrival_rate));
            let time_interarrival = max(time_interarrival_bf, Duration::from_nanos(10));

            println!(
                "[SUPERDEBUG] Time_inter : {} , max: {} ",
                time_interarrival_bf.as_secs_f64(),
                time_interarrival.as_secs_f64()
            );

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
    pub output_port: Output<MpduPacket>,
    pub queue: VecDeque<MpduPacket>,
    pub queue_maxsize: usize,
    pub service_timer: Duration,
    pub aux_packet_serviced: MpduPacket,
    pub packet_being_served: bool,
    pub blocked_packet_counter: usize,
    pub arrived_packet_counter: usize,
    pub queue_length_counter: usize,
    pub arrival_rate: f64,
    pub service_rate: f64,
    pub rate_departures_bps: f64,
    pub t0_time: Instant,

    pub csv_metrics: CsvType,
}

impl QueueModule {
    pub fn new(queue_size: usize, rate_departures_bps: f64) -> Self {
        Self {
            queue: VecDeque::new(),
            queue_maxsize: queue_size,
            output_port: Default::default(),
            service_timer: Duration::ZERO,
            aux_packet_serviced: MpduPacket::new(),
            packet_being_served: false,
            blocked_packet_counter: 0,
            arrived_packet_counter: 0,
            queue_length_counter: 0,
            arrival_rate: 0.0,
            service_rate: 0.0,
            rate_departures_bps,
            t0_time: Instant::now(),

            csv_metrics: CsvType::new(),
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
                    "{} [DBG SERVE] --Packet {} sent, Q_size = {}",
                    format_elapsed!(elapsed),
                    self.aux_packet_serviced.packet_id,
                    self.queue.len()
                );
                // self.aux_packet_serviced.print();
                self.output_port.send(self.aux_packet_serviced).await;
                self.aux_packet_serviced = MpduPacket::new();
                self.packet_being_served = false;
            }

            if let Some(packet) = self.queue.pop_front() {
                let now: tai_time::TaiTime<0> = context.scheduler.time();

                let mut serviced_packet = packet.clone();
                serviced_packet.queue_out_instant = now;
                serviced_packet.T_q = now.duration_since(serviced_packet.queue_in_instant);
                let elapsed: tai_time::TaiTime<0> = context.scheduler.time();

                // println!("Length packet: {}", serviced_packet.length_packet);
                let time_of_service_secs = Duration::from_secs_f64(
                    serviced_packet.length_packet as f64 / self.rate_departures_bps,
                );

                serviced_packet.expected_T_s = time_of_service_secs;

                debug_print!(
                    DebugColor::Yellow,
                    "{} [DBG DEQUE] -Packet {} dequeued, length: {}, Q_size = {}, T_q = {}, exp_T_s = {}",
                    format_elapsed!(elapsed),
                    serviced_packet.packet_id,
                    serviced_packet.length_packet,
                    self.queue.len(),
                    serviced_packet.T_q.as_secs_f32(),
                    serviced_packet.expected_T_s.as_secs_f32(),
                );
                self.packet_being_served = true;
                self.aux_packet_serviced = serviced_packet.clone();
                let elapsed = context.scheduler.time();
                self.csv_metrics.update_stats(
                    elapsed,
                    serviced_packet.packet_id,
                    self.queue.len(),
                    serviced_packet.expected_T_s.as_secs_f64(),
                    serviced_packet.T_q.as_secs_f64(),
                    serviced_packet.length_packet,
                );

                context
                    .scheduler
                    .schedule_event(time_of_service_secs, Self::deque_schedule_service, ())
                    .unwrap();
            }
        }
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

    pub async fn input(&mut self, packet: MpduPacket, context: &Context<Self>) {
        let elapsed = context.scheduler.time();
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

impl Model for Sink {}

fn main() {
    // DEFINE SIM PARAMS

    let mean_length: f64 = 12000.0;
    let k_queue: usize = 100;
    let rate_bps = 5E4;
    let rate_queue_bps: f64 = 6E8;

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

    // let mut simu = SimInit::new()
    // .add_model(source, mbox_src, "Source")
    // .add_model(sink, sink_mbox, "Sink")
    // .init(t0);

    let mut simu = SimInit::new()
        .add_model(source, mbox_src, "Poisson")
        .add_model(queue, mbox_queue, "Queue")
        .add_model(sink, sink_mbox, "Sink")
        .init(t0);

    // let clock = NoClock::new();

    // let mut simu = SimInit::new()
    // .set_clock(clock)
    // .add_model(source, mbox_src, "Poisson")
    // .add_model(queue, mbox_queue, "Queue")
    // .add_model(sink, sink_mbox, "Sink")
    // .init(t0);
    // ;

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

    // DUMP CSV METRICS

    // fn dump_csvs(queue: QueueModule, T_end: f64, k_queue: usize){

    //     let filename = format!("Results/QUEUE_T{}_K{}.csv", T_end, k_queue) ;
    //     let mut file = File::create(&filename)?;

    //     writeln!(file, "timestamp,packet_ID,queue_size,L_packet,T_q,T_s");

    //     for i in 0..queue.csv_metrics.v_timestamp.len(){

    //         writeln!(file, "{},{},{},{},{},{}",
    //             queue.csv_metrics.v_timestamp[i],
    //             queue.csv_metrics.v_packet_id[i],
    //             queue.csv_metrics.v_queue_size[i],
    //             queue.csv_metrics.v_packet_l[i],
    //             queue.csv_metrics.v_queue_tq[i],
    //             queue.csv_metrics.v_queue_ts[i]
    //         );
    //     }
    //     println!("QUEUE CSV file has been created successfully.");

    // }

    // dump_csvs(queue.clone(), stoptime as f64, k_queue);
}
