use std::future::Future;
use std::time::{Duration, Instant};
use std::collections::VecDeque;

use rand::Rng; 

use asynchronix::model::{Context, Model};
use asynchronix::ports::Output;

// Constants for colors
const RESET: &str = "\x1B[0m";
const RED: &str = "\x1B[31m";
const GREEN: &str = "\x1B[32m";
const YELLOW: &str = "\x1B[33m";
const BLUE: &str = "\x1B[34m";
const MAGENTA: &str = "\x1B[35m";
const CYAN: &str = "\x1B[36m";

pub fn exponential(mean: f64) -> f64 {
    let mut rng = rand::thread_rng();
    -mean * (rng.gen::<f64>().ln())
}

// Remove Default derive for MpduPacket since Instant doesn't implement Default
#[derive(Debug, Clone, Copy)]
pub struct MpduPacket {
    pub packet_id: f64,
    pub length_packet: usize,
    pub queue_in_instant: Instant,
    pub queue_out_instant: Instant,
    pub sink_in_instant: Instant,
    pub T_q: Duration,
    pub T_s: Duration,
    pub expected_T_s: Duration,
}

impl MpduPacket {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            packet_id: 0.0,
            length_packet: 0,
            queue_in_instant: now,
            queue_out_instant: now,
            sink_in_instant: now,
            T_q: Duration::ZERO,
            T_s: Duration::ZERO,
            expected_T_s: Duration::ZERO,
        }
    }

    pub fn print(&self) {
        println!("MPDU Packet ID: {}, Length: {}", self.packet_id, self.length_packet);
    }
}

#[derive(Default)]
pub struct PoissonSource {
    pub output: Output<MpduPacket>,
    pub rate_bps: f64,
    pub arrival_rate: f64,
    pub mean_packet_length: usize,
}

impl PoissonSource {
    pub fn new(mean_packet_length: usize, rate_bps: f64) -> Self {
        let arrival_rate: f64 = rate_bps / (mean_packet_length as f64);
        Self {
            output: Default::default(),
            rate_bps,
            arrival_rate,
            mean_packet_length,
        }
    }

    pub fn start<'a>(&'a mut self, context: &'a Context<Self>) {
        let duration_until_next = Duration::from_secs_f64(exponential(1.0 / self.arrival_rate));
        context.scheduler.schedule_event(
            duration_until_next,
            Self::new_packet,
            (),
        ).unwrap();
    }

    async fn new_packet(&mut self, _: (), context: &Context<Self>) {
        let mut packet = MpduPacket::new();
        packet.length_packet = exponential(self.mean_packet_length as f64) as usize;

        self.output.send(packet).await;

        let duration_until_next = Duration::from_secs_f64(exponential(1.0 / self.arrival_rate));
        context.scheduler.schedule_event(
            duration_until_next,
            Self::new_packet,
            (),
        ).unwrap();
    }
}

impl Model for PoissonSource {}

#[derive(Default)]
pub struct QueueModule {
    pub queue: VecDeque<MpduPacket>,
    pub queue_maxsize: usize,
    pub output: Output<MpduPacket>,
    pub service_timer: Duration,
    pub aux_packet_serviced: MpduPacket,
    pub packet_being_served: bool,
    pub blocked_packet_counter: usize,
    pub arrived_packet_counter: usize,
    pub queue_length_counter: usize,
    pub arrival_rate: f64,
    pub service_rate: f64,
    pub rate_departures_bps: f64,
}

impl QueueModule {
    pub fn new(queue_size: usize, rate_departures_bps: f64) -> Self {
        Self {
            queue: VecDeque::new(),
            queue_maxsize: queue_size,
            output: Default::default(),
            service_timer: Duration::ZERO,
            aux_packet_serviced: MpduPacket::new(),
            packet_being_served: false,
            blocked_packet_counter: 0,
            arrived_packet_counter: 0,
            queue_length_counter: 0,
            arrival_rate: 0.0,
            service_rate: 0.0,
            rate_departures_bps,
        }
    }

    pub async fn input(&mut self, packet: MpduPacket, context: &Context<Self>) {
        self.arrived_packet_counter += 1;
        self.queue_length_counter += self.queue.len();

        if self.queue.len() < self.queue_maxsize {
            self.queue.push_back(packet);

            if self.queue.len() == 1 && !self.packet_being_served {
                self.deque_schedule_service((), context).await;
            }
        } else {
            self.blocked_packet_counter += 1;
        }
    }

    async fn deque_schedule_service(&mut self, _: (), context: &Context<Self>) {
        if let Some(packet) = self.queue.pop_front() {
            self.aux_packet_serviced = packet;
            self.aux_packet_serviced.queue_out_instant = Instant::now();
            self.aux_packet_serviced.T_q = self.aux_packet_serviced.queue_out_instant
                .duration_since(self.aux_packet_serviced.queue_in_instant);

            let time_of_service_secs = Duration::from_secs_f64(
                self.aux_packet_serviced.length_packet as f64 / self.rate_departures_bps
            );

            self.aux_packet_serviced.expected_T_s = time_of_service_secs;
            self.packet_being_served = true;

            context.scheduler.schedule_event(
                time_of_service_secs,
                Self::send_packet,
                (),
            ).unwrap();
        }
    }

    async fn send_packet(&mut self, _: (), context: &Context<Self>) {
        if self.packet_being_served {
            self.output.send(self.aux_packet_serviced).await;
            self.packet_being_served = false;
        }

        if !self.queue.is_empty() {
            self.deque_schedule_service((), context).await;
        }
    }
}

impl Model for QueueModule {}

#[derive(Default)]
pub struct Sink {
    pub received_packet_counter: usize,
    pub packet_length_counter: usize,
    pub system_time_counter: f64,
    pub queue_time_counter: f64,
    pub service_time_counter: f64,
    pub total_time_q_tx: f64,
    pub subsampling_counter: usize,
    pub subsampling_const: usize,
}

impl Sink {
    pub fn new() -> Self {
        Self {
            received_packet_counter: 0,
            packet_length_counter: 0,
            system_time_counter: 0.0,
            queue_time_counter: 0.0,
            service_time_counter: 0.0,
            total_time_q_tx: 0.0,
            subsampling_counter: 0,
            subsampling_const: 1,
        }
    }

    pub async fn input(&mut self, mut packet: MpduPacket) {
        let elapsed_time_f64 = (Instant::now() - packet.queue_in_instant).as_secs_f64();
        self.system_time_counter += elapsed_time_f64;
        self.packet_length_counter += packet.length_packet;
        self.received_packet_counter += 1;

        packet.sink_in_instant = Instant::now();
        packet.T_s = packet.sink_in_instant.duration_since(packet.queue_out_instant);

        self.queue_time_counter += packet.T_q.as_secs_f64();
        self.service_time_counter += packet.T_s.as_secs_f64();

        let packet_total_time_q_tx = Instant::now().duration_since(packet.queue_in_instant);

        self.subsampling_counter += 1;

        if self.subsampling_counter >= self.subsampling_const {
            self.subsampling_counter = 0;
            println!(
                "[DBG SINK {:.?} - L{}] T: {:.4}, T_q: {:.4}, T_s: {:.4} \n( E(T_s) = {:.4}, (T_q+T_s) = {:.4} )\n",
                Instant::now(),
                packet.length_packet,
                packet_total_time_q_tx.as_secs_f64(),
                packet.T_q.as_secs_f64(),
                packet.T_s.as_secs_f64(),
                packet.expected_T_s.as_secs_f64(),
                (packet.T_q.as_secs_f64() + packet.T_s.as_secs_f64())
            );
        }
    }
}

impl Model for Sink {}

fn main() {
    println!("Hi mom");
}