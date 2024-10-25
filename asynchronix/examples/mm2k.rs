use std::time::{Duration, Instant};
use std::collections::VecDeque;
use rand::Rng;
use asynchronix::model::{Context, Model};
use asynchronix::ports::Output;

use rand_distr::{Exp, Distribution}; // Make sure to add `rand_distr` crate for exponential sampling

// Constants remain unchanged
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

// MpduPacket struct and impl remain unchanged
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

use rand_distr::{Exp, Distribution};
use std::time::Duration;

pub struct PoissonSource {
    pub output: Output<MpduPacket>,
    pub rate_bps: f64,
    pub arrival_rate: f64,
    pub mean_packet_length: usize,
}

impl PoissonSource {
    pub fn new(mean_packet_length: usize, rate_bps: f64) -> Self {
        let arrival_rate = rate_bps / (mean_packet_length as f64);
        Self {
            output: Default::default(),
            rate_bps,
            arrival_rate,
            mean_packet_length,
        }
    }

    pub fn start(&mut self, context: &Context<Self>) {
        let duration_until_next = self.get_next_duration();
        context.scheduler.schedule_event(
            duration_until_next,
            |this: &mut Self, ctx: &Context<Self>| {
                this.schedule_next(ctx);
            },
            (),
        );
    }

    fn get_next_duration(&self) -> Duration {
        let exp_dist = Exp::new(1.0 / self.arrival_rate).expect("Invalid rate");
        Duration::from_secs_f64(exp_dist.sample(&mut rand::thread_rng()))
    }

    fn schedule_next(&mut self, context: &Context<Self>) {
        let duration_until_next = self.get_next_duration();
        // Schedule the event without async
        context.scheduler.schedule_event(
            duration_until_next,
            |this: &mut Self, ctx: &Context<Self>| {
                // Call generate_packet asynchronously in a separate task
                let _ = tokio::spawn(this.generate_packet(ctx.clone()));
            },
            (),
        );
    }

    async fn generate_packet(&mut self, context: &Context<Self>) {
        let mut packet = MpduPacket::new();
        let exp_dist = Exp::new(1.0 / (self.mean_packet_length as f64)).expect("Invalid mean packet length");
        packet.length_packet = exp_dist.sample(&mut rand::thread_rng()) as usize;

        self.output.send(packet).await;

        // Call schedule_next to re-schedule itself
        self.schedule_next(context);
    }
}

impl Model for PoissonSource {}

// pub struct QueueModule {
//     pub queue: VecDeque<MpduPacket>,
//     pub queue_maxsize: usize,
//     pub output: Output<MpduPacket>,
//     pub service_timer: Duration,
//     pub aux_packet_serviced: Option<MpduPacket>,
//     pub packet_being_served: bool,
//     pub blocked_packet_counter: usize,
//     pub arrived_packet_counter: usize,
//     pub queue_length_counter: usize,
//     pub arrival_rate: f64,
//     pub service_rate: f64,
//     pub rate_departures_bps: f64,
// }

// impl QueueModule {
//     pub fn new(queue_size: usize, rate_departures_bps: f64) -> Self {
//         Self {
//             queue: VecDeque::new(),
//             queue_maxsize: queue_size,
//             output: Default::default(),
//             service_timer: Duration::ZERO,
//             aux_packet_serviced: None,
//             packet_being_served: false,
//             blocked_packet_counter: 0,
//             arrived_packet_counter: 0,
//             queue_length_counter: 0,
//             arrival_rate: 0.0,
//             service_rate: 0.0,
//             rate_departures_bps,
//         }
//     }

//     pub async fn input(&mut self, packet: MpduPacket, context: &Context<Self>) {
//         self.arrived_packet_counter += 1;
//         self.queue_length_counter += self.queue.len();

//         if self.queue.len() < self.queue_maxsize {
//             self.queue.push_back(packet);

//             if self.queue.len() == 1 && !self.packet_being_served {
//                 self.deque_schedule_service(context);
//             }
//         } else {
//             self.blocked_packet_counter += 1;
//         }
//     }

//     fn deque_schedule_service(&mut self, context: &Context<Self>) {
//         if let Some(packet) = self.queue.pop_front() {
//             let now = Instant::now();
//             let mut serviced_packet = packet;
//             serviced_packet.queue_out_instant = now;
//             serviced_packet.T_q = now.duration_since(serviced_packet.queue_in_instant);

//             let time_of_service_secs = Duration::from_secs_f64(
//                 serviced_packet.length_packet as f64 / self.rate_departures_bps
//             );

//             serviced_packet.expected_T_s = time_of_service_secs;
//             self.packet_being_served = true;
//             self.aux_packet_serviced = Some(serviced_packet);

//             context.scheduler.schedule_event(
//                 time_of_service_secs,
//                 |this: &mut Self, ctx: &Context<Self>| async move {
//                     this.send_packet(ctx).await;
//                 },
//                 (),
//             );
//         }
//     }

//     async fn send_packet(&mut self, context: &Context<Self>) {
//         if self.packet_being_served {
//             if let Some(packet) = self.aux_packet_serviced.take() {
//                 self.output.send(packet).await;
//             }
//             self.packet_being_served = false;
//         }

//         if !self.queue.is_empty() {
//             self.deque_schedule_service(context);
//         }
//     }
// }

// impl Model for QueueModule {}

// // Sink implementation remains unchanged
// pub struct Sink {
//     pub received_packet_counter: usize,
//     pub packet_length_counter: usize,
//     pub system_time_counter: f64,
//     pub queue_time_counter: f64,
//     pub service_time_counter: f64,
//     pub total_time_q_tx: f64,
//     pub subsampling_counter: usize,
//     pub subsampling_const: usize,
// }

// impl Sink {
//     pub fn new() -> Self {
//         Self {
//             received_packet_counter: 0,
//             packet_length_counter: 0,
//             system_time_counter: 0.0,
//             queue_time_counter: 0.0,
//             service_time_counter: 0.0,
//             total_time_q_tx: 0.0,
//             subsampling_counter: 0,
//             subsampling_const: 1,
//         }
//     }

//     pub async fn input(&mut self, mut packet: MpduPacket) {
//         let elapsed_time_f64 = (Instant::now() - packet.queue_in_instant).as_secs_f64();
//         self.system_time_counter += elapsed_time_f64;
//         self.packet_length_counter += packet.length_packet;
//         self.received_packet_counter += 1;

//         packet.sink_in_instant = Instant::now();
//         packet.T_s = packet.sink_in_instant.duration_since(packet.queue_out_instant);

//         self.queue_time_counter += packet.T_q.as_secs_f64();
//         self.service_time_counter += packet.T_s.as_secs_f64();

//         let packet_total_time_q_tx = Instant::now().duration_since(packet.queue_in_instant);

//         self.subsampling_counter += 1;

//         if self.subsampling_counter >= self.subsampling_const {
//             self.subsampling_counter = 0;
//             println!(
//                 "[DBG SINK {:.?} - L{}] T: {:.4}, T_q: {:.4}, T_s: {:.4} \n( E(T_s) = {:.4}, (T_q+T_s) = {:.4} )\n",
//                 Instant::now(),
//                 packet.length_packet,
//                 packet_total_time_q_tx.as_secs_f64(),
//                 packet.T_q.as_secs_f64(),
//                 packet.T_s.as_secs_f64(),
//                 packet.expected_T_s.as_secs_f64(),
//                 (packet.T_q.as_secs_f64() + packet.T_s.as_secs_f64())
//             );
//         }
//     }
// }

impl Model for Sink {}

fn main() {
    println!("Hi!");
}
