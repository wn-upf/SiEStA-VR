use std::future::Future;

use asynchronix::model::{Context, InitializedModel, Model};
use asynchronix::ports::{EventBuffer, Output};
use asynchronix::simulation::{Mailbox, SimInit};
use asynchronix::time::MonotonicTime;

use std::cmp::{self, max, min}; 
use rand::thread_rng;
use rand_distr::{Exp, Distribution}; 

use std::time::{Instant, Duration}; 

use std::collections::VecDeque;

mod libs;                // for callign local library
use crate::libs::{LittleTheoremMM1K, compute_mm1k_metrics}; 
use colored::*;
use std::fmt::Display;

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

pub fn exponential(mean: f64) -> f64{
    let mut rng = thread_rng(); 
    let exp = Exp::new(1.0 / mean).unwrap(); 
    let value = exp.sample(&mut rng); 
    value
}


fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs() as f64 + duration.subsec_nanos() as f64 / 1_000_000_000.0;
    // Format to a string with six decimal places
    format!("{:.6}", seconds)
}

#[derive(Debug, Clone, Copy)]
pub struct MpduPacket {
    pub packet_id: usize,
    pub length_packet: usize,
    pub queue_in_instant: Instant,
    pub queue_out_instant: Instant,
    pub sink_in_instant: Instant,
    pub T_q: Duration,
    pub T_s: Duration,
    pub expected_T_s: Duration,
}

impl MpduPacket{
    pub fn new() -> Self {
        Self {
            packet_id : 0, 
            length_packet: 0, 
            queue_in_instant:  Instant::now(),
            queue_out_instant: Instant::now(),
            sink_in_instant: Instant::now(),
            T_q: Duration::ZERO,
            T_s: Duration::ZERO,
            expected_T_s: Duration::ZERO,
        }
    }

    pub fn print(&self){
        println!("Packet ID: {}, L: {}", self.packet_id, self.length_packet); 
    }
}


pub struct PoissonSource{

    pub arrival_rate: f64, 
    pub mean_length_packets: f64, 

    pub output_port: Output<MpduPacket>, 

    pub num_packets_sent: usize, 

} 

impl PoissonSource{


    pub fn new(arrival_rate: f64, mean_length: f64) -> Self{
        Self {
            arrival_rate: arrival_rate, 
            mean_length_packets: mean_length, 
            output_port: Default::default(), 
            num_packets_sent: 0, 
         }
    }

    fn send_packet<'a> (
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {

        async move {

            let mut packet =  MpduPacket::new() ; 

            let time_interarrival = Duration::from_secs_f64(exponential(1.0/ self.arrival_rate)) ;
            let len_random = exponential(self.mean_length_packets as f64) as usize; 
            packet.length_packet = cmp::max(1, len_random); 


            self.num_packets_sent += 1; 
            packet.packet_id = self.num_packets_sent; 
            self.output_port.send(packet.clone()).await; 

            context.scheduler.schedule_event(time_interarrival, Self::send_packet, () ).unwrap(); 
        
        }
    }
}

impl Model for PoissonSource{} 


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
        }
    }

    pub async fn input(&mut self, packet: MpduPacket, context: &Context<Self>) {
        
        self.arrived_packet_counter += 1;
        self.queue_length_counter += self.queue.len();
     
        if self.queue.len() < self.queue_maxsize {
            self.queue.push_back(packet);
            let elapsed =Instant::now().duration_since(self.t0_time); 
            debug_print!(
                DebugColor::Blue,
                "{} [DBG QUEUE] Packet {} arrives, Q_size = {}",
                format_duration(elapsed),
                packet.packet_id,
                self.queue.len()
            ); 

            if self.queue.len() == 1 && !self.packet_being_served {
                self.deque_schedule_service((), context).await;
            }
        } else {
            self.blocked_packet_counter += 1;
            let elapsed = Instant::now().duration_since(self.t0_time); 
            debug_print!(
                DebugColor::Red,
                "{} [DBG FULL QUEUE] Packet {} DROPPED!! , Q_size = {}",
                format_duration(elapsed),
                packet.packet_id,
                self.queue.len()
            ); 
        }
    }

    fn deque_schedule_service<'a> (
            &'a mut self,
            _: (),
            context: &'a Context<Self>,
        ) -> impl Future<Output = ()> + Send + 'a {
    
        async move {
            if self.packet_being_served == true {

                let elapsed = Instant::now().duration_since(self.t0_time); 
                debug_print!(
                    DebugColor::Magenta,
                    "{} [DBG SERVE] --Packet {} sent, Q_size = {}",
                    format_duration(elapsed),
                    self.aux_packet_serviced.packet_id,
                    self.queue.len()
                );
                // self.aux_packet_serviced.print(); 
                self.output_port.send(self.aux_packet_serviced).await; 
                self.aux_packet_serviced = MpduPacket::new(); 
                self.packet_being_served = false; 
            }

            if let Some(packet) = self.queue.pop_front() {
                let now = Instant::now();
                let mut serviced_packet = packet;
                serviced_packet.queue_out_instant = now;
                serviced_packet.T_q = now.duration_since(serviced_packet.queue_in_instant);
                let elapsed = now.duration_since(self.t0_time); 
                debug_print!(
                    DebugColor::Yellow,
                    "{} [DBG DEQUE] -Packet {} dequeued, length: {}, Q_size = {}",
                    format_duration(elapsed),
                    serviced_packet.packet_id,
                    serviced_packet.length_packet,
                    self.queue.len()
                ); 
                // println!("Length packet: {}", serviced_packet.length_packet); 
                let time_of_service_secs = Duration::from_secs_f64(
                    serviced_packet.length_packet as f64 / self.rate_departures_bps
                );

                serviced_packet.expected_T_s = time_of_service_secs;
                self.packet_being_served = true;
                self.aux_packet_serviced = serviced_packet;

                context.scheduler.schedule_event(
                    time_of_service_secs,
                    Self::deque_schedule_service, () ).unwrap(); 
            }
        }
    }

    
}

impl Model for QueueModule {}


// #[derive(Default)]
pub struct Sink{
    // pub input: Input <MpduPacket>, 
    pub received_packet_counter: usize, 
    pub t0_sink:    Instant, 
    // pub packet_length_counter: usize, 
    
    // pub system_time_counter : f64, 
    // pub queue_time_counter:     f64,
    // pub service_time_counter:   f64, 
    // pub total_time_q_tx :       f64, 

    // pub subsampling_counter: usize, 
    // pub subsampling_const:  usize,  

}

impl Sink {

    pub fn new() -> Self{
        Self{
            received_packet_counter :   0, 
            t0_sink:                    Instant::now(), 
        }
    }

    pub async fn input(&mut self, packet: MpduPacket){

        let elapsed = self.t0_sink.elapsed(); 
        debug_print!(
            DebugColor::Red,
            "{} [DBG SINK]  --- Packet {} received, Length: {}",
            format_duration(elapsed),
            packet.packet_id,
            packet.length_packet
        );
        // println!("{} - Packet received!!", format_duration(elapsed)); 
        // packet.print(); 
        self.received_packet_counter += 1;  
    }
}

impl Model for Sink {}

fn main( ){
    // DEFINE SIM PARAMS
    let mean_length: f64 = 1000.0; 
    let rate_bps = 20.0; 

    let k_queue: usize = 100; 
    let rate_queue_bps:f64 = 20000.0; 


    let LT = compute_mm1k_metrics(rate_bps, mean_length, rate_queue_bps, k_queue); 



    //// DEFINE COMPONENTS
    let mut source = PoissonSource::new(rate_bps, mean_length); 
    let mut queue: QueueModule = QueueModule::new(k_queue-1 as usize, rate_queue_bps) ; 
    let mut sink = Sink::new() ; 

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

    let scheduler = simu.scheduler(); 
    // ----------
    // Simulation.
    // ----------

    // Check initial conditions.

    let mut t = t0; 
    assert_eq!(simu.time(), t); 





    // START WITH FIRST EVENT
    scheduler.schedule_event(
        Duration::from_secs(1),
        PoissonSource::send_packet,
        (), 
        &mbox_src_address,  
    ) 
    .unwrap(); 


    for i in 0..100{ // CARRY ON 
        simu.step(); 

    }
    // t += Duration::new(3, 0 ); 
    // assert_eq!(simu.time(), t); 

}

   