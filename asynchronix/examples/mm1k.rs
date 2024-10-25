use std::future::Future;
use std::time::{Duration, Instant};
use std::collections::VecDeque; 

use asynchronix::model::{Context, InitializedModel, Model};
use asynchronix::ports::{EventBuffer, Output};
use asynchronix::simulation::{Mailbox, SimInit};
use asynchronix::time::MonotonicTime;

use asynchronix::simulation::LocalScheduler;
// use std::task::Context;
use std::fmt;
use std::f64; // Import standard f64 library for mathematical functions
use asynchronix::simulation::Scheduler;

// Define constants for colors
const RESET: &str = "\x1B[0m";
const RED: &str = "\x1B[31m";
const GREEN: &str = "\x1B[32m";
const YELLOW: &str = "\x1B[33m";
const BLUE: &str = "\x1B[34m";
const MAGENTA: &str = "\x1B[35m";
const CYAN: &str = "\x1B[36m";

// // #[cfg(debug_assertions)] // This will only compile in debug mode
// macro_rules! printf_color {
//     ($color:expr, $fmt:expr $(, $($arg:tt)*)?) => {
//         print!("{}{}{}", $color, format!($fmt, $($arg)*), RESET);
//     };
// }
pub fn exponential(mean: f64) -> f64 {

    let mut rng = rand::thread_rng(); 
    - mean * (rng.gen::<f64>().ln())
}

// pub fn exponential<T>(mean: T) -> f64
// where
//     T: Float + Into<f64>, // Restrict to types that implement Float and can be converted to f64
// {
//     let mut rng = rand::thread_rng();
//     -mean.into() * (rng.gen::<f64>().ln())
// }


#[derive(Debug, Clone, Copy)]
pub struct MpduPacket {
    pub packet_id: f64,          // Equivalent to long double in C++
    pub length_packet: usize,             // Packet Length
    
    pub queue_in_instant: Instant,
    pub queue_out_instant: Instant, 
    pub sink_in_instant: Instant,

    pub T_q: Duration, 
    pub T_s: Duration,
    pub expected_T_s: Duration,  
}



impl MpduPacket {
    // Method to print MpduPacket values

    pub fn default() -> Self{
        Self {
            packet_id: 0.0,                          // Default value for packet_id
            length_packet: 0,                        // Default value for length_packet
            queue_in_instant: Instant::now(),       // Current time as default
            queue_out_instant: Instant::now(),      // Current time as default
            sink_in_instant: Instant::now(),        // Current time as default
            T_q: Duration::new(0, 0),                // Default value for T_q
            T_s: Duration::new(0, 0),                // Default value for T_s
            expected_T_s: Duration::new(0, 0),       // Default value for expected_T_s
        }
    }
    pub fn print(&self) {
        println!("MPDU Packet ID: {}, Length: {}", self.packet_id, self.length_packet);
    }
}

#[derive(Default)]
pub struct PoissonSource{

    pub output: Output <MpduPacket>,

    pub rate_bps : f64, 
    pub arrival_rate: f64, 
    pub mean_packet_length: usize, 

}

impl PoissonSource {
    pub fn new(mean_packet_length: usize, rate_bps: f64 ) -> Self{
        
        let arrival_rate: f64 = rate_bps / (mean_packet_length as f64);     
        
        Self{
            output: Default::default(), 
            rate_bps,
            arrival_rate,
            mean_packet_length
        }
    }

    pub fn start(&mut self, scheduler: &Scheduler<Self> ){
        scheduler.schedule_event(Duration::from_secs_f64(exponential(1.0/self.arrival_rate) as f64) , Self::new_packet, () ).unwrap(); 
    }

    // pub fn new_packet <'a>(&'a mut self, scheduler: &Scheduler<Self>){
    //     let mut packet: MpduPacket; 
    //     packet.length_packet = exponential(self.mean_packet_length as f64) as usize ; 

    //     self.output.send(packet); 
    //     scheduler.schedule_event(Duration::from_secs_f64(exponential(1.0/self.arrival_rate) as f64 ),
    //                             Self::new_packet,
    //                             (scheduler))
    //                             .unwrap(); 
    // }
    fn new_packet<'a>(
            &'a mut self, 
            _: (), 
            context: &'a Context<Self>, 
            ) -> impl Future<Output = ()> + Send + 'a {

        async move {

            let mut packet = MpduPacket::default();           
            let duration_until_next = Duration::from_secs_f64(exponential(1.0/self.arrival_rate) as f64 ); 
            packet.length_packet = exponential(self.mean_packet_length as f64) as usize ; 

            self.output.send(packet); 
            context
                .scheduler
                .schedule_event(duration_until_next, Self::new_packet, ())
                .unwrap(); 
        }
    }


}
impl Model for PoissonSource{}

// fn send_pulse<'a>(
//     &'a mut self,
//     _: (),
//     context: &'a Context<Self>,
// ) -> impl Future<Output = ()> + Send + 'a {
//     println!(
//         "Model instance {} at time {}: sending pulse",
//         context.name(),
//         context.scheduler.time()
//     );

//     async move {
//         let current_out = match self.next_phase {
//             0 => (self.current, 0.0),
//             1 => (0.0, self.current),
//             2 => (-self.current, 0.0),
//             3 => (0.0, -self.current),
//             _ => unreachable!(),
//         };
//         self.current_out.send(current_out).await;

//         if self.pps == 0.0 {
//             return;
//         }

//         self.next_phase = (self.next_phase + (self.pps.signum() + 4.0) as u8) % 4;

//         let pulse_duration = Duration::from_secs_f64(1.0 / self.pps.abs());

//         // Schedule the next pulse.
//         context
//             .scheduler
//             .schedule_event(pulse_duration, Self::send_pulse, ())
//             .unwrap();
//     }
// }



#[derive(Default)]
pub struct QueueModule {
    pub queue: VecDeque<MpduPacket>,
    pub queue_maxsize: usize,

    pub output: Output <MpduPacket>, 
    
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

    pub fn new(queue_size: usize, rate_departures_bps: f64) -> Self{
        
        Self{
            queue: Default::default(), 
            queue_maxsize:queue_size,

            // input: Default::default(),
            output: Default::default(), 
    
            service_timer : Duration::ZERO, 
            aux_packet_serviced: MpduPacket::default(), 
            packet_being_served : false, 

    
            blocked_packet_counter: 0,
            arrived_packet_counter: 0, 
            queue_length_counter : 0, 

            arrival_rate: 0., 
            service_rate: 0., 
            rate_departures_bps, 
        }
        
    }
    
    pub async fn input(&mut self, mut packet: MpduPacket) {
        
        self.arrived_packet_counter += 1; 
        self.queue_length_counter += self.queue.len(); 

        packet.queue_in_instant = Instant::now();

        if self.queue.len() < self.queue_maxsize{

            self.queue.push_back(packet); 

            if self.queue.len() == 1 && self.packet_being_served != true{ // queue was empty and not servicing
              self.deque_schedule_service(); 
            }   
        }
        else{
            self.blocked_packet_counter += 1; 
        }
    }
    

    fn deque_schedule_service<'a> (
        &'a mut self,
        _: (),
        context: &'a Context<Self>, 
    ) -> impl Future<Output = ()> + Send + 'a {

        async move {

            self.aux_packet_serviced = self.queue.front().unwrap().clone(); 
            self.queue.pop_front(); 

            self.aux_packet_serviced.queue_out_instant = Instant::now(); 
            self.aux_packet_serviced.T_q = self.aux_packet_serviced.queue_out_instant.duration_since(self.aux_packet_serviced.queue_in_instant) ; 
            
            let time_of_service_secs = Duration::from_secs_f64(self.aux_packet_serviced.length_packet as f64 / self.rate_departures_bps) ;
            
            self.aux_packet_serviced.expected_T_s = time_of_service_secs; 
            self.packet_being_served = true; 

            context.scheduler.schedule_event(time_of_service_secs, Self::send_packet, ()).unwrap(); 
        }
    }


    pub async fn send_packet(&mut self, scheduler: &Scheduler<Self>){

        // SEND THE PACKET (instantly) THAT WAS IN SERVICE ALREADY
        if self.packet_being_served == true{
            self.output.send(self.aux_packet_serviced).await; 
        }
	    
        // DEQUE NEXT PACKET AND PREPARE SERVICE
        if self.queue.len() > 0 {
            self.deque_schedule_service(scheduler); 
        }
    }
}
impl Model for QueueModule {}


#[derive(Default)]
pub struct Sink{
    // pub input: Input <MpduPacket>, 
    pub received_packet_counter: usize, 
    pub packet_length_counter: usize, 
    
    pub system_time_counter : f64, 
    pub queue_time_counter:     f64,
    pub service_time_counter:   f64, 
    pub total_time_q_tx :       f64, 

    pub subsampling_counter: usize, 
    pub subsampling_const:  usize,  

}

impl Sink {

    pub fn new(&mut self) -> Self{
        Self{
            received_packet_counter :   0, 
            packet_length_counter:      0, 
            
            system_time_counter :       0., 
            queue_time_counter :        0., 
            service_time_counter :      0., 
            total_time_q_tx :           0., 

            subsampling_counter :       0,
            subsampling_const :         1, 
        }
    }

    pub async fn input(&mut self, mut packet: MpduPacket){

        let elapsed_time_f64 = (Instant::now() - packet.queue_in_instant).as_secs_f64() ; 
        self.system_time_counter += elapsed_time_f64; 

        self.packet_length_counter += packet.length_packet; 
        self.received_packet_counter += 1; 
        
        
        packet.sink_in_instant = Instant::now(); 
        packet.T_s = packet.sink_in_instant.duration_since(packet.queue_out_instant); 

        self.queue_time_counter += packet.T_q.as_secs_f64(); 
        self.service_time_counter += packet.T_s.as_secs_f64(); 

        let packet_total_time_q_tx:Duration = Instant::now().duration_since(packet.queue_in_instant) ; 

        self.subsampling_counter += 1; 

        if self.subsampling_counter >= self.subsampling_const {

            self.subsampling_counter = 0; 

            println!("[DBG SINK {:.?} - L{}] T: {:.4}, T_q: {:.4}, T_s: {:.4} \n( E(T_s) = {:.4}, (T_q+T_s) = {:.4} )\n",
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



fn main (){






}