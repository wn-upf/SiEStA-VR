use std::future::Future;

use asynchronix::model::{Context, InitializedModel, Model};
use asynchronix::ports::{EventBuffer, Output};
use asynchronix::simulation::{Mailbox, SimInit};
use asynchronix::time::MonotonicTime;

use rand::thread_rng;
use rand_distr::{Exp, Distribution}; 

use std::time::{Instant, Duration}; 



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
}
impl MpduPacket{
    pub fn new() -> Self {
        Self {
            packet_id : 0, 
            length_packet: 0, 
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

            let mut time_interarrival = Duration::from_secs_f64(exponential(1.0/ self.arrival_rate)) ;
             packet.length_packet = exponential(self.mean_length_packets as f64) as usize; 
            

            self.num_packets_sent += 1; 
            packet.packet_id = self.num_packets_sent; 
            self.output_port.send(packet.clone()).await; 

            context.scheduler.schedule_event(time_interarrival, Self::send_packet, () ).unwrap(); 
        }
    }
}

impl Model for PoissonSource{} 




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
        println!("{} - Packet received!!", format_duration(elapsed)); 
        packet.print(); 
        self.received_packet_counter += 1; 
        
    }
}

impl Model for Sink {}

fn main( ){

    let mean_length: f64 = 1000.0; 
    let rate_bps = 50.0; 

    let mut source = PoissonSource::new(rate_bps, mean_length); 
    let mbox_src = Mailbox::new(); 
    let mbox_src_address = mbox_src.address(); 


    let mut sink = Sink::new() ; 
    let sink_mbox = Mailbox::new(); 
    let sink_mbox_address = sink_mbox.address(); 
    source.output_port.connect(Sink::input, &sink_mbox); 


    let t0 = MonotonicTime::EPOCH; 

    let mut simu = SimInit::new()
    .add_model(source, mbox_src, "Source")
    .add_model(sink, sink_mbox, "Sink")
    .init(t0); 

    let scheduler = simu.scheduler(); 

    // ----------
    // Simulation.
    // ----------

    // Check initial conditions.


    let mut t = t0; 
    assert_eq!(simu.time(), t); 
    
    scheduler.schedule_event(
        Duration::from_secs(1),
        PoissonSource::send_packet,
        (), 
        &mbox_src_address,  
    ) 
    .unwrap(); 

    for i in 0..80000{
        simu.step(); 

    }
    // t += Duration::new(3, 0 ); 
    // assert_eq!(simu.time(), t); 

}

   