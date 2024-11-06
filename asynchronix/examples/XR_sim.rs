#[allow(unused_imports)]
#[allow(dead_code)]
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
use futures_util::Stream;
use lib::alvr_stream_socket::{Buffer, StreamReceiver};


use crate::lib::models_mm1k::{Sink, QueueModule, QueueStats, STA_source};


use rand::Rng;
use rand_distr::{Normal, Distribution};

use lib::{HeaderALVRStream, write_all_sta_csvs};
// use std::intrinsics::size_of;
use serde::{Deserialize, Serialize};
use std::mem;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use std::time::{SystemTime};

use once_cell::sync::Lazy;

// mod mm1k_sim; 
// use crate::mm1k_sim::{QueueModule, QueueStats, Sink, DataSink}; 

use crate::lib::alvr_packets::{ ClientStatistics, NetworkStatisticsPacket};
use crate::lib::alvr_stream_socket::{
    AnyhowToCon, DscpTos, Haptics, ReceiverData, SocketBufferSize, SocketProtocol,
    StreamSender, StreamSocketBuilder, Tracking,  VideoPacketHeader, parse_shard_data, SocketReader,  
};
use tai_time::TaiTime;

use crate::lib::alvr_stream_socket::{
    AUDIO, HAPTICS, INITIAL_FRAMERATE_FPS, MAX_HISTORY_SIZE, STATISTICS,
    TRACKING, VIDEO,
};

use rand::{random};
use std::cmp::{self, max};
use std::collections::VecDeque;
use std::env;
use std::f64::consts::PI;
use std::future::Future;

use asynchronix::model::{Context, Model};
use asynchronix::ports::Output;

use std::collections::HashMap;
use std::sync::RwLock;

// mod alvr_statistics_manager;  //TODO!
// use alvr_statistics_manager::*;
mod lib; // for calling m own local library
use crate::lib::{
    exponential, frametransmission_delay, perStaLockStats, AmpduPacket, Coords, CsvType,
    CumulativeStats, DebugColor, MpduPacket, SlidingWindowAverage, DEFAULT_TMAX_AGG,
    MAX_AMPDU_SIZE, P_TX,
};

use crate::lib::alvr_statistics::StatisticsManager;

const RETRY_CONNECT_MIN_INTERVAL: Duration = Duration::from_secs(1);
const HANDSHAKE_ACTION_TIMEOUT: Duration = Duration::from_secs(2);
const STREAMING_RECV_TIMEOUT: Duration = Duration::from_millis(2000);

const UPDATE_BITRATE_INTERVAL: Duration = Duration::from_secs(1);

const INITIAL_BITRATE_MBPS: f32 = 5.0; 


// use crate::lib::{AmpduPacket, MpduPacket, exponential, Coords, CumulativeStats, CsvType};
// use crate::{debug_print, format_elapsed, format_timestamp};

const MAX_UNREAD_PACKETS: usize = 10; // Applies per stream

use crate::lib::DEBUG_PRINT_ENABLED;

const SHARD_PREFIX_SIZE: usize = mem::size_of::<u32>() // packet length - field itself (4 bytes)
    + mem::size_of::<u16>() // stream ID
    + mem::size_of::<u32>() // packet index
    + mem::size_of::<u32>() // shards count
    + mem::size_of::<u32>() // shards index
    + mem::size_of::<f32>(); // tx relative timestamp

type InstantMap = Arc<RwLock<HashMap<u32, Instant>>>;

#[derive(Clone)]
pub struct BitrateManager {
    last_frame_instant: Instant,
    last_update_instant: Instant,

    frame_index: usize,

    frame_interval_average: SlidingWindowAverage<Duration>,
    encoder_latency_average: SlidingWindowAverage<Duration>,
    network_latency_average: SlidingWindowAverage<Duration>,

    bitrate_average_mbps: SlidingWindowAverage<f32>,

    pub last_target_bitrate_mbps: f32,
    update_interval_s: Duration,

    rtt_average: SlidingWindowAverage<Duration>,
    peak_throughput_average: SlidingWindowAverage<f32>,
    frame_interarrival_average: SlidingWindowAverage<f32>,
}

// static BITRATE_MANAGER: Lazy<Mutex<BitrateManager>> =
//     Lazy::new(|| Mutex::new(BitrateManager::new(256, 60.0, 30.0)));

pub type OptLazy<T> = Lazy<Mutex<Option<T>>>;

pub const fn lazy_mut_none<T>() -> OptLazy<T> {
    Lazy::new(|| Mutex::new(None))
}

static STATISTICS_MANAGER: OptLazy<StatisticsManager> = lazy_mut_none();

impl BitrateManager {
    // TODO: Add method for CBR
    pub fn new(max_history_size: usize, initial_framerate: f32, initial_bitrate_mbps: f32) -> Self {
        Self {
            last_frame_instant: Instant::now(),
            last_update_instant: Instant::now(),

            frame_index: 0,

            frame_interval_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),
            encoder_latency_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),
            network_latency_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),

            bitrate_average_mbps: SlidingWindowAverage::new(
                initial_bitrate_mbps,
                max_history_size,
            ),
            last_target_bitrate_mbps: initial_bitrate_mbps,
            update_interval_s: UPDATE_BITRATE_INTERVAL,

            rtt_average: SlidingWindowAverage::new(Duration::from_millis(5), max_history_size),
            peak_throughput_average: SlidingWindowAverage::new(300E6, max_history_size),
            frame_interarrival_average: SlidingWindowAverage::new(
                1. / initial_framerate,
                max_history_size,
            ),
        }
    }
}

// pub struct SimRuntimeSockets {
//     pub video_sender: Option<StreamSender<VideoPacketHeader>>,
//     pub game_audio_sender: Option<StreamSender<()>>,
//     pub tracking_receiver: Option<StreamReceiver<Tracking>>,
//     pub haptics_sender: Option<StreamSender<Haptics>>,
//     pub statistics_receiver: Option<StreamReceiver<ClientStatistics>>,
// }
pub struct XRServer {
    pub ip_self: IpAddr, 
    pub ip_client: IpAddr, 

    pub t_0: TaiTime<0>,
    pub bitrate_manager: BitrateManager,

    pub video_app_sender: Option<StreamSender<VideoPacketHeader>> , 

    pub outport_video: Output<MpduPacket>, // ONLY VIDEO FOR NOW! 


    // pub output_video: Output<MpduPacket>,
    // pub output_audio: Output<MpduPacket>,
    // pub output_haptics: Output<MpduPacket>,

    pub is_streaming: bool,

    pub fps: f64,

    // pub sockets: SimRuntimeSockets,
    pub frames_sent_counter: usize,
}


impl XRServer {
    pub fn new(ip_self:IpAddr, ip_client: IpAddr) -> Self {
        // let arrival_rate = arrival_rate_bps / mean_length;
        // let effective_mu = rate_service_bps /mean_length;
        // println!("\n*************************************************");
        // println!("[DEBUG STA{}]\tCoordinates: {:?}\n\tDestination: STA{} | RATE_IN: {:.3} Mbps, Rate_service: {:.3} (packs/s),\n\t Arrival_rate (pack/s): {:.3}, Departure_rate: {:.3},  L = {}",
        //                     src, coordinates, dest,                     arrival_rate_bps/1E6, rate_service_bps / 1E6 , arrival_rate,effective_mu ,mean_length);

        // let sockets = SimRuntimeSockets {
        //     video_sender: None,
        //     game_audio_sender: None,
        //     tracking_receiver: None,
        //     haptics_sender: None,
        //     statistics_receiver: None,
        // };
        let system_time = SystemTime::UNIX_EPOCH; 
        Self {
            ip_self,
            ip_client, 
            t_0: TaiTime::from_system_time(&system_time, 5),
            bitrate_manager: BitrateManager::new(
                MAX_HISTORY_SIZE,
                INITIAL_FRAMERATE_FPS,
                INITIAL_BITRATE_MBPS,
            ),

            video_app_sender: None, 

            outport_video: Output::default(),
            // output_audio: Output::default(),
            // output_haptics: Output::default(),
            is_streaming: false,
            fps: 90.0,
            // sockets,
            frames_sent_counter: 0,
        }
    }

    fn read_send_network_interface<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
        mut buffer: Vec<u8>, 
        // mut receiver: std::sync::MutexGuard<'_, Box<dyn SocketReader>>, 
        receiver: Arc<Mutex<Box<dyn SocketReader>>>, 

        ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            
            let mut stop = false;
            while !stop {

                let bytes_received = {
                    let mut guard = receiver.lock().unwrap();
                    guard.recv(&mut buffer)
                    };
                
                match bytes_received {

                    Ok(bytes_received) => {
                        if bytes_received == 0 {
                            // If no data is received, stop the loop
                            println!("No new data received, stopping.");
                            stop = true;
                            break; 
                        } else {
    
                            // TODO: CHECK WITH WIRESHARK ENCAPSULATION OF PACKET 
                            println!("Parsed from connection output:");
                            if let Ok((packet_length, stream_id, next_packet_index, shards_count, shard_index, tx_r_instant)) = parse_shard_data(&buffer[..100]) {
                                // println!("Data: {:?}", buffer); 
                                println!("Parsed shard data:");
                                println!("Packet length: {}", packet_length);
                                println!("Stream ID: {}", stream_id);
                                println!("Next packet index: {}", next_packet_index);
                                println!("Shards count: {}", shards_count);
                                println!("Shard index: {}", shard_index);
                                println!("Transmit-receive instant: {} )", tx_r_instant);
                                println!("--------------------------------------------------------------------------------------------------------------------------------------------------------------------------__");
                                
                                let mut packet = MpduPacket::new(); 
                            
                                packet.header_alvr = HeaderALVRStream {
                                    packet_length, 
                                    stream_id, 
                                    next_packet_index, 
                                    shards_count, 
                                    shard_index, 
                                    tx_instant: tx_r_instant, 
                                }; 
                                packet.data_inner = buffer[..packet_length as usize].to_vec();  
        
                                self.outport_video.send(packet.clone()).await;
        
                            } else {
                                println!("Failed to parse shard data, stopping.");
                                stop = true;
                                break; 
                            }
                        }   

                    },
                    Err(_) => {println!("Error receiving data, stopping"); 
                                stop = true;} 
                }

               
            }
        }
    }

    // pub fn generate_video_frame(&mut self, context: &Context<Self> ){

    pub fn generate_video_frame<'a>(
            &'a mut self,
            _: (),
            context: &'a Context<Self>,
        ) -> impl Future<Output = ()> + Send + 'a {

        async move{
            // STEP 1: DEBUG VIDEO
            if let Some(mut send_socket) = self.video_app_sender.clone() {

                let is_idr = false;
                let header = VideoPacketHeader::new(Duration::from_secs(1), is_idr);
                println!("Created header");
                
                let current_bitrate_mbps: f32 = self.bitrate_manager.last_target_bitrate_mbps;

                let mut buffer_emu = send_socket.get_buffer_emu(&header, current_bitrate_mbps).unwrap();
                
                println!("DBG-> Bitrate: {} Mbps,  Buffer length: {}  buffer.LENGTH: {:?}",current_bitrate_mbps ,buffer_emu.inner.len(),buffer_emu.length); 

                let mut payload = buffer_emu.inner.clone(); 
                buffer_emu
                    .get_range_mut(0, payload.len())
                    .copy_from_slice(&payload);

                let mut arc_receiver = send_socket.network_interface.clone(); 

                let send_result = send_socket.send(buffer_emu); 

                const BUFFER_SIZE: usize = 2000;  

                let mut buffer: Vec<u8> = vec![0; BUFFER_SIZE]; 

                // let mut receiver: std::sync::MutexGuard<'_, Box<dyn SocketReader>> = arc_receiver.lock().unwrap(); 

                XRServer::read_send_network_interface(self, (), context,  buffer, arc_receiver).await;  // FUNCTION TO HANDLE NETWORK PACKETS!


                let normal = Normal::new(0.0, 5.0).unwrap();  // Mean = 0, Std dev = 5
                let epsilon = normal.sample(&mut rand::thread_rng());  // Random Gaussian value
                
                let time_until_next_frame = Duration::from_secs_f64(1.0 / (self.fps + epsilon));

                context
                    .scheduler
                    .schedule_event(time_until_next_frame, Self::generate_video_frame, ())
                    .unwrap();
            }
        }
    }

    pub async fn connection_pipeline(&mut self, client_ip: IpAddr, context: &Context<Self>){
    // no return from this function for now
        self.bitrate_manager = BitrateManager::new(MAX_HISTORY_SIZE, 90.0, INITIAL_BITRATE_MBPS);

        // obtained by printing debug. We're using channel for purposes of mpsc for separate client and server processes, and separating the network interface of each. 
        let stream_port: u16 = 9944;
        let stream_protocol: SocketProtocol = SocketProtocol::Channel;
        let dscp: Option<DscpTos> = None;
        let server_send_buffer_bytes: SocketBufferSize = SocketBufferSize::Maximum;
        let client_recv_buffer_bytes: SocketBufferSize = SocketBufferSize::Maximum;
        let client_send_buffer_bytes: SocketBufferSize = SocketBufferSize::Maximum;
        let server_recv_buffer_bytes: SocketBufferSize = SocketBufferSize::Maximum;
        let packet_size: i32 = 1400;

        // let stream_socket_builder = StreamSocketBuilder::listen_for_server(
        //     Duration::from_secs(1),
        //     stream_port,
        //     stream_protocol.clone(),
        //     dscp.clone(),
        //     client_send_buffer_bytes,
        //     client_recv_buffer_bytes,
        // )
        // .to_con();

        // let (mut control_sender, mut control_receiver) = proto_control_socket // not doing control sockets for now, TODO!!
        // .split(STREAMING_RECV_TIMEOUT)
        // .to_con()?;

        // if let Err(e) = control_sender.send(&ClientControlPacket::StreamReady) {
        //     println!("Server disconnected. Cause: {e:?}");
        //     // set_hud_message(SERVER_DISCONNECTED_MESSAGE);
        //     return Ok(());
        // }

        if let Ok(stream_socket) = StreamSocketBuilder::connect_to_client_mod(
            HANDSHAKE_ACTION_TIMEOUT,
            client_ip,
            stream_port,
            stream_protocol,
            dscp,
            server_send_buffer_bytes,
            server_recv_buffer_bytes,
            packet_size as _,
        ) {
            println!("Connection established!");
            self.is_streaming = true;
            
            // Create sender and receiver from the same stream socket
            self.video_app_sender = Some(stream_socket.request_stream::<VideoPacketHeader>(VIDEO)); 

            // let mut video_sender: StreamSender<VideoPacketHeader> = stream_socket.request_stream::<VideoPacketHeader>(VIDEO);
            // let mut video_receiver = stream_socket.subscribe_to_stream::<VideoPacketHeader>(VIDEO, MAX_UNREAD_PACKETS);
            
            XRServer::generate_video_frame(self, (), context).await; 
            // STEP 2: DO SAME FOR REST OF PACKETS (VIDEO; HAPTICS) and loop using context.scheduler! 
            // TODO! 
        }
    }
}

// TODO : More functions to process inputs, handle ABR, etc.

impl Model for XRServer {}

// pub struct XRClient {
//     pub decoder_queue: VecDeque<Buffer>,

//     pub output_statistics: Output<MpduPacket>,
//     pub output_tracking: Output<MpduPacket>,

//     pub coordinates: Coords,
//     pub is_streaming: bool,

//     pub frames_dropped_counter: usize,
// }

// impl XRClient {
//     fn new() -> Self {
//         Self {
//             decoder_queue: VecDeque::new(),
//             output_statistics: Output::default(),
//             output_tracking: Output::default(),

//             coordinates: Coords::new(),
//             is_streaming: false,
//             frames_dropped_counter: 0,
//         }
//     }
//     fn input_packets(packet: MpduPacket) {

//         // TODO: Based on the stream type (VIDEO, AUDIO, etc.) call one function or the other for the same packet
//     }

//     fn recv_video(&mut self, data: ReceiverData<VideoPacketHeader>)
//     // inside the thread::spawn(move) of connection_pipeline
//     {
//         let packet_stats = NetworkStatisticsPacket {
//             frame_index: data.get_frame_index() as i32, // index of the current frame
//             frame_span: data.get_frame_span(),          // duration of the current frame

//             bytes_in_frame: data.get_bytes_in_frame(), // bytes received for the current frame, including both prefixes and network headers
//             bytes_in_frame_app: data.get_bytes_in_frame_app(), // bytes received for the current frame, excluding both prefixes and network headers

//             // Interval specific metrics
//             frame_interarrival: data.get_frame_interarrival(), // time interval between consecutive frames

//             interarrival_jitter: data.get_interarrival_jitter(), // measure of the variability in the time between the reception of consecutive video shards
//             ow_delay: data.get_ow_delay(), // one-way delay of the received video shards
//             filtered_ow_delay: data.get_filtered_ow_delay(), // kalman filtered one-way delay of the received video shards, as GCC does

//             frames_skipped: data.get_frames_skipped(), // number of frames skipped

//             rx_bytes: data.get_rx_bytes(), // bytes received in the interval between the consecutive frames, including any prefixes and network headers

//             rx_shard_counter: data.get_rx_shard_counter(), // non-duplicated video shards received during the interval between consecutive frames
//             duplicated_shard_counter: data.get_duplicated_shard_counter(), // duplicated video shards received during the interval between consecutive frames

//             highest_rx_frame_index: data.get_highest_rx_frame_index(), // index of the highest video FRAME received during the interval between consecutive frames
//             highest_rx_shard_index: data.get_highest_rx_shard_index(), // index of the highest video SHARD received ...
//         };

//         // // TODO: Make the function to actually schedule sending this packet!
//         // let Ok((header, nal)) = data.get();

//         // if !push_frame_decoder(header.timestamp, nal){

//         //     println!("frame is dropped!!")
//         //     // report_video_packet_dropped(data.get_frame_index()); // report in HistoryFrame the lost packet, but we're not doing HistoryFrame (?)
//         //     self.frames_dropped_counter += 1;
//         // }
//         // else{
//         //     // TODO:
//         //     // stats.report_video_packet_data(header.timestamp, data.get_frame_index(), frames_dropped); //

//         // }

//         // if header.is_idr {}

//         // if data.had_packet_loss(){

//         // }
//     }
//     // fn send_statistics_packet(){
//     //     // TODO!
//     // }

//     fn recv_audio(data: ReceiverData<()>) {

//         // actually we do nothing on this, just consume the packet
//     }

//     fn receive_control_packet() {}
// }


#[allow(non_camel_case_types)]
pub struct STA_extended {
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

impl STA_extended{
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



    pub async fn input_XR_app(&mut self, mut packet: MpduPacket, context: &Context<Self>){

        // do everything else to the packet: 

        packet.length_packet = packet.header_alvr.packet_length as usize;
        packet.packet_id = self.num_packets_sent;

        packet.sta_src_id = self.sta_id;
        packet.sta_dest_id = self.destination_id;

        packet.sta_dest_coords = self.sta_coordinates;

        self.output_port.send(packet.clone()).await;
        self.num_packets_sent += 1;

    }

    pub async fn input_wireless(&mut self, ampdu_packet: AmpduPacket, context: &Context<Self>) {
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

                // context // reschedule this function // DON'T SELF-schedule (depends on XR_source)
                //     .scheduler
                //     .schedule_event(time_interarrival, Self::send_packet, ())
                //     .unwrap();
            }
        }
    }
}

impl Model for STA_extended {}

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

    const num_STAs: usize = 2; 

    let v_distance = vec![1.0, distance, distance]; // just some random values

    const NUM_STAS_UL: usize = 1; //for now

    let coords_staxr = Coords {
        x: v_distance[0],
        y: 0.0,
        z: 0.0,
    };
    let coords_sink = Coords {
        x: v_distance[1],
        y: 0.0,
        z: 0.0,
    };

    let vec_coords = vec![coords_staxr, coords_sink];
    println!("vec_coords: {:?}\n", vec_coords);
    
    // TODO: Make this dynamic based on Vec<Coords> and Vec<ResultsFrameTXDelay> with a function

    let results1 = frametransmission_delay(
        INITIAL_BITRATE_MBPS as f64 * 1E6, // optimistic assumption of max throughput
        MAX_AMPDU_SIZE,
        Coords::new(),
        coords_staxr,
        P_TX,
    );

    let results2 = frametransmission_delay(
        mean_length * MAX_AMPDU_SIZE as f64,
        MAX_AMPDU_SIZE,
        Coords::new(),
        coords_sink,        // TO TEST
        P_TX,
    );

    let effective_rate1 = mean_length / results1.service_delay;
    let effective_rate2 = mean_length / results2.service_delay;
    let effective_rate = (effective_rate1 + effective_rate2) / 2.0;

    let aggregated_rate_in = (num_STAs - NUM_STAS_UL) as f64 * rate_bps_in;

    println!("*******************************************************************");
    println!(
        "Inputs--> rate: {}, l_mean :{}, effective_rate: {}, k: {}",
        aggregated_rate_in, mean_length, effective_rate, k_queue
    );

    let ip_src = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)); 
    let ip_dest = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)); 

    let mut xr_server = XRServer::new( ip_src, ip_dest );
    


    


    let mut sta1_xr: STA_extended = STA_extended::new(
        INITIAL_BITRATE_MBPS as f64 * 1E6,
        mean_length,
        0,
        2,
        coords_staxr,
        true,
        effective_rate1,
    ); // STAs 0 and 1 send traffic to 5 through AP

    println!("STA XR PathLoss: {:.2}, P_rx : {:.2}, T_total: {:.3} ms, T_s(data): {:.3} ms , rate_total: {:.2} \n\n",
        results1.pathloss, results1.p_rx, results1.service_delay * 1000.0, results1.data_service_delay * 1000.0, (1.0 / results1.service_delay) * mean_length);

    println!("STA2 PathLoss: {:.2}, P_rx : {:.2}, T_total: {:.3} ms, T_s(data): {:.3} ms , rate_total: {:.2} \n\n",
        results2.pathloss, results2.p_rx, results2.service_delay * 1000.0, results2.data_service_delay * 1000.0, (1.0 / results2.service_delay) * mean_length);

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

    
    let mbox_xr_server = Mailbox::new(); 
    let mbox_sta_xr = Mailbox::new();
    let mbox_queue = Mailbox::new();
    let mbox_sink = Mailbox::new(); 

    let xr_server_address = mbox_xr_server.address(); 
    let sta1_address = mbox_sta_xr.address();

    let queue_address = mbox_queue.address();
    let sink_address = mbox_sink.address();

    // CONNECT COMPONENTS

    xr_server.outport_video.connect(STA_extended::input_XR_app, &mbox_sta_xr); 
    sta1_xr.output_port.connect(QueueModule::input, &mbox_queue); 
    queue.output_port.connect(Sink::input, &mbox_sink);

    let t0 = MonotonicTime::EPOCH;

    let mut simu: asynchronix::simulation::Simulation = SimInit::with_num_threads(64)
        .add_model(xr_server, mbox_xr_server, "XR Server")
        .add_model(sta1_xr, mbox_sta_xr,      "STA1 (XR_s)")
        .add_model(queue, mbox_queue,         "Queue")
        .add_model(sink, mbox_sink,           "SINK")
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

    let duration_scheduled1 = Duration::from_secs(10) + epsilon1;


    scheduler
        .schedule_event(
            duration_scheduled1,
            XRServer::connection_pipeline,
            (ip_dest),
            &xr_server_address,
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

    if let Ok(sink_stats) = sinkstats_data_handle.lock() {
        sink_stats.print_nicely();
    }

    // println!("************ END RESULTS STAS***********\n LT: ");

    println!("*******************************************************************");
    println!(
        "Inputs--> rate: {}, l_mean :{}, effective_rate: {}, k: {}",
        aggregated_rate_in, mean_length, effective_rate, k_queue
    );


}
