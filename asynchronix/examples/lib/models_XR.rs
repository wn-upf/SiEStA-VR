use crate::lib::alvr_control_socket::{framed_recv, framed_recv_vec, ControlSocketReceiver, ControlSocketSender};
use crate::lib::alvr_stream_socket::{Buffer, StreamReceiver};
use rand::Rng;
use rand_distr::{Distribution, Normal};

use crate::debug_print;
use crate::format_elapsed;
use crate::lib::HeaderALVRStream;

// use std::intrinsics::size_of;
use serde::{de::DeserializeOwned, Serialize};
use crate::debug_bgprint;
use core::net;
use std::fmt::Debug;
use std::mem;
use std::net::IpAddr;
// use std::process::Output;
use std::sync::{Arc, Mutex};
use std::thread::yield_now;
use std::time::{Duration, Instant};

use std::time::SystemTime;
use dashmap::DashMap;


use once_cell::sync::Lazy;

// mod mm1k_sim;
// use crate::mm1k_sim::{QueueModule, QueueStats, Sink, DataSink};

use crate::lib::alvr_packets::{ClientControlPacket, ClientStatistics, NetworkStatisticsPacket};
use crate::lib::alvr_stream_socket::{
    parse_shard_data, ConnectionError, DscpTos, Haptics, ReceiverData,
    SocketBufferSize, SocketProtocol, SocketReader, StreamSender, StreamSocketBuilder, Tracking,
    VideoPacketHeader,
};

use crate::lib::alvr_control_socket::{ProtoControlSocket, ControlPacketType}; 
use tai_time::TaiTime;

use crate::lib::alvr_stream_socket::{
    AUDIO, HAPTICS, INITIAL_FRAMERATE_FPS, MAX_HISTORY_SIZE, STATISTICS, TRACKING, VIDEO,
};
use crate::lib::DEBUG_PRINT_ENABLED;


pub const UPDATE_BITRATE_INTERVAL: Duration = Duration::from_secs(1);
pub const HANDSHAKE_ACTION_TIMEOUT: Duration = Duration::from_secs(2);
pub const MAX_UNREAD_PACKETS: usize = 5; // Applies per stream

pub const CAPACITY_RX_BUFFER: usize = 2000;
pub const STREAMING_RECV_TIMEOUT: Duration = Duration::from_millis(10);
pub const FRAMED_PREFIX_CONTROL_LENGTH: usize = mem::size_of::<u32>();

pub const DECODER_BUFFERING_FRAMES: usize = 4;
pub const TARGET_FRAMES_DECODER_QUEUE: usize = 2; 



static STATISTICS_MANAGER: OptLazy<StatisticsManager> = lazy_mut_none();




use std::cmp::{self, max};
use std::collections::VecDeque;
use std::f64::consts::PI;
use std::future::Future;

use asynchronix::model::{Context, Model};
use asynchronix::ports::Output;

use std::collections::HashMap;
use std::sync::RwLock;

use crate::lib::{
    exponential, AmpduPacket, Coords, DebugColor,
    MpduPacket, SlidingWindowAverage,
};

use crate::lib::alvr_statistics::StatisticsManager;
use crate::lib::INITIAL_BITRATE_MBPS_SIM;

use super::alvr_packets::DeadlineShardlossStatPacket;
use super::alvr_stream_socket::{CONTROL_STREAM, MAX_DEADLINE_IN_STATS};
use super::alvr_stream_socket::{SocketWriter, StreamSocket, MAX_PACKET_SIZE_RECV};
use super::SlidingWindowTimely;

pub const SHARD_PREFIX_SIZE: usize = mem::size_of::<u32>() // packet length - field itself (4 bytes)
    + mem::size_of::<u16>() // stream ID
    + mem::size_of::<u32>() // packet index
    + mem::size_of::<u32>() // shards count
    + mem::size_of::<u32>() // shards index
    + mem::size_of::<f32>(); // tx relative timestamp

type InstantMap = Arc<RwLock<HashMap<u32, TaiTime<0>>>>;

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

impl BitrateManager{
    pub fn report_network_statistics(
        &mut self,
        network_rtt: Duration,
        peak_throughput_bps: f32,
        frame_interarrival_s: f32,
    ) {
        self.rtt_average.submit_sample(network_rtt);

        self.peak_throughput_average
            .submit_sample(peak_throughput_bps);

        self.frame_interarrival_average
            .submit_sample(frame_interarrival_s);
    }
}



// static BITRATE_MANAGER: Lazy<Mutex<BitrateManager>> =
//     Lazy::new(|| Mutex::new(BitrateManager::new(256, 60.0, 30.0)));

pub type OptLazy<T> = Lazy<Mutex<Option<T>>>;

pub const fn lazy_mut_none<T>() -> OptLazy<T> {
    Lazy::new(|| Mutex::new(None))
}

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

            bitrate_average_mbps: SlidingWindowAverage::new(initial_bitrate_mbps, max_history_size),
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

pub struct XRServer {
    pub ip_self: IpAddr,
    pub ip_client: IpAddr,

    pub t_0: TaiTime<0>,
    pub bitrate_manager: BitrateManager,

    pub video_app_sender: Option<StreamSender<VideoPacketHeader>>,

    pub tracking_app_receiver: Option<StreamReceiver<Tracking>>,
    pub statistics_app_receiver: Option<StreamReceiver<ClientStatistics>>,

    pub control_socket_sender: Option<ControlSocketSender<ClientControlPacket>>, 
    pub control_socket_receiver: Option<ControlSocketReceiver<ClientControlPacket>>, 
   
   
    pub outport_videoapp_network: Output<MpduPacket>, // ONLY VIDEO FOR NOW!

    pub is_streaming: bool,

    pub fps: f32,

    // pub sockets: SimRuntimeSockets,
    pub frames_sent_counter: usize,

    pub map_rtt: Arc<DashMap<u32, TaiTime<0>>>, 
    pub STATISTICS_MANAGER: StatisticsManager, 
}

impl XRServer {
    pub fn new(ip_self: IpAddr, ip_client: IpAddr, t0_sim: TaiTime<0>, frame_rate: f32, initial_bitrate: f32, name_folder: &str) -> Self {
        let system_time = SystemTime::UNIX_EPOCH;
        Self {
            ip_self,
            ip_client,
            t_0: t0_sim,
            bitrate_manager: BitrateManager::new(
                MAX_HISTORY_SIZE,
                INITIAL_FRAMERATE_FPS,
                initial_bitrate,
            ),

            video_app_sender: None,
            
            tracking_app_receiver: None,
            statistics_app_receiver: None,

            control_socket_sender: None, 
            control_socket_receiver: None,
            outport_videoapp_network: Output::default(),
            // output_audio: Output::default(),
            // output_haptics: Output::default(),
            is_streaming: false,
            fps: frame_rate,
            // sockets,
            frames_sent_counter: 0,

            map_rtt : Arc::new(DashMap::new()), 
            STATISTICS_MANAGER: StatisticsManager::new(MAX_HISTORY_SIZE, Duration::from_secs_f32(1.0/frame_rate) , 0.0, name_folder), 
        }
    }

    pub fn handle_control_packet(&mut self, packet: ClientControlPacket, now: TaiTime<0>){

    
        if let Some(mut protorecv) = self.control_socket_receiver.clone(){
            // let packet = protorecv.recv(STREAMING_RECV_TIMEOUT).unwrap(); 
            let map_clone: Arc<DashMap<u32, TaiTime<0>>> = Arc::clone(&self.map_rtt) ;

            match packet {    
                ClientControlPacket::NetworkStatistics(network_stats) => {
                    
                    // debug_bgprint!(DebugColor:: Teal, "{:.9}[DBG SERVER STATS]- Received stats for frame {:2.0}: \nNetwork stats:\n\t\t{:#?}",now.duration_since(self.t_0).as_secs_f64(), network_stats.frame_index,network_stats);
                    
                                        
                    // let mut map_rtt_lock = map_clone.write().unwrap();
                    let frame_id = network_stats.frame_index as u32;
                    let rtt: Duration;
                    // if let send_instant = map_clone.get(&frame_id).unwrap()
                    if let send_instant = map_clone.remove(&frame_id).unwrap().1 
                    {
                        rtt = now.duration_since(send_instant);
                        // println!("SEND INSTANT: {}, now: {}, rtt: {}", format_elapsed!(send_instant), format_elapsed!(now), rtt.as_secs_f32()); 

                    }
                    else {
                        println!("frame {} RTT ZEROOOOOOOOOOOOOO!!!!!!!!!!!!!!!!!!!!!!!!!",  network_stats.frame_index);
                        rtt = Duration::ZERO;
                    }        

                    // // Before removing an entry, check whether its lifetime has exceeded the expected range.
                    // if let Some(send_instant) = map_rtt_lock.remove(&frame_id) {// Process RTT normally
                    //         rtt = now.duration_since(send_instant);
                    // } else {
                    //     println!("Frame {} missing in map_rtt, possible packet loss or eviction!", frame_id);
                    //     rtt = Duration::ZERO;
                    // }
                    // if let Some(send_instant) = hashmap.remove(&frame_id) {
                    //     rtt = now.duration_since(send_instant);
                    //     println!("rtt = {:.9}", rtt.as_secs_f64()); 
                    // }
                    // else {
                    //     println!("frame {} RTT ZEROOOOOOOOOOOOOO!!!!!!!!!!!!!!!!!!!!!!!!!",  network_stats.frame_index);
                    //     rtt = Duration::ZERO;
                    // }

                    debug_bgprint!(DebugColor::Teal, "RTT = {:.9}", rtt.as_secs_f64()); 
                    
                    let (peak_network_throughput_bps, frame_interarrival_s) =
                        self.STATISTICS_MANAGER.report_network_statistics(network_stats, rtt, now);

                    // BITRATE_MANAGER.lock().report_network_statistics
                    self.bitrate_manager.report_network_statistics(
                        rtt,
                        peak_network_throughput_bps,
                        frame_interarrival_s,
                    );   
                }
                ClientControlPacket::DeadlineShardLossStat(inner) => {  
                    let frames_lost = inner.frame_indexes; 
                    let shards_lost = inner.shards_lost; 
                    
                    for (frame,shard) in frames_lost.iter().zip(shards_lost.iter()) {
                        println!("[DBG_DEAD_RX server] Frame {} lost {} shards", frame, shard); 
                    }; 
                    
                }
                
                _ =>  {println!("UNEXPECTED CONTROL PACKET RECEIVED!!"); }  
        
            }
        }

       



    }

    pub async fn in_from_network(&mut self, frame: TimedFrame) {

        let packet_vec = frame.vec; 
        let now = frame.timestamp; 

        for packet in packet_vec{
            let header = packet.header_alvr.clone();
            let buffer = packet.data_inner.clone();
            // println!("XRServer IN NETWORK. Header: {:?}, buffer_len = {}", header, buffer.len()); 

            match header.stream_id {
                TRACKING => {
                    if let Some(sock) = self.tracking_app_receiver.clone() {
                        let sender = sock.network_app_interface.lock().unwrap().send(&buffer);
                        let mut new_buffer: Vec<u8> = vec![0; MAX_PACKET_SIZE_RECV];
                        let receiver = sock.inner.lock().unwrap().recv(&mut new_buffer);
                    
                        println!("TODO THE REST!!"); 
                    }
                }
                STATISTICS => {
                    if let Some(sock) = self.statistics_app_receiver.clone() {
                        let sender = sock.network_app_interface.lock().unwrap().send(&buffer);
                        let mut new_buffer: Vec<u8> = vec![0; MAX_PACKET_SIZE_RECV];
                        let receiver = sock.inner.lock().unwrap().recv(&mut new_buffer);
                        println!("TODO THE REST!!"); 

                    }
                }
                
                CONTROL_STREAM => {
                    if let Some(mut sock) = self.control_socket_sender.as_mut() {
                        // println!("Received control stream!!");      
                        // Deserialize into ClientControlPacket directly, not a reference

                        // println!("Size of buffer: {}", packet.data_inner.len() ); 
                        let stats: ClientControlPacket = framed_recv_vec(&packet.data_inner).unwrap(); 
                        // println!("STATS IS {:?}", stats); 

                        let results = sock.send(&stats);

                        XRServer::handle_control_packet(self, stats, now);
                    }
                }
                
                _ => {
                    println!("ERROR WRONG STREAM SENT? {} XRSERVER", header.stream_id);
                }
            };

        }
        
    }

    fn read_app_send_network_interface<'a>(
        &'a mut self,
        _: (),
        now: TaiTime<0>, 
        mut buffer: Vec<u8>,
        // mut receiver: std::sync::MutexGuard<'_, Box<dyn SocketReader>>,
        receiver: Arc<Mutex<Box<dyn SocketReader>>>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let mut stop = false;

            let mut elapsed = now.duration_since(self.t_0);

            debug_print!(
                DebugColor::DarkGreen,
                "{}[DBG XR_SERVER] Sending to network the following packets:",
                elapsed.as_secs_f64(),
            );
            while !stop {
                let bytes_received = {
                    let mut guard = receiver.lock().unwrap();
                    guard.recv(&mut buffer)
                };

                match bytes_received {
                    Ok(bytes_received) => {
                        if bytes_received == 0 {
                            // If no data is received, stop the loop
                            // println!(
                            //     "{}",
                            //     DebugColor::DarkGreen.to_color_fn()(String::from(
                            //         "No new data received, stopping."
                            //     ))
                            // );
                            stop = true;
                            break;
                        } else {
                            // TODO: CHECK WITH WIRESHARK ENCAPSULATION OF PACKET
                            // println!("{}", DebugColor::DarkGreen.to_color_fn()(String::from("Parsed from connection output:")));
                            if let Ok((
                                packet_length,
                                stream_id,
                                next_packet_index,
                                shards_count,
                                shard_index,
                                tx_r_instant,
                            )) = parse_shard_data(&buffer[..100])
                            {
                                let str_id = match stream_id {
                                    0 => "Tracking",
                                    1 => "Haptics",
                                    2 => "Audio",
                                    3 => "Video",
                                    4 => "Statistics",
                                    _ => "?? IDK",
                                };
                                elapsed = now.duration_since(self.t_0);

                           

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
                                
                                if packet.header_alvr.shard_index == 0{
                                    println!("\n{:.9}-Server sending {:#?}",now.duration_since(self.t_0).as_secs_f64(), packet.header_alvr);
                                }

                                debug_bgprint!(
                                    DebugColor::DarkGreen,
                                    "\t|Packet length: {}| Stream ID: {}| Next packet index: {}| Shards count: {} | Shard index: {} ||\n--------------------------------------------------------------------------------------------------------------------------------------------------------------------------",
                                    packet_length,
                                    str_id,
                                    next_packet_index,
                                    shards_count,
                                    shard_index,
                                );
                                self.outport_videoapp_network.send(packet).await;

                            } else {
                                println!(
                                    "{}",
                                    DebugColor::DarkGreen.to_color_fn()(String::from(
                                        "Failed to parse shard data, stopping."
                                    ))
                                );
                                stop = true;
                                break;
                            }
                        }
                    }
                    Err(_) => {
                        println!(
                            "{}",
                            DebugColor::DarkGreen.to_color_fn()(String::from(
                                "Error receiving data, stopping"
                            ))
                        );
                        stop = true;
                    }
                }
            }
        }
    }

    // pub fn generate_video_frame(&mut self, context: &Context<Self> ){

    async fn read_network_interface_to_app<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
        mut buffer: Vec<u8>,
        // mut receiver: std::sync::MutexGuard<'_, Box<dyn SocketReader>>,
        receiver: Arc<Mutex<Box<dyn SocketWriter>>>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let bytes_transmitted = {
                let mut guard = receiver.lock().unwrap();
                guard.send(&mut buffer)
            };
        }
    }

    pub fn generate_video_frame<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let now = context.scheduler.time(); 
            // let map_clone: Arc<RwLock<HashMap<u32, TaiTime<0>>>> = Arc::clone(&self.map_rtt);

            let map_clone: Arc<DashMap<u32, TaiTime<0>>> = Arc::clone(&self.map_rtt) ;
            self.video_app_sender.as_mut().unwrap().next_packet_index =
                self.frames_sent_counter as u32;
            self.frames_sent_counter += 1;
            // STEP 1: DEBUG VIDEO
            if let Some(mut send_socket) = self.video_app_sender.clone() {
                let is_idr = false;
                let header = VideoPacketHeader::new(Duration::from_secs(1), is_idr);

                let current_bitrate_mbps: f32 = self.bitrate_manager.last_target_bitrate_mbps;

                let mut buffer_emu =
                    send_socket // generate the actual video frame data
                        .get_buffer_emu(&header, current_bitrate_mbps)
                        .unwrap();
                

                            // Use DashMap's thread-safe `insert` API instead of write locks
                let frame_tracker_map = send_socket.get_frame_tracker_map();
                frame_tracker_map.into_iter().for_each(|(key, value)| {
                    map_clone.insert(key, value);
                });
                // match map_clone.write() {
                //     Ok(mut guard) => {
                //         *guard = send_socket.get_frame_tracker_map();
                //     }
                //     Err(_) => {
                //         println!("Failed to acquire write lock in RTT hashmap");
                //     }
                // }                // println!(
                //     "DBG-> Bitrate: {} Mbps,  Buffer length: {}  buffer.LENGTH: {:?}",
                //     current_bitrate_mbps,
                //     buffer_emu.inner.len(),
                //     buffer_emu.length
                // );

                let payload = buffer_emu.inner.clone();
                buffer_emu
                    .get_range_mut(0, payload.len())
                    .copy_from_slice(&payload);

                let arc_receiver: Arc<Mutex<Box<dyn SocketReader>>> =
                    send_socket.app_network_interface.clone();

                let send_result = send_socket.send(buffer_emu, now);

                            // Update the DashMap again with any new frame tracker data
                let cloned_socket_map = send_socket.get_frame_tracker_map();
                cloned_socket_map.into_iter().for_each(|(key, value)| {
                    map_clone.insert(key, value);
                });

                let buffer: Vec<u8> = vec![0; CAPACITY_RX_BUFFER];

                // let mut receiver: std::sync::MutexGuard<'_, Box<dyn SocketReader>> = arc_receiver.lock().unwrap();

                XRServer::read_app_send_network_interface(self, (), now, buffer, arc_receiver)
                    .await; // FUNCTION TO HANDLE NETWORK PACKETS!

                let normal = Normal::new(0.0, 2.0).unwrap(); // Mean = 0, Std dev = 5
                let epsilon = normal.sample(&mut rand::thread_rng()); // Random Gaussian value

                let time_until_next_frame = Duration::from_secs_f32(1.0 / (self.fps + epsilon));

                context
                    .scheduler
                    .schedule_event(time_until_next_frame, Self::generate_video_frame, ())
                    .unwrap();
            }
        }
    }

    pub async fn connection_pipeline(&mut self, client_ip: IpAddr, context: &Context<Self>) {
        // no return from this function for now
        // self.bitrate_manager = BitrateManager::new(MAX_HISTORY_SIZE, 90.0, INITIAL_BITRATE_MBPS_SIM);

        // obtained by printing debug. We're using channel for purposes of mpsc for separate client and server processes, and separating the network interface of each.
        let stream_port: u16 = 9944;
        let stream_protocol: SocketProtocol = SocketProtocol::Channel;
        let dscp: Option<DscpTos> = None;
        let server_send_buffer_bytes: SocketBufferSize = SocketBufferSize::Maximum;
        let client_recv_buffer_bytes: SocketBufferSize = SocketBufferSize::Maximum;
        let client_send_buffer_bytes: SocketBufferSize = SocketBufferSize::Maximum;
        let server_recv_buffer_bytes: SocketBufferSize = SocketBufferSize::Maximum;
        let packet_size: i32 = 1400;

        if let Ok(mut stream_socket) = StreamSocketBuilder::connect_to_client_mod(
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

            self.video_app_sender =
                Some(stream_socket.request_stream::<VideoPacketHeader>(VIDEO, self.t_0));
            self.tracking_app_receiver =
                Some(stream_socket.subscribe_to_stream::<Tracking>(TRACKING, MAX_UNREAD_PACKETS));
            self.statistics_app_receiver = Some(
                stream_socket
                    .subscribe_to_stream::<ClientStatistics>(STATISTICS, MAX_UNREAD_PACKETS),
            );

            // self.control_receiver = Some(
            //     stream_socket.subscribe_to_stream::<()>(CONTROL_STREAM,  MAX_UNREAD_PACKETS),
            // );

            let (proto_socket,_) = ProtoControlSocket::connect(STREAMING_RECV_TIMEOUT).unwrap(); 

            let (mut control_sender, mut control_receiver):
                    (ControlSocketSender<ClientControlPacket>, ControlSocketReceiver<ClientControlPacket>)
                    = proto_socket.split().unwrap(); 

            self.control_socket_sender = Some(control_sender);
            self.control_socket_receiver = Some(control_receiver);  

            XRServer::generate_video_frame(self, (), context).await;
            // STEP 2: DO SAME FOR REST OF PACKETS (VIDEO; HAPTICS) and loop using context.scheduler!
            // TODO!
        }
    }
}

impl Model for XRServer {}


struct DroppingVecDeque<T> {
    deque: VecDeque<T>,
    capacity: usize,
    dropped_frame_counter: usize, 
    ok_dequed_frame_counter: usize, 
    enqued_frame_counter: usize, 
}

impl<T> DroppingVecDeque<T> {
    fn new(capacity: usize) -> Self {
        Self {
            deque: VecDeque::with_capacity(capacity),
            capacity,
            dropped_frame_counter: 0,
            ok_dequed_frame_counter: 0,
            enqued_frame_counter: 0, 
        }
    }
    fn push(&mut self, item: T) {
        // If we are at capacity, pop the oldest frame from the front
        self.enqued_frame_counter += 1; 
        debug_print!(DebugColor::Gold, "[VecDecoder] Pushing frame {}, decoder_length: {}, max: {},",
             self.enqued_frame_counter,
             self.deque.len(), 
             self.capacity, 
             );

        if self.deque.len() == self.capacity {
            self.deque.pop_front(); 
            self.dropped_frame_counter += 1; 
            println!("DROPPED A FRAME IN DECODER!!"); 
        }
        // Push the new item to the back
        self.deque.push_back(item);
    }

    fn pop(&mut self) -> Option<T> {
        self.ok_dequed_frame_counter += 1; 
        self.deque.pop_front()
    }

    fn len(&self) -> usize {
        self.deque.len()
    }
}

#[allow(unused)]
pub struct XRClient {
    pub decoder_queue: DroppingVecDeque<Vec<u8>>,

    pub outport_streams: Output<Vec<u8>>,

    pub input_app_video: Option<StreamReceiver<VideoPacketHeader>>,
    pub input_app_audio: Option<StreamReceiver<()>>,
    pub input_app_haptics: Option<StreamReceiver<Haptics>>,

    pub out_video_decoded: Output<Vec<u8>>, 
    pub framerate: f32,
    pub last_decoded_frame_instant: TaiTime<0>, 


    pub output_app_network: Output<MpduPacket>,

    pub coordinates: Coords,
    pub is_streaming: bool,

    pub frames_dropped_counter: usize,
    pub server_ip: IpAddr,

    pub streamsocket_clone: Option<StreamSocket>,

}

impl XRClient {
    pub fn new(server_ip: IpAddr, fps: f32) -> Self {
        Self {
            decoder_queue: DroppingVecDeque::new(DECODER_BUFFERING_FRAMES),
            outport_streams: Output::default(),
            input_app_video: None,
            input_app_audio: None,
            input_app_haptics: None,
            out_video_decoded: Output::default(), 
            framerate: fps,
            last_decoded_frame_instant: TaiTime::EPOCH, 
            output_app_network: Output::default(),
            // output_tracking: Output::default(),
            coordinates: Coords::new(),
            is_streaming: false,
            frames_dropped_counter: 0,
            server_ip,
            streamsocket_clone: None,
        }
    }

    pub async fn framed_send<S: Serialize>(
        &mut self,
        packet: &S,
        context: &Context<Self>, 
    ) -> Result<(), Box<dyn std::error::Error>> {
        // println!("FRAMEDSEND!");
        let mut buffer = vec![0; MAX_PACKET_SIZE_RECV];

        let serialized_size = bincode::serialized_size(&packet)? as usize;
        let packet_size = serialized_size + FRAMED_PREFIX_CONTROL_LENGTH;

        if buffer.len() < packet_size {
            buffer.resize(packet_size, 0);
        }

        buffer[0..FRAMED_PREFIX_CONTROL_LENGTH]
            .copy_from_slice(&(serialized_size as u32).to_be_bytes());
        bincode::serialize_into(
            &mut buffer[FRAMED_PREFIX_CONTROL_LENGTH..packet_size],
            &packet,
        )?;
        let mut packetz = MpduPacket::new();
        packetz.data_inner = buffer[0..packet_size].to_vec();
        packetz.header_alvr.stream_id = CONTROL_STREAM;
        
        context.scheduler.schedule_event(Duration::from_nanos(10), Self::output_app_network_send , packetz).unwrap(); 
        Ok(())
    }

    pub async fn output_app_network_send(&mut self, packet: MpduPacket){
        self.output_app_network.send(packet).await; // send to input_XR_app of STA
    }

    pub async fn output_control(&mut self, packet: ClientControlPacket, context: &Context<Self>) -> () {
        // Sends directly TCP packets related to Control. For now, just NetworkStatistics
        // println!("output_control"); 
        let pack = packet.clone(); 
        match packet {
            ClientControlPacket::NetworkStatistics(inner) => {
                let result = Self::framed_send(self, &pack, context).await;
                // println!("result of output control: {:?}", result); 
            }
            ClientControlPacket::DeadlineShardLossStat(inner) => {
                let result = Self::framed_send(self, &pack, context).await;

            }
            _ => eprintln!("Uncovered match case!!"),
        }
        ()
    }
    pub fn report_frame_lost( mut frames:  Vec<u32>, mut shards_lost: Vec<usize>, context: &Context<Self> ) {
        frames.truncate(MAX_DEADLINE_IN_STATS);
        shards_lost.truncate(MAX_DEADLINE_IN_STATS);

        let net = DeadlineShardlossStatPacket{
            frame_indexes: frames, 
            shards_lost: shards_lost, 
        }; 
        context.scheduler.schedule_event(Duration::from_nanos(10), Self::output_control, ClientControlPacket::DeadlineShardLossStat(net)).unwrap();
    }
    pub fn video_receive_thread<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            if self.is_streaming {
                if let Some(mut receiver) = self.input_app_video.clone() {
                    
                    
                    let (frames_lost, shards_lost) : (Vec<u32>, Vec<usize>); 

                    if let Some(mut ssocket) = self.streamsocket_clone.as_mut() {
                        let mut counter = 0;                     
                        (frames_lost, shards_lost) =StreamSocket::flush_shards_lost_deadline(&mut ssocket); 
                        XRClient::report_frame_lost(frames_lost, shards_lost, context); 

                        
                    }

                    let data: ReceiverData<VideoPacketHeader> =
                        match receiver.recv(STREAMING_RECV_TIMEOUT) {
                            Ok(data) => data,
                            Err(ConnectionError::TryAgain(_)) => return,
                            Err(ConnectionError::Other(_)) => return,
                        };

                    let mut packets_lost_deadline = 0; 
                                      
                        
                    let net = NetworkStatisticsPacket {
                        // Frame specific metrics
                        frame_index: data.get_frame_index() as i32, // index of the current frame
                        frame_span: data.get_frame_span(),          // duration of the current frame

                        bytes_in_frame: data.get_bytes_in_frame(), // bytes received for the current frame, including both prefixes and network headers
                        bytes_in_frame_app: data.get_bytes_in_frame_app(), // bytes received for the current frame, excluding both prefixes and network headers

                        // Interval specific metrics
                        frame_interarrival: data.get_frame_interarrival(), // time interval between consecutive frames

                        interarrival_jitter: data.get_interarrival_jitter(), // measure of the variability in the time between the reception of consecutive video shards
                        ow_delay: data.get_ow_delay(), // one-way delay of the received video shards
                        filtered_ow_delay: data.get_filtered_ow_delay(), // kalman filtered one-way delay of the received video shards, as GCC does

                        frames_skipped: data.get_frames_skipped(), // number of frames skipped

                        rx_bytes: data.get_rx_bytes(), // bytes received in the interval between the consecutive frames, including any prefixes and network headers

                        rx_shard_counter: data.get_rx_shard_counter(), // non-duplicated video shards received during the interval between consecutive frames
                        duplicated_shard_counter: data.get_duplicated_shard_counter(), // duplicated video shards received during the interval between consecutive frames

                        highest_rx_frame_index: data.get_highest_rx_frame_index(), // index of the highest video frame received during the interval between consecutive frames
                        highest_rx_shard_index: data.get_highest_rx_shard_index(), // index of the highest video shard received during the interval between consecutive frames
                        lost_shards_deadline: packets_lost_deadline, 
                        // tx_instant: data.get_tx_instant(), 

                    };
                    // println!("[CLIENT] Sending networkstats packet in UL: {:#?}", net);

                    // send frame and network statistics for every reconstructed video frame
                    debug_print!(DebugColor::Gold, "[DBG Client RX frame] Frame {:2.0} received, sending stats packet in UL", data.get_frame_index()); 

                    context.scheduler.schedule_event(Duration::from_nanos(10), Self::output_control, ClientControlPacket::NetworkStatistics(net)).unwrap();
                    // self.output_control(ClientControlPacket::NetworkStatistics(net)).await;

                    let Ok((_header, nal)) = data.get() else {
                        println!("UNABLE TO GET HEADER NAL? ");
                        return;
                    };
                    let sized_vec = nal[..20.min(nal.len())].to_vec();

                    debug_print!(DebugColor::Gold, "[DEBUG DECODE] NAL first 20 bytes: {:?}", sized_vec); 
                    self.decoder_queue.push(nal.to_vec());
                    ()
                }
                // if let Some(stats) = &mut *STATISTICS_MANAGER.lock() {
                //     stats.report_video_packet_received(header.timestamp);
                //     }
                // }
            }
            //// TODO IN THE FUTURE? :)
            // periodically request an IDR frame using the settings' client_idr_refresh_interval_ms
            // if settings.connection.idr_periodic_bool {
            //     if Instant::now()
            //         .saturating_duration_since(last_instant_IDR_client)
            //         .as_secs_f32()
            //         >= interval_IDR_seconds_f32
            //     {
            //         if let Some(sender) = &mut *CONTROL_SENDER.lock() {
            //             sender.send(&ClientControlPacket::RequestIdr).ok();
            //         }
            //         last_instant_IDR_client = Instant::now();
            //     }
            // }

            // if header.is_idr {
            //     stream_corrupted = false;
            // } else if data.had_packet_loss() {
            //     stream_corrupted = true;
            //     if let Some(sender) = &mut *CONTROL_SENDER.lock() {
            //         sender.send(&ClientControlPacket::RequestIdr).ok();
            //     }
            //     warn!(
            //         "Network skipped {} video packets",
            //         data.get_frames_skipped()
            //     );
            // }
            // if !stream_corrupted || !settings.connection.avoid_video_glitching {
            //     if !decoder::push_nal(header.timestamp, nal) {
            //         stream_corrupted = true;
            //         if let Some(sender) = &mut *CONTROL_SENDER.lock() {
            //             sender.send(&ClientControlPacket::RequestIdr).ok();
            //         }
            //         if let Some(stats) = &mut *STATISTICS_MANAGER.lock() {
            //             stats.report_video_packet_dropped(data.get_frame_index());
            //         }
            //         warn!(
            //             "Dropped video packet {}. Reason: Decoder saturation",
            //             data.get_frame_index()
            //         );
            //         frames_dropped += 1;
            //     } else {
            //         // frame is decoded correctly
            //         if let Some(stats) = &mut *STATISTICS_MANAGER.lock() {
            //             stats.report_video_packet_data(
            //                 header.timestamp,
            //                 data.get_frame_index(),
            //                 frames_dropped,
            //             );
            //         }
            //         frames_dropped = 0;
            //     }
            // } else {
            //     if let Some(sender) = &mut *CONTROL_SENDER.lock() {
            //         sender.send(&ClientControlPacket::RequestIdr).ok();
            //     }
            //     if let Some(stats) = &mut *STATISTICS_MANAGER.lock() {
            //         stats.report_video_packet_dropped(data.get_frame_index());
            //     }
            //     warn!(
            //         "Dropped video packet {}. Reason: Waiting for IDR frame",
            //         data.get_frame_index()
            //     );
            //     frames_dropped += 1;
        }
    }

    pub fn vsync<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    )-> impl Future<Output = ()> + Send + 'a {
        async move{
            let now = context.scheduler.time(); 
            let mut T_vsync = Duration::from_secs_f64(1.0 / self.framerate as f64);

            if let Some(video_frame) = self.decoder_queue.pop(){
                if let Some(interarrival) = now.checked_duration_since(self.last_decoded_frame_instant){
                    debug_bgprint!(DebugColor::Violet, "[DBG VSYNC] Frame decoded OK! Q: {}, Interarrival: {},  ok: {} | dropped: {} , data: {:?}", 
                        self.decoder_queue.len(),
                        interarrival.as_secs_f32(),
                        self.decoder_queue.ok_dequed_frame_counter,
                        self.decoder_queue.dropped_frame_counter,
                        &video_frame[0..10], ); 
                }
                else{eprint!("?? VSYNC ??? " );}
                
                
                self.last_decoded_frame_instant = now; 
                self.out_video_decoded.send(video_frame).await;  

            // self.output_statistics.send(stats);
            }
            else{
            }
            if self.decoder_queue.len() < TARGET_FRAMES_DECODER_QUEUE
            {   
                T_vsync = T_vsync.mul_f64(2.0) ; // duplicate wait so queue can fill
                debug_bgprint!(DebugColor::Violet, "[DBG VSYNC] Doubling time ({}) until frame deque due to length ({}) UNDER target ({})", T_vsync.as_secs_f32(), self.decoder_queue.len(), TARGET_FRAMES_DECODER_QUEUE  ); 
            }
            else if self.decoder_queue.len() > TARGET_FRAMES_DECODER_QUEUE {
                {   
                    T_vsync = T_vsync.mul_f64(0.5); // duplicate wait so queue can fill
                    debug_bgprint!(DebugColor::Violet, "[DBG VSYNC] Dividing time ({}) until frame deque due to length ({}) OVER target ({})", T_vsync.as_secs_f32(), self.decoder_queue.len(), TARGET_FRAMES_DECODER_QUEUE  ); 
                }
            }
            context.scheduler.schedule_event( T_vsync, Self::vsync, ()).unwrap(); 
            // TODO:
            // let video_frame = self.decoder_queue.pop_front();
            // let stats = NetworkStatisticsPacket::new();

            // self.output_statistics.send(stats);

            // context
            //     .scheduler
            //     .schedule_event(T_vsync, Self::vsync, ())
            //     .unwrap();
        }
    }


    pub async fn configure_streams(&mut self) {
        // obtained by printing debug. We're using channel for purposes of mpsc for separate client and server processes, and separating the network interface of each.
        let stream_port: u16 = 9944;
        let stream_protocol: SocketProtocol = SocketProtocol::Channel;
        let dscp: Option<DscpTos> = None;
        let server_send_buffer_bytes: SocketBufferSize = SocketBufferSize::Maximum;
        let client_recv_buffer_bytes: SocketBufferSize = SocketBufferSize::Maximum;
        let client_send_buffer_bytes: SocketBufferSize = SocketBufferSize::Maximum;
        let server_recv_buffer_bytes: SocketBufferSize = SocketBufferSize::Maximum;
        let packet_size: i32 = 1400;

        if let Ok(mut stream_socket) = StreamSocketBuilder::accept_from_server_mod(
            self.server_ip,
            stream_port,
            packet_size as _,
        ) {
            println!("Connection established!");
            self.is_streaming = true;

            self.input_app_video = Some(
                stream_socket.subscribe_to_stream::<VideoPacketHeader>(VIDEO, MAX_UNREAD_PACKETS),
            );
            self.input_app_audio =
                Some(stream_socket.subscribe_to_stream(AUDIO, MAX_UNREAD_PACKETS));
            self.input_app_haptics =
                Some(stream_socket.subscribe_to_stream::<Haptics>(HAPTICS, MAX_UNREAD_PACKETS));
            self.streamsocket_clone = Some(stream_socket.clone());

            // {
            //     // retrieve video packets in RX buffer
            //     if let Some(rx_socket) = self.input_app_video.clone(){

            //             let  buffer_tx: Vec<u8> = vec![0; CAPACITY_RX_BUFFER];
            //             let  buffer_rx = buffer_tx.clone();

            //             let arc_receiver = rx_socket.network_app_interface.clone();

            //             XRClient::read_network_interface_to_app(self, (), context, buffer_rx, arc_receiver);

            //             let mut buffer_app: Vec<u8> = vec![0;MAX_PACKET_SIZE_RECV];
            //             let data_app = rx_socket.inner.lock().unwrap().recv(&mut buffer_app);

            //             println!("DATA OF APP: {:?}", &buffer_app[0..100]);
            //     }
            // }
        }
    }

    pub async fn in_from_network(&mut self, frame: TimedFrame, context: &Context<Self>) {

        let packet_vec = frame.vec; 
        let now = frame.timestamp; 

        for packet in packet_vec{

            let header = packet.header_alvr;

            let buffer = packet.data_inner.clone();
            // println!("buffer is {:?}", &buffer[..100]);
    
            match header.stream_id.clone() {
                HAPTICS => {
                    if let Some(sock) = self.input_app_haptics.clone() {
                        let sender = sock.network_app_interface.lock().unwrap().send(&buffer);
                        let mut new_buffer: Vec<u8> = vec![0; MAX_PACKET_SIZE_RECV];
                        let receiver = sock.inner.lock().unwrap().recv(&mut new_buffer);
                        println!("receiver: {:?}", receiver);
                    }
                }
    
                AUDIO => {
                    if let Some(sock) = self.input_app_audio.clone() {
                        let sender = sock.network_app_interface.lock().unwrap().send(&buffer);
                        let mut new_buffer: Vec<u8> = vec![0; MAX_PACKET_SIZE_RECV];
                        let receiver = sock.inner.lock().unwrap().recv(&mut new_buffer);
                        println!("receiver: {:?}", receiver);
                    }
                }
                VIDEO => {
                    if let Some(sock) = self.input_app_video.clone() {
                        // println!("app lock");
                        let _sender = sock.network_app_interface.lock().unwrap().send(&buffer); // We send the packet from network to the application, where it needs to be now read and passed to the application!
                        // println!("reader lock");
    
                        if let Some(mut ssocket) = self.streamsocket_clone.as_mut() {
                            let _resulllt = StreamSocket::recv(&mut ssocket, sock.inner, context);

             
                            // println!("result of sender {:?}", sender ); 
                            // println!("Result of reader? {:?}" , resulllt);
                        }
    
                    } else {
                        println!("NO SOME??");
                    }
                }
                _ => {
                    println!("ERROR WRONG STREAM SENT? XRCLIENT {}", header.stream_id);
                }
            };
            // println!("\tEND shard {:?}, ", header.clone());
            context
                .scheduler
                .schedule_event(Duration::from_nanos(10), Self::video_receive_thread, ())
                .unwrap();
            ()

        }
        
    }

    fn recv_audio(data: ReceiverData<()>) {

        // actually we do nothing on this, just consume the packet
    }

    fn receive_control_packet() {}
}

impl Model for XRClient {}

pub struct SinkVideo_XR {
    pub counter_decoded: usize, 
    pub last_update: TaiTime<0>, 
    pub window_timed_fps: SlidingWindowTimely<f64>

}
impl SinkVideo_XR{
    pub fn new() ->  Self{
            Self { counter_decoded: (0), last_update: TaiTime::EPOCH, window_timed_fps: SlidingWindowTimely::new(1.0 / 60.0, 16., 1.0) , }
    }
    pub async fn in_video(&mut self, input: Vec<u8>, context: &Context<Self>){
           // Select only the first N bytes
        let now = context.scheduler.time(); 
        let n = 10; 
        let first_n_bytes = &input[..n.min(input.len())];
        
        if let Some(user_interarrival) = now.checked_duration_since(self.last_update){
            self.window_timed_fps.submit_sample(user_interarrival.as_secs_f64(), user_interarrival.as_secs_f32());
            let avg_samples = self.window_timed_fps.get_interval_buffer_mean(); 
            let length_samples = self.window_timed_fps.get_length(); 

            debug_bgprint!(DebugColor::Cyan, "[USER HMD] Reproducing video! {:?}. video_interarrival: {}, avg_fps: {}, ({:2.0} samples)", first_n_bytes, user_interarrival.as_secs_f32(), 1.0 / avg_samples,length_samples ); 
        }
        self.last_update = now; 
        return;
    }
}
impl Model for SinkVideo_XR{}

pub trait XRDevice {
    fn some_shared_method(&self);
}
impl XRDevice for XRClient {
    fn some_shared_method(&self) {
        println!("WOWWWWW");
    }
}
impl XRDevice for XRServer {
    fn some_shared_method(&self) {
        println!("WOWZA!!");
    }
}
#[derive(Clone)]
pub struct TimedFrame{
    vec: Vec<MpduPacket>,
    timestamp: TaiTime<0>, 
}


#[allow(non_camel_case_types)]
pub struct STA_extended {
    // extended class to PoissonGen
    pub output_network_port: Output<MpduPacket>,

    pub to_app_socket: Output<TimedFrame>,
    // pub to_app_socket_end_ampdu: Output<bool>,

    pub sta_id: i32,
    pub destination_id: i32,

    pub arrival_rate_BG: f64,
    pub mean_length_packets_BG: f64,
    pub num_packets_sent: usize,
    pub received_packet_counter: usize,

    pub sta_coordinates: Coords,
    pub does_sta_tx: bool,

    pub t_0: TaiTime<0>,
}

impl STA_extended {
    pub fn new(
        arrival_rate_bps: f64,
        mean_length: f64,
        src: i32,
        dest: i32,
        coordinates: Coords,
        does_sta_transmit: bool,
        rate_service_bps: f64,
        t0_sim: TaiTime<0>,
    ) -> Self {
        let arrival_rate_BG = arrival_rate_bps / mean_length;
        let effective_mu = rate_service_bps / mean_length;
        println!("\n*************************************************");
        println!("[DEBUG STA{}]\tCoordinates: {:?}\n\tDestination: STA{} | RATE_IN: {:.3} Mbps, Rate_service: {:.3} (packs/s),\n\t arrival_rate_BG (pack/s): {:.3}, Departure_rate: {:.3},  L = {}",
                            src, coordinates, dest,                     arrival_rate_bps/1E6, rate_service_bps / 1E6 , arrival_rate_BG,effective_mu ,mean_length);

        Self {
            output_network_port: Default::default(),

            to_app_socket: Default::default(),
            // to_app_socket_end_ampdu: Default::default(),

            sta_id: src,
            destination_id: dest,
            arrival_rate_BG: arrival_rate_BG,
            mean_length_packets_BG: mean_length,
            num_packets_sent: 0,
            sta_coordinates: coordinates,

            received_packet_counter: 0,
            does_sta_tx: does_sta_transmit,
            t_0: t0_sim,
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

    pub async fn input_XR_app(&mut self, mut packet: MpduPacket, context: &Context<Self>) {
        // do everything else to the packet:

        packet.length_packet = packet.header_alvr.packet_length as usize;
        packet.packet_id = self.num_packets_sent;

        packet.sta_src_id = self.sta_id;
        packet.sta_dest_id = self.destination_id;

        packet.sta_src_coords = self.sta_coordinates; 

        // println!("STA IN: packet.src_id = {}, packet.sta_dest_id = {}\n Coords src: {:?}", packet.sta_src_id, packet.sta_dest_id, packet.sta_src_coords);
        context.scheduler.schedule_event(Duration::from_nanos(10), Self::send_packet_wireless, packet).unwrap(); 
        // self.output_network_port.send(packet).await;
        self.num_packets_sent += 1;
    }
    pub async fn send_packet_wireless(&mut self, packet: MpduPacket){
        self.output_network_port.send(packet).await; 
    }

    pub async fn input_wireless(&mut self, ampdu_packet: AmpduPacket, context: &Context<Self>) {
        let mut packet_batch = Vec::new(); // Create a batch to hold packets
        let now = context.scheduler.time(); 

        if ampdu_packet.sta_dest_id == self.sta_id { // make sure we ignore packets not corresponding to STA
            for packet in ampdu_packet.mpdu_packets { // iterate through whole AMPDU

                debug_print!(
                    DebugColor::Red,
                    "[DBG STA{} IN]  ---Packet {} arrived from STA{} into STA{}",
                    // format_elapsed!(now),
                    self.sta_id,
                    packet.packet_id,
                    packet.sta_src_id,
                    packet.sta_dest_id,
                );
                // if packet.data_inner.len() >= 100 {
                // debug_print!(
                //     DebugColor::DarkRed,
                //     "[DBG NET_IN -> APP_OUT] : XR Packet received: ",
                //     // format_elapsed!(now.duration_since(self.t_0)),
                // );

                self.received_packet_counter += 1;
                packet_batch.push(packet); 
            }
        }
        if !packet_batch.is_empty() {
            let frame = TimedFrame{
                vec: packet_batch,
                timestamp: now,
            }; 
            context.scheduler.schedule_event(Duration::from_nanos(10), Self::to_app_socket_send, frame).unwrap(); 
            // Send the batch to the app socket in one go
            // self.to_app_socket.send(packet_batch).await;
        }
        yield_now();

    }
    pub async fn to_app_socket_send(&mut self, frame: TimedFrame){
        self.to_app_socket.send(frame).await; 
    }

    fn send_packet_BG<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            if self.does_sta_tx {
                // if STA is "TX type"         (and not "RX only")

                let mut packet = MpduPacket::new();

                let mut time_interarrival =
                    Duration::from_secs_f64(exponential(1.0 / self.arrival_rate_BG));

                time_interarrival = max(time_interarrival, Duration::from_nanos(1));

                let len_random = exponential(self.mean_length_packets_BG as f64) as usize;

                // let len_random = self.mean_length_packets_BG as usize;

                packet.length_packet = cmp::max(1, len_random);
                packet.packet_id = self.num_packets_sent;

                packet.sta_src_id = self.sta_id;
                packet.sta_dest_id = self.destination_id;

                packet.sta_src_coords = self.sta_coordinates;

                // self.output_network_port.send(packet).await;
                context.scheduler.schedule_event(Duration::from_nanos(10), Self::send_packet_wireless, packet).unwrap(); 

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
