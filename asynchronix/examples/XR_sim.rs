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

// use std::intrinsics::size_of;
use serde::{Deserialize, Serialize};
use std::mem;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use std::time::{SystemTime};

use once_cell::sync::Lazy;

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

use rand::{random, Rng};
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

pub struct SimRuntimeSockets {
    pub video_sender: Option<StreamSender<VideoPacketHeader>>,
    pub game_audio_sender: Option<StreamSender<()>>,
    pub tracking_receiver: Option<StreamReceiver<Tracking>>,
    pub haptics_sender: Option<StreamSender<Haptics>>,
    pub statistics_receiver: Option<StreamReceiver<ClientStatistics>>,
}
pub struct XRServer {
    pub ip_self: IpAddr, 
    pub ip_client: IpAddr, 

    pub t_0: TaiTime<0>,
    pub bitrate_manager: BitrateManager,

    pub output_video: Output<MpduPacket>,
    pub output_audio: Output<MpduPacket>,
    pub output_haptics: Output<MpduPacket>,

    pub is_streaming: bool,

    pub fps: f64,

    pub sockets: SimRuntimeSockets,
    pub frames_sent_counter: usize,
}
impl XRServer {
    pub fn new(ip_self:IpAddr, ip_client: IpAddr) -> Self {
        // let arrival_rate = arrival_rate_bps / mean_length;
        // let effective_mu = rate_service_bps /mean_length;
        // println!("\n*************************************************");
        // println!("[DEBUG STA{}]\tCoordinates: {:?}\n\tDestination: STA{} | RATE_IN: {:.3} Mbps, Rate_service: {:.3} (packs/s),\n\t Arrival_rate (pack/s): {:.3}, Departure_rate: {:.3},  L = {}",
        //                     src, coordinates, dest,                     arrival_rate_bps/1E6, rate_service_bps / 1E6 , arrival_rate,effective_mu ,mean_length);

        let sockets = SimRuntimeSockets {
            video_sender: None,
            game_audio_sender: None,
            tracking_receiver: None,
            haptics_sender: None,
            statistics_receiver: None,
        };
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

            // sender_video: StreamSender::new(VIDEO),
            // sender_audio: StreamSender::new(AUDIO),
            // sender_haptics: StreamSender::new(HAPTICS),
            output_video: Output::default(),
            output_audio: Output::default(),
            output_haptics: Output::default(),
            is_streaming: false,
            fps: 90.0,
            sockets,
            frames_sent_counter: 0,
        }
    }

    pub fn read_network_interface(mut buffer: Vec<u8> ,mut receiver: std::sync::MutexGuard<'_, Box<dyn SocketReader>>){
        let mut stop = false;
        while !stop {
            let ok = receiver.recv(&mut buffer);

            // Check if we successfully received data
            match ok {
                Ok(bytes_received) => {
                    if bytes_received == 0 {
                        // If no data is received, stop the loop
                        println!("No new data received, stopping.");
                        stop = true;
                    } else {

                        // TODO: CHECK WITH WIRESHARK ENCAPSULATION OF PACKET 

                        


                        println!("Parsed from connection output:");
                        if let Ok((packet_length, stream_id, next_packet_index, shards_count, shard_index, tx_r_instant)) = parse_shard_data(&buffer[..100]) {
                            println!("Parsed shard data:");
                            println!("Packet length: {}", packet_length);
                            println!("Stream ID: {}", stream_id);
                            println!("Next packet index: {}", next_packet_index);
                            println!("Shards count: {}", shards_count);
                            println!("Shard index: {}", shard_index);
                            println!("Transmit-receive instant: {} )", tx_r_instant);
                            println!("--------------------------------------------------------------------------------------------------------------------------------------------------------------------------__");

                            std::thread::sleep(Duration::from_secs(1));
                        } else {
                            println!("Failed to parse shard data, stopping.");
                            stop = true;
                        }
                    }
                },
                Err(_) => {
                    // Handle the error (e.g., if recv times out or there is a network issue)
                    println!("Error receiving data, stopping.");
                    stop = true;
                }
            }
        }
    }

    pub fn connection_pipeline(&mut self, client_ip: IpAddr){
    // no return from this function for now
        self.bitrate_manager = BitrateManager::new(MAX_HISTORY_SIZE, 90.0, INITIAL_BITRATE_MBPS);

        // let mut server_data_lock = SERVER_DATA_MANAGER.write(); //

        // let settings = server_data_lock.settings().clone();

        // obtained by printing debug. We're using channel for purposes of mpsc for separate client and server processes
        let stream_port: u16 = 9944;
        let stream_protocol: SocketProtocol = SocketProtocol::Channel;
        let dscp: Option<DscpTos> = None;
        let server_send_buffer_bytes: SocketBufferSize = SocketBufferSize::Maximum;
        let client_recv_buffer_bytes: SocketBufferSize = SocketBufferSize::Maximum;
        let client_send_buffer_bytes: SocketBufferSize = SocketBufferSize::Maximum;
        let server_recv_buffer_bytes: SocketBufferSize = SocketBufferSize::Maximum;
        let packet_size: i32 = 1400;

        let stream_socket_builder = StreamSocketBuilder::listen_for_server(
            Duration::from_secs(1),
            stream_port,
            stream_protocol.clone(),
            dscp.clone(),
            client_send_buffer_bytes,
            client_recv_buffer_bytes,
        )
        .to_con();

        // let (mut control_sender, mut control_receiver) = proto_control_socket // not doing control sockets for now, TODO!!
        // .split(STREAMING_RECV_TIMEOUT)
        // .to_con()?;

        // if let Err(e) = control_sender.send(&ClientControlPacket::StreamReady) {
        //     println!("Server disconnected. Cause: {e:?}");
        //     // set_hud_message(SERVER_DISCONNECTED_MESSAGE);
        //     return Ok(());
        // }

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
            
            // Create sender and receiver from the same stream socket
            let mut video_sender = stream_socket.request_stream::<VideoPacketHeader>(VIDEO);
            let mut video_receiver = stream_socket.subscribe_to_stream::<VideoPacketHeader>(VIDEO, MAX_UNREAD_PACKETS);
            
            if let mut send_socket = video_sender {

                let is_idr = false;
                let header = VideoPacketHeader::new(Duration::from_secs(1), is_idr);
                println!("Created header");
                
                let current_bitrate_mbps: f32 = self.bitrate_manager.last_target_bitrate_mbps;

                let mut buffer_emu = send_socket.get_buffer_emu(&header, current_bitrate_mbps).unwrap();
                
                // Fill buffer with meaningful test data
                // buffer_emu.inner = vec![42u8; 1400]; // Use packet_size for buffer
                // println!("Buffer size: {}\nWhole buffer: {:?}", buffer_emu.inner.len(), buffer_emu.);
                println!("DBG-> Bitrate: {} Mbps,  Buffer length: {}  buffer.LENGTH: {:?}",current_bitrate_mbps ,buffer_emu.inner.len(),buffer_emu.length); 

                let mut payload = buffer_emu.inner.clone(); 
                buffer_emu
                    .get_range_mut(0, payload.len())
                    .copy_from_slice(&payload);

                let mut arc_receiver = send_socket.network_interface.clone(); 


                let send_result = send_socket.send(buffer_emu); 

                const BUFFER_SIZE: usize = 2000;  

                let mut buffer: Vec<u8> = vec![0; BUFFER_SIZE]; 
                let mut receiver: std::sync::MutexGuard<'_, Box<dyn SocketReader>> = arc_receiver.lock().unwrap(); 

                XRServer::read_network_interface(buffer, receiver); 
                
                
                // println!("DATA!!!\nt\t\t{:?}\n\n************************************************************************************************************", &buffer[..100]); 
                
                // // After sending data, check if it was successfully sent.
                // let send_result = std::thread::spawn(move || {
                //     println!("Sending data...");
                //     match send_socket.send(buffer_emu) {
                //         Ok(_) => {
                //             println!("All sent successfully.");
                            
                //             Ok(())
                //         },
                //         Err(e) => {
                //             println!("Failed to send data: {:?}", e);
                //             Err(e)
                //         },
                //     }
                // });

                //     println!("DBG Receiving data"); 
                //     const BUFFER_SIZE: usize = 100000; 
                //     let mut buffer = vec![0; BUFFER_SIZE]; 

                //     let data = video_sender.network_interface.lock().unwrap().recv(&mut buffer[..]).unwrap();              
                //         //   let data = video_sender.network_interface.lock().unwrap().recv(&buffer); 
                //     println!("DATA!!!: {}", data); 
                // }); 
                                
            }
        }

        // self.sockets.video_sender = Some(stream_socket.request_stream::<VideoPacketHeader>(VIDEO));
        // self.sockets.game_audio_sender = Some(stream_socket.request_stream::<()>(AUDIO));
        // self.sockets.tracking_receiver = Some(stream_socket.subscribe_to_stream::<Tracking>(TRACKING, MAX_UNREAD_PACKETS));
        // self.sockets.haptics_sender = Some(stream_socket.request_stream::<Haptics>(HAPTICS));
        // self.sockets.statistics_receiver = Some(stream_socket.subscribe_to_stream::<ClientStatistics>(STATISTICS, MAX_UNREAD_PACKETS));

       
        // while self.is_streaming == true
        //     {
        //         // VIDEO STREAMING
        //         let current_bitrate_mbps = self.bitrate_manager.last_target_bitrate_mbps;
        //         let current_rate = 90; // here we can model the source dependent on fps

        //         let epsilon = Duration::ZERO;

        //         let elapsed = (context.scheduler.time() + epsilon).duration_since(self.t_0);
        //         let is_idr = false;

        //         let header =  VideoPacketHeader::new(elapsed, is_idr);

        //         let header_size: usize = bincode::serialized_size(&header).unwrap() as usize;
        //         let hidden_offset = SHARD_PREFIX_SIZE + header_size;

        //         if let Some(mut video_sender) = self.sockets.video_sender.clone() {

        //             let mut buffer_emu = video_sender.get_buffer_emu(&header, current_bitrate_mbps).unwrap();
        //             let payload = buffer_emu.inner.clone();

        //             buffer_emu
        //                 .get_range_mut(0, payload.len())
        //                 .copy_from_slice(&payload);

        //             video_sender.send(buffer_emu).ok();
        //             self.frames_sent_counter += 1;
        //             println!("Sending frame {}!", self.frames_sent_counter);

        //             let mut time_interarrival =
        //             Duration::from_secs_f64(exponential(1.0 / self.fps));

        //             // let packets_in_queue = self.
        //             // context.scheduler.schedule_event(time_interarrival, send_, arg)

        //         }

        //         // TODO!
        //         // match map_clone.write() {
        //         //     Ok(mut guard) => {
        //         //         *guard = cloned_socket_map;
        //         //     }
        //         //     Err(_) => {
        //         //         println!("Failed to acquire write lock in RTT hashmap");
        //         //     }
        //         // }
        //         // let frame_index = video_sender.get_last_packet_id();
        //         // let shards_count = video_sender.get_shards_count();
        //         // if let Some(stats) = &mut *STATISTICS_MANAGER.lock() {
        //         //     stats.report_frame_sent(header.timestamp, frame_index, shards_count);
        //         // }

        //     }
        //     // {
        //     //     //AUDIO STREAMING: TODO
        //     // }

        //     // {
        //     //     // HAPTICS STREAMING: TODO

        //     // }

        // ConResult::Ok(())
        // }
    }
}
// TODO : More functions to process inputs, handle ABR, etc.

impl Model for XRServer {}

pub struct XRClient {
    pub decoder_queue: VecDeque<Buffer>,

    pub output_statistics: Output<MpduPacket>,
    pub output_tracking: Output<MpduPacket>,

    pub coordinates: Coords,
    pub is_streaming: bool,

    pub frames_dropped_counter: usize,
}

impl XRClient {
    fn new() -> Self {
        Self {
            decoder_queue: VecDeque::new(),
            output_statistics: Output::default(),
            output_tracking: Output::default(),

            coordinates: Coords::new(),
            is_streaming: false,
            frames_dropped_counter: 0,
        }
    }
    fn input_packets(packet: MpduPacket) {

        // TODO: Based on the stream type (VIDEO, AUDIO, etc.) call one function or the other for the same packet
    }

    fn recv_video(&mut self, data: ReceiverData<VideoPacketHeader>)
    // inside the thread::spawn(move) of connection_pipeline
    {
        let packet_stats = NetworkStatisticsPacket {
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

            highest_rx_frame_index: data.get_highest_rx_frame_index(), // index of the highest video FRAME received during the interval between consecutive frames
            highest_rx_shard_index: data.get_highest_rx_shard_index(), // index of the highest video SHARD received ...
        };

        // // TODO: Make the function to actually schedule sending this packet!
        // let Ok((header, nal)) = data.get();

        // if !push_frame_decoder(header.timestamp, nal){

        //     println!("frame is dropped!!")
        //     // report_video_packet_dropped(data.get_frame_index()); // report in HistoryFrame the lost packet, but we're not doing HistoryFrame (?)
        //     self.frames_dropped_counter += 1;
        // }
        // else{
        //     // TODO:
        //     // stats.report_video_packet_data(header.timestamp, data.get_frame_index(), frames_dropped); //

        // }

        // if header.is_idr {}

        // if data.had_packet_loss(){

        // }
    }
    // fn send_statistics_packet(){
    //     // TODO!
    // }

    fn recv_audio(data: ReceiverData<()>) {

        // actually we do nothing on this, just consume the packet
    }

    fn receive_control_packet() {}
}

fn main() {

    let ip_src = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)); 
    let ip_dest = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)); 

    let mut XRServer = XRServer::new( ip_src, ip_dest );
    
    XRServer.connection_pipeline(ip_dest); 
}
