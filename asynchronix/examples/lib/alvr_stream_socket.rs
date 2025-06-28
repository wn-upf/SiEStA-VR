use asynchronix::model::Context;
use crossbeam::channel::{bounded, unbounded, Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::collections::HashMap;
use std::io::{Read};
#[allow(unused_imports)]
#[allow(dead_code)]
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use crate::lib::{get_third_octet, HevcParser};
use crate::DebugColor;
use ffmpeg_sidecar::command::FfmpegCommand;
use std::io::BufReader;
use crate::lib::CsvTrace;
use crate::print_green;
use crate::lib::models_mm1k::NetworkPattern;

use crate::{lib::DEBUG_PRINT_ENABLED, lib::USE_FFMPEG, print_pretty};

use crate::debug_bgprint;
use std::cell::RefCell;
use std::fmt::{self, Debug};
use std::{
    cmp::Ordering,
    collections::{HashSet, VecDeque},
    io,
    marker::PhantomData,
    mem,
    // net::{TcpListener, UdpSocket},
    time::Duration,
};
use crate::lib::models_XR::{XRDevice, FRAMERATE_WINDOWS, HEIGHT_ENCODER, WIDTH_ENCODER};
use crate::lib::models_XR::SHARD_PREFIX_SIZE;
// use crate::lib::DebugColor;
use anyhow::{anyhow, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::error::Error;
// use std::io::{Read, Write};
use std::net::IpAddr;

use std::result::Result::Ok;
use tai_time::TaiTime;
use csv::Writer;

use crate::lib::alvr_packets::{DeviceMotion, Pose};

// use super::alvr_packets::NetworkStatisticsPacket;


pub const ALVR_ORIGINAL_SOCKETRX_BEHAVIOR: bool = false; // TODO: Bring these 2 from input args to simulator
pub const INTRAREFRESH_ENABLED: bool = true;

// pub const UPDATE_BITRATE_INTERVAL: Duration = Duration::from_secs(1);
pub const MAX_HISTORY_SIZE: usize = 256;
pub const INITIAL_FRAMERATE_FPS: f32 = 90.0;

pub const CHUNK_DURATION_F64_S: f64 = 1.5;
pub const DEADLINE_PACKETS_S: Duration = Duration::from_millis(100);
pub const MAX_DEADLINE_IN_STATS: usize = 10;
pub const OFFSET_VIDEO: f64 = 5.0;

// pub const CHUNK_SIZE_FRAMES: usize = 300;
pub const IDR_FRAME_SIZE_GOP: usize = 60;

pub const MAX_PACKET_SIZE_RECV: usize = 2000 * 8;
pub const TRACKING: u16 = 0;
pub const HAPTICS: u16 = 1;
pub const AUDIO: u16 = 2;
pub const VIDEO: u16 = 3;
pub const STATISTICS: u16 = 4;

pub const CONTROL_STREAM: u16 = 5;

pub const _SERVER_DISCONNECTED_MESSAGE: &str = "The streamer has disconnected.";
pub struct ChunkedHevcEncoder {
    input: String,
    width: u32,
    height: u32,
    bitrate: String,
    chunk_duration: f64,
    current_offset: f64,
    frame_tx: Sender<Vec<u8>>,
    frame_rx: Receiver<Vec<u8>>,

    frame_queue: VecDeque<Vec<u8>>,
    parser: HevcParser,
    encoder_str: String,

    framerate: f32, 
}
#[allow(unused)]
impl ChunkedHevcEncoder {
    pub fn new(
        input: &str,
        width: u32,
        height: u32,
        bitrate: &str,
        chunk_duration: f64,
        string: String,
        offset_video: f64,
        framerate: f32, 
    ) -> Self {
        println!("Initializing chunkedhevcencoder");
        let (frame_tx, frame_rx) = bounded(100);

        Self {
            input: input.to_string(),
            width,
            height,
            bitrate: bitrate.to_string(),
            chunk_duration,
            current_offset: offset_video,
            frame_tx,
            frame_rx,
            frame_queue: VecDeque::new(),
            parser: HevcParser::new(),
            encoder_str: string.clone(),
            framerate, 
        }
    }

    pub fn clear_parser(&mut self) {
        self.parser.buffer.clear();
    }

    /// Continuously spawn ffmpeg processes to produce video chunks.
    /// Each process is configured to start at the current_offset and run for chunk_duration seconds.
    /// As data is read from ffmpeg’s stdout, it is fed to a HevcParser which extracts complete frames.
    /// Each complete frame is sent via the async channel.

    pub async fn start_chunking(&mut self, bitrate_mbps: f32) {
        let bitrate_adjusted_fps = bitrate_mbps * FRAMERATE_WINDOWS as f32 / self.framerate;
        // Since the encoded video samples are 60fps, we thus adjust bitrate to match with the actual second units.

        self.bitrate = format!("{:.2}M", bitrate_adjusted_fps);

        println!(
            "{} CHUNKING with bitrate {} Mbps",
            self.encoder_str, bitrate_mbps
        );
        self.parser.buffer.clear();
        let mut command = FfmpegCommand::new();
        if INTRAREFRESH_ENABLED {
            command
                .hwaccel("cuda")
                .args(&["-ss", &self.current_offset.to_string()])
                .args(&["-t", &self.chunk_duration.to_string()])
                .args(&["-re"]) // read at real-time speed
                .input(&self.input)
                .args(&[
                    "-vf",
                    &format!(
                        "scale={}:{}:force_original_aspect_ratio=disable,format=yuv420p",
                        self.width, self.height
                    ),
                ])
                .args(&["-c:v", "hevc_nvenc"])
                .args(&["-preset", "fast"])
                .args(&["-rc", "cbr"])
                .args(&["-b:v", &self.bitrate, "-maxrate", &self.bitrate])
                .args(&["-bufsize", &self.bitrate])   // 1-second VBV window (optional but keeps it tight)
                // the throughput distribution will match that of the bitrate target strictly by padding. 
                .args(&["-rc-lookahead", "0"])
                .args(&["-g", "0"]) // Disable GOP, intra-refresh instead
                .args(&["-intra-refresh", "1"]) // Enable intra-refresh coding
                .args(&["-movflags", "+frag_keyframe+empty_moov"])
                .args(&["-flush_packets", "1"])
                .args(&["-bsf:v", "hevc_mp4toannexb"])
                .args(&["-an"])
                .args(&["-f", "hevc", "-"]); // output raw HEVC
        } else {
            command
                .hwaccel("cuda")
                .args(&["-ss", &self.current_offset.to_string()])
                .args(&["-t", &self.chunk_duration.to_string()])
                .args(&["-re"]) // read at realtime speed
                .input(&self.input)
                .args(&[
                    "-vf",
                    &format!(
                        "scale={}:{}:force_original_aspect_ratio=disable,format=yuv420p",
                        self.width, self.height
                    ),
                ])
                .args(&["-c:v", "hevc_nvenc"])
                .args(&["-preset", "fast"])
                .args(&["-rc", "cbr"])
                .args(&["-b:v", &self.bitrate, "-maxrate", &self.bitrate])
                .args(&["-bufsize", &self.bitrate])   // 1-second VBV window (optional but keeps it tight)

                .args(&["-rc-lookahead", "0"])
                .args(&["-g", &format!("{:.0}", IDR_FRAME_SIZE_GOP)]) // using your GOP size constant
                .args(&["-movflags", "+frag_keyframe+empty_moov"])
                .args(&["-flush_packets", "1"])
                .args(&["-bsf:v", "hevc_mp4toannexb"])
                .args(&["-an"])
                .args(&["-f", "hevc", "-"]); // output raw HEVC
        }

        // Spawn the ffmpeg process for this chunk.
        let mut child = command.spawn().unwrap();
        let stdout = child.take_stdout().unwrap();
        let mut reader = BufReader::new(stdout);

        // if let Some(stderr) = child.take_stderr() {
        //     let mut err_reader = std::io::BufReader::new(stderr);
        //     std::thread::spawn(move || {
        //         for line in err_reader.lines() {
        //             match line {
        //                 Ok(l) => println!("ffmpeg stderr: {}", l),
        //                 Err(e) => {
        //                     eprintln!("Error reading ffmpeg stderr: {}", e);
        //                     break;
        //                 }
        //             }
        //         }
        //     });
        // }

        // let mut parser = HevcParser::new();
        let mut buf = [0u8; 4096];

        loop {
            match reader.read(&mut buf) {
                Ok(0) => break, // end of chunk
                Ok(n) => {
                    self.parser.add_data(&buf[..n]);
                    // Extract complete frames and send them on the channel.
                    let frames = self.parser.get_frames();
                    for frame in frames {
                        if let Err(e) = self.frame_tx.send(frame) {
                            eprintln!("{} Error sending frame: {}", e, self.encoder_str,);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("{} Error reading ffmpeg chunk: {}", e, self.encoder_str,);
                    break;
                }
            }
        }
        // let _ = child.wait();
        let _ = child.wait();

        // Update offset for the next chunk.
        self.current_offset += self.chunk_duration;
        // Add a safety check to clear parser buffer if it gets too large
        if self.parser.buffer.len() > 1_000_000_00 {
            // 100MB limit
            println!(
                "{} Parser buffer getting too large ({}), clearing",
                self.parser.buffer.len(),
                self.encoder_str,
            );
            self.parser.buffer.clear();
        }
    }
    pub async fn next_frame(&mut self) -> Option<Vec<u8>> {
        // First try parser's frames - keeping original behavior
        let extracted_frames = self.parser.get_frames();
        if !extracted_frames.is_empty() {
            println!(
                "{} Extracted {} frames from parser buffer, size {}",
                self.encoder_str,
                extracted_frames.len(),
                self.parser.buffer.len()
            );

            // Store all but first frame for future use
            for frame in extracted_frames.iter().skip(1) {
                self.frame_queue.push_back(frame.clone());
            }

            // Return the first extracted frame immediately
            return Some(extracted_frames[0].clone());
        }

        // Check queue next
        if let Some(frame) = self.frame_queue.pop_front() {
            return Some(frame);
        }

        // Only now try channel
        if let Ok(frame) = self.frame_rx.recv_timeout(Duration::from_millis(1)) {
            return Some(frame);
        }

        None
    }
}

/// A parser for HEVC bitstreams to extract individual frames
/// A parser for HEVC bitstreams to extract individual frames

pub trait SocketWriter: Send {
    fn send(&mut self, buffer: &[u8]) -> Result<()>;
}

// Trait used to abstract different socket (or other input/output) implementations. The funtionality
// is the intersection of the functionality of each implementation, that is it inheirits all
// limitations
pub trait SocketReader: Send {
    // Returns number of bytes written. buffer must be big enough to be able to receive a full
    // packet (size of MTU) otherwise data will be corrupted. The size of the data is
    fn recv(&mut self, buffer: &mut [u8]) -> ConResult<usize>;

    fn peek(&self, buffer: &mut [u8]) -> ConResult<usize>;
}

impl SocketWriter for Sender<Vec<u8>> {
    fn send(&mut self, buffer: &[u8]) -> Result<()> {
        Sender::send(self, buffer.to_vec()).unwrap();
        Ok(())
    }
}

#[derive(Clone)]
pub struct BufferedReceiver<T> {
    receiver: Receiver<T>,
    buffer: RefCell<Option<T>>,
}

impl<T> BufferedReceiver<T> {
    pub fn new(receiver: Receiver<T>) -> Self {
        BufferedReceiver {
            receiver,
            buffer: RefCell::new(None),
        }
    }
}

impl SocketReader for BufferedReceiver<Vec<u8>> {
    fn recv(&mut self, buffer: &mut [u8]) -> ConResult<usize> {
        // First check if we have data in the buffer
        if let Some(data) = self.buffer.take() {
            let data_len = data.len();
            if data_len <= buffer.len() {
                buffer[..data_len].copy_from_slice(&data);
                Ok(data_len)
            } else {
                // Put the data back in the buffer since it didn't fit
                *self.buffer.borrow_mut() = Some(data);
                Err(ConnectionError::Other(anyhow!("Buffer too small")))
            }
        } else {
            // If no buffered data, try to receive new data
            match self.receiver.try_recv() {
                Ok(data) => {
                    let data_len = data.len();
                    if data_len <= buffer.len() {
                        buffer[..data_len].copy_from_slice(&data);
                        Ok(data_len)
                    } else {
                        Err(ConnectionError::Other(anyhow!("Buffer too small")))
                    }
                }
                Err(TryRecvError::Empty) => Ok(0),
                Err(TryRecvError::Disconnected) => {
                    Err(ConnectionError::Other(anyhow!("Channel disconnected")))
                }
            }
        }
    }

    fn peek(&self, buffer: &mut [u8]) -> ConResult<usize> {
        let mut buffer_guard = self.buffer.borrow_mut();

        // If we don't have data in the buffer, try to receive it
        if buffer_guard.is_none() {
            match self.receiver.try_recv() {
                Ok(data) => {
                    *buffer_guard = Some(data);
                }
                Err(TryRecvError::Empty) => return Ok(0),
                Err(TryRecvError::Disconnected) => {
                    return Err(ConnectionError::Other(anyhow!("Channel disconnected")))
                }
            }
        }

        // Now we either had data in the buffer or just received it
        if let Some(ref data) = *buffer_guard {
            let data_len = data.len();
            if data_len <= buffer.len() {
                buffer[..data_len].copy_from_slice(data);
                Ok(data_len)
            } else {
                Err(ConnectionError::Other(anyhow!("Buffer too small")))
            }
        } else {
            // This should never happen due to the logic above
            Ok(0)
        }
    }
}

// Helper function to create a buffered channel
pub fn buffered_channel<T>() -> (Sender<T>, BufferedReceiver<T>) {
    let (sender, receiver) = unbounded();
    (sender, BufferedReceiver::new(receiver))
}
#[allow(unused)]
#[derive(Debug, PartialEq, Eq)]
pub enum ConError {
    WouldBlock,
    Disconnected,
    BufferTooSmall,
    Unsupported,
}

impl std::error::Error for ConError {}

impl std::fmt::Display for ConError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConError::WouldBlock => write!(f, "Operation would block"),
            ConError::Disconnected => write!(f, "Disconnected"),
            ConError::BufferTooSmall => write!(f, "Buffer is too small"),
            ConError::Unsupported => write!(f, "Operation is not supported"),
        }
    }
}


#[derive(Clone)]
pub struct InProgressPacket {
    buffer: Vec<u8>,
    buffer_length: usize,
    received_shard_indices: HashSet<usize>,
    deadline: Option<TaiTime<0>>,
    num_shards_expected: usize,
    id_frame: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct VideoPacketHeader {
    pub timestamp: Duration,
    pub is_idr: bool,
}

impl VideoPacketHeader {
    pub fn new(timestamp: Duration, is_idr: bool) -> Self {
        Self { timestamp, is_idr }
    }
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct FaceData {
    pub eye_gazes: [Option<Pose>; 2],
    pub fb_face_expression: Option<Vec<f32>>, // issue: Serialize does not support [f32; 63]
    pub htc_eye_expression: Option<Vec<f32>>,
    pub htc_lip_expression: Option<Vec<f32>>, // issue: Serialize does not support [f32; 37]
}

// Note: face_data does not respect target_timestamp.
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Tracking {
    pub target_timestamp: Duration,
    pub device_motions: Vec<(u64, DeviceMotion)>,
    pub hand_skeletons: [Option<[Pose; 26]>; 2],
    pub face_data: FaceData,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Haptics {
    pub device_id: u64,
    pub duration: Duration,
    pub frequency: f32,
    pub amplitude: f32,
}

/// Memory buffer that contains a hidden prefix
#[derive(Default, Debug)]
pub struct Buffer<H = ()> {
    pub inner: Vec<u8>,
    pub hidden_offset: usize, // this corresponds to prefix + header
    pub length: usize,
    pub _phantom: PhantomData<H>,
}
#[allow(unused)]
impl<H> Buffer<H> {
    /// Length of payload (without prefix)
    #[must_use]
    pub fn len(&self) -> usize {
        self.length
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the whole payload of the buffer
    pub fn get(&self) -> &[u8] {
        &self.inner[self.hidden_offset..][..self.length]
    }

    /// If the range is outside the valid range, new space will be allocated
    /// NB: the offset parameter is applied on top of the internal offset of the buffer
    pub fn get_range_mut(&mut self, offset: usize, size: usize) -> &mut [u8] {
        let required_size = self.hidden_offset + offset + size;
        if required_size > self.inner.len() {
            self.inner.resize(required_size, 0);
        }

        self.length = self.length.max(offset + size);

        &mut self.inner[self.hidden_offset + offset..][..size]
    }

    /// If length > current length, allocate more space
    pub fn set_len(&mut self, length: usize) {
        self.inner.resize(self.hidden_offset + length, 0);
        self.length = length;
    }
}

const Q_KALMAN: f32 = 10E-8;

#[derive(Clone)]
pub struct KalmanFilter {
    ow_delay: f32,
    m_current: f32,
    p_current: f32,
    noise_prev: f32,
    residual_z: f32,
    noise_estimation: f32,
    m_prev: f32,
    p_prev: f32,
    k_gain: f32,
    measured_delay: f32,
}

impl Default for KalmanFilter {
    fn default() -> Self {
        KalmanFilter {
            ow_delay: 0.0,
            m_current: 0.0,
            p_current: 0.1,
            noise_prev: 0.0,
            residual_z: 0.0,
            noise_estimation: 0.0,
            m_prev: 0.0,
            p_prev: 0.0,
            k_gain: 0.0,
            measured_delay: 0.0,
        }
    }
}

#[derive(Clone)]
struct ShardMapStats {
    tx_r_instant: f32,
    rx_instant: TaiTime<0>,
    rx_bytes: u32,
    rx_bytes_app: u32,
}

// struct RecvState {
//     packet_length: usize, // contains length prefix
//     packet_cursor: usize, // counts also the length prefix bytes

//     packet_index: u32,

// }

#[derive(Clone)]
struct RecvState {
    shard_length: usize, // contains prefix length itself
    stream_id: u16,
    packet_index: u32,
    shards_count: usize,
    shard_index: usize,
    packet_cursor: usize, // counts also the prefix bytes
    overwritten_data_backup: Option<[u8; SHARD_PREFIX_SIZE]>,
    should_discard: bool,
    frame_first_shard_deadline: Option<TaiTime<0>>,
}

#[derive(Clone)]
struct ReconstructedPacket {
    index: u32,
    buffer: Vec<u8>,
    size: usize, // contains prefix

    frame_index: u32,
    frame_span: f32,
    frame_interarrival: f32,
    interarrival_jitter: f32,
    ow_delay: f32,
    filtered_ow_delay: f32,

    rx_bytes: u32,
    bytes_in_frame: u32,
    bytes_in_frame_app: u32,

    rx_shard_counter: u32,
    duplicated_shard_counter: u32,

    highest_rx_frame_index: i32,
    highest_rx_shard_index: i32,
}

impl fmt::Debug for ReconstructedPacket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReconstructedPacket")
            .field("index", &self.index)
            // buffer is omitted
            .field("size", &self.size)
            .field("frame_index", &self.frame_index)
            .field("frame_span", &self.frame_span)
            .field("frame_interarrival", &self.frame_interarrival)
            .field("interarrival_jitter", &self.interarrival_jitter)
            .field("ow_delay", &self.ow_delay)
            .field("filtered_ow_delay", &self.filtered_ow_delay)
            .field("rx_bytes", &self.rx_bytes)
            .field("bytes_in_frame", &self.bytes_in_frame)
            .field("bytes_in_frame_app", &self.bytes_in_frame_app)
            .field("rx_shard_counter", &self.rx_shard_counter)
            .field("duplicated_shard_counter", &self.duplicated_shard_counter)
            .field("highest_rx_frame_index", &self.highest_rx_frame_index)
            .field("highest_rx_shard_index", &self.highest_rx_shard_index)
            .finish()
    }
}
#[derive(Clone)]
struct StreamRecvComponents {
    used_buffer_sender: Sender<Vec<u8>>,
    used_buffer_receiver: Receiver<Vec<u8>>,
    packet_queue: Sender<ReconstructedPacket>,
    in_progress_packets: HashMap<u32, InProgressPacket>,
    discarded_shards_sink: InProgressPacket,
}
#[derive(Clone)]
pub struct FrameTracker {
    // Struct to store frame index and transmission instant pairs
    pub map: HashMap<u32, TaiTime<0>>,
    pub queue: VecDeque<u32>,
    pub max_size: usize,
}

impl FrameTracker {
    pub fn new() -> Self {
        FrameTracker {
            map: HashMap::new(),
            queue: VecDeque::new(),
            max_size: 1000,
        }
    }
    pub fn insert(&mut self, frame_id: u32, instant: TaiTime<0>) {
        self.map.insert(frame_id, instant);
        self.queue.push_back(frame_id);
        // debug_bgprint!(DebugColor::Green, "Inserted Frame (K: {} , V: {:.9}) in rtt map", frame_id, format_elapsed!(instant));

        // Drop oldest pairs if size exceeds max_size
        while self.queue.len() > self.max_size {
            if let Some(oldest_frame_id) = self.queue.pop_front() {
                self.map.remove(&oldest_frame_id);
                println!("FrameTracker removed {}", oldest_frame_id);
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum DropProbability {
    Low = 0x01,
    Medium = 0x10,
    High = 0x11,
}

#[derive(Debug)]
#[allow(unused)]
pub enum ConnectionError {
    TryAgain(anyhow::Error),
    Other(anyhow::Error),
}
#[allow(unused)]
pub trait AnyhowToCon<T> {
    fn to_con(self) -> ConResult<T>;
}

impl<T> AnyhowToCon<T> for Result<T, anyhow::Error> {
    fn to_con(self) -> ConResult<T> {
        self.map_err(ConnectionError::Other)
    }
}

pub type ConResult<T = ()> = Result<T, ConnectionError>;

pub fn try_again<T>() -> ConResult<T> {
    Err(ConnectionError::TryAgain(anyhow!("Try again")))
}
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum SocketProtocol {
    // Tcp,
    // Udp,
    Channel,
}
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum SocketBufferSize {
    Default,
    Maximum,
    Custom(u32),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum DscpTos {
    BestEffort,

    ClassSelector(u8),

    AssuredForwarding {
        class: u8,
        drop_probability: DropProbability,
    },

    ExpeditedForwarding,
}
#[allow(unused)]
pub struct ReceiverData<H> {
    buffer: Option<Vec<u8>>,
    size: usize, // counting the prefix
    used_buffer_queue: Sender<Vec<u8>>,
    had_packet_loss: bool,
    _phantom: PhantomData<H>,

    frame_index: u32,

    frame_span: f32,
    frame_interarrival: f32,
    interarrival_jitter: f32,
    ow_delay: f32,
    filtered_ow_delay: f32,

    rx_bytes: u32,
    bytes_in_frame: u32,
    bytes_in_frame_app: u32,
    frames_skipped: u32,
    rx_shard_counter: u32,
    duplicated_shard_counter: u32,

    highest_rx_frame_index: i32,
    highest_rx_shard_index: i32,
    // tx_instant_first_shard: TaiTime<0>,
}
#[allow(unused)]
impl<H> ReceiverData<H> {
    pub fn get_buffer(&self) -> Vec<u8> {
        if let Some(buf) = self.buffer.clone() {
            buf
        } else {
            vec![2 as u8, 2]
        }
    }

    pub fn had_packet_loss(&self) -> bool {
        self.had_packet_loss
    }
    pub fn get_frame_index(&self) -> u32 {
        self.frame_index
    }
    pub fn get_frame_span(&self) -> f32 {
        self.frame_span
    }
    pub fn get_frame_interarrival(&self) -> f32 {
        self.frame_interarrival
    }
    pub fn get_interarrival_jitter(&self) -> f32 {
        self.interarrival_jitter
    }
    pub fn get_ow_delay(&self) -> f32 {
        self.ow_delay
    }
    pub fn get_filtered_ow_delay(&self) -> f32 {
        self.filtered_ow_delay
    }
    pub fn get_rx_bytes(&self) -> u32 {
        self.rx_bytes
    }
    pub fn get_bytes_in_frame(&self) -> u32 {
        self.bytes_in_frame
    }
    pub fn get_bytes_in_frame_app(&self) -> u32 {
        self.bytes_in_frame_app
    }
    pub fn get_frames_skipped(&self) -> u32 {
        self.frames_skipped
    }
    pub fn get_rx_shard_counter(&self) -> u32 {
        self.rx_shard_counter
    }
    pub fn get_duplicated_shard_counter(&self) -> u32 {
        self.duplicated_shard_counter
    }
    pub fn get_highest_rx_frame_index(&self) -> i32 {
        self.highest_rx_frame_index
    }
    pub fn get_highest_rx_shard_index(&self) -> i32 {
        self.highest_rx_shard_index
    }
    // pub fn get_tx_instant(&self)-> TaiTime<0> {
    //     self.tx_instant_first_shard
    // }
}
#[allow(unused)]
impl<H: DeserializeOwned> ReceiverData<H> {
    pub fn get(&self) -> Result<(&[u8])> {
        // println!("[DBG get data]" );
        let mut data: &[u8] = &self.buffer.as_ref().unwrap()[(SHARD_PREFIX_SIZE + 13)..self.size];

        // print_pretty!(
        //     DebugColor::Purple,
        //     "\t[CLIENT DCD ] .get() at client frame size: {} bytes ({} KB)\nData = {:?}",
        //     data.len(),
        //     data.len() / 1024,
        //     &data[..200]
        // );
        // // This will partially consume the slice, leaving only the actual payload
        // match header = bincode::deserialize_from(&mut data){

        Ok((data))
    }
    // pub fn get_header(&self) -> Result<H> {
    //     Ok(self.get()?.0)
    // }
}
#[derive(Clone)]
pub struct StreamReceiver<H> {
    // debug_receiver_channel: Arc<Mutex<Vec<u8>>>,
    packet_receiver: Receiver<ReconstructedPacket>,
    used_buffer_queue: Sender<Vec<u8>>,
    last_packet_index: Option<u32>,
    _phantom: PhantomData<H>,

    frame_interarrival: f32,
    rx_bytes: u32,
    rx_shard_counter: u32,
    duplicated_shard_counter: u32,

    pub network_app_interface: Arc<Mutex<Box<dyn SocketWriter>>>,
    pub inner: Arc<Mutex<Box<dyn SocketReader>>>,
}
#[derive(Clone)]
pub struct StreamSocket {
    max_packet_size: usize,
    send_socket: Arc<Mutex<Box<dyn SocketWriter>>>,
    receive_socket: Arc<Mutex<Box<dyn SocketReader>>>,
    shard_recv_state: Option<RecvState>,
    stream_recv_components: HashMap<u16, StreamRecvComponents>,

    transport_protocol: SocketProtocol,

    map_rx: HashMap<u32, HashMap<usize, ShardMapStats>>,
    rx_bytes: u32,

    prev_shard_tx_r_instant: Option<f32>,
    prev_shard_rx_instant: Option<TaiTime<0>>,

    interarrival_jitter: f32,

    kalman: KalmanFilter,
    prev_frame_rx_instant: TaiTime<0>,
    prev_frame_tx_r_instant: Option<f32>,

    rx_shard_counter: u32,
    duplicated_shard_counter: u32,

    highest_rx_shard_index: i32,
    highest_rx_frame_index: i32,

    pub lost_shards_deadline_map: HashMap<u32, usize>, // key: frame_id, val: shard loss
}
#[allow(unused)]
impl StreamSocket {
    pub fn request_stream<T>(&self, stream_id: u16, t0: TaiTime<0>) -> StreamSender<T> {
        StreamSender::<T> {
            inner: Arc::clone(&self.send_socket),
            app_network_interface: Arc::clone(&self.receive_socket),
            stream_id,
            max_packet_size: self.max_packet_size,
            next_packet_index: 0,
            used_buffers: vec![],

            _phantom: PhantomData,
            shards_count: 0,
            ref_time: t0,
            frame_tracker: FrameTracker::new(),
            ffmpeg_encoder: None,
            // ffmpeg_maxbitrate_encoder: None,
            // chunk_frames: VecDeque::new(),
            time_since_last_update: t0,
            csv_trace: CsvTrace::default(), 
        }
    }

    pub fn subscribe_to_stream<T>(
        &mut self,
        stream_id: u16,
        max_concurrent_buffers: usize,
    ) -> StreamReceiver<T> {
        let (packet_sender, packet_receiver): (
            Sender<ReconstructedPacket>,
            Receiver<ReconstructedPacket>,
        ) = unbounded();
        let (used_buffer_sender, used_buffer_receiver): (Sender<Vec<u8>>, Receiver<Vec<u8>>) =
            unbounded();

        // let EXPECTED_NO_PACKETS = match stream_id{ // TODO:
        //     VIDEO => let bitrate = ....
        //     AUDIO =>
        //     STATISTICS =>
        //     HAPTICS =>
        //     TRACKING =>
        // }
        let EXPECTED_NO_PACKETS: usize = 100;

        // Initialize the used buffers
        for _ in 0..max_concurrent_buffers {
            used_buffer_sender.send(vec![]).unwrap(); // Ignoring the result as in the original code
        }

        self.stream_recv_components.insert(
            stream_id,
            StreamRecvComponents {
                used_buffer_sender: used_buffer_sender.clone(),
                used_buffer_receiver,
                packet_queue: packet_sender,
                in_progress_packets: HashMap::new(),
                discarded_shards_sink: InProgressPacket {
                    buffer: vec![],
                    buffer_length: 0,
                    received_shard_indices: HashSet::new(),
                    deadline: None,
                    num_shards_expected: 0,
                    id_frame: 0,
                },
            },
        );

        StreamReceiver {
            packet_receiver,
            used_buffer_queue: used_buffer_sender,
            _phantom: PhantomData,
            last_packet_index: None,
            frame_interarrival: 0.,
            rx_bytes: 0,
            rx_shard_counter: 0,
            duplicated_shard_counter: 0,

            network_app_interface: Arc::clone(&self.send_socket),
            inner: Arc::clone(&self.receive_socket),
        }
    }

    pub fn flush_shards_lost_deadline(&mut self) -> (Vec<u32>, Vec<usize>) {
        let mut total_lost_deadline = 0;
        // Collect keys into a vector before modifying the map
        let keys: Vec<_> = self.lost_shards_deadline_map.keys().cloned().collect();

        let mut vec_keys = vec![];
        let mut vec_lost = vec![];

        // Now you can iterate over the keys and remove them from the map
        for frame_deadlined in keys {
            vec_keys.push(frame_deadlined);

            let lost_in_frame = self
                .lost_shards_deadline_map
                .remove(&frame_deadlined)
                .unwrap();
            // println!("LOST {} packets in frame {}", self.lost_shards_deadline_map.get(&frame_deadlined).unwrap(), frame_deadlined);
            debug_bgprint!(
                DebugColor::Red,
                "[Flush deadline] Packets lost in frame {}: {:?}",
                frame_deadlined,
                lost_in_frame
            );
            vec_lost.push(lost_in_frame);
            total_lost_deadline += lost_in_frame;
        }
        (vec_keys, vec_lost)
    }

    pub fn recv<T: XRDevice + asynchronix::model::Model>(
        &mut self,
        ip_client: IpAddr,
        arc_receiver: Arc<Mutex<Box<dyn SocketReader>>>,
        context: &Context<T>,
    ) -> ConResult {
        let now = context.scheduler.time();

        // println!("Recv function of shards!");
        let shard_recv_state_mut = if let Some(state) = &mut self.shard_recv_state {
            state
        } else {
            let mut bytes = [0; MAX_PACKET_SIZE_RECV];
            // let count = self.receive_socket.lock().unwrap().recv(&mut bytes)?;
            let count = arc_receiver.lock().unwrap().peek(&mut bytes).unwrap();
            // println!("DEBBG -> BYTES INSIDE PACKET {}", count);
            // println!("DEBBG -> Data inside packet{:?}", &bytes[0..100]);

            if count < SHARD_PREFIX_SIZE {
                return try_again();
            }

            // todo: switch to little endian
            // todo: do not remove sizeof<u32> for packet length
            let shard_length = mem::size_of::<u32>()
                + u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
            let stream_id = u16::from_be_bytes(bytes[4..6].try_into().unwrap());
            let packet_index = u32::from_be_bytes(bytes[6..10].try_into().unwrap());
            let shards_count = u32::from_be_bytes(bytes[10..14].try_into().unwrap()) as usize;
            let shard_index = u32::from_be_bytes(bytes[14..18].try_into().unwrap()) as usize;
            let tx_r_instant = f32::from_be_bytes(bytes[18..22].try_into().unwrap());

            if stream_id == VIDEO {
                let rx_instant = now;

                if self.highest_rx_frame_index == packet_index as i32 {
                    if self.highest_rx_shard_index < shard_index as i32 {
                        self.highest_rx_shard_index = shard_index as i32;
                    }
                } else if self.highest_rx_frame_index < packet_index as i32 {
                    self.highest_rx_frame_index = packet_index as i32;
                    self.highest_rx_shard_index = shard_index as i32;
                }

                let header_bytes_transport: u32 = match self.transport_protocol {
                    // SocketProtocol::Udp => 42,
                    // SocketProtocol::Tcp => 54,
                    SocketProtocol::Channel => 0, // let's NOT emulate UDP for now
                };
                let packet = ShardMapStats {
                    tx_r_instant,
                    rx_instant,
                    rx_bytes: shard_length as u32 + header_bytes_transport,
                    rx_bytes_app: (shard_length - SHARD_PREFIX_SIZE) as u32,
                };

                let shards_map = self.map_rx.entry(packet_index).or_insert(HashMap::new());

                if shards_map.contains_key(&shard_index) {
                    self.duplicated_shard_counter += 1;
                } else {
                    shards_map.insert(shard_index, packet);
                    self.rx_shard_counter += 1;
                }

                self.rx_bytes += shard_length as u32 + header_bytes_transport;

                // Jitter
                {
                    if let (Some(prev_shard_rx_instant), Some(prev_shard_tx_r_instant)) =
                        (self.prev_shard_rx_instant, self.prev_shard_tx_r_instant)
                    {
                        let transit_diff = (rx_instant
                            .duration_since(prev_shard_rx_instant)
                            .as_secs_f32())
                            - (tx_r_instant - prev_shard_tx_r_instant); // D(i-1,i), according to RFC 3550
                        self.interarrival_jitter +=
                            (transit_diff.abs() - self.interarrival_jitter) / 16.0;
                    }
                    self.prev_shard_tx_r_instant = Some(tx_r_instant);
                    self.prev_shard_rx_instant = Some(rx_instant);
                }
            } // if ID ==VIDEO END

            self.shard_recv_state.insert(RecvState {
                shard_length,
                stream_id,
                packet_index,
                shards_count,
                shard_index,
                packet_cursor: 0,
                overwritten_data_backup: None,
                should_discard: false,
                frame_first_shard_deadline: None,
            })
        };

        if shard_recv_state_mut.frame_first_shard_deadline.is_none() {
            shard_recv_state_mut.frame_first_shard_deadline = now.checked_add(DEADLINE_PACKETS_S);
        }

        let Some(components) = self
            .stream_recv_components
            .get_mut(&shard_recv_state_mut.stream_id)
        else {
            println!(
                "Received packet from stream {} before subscribing!",
                shard_recv_state_mut.stream_id
            );
            return try_again();
        };

        // print_prettyy!( DebugColor::Orange, "{:.9} [DBG Socket RX {}] F: {}, S:{:2.0}/{:2.0} |deadline_current: {:?}|in_progress_packets: {:?}| indices {:?}| " ,format_elapsed!(now),ip_client ,shard_recv_state_mut.packet_index, shard_recv_state_mut.shard_index, shard_recv_state_mut.shards_count - 1 ,format_elapsed!(shard_recv_state_mut.frame_first_shard_deadline.unwrap()), components.in_progress_packets.len(), components.in_progress_packets.keys());

        let in_progress_packet = if shard_recv_state_mut.should_discard {
            &mut components.discarded_shards_sink
        } else if let Some(packet) = components
            .in_progress_packets
            .get_mut(&shard_recv_state_mut.packet_index)
        {
            packet
        } else {
            // Try to get a buffer through three fallback mechanisms
            let buffer = components
                .used_buffer_receiver
                .try_recv()
                .ok()
                .or_else(|| {
                    println!("First fallback");
                    // First fallback: Try to recycle old packets
                    let recyclable = components
                        .in_progress_packets
                        .iter()
                        .find(|(&idx, _)| {
                            wrapping_cmp(idx, shard_recv_state_mut.packet_index.wrapping_sub(5))
                                == Ordering::Less
                        })
                        .map(|(&k, _)| k);

                    // println!(
                    //     "{:.9}[INSIDE1!]Buffer stats - Pool: {}, In-progress: {}",
                    //     context.scheduler.time().duration_since(TaiTime::EPOCH).as_secs_f32(),
                    //     components.used_buffer_receiver.len(),
                    //     components.in_progress_packets.len()
                    // );

                    recyclable.and_then(|idx| {
                        components
                            .in_progress_packets
                            .remove(&idx)
                            .map(|packet| packet.buffer)
                    })
                })
                .or_else(|| {
                    // Second fallback: If still no buffer, create a new emergency buffer
                    println!(
                        "Warning: Creating new emergency buffer - consider increasing buffer pool"
                    );
                    // println!(
                    //     "{:.9}[INSIDE2!]Buffer stats - Pool: {}, In-progress: {}",
                    //     context.scheduler.time().duration_since(TaiTime::EPOCH).as_secs_f32(),
                    //     components.used_buffer_receiver.len(),
                    //     components.in_progress_packets.len()
                    // );
                    Some(Vec::with_capacity(
                        self.max_packet_size * shard_recv_state_mut.shards_count,
                    ))
                })
                .unwrap(); // Now safe to unwrap as we always have a buffer

            components.in_progress_packets.insert(
                shard_recv_state_mut.packet_index,
                InProgressPacket {
                    buffer,
                    buffer_length: 0,
                    received_shard_indices: HashSet::with_capacity(
                        shard_recv_state_mut.shards_count,
                    ),
                    deadline: shard_recv_state_mut.frame_first_shard_deadline,
                    num_shards_expected: shard_recv_state_mut.shards_count,
                    id_frame: shard_recv_state_mut.packet_index,
                },
            );
            components
                .in_progress_packets
                .get_mut(&shard_recv_state_mut.packet_index)
                .unwrap()
        };

        let max_shard_data_size = self.max_packet_size - SHARD_PREFIX_SIZE;
        // Note: there is no prefix offset, since we want to write the prefix too.
        let packet_start_index = shard_recv_state_mut.shard_index * max_shard_data_size;

        // println!("packet_start_index: {}", packet_start_index);
        // Prepare buffer to accomodate receiving shard
        {
            // println!("ACCOMODATE BUFFER BL {}, other: {}", in_progress_packet.buffer_length, packet_start_index + shard_recv_state_mut.shard_length);
            // Note: this contains the prefix offset
            in_progress_packet.buffer_length = usize::max(
                in_progress_packet.buffer_length,
                packet_start_index + shard_recv_state_mut.shard_length,
            );

            if in_progress_packet.buffer.len() < in_progress_packet.buffer_length {
                in_progress_packet
                    .buffer
                    .resize(in_progress_packet.buffer_length, 0);
            }
        }

        let sub_buffer = &mut in_progress_packet.buffer[packet_start_index..];

        // Read shard into the single contiguous buffer
        {
            // Backup the small section of bytes that will be overwritten by reading from socket.
            if shard_recv_state_mut.overwritten_data_backup.is_none() {
                shard_recv_state_mut.overwritten_data_backup =
                    Some(sub_buffer[..SHARD_PREFIX_SIZE].try_into().unwrap())
            }

            // This loop may bail out at any time if a timeout is reached. This is correctly handled by
            // the previous code.
            while shard_recv_state_mut.packet_cursor < shard_recv_state_mut.shard_length {
                let size = arc_receiver
                    .lock()
                    .unwrap()
                    .recv(
                        &mut sub_buffer
                            [shard_recv_state_mut.packet_cursor..shard_recv_state_mut.shard_length],
                    )
                    .unwrap();
                shard_recv_state_mut.packet_cursor += size;
            }
            // Restore backed up bytes
            // Safety: overwritten_data_backup is always set just before receiving the packet
            sub_buffer[..SHARD_PREFIX_SIZE]
                .copy_from_slice(&shard_recv_state_mut.overwritten_data_backup.take().unwrap());
        }

        if !shard_recv_state_mut.should_discard {
            if !in_progress_packet
                .received_shard_indices
                .contains(&shard_recv_state_mut.shard_index)
            {
                in_progress_packet
                    .received_shard_indices
                    .insert(shard_recv_state_mut.shard_index);
            }
            // println!("NOT DISCARDING");
        }

        let mut frame_span = 0.0;
        let mut frame_interarrival: f32 = 0.0;

        let mut all_bytes_in_frame: u32 = 0;
        let mut all_bytes_in_frame_app: u32 = 0;

        // Check if packet is complete and send
        if in_progress_packet.received_shard_indices.len() == shard_recv_state_mut.shards_count {
            print_pretty!(
                DebugColor::DarkGreen,
                "(socketRX {} ) FRAME {} IS COMPLETE! ({} / {}) ",
                ip_client,
                in_progress_packet.id_frame,
                in_progress_packet.received_shard_indices.len(),
                shard_recv_state_mut.shards_count
            );
            if shard_recv_state_mut.stream_id == VIDEO {
                if let Some(inner_map) = self.map_rx.get(&shard_recv_state_mut.packet_index) {
                    // println!("Retrieved from innermap, got {}",shard_recv_state_mut.packet_index);

                    let values: Vec<&ShardMapStats> = inner_map.values().collect();
                    let min_time = values.iter().map(|shard| shard.rx_instant).min().unwrap();
                    let max_time = values.iter().map(|shard| shard.rx_instant).max().unwrap();

                    frame_span = max_time.duration_since(min_time).as_secs_f32();
                    frame_interarrival = max_time
                        .duration_since(self.prev_frame_rx_instant)
                        .as_secs_f32();

                    self.prev_frame_rx_instant = max_time;

                    all_bytes_in_frame = values.iter().map(|shard| shard.rx_bytes).sum();
                    all_bytes_in_frame_app = values.iter().map(|shard| shard.rx_bytes_app).sum();

                    // println!("SEND COMPLETE PACKET!");

                    // One way delay gradient
                    if let Some(first_shard_stats) = inner_map.get(&0) {
                        if let Some(prev_frame_tx_r_instant) = self.prev_frame_tx_r_instant {
                            self.kalman.ow_delay = frame_interarrival
                                - (first_shard_stats.tx_r_instant - prev_frame_tx_r_instant);
                        }
                        self.prev_frame_tx_r_instant = Some(first_shard_stats.tx_r_instant);

                        self.kalman.k_gain = (self.kalman.p_prev + Q_KALMAN)
                            / (self.kalman.p_prev + Q_KALMAN + self.kalman.noise_estimation);

                        self.kalman.m_current = (1.0 - self.kalman.k_gain) * self.kalman.m_prev
                            + self.kalman.k_gain * self.kalman.ow_delay;

                        self.kalman.residual_z = self.kalman.ow_delay - self.kalman.m_prev;

                        self.kalman.noise_estimation = (0.95 * self.kalman.noise_prev)
                            + self.kalman.residual_z.powf(2.0) * 0.05;

                        self.kalman.p_current =
                            (1.0 - self.kalman.k_gain) * (self.kalman.p_prev + Q_KALMAN);

                        self.kalman.p_prev = self.kalman.p_current;
                        self.kalman.m_prev = self.kalman.m_current;
                        self.kalman.noise_prev = self.kalman.noise_estimation;

                        self.kalman.measured_delay += self.kalman.m_current;
                    }
                }
            }
            let size = in_progress_packet.buffer_length;

            let reconstruct = ReconstructedPacket {
                index: shard_recv_state_mut.packet_index,
                buffer: components
                    .in_progress_packets
                    .remove(&shard_recv_state_mut.packet_index)
                    .unwrap()
                    .buffer,
                size,
                // print everything but the buffer
                frame_index: shard_recv_state_mut.packet_index,

                frame_span: frame_span,
                frame_interarrival: frame_interarrival,

                interarrival_jitter: self.interarrival_jitter,
                ow_delay: self.kalman.ow_delay,
                filtered_ow_delay: self.kalman.m_current,

                rx_bytes: self.rx_bytes,
                bytes_in_frame: all_bytes_in_frame,
                bytes_in_frame_app: all_bytes_in_frame_app,

                rx_shard_counter: self.rx_shard_counter,
                duplicated_shard_counter: self.duplicated_shard_counter,

                highest_rx_frame_index: self.highest_rx_frame_index,
                highest_rx_shard_index: self.highest_rx_shard_index,
                // tx_instant_first_shard: self.first_shard_instant_tx,
            };

            let empty_buffer = Vec::with_capacity(reconstruct.buffer.capacity());
            // println!("capacity of empty buffer! {}, len : {}", empty_buffer.capacity(), empty_buffer.len());

            // println!("{:?} Reconstructed packet!!", reconstruct);
            components.packet_queue.send(reconstruct).ok();

            // Immediately return the buffer to the pool
            components.used_buffer_sender.send(empty_buffer).ok();

            if shard_recv_state_mut.stream_id == VIDEO {
                self.rx_bytes = 0;
                self.rx_shard_counter = 0;
                self.duplicated_shard_counter = 0;

                // Keep only shards data from the latest 5 frames (using wrapping logic)
                let mut idxs_to_remove = Vec::new();
                for &idx in self.map_rx.keys() {
                    if wrapping_cmp(idx.wrapping_add(5), shard_recv_state_mut.packet_index)
                        == Ordering::Less
                    {
                        idxs_to_remove.push(idx);
                    }
                }
                for idx in idxs_to_remove {
                    self.map_rx.remove(&idx);
                }
            }
        } // if len == shard_count END
          // Initialize a counter to track the total loss
        let mut total_loss = 0;

        // Create a vector to store the keys of expired packets (to remove them later)
        let mut expired_keys = Vec::new();

        // Iterate through in_progress_packets to identify expired packets
        // In the deadline check section:
        for (id, shard) in components.in_progress_packets.iter_mut() {
            if let Some(deadline) = shard.deadline {
                if deadline <= now {
                    let expected_shards = shard.num_shards_expected;
                    let shards_arrived = shard.received_shard_indices.len();
                    let shards_lost = expected_shards - shards_arrived;

                    // Store stats
                    self.lost_shards_deadline_map
                        .insert(shard.id_frame, shards_lost);

                    // Mark for removal
                    expired_keys.push(*id);
                }
            }
        }

        // Remove expired packets and cleanup
        for key in expired_keys {
            if let Some(packet) = components.in_progress_packets.remove(&key) {
                // Return the buffer to the pool if possible
                if !packet.buffer.is_empty() {
                    let empty_buffer = Vec::with_capacity(packet.buffer.capacity());
                    components.used_buffer_sender.send(empty_buffer).ok();
                }
            }
        }

        if ALVR_ORIGINAL_SOCKETRX_BEHAVIOR{

             // Keep only shards with later packet index (using wrapping logic)
             while let Some((idx, inprog)) = components.in_progress_packets.iter().find(|(idx, _)| {
                wrapping_cmp(**idx, shard_recv_state_mut.packet_index) == Ordering::Less
            }) {

                let mut editprog = inprog.clone(); 
                let idx = *idx; // fix borrow rule
                let packet = components.in_progress_packets.remove(&idx).unwrap();
                
                let shards_lost = editprog.num_shards_expected - editprog.received_shard_indices.len(); 
                self.lost_shards_deadline_map
                .insert(idx, shards_lost);
                // Recycle buffer
                components.used_buffer_sender.send(packet.buffer).ok();
            }
        }


        // Mark current shard as read and allow for a new shard to be read
        self.shard_recv_state = None;

        Ok(())
    }
}

pub enum StreamSocketBuilder {
    // Tcp(TcpListener),
    // Udp(UdpSocket),
    Channel(Sender<Vec<u8>>, BufferedReceiver<Vec<u8>>),
}

#[allow(unused)]
impl StreamSocketBuilder {
    pub fn build(self, max_packet_size: usize) -> StreamSocket {
        match self {
            StreamSocketBuilder::Channel(sender, receiver) => {
                StreamSocket {
                    max_packet_size,
                    send_socket: Arc::new(Mutex::new(Box::new(sender))),
                    receive_socket: Arc::new(Mutex::new(Box::new(receiver))),
                    // Initialize remaining fields as needed
                    shard_recv_state: None,
                    stream_recv_components: HashMap::new(),
                    transport_protocol: SocketProtocol::Channel,
                    // Initialize other fields based on your requirements
                    map_rx: HashMap::new(),
                    rx_bytes: 0,
                    prev_shard_tx_r_instant: None,
                    prev_shard_rx_instant: None,
                    interarrival_jitter: 0.0,
                    kalman: KalmanFilter::default(),
                    prev_frame_rx_instant: TaiTime::EPOCH,
                    prev_frame_tx_r_instant: None,
                    rx_shard_counter: 0,
                    duplicated_shard_counter: 0,
                    highest_rx_shard_index: -1,
                    highest_rx_frame_index: -1,
                    lost_shards_deadline_map: HashMap::new(),
                }
            }
        }
    }
    #[allow(unused)]
    pub fn listen_for_server(
        timeout: Duration,
        port: u16,
        stream_socket_config: SocketProtocol,
        stream_tos_config: Option<DscpTos>,
        send_buffer_bytes: SocketBufferSize,
        recv_buffer_bytes: SocketBufferSize,
    ) -> Result<Self> {
        Ok(match stream_socket_config {
            // SocketProtocol::Udp => StreamSocketBuilder::Udp(udp::bind(
            //     port,
            //     stream_tos_config,
            //     send_buffer_bytes,
            //     recv_buffer_bytes,
            // )?),
            // SocketProtocol::Tcp => StreamSocketBuilder::Tcp(tcp::bind(
            //     timeout,
            //     port,
            //     stream_tos_config,
            //     send_buffer_bytes,
            //     recv_buffer_bytes,
            // )?),
            SocketProtocol::Channel => {
                let (sender, receiver) = buffered_channel();
                StreamSocketBuilder::Channel(sender, receiver)
            }
        })
    }

    pub fn accept_from_server(
        self,
        server_ip: IpAddr,
        port: u16,
        max_packet_size: usize,
        timeout: Duration,
    ) -> ConResult<StreamSocket> {
        let protocol: SocketProtocol;
        let (send_socket, receive_socket): (Box<dyn SocketWriter>, Box<dyn SocketReader>) =
            match self {
                // StreamSocketBuilder::Udp(socket) => {
                //     let (send_socket, receive_socket) =
                //         udp::connect(&socket, server_ip, port, timeout).to_con()?;
                //     protocol = SocketProtocol::Udp;

                //     (Box::new(send_socket), Box::new(receive_socket))
                // }
                // StreamSocketBuilder::Tcp(listener) => {
                //     let (send_socket, receive_socket) =
                //         tcp::accept_from_server(&listener, Some(server_ip), timeout)?;
                //     protocol = SocketProtocol::Tcp;

                //     (Box::new(send_socket), Box::new(receive_socket))
                // }
                StreamSocketBuilder::Channel(sender, receiver) => {
                    protocol = SocketProtocol::Channel;
                    (Box::new(sender), Box::new(receiver)) // TODO: SIMULATE "UDP/TCP" here in some way
                }
            };

        Ok(StreamSocket {
            // +4 is a workaround to retain compatibilty with old protocol
            // todo: remove +4
            max_packet_size: max_packet_size + 4,
            send_socket: Arc::new(Mutex::new(send_socket)),
            receive_socket: Arc::new(Mutex::new(receive_socket)),
            shard_recv_state: None,
            stream_recv_components: HashMap::new(),

            transport_protocol: protocol,
            map_rx: HashMap::new(),
            rx_bytes: 0,

            prev_shard_tx_r_instant: None,
            prev_shard_rx_instant: None,

            interarrival_jitter: 0.,

            kalman: KalmanFilter::default(),
            prev_frame_rx_instant: TaiTime::EPOCH,
            prev_frame_tx_r_instant: None,

            rx_shard_counter: 0,
            duplicated_shard_counter: 0,

            highest_rx_frame_index: -1,
            highest_rx_shard_index: -1,
            lost_shards_deadline_map: HashMap::new(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn connect_to_client(
        timeout: Duration,
        client_ip: IpAddr,
        port: u16,
        protocol: SocketProtocol,
        dscp: Option<DscpTos>,
        send_buffer_bytes: SocketBufferSize,
        recv_buffer_bytes: SocketBufferSize,
        max_packet_size: usize,
    ) -> ConResult<StreamSocket> {
        let (send_socket, receive_socket): (Box<dyn SocketWriter>, Box<dyn SocketReader>) =
            match protocol {
                // SocketProtocol::Udp => {
                //     let socket =
                //         udp::bind(port, dscp, send_buffer_bytes, recv_buffer_bytes).to_con()?;
                //     let (send_socket, receive_socket) =
                //         udp::connect(&socket, client_ip, port, timeout).to_con()?;

                //     (Box::new(send_socket), Box::new(receive_socket))
                // }
                // SocketProtocol::Tcp => {
                //     let (send_socket, receive_socket) = tcp::connect_to_client(
                //         timeout,
                //         &[client_ip],
                //         port,
                //         send_buffer_bytes,
                //         recv_buffer_bytes,
                //     )?;

                //     (Box::new(send_socket), Box::new(receive_socket))
                // }
                SocketProtocol::Channel => {
                    let (sender, receiver) = buffered_channel();
                    (Box::new(sender), Box::new(receiver))
                }
            };

        Ok(StreamSocket {
            // +4 is a workaround to retain compatibilty with old protocol
            // todo: remove +4
            max_packet_size: max_packet_size + 4,
            send_socket: Arc::new(Mutex::new(send_socket)),
            receive_socket: Arc::new(Mutex::new(receive_socket)),
            shard_recv_state: None,
            stream_recv_components: HashMap::new(),

            transport_protocol: protocol,

            map_rx: HashMap::new(),
            rx_bytes: 0,

            prev_shard_tx_r_instant: None,
            prev_shard_rx_instant: None,

            interarrival_jitter: 0.,

            kalman: KalmanFilter::default(),
            prev_frame_rx_instant: TaiTime::EPOCH,
            prev_frame_tx_r_instant: None,

            rx_shard_counter: 0,
            duplicated_shard_counter: 0,

            highest_rx_frame_index: -1,
            highest_rx_shard_index: -1,
            lost_shards_deadline_map: HashMap::new(),
        })
    }

    pub fn connect_to_client_mod(
        handshake_timeout: Duration,
        client_ip: IpAddr,
        stream_port: u16,
        protocol: SocketProtocol,
        dscp: Option<DscpTos>,
        send_buffer: SocketBufferSize,
        recv_buffer: SocketBufferSize,
        packet_size: usize,
    ) -> Result<StreamSocket> {
        let (sender, receiver) = buffered_channel();

        Ok(StreamSocketBuilder::Channel(sender, receiver).build(packet_size))
    }

    pub fn accept_from_server_mod(
        server_ip: IpAddr,
        port: u16,
        packet_size: usize,
    ) -> Result<StreamSocket> {
        // let (send_socket, receive_socket): (Box<dyn SocketWriter>, Box<dyn SocketReader>) = match self {
        //     StreamSocketBuilder::Channel(sender, receiver) => {
        //         let protocol = SocketProtocol::Channel;
        //         (Box::new(sender), Box::new(receiver))
        //     }
        // };

        let (sender, receiver) = buffered_channel();

        Ok(StreamSocketBuilder::Channel(sender, receiver).build(packet_size))
    }
}

/// Get next packet reconstructing from shards.
/// Returns true if a packet has been recontructed and copied into the buffer.
///
#[allow(unused)]
impl<H: DeserializeOwned + Serialize> StreamReceiver<H> {
    pub fn recv(&mut self, timeout: Duration) -> ConResult<ReceiverData<H>> {
        // println!("receiving FULL packet from shards!!!");
        // let packet = self
        //     .packet_receiver
        //     .recv_timeout(timeout)
        //     .handle_try_again()?;

        let packet = self.packet_receiver.try_recv().handle_try_again()?;
        // print_pretty!(DebugColor::DarkOrange, "[DBG StreamReceiver] Reconstructed frame {}, buffer: L = header+data:{}, data {},\nData = {:?}", packet.frame_index, packet.buffer.len() ,packet.buffer.len() - SHARD_PREFIX_SIZE - 13,&packet.buffer[(SHARD_PREFIX_SIZE + 13)..( 200 + SHARD_PREFIX_SIZE ) ] );

        self.frame_interarrival += packet.frame_interarrival;

        self.rx_bytes += packet.rx_bytes;

        self.rx_shard_counter += packet.rx_shard_counter;

        self.duplicated_shard_counter += packet.duplicated_shard_counter;

        let mut had_packet_loss = false;
        let mut frames_skipped: u32 = 0;

        if let Some(last_idx) = self.last_packet_index {
            // Use wrapping arithmetics
            match wrapping_cmp(packet.index, last_idx.wrapping_add(1)) {
                Ordering::Equal => (),
                Ordering::Greater => {
                    // Skipped some indices
                    frames_skipped = packet.index - last_idx.wrapping_add(1);
                    had_packet_loss = true
                }
                Ordering::Less => {
                    // Old packet, discard
                    self.used_buffer_queue.send(packet.buffer).to_con()?;
                    return try_again();
                }
            }
        }

        let interarrival = self.frame_interarrival;
        let rx_bytes_val = self.rx_bytes;
        let rx_counter = self.rx_shard_counter;
        let duplicated_counter = self.duplicated_shard_counter;

        self.frame_interarrival = 0.0;
        self.rx_bytes = 0;
        self.rx_shard_counter = 0;
        self.duplicated_shard_counter = 0;

        self.last_packet_index = Some(packet.index);

        Ok(ReceiverData {
            buffer: Some(packet.buffer),
            size: packet.size,
            used_buffer_queue: self.used_buffer_queue.clone(),
            had_packet_loss,
            _phantom: PhantomData,

            frame_index: packet.frame_index,

            frame_span: packet.frame_span,
            frame_interarrival: interarrival,

            interarrival_jitter: packet.interarrival_jitter,
            ow_delay: packet.ow_delay,
            filtered_ow_delay: packet.filtered_ow_delay,

            rx_bytes: rx_bytes_val,
            bytes_in_frame: packet.bytes_in_frame,
            bytes_in_frame_app: packet.bytes_in_frame_app,

            frames_skipped: frames_skipped,

            rx_shard_counter: rx_counter,
            duplicated_shard_counter: duplicated_counter,

            highest_rx_frame_index: packet.highest_rx_frame_index,
            highest_rx_shard_index: packet.highest_rx_shard_index,
        })
    }
}

pub fn parse_shard_data(data: &[u8]) -> Result<(u32, u16, u32, u32, u32, f32), &'static str> {
    if data.len() < 22 {
        return Err("Received data is too short to contain a complete shard prefix");
    }

    let packet_length: u32 = u32::from_be_bytes(data[0..4].try_into().unwrap()) + 4;
    let stream_id = u16::from_be_bytes(data[4..6].try_into().unwrap());
    let next_packet_index = u32::from_be_bytes(data[6..10].try_into().unwrap());
    let shards_count = u32::from_be_bytes(data[10..14].try_into().unwrap()) as u32;
    let shard_index = u32::from_be_bytes(data[14..18].try_into().unwrap()) as u32;
    let tx_r_instant = f32::from_be_bytes(data[18..22].try_into().unwrap());

    Ok((
        packet_length,
        stream_id,
        next_packet_index,
        shards_count,
        shard_index,
        tx_r_instant,
    ))
}



#[derive(Clone)]
pub struct StreamSender<H> {
    inner: Arc<Mutex<Box<dyn SocketWriter>>>,
    pub app_network_interface: Arc<Mutex<Box<dyn SocketReader>>>,

    stream_id: u16,
    max_packet_size: usize,

    pub next_packet_index: u32,
    used_buffers: Vec<Vec<u8>>,
    _phantom: PhantomData<H>,

    shards_count: usize,
    ref_time: TaiTime<0>,
    frame_tracker: FrameTracker,
    // encoder_hevc: Option<Arc<tokMutex<HevcEncoder>>>,

    // encoder_wrapper: Option<Arc<tokMutex<EncoderWrapper>>>,
    // chunk_frames: VecDeque<Vec<u8>>,

    // encoder_wrapper: Option<Arc<tokMutex<HevcEncoder>>>,
    pub ffmpeg_encoder: Option<Arc<async_std::sync::Mutex<ChunkedHevcEncoder>>>,
    // pub ffmpeg_maxbitrate_encoder: Option<Arc<async_std::sync::Mutex<ChunkedHevcEncoder>>>,

    // Keep the initialization flag:
    pub time_since_last_update: TaiTime<0>,

    csv_trace: CsvTrace, 
}

#[allow(unused)]
impl<H> StreamSender<H> {
    pub fn get_shards_count(&self) -> usize {
        self.shards_count
    }
    pub fn get_last_packet_id(&self) -> u32 {
        self.next_packet_index - 1
    }

    pub fn get_frame_tracker_map(&self) -> HashMap<u32, TaiTime<0>> {
        self.frame_tracker.map.clone()
    }

    /// Shard and send a buffer with zero copies and zero allocations.
    /// The prefix of each shard is written over the previously sent shard to avoid reallocations.
    pub fn send(&mut self, mut buffer: Buffer<H>, now: TaiTime<0>) -> Result<()> {
        let max_shard_data_size = self.max_packet_size - SHARD_PREFIX_SIZE;
        let actual_buffer_size = buffer.hidden_offset + buffer.length;
        let data_size = actual_buffer_size - SHARD_PREFIX_SIZE;
        let shards_count = (data_size as f32 / max_shard_data_size as f32).ceil() as usize;

        for idx in 0..shards_count {
            // this overlaps with the previous shard, this is intended behavior and allows to
            // reduce allocations

            // println!("sending shard {}", idx);
            let packet_start_position = idx * max_shard_data_size;
            let sub_buffer = &mut buffer.inner[packet_start_position..];

            // NB: true shard length (account for last shard that is smaller)
            let packet_length = usize::min(
                self.max_packet_size,
                actual_buffer_size - packet_start_position,
            );

            // let tx_r_instant: f32 = Instant::now().duration_since(self.ref_time).as_secs_f32();

            let tx_r_instant = now.duration_since(self.ref_time).as_secs_f32();

            // todo: switch to little endian
            // todo: do not remove sizeof<u32> for packet length
            sub_buffer[0..4]
                .copy_from_slice(&((packet_length - mem::size_of::<u32>()) as u32).to_be_bytes());
            sub_buffer[4..6].copy_from_slice(&self.stream_id.to_be_bytes());
            sub_buffer[6..10].copy_from_slice(&self.next_packet_index.to_be_bytes());
            sub_buffer[10..14].copy_from_slice(&(shards_count as u32).to_be_bytes());
            sub_buffer[14..18].copy_from_slice(&(idx as u32).to_be_bytes());
            sub_buffer[18..22].copy_from_slice(&tx_r_instant.to_be_bytes());

            // println!("sending data: \n{:?}", &sub_buffer[..100]);

            self.inner
                .lock()
                .unwrap()
                .send(&sub_buffer[..packet_length])?;

            // println!("Let's see the output of the channel after sending: *" );
            if idx == 0 {
                //store next_packet_index - Instant value pair for RTT
                self.frame_tracker.insert(self.next_packet_index, now);
            }
        }
        self.shards_count = shards_count;
        self.used_buffers.push(buffer.inner);

        Ok(())
    }
}

impl<H: Serialize> StreamSender<H> {

    pub async fn get_buffer_emu(
        &mut self,
        header: &H,
        current_bitrate_mbps: f32,
        now: TaiTime<0>,
        ip: IpAddr,
        id_frame: usize,
        name_folder: &str,
        // max_bitrate_ladder_mbps: f32,
        network_effects: &[NetworkPattern], 
        final_file: &str, 
        framerate: f32, 
    ) -> Result<Buffer<H>> {
        let id_frame_files_ref = id_frame + 1;



        let input_path = &format!(
            "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/{final_file}.mp4"
        );

        let mut buffer: Vec<u8> = Vec::new();
        print_pretty!(
            DebugColor::Blue,
            "[BUFFEREMU] SENDING FRAME {} from SERVER",
            id_frame_files_ref
        );



        if USE_FFMPEG {
            if self.ffmpeg_encoder.is_none() {
                // Create a new ChunkedHevcEncoder
                let bitrate_cmd = format!("{:.0}M", current_bitrate_mbps);

                // print_prettyy!(
                //     DebugColor::Yellow,
                //     "FRAME {} MAXENCODER EXISTS: {}",
                //     id_frame_files_ref,
                //     self.ffmpeg_maxbitrate_encoder.is_some()
                // );

                // let mut random_offset = rand::thread_rng().gen_range(3.0..OFFSET_VIDEO);
                let random_offset = OFFSET_VIDEO;

                let third_octet = get_third_octet(ip).unwrap(); 

                if self.csv_trace.path.as_os_str().is_empty() {
                    // one CSV per run – put it next to the hevc files, but anywhere is fine
                    let csv_path = format!(
                        "/home/boris/Desktop/Rust_MG1/asynchronix/Results/{}/trace_offline_video{}.csv",
                        name_folder,
                        third_octet,
                    );


                    let csv_path_emu = format!(
                        "/home/boris/Desktop/Rust_MG1/asynchronix/Results/{}/trace_emu_effects{}.csv",
                        name_folder,
                        third_octet,
                    );

                    print_green!("Creating OFFLINE CSV at: {csv_path}", ); 

                    let mut wtr = Writer::from_path(&csv_path)?;
                    let mut wtr2 = Writer::from_path(&csv_path_emu)?;                     

                    wtr.write_record(&[
                        "OFFSET_VIDEO",
                        "PATH_VIDEO",
                        "IDR_FREQUENCY", 
                        "timestamp",
                        "ID_frame",
                        "Lost", 
                        "Throughput(avg)",
                    ])?; 

                    // wtr2.write_record(&["EMU_EFFECTS", ] )?; 
                    wtr2.write_record(NetworkPattern::csv_headers())?;

                    for emu in network_effects {

                        wtr2.write_record(emu.to_csv_row())?;
                        // wtr2.write_record(&[ format!("{:#?}", emu) ] )?; 

                    }

                    wtr.write_record(&[
                        format!("{random_offset:.4}"),     // offset used for this run
                        input_path.to_owned(),             // source clip
                        format!("{}", IDR_FRAME_SIZE_GOP), 
                        // json, 
                        "".to_string(),                                // placeholder timestamp
                        "".to_string(),                                // placeholder id_f
                        "".to_string(),                                // placeholder lost
                        "".to_string(), 
                    ])?;


                    
                   

                    wtr.flush()?;
                    wtr2.flush()?; 
                    self.csv_trace.path = csv_path.into();
                }
    

                let encoder: ChunkedHevcEncoder = ChunkedHevcEncoder::new(
                    input_path,
                    WIDTH_ENCODER as u32,
                    HEIGHT_ENCODER as u32,
                    &bitrate_cmd,
                    CHUNK_DURATION_F64_S, // Chunk duration in seconds
                    format!("[ENCODER {}]", ip),
                    random_offset,
                    framerate 
                );

                // Wrap the encoder in an Arc<Mutex<_>>
                let encoder_arc = Arc::new(async_std::sync::Mutex::new(encoder));

                // let maxencoder_arc: Arc<async_std::sync::Mutex<ChunkedHevcEncoder>> = Arc::new(async_std::sync::Mutex::new(max_encoder));

                // Initialize the encoder BEFORE storing it
                // {
                //     print_prettyy!(DebugColor::Coral, "Initializing MAXENCODER",);
                //     let mut maxencoder = maxencoder_arc.lock().await;
                //     maxencoder.start_chunking(max_bitrate_ladder_mbps).await;
                // }
                {
                    let mut encoder: async_std::sync::MutexGuard<'_, ChunkedHevcEncoder> =
                        encoder_arc.lock().await;
                    encoder.start_chunking(current_bitrate_mbps).await;
                } // Lock is dropped here

                // Store the initialized encoder
                self.ffmpeg_encoder = Some(encoder_arc);
                // self.ffmpeg_maxbitrate_encoder = Some(maxencoder_arc);
                self.time_since_last_update = now;
            }
            // Now that the encoder is initialized and the lock released, get a frame
            if let Some(encoder_arc) = self.ffmpeg_encoder.as_ref() {
                let mut encoder = encoder_arc.lock().await;

                match encoder.next_frame().await {
                    Some(frame) => {
                        buffer = frame;

                        // let hevc_file_path: String = format!(
                        //     "/home/boris/Desktop/Rust_MG1/asynchronix/Sink_for_video/{}/{}/hevc_ref",
                        //     name_folder, ip
                        // );

                        // std::fs::create_dir_all(hevc_file_path).unwrap();
                        // let filename = format!("/home/boris/Desktop/Rust_MG1/asynchronix/Sink_for_video/{}/{}/hevc_ref/{}.hevc",name_folder,ip, id_frame_files_ref);
                        // print_pretty!(
                        //     DebugColor::ForestGreen,
                        //     "[Encoder XRServer] REF FRAME {} SAVED TO MEMORY",
                        //     id_frame_files_ref
                        // );
                        // let mut file = std::fs::File::create(filename).unwrap();
                        // file.write_all(&buffer).unwrap();
                    }
                    None => {
                        print_pretty!(
                            DebugColor::SaddleBrown,
                            "No frame available, restarting encoder",
                        );

                        // Clear ALL buffers before restart
                        encoder.parser.buffer.clear();
                        encoder.frame_queue.clear();

                        // Restart chunking
                        encoder.start_chunking(current_bitrate_mbps).await;

                        // Wait for encoder to produce frames
                        // task::sleep(Duration::from_millis(100)).await;

                        // Try again after waiting
                        match encoder.next_frame().await {
                            Some(frame) => {
                                // let hevc_file_path = format!("/home/boris/Desktop/Rust_MG1/asynchronix/Sink_for_video/{}/{}/hevc_ref",name_folder ,ip);

                                // std::fs::create_dir_all(hevc_file_path).unwrap();
                                // let mut file = std::fs::File::create(format!("/home/boris/Desktop/Rust_MG1/asynchronix/Sink_for_video/{}/{}/hevc_ref/{}.hevc", name_folder ,ip, id_frame_files_ref)).unwrap();
                                // print_pretty!(
                                //     DebugColor::Peach,
                                //     "^^^^^^^^^^^^^^^^^[DBG ENCODER REF SAVE] Storing {}, len: {}",
                                //     id_frame_files_ref,
                                //     buffer.len()
                                // );

                                // file.write_all(&frame.clone()).unwrap();

                                buffer = frame
                            }
                            None => {
                                print_pretty!(
                                    DebugColor::Red,
                                    "Still no frame after restart, using empty buffer",
                                );
                                buffer = Vec::new();
                            }
                        }
                    }
                };
            };
        } else {
            // Fallback for non-FFMPEG mode
            buffer = generate_fibonacci_video_payload(current_bitrate_mbps);
        }

        // Rest of your function remains the same
        let header_size = bincode::serialized_size(header)? as usize;
        let hidden_offset = SHARD_PREFIX_SIZE + header_size;

        if buffer.len() < hidden_offset {
            buffer.resize(hidden_offset, 0);
        }

        let buffer_len = buffer.len();

        self.next_packet_index += 1;

        Ok(Buffer {
            inner: buffer,
            hidden_offset,
            length: buffer_len,
            _phantom: PhantomData,
        })
    }

    pub fn get_buffer_tracking(&mut self, header: &H, _now: TaiTime<0>) -> Result<Buffer<H>> {
        let mut buffer = vec![0; 1000];
        let header_size = bincode::serialized_size(header)? as usize;
        let hidden_offset = SHARD_PREFIX_SIZE + header_size;

        if buffer.len() < hidden_offset {
            buffer.resize(hidden_offset, 0);
        }

        bincode::serialize_into(&mut buffer[SHARD_PREFIX_SIZE..hidden_offset], header)?;

        Ok(Buffer {
            inner: buffer,
            hidden_offset,
            length: 0,
            _phantom: PhantomData,
        })
    }
    pub fn send_header_tracking(&mut self, header: &H, now: TaiTime<0>) -> Result<()> {
        let buffer = self.get_buffer_tracking(header, now).unwrap();
        self.send(buffer, now)
    }
}
pub trait HandleTryAgain<T> {
    fn handle_try_again(self) -> ConResult<T>;
}

impl<T> HandleTryAgain<T> for io::Result<T> {
    fn handle_try_again(self) -> ConResult<T> {
        self.map_err(|e| {
            if e.kind() == io::ErrorKind::TimedOut || e.kind() == io::ErrorKind::WouldBlock {
                ConnectionError::TryAgain(e.into())
            } else {
                ConnectionError::Other(e.into())
            }
        })
    }
}

impl<T> HandleTryAgain<T> for std::result::Result<T, RecvTimeoutError> {
    fn handle_try_again(self) -> ConResult<T> {
        self.map_err(|e| match e {
            RecvTimeoutError::Timeout => ConnectionError::TryAgain(e.into()),
            RecvTimeoutError::Disconnected => ConnectionError::Other(e.into()),
        })
    }
}

impl<T> HandleTryAgain<T> for std::result::Result<T, TryRecvError> {
    fn handle_try_again(self) -> ConResult<T> {
        self.map_err(|e| match e {
            TryRecvError::Empty => ConnectionError::TryAgain(e.into()),
            TryRecvError::Disconnected => ConnectionError::Other(e.into()),
        })
    }
}

impl<T> ToCon<T> for Option<T> {
    fn to_con(self) -> ConResult<T> {
        self.ok_or_else(|| ConnectionError::Other(anyhow!("Unexpected None")))
    }
}

pub trait ToCon<T> {
    /// Convert result to ConResult. The error is always mapped to `Other()`
    fn to_con(self) -> ConResult<T>;
}

impl<T, E: Error + Send + Sync + 'static> ToCon<T> for Result<T, E> {
    fn to_con(self) -> ConResult<T> {
        self.map_err(|e| ConnectionError::Other(e.into()))
    }
}

fn wrapping_cmp(lhs: u32, rhs: u32) -> Ordering {
    let diff = lhs.wrapping_sub(rhs);
    if diff == 0 {
        Ordering::Equal
    } else if diff < u32::MAX / 2 {
        Ordering::Greater
    } else {
        // if diff > u32::MAX / 2, it means the sub operation wrapped
        Ordering::Less
    }
}
pub struct ReceiverDataStats {
    frame_index: u32,
    frame_span: f32,
    frame_interarrival: f32,
    interarrival_jitter: f32,
    ow_delay: f32,
    filtered_ow_delay: f32,

    had_packet_loss: bool,

    rx_bytes: u32,
    bytes_in_frame: u32,
    bytes_in_frame_app: u32,
    frames_skipped: u32,
    rx_shard_counter: u32,
    duplicated_shard_counter: u32,

    highest_rx_frame_index: i32,
    highest_rx_shard_index: i32,
}
#[allow(unused)]
impl ReceiverDataStats {
    pub fn had_packet_loss(&self) -> bool {
        self.had_packet_loss
    }
    pub fn get_frame_index(&self) -> u32 {
        self.frame_index
    }
    pub fn get_frame_span(&self) -> f32 {
        self.frame_span
    }
    pub fn get_frame_interarrival(&self) -> f32 {
        self.frame_interarrival
    }
    pub fn get_interarrival_jitter(&self) -> f32 {
        self.interarrival_jitter
    }
    pub fn get_ow_delay(&self) -> f32 {
        self.ow_delay
    }
    pub fn get_filtered_ow_delay(&self) -> f32 {
        self.filtered_ow_delay
    }
    pub fn get_rx_bytes(&self) -> u32 {
        self.rx_bytes
    }
    pub fn get_bytes_in_frame(&self) -> u32 {
        self.bytes_in_frame
    }
    pub fn get_bytes_in_frame_app(&self) -> u32 {
        self.bytes_in_frame_app
    }
    pub fn get_frames_skipped(&self) -> u32 {
        self.frames_skipped
    }
    pub fn get_rx_shard_counter(&self) -> u32 {
        self.rx_shard_counter
    }
    pub fn get_duplicated_shard_counter(&self) -> u32 {
        self.duplicated_shard_counter
    }
    pub fn get_highest_rx_frame_index(&self) -> i32 {
        self.highest_rx_frame_index
    }
    pub fn get_highest_rx_shard_index(&self) -> i32 {
        self.highest_rx_shard_index
    }
}

pub fn generate_fibonacci_video_payload(current_bitrate_mbps: f32) -> Vec<u8> {
    // Calculate the payload size based on bitrate
    let no_bytes_based_bitrate = (1416.97 * current_bitrate_mbps + -810.06) as usize;

    // Initialize a vector to hold the Fibonacci sequence
    let mut buffer_inner = Vec::with_capacity(no_bytes_based_bitrate);

    // Generate the Fibonacci sequence
    let mut a: u8 = 0;
    let mut b: u8 = 1;

    for _ in 0..no_bytes_based_bitrate {
        buffer_inner.push(a); // Add the current value to the payload
        let next = a.wrapping_add(b); // Use wrapping_add to prevent overflow
        a = b;
        b = next;
    }
    let miin: usize = usize::min(buffer_inner.len(), 50);
    print_pretty!(
        DebugColor::Salmon,
        "Encoded frame size: {} bytes ({} KB)\nData = {:?}",
        buffer_inner.len(),
        buffer_inner.len() / 1024,
        &buffer_inner[..miin]
    );

    buffer_inner
}
