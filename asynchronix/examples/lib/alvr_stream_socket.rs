use asynchronix::model::Context;
use crossbeam::channel::{unbounded, Receiver, RecvTimeoutError, Sender, TryRecvError};
#[allow(unused_imports)]
#[allow(dead_code)]

use crate::lib::DEBUG_PRINT_ENABLED; 

use rand::Rng;
use std::cell::RefCell;
use std::fmt;
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    io,
    marker::PhantomData,
    mem,
    // net::{TcpListener, UdpSocket},
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::debug_print;
use crate::lib::models_XR::{
    XRDevice,
    XRServer, // ,XRClient
};

use crate::lib::models_XR::SHARD_PREFIX_SIZE;
use crate::lib::DebugColor;
use anyhow::{anyhow, Result};
use glam::{Quat, Vec3};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::error::Error;
// use std::io::{Read, Write};
use std::net::IpAddr;

use std::result::Result::Ok;
use tai_time::TaiTime;

// pub const UPDATE_BITRATE_INTERVAL: Duration = Duration::from_secs(1);
pub const MAX_HISTORY_SIZE: usize = 256;
pub const INITIAL_FRAMERATE_FPS: f32 = 90.0;

pub const MAX_PACKET_SIZE_RECV: usize = 2000;
pub const TRACKING: u16 = 0;
pub const HAPTICS: u16 = 1;
pub const AUDIO: u16 = 2;
pub const VIDEO: u16 = 3;
pub const STATISTICS: u16 = 4;

pub const CONTROL_STREAM:u16  = 5; 

pub const SERVER_DISCONNECTED_MESSAGE: &str = "The streamer has disconnected.";

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
// impl SocketReader for Receiver<Vec<u8>> {
//     fn recv(&mut self, buffer: &mut [u8]) -> ConResult<usize> {
//         match self.try_recv() {
//             Ok(data) => {
//                 let data_len = data.len();
//                 if data_len <= buffer.len() {
//                     buffer[..data_len].copy_from_slice(&data);
//                     Ok(data_len)
//                 } else {
//                     Err(ConnectionError::Other(anyhow!("Buffer too small")))
//                 }
//             }
//             Err(TryRecvError::Empty) => Ok(0),
//             Err(TryRecvError::Disconnected) => {
//                 Err(ConnectionError::Other(anyhow!("Channel disconnected")))
//             }
//         }
//     }

//     fn peek(&self, _buffer: &mut [u8]) -> ConResult<usize> {
//         Err(ConnectionError::Other(anyhow!("Unsupported operation")))
//     }
// }

// Wrapper struct to hold the receiver and its buffer

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

// impl From<mpsc::TryRecvError> for ConError {
//     fn from(err: mpsc::TryRecvError) -> Self {
//         match err {
//             mpsc::TryRecvError::Empty => ConError::WouldBlock,
//             mpsc::TryRecvError::Disconnected => ConError::Disconnected,
//         }
//     }
// }

// impl SocketReader for mpsc::Receiver<Vec<u8>> {
//     fn recv(&mut self, buffer: &mut [u8]) -> ConResult<usize> {
//         match self.try_recv() {
//             Ok(data) => {
//                 let data_len = data.len();
//                 if data_len <= buffer.len() {
//                     buffer[..data_len].copy_from_slice(&data);
//                     Ok(data_len)
//                 } else {
//                     Err(ConnectionError::Other(anyhow!("Buffer too small")))
//                 }
//             }
//             Err(mpsc::TryRecvError::Empty) => try_again(),
//             Err(mpsc::TryRecvError::Disconnected) => {
//                 Err(ConnectionError::Other(anyhow!("Channel disconnected")))
//             }
//         }
//     }

//     fn peek(&self, _buffer: &mut [u8]) -> ConResult<usize> {
//         Err(ConnectionError::Other(anyhow!("Unsupported operation")))
//     }
// }
#[derive(Clone)]
pub struct InProgressPacket {
    buffer: Vec<u8>,
    buffer_length: usize,
    received_shard_indices: HashSet<usize>,
}
pub struct VideoPacket {
    pub header: VideoPacketHeader,
    pub payload: Vec<u8>,
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

#[derive(Serialize, Deserialize, Clone, Copy, Default, Debug)]
pub struct Pose {
    pub orientation: Quat, // NB: default Quat is identity
    pub position: Vec3,
}

#[derive(Serialize, Deserialize, Clone, Copy, Default, Debug)]
pub struct DeviceMotion {
    pub pose: Pose,
    pub linear_velocity: Vec3,
    pub angular_velocity: Vec3,
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

pub struct ShardPacket {
    shard_id: i32,
    frame_id: i32,
    length_shard_bits: usize,
}

/// Memory buffer that contains a hidden prefix
#[derive(Default, Debug)]
pub struct Buffer<H = ()> {
    pub inner: Vec<u8>,
    pub hidden_offset: usize, // this corresponds to prefix + header
    pub length: usize,
    pub _phantom: PhantomData<H>,
}

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
            max_size: 256,
        }
    }
    pub fn insert(&mut self, frame_id: u32, instant: TaiTime<0>) {
        self.map.insert(frame_id, instant);
        self.queue.push_back(frame_id);

        // Drop oldest pairs if size exceeds max_size
        while self.queue.len() > self.max_size {
            if let Some(oldest_frame_id) = self.queue.pop_front() {
                self.map.remove(&oldest_frame_id);
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
pub enum ConnectionError {
    TryAgain(anyhow::Error),
    Other(anyhow::Error),
}
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
}

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
}

impl<H: DeserializeOwned> ReceiverData<H> {
    pub fn get(&self) -> Result<(H, &[u8])> {
        let mut data: &[u8] = &self.buffer.as_ref().unwrap()[SHARD_PREFIX_SIZE..self.size];
        // This will partially consume the slice, leaving only the actual payload
        let header = bincode::deserialize_from(&mut data)?;

        Ok((header, data))
    }
    pub fn get_header(&self) -> Result<H> {
        Ok(self.get()?.0)
    }
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
}

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

        // Initialize the used buffers
        for _ in 0..max_concurrent_buffers {
            used_buffer_sender.send(vec![]).ok(); // Ignoring the result as in the original code
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

    pub fn recv<T: XRDevice + asynchronix::model::Model>(
        &mut self,
        arc_receiver: Arc<Mutex<Box<dyn SocketReader>>>,
        context: &Context<T>,
    ) -> ConResult {
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

            // debug_print!(DebugColor::Blue, "[StreamSocket recv] Length: {}, streamID: {}, FrameID: {}, shardID: {} / {}, tx_r_instant: {}", shard_length, stream_id, packet_index, shard_index + 1, shards_count, tx_r_instant );

            if stream_id == VIDEO {
                let rx_instant = context.scheduler.time();

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
            })
        };

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
                    // First fallback: Try to recycle old packets
                    let recyclable = components
                        .in_progress_packets
                        .iter()
                        .find(|(&idx, _)| {
                            wrapping_cmp(idx, shard_recv_state_mut.packet_index.wrapping_sub(5))
                                == Ordering::Less
                        })
                        .map(|(&k, _)| k);

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
            // println!(
            //     "final length: {} (buffer_length = {})",
            //     in_progress_packet.buffer.len(),
            //     in_progress_packet.buffer_length
            // );
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

                if shard_recv_state_mut.stream_id == VIDEO {
                    // println!(" inside while :) size = {}, packet_cursor = {}\\n",  size, shard_recv_state_mut.packet_cursor);
                }
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
            // println!("PACKET IS COMPLETE, SENDING!!");
            if shard_recv_state_mut.stream_id == VIDEO {
                if let Some(inner_map) = self.map_rx.get(&shard_recv_state_mut.packet_index) {
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
            };

            // println!("{:?} Reconstructed packet!!", reconstruct);
            components.packet_queue.send(reconstruct).ok();

            if shard_recv_state_mut.stream_id == VIDEO {
                self.rx_bytes = 0;
                self.rx_shard_counter = 0;
                self.duplicated_shard_counter = 0;

                // Keep only shards data from the latest packets (using wrapping logic)
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
            // Keep only shards with later packet index (using wrapping logic)
            while let Some((idx, _)) = components.in_progress_packets.iter().find(|(idx, _)| {
                wrapping_cmp(**idx, shard_recv_state_mut.packet_index) == Ordering::Less
            }) {
                let idx = *idx; // fix borrow rule
                let packet = components.in_progress_packets.remove(&idx).unwrap();
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
                }
            }
        }
    }

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
impl<H: DeserializeOwned + Serialize> StreamReceiver<H> {
    pub fn recv(&mut self, timeout: Duration) -> ConResult<ReceiverData<H>> {
        // println!("receiving FULL packet from shards!!!");
        let packet = self
            .packet_receiver
            .recv_timeout(timeout)
            .handle_try_again()?;
        // println!("receiving packet2!!!");

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

        // println!("AAAAAAAAAAAAA!!!!!!!!!");
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

            let tx_r_instant = now
                .duration_since(self.ref_time)
                .as_secs_f32();

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
                self.frame_tracker
                    .insert(self.next_packet_index, now);
            }
        }
        self.shards_count = shards_count;
        self.used_buffers.push(buffer.inner);

        Ok(())
    }
}

impl<H: Serialize> StreamSender<H> {
    pub fn get_buffer_emu(&mut self, header: &H, current_bitrate_mbps: f32) -> Result<Buffer<H>> {
        let mut buffer = generate_random_video_payload(current_bitrate_mbps);

        let header_size = bincode::serialized_size(header)? as usize;
        let hidden_offset = SHARD_PREFIX_SIZE + header_size;

        if buffer.len() < hidden_offset {
            buffer.resize(hidden_offset, 0);
        }

        bincode::serialize_into(&mut buffer[SHARD_PREFIX_SIZE..hidden_offset], header)?;
        let buffer_len = buffer.len();

        self.next_packet_index += 1;
        Ok(Buffer {
            inner: buffer,
            hidden_offset,
            length: buffer_len,
            _phantom: PhantomData,
        })
    }

    pub fn send_header(&mut self, header: &H, now:TaiTime<0> ) -> Result<()> {

        let buffer = self.get_buffer_emu(header, 20.0 as f32)?;

        println!("WATCHOUT, using 20 as default!!");
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

pub fn generate_random_video_payload(current_bitrate_mbps: f32) -> Vec<u8> {
    // Initialize the random number generator
    let mut rng = rand::thread_rng();

    // Generate a random u8
    let random_u8: u8 = rng.gen();

    // Calculate the payload size based on bitrate
    let no_bytes_based_bitrate = (1416.97 * current_bitrate_mbps + -810.06) as usize;

    // Create buffer with random values
    let buffer_inner = vec![7; no_bytes_based_bitrate]; // TODO: MAKE EACH RANDOM; NOW FOR DBG is 7!

    buffer_inner
}
