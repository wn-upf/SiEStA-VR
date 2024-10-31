

const UPDATE_BITRATE_INTERVAL: Duration = Duration::from_secs(1); 

const MAX_HISTORY_SIZE: usize = 256; 

const INITIAL_BITRATE_MBPS: f32 = 100.0; 
const INITIAL_FRAMERATE_FPS: f32 = 90.0; 


pub const TRACKING: u16 = 0;
pub const HAPTICS: u16 = 1;
pub const AUDIO: u16 = 2;
pub const VIDEO: u16 = 3;
pub const STATISTICS: u16 = 4;

pub struct ShardPacket{

    shard_id: i32, 
    packet_id: i32, 

    length_shard_bits: usize, 
}


pub struct ReceiverDataStats{
    frame_index:        u32,
    frame_span:         f32,
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


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct NetworkStatisticsPacket {
    pub frame_index: i32,
    pub frame_span: f32,

    pub bytes_in_frame: u32,
    pub bytes_in_frame_app: u32,

    pub frame_interarrival: f32,

    pub interarrival_jitter: f32,
    pub ow_delay: f32,
    pub filtered_ow_delay: f32,

    pub frames_skipped: u32,

    pub rx_bytes: u32,

    pub rx_shard_counter: u32,
    pub duplicated_shard_counter: u32,

    pub highest_rx_frame_index: i32,
    pub highest_rx_shard_index: i32,
}


pub struct ReceiverData<H> {
    buffer: Option<Vec<u8>>,
    size: usize, // counting the prefix
    used_buffer_queue: mpsc::Sender<Vec<u8>>,
    had_packet_loss: bool,
    _phantom: PhantomData<H>,

    stats_frame: ReceiverDataStats, 
}





#[derive(Serialize, Deserialize)]
pub struct VideoPacketHeader {
    pub timestamp: Duration,
    pub is_idr: bool,
}


pub struct VideoFrame{

    packet_id: i32, 
    length_frame_bits: usize, 
}

