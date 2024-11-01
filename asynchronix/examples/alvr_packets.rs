




use std::{
    fmt::{self, Debug},
    net::IpAddr,
    path::PathBuf,
    time::Duration,
};




#[derive(Serialize, Deserialize)]
pub enum ClientControlPacket {
    PlayspaceSync(Option<Vec2>),
    RequestIdr,
    KeepAlive,
    StreamReady, // This flag notifies the server the client streaming socket is ready listening
    ViewsConfig(ViewsConfig),
    Battery(BatteryPacket),
    VideoErrorReport, // legacy
    Buttons(Vec<ButtonEntry>),
    ActiveInteractionProfile { device_id: u64, profile_id: u64 },
    Log { level: LogSeverity, message: String },
    Reserved(String),
    ReservedBuffer(Vec<u8>),
    NetworkStatistics(NetworkStatisticsPacket),
}


#[derive(Serialize, Deserialize, Default, Clone)]
pub struct ClientStatistics {
    pub target_timestamp: Duration, // identifies the frame
    pub frame_index: i32,

    pub frame_interval: Duration,

    pub video_decode: Duration,
    pub video_decoder_queue: Duration,
    pub rendering: Duration,
    pub vsync_queue: Duration,
    pub total_pipeline_latency: Duration,

 
    pub frames_dropped: u32,
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