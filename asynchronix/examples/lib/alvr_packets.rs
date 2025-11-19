use glam::{Quat, Vec3};
use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use std::time::Duration;

// use crate::lib::EdcaAc;
/// A 2-dimensional vector.
#[derive(Clone, Copy, PartialEq, Serialize, Deserialize, Debug)]
// #[cfg_attr(feature = "cuda", repr(align(8)))]
// #[cfg_attr(not(target_arch = "spirv"), repr(C))]
// #[cfg_attr(target_arch = "spirv", repr(simd))]
pub struct Vec2 {
    pub x: f32,
    pub y: f32,
}

// Field of view in radians
#[derive(Serialize, Deserialize, PartialEq, Default, Clone, Copy, Debug)]
pub struct Fov {
    pub left: f32,
    pub right: f32,
    pub up: f32,
    pub down: f32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ViewsConfig {
    // Note: the head-to-eye transform is always a translation along the x axis
    pub ipd_m: f32,
    pub fov: [Fov; 2],
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub enum ButtonValue {
    Binary(bool),
    Scalar(f32),
}

#[derive(Serialize, Deserialize)]
pub struct ButtonEntry {
    pub path_id: u64,
    pub value: ButtonValue,
}
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BatteryPacket {
    pub device_id: u64,
    pub gauge_value: f32, // range [0, 1]
    pub is_plugged: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogSeverity {
    Error = 3,
    Warning = 2,
    Info = 1,
    Debug = 0,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ClientControlPacket {
    PlayspaceSync(Option<Vec2>),
    RequestIdr,
    KeepAlive,
    StreamReady, // This flag notifies the server the client streaming socket is ready listening
    ViewsConfig(ViewsConfig),
    Battery(BatteryPacket),
    VideoErrorReport, // legacy
    // Buttons(Vec<ButtonEntry>),
    ActiveInteractionProfile { device_id: u64, profile_id: u64 },
    Log { level: LogSeverity, message: String },
    Reserved(String),
    ReservedBuffer(Vec<u8>),

    NetworkStatistics(NetworkStatisticsPacket),
    DeadlineShardLossStat(DeadlineShardlossStatPacket),
}

// pub struct ProtoControlSocket {
//     inner: TcpStream,
// }

// impl ProtoControlSocket {
//     pub fn connect_to(timeout: Duration, peer: PeerType<'_>) -> ConResult<(Self, IpAddr)> {
//         let socket = match peer {
//             PeerType::AnyClient(ips) => {
//                 tcp::connect_to_client(
//                     timeout,
//                     &ips,
//                     CONTROL_PORT,
//                     SocketBufferSize::Default,
//                     SocketBufferSize::Default,
//                 )?
//                 .0
//             }
//             PeerType::Server(listener) => tcp::accept_from_server(listener, None, timeout)?.0,
//         };

//         let peer_ip = socket.peer_addr().to_con()?.ip();

//         Ok((Self { inner: socket }, peer_ip))
//     }

//     pub fn send<S: Serialize>(&mut self, packet: &S) -> Result<()> {
//         framed_send(&mut self.inner, &mut vec![], packet)
//     }

//     pub fn recv<R: DeserializeOwned>(&mut self, timeout: Duration) -> ConResult<R> {
//         framed_recv(&mut self.inner, &mut vec![], &mut None, timeout)
//     }

//     pub fn split<S: Serialize, R: DeserializeOwned>(
//         self,
//         timeout: Duration,
//     ) -> Result<(ControlSocketSender<S>, ControlSocketReceiver<R>)> {
//         self.inner.set_read_timeout(Some(timeout))?;

//         Ok((
//             ControlSocketSender {
//                 inner: self.inner.try_clone()?,
//                 buffer: vec![],
//                 _phantom: PhantomData,
//             },
//             ControlSocketReceiver {
//                 inner: self.inner,
//                 buffer: vec![],
//                 recv_state: None,
//                 _phantom: PhantomData,
//             },
//         ))
//     }
// }

// pub enum PeerType<'a> {
//     AnyClient(Vec<IpAddr>),
//     Server(&'a TcpListener),
// }

/// A 3-dimensional vector.
// #[derive(Clone, Copy, PartialEq)]
// #[cfg_attr(not(target_arch = "spirv"), repr(C))]
// #[cfg_attr(target_arch = "spirv", repr(simd))]
// pub struct Vec3 {
//     pub x: f32,
//     pub y: f32,
//     pub z: f32,
// }

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
pub struct DeadlineShardlossStatPacket {
    pub frame_indexes: Vec<u32>,
    pub shards_lost: Vec<usize>,
    // pub edca_ac: EdcaAc,
}
#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct NadaStats {
    pub frame_send_timestamp: i64,
    pub shard_loss_rate: f64,
    pub plr: f64,
    pub is_idr: bool,

    //RTCP Feedback Report: NADA Receiver--> Sender
    pub nada_feedback: bool,
    pub nada_xcurr: f64,
    pub nada_rmode: i8,
    pub nada_recv: i64,

    //Only to debug NADA Receiver
    pub t_last: i64,
    pub d_queue: i64,
    pub d_tilde: f64,
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

    pub lost_shards_deadline: usize,

    pub everest_capacity_update: f32,
    pub everest_throughput_update: f32,
    pub everest_dshort: f32,
    pub everest_dlong: f32,
    pub everest_command: EverestCommand,
    pub buffer_level_decoder: u8,
    pub rebuffering_events_last_s: u8,

    pub nada_stats: NadaStats,
    // NADA FIELDS after checking code implementation, loss ratio computation of NADA seems not too work, reluctant about adding it to testbed
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum EverestCommand {
    SlowDown,
    SpeedUp,
    Continue,
}
