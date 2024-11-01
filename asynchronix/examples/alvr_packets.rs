




use std::{
    fmt::{self, Debug},
    net::IpAddr,
    path::PathBuf,
    time::Duration,
};

use std::{
    marker::PhantomData,
    mem,
    net::{IpAddr, TcpListener, TcpStream},
    time::{Duration, Instant},
};



pub struct ProtoControlSocket {
    inner: TcpStream,
}


pub enum PeerType<'a> {
    AnyClient(Vec<IpAddr>),
    Server(&'a TcpListener),
}

impl ProtoControlSocket {
    pub fn connect_to(timeout: Duration, peer: PeerType<'_>) -> ConResult<(Self, IpAddr)> {
        let socket = match peer {
            PeerType::AnyClient(ips) => {
                tcp::connect_to_client(
                    timeout,
                    &ips,
                    CONTROL_PORT,
                    SocketBufferSize::Default,
                    SocketBufferSize::Default,
                )?
                .0
            }
            PeerType::Server(listener) => tcp::accept_from_server(listener, None, timeout)?.0,
        };

        let peer_ip = socket.peer_addr().to_con()?.ip();

        Ok((Self { inner: socket }, peer_ip))
    }

    pub fn send<S: Serialize>(&mut self, packet: &S) -> Result<()> {
        framed_send(&mut self.inner, &mut vec![], packet)
    }

    pub fn recv<R: DeserializeOwned>(&mut self, timeout: Duration) -> ConResult<R> {
        framed_recv(&mut self.inner, &mut vec![], &mut None, timeout)
    }

    pub fn split<S: Serialize, R: DeserializeOwned>(
        self,
        timeout: Duration,
    ) -> Result<(ControlSocketSender<S>, ControlSocketReceiver<R>)> {
        self.inner.set_read_timeout(Some(timeout))?;

        Ok((
            ControlSocketSender {
                inner: self.inner.try_clone()?,
                buffer: vec![],
                _phantom: PhantomData,
            },
            ControlSocketReceiver {
                inner: self.inner,
                buffer: vec![],
                recv_state: None,
                _phantom: PhantomData,
            },
        ))
    }
}


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