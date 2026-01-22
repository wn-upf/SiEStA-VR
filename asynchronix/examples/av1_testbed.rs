use crossbeam::channel::{bounded, unbounded, Receiver, Sender, TryRecvError};
use minifb::{Key, Window, WindowOptions};
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};
use std::sync::{
    Arc,
};
use std::thread;
use rand::Rng; 
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

use std::collections::BTreeMap;
use tokio::sync::mpsc;


/////////////////////////////////////////////////////////////////////////////////////
////////////////////// PARALLEL SETTINGS ////////////////////////////////////////////

pub const NUM_PARALLEL_THREADS_ENCODE: usize = 16; 
pub const NUM_PARALLEL_THREADS_DECODE: usize = 4; 

/////////////////////////////////////////////////////////////////////////////////////
////////////////////// VIDEO SETTINGS ///////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////

pub const VIDEO_WINDOW_SCALE_FACTOR: f64 = 0.44; 
pub const VIDEO_PATH: &str = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/swordsmith_90fps.mp4";
pub const VIDEO_FPS : f32 = 90.0; 
pub const VIDEO_LOOP_DURATION_SECONDS: f32 = 80.0; // loop the video after 30 secs 
pub const VIDEO_GOP_SIZE: usize = 60; // Group of Pictures size, I-P frame frequency

pub const VIDEO_BOOL_RANDOM_OFFSET: bool = false; 
/////////////////////////////////////////////////////////////////////////////////////
//////////////////////  ABR DEMO ////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////

pub const DEMO_MAX_TARGET_BITRATE: f32 = 100.0; // on/off style of different bitrates,
                                                // and visual effect. 
pub const DEMO_MIN_TARGET_BITRATE: f32 = 1.0; 
pub const DEMO_DURATION_BITRATE_SWITCH: f32 = 3.0; 
/////////////////////////////////////////////////////////////////////////////////////
////////////////////// GRAPH FOR FRAME SIZES ////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////

pub const DISPLAY_GRAPH_HUD_HEIGTH: usize = 260; 

pub const DISPLAY_GRAPH_MAX_FRAMES: usize = 140; 
pub const DISPLAY_GRAPH_SCALE_HEIGHT: f32 = 180.0; 
pub const DISPLAY_GRAPH_UPPER_KB_BOUND: f32 = 800.0; 
pub const DISPLAY_GRAPH_SCALE_TEXT: usize = 2; 
pub const DISPLAY_GRAPH_LOG_Y_SCALE: bool = true; 
pub const DISPLAY_TARGET_FRAMES_PER_SECOND: f32 = 24.0; // Absolute cinema (the encoder is too slow for real time 90FPS@4K processing demo) 
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// --- 1. Helpers ---
use colored::Colorize;

// use crate::lib::alvr_stream_socket::ConResult;
#[allow(unused)]
pub enum DebugColor {
    Red,
    Green,
    Blue,
    Yellow,
    Magenta,
    Cyan,
    White,
    Black,
    Orange,
    Purple,
    DarkGreen,
    DarkRed,
    DarkBlue,
    LightGray,
    DarkGray,
    LightPink,
    Teal,
    Gold,
    Violet,
    Lime,
    DarkOrange,
    Peach,
    Coral,
    Mint,
    Navy,
    Lavender,
    Salmon,
    Chocolate,
    Indigo,
    Turquoise,
    Maroon,
    LightBlue,
    ForestGreen,
    Azure,
    Rose,
    Crimson,
    Amber,
    SaddleBrown,
    Tan,
}
#[allow(unused)]
impl DebugColor {
    pub fn to_color_fn(&self) -> fn(String) -> colored::ColoredString {
        match self {
            DebugColor::Red => |s| s.red(),
            DebugColor::Green => |s| s.green(),
            DebugColor::Blue => |s| s.blue(),
            DebugColor::Yellow => |s| s.yellow(),
            DebugColor::Magenta => |s| s.magenta(),
            DebugColor::Cyan => |s| s.cyan(),
            DebugColor::White => |s| s.white(),
            DebugColor::Black => |s| s.black(),
            DebugColor::Orange => |s| s.truecolor(255, 165, 0),
            DebugColor::Purple => |s| s.truecolor(128, 0, 128),
            DebugColor::DarkGreen => |s| s.truecolor(0, 100, 0),
            DebugColor::DarkRed => |s| s.truecolor(139, 0, 0),
            DebugColor::DarkBlue => |s| s.truecolor(0, 0, 139),
            DebugColor::LightGray => |s| s.truecolor(211, 211, 211),
            DebugColor::DarkGray => |s| s.truecolor(169, 169, 169),
            DebugColor::LightPink => |s| s.truecolor(255, 182, 193),
            DebugColor::Teal => |s| s.truecolor(0, 128, 128),
            DebugColor::Gold => |s| s.truecolor(255, 215, 0),
            DebugColor::Violet => |s| s.truecolor(238, 130, 238),
            DebugColor::Lime => |s| s.truecolor(50, 205, 50),
            DebugColor::DarkOrange => |s| s.truecolor(255, 140, 0),
            DebugColor::Peach => |s| s.truecolor(255, 218, 185),
            DebugColor::Coral => |s| s.truecolor(255, 127, 80),
            DebugColor::Mint => |s| s.truecolor(189, 252, 201),
            DebugColor::Navy => |s| s.truecolor(0, 0, 128),
            DebugColor::Lavender => |s| s.truecolor(230, 230, 250),
            DebugColor::Salmon => |s| s.truecolor(250, 128, 114),
            DebugColor::Chocolate => |s| s.truecolor(210, 105, 30),
            DebugColor::Indigo => |s| s.truecolor(75, 0, 130),
            DebugColor::Turquoise => |s| s.truecolor(64, 224, 208),
            DebugColor::Maroon => |s| s.truecolor(128, 0, 0),
            DebugColor::LightBlue => |s| s.truecolor(173, 216, 230),
            DebugColor::ForestGreen => |s| s.truecolor(34, 139, 34),
            DebugColor::Azure => |s| s.truecolor(240, 255, 255),
            DebugColor::Rose => |s| s.truecolor(255, 228, 225),
            DebugColor::Crimson => |s| s.truecolor(220, 20, 60),
            DebugColor::Amber => |s| s.truecolor(255, 191, 0),
            DebugColor::SaddleBrown => |s| s.truecolor(139, 69, 19),
            DebugColor::Tan => |s| s.truecolor(160, 82, 45),
        }
    }
    pub fn to_background_fn(&self) -> fn(String) -> colored::ColoredString {
        match self {
            DebugColor::Red => |s| s.on_red(),
            DebugColor::Green => |s| s.on_green(),
            DebugColor::Blue => |s| s.on_blue(),
            DebugColor::Yellow => |s| s.on_yellow(),
            DebugColor::Magenta => |s| s.on_magenta(),
            DebugColor::Cyan => |s| s.on_cyan(),
            DebugColor::White => |s| s.on_white(),
            DebugColor::Black => |s| s.on_black(),
            DebugColor::Orange => |s| s.on_truecolor(255, 165, 0),
            DebugColor::Purple => |s| s.on_truecolor(128, 0, 128),
            DebugColor::DarkGreen => |s| s.on_truecolor(0, 100, 0),
            DebugColor::DarkRed => |s| s.on_truecolor(139, 0, 0),
            DebugColor::DarkBlue => |s| s.on_truecolor(0, 0, 139),
            DebugColor::LightGray => |s| s.on_truecolor(211, 211, 211),
            DebugColor::DarkGray => |s| s.on_truecolor(169, 169, 169),
            DebugColor::LightPink => |s| s.on_truecolor(255, 182, 193),
            DebugColor::Teal => |s| s.on_truecolor(0, 128, 128),
            DebugColor::Gold => |s| s.on_truecolor(255, 215, 0),
            DebugColor::Violet => |s| s.on_truecolor(238, 130, 238),
            DebugColor::Lime => |s| s.on_truecolor(50, 205, 50),
            DebugColor::DarkOrange => |s| s.on_truecolor(255, 140, 0),
            DebugColor::Peach => |s| s.on_truecolor(255, 218, 185),
            DebugColor::Coral => |s| s.on_truecolor(255, 127, 80),
            DebugColor::Mint => |s| s.on_truecolor(189, 252, 201),
            DebugColor::Navy => |s| s.on_truecolor(0, 0, 128),
            DebugColor::Lavender => |s| s.on_truecolor(230, 230, 250),
            DebugColor::Salmon => |s| s.on_truecolor(250, 128, 114),
            DebugColor::Chocolate => |s| s.on_truecolor(210, 105, 30),
            DebugColor::Indigo => |s| s.on_truecolor(75, 0, 130),
            DebugColor::Turquoise => |s| s.on_truecolor(64, 224, 208),
            DebugColor::Maroon => |s| s.on_truecolor(128, 0, 0),
            DebugColor::LightBlue => |s| s.on_truecolor(173, 216, 230),
            DebugColor::ForestGreen => |s| s.on_truecolor(34, 139, 34),
            DebugColor::Azure => |s| s.on_truecolor(240, 255, 255),
            DebugColor::Rose => |s| s.on_truecolor(255, 228, 225),
            DebugColor::Crimson => |s| s.on_truecolor(220, 20, 60),
            DebugColor::Amber => |s| s.on_truecolor(255, 191, 0),
            DebugColor::SaddleBrown => |s| s.truecolor(139, 69, 19),
            DebugColor::Tan => |s| s.truecolor(160, 82, 45),
        }
    }
}


macro_rules! print_pretty {
    ($color:expr, $fmt:expr, $($arg:tt)*) => {
            let msg = format!($fmt, $($arg)*);
            println!("{}", $color.to_color_fn()(msg));
    }
}

macro_rules! format_elapsed2{
    ($elapsed:expr) => {{
        let total_seconds =
            $elapsed.as_secs() as f64 + ($elapsed.subsec_nanos() as f64 / 1_000_000_000.0);
        format!("{:.9}", total_seconds)
    }};
}

#[derive(Clone, Copy, Debug, Default)]
pub struct FrameMetadata {
    pub bitrate_mbps: f32,
    pub video_timestamp: f64, // Exact presentation time in seconds
    pub chunk_id: usize,      // Which 2.5s segment this belongs to
    pub frame_size_bytes: usize, 
}

// What travels over the network/channels
pub struct TaggedPacket {
    data: Vec<u8>,
    meta: FrameMetadata,
}

// --- CHUNKED AV1 ENCODER ---

pub struct ChunkedAv1Encoder {
    input: String,
    width: u32,
    height: u32,
    bitrate: String,
    chunk_duration: f64,
    current_offset: f64,
    frame_tx: Sender<TaggedPacket>,
    frame_rx: Receiver<TaggedPacket>,
    frame_queue: VecDeque<TaggedPacket>,
    parser: Av1Parser, // Uses the new Av1Parser
    encoder_str: String,
    gop_size: usize,
    intra_refresh: bool,
    framerate: f32, // Added to allow FPS control
    start_instant: Instant, 
    frame_count_in_chunk: usize, // Track how many frames we've emitted for this chunk
    chunk_start_timestamp: f64,  // The 'offset' passed in new()
}

impl ChunkedAv1Encoder {
    pub fn new(
        input: &str,
        width: u32,
        height: u32,
        bitrate: &str,
        chunk_duration: f64,
        string: String,
        offset_video: f64,
        framerate: f32,
        gop_size: usize,
        intra_refresh: bool,
    ) -> Self {
        // println!("Initializing ChunkedAv1Encoder");
        let (frame_tx, frame_rx) = bounded(1000);

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
            parser: Av1Parser::new(),
            encoder_str: string.clone(),
            gop_size,
            intra_refresh,
            framerate,
            start_instant: Instant::now(), 
            frame_count_in_chunk: 0,
            chunk_start_timestamp: offset_video, 
        }
    }

    pub async fn start_chunking(&mut self, bitrate_mbps: f32, now: Instant ) {
        let bitrate_adjusted_fps = bitrate_mbps; 
        self.bitrate = format!("{:.2}M", bitrate_adjusted_fps);
        
        print_pretty!(
            DebugColor::DarkBlue, 
            "{} AV1 CHUNKING with bitrate {} Mbps", // Changed to {:?} just in case TaiTime doesn't implement Display
            // now.duration_since(self.start_instant).as_secs_f32(),
            self.encoder_str,
            bitrate_mbps,
        );

        // Clear parser buffer to avoid stale data
        // self.parser.buffer.clear(); 

        let mut command = FfmpegCommand::new();
        
        // Base arguments
        let mut child = command
            // .hwaccel("cuda")
            .args(&["-ss", &self.current_offset.to_string()])
            .args(&["-t", &self.chunk_duration.to_string()])
            .args(&["-threads", &format!("{}", NUM_PARALLEL_THREADS_ENCODE)])
            .args(&["-hide_banner", "-nostats", "-loglevel", "error"])
            .input(&self.input)
            .args(&["-vf", &format!( "scale={}:{}:force_original_aspect_ratio=disable,format=yuv420p"         // Video Filter
                                    ,self.width, self.height)])
            .args(&["-c:v", "libsvtav1"]) 
            .args(&["-preset", "9"]) // 9 is highest for 4k, so choosing it for XR RTC. 
            // .args(&["-svtav1-params", "rc=2:lookahead=0:pred-struct=1"]) // rc=1 (VBR), lookahead=0 , pred-struct=2 (Low Delay P, no future encoded frames for XR)
            .args(&["-svtav1-params", "rc=2:lookahead=0:pred-struct=1:lp=3:tile-columns=2:tile-rows=1:fast-decode=1"]) // Tiling for fastness, lp: level of parallelism,
            .args(&["-b:v", &self.bitrate ])
            .args(&["-bufsize", &self.bitrate])
            .args(&["-g", &format!("{}", self.gop_size)])
            // .args(&["-intra-refresh", &format!("{}", self.intra_refresh as i32)])
            .args(&["-f", "obu", "-"])
            .spawn().unwrap();

        let stdout = child.stdout.take().unwrap(); 
        let mut reader = BufReader::new(stdout);

        // Access the public field .stderr directly and .take() the option
        if let Some(stderr) = child.stderr.take() {
            let mut err_reader = BufReader::new(stderr);
            std::thread::spawn(move || {
                for line in err_reader.lines() {
                    if let Ok(l) = line {
                        println!("ffmpeg stderr (AV1): {}", l);
                    }
                }
            });
        }
        // --- FIX ENDS HERE ---

        let mut buf = [0u8; 4096];
        let mut frame_count_in_chunk = 0;
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    self.parser.add_data(&buf[..n]);
                    let frames = self.parser.get_frames();
                    for frame in frames {

                        let pts = self.chunk_start_timestamp + (self.frame_count_in_chunk as f64 / self.framerate as f64);
                        let lenframebytes = frame.len(); 
                        let tagged = TaggedPacket {
                            data: frame,
                            meta: FrameMetadata {
                                bitrate_mbps: bitrate_mbps,
                                video_timestamp: pts,
                                chunk_id: self.current_offset as usize,
                                frame_size_bytes: lenframebytes, 
                            }
                        };
                        if let Err(e) = self.frame_tx.send(tagged)
                         {
                            eprintln!("{} Error sending AV1 frame: {}", e, self.encoder_str);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("{} Error reading AV1 chunk: {}", e, self.encoder_str);
                    break;
                }
            }
        }
        let _ = child.wait();
        self.current_offset += self.chunk_duration;
    }

    pub async fn next_frame(&mut self) -> Option<TaggedPacket> {
        // 1. Priority: Check if we have frames in the local queue
        if let Some(frame) = self.frame_queue.pop_front() {
            return Some(frame);
        }

        // 2. Check the crossbeam channel
        // We use try_recv() to avoid blocking the async executor.
        // Since start_chunking fills this channel, frames should be ready.
        if let Ok(frame) = self.frame_rx.try_recv() {
            return Some(frame);
        }

        None
    }

}


// Wrapper for Command to match your syntax
pub struct FfmpegCommand {
    cmd: Command,
}
impl FfmpegCommand {
    pub fn new() -> Self {
        Self { cmd: Command::new("ffmpeg") }
    }
    pub fn args(mut self, args: &[&str]) -> Self {
        self.cmd.args(args);
        self
    }
    pub fn hwaccel(mut self, _hw: &str) -> Self {
        self.cmd.args(&["-hwaccel", "cuda"]); // hardcoded for this example
        self
    }
    pub fn input(mut self, input: &str) -> Self {
        self.cmd.arg("-i").arg(input);
        self
    }
    pub fn spawn(mut self) -> std::io::Result<std::process::Child> {
        self.cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
    }
}

// --- 2. AV1 Parser Logic ---

#[derive(Debug, Clone)]
pub struct ObuUnit {
    pub obu_type: u8,
    pub data: Vec<u8>,
    pub is_sequence_header: bool,
}

pub struct Av1Parser {
    buffer: Vec<u8>,
    sequence_header: Option<Vec<u8>>,
}

impl Av1Parser {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            sequence_header: None,
        }
    }

    pub fn add_data(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    pub fn update_sequence_header(&mut self, data: &[u8]) {
        self.sequence_header = Some(data.to_vec());
    }

    pub fn get_sequence_header(&self) -> Option<&Vec<u8>> {
        self.sequence_header.as_ref()
    }

    fn parse_leb128(&self, offset: usize) -> Option<(usize, usize)> {
        let mut value: usize = 0;
        let mut bytes_read = 0;
        let mut shift = 0;

        loop {
            if offset + bytes_read >= self.buffer.len() { return None; }
            let byte = self.buffer[offset + bytes_read];
            value |= ((byte & 0x7F) as usize) << shift;
            bytes_read += 1;
            shift += 7;
            if (byte & 0x80) == 0 { break; }
            if bytes_read > 8 { return None; } // Safety
        }
        Some((value, bytes_read))
    }

    pub fn next_obu(&mut self) -> Option<ObuUnit> {
        if self.buffer.is_empty() { return None; }

        // OBU Header parsing
        let header_byte = self.buffer[0];
        let obu_type = (header_byte >> 3) & 0xF;
        let extension_flag = (header_byte >> 2) & 1;
        let has_size_field = (header_byte >> 1) & 1;

        if has_size_field == 0 {
            // Without size fields, we can't parse a stream easily.
            // ffmpeg -f obu usually includes them.
            self.buffer.clear();
            return None;
        }

        let mut offset = 1;
        if extension_flag == 1 {
            offset += 1;
            if self.buffer.len() < offset { return None; }
        }

        let (payload_size, leb_bytes) = self.parse_leb128(offset)?;
        offset += leb_bytes;

        let total_size = offset + payload_size;
        if self.buffer.len() < total_size { return None; }

        let obu_data = self.buffer[0..total_size].to_vec();
        self.buffer.drain(0..total_size);

        // OBU Type 1 is Sequence Header
        let is_sequence_header = obu_type == 1;
        if is_sequence_header {
            self.sequence_header = Some(obu_data.clone());
        }

        Some(ObuUnit {
            obu_type,
            data: obu_data,
            is_sequence_header,
        })
    }

    pub fn get_frames(&mut self) -> Vec<Vec<u8>> {
        let mut frames = Vec::new();
        while let Some(obu) = self.next_obu() {
            frames.push(obu.data);
        }
        frames
    }
}

// --- 3. The AV1 Decoder (Adapted from HevcDecoder) ---

pub struct Av1Decoder {
    frame_rx: Receiver<Vec<u8>>,
    packet_tx: Sender<Vec<u8>>,
    _stdin_handle: thread::JoinHandle<()>,
    _stderr_handle: thread::JoinHandle<()>,
    pub width: u32,
    pub height: u32,
    parser: Av1Parser,
    frame_buffer: VecDeque<Vec<u8>>,
    decoded_frames: VecDeque<Vec<u8>>,
    
    // Metrics
    pub frames_processed: usize,
    pub keyframes_seen: usize,
    pub expected_frame_size: usize,
    pub total_bytes_processed: f64,
    
    // Control
    decoder_string: String,
    priming_complete: bool,
    processing_semaphore: Arc<Semaphore>,
    
    // Sync
    id_queue: VecDeque<u32>,
    decoded_frame_counter: usize,

    pub metadata_queue: VecDeque<FrameMetadata>, 
}

impl Av1Decoder {
    pub fn new(width: u32, height: u32, decoder_str: &str) -> Self {
        let frame_size = (width as usize) * (height as usize) * 3; // RGB24
        let decoder_string = decoder_str.to_string();

        //  - We construct the FFmpeg pipe here
        let mut child = FfmpegCommand::new()
            .args(&["-threads", &format!("{}", NUM_PARALLEL_THREADS_DECODE)])
            // .hwaccel("cuda") // Enable if you have RTX 30/40 series
            .args(&["-hide_banner", "-loglevel", "error"])
            .args(&["-f", "obu"]) // Input format is raw OBU
            .args(&["-i", "-"])   // Read from stdin
            .args(&["-vsync", "0"])
            .args(&["-pix_fmt", "rgb24"]) // Output format
            .args(&["-f", "rawvideo", "-"]) // Write to stdout
            .spawn()
            .expect("Failed to spawn ffmpeg decoder");

        let stdout = child.stdout.take().unwrap();
        let stdin =  child.stdin.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        let (frame_tx, frame_rx) = unbounded::<Vec<u8>>();
        let (packet_tx, packet_rx) = bounded::<Vec<u8>>(100);

        // 1. STDOUT Reader (Decoded Frames)
        let decoder_str_clone = decoder_string.clone();
        thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut buffer = Vec::with_capacity(frame_size * 2);
            let mut chunk = vec![0u8; 8192];

            loop {
                match reader.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => {
                        buffer.extend_from_slice(&chunk[..n]);
                        while buffer.len() >= frame_size {
                            let frame = buffer.drain(..frame_size).collect::<Vec<u8>>();
                            if let Err(_) = frame_tx.send(frame) { return; }
                        }
                    }
                    Err(e) => {
                        eprintln!("{} Read Error: {}", decoder_str_clone, e);
                        break;
                    }
                }
            }
        });

        // 2. STDIN Writer (Encoded Packets)
        let decoder_str_clone2 = decoder_string.clone();
        let stdin_handle = thread::spawn(move || {
            let mut writer = stdin;
            for packet in packet_rx {
                if let Err(e) = writer.write_all(&packet) {
                    eprintln!("{} Write Error: {}", decoder_str_clone2, e);
                    break;
                }
                let _ = writer.flush();
            }
        });

        // 3. STDERR Handler
        let decoder_str_clone3 = decoder_string.clone();
        let stderr_handle = thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines() {
                if let Ok(l) = line {
                    println!("{} [FFMPEG]: {}", decoder_str_clone3, l);
                }
            }
        });

        Self {
            frame_rx,
            packet_tx,
            _stdin_handle: stdin_handle,
            _stderr_handle: stderr_handle,
            width,
            height,
            parser: Av1Parser::new(),
            frame_buffer: VecDeque::new(),
            decoded_frames: VecDeque::new(),
            frames_processed: 0,
            keyframes_seen: 0,
            expected_frame_size: frame_size,
            total_bytes_processed: 0.0,
            decoder_string: decoder_string,
            priming_complete: false,
            processing_semaphore: Arc::new(Semaphore::new(10)),
            id_queue: VecDeque::new(),
            decoded_frame_counter: 0,

            metadata_queue: VecDeque::new(), 
        }
    }

    pub async fn process_packet(&mut self, packet: TaggedPacket, id: u32) {
        let _permit = self.processing_semaphore.acquire().await.unwrap();

        // Add to parser
        self.parser.add_data(&packet.data);


        // In AV1, we don't need to manually extract "Frames" as strictly as HEVC NALs
        // for the decoder pipe, but we do it to maintain your logic structure.
        let obus = self.parser.get_frames();
        
        for obu_data in obus {
            // Detect Sequence Header (Keyframe-ish)
            if obu_data.len() > 1 {
                let obu_type = (obu_data[0] >> 3) & 0xF;
                let is_display_frame = obu_type == 6 || obu_type == 3;

                if is_display_frame {
                    self.metadata_queue.push_back(packet.meta);
                    self.frames_processed += 1;
                } else if obu_type == 1 {
                    // Sequence header - do not push metadata, it produces no output frame
                    self.keyframes_seen += 1;
                    print_pretty!(DebugColor::Magenta, "{} 🔑 Seq Header", self.decoder_string);
                }

            }

            self.frames_processed += 1;
            
            // Send to FFmpeg
            if let Err(e) = self.packet_tx.send(obu_data) {
                 eprintln!("Failed to send to ffmpeg: {}", e);
            }
            
            // Keep ID sync
            self.id_queue.push_back(id);
        }
    }

    pub fn next_decoded_frame(&mut self) -> Option<(Vec<u8>, FrameMetadata)> {
        // Poll the receiver channel
        loop {
            match self.frame_rx.try_recv() {
                Ok(frame) => {
                    if frame.len() == self.expected_frame_size {
                        self.decoded_frames.push_back(frame);
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return None,
            }
        }

        if let Some(frame) = self.decoded_frames.pop_front() {
            // In a real scenario, you handle ID queue sync carefully. 
            // For this visualization, we just pop.
            let id = self.id_queue.pop_front().unwrap_or(0);
            let meta = self.metadata_queue.pop_front().unwrap_or(FrameMetadata::default()); 
            return Some((frame, meta));
        }
        None
    }
}



#[tokio::main]
async fn main() {
    let width = 3840;
    let height = 2160;
    
    // 1. Scaling Configuration
    let scale_factor = VIDEO_WINDOW_SCALE_FACTOR; 
    let scaled_w = (width as f64 * scale_factor) as usize;
    let scaled_h = (height as f64 * scale_factor) as usize;

    let window_h = scaled_h + DISPLAY_GRAPH_HUD_HEIGTH; 
    let input_file = VIDEO_PATH; 

    println!("Initializing {}x{} display (scaled from 4K)...", scaled_w, scaled_h);

    // 2. Setup Decoder and Channels
    let mut decoder = Av1Decoder::new(width, height, "AV1_DEC_01");
    let (tx_source, rx_source) = unbounded::<TaggedPacket>();
    
    // Removed the incorrect 'let now: f64 = ...' definition here


    //// GRAPH OVERLAY CODE: FRAME SIZES. 
    let mut size_history: VecDeque<f32> = VecDeque::from(vec![0.0; DISPLAY_GRAPH_MAX_FRAMES]);
    let max_graph_height = 100; // Pixels
    let graph_x = 10;
    let graph_y = scaled_h - 200; // Position near bottom



    // 3. Setup Source Thread (FFmpeg)
    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        
        rt.block_on(async {
            let chunk_len = 2.5; 
            let max_parallel_chunks = 1; // How many FFmpeg instances to run at once
            let (result_tx, mut result_rx) = mpsc::channel(100);
            

            let mut rng = rand::thread_rng();
            let random_offset: f64 = rng.gen_range(0.0..25.0);      
            
            let mut next_offset_to_encode = if VIDEO_BOOL_RANDOM_OFFSET {random_offset} else {0.0};
            let mut next_offset_to_send = 0.0;
            let mut reorder_buffer: BTreeMap<u64, Vec<TaggedPacket>> = BTreeMap::new();
            let mut active_workers = 0;

            let start_time = Instant::now();

            loop {
                // A. SPAWN WORKERS: Fill the pipeline until we hit max_parallel_chunks
                while active_workers < max_parallel_chunks {
                    let offset = next_offset_to_encode;
                    let tx = result_tx.clone();
                    let input = input_file.to_string();
                    
                    // Determine bitrate for this specific chunk
                    let elapsed = start_time.elapsed().as_secs();
                    let bitrate = if (elapsed  / DEMO_DURATION_BITRATE_SWITCH as u64) % 2 == 0 { DEMO_MAX_TARGET_BITRATE } else { DEMO_MIN_TARGET_BITRATE};

                    tokio::spawn(async move {
                        // Create a fresh encoder for this chunk
                        let mut encoder = ChunkedAv1Encoder::new(
                            &input, 3840, 2160, "10M", chunk_len,
                            "AV1_Parallel_Worker".to_string(),
                            offset, VIDEO_FPS, VIDEO_GOP_SIZE, false
                        );

                        // --- CRITICAL OPTIMIZATION ---
                        // Since we are running 3 encoders, we restrict each to use 
                        // only a portion of the CPU to prevent thrashing.
                        // Preset 12 is essential for real-time 4K software encoding.
                        encoder.start_chunking(bitrate, Instant::now()).await;

                        let mut frames = Vec::new();
                        while let Some(f) = encoder.next_frame().await {
                            frames.push(f);
                        }
                        
                        // Send frames back with the offset as a key (converted to u64 for BTreeMap)
                        let _ = tx.send(((offset * 1000.0) as u64, frames)).await;
                    });

                    next_offset_to_encode += chunk_len;
                    active_workers += 1;

                    // Loop video logic
                    if next_offset_to_encode > VIDEO_LOOP_DURATION_SECONDS as f64 {
                        next_offset_to_encode = 0.0;
                    }
                }

                // B. COLLECT RESULTS: Wait for any worker to finish
                if let Some((offset_key, frames)) = result_rx.recv().await {
                    reorder_buffer.insert(offset_key, frames);
                    active_workers -= 1;
                }

                // C. DISPATCH: Send chunks to the main thread in the correct order
                let current_key = (next_offset_to_send * 1000.0) as u64;
                while let Some(frames) = reorder_buffer.remove(&current_key) {
                    for frame in frames {
                        if let Err(_) = tx_source.send(frame) { return; }
                    }
                    next_offset_to_send += chunk_len;
                    if next_offset_to_send > VIDEO_LOOP_DURATION_SECONDS as f64{ next_offset_to_send = 0.0; }
                }
            }
        });
    });

    // 4. Visualization Window 
    let mut window = Window::new(
        &format!("AV1 Realtime Decode - Scaled View ({:.2}:1)", VIDEO_WINDOW_SCALE_FACTOR),
        scaled_w,
        window_h,
        WindowOptions::default(),
    ).unwrap_or_else(|e| {
        panic!("{}", e);
    });

    let mut scaled_buffer = vec![0u32; scaled_w * window_h];
    let mut frame_count = 0;
    let mut last_log = Instant::now();

    let target_fps = DISPLAY_TARGET_FRAMES_PER_SECOND;  // Absolute cinema (the encoder is too slow for real time 90FPS@4K processing demo) 
    let target_frame_time = Duration::from_secs_f64(1.0 / target_fps as f64);
    let mut next_frame_time = Instant::now();

    let mut frame_count_timing = 0; 

   while window.is_open() && !window.is_key_down(Key::Escape) {
        
        // A. ALWAYS Ingest packets as fast as they arrive
        // We do this continuously so the UDP/channel buffer doesn't overflow
        while let Ok(tagpacket) = rx_source.try_recv() {

            // let packet = tagpacket.data; 
            decoder.process_packet(tagpacket, 0).await;
        }

        // B. PACE the Rendering (VSync Logic)
        let now = Instant::now();
        if now >= next_frame_time {
            
            // Try to get ONE decoded frame
            if let Some((rgb_data, _id)) = decoder.next_decoded_frame() {
                frame_count += 1;
                frame_count_timing += 1; 

                let rawdog_video_time = frame_count_timing as f32 / VIDEO_FPS;
                // Optimized Scaling (Same as before)
                for y in 0..scaled_h {
                    for x in 0..scaled_w {
                        let sx = (x * width as usize) / scaled_w;
                        let sy = (y * height as usize) / scaled_h;
                        let src_idx = (sy * width as usize + sx) * 3;
                        
                        if src_idx + 2 < rgb_data.len() {
                            let r = rgb_data[src_idx] as u32;
                            let g = rgb_data[src_idx + 1] as u32;
                            let b = rgb_data[src_idx + 2] as u32;
                            scaled_buffer[y * scaled_w + x] = (r << 16) | (g << 8) | b;
                        }
                    }
                }

                let hud_bg_color = 0x101010; // Very dark grey
                for y in scaled_h..window_h {
                    for x in 0..scaled_w {
                        scaled_buffer[y * scaled_w + x] = hud_bg_color;
                    }
                }

                size_history.pop_front();
                size_history.push_back(_id.frame_size_bytes as f32);

                let g_height = DISPLAY_GRAPH_SCALE_HEIGHT;  // Easily change this to 100, 300, etc.
                let g_ceiling = DISPLAY_GRAPH_UPPER_KB_BOUND; // The max kB the graph represents

                render_graph(
                    &mut scaled_buffer,
                    &size_history,
                    85,                                  // x_offset
                    scaled_h + 60,                       // y_offset (Below video)
                    scaled_w,                            // stride
                    (_id.bitrate_mbps * 1_000_000.0), 
                    DISPLAY_TARGET_FRAMES_PER_SECOND,
                    g_height as usize,
                    g_ceiling,
                    DISPLAY_GRAPH_LOG_Y_SCALE, 
                );

                render_text(
                    &mut scaled_buffer,
                    &format!("Sampled FPS: {:.0} ({} in window)", VIDEO_FPS, DISPLAY_TARGET_FRAMES_PER_SECOND),
                    10, 10, scaled_w, 0xFFCC00, 3 
                );

                render_text(
                    &mut scaled_buffer,
                    &format!("Bitrate: {:.1} Mbps", _id.bitrate_mbps),
                    10, 80, scaled_w, 0x00FF00, 3 
                );

                render_text(
                    &mut scaled_buffer,
                    &format!("Video playback: {:.3}s", rawdog_video_time % VIDEO_LOOP_DURATION_SECONDS),
                    10, 45, scaled_w, 0x00FF00, 3 
                );

               
                
                
                // Update window
                window.update_with_buffer(&scaled_buffer, scaled_w, window_h).unwrap();
                
                // Schedule next frame time. 
                // If we are lagging, reset to 'now' to catch up, otherwise add target duration.
                if now > next_frame_time + target_frame_time {
                    next_frame_time = now + target_frame_time;
                } else {
                    next_frame_time += target_frame_time;
                }
            } else {
                // Buffer underflow (no frame ready yet), just keep window open
                window.update();
            }
        } else {
            // If we have time to spare, sleep a tiny bit to save CPU
            // (But keep it short to keep polling network packets)
            thread::sleep(Duration::from_millis(1));
            window.update(); // Keep window responsive
        }

        // FPS Logging
        if last_log.elapsed().as_secs() >= 1 {
            print_pretty!(DebugColor::Blue, "FPS: {} | Keyframes: {}", frame_count, decoder.keyframes_seen);
            frame_count = 0;
            last_log = Instant::now();
        }
    }
}


#[allow(unused)]
// Renders ASCII text into the minifb window with coordinates.
pub fn render_text(
    buffer: &mut [u32],
    text: &str,
    x: usize,
    y: usize,
    stride: usize,
    color: u32,
    scale: usize,
) {
    const FONT_WIDTH: usize = 5;
    const FONT_HEIGHT: usize = 7;
    const CHAR_SPACING: usize = 1;

    let scaled_font_width = FONT_WIDTH * scale;
    let scaled_char_spacing = CHAR_SPACING * scale;

    // Extended font with lowercase letters
    let font = [
        // Space (0)
        [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        // ! (1)
        [0x04, 0x04, 0x04, 0x04, 0x00, 0x04, 0x00],
        // " (2)
        [0x0A, 0x0A, 0x00, 0x00, 0x00, 0x00, 0x00],
        // # (3)
        [0x0A, 0x0A, 0x1F, 0x0A, 0x1F, 0x0A, 0x0A],
        // $ (4)
        [0x04, 0x0F, 0x14, 0x0E, 0x05, 0x1E, 0x04],
        // % (5)
        [0x18, 0x19, 0x02, 0x04, 0x08, 0x13, 0x03],
        // & (6)
        [0x0C, 0x12, 0x14, 0x08, 0x15, 0x12, 0x0D],
        // ' (7)
        [0x0C, 0x04, 0x08, 0x00, 0x00, 0x00, 0x00],
        // ( (8)
        [0x02, 0x04, 0x08, 0x08, 0x08, 0x04, 0x02],
        // ) (9)
        [0x08, 0x04, 0x02, 0x02, 0x02, 0x04, 0x08],
        // * (10)
        [0x00, 0x04, 0x15, 0x0E, 0x15, 0x04, 0x00],
        // + (11)
        [0x00, 0x04, 0x04, 0x1F, 0x04, 0x04, 0x00],
        // , (12)
        [0x00, 0x00, 0x00, 0x00, 0x0C, 0x04, 0x08],
        // - (13)
        [0x00, 0x00, 0x00, 0x1F, 0x00, 0x00, 0x00],
        // . (14)
        [0x00, 0x00, 0x00, 0x00, 0x00, 0x0C, 0x0C],
        // / (15)
        [0x00, 0x01, 0x02, 0x04, 0x08, 0x10, 0x00],
        // 0-9 (16-25)
        [0x0E, 0x11, 0x13, 0x15, 0x19, 0x11, 0x0E],
        [0x04, 0x0C, 0x04, 0x04, 0x04, 0x04, 0x0E],
        [0x0E, 0x11, 0x01, 0x02, 0x04, 0x08, 0x1F],
        [0x1F, 0x02, 0x04, 0x02, 0x01, 0x11, 0x0E],
        [0x02, 0x06, 0x0A, 0x12, 0x1F, 0x02, 0x02],
        [0x1F, 0x10, 0x1E, 0x01, 0x01, 0x11, 0x0E],
        [0x06, 0x08, 0x10, 0x1E, 0x11, 0x11, 0x0E],
        [0x1F, 0x01, 0x02, 0x04, 0x08, 0x08, 0x08],
        [0x0E, 0x11, 0x11, 0x0E, 0x11, 0x11, 0x0E],
        [0x0E, 0x11, 0x11, 0x0F, 0x01, 0x02, 0x0C],
        // : ; < = > ? @ (26-32)
        [0x00, 0x0C, 0x0C, 0x00, 0x0C, 0x0C, 0x00],
        [0x00, 0x0C, 0x0C, 0x00, 0x0C, 0x04, 0x08],
        [0x02, 0x04, 0x08, 0x10, 0x08, 0x04, 0x02],
        [0x00, 0x00, 0x1F, 0x00, 0x1F, 0x00, 0x00],
        [0x08, 0x04, 0x02, 0x01, 0x02, 0x04, 0x08],
        [0x0E, 0x11, 0x01, 0x02, 0x04, 0x00, 0x04],
        [0x0E, 0x11, 0x01, 0x0D, 0x15, 0x15, 0x0E],
        // A-Z (33-58)
        [0x0E, 0x11, 0x11, 0x11, 0x1F, 0x11, 0x11],
        [0x1E, 0x11, 0x11, 0x1E, 0x11, 0x11, 0x1E],
        [0x0E, 0x11, 0x10, 0x10, 0x10, 0x11, 0x0E],
        [0x1C, 0x12, 0x11, 0x11, 0x11, 0x12, 0x1C],
        [0x1F, 0x10, 0x10, 0x1E, 0x10, 0x10, 0x1F],
        [0x1F, 0x10, 0x10, 0x1E, 0x10, 0x10, 0x10],
        [0x0E, 0x11, 0x10, 0x17, 0x11, 0x11, 0x0F],
        [0x11, 0x11, 0x11, 0x1F, 0x11, 0x11, 0x11],
        [0x0E, 0x04, 0x04, 0x04, 0x04, 0x04, 0x0E],
        [0x07, 0x02, 0x02, 0x02, 0x02, 0x12, 0x0C],
        [0x11, 0x12, 0x14, 0x18, 0x14, 0x12, 0x11],
        [0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x1F],
        [0x11, 0x1B, 0x15, 0x15, 0x11, 0x11, 0x11],
        [0x11, 0x11, 0x19, 0x15, 0x13, 0x11, 0x11],
        [0x0E, 0x11, 0x11, 0x11, 0x11, 0x11, 0x0E],
        [0x1E, 0x11, 0x11, 0x1E, 0x10, 0x10, 0x10],
        [0x0E, 0x11, 0x11, 0x11, 0x15, 0x12, 0x0D],
        [0x1E, 0x11, 0x11, 0x1E, 0x14, 0x12, 0x11],
        [0x0F, 0x10, 0x10, 0x0E, 0x01, 0x01, 0x1E],
        [0x1F, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04],
        [0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x0E],
        [0x11, 0x11, 0x11, 0x11, 0x11, 0x0A, 0x04],
        [0x11, 0x11, 0x11, 0x15, 0x15, 0x15, 0x0A],
        [0x11, 0x11, 0x0A, 0x04, 0x0A, 0x11, 0x11],
        [0x11, 0x11, 0x11, 0x0A, 0x04, 0x04, 0x04],
        [0x1F, 0x01, 0x02, 0x04, 0x08, 0x10, 0x1F],
        // [ \ ] ^ _ (59-63)
        [0x0E, 0x08, 0x08, 0x08, 0x08, 0x08, 0x0E],
        [0x00, 0x10, 0x08, 0x04, 0x02, 0x01, 0x00],
        [0x0E, 0x02, 0x02, 0x02, 0x02, 0x02, 0x0E],
        [0x04, 0x0A, 0x11, 0x00, 0x00, 0x00, 0x00],
        [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1F],
        // a-z (64-89) - lowercase letters
        [0x00, 0x00, 0x0E, 0x01, 0x0F, 0x11, 0x0F],  // a
        [0x10, 0x10, 0x16, 0x19, 0x11, 0x11, 0x1E],  // b
        [0x00, 0x00, 0x0E, 0x10, 0x10, 0x11, 0x0E],  // c
        [0x01, 0x01, 0x0D, 0x13, 0x11, 0x11, 0x0F],  // d
        [0x00, 0x00, 0x0E, 0x11, 0x1F, 0x10, 0x0E],  // e
        [0x06, 0x09, 0x08, 0x1C, 0x08, 0x08, 0x08],  // f
        [0x00, 0x0F, 0x11, 0x11, 0x0F, 0x01, 0x0E],  // g
        [0x10, 0x10, 0x16, 0x19, 0x11, 0x11, 0x11],  // h
        [0x04, 0x00, 0x0C, 0x04, 0x04, 0x04, 0x0E],  // i
        [0x02, 0x00, 0x06, 0x02, 0x02, 0x12, 0x0C],  // j
        [0x10, 0x10, 0x12, 0x14, 0x18, 0x14, 0x12],  // k
        [0x0C, 0x04, 0x04, 0x04, 0x04, 0x04, 0x0E],  // l
        [0x00, 0x00, 0x1A, 0x15, 0x15, 0x11, 0x11],  // m
        [0x00, 0x00, 0x16, 0x19, 0x11, 0x11, 0x11],  // n
        [0x00, 0x00, 0x0E, 0x11, 0x11, 0x11, 0x0E],  // o
        [0x00, 0x00, 0x1E, 0x11, 0x1E, 0x10, 0x10],  // p
        [0x00, 0x00, 0x0D, 0x13, 0x0F, 0x01, 0x01],  // q
        [0x00, 0x00, 0x16, 0x19, 0x10, 0x10, 0x10],  // r
        [0x00, 0x00, 0x0E, 0x10, 0x0E, 0x01, 0x1E],  // s
        [0x08, 0x08, 0x1C, 0x08, 0x08, 0x09, 0x06],  // t
        [0x00, 0x00, 0x11, 0x11, 0x11, 0x13, 0x0D],  // u
        [0x00, 0x00, 0x11, 0x11, 0x11, 0x0A, 0x04],  // v
        [0x00, 0x00, 0x11, 0x11, 0x15, 0x15, 0x0A],  // w
        [0x00, 0x00, 0x11, 0x0A, 0x04, 0x0A, 0x11],  // x
        [0x00, 0x00, 0x11, 0x11, 0x0F, 0x01, 0x0E],  // y
        [0x00, 0x00, 0x1F, 0x02, 0x04, 0x08, 0x1F],  // z
    ];

    let mut char_x = x;

    for c in text.chars() {
        let index = match c {
            ' ' => 0,
            '!' => 1,
            '"' => 2,
            '#' => 3,
            '$' => 4,
            '%' => 5,
            '&' => 6,
            '\'' => 7,
            '(' => 8,
            ')' => 9,
            '*' => 10,
            '+' => 11,
            ',' => 12,
            '-' => 13,
            '.' => 14,
            '/' => 15,
            '0'..='9' => (c as usize) - ('0' as usize) + 16,
            ':' => 26,
            ';' => 27,
            '<' => 28,
            '=' => 29,
            '>' => 30,
            '?' => 31,
            '@' => 32,
            'A'..='Z' => (c as usize) - ('A' as usize) + 33,
            '[' => 59,
            '\\' => 60,
            ']' => 61,
            '^' => 62,
            '_' => 63,
            'a'..='z' => (c as usize) - ('a' as usize) + 64,  // Now maps to lowercase glyphs
            _ => 0,
        };

        // Draw the character with scaling
        for row in 0..FONT_HEIGHT {
            for scaled_row in 0..scale {
                let buffer_y = y + (row * scale) + scaled_row;

                for col in 0..FONT_WIDTH {
                    if (font[index][row] & (1 << (FONT_WIDTH - 1 - col))) != 0 {
                        for scaled_col in 0..scale {
                            let buffer_x = char_x + (col * scale) + scaled_col;

                            if buffer_y < buffer.len() / stride && buffer_x < stride {
                                let buffer_index = buffer_y * stride + buffer_x;
                                if buffer_index < buffer.len() {
                                    buffer[buffer_index] = color;
                                }
                            }
                        }
                    }
                }
            }
        }

        char_x += scaled_font_width + scaled_char_spacing;
    }
}


pub fn render_graph(
    buffer: &mut [u32],
    history: &VecDeque<f32>,
    x_offset: usize,
    y_offset: usize,
    stride: usize,
    target_bps: f32,
    fps: f32,
    graph_height: usize,
    max_size_kb: f32,
    use_log: bool, 
) {
    let bar_width = 5;
    let spacing = 1;
    let graph_width = history.len() * (bar_width + spacing);
    
    // --- CONFIGURATION ---
    // Define pastel colors for less saturation
    const COL_RED: u32 = 0xEE6666;    // Soft Pastel Red
    const COL_YELLOW: u32 = 0xF0E68C; // Khaki/Soft Yellow
    const COL_GREEN: u32 = 0x8FBC8F;  // Dark Sea Green (Soft Green)
    const COL_GRID: u32 = 0x555555;   // Slightly lighter grid for visibility
    const COL_TEXT: u32 = 0xCCCCCC;   // Light Grey text (not pure white)
    const COL_TGT: u32 = 0x87CEFA;    // Light Sky Blue (softer Cyan)

    // Layout Offsets
    let label_margin = 55; // Increased from 40 to add distance
    let title_margin = 45; // Increased from 13 to move title up

    // --- 0. DRAW BACKGROUND PLATE ---
    let bg_y_start = y_offset.saturating_sub(title_margin + 10); // Expanded top
    let bg_y_end = y_offset + graph_height + 20;
    let bg_x_start = x_offset.saturating_sub(label_margin + 20); // Expanded left
    let bg_x_end = x_offset + graph_width + 170;    

    for y in bg_y_start..bg_y_end {
        if y >= buffer.len() / stride { continue; }
        for x in bg_x_start..bg_x_end {
            if x >= stride { continue; }
            
            let pixel_idx = y * stride + x;
            let current_pixel = buffer[pixel_idx];
            
            // Dimming logic (50% opacity)
            let r = ((current_pixel >> 16) & 0xFF) / 2;
            let g = ((current_pixel >> 8) & 0xFF) / 2;
            let b = (current_pixel & 0xFF) / 2;
            buffer[pixel_idx] = (r << 16) | (g << 8) | b;
        }
    }

    // --- LOG SCALE HELPERS ---
    let min_log_kb = 1.0f32;
    let log_min = min_log_kb.ln();
    let log_max = max_size_kb.max(min_log_kb + 0.1).ln();
    let log_range = log_max - log_min;

    let get_normalized_height = |kb_val: f32| -> f32 {
        if use_log {
            if kb_val < min_log_kb {
                0.0
            } else {
                ((kb_val.ln() - log_min) / log_range).min(1.0).max(0.0)
            }
        } else {
            (kb_val / max_size_kb).min(1.0).max(0.0)
        }
    };

    let current_target_kb = (target_bps / (8.0 * fps)) / 1024.0;

    // --- 1. TITLE & INFO ---
    render_text(
        buffer,
        &format!(" Frame size ({} window) in kBytes", history.len()), // Shortened text for cleaner look
        x_offset,
        y_offset.saturating_sub(title_margin), 
        stride,
        0xFFFFFF, 
        3,
    );
    
    // Axis Unit Label (Moved slightly left to align with numbers)
    render_text(
        buffer, 
        "[kB]", 
        x_offset.saturating_sub(label_margin), 
        y_offset.saturating_sub(title_margin), 
        stride, 
        COL_TEXT, 
        2
    );

    // --- 2. DRAW Y-AXIS MARKERS & GRID ---
    for i in 1..=4 {
        let visual_percentage = i as f32 * 0.25;
        let marker_y = y_offset + graph_height - (visual_percentage * graph_height as f32) as usize;
        
        let label_val = if use_log {
            (visual_percentage * log_range + log_min).exp()
        } else {
            max_size_kb * visual_percentage
        };

        if marker_y < buffer.len() / stride {
            for px in x_offset..(x_offset + graph_width) {
                if px < stride {
                    buffer[marker_y * stride + px] = COL_GRID; 
                }
            }
        }

        // Label: Pushed further left via `label_margin`
        render_text(
            buffer,
            &format!("{:.0}", label_val),
            x_offset.saturating_sub(label_margin), 
            marker_y - 4,
            stride,
            COL_TEXT,
            DISPLAY_GRAPH_SCALE_TEXT,
        );
    }

    // --- 3. DRAW THE DATA BARS ---
    for (i, &size_bytes) in history.iter().enumerate() {
        let size_kb = size_bytes / 1024.0;
        let norm_h = get_normalized_height(size_kb);
        let bar_height = (norm_h * graph_height as f32) as usize;

        // "Alive" Color Logic: Adjust brightness based on proximity to target
        let color = if size_kb > current_target_kb * 1.5 {
            0xFF5555 // Danger: High Intensity Red
        } else if size_kb > current_target_kb {
            // Alert: Brighten the yellow if it's way over target
            let intensity = ((size_kb / (current_target_kb * 1.5)) * 255.0) as u32;
            0xFF0000 | (intensity << 8) // Shifts from Orange to Yellow
        } else {
            // Healthy: The closer to target, the "greener" it gets
            let health = (size_kb / current_target_kb).min(1.0);
            let g = (150.0 + (105.0 * health)) as u32; // 150 to 255
            (g << 8) | 100 // Emerald Green with a hint of Blue
        };

        // Render Bar
        for bh in 0..bar_height {
            let py = y_offset + graph_height - bh;
            if py < buffer.len() / stride {
                for bw in 0..bar_width {
                    let px = x_offset + (i * (bar_width + spacing)) + bw;
                    if px < stride {
                        buffer[py * stride + px] = color;
                    }
                }
            }
        }
    }

    // --- 4. DRAW DYNAMIC TARGET LINE ---
    let target_norm = get_normalized_height(current_target_kb);
    let target_y = y_offset + graph_height - (target_norm * graph_height as f32) as usize;

    if target_y < (buffer.len() / stride) {
        for px in x_offset..(x_offset + graph_width) {
            if px < stride {
                buffer[target_y * stride + px] = COL_TGT;
            }
        }
        
        render_text(
            buffer, 
            &format!("TGT: {:.1} kB", current_target_kb), 
            x_offset + graph_width + 5, 
            target_y - 4, 
            stride, 
            COL_TGT, 
            DISPLAY_GRAPH_SCALE_TEXT
        );
    }
}