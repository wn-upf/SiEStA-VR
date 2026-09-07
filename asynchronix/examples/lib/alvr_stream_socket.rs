use crate::debug_bgprint;
use crate::lib::{
    get_prefix_path, get_third_octet, models_mm1k::NetworkPattern, Av1Parser, DebugColor,
    HevcParser, OldCsvTrace,
};
use glam::{Quat, EulerRot};
use crate::print_green;
use crate::{lib::DEBUG_PRINT_ENABLED, lib::USE_FFMPEG_DEMO, print_pretty};
use asynchronix::model::Context;
use crossbeam::channel::{bounded, unbounded, Receiver, RecvTimeoutError, Sender, TryRecvError};
use ffmpeg_sidecar::command::FfmpegCommand;
use rand::Rng;
use rayon::result;
use std::collections::HashMap;
#[allow(unused)]
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
#[allow(unused_imports)]
#[allow(dead_code)]
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};

// use crate::debug_bgprint;
use crate::lib::models_XR::SHARD_PREFIX_SIZE;
use crate::lib::models_XR::{XRDevice, HEIGHT_ENCODER, WIDTH_ENCODER};
use std::cell::{Cell, RefCell};
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
// use crate::lib::DebugColor;
use anyhow::{anyhow, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::error::Error;
// use std::io::{Read, Write};
use std::net::IpAddr;

use csv::Writer;
use std::result::Result::Ok;
use tai_time::TaiTime;

use crate::lib::alvr_packets::{DeviceMotion, Pose};

// use super::alvr_packets::NetworkStatisticsPacket;
// use std::env;

pub const DEBUG_FFMPEG_AV1_LOGS: bool = false;
pub const ALVR_ORIGINAL_SOCKETRX_BEHAVIOR: bool = false; // TODO: Bring this from input args to simulator

// pub const UPDATE_BITRATE_INTERVAL: Duration = Duration::from_secs(1);
pub const MAX_HISTORY_SIZE: usize = 256; // shorter term averages
                                        // pub const INITIAL_FRAMERATE_FPS: f32 = 90.0;

pub const DEADLINE_PACKETS_S: Duration = Duration::from_millis(30);
pub const MAX_DEADLINE_IN_STATS: usize = 10;
pub const OFFSET_VIDEO: f64 = 95.0;

pub const VBV_SETTING_RELAXATION_MULTIPLIER: f32 = 3.0; // Relax the VBV buffer size, only one frame makes lower bitrates (<35 Mbps) generate frames much larger in average than the expected.


// pub const CHUNK_SIZE_FRAMES: usize = 300;
// pub const IDR_FRAME_SIZE_GOP: usize = 60;

pub const MAX_PACKET_SIZE_RECV: usize = 2000 * 8;
pub const TRACKING: u16 = 0;
pub const HAPTICS: u16 = 1;
pub const AUDIO: u16 = 2;
pub const VIDEO: u16 = 3;
pub const STATISTICS: u16 = 4;
pub const CONTROL_STREAM: u16 = 5;
// CUSTOM / ADDED
pub const FRAMELOSS_PACKET: u16 = 8;
pub const FOVOPTIX_BW_PROBE: u16 = 9;

pub const BG_TRAFFIC_STREAM_ID: u16 = 7; // For synthetic background traffic packets, if needed in the future

pub const _SERVER_DISCONNECTED_MESSAGE: &str = "The streamer has disconnected.";


pub const USE_HARDCODED_SIZES_VALIDATION: bool = false;
// Define the path to your hardcoded CSV

/// Probes a video file's duration (in seconds) via `ffprobe`.
/// Used so chunk seek offsets can be wrapped modulo the real duration when
/// looping a sample video with `-stream_loop -1` (ffmpeg only honors `-ss`
/// within the first loop iteration, so offsets must be pre-wrapped in Rust).
pub fn probe_video_duration_secs(input: &str) -> f64 {
    let output = Command::new("ffprobe")
        .args(&[
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            input,
        ])
        .output();

    match output {
        Ok(out) if out.status.success() => {
            String::from_utf8_lossy(&out.stdout)
                .trim()
                .parse::<f64>()
                .unwrap_or(0.0)
        }
        _ => {
            eprintln!("Warning: ffprobe failed to get duration for '{}', looping may misbehave", input);
            0.0
        }
    }
}



pub fn get_stream_name(stream_id: u16) -> &'static str {
    match stream_id {
        TRACKING => "TRACKING",
        HAPTICS => "HAPTICS",
        AUDIO => "AUDIO",
        VIDEO => "VIDEO",
        STATISTICS => "STATISTICS",
        CONTROL_STREAM => "CONTROL",
        FRAMELOSS_PACKET => "FRAME LOSS",
        BG_TRAFFIC_STREAM_ID => "BG TRAFFIC",

        _ => "UNKNOWN_STREAM",
    }
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoCodec {
    HEVC,
    AV1,
}

impl fmt::Display for VideoCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VideoCodec::HEVC => write!(f, "HEVC"),
            VideoCodec::AV1 => write!(f, "AV1"),
        }
    }
}

pub enum ChunkedEncoder {
    Hevc(ChunkedHevcEncoder),
    HevcSoftware(ChunkedSoftwareHevcEncoder),
    Av1(ChunkedAv1Encoder),
}

impl ChunkedEncoder {
    pub async fn start_chunking(&mut self, bitrate_mbps: f32, now: TaiTime<0>, latest_gaze: Vec<[Option<Quat>; 2]>) {
        match self {
            ChunkedEncoder::Hevc(e) => e.start_chunking(bitrate_mbps, now, latest_gaze).await,
            ChunkedEncoder::Av1(e) => e.start_chunking(bitrate_mbps, now, latest_gaze).await,
            ChunkedEncoder::HevcSoftware(e) => e.start_chunking(bitrate_mbps, now, latest_gaze).await,
        }
    }

    pub async fn next_frame(&mut self) -> Option<Vec<u8>> {
        match self {
            ChunkedEncoder::Hevc(e) => e.next_frame().await,
            ChunkedEncoder::Av1(e) => e.next_frame().await,
            ChunkedEncoder::HevcSoftware(e) => e.next_frame().await,
        }
    }

    pub fn clear_buffers(&mut self) {
        match self {
            ChunkedEncoder::Hevc(e) => e.clear_buffers(),
            ChunkedEncoder::Av1(e) => e.clear_buffers(),
            ChunkedEncoder::HevcSoftware(e) => e.clear_buffers(),
        }
    }
}

#[allow(unused)]
pub struct ChunkedAv1Encoder {
    input: String,
    width: u32,
    height: u32,
    bitrate: String,
    chunk_duration: f64,
    current_offset: f64,
    video_duration: f64, // real duration of `input`, used to wrap seeks when looping
    // Adapted to Vec<u8> to match ChunkedHevcEncoder's interface for easy integration
    frame_tx: Sender<Vec<u8>>,
    frame_rx: Receiver<Vec<u8>>,
    frame_queue: VecDeque<Vec<u8>>,
    parser: Av1Parser,
    encoder_str: String,
    gop_size: usize,
    intra_refresh: bool,
    framerate: f32,

    aggregation_buffer: Vec<u8>, // to aggregate multiple OBUs into full frame.
    chunk_index: usize,
    use_foveation: bool, 
    vbv_perframe: bool, 

}
#[allow(unused)]
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
        use_foveation: bool, 
        vbv_perframe: bool, 

    ) -> Self {
        println!("Initializing ChunkedAv1Encoder");
        let (frame_tx, frame_rx) = bounded(1000);
        let video_duration = probe_video_duration_secs(input);

        Self {
            input: input.to_string(),
            width,
            height,
            bitrate: bitrate.to_string(),
            chunk_duration,
            current_offset: offset_video,
            video_duration,
            frame_tx,
            frame_rx,
            frame_queue: VecDeque::new(),
            parser: Av1Parser::new(),
            encoder_str: string.clone(),
            gop_size,
            intra_refresh,
            framerate,
            aggregation_buffer: Vec::new(),
            chunk_index: 0, 
            use_foveation, 
            vbv_perframe
        }
    }

    pub fn clear_parser(&mut self) {
        self.parser.buffer.clear();
    }

    pub fn clear_buffers(&mut self) {
        // Clear the parser's internal buffer
        self.parser.buffer.clear();
        // Clear the waiting frame queue
        self.frame_queue.clear();

        // Optional: drain the channel if necessary, though usually
        // queue and parser are sufficient for a reset.
        while let Ok(_) = self.frame_rx.try_recv() {}
    }
    #[inline]
    pub async fn start_chunking(&mut self, bitrate_mbps: f32, now: TaiTime<0>, latest_gaze: Vec<[Option<Quat>; 2]>) {
        let bitrate_adjusted_fps = bitrate_mbps;
        self.bitrate = format!("{:.2}M", bitrate_adjusted_fps);

        println!(
            "{} - {} AV1 CHUNKING with bitrate {} Mbps",
            crate::format_elapsed!(now),
            self.encoder_str,
            bitrate_mbps,
        );

        self.parser.buffer.clear();
        
        let frames_per_chunk = (self.framerate * self.chunk_duration as f32).round() as usize;
        // let start_frame_idx: usize = self.chunk_index * frames_per_chunk;

        let exact_offset = self.current_offset;
        // let start_frame_idx = (exact_offset * self.framerate as f64).round() as usize  + self.chunk_index * frames_per_chunk ;
        let start_frame_idx = (exact_offset * self.framerate as f64).round() as usize;
        // let exact_offset = start_frame_idx as f64 / self.framerate as f64;

        // `exact_offset`/`start_frame_idx` stay monotonic (they drive the burned-in
        // OCR frame counter, which must match the reference reader's absolute frame
        // count). But ffmpeg's `-ss` only seeks within the first `-stream_loop`
        // iteration, so the actual seek position must be wrapped into video bounds.
        let seek_offset = if self.video_duration > 0.0 {
            exact_offset % self.video_duration
        } else {
            exact_offset
        };
        // Clamp this chunk's read length so it never straddles the loop point:
        // reading across the wrap resets the source PTS mid-stream, which the
        // muxer rejects as non-monotonic DTS. A small safety margin (a couple of
        // frame durations) is subtracted because ffmpeg can roll over into the
        // next loop iteration slightly before the exact end-of-file timestamp.
        let loop_safety_margin = (2.0 / self.framerate as f64).max(0.02);
        let chunk_duration_clamped = if self.video_duration > 0.0 {
            self.chunk_duration.min((self.video_duration - seek_offset - loop_safety_margin).max(0.05))
        } else {
            self.chunk_duration
        };

        let frame_duration_ms = 1000.0 / self.framerate;
        let bufsize_ms = frame_duration_ms.max(20.0); // Force at least 20ms for AV1: The maximum buffer size must be between [20, 10000]

        let bufsize_kbits = (  VBV_SETTING_RELAXATION_MULTIPLIER * bitrate_mbps * 1000.0) / self.framerate; // Calculate single-frame VBV buffer size to limit max frame size, as in 'How to model Cloud VR' paper by Korneev et al.
                // Relaxation multiplier because only one frame makes lower bitrates (<35 Mbps) generate frames much larger in average than the expected.
        let bufsize_str = if self.vbv_perframe{
            format!("{:.0}k", bufsize_kbits)
        }
        else{
            format!("{:.0}k", bitrate_mbps * 1000.0)
        }; 

        let fovea_w = 1000;
        let fovea_h = 1000;
        
        // Convert to f32 for math
        let range_x = (self.width - fovea_w) as f32;
        let range_y = (self.height - fovea_h) as f32;

        let frames_in_chunk = (self.framerate * self.chunk_duration as f32).round() as usize;
        let gaze_count = latest_gaze.len();

        let mut expr_x = String::new();
        let mut expr_y = String::new();
        

        if gaze_count == 0 || !self.use_foveation || frames_in_chunk == 0 {
            // Fallback to absolute center if no data or foveation is off
            expr_x = format!("{:.0}", range_x / 2.0);
            expr_y = format!("{:.0}", range_y / 2.0);
        } else {
            // Start the additive string with 0
            expr_x.push_str("0");
            expr_y.push_str("0");

            let max_yaw = std::f32::consts::FRAC_PI_4; 
            let max_pitch = std::f32::consts::FRAC_PI_4;

            for f in 0..frames_in_chunk {
                // Time-slice mapping logic (using your existing method)
                let start_idx = ((f as f32 / frames_in_chunk as f32) * gaze_count as f32).floor() as usize;
                let mut end_idx = (((f + 1) as f32 / frames_in_chunk as f32) * gaze_count as f32).floor() as usize;
                end_idx = end_idx.clamp(start_idx + 1, gaze_count);

                let mut gaze_yaw = 0.0;
                let mut gaze_pitch = 0.0;
                let mut valid_eyes = 0;

                for gazes in &latest_gaze[start_idx..end_idx] {
                    for gaze in gazes.into_iter().flatten() {
                        let (yaw, pitch, _roll) = gaze.to_euler(EulerRot::YXZ);
                        gaze_yaw += yaw;
                        gaze_pitch += pitch;
                        valid_eyes += 1;
                    }
                }

                if valid_eyes > 0 {
                    gaze_yaw /= valid_eyes as f32;
                    gaze_pitch /= valid_eyes as f32;
                }

                let norm_x = (gaze_yaw / max_yaw).clamp(-1.0, 1.0);
                let norm_y = (-gaze_pitch / max_pitch).clamp(-1.0, 1.0);

                let target_x = ((norm_x + 1.0) / 2.0 * range_x).round();
                let target_y = ((norm_y + 1.0) / 2.0 * range_y).round();

                // Build the flat additive string
                if f == frames_in_chunk - 1 {
                    // Replace 'n' with 'round(t*{framerate})'
                    expr_x.push_str(&format!("+gte(round(t*{}),{})*{:.0}", self.framerate, f, target_x));
                    expr_y.push_str(&format!("+gte(round(t*{}),{})*{:.0}", self.framerate, f, target_y));
                } else {
                    // Replace 'n' with 'round(t*{framerate})'
                    expr_x.push_str(&format!("+eq(round(t*{}),{})*{:.0}", self.framerate, f, target_x));
                    expr_y.push_str(&format!("+eq(round(t*{}),{})*{:.0}", self.framerate, f, target_y));
                }
            }
        }

        let filter_complex_foveation = if self.use_foveation {
            format!(
                "[0:v]scale={w}:{h}:force_original_aspect_ratio=disable,format=yuv420p[scaled]; \
                [scaled]split=2[bg][fg]; \
                [bg]boxblur=luma_radius=10:chroma_radius=10[blurred]; \
                [fg]crop=w={fw}:h={fh}:x='{expr_x}':y='{expr_y}', \
                    drawbox=x=0:y=0:w={fw}:h={fh}:color=red@0.8:t=4[sharp]; \
                [blurred][sharp]overlay=x='{expr_x}':y='{expr_y}'[foveated]; \
                [foveated]drawtext=\
                    fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf:\
                    text='%{{eif\\:n\\:d\\:5}}':start_number={start}:\
                    x=10:y=10:fontsize=96:fontcolor=white:box=1:boxcolor=black:boxborderw=30",
                w = self.width,
                h = self.height,
                fw = fovea_w,
                fh = fovea_h,
                expr_x = expr_x,
                expr_y = expr_y,
                start = start_frame_idx
            )
        } else {    
            format!(
                "scale={w}:{h}:force_original_aspect_ratio=disable,format=yuv420p,\
                drawtext=fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf: text='%{{eif\\:n\\:d\\:5}}': start_number={start}: x=10: y=10: fontsize=96: fontcolor=white: box=1: boxcolor=black: boxborderw=30",
                w = self.width, 
                h = self.height, 
                start = start_frame_idx
            )
        };

        let mut command = FfmpegCommand::new();
        // SVT-AV1 Arguments from av1_testbed.rs
        let mut child = command
            // .hwaccel("cuda")
            // .args(&["-ss", &self.current_offset.to_string()])
            .args(&["-ss", &format!("{:.6}", seek_offset)]) // Use high precision; wrapped into video bounds for looping
            .args(&["-t", &chunk_duration_clamped.to_string()]) // Clamped to avoid straddling the loop point
            // .args(&["-threads", &format!("{}", NUM_PARALLEL_THREADS_ENCODE)]) // Use const or hardcode
            .args(&["-threads", "8"])
            .args(&["-hide_banner", "-nostats", "-loglevel", "error"])
            .args(&["-stream_loop", "-1"]) // Loop the sample video indefinitely
            .input(&self.input)
            .args(&[
                    "-vf", &filter_complex_foveation,
                ])
            .args(&["-c:v", "libsvtav1"]) // Using SVT-AV1
            .args(&["-preset", "9"])      // High speed preset for RTC
            .args(&["-svtav1-params", "rc=2:lookahead=0:pred-struct=1:lp=3:tile-columns=3:tile-rows=1:fast-decode=1:include-td=1"]) // Tiling for fastness, lp: level of parallelism,
            // .args(&["-b:v", &self.bitrate, ])
            // .args(&["-bufsize", &self.bitrate])
            .args(&["-b:v", &self.bitrate]) 
            .args(&["-maxrate", &self.bitrate]) // MUST be added alongside bufsize
            .args(&["-bufsize", &bufsize_str]) // Updated VBV
            .args(&["-g", &format!("{}", self.gop_size)])
            // .args(&["-f", "ivf", "-"]) // IVF is standard for raw AV1 piping
            // .args(&["-intra-refresh", &format!("{}", self.intra_refresh as i32)]) // TODO: Test IR on AV1, don't have access to nvenc GPU 
            .args(&["-f", "obu", "-"]) 
            // .args(&["-f", "ivf", "-"]) // ivf is container for single frames, woth 12 byte header per frame

            .spawn()
            .unwrap();

        self.chunk_index += 1;    
        let stdout = child.take_stdout().unwrap();
        let mut reader = BufReader::new(stdout);    


        if DEBUG_FFMPEG_AV1_LOGS {
            if let Some(stderr) = child.take_stderr() {
                let mut err_reader = std::io::BufReader::new(stderr);
                std::thread::spawn(move || {
                    for line in err_reader.lines() {
                        if let Ok(l) = line {
                            println!("ffmpeg stderr: {}", l);
                        }
                    }
                });
            }
        }
        // Optional: Stderr handling similar to HEVC implementation

        self.aggregation_buffer.clear();
        let mut buf = [0u8; 4096];

        loop {
            match reader.read(&mut buf) {
                Ok(0) => {
                    // EOF: Flush any remaining data in the aggregation buffer
                    if !self.aggregation_buffer.is_empty() {
                        if let Err(e) = self.frame_tx.send(self.aggregation_buffer.clone()) {
                            eprintln!("Error sending last AV1 frame: {}", e);
                        }
                        self.aggregation_buffer.clear();
                    }
                    break;
                }
                Ok(n) => {
                    self.parser.add_data(&buf[..n]);

                    let units = self.parser.get_obu_units();
                    for unit in units {
                        // OBU Type 2 is OBU_TEMPORAL_DELIMITER.
                        // It marks the beginning of a NEW Temporal Unit (Frame).
                        if unit.obu_type == 2 {
                            // If we have data accumulated, it means the *previous* frame is done.
                            if !self.aggregation_buffer.is_empty() {
                                if let Err(e) = self.frame_tx.send(self.aggregation_buffer.clone())
                                {
                                    eprintln!("Error sending AV1 frame: {}", e);
                                }
                                self.aggregation_buffer.clear();
                            }
                        }

                        // Add current OBU to the buffer (it belongs to the frame starting now)
                        self.aggregation_buffer.extend_from_slice(&unit.data);
                    }
                }
                Err(e) => {
                    eprintln!("Error reading AV1 ffmpeg chunk: {}", e);
                    break;
                }
            }
        }
        let _ = child.wait();

        self.current_offset += self.chunk_duration;

        // Safety buffer clear
        if self.parser.buffer.len() > 100_000_000 {
            self.parser.buffer.clear();
        }
    }

    pub async fn next_frame(&mut self) -> Option<Vec<u8>> {
        // Logic identical to ChunkedHevcEncoder for consistency
        let extracted_frames = self.parser.get_frames();
        if !extracted_frames.is_empty() {
            for frame in extracted_frames.iter().skip(1) {
                self.frame_queue.push_back(frame.clone()); // Adapt .clone() if needed
            }
            return Some(extracted_frames[0].clone());
        }

        if let Some(frame) = self.frame_queue.pop_front() {
            return Some(frame);
        }

        if let Ok(frame) = self.frame_rx.recv_timeout(Duration::from_millis(1)) {
            return Some(frame);
        }

        None
    }
}

pub struct ChunkedSoftwareHevcEncoder {
    input: String,
    width: u32,
    height: u32,
    bitrate: String,
    framerate: f32,

    chunk_duration: f64,
    current_offset: f64,
    video_duration: f64, // real duration of `input`, used to wrap seeks when looping
    frame_tx: Sender<Vec<u8>>,
    frame_rx: Receiver<Vec<u8>>,

    frame_queue: VecDeque<Vec<u8>>,
    parser: HevcParser,
    encoder_str: String,
    gop_size: usize,
    intra_refresh: bool,
    chunk_index: usize, 
    use_foveation: bool, 
    vbv_perframe: bool, 
}

#[allow(unused)]
impl ChunkedSoftwareHevcEncoder {
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
        use_foveation: bool, 
        vbv_perframe: bool, 

    ) -> Self {
        println!("Initializing ChunkedSoftwareHevcEncoder (libx265)");
        let (frame_tx, frame_rx) = bounded(1000);
        let video_duration = probe_video_duration_secs(input);

        Self {
            input: input.to_string(),
            width,
            height,
            bitrate: bitrate.to_string(),
            framerate,
            chunk_duration,
            current_offset: offset_video,
            video_duration,
            frame_tx,
            frame_rx,
            frame_queue: VecDeque::new(),
            parser: HevcParser::new(),
            encoder_str: string.clone(),
            gop_size,
            intra_refresh,
            chunk_index: 0, 
            use_foveation, 
            vbv_perframe, 
        }
    }

    pub fn clear_buffers(&mut self) {
        self.parser.reset_for_new_chunk();
        self.frame_queue.clear();
        while let Ok(_) = self.frame_rx.try_recv() {}
    }

    pub fn clear_parser(&mut self) {
        self.parser.reset_for_new_chunk();
    }
    #[inline]
    pub async fn start_chunking(&mut self, bitrate_mbps: f32, now: TaiTime<0>, latest_gaze: Vec<[Option<Quat>; 2]>) {
        let bitrate_adjusted_fps = bitrate_mbps;
        self.bitrate = format!("{:.2}M", bitrate_adjusted_fps);

        let frames_per_chunk = (self.framerate * self.chunk_duration as f32).round() as usize;
        let exact_offset = self.current_offset;
        let start_frame_idx = (exact_offset * self.framerate as f64).round() as usize;
        let seek_offset = if self.video_duration > 0.0 {
            exact_offset % self.video_duration
        } else {
            exact_offset
        };
        // Clamp this chunk's read length so it never straddles the loop point:
        // reading across the wrap resets the source PTS mid-stream, which the
        // muxer rejects as non-monotonic DTS. A small safety margin (a couple of
        // frame durations) is subtracted because ffmpeg can roll over into the
        // next loop iteration slightly before the exact end-of-file timestamp.
        let loop_safety_margin = (2.0 / self.framerate as f64).max(0.02);
        let chunk_duration_clamped = if self.video_duration > 0.0 {
            self.chunk_duration.min((self.video_duration - seek_offset - loop_safety_margin).max(0.05))
        } else {
            self.chunk_duration
        };

        // let bufsize_kbits = (bitrate_mbps * 1000.0) / self.framerate;
        let bufsize_kbits = (  VBV_SETTING_RELAXATION_MULTIPLIER * bitrate_mbps * 1000.0) / self.framerate; // Calculate single-frame VBV buffer size to limit max frame size, as in 'How to model Cloud VR' paper by Korneev et al.
        // Relaxation multiplier because only one frame makes lower bitrates (<35 Mbps) generate frames much larger in average than the expected.
        let bufsize_str = if self.vbv_perframe{
            format!("{:.0}k", bufsize_kbits)
        }
        else{
            format!("{:.0}k", bitrate_mbps * 1000.0)
        };

        println!(
            "{} - {} SOFTWARE CHUNKING with bitrate {} Mbps",
            crate::format_elapsed!(now),
            self.encoder_str,
            bitrate_mbps,
        );
        self.parser.reset_for_new_chunk();

        let mut command = FfmpegCommand::new();
        
        let fovea_w = 1000;
        let fovea_h = 1000;
        
        // Convert to f32 for math
        let range_x = (self.width - fovea_w) as f32;
        let range_y = (self.height - fovea_h) as f32;

        let frames_in_chunk = (self.framerate * self.chunk_duration as f32).round() as usize;
        let gaze_count = latest_gaze.len();

        let mut expr_x = String::new();
        let mut expr_y = String::new();
        

        if gaze_count == 0 || !self.use_foveation || frames_in_chunk == 0 {
            // Fallback to absolute center if no data or foveation is off
            expr_x = format!("{:.0}", range_x / 2.0);
            expr_y = format!("{:.0}", range_y / 2.0);
        } else {
            // Start the additive string with 0
            expr_x.push_str("0");
            expr_y.push_str("0");

            let max_yaw = std::f32::consts::FRAC_PI_4; 
            let max_pitch = std::f32::consts::FRAC_PI_4;

            for f in 0..frames_in_chunk {
                // Time-slice mapping logic (using your existing method)
                let start_idx = ((f as f32 / frames_in_chunk as f32) * gaze_count as f32).floor() as usize;
                let mut end_idx = (((f + 1) as f32 / frames_in_chunk as f32) * gaze_count as f32).floor() as usize;
                end_idx = end_idx.clamp(start_idx + 1, gaze_count);

                let mut gaze_yaw = 0.0;
                let mut gaze_pitch = 0.0;
                let mut valid_eyes = 0;

                for gazes in &latest_gaze[start_idx..end_idx] {
                    for gaze in gazes.into_iter().flatten() {
                        let (yaw, pitch, _roll) = gaze.to_euler(EulerRot::YXZ);
                        gaze_yaw += yaw;
                        gaze_pitch += pitch;
                        valid_eyes += 1;
                    }
                }

                if valid_eyes > 0 {
                    gaze_yaw /= valid_eyes as f32;
                    gaze_pitch /= valid_eyes as f32;
                }

                let norm_x = (gaze_yaw / max_yaw).clamp(-1.0, 1.0);
                let norm_y = (-gaze_pitch / max_pitch).clamp(-1.0, 1.0);

                let target_x = ((norm_x + 1.0) / 2.0 * range_x).round();
                let target_y = ((norm_y + 1.0) / 2.0 * range_y).round();

                // Build the flat additive string
                if f == frames_in_chunk - 1 {
                    // Replace 'n' with 'round(t*{framerate})'
                    expr_x.push_str(&format!("+gte(round(t*{}),{})*{:.0}", self.framerate, f, target_x));
                    expr_y.push_str(&format!("+gte(round(t*{}),{})*{:.0}", self.framerate, f, target_y));
                } else {
                    // Replace 'n' with 'round(t*{framerate})'
                    expr_x.push_str(&format!("+eq(round(t*{}),{})*{:.0}", self.framerate, f, target_x));
                    expr_y.push_str(&format!("+eq(round(t*{}),{})*{:.0}", self.framerate, f, target_y));
                }
            }
        }

        let filter_complex_foveation = if self.use_foveation {
            format!(
                "[0:v]scale={w}:{h}:force_original_aspect_ratio=disable,format=yuv420p[scaled]; \
                [scaled]split=2[bg][fg]; \
                [bg]boxblur=luma_radius=10:chroma_radius=10[blurred]; \
                [fg]crop=w={fw}:h={fh}:x='{expr_x}':y='{expr_y}', \
                    drawbox=x=0:y=0:w={fw}:h={fh}:color=red@0.8:t=4[sharp]; \
                [blurred][sharp]overlay=x='{expr_x}':y='{expr_y}'[foveated]; \
                [foveated]drawtext=\
                    fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf:\
                    text='%{{eif\\:n\\:d\\:5}}':start_number={start}:\
                    x=10:y=10:fontsize=96:fontcolor=white:box=1:boxcolor=black:boxborderw=30",
                w = self.width,
                h = self.height,
                fw = fovea_w,
                fh = fovea_h,
                expr_x = expr_x,
                expr_y = expr_y,
                start = start_frame_idx
            )
        } else {    
            format!(
                "scale={w}:{h}:force_original_aspect_ratio=disable,format=yuv420p,\
                drawtext=fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf: text='%{{eif\\:n\\:d\\:5}}': start_number={start}: x=10: y=10: fontsize=96: fontcolor=white: box=1: boxcolor=black: boxborderw=30",
                w = self.width, 
                h = self.height, 
                start = start_frame_idx
            )
        };



        // Common arguments for both modes
        command
            .args(&["-ss", &format!("{:.6}", seek_offset)]) // Use high precision; wrapped into video bounds for looping
            // .args(&["-ss", &self.current_offset.to_string()])
            .args(&["-t", &chunk_duration_clamped.to_string()]) // Clamped to avoid straddling the loop point
            .args(&["-threads", "4"]) // Software encoding needs CPU threads
            .args(&["-hide_banner", "-nostats", "-loglevel", "error"])
            .args(&["-stats_period", "8"])
            .args(&["-stream_loop", "-1"]) // Loop the sample video indefinitely
            .input(&self.input)
            .args(&[
                    "-vf", &filter_complex_foveation ,
                ])
            .args(&["-c:v", "libx265"]) // SW Encoding
            .args(&["-preset", "ultrafast"]) // Crucial for realtime SW encoding
            .args(&["-tune", "zerolatency"]) // Minimize delay
            .args(&["-fps_mode", "passthrough"]);

        if self.intra_refresh {
            command
                .args(&["-analyzeduration", "200M"])
                .args(&["-probesize", "200M"])
                // Rate Control
                .args(&["-b:v", &self.bitrate, "-maxrate", &self.bitrate])
                .args(&["-bufsize", &bufsize_str]) // updated VBV
                .args(&["-rc-lookahead", "0"])
                // Structural args
                .args(&["-g", "0"]) // Let x265 params handle structure
                .args(&["-bf", "0"]) // No B-frames for intra-refresh
                // libx265 specific params for intra-refresh
                .args(&[
                    "-x265-params",
                    // "intra-refresh=1:keyint=30:min-keyint=30:pools=4",
                    "intra-refresh=1:keyint=30:min-keyint=30:pools=4:repeat-headers=1:aud=1:hrd=1:slices=1",
                ])
                // Container flags
                .args(&["-movflags", "+frag_keyframe+empty_moov"])
                .args(&["-flush_packets", "1"])
                .args(&["-bsf:v", "hevc_mp4toannexb"])
                .args(&["-an"])
                .args(&["-f", "hevc", "-"]);
        } else {
            command
                .args(&["-analyzeduration", "100M"])
                .args(&["-probesize", "100M"])
                .args(&["-s", &format!("{}x{}", self.width, self.height)])
                // Rate Control
                .args(&["-b:v", &self.bitrate, "-maxrate", &self.bitrate])
                .args(&["-bufsize", &bufsize_str]) // updated VBV
                // Structural args
                .args(&["-sc_threshold", "0"]) // Disable scene detection
                .args(&["-g", &format!("{:.0}", self.gop_size)])
                // libx265 specific params for Closed GOP
                .args(&[
                    "-x265-params",
                    &format!(
                        "no-open-gop=1:keyint={}:min-keyint={}:pools=4:slices=1",
                        self.gop_size, self.gop_size
                    ),
                ])
                // Container flags
                .args(&["-movflags", "+frag_keyframe+empty_moov"])
                .args(&["-flush_packets", "1"])
                .args(&["-bsf:v", "hevc_mp4toannexb"])
                .args(&["-an"])
                .args(&["-f", "hevc", "-"]);
        }
        self.chunk_index += 1;

        // Spawn the ffmpeg process
        let mut child = command.spawn().unwrap();
        let stdout = child.take_stdout().unwrap();
        let mut reader = BufReader::new(stdout);

        // Handle Stderr in separate thread
        if let Some(stderr) = child.take_stderr() {
            let mut err_reader = std::io::BufReader::new(stderr);
            std::thread::spawn(move || {
                for line in err_reader.lines() {
                    match line {
                        Ok(l) => println!("ffmpeg stderr: {}", l),
                        Err(e) => {
                            eprintln!("Error reading ffmpeg stderr: {}", e);
                            break;
                        }
                    }
                }
            });
        }

        let mut buf = [0u8; 4096];

        loop {
            match reader.read(&mut buf) {
                Ok(0) => break, // end of chunk
                Ok(n) => {
                    self.parser.add_data(&buf[..n]);
                    // Extract complete frames
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
        // ffmpeg exited: nothing more will complete the currently open frame's
        // boundary, so flush it now (discarded if it's only leftover parameter sets).
        if let Some(frame) = self.parser.flush_final_frame() {
            if let Err(e) = self.frame_tx.send(frame) {
                eprintln!("{} Error sending frame: {}", e, self.encoder_str,);
            }
        }

        let _ = child.wait();

        // Update offset
        self.current_offset += self.chunk_duration;

        // Safety check for parser buffer
        if self.parser.buffer.len() > 1_000_000_00 {
            println!(
                "{} Parser buffer getting too large ({}), clearing",
                self.parser.buffer.len(),
                self.encoder_str,
            );
            self.parser.buffer.clear();
        }
    }

    #[inline]
    pub async fn next_frame(&mut self) -> Option<Vec<u8>> {
        // First try parser's frames
        let extracted_frames = self.parser.get_frames();
        if !extracted_frames.is_empty() {
            // Store all but first frame for future use
            for frame in extracted_frames.iter().skip(1) {
                self.frame_queue.push_back(frame.clone());
            }
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

pub struct ChunkedHevcEncoder {
    input: String,
    width: u32,
    height: u32,
    bitrate: String,
    framerate: f32,
    chunk_duration: f64,
    current_offset: f64,
    video_duration: f64, // real duration of `input`, used to wrap seeks when looping
    frame_tx: Sender<Vec<u8>>,
    frame_rx: Receiver<Vec<u8>>,

    frame_queue: VecDeque<Vec<u8>>,
    parser: HevcParser,
    encoder_str: String,
    gop_size: usize,
    intra_refresh: bool,
    chunk_index: usize,
    use_foveation: bool,
    vbv_perframe: bool,
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
        gop_size: usize,
        intra_refresh: bool,
        use_foveation: bool, 
        vbv_perframe: bool, 

    ) -> Self {
        println!("Initializing chunkedhevcencoder");
        let (frame_tx, frame_rx) = bounded(1000);
        let video_duration = probe_video_duration_secs(input);

        Self {
            input: input.to_string(),
            width,
            height,
            bitrate: bitrate.to_string(),
            framerate,
            chunk_duration,
            current_offset: offset_video,
            video_duration,
            frame_tx,
            frame_rx,
            frame_queue: VecDeque::new(),
            parser: HevcParser::new(),
            encoder_str: string.clone(),
            gop_size,
            intra_refresh,
            chunk_index: 0, 
            use_foveation, 
            vbv_perframe, 
        }
    }

    pub fn clear_buffers(&mut self) {
        self.parser.reset_for_new_chunk();
        self.frame_queue.clear();
        while let Ok(_) = self.frame_rx.try_recv() {}
    }

    pub fn clear_parser(&mut self) {
        self.parser.reset_for_new_chunk();
    }

    /// Continuously spawn ffmpeg processes to produce video chunks.
    /// Each process is configured to start at the current_offset and run for chunk_duration seconds.
    /// As data is read from ffmpeg’s stdout, it is fed to a HevcParser which extracts complete frames.
    /// Each complete frame is sent via the async channel.
    #[inline]
    pub async fn start_chunking(&mut self, bitrate_mbps: f32, now: TaiTime<0>, latest_gaze:Vec<[Option<Quat>; 2]>) {
        // let bitrate_adjusted_fps = bitrate_mbps * FRAMERATE_WINDOWS as f32 / self.framerate;

        let bitrate_adjusted_fps = bitrate_mbps;
        // Since the encoded video samples are 60fps, we thus adjust bitrate to match with the actual second units.

        self.bitrate = format!("{:.2}M", bitrate_adjusted_fps);

        let frames_per_chunk = (self.framerate * self.chunk_duration as f32).round() as usize;
        // let start_frame_idx: usize = self.chunk_index * frames_per_chunk;
        // let exact_offset = start_frame_idx as f64 / self.framerate as f64;

        let exact_offset: f64 = self.current_offset;
        let start_frame_idx = (exact_offset * self.framerate as f64).round() as usize;
        let seek_offset = if self.video_duration > 0.0 {
            exact_offset % self.video_duration
        } else {
            exact_offset
        };
        // Clamp this chunk's read length so it never straddles the loop point:
        // reading across the wrap resets the source PTS mid-stream, which the
        // muxer rejects as non-monotonic DTS. A small safety margin (a couple of
        // frame durations) is subtracted because ffmpeg can roll over into the
        // next loop iteration slightly before the exact end-of-file timestamp.
        let loop_safety_margin = (2.0 / self.framerate as f64).max(0.02);
        let chunk_duration_clamped = if self.video_duration > 0.0 {
            self.chunk_duration.min((self.video_duration - seek_offset - loop_safety_margin).max(0.05))
        } else {
            self.chunk_duration
        };

        let bufsize_str = if self.vbv_perframe{
            let bufsize_kbits = (  VBV_SETTING_RELAXATION_MULTIPLIER * bitrate_mbps * 1000.0) / self.framerate; // Calculate single-frame VBV buffer size to limit max frame size, as in 'How to model Cloud VR' paper by Korneev et al.
            format!("{:.0}k", bufsize_kbits)
        }
        else{
            format!("{:.0}k", bitrate_mbps * 1000.0)
        };

        println!(
            "{} - {} CHUNKING with bitrate {} Mbps",
            crate::format_elapsed!(now),
            self.encoder_str,
            bitrate_mbps,
        );
        self.parser.reset_for_new_chunk();

        let fovea_w = 1000;
        let fovea_h = 1000;
        
        // Convert to f32 for math
        let range_x = (self.width - fovea_w) as f32;
        let range_y = (self.height - fovea_h) as f32;

        let frames_in_chunk = (self.framerate * self.chunk_duration as f32).round() as usize;
        let gaze_count = latest_gaze.len();

        let mut expr_x = String::new();
        let mut expr_y = String::new();
        

        if gaze_count == 0 || !self.use_foveation || frames_in_chunk == 0 {
            // Fallback to absolute center if no data or foveation is off
            expr_x = format!("{:.0}", range_x / 2.0);
            expr_y = format!("{:.0}", range_y / 2.0);
        } else {
            // Start the additive string with 0
            expr_x.push_str("0");
            expr_y.push_str("0");

            let max_yaw = std::f32::consts::FRAC_PI_4; 
            let max_pitch = std::f32::consts::FRAC_PI_4;

            for f in 0..frames_in_chunk {
                // Time-slice mapping logic (using your existing method)
                let start_idx = ((f as f32 / frames_in_chunk as f32) * gaze_count as f32).floor() as usize;
                let mut end_idx = (((f + 1) as f32 / frames_in_chunk as f32) * gaze_count as f32).floor() as usize;
                end_idx = end_idx.clamp(start_idx + 1, gaze_count);

                let mut gaze_yaw = 0.0;
                let mut gaze_pitch = 0.0;
                let mut valid_eyes = 0;

                for gazes in &latest_gaze[start_idx..end_idx] {
                    for gaze in gazes.into_iter().flatten() {
                        let (yaw, pitch, _roll) = gaze.to_euler(EulerRot::YXZ);
                        gaze_yaw += yaw;
                        gaze_pitch += pitch;
                        valid_eyes += 1;
                    }
                }

                if valid_eyes > 0 {
                    gaze_yaw /= valid_eyes as f32;
                    gaze_pitch /= valid_eyes as f32;
                }

                let norm_x = (gaze_yaw / max_yaw).clamp(-1.0, 1.0);
                let norm_y = (-gaze_pitch / max_pitch).clamp(-1.0, 1.0);

                let target_x = ((norm_x + 1.0) / 2.0 * range_x).round();
                let target_y = ((norm_y + 1.0) / 2.0 * range_y).round();

                // Build the flat additive string
                if f == frames_in_chunk - 1 {
                    // Replace 'n' with 'round(t*{framerate})'
                    expr_x.push_str(&format!("+gte(round(t*{}),{})*{:.0}", self.framerate, f, target_x));
                    expr_y.push_str(&format!("+gte(round(t*{}),{})*{:.0}", self.framerate, f, target_y));
                } else {
                    // Replace 'n' with 'round(t*{framerate})'
                    expr_x.push_str(&format!("+eq(round(t*{}),{})*{:.0}", self.framerate, f, target_x));
                    expr_y.push_str(&format!("+eq(round(t*{}),{})*{:.0}", self.framerate, f, target_y));
                }
            }
        }

        let filter_complex_foveation = if self.use_foveation {
            format!(
                "[0:v]scale={w}:{h}:force_original_aspect_ratio=disable,format=yuv420p[scaled]; \
                [scaled]split=2[bg][fg]; \
                [bg]boxblur=luma_radius=10:chroma_radius=10[blurred]; \
                [fg]crop=w={fw}:h={fh}:x='{expr_x}':y='{expr_y}', \
                    drawbox=x=0:y=0:w={fw}:h={fh}:color=red@0.8:t=4[sharp]; \
                [blurred][sharp]overlay=x='{expr_x}':y='{expr_y}'[foveated]; \
                [foveated]drawtext=\
                    fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf:\
                    text='%{{eif\\:n\\:d\\:5}}':start_number={start}:\
                    x=10:y=10:fontsize=96:fontcolor=white:box=1:boxcolor=black:boxborderw=30",
                w = self.width,
                h = self.height,
                fw = fovea_w,
                fh = fovea_h,
                expr_x = expr_x,
                expr_y = expr_y,
                start = start_frame_idx
            )
        } else {    
            format!(
                "scale={w}:{h}:force_original_aspect_ratio=disable,format=yuv420p,\
                drawtext=fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf: text='%{{eif\\:n\\:d\\:5}}': start_number={start}: x=10: y=10: fontsize=96: fontcolor=white: box=1: boxcolor=black: boxborderw=30",
                w = self.width, 
                h = self.height, 
                start = start_frame_idx
            )
        };
    

        let mut command = FfmpegCommand::new();
        if self.intra_refresh {
            command
                .hwaccel("cuda")
                .args(&["-analyzeduration", "200M"])
                .args(&["-probesize", "200M"])
                .args(&["-ss", &format!("{:.6}", seek_offset)]) // Use high precision; wrapped into video bounds for looping
                .args(&["-t", &chunk_duration_clamped.to_string()]) // Clamped to avoid straddling the loop point
                .args(&["-threads", "2"])
                .args(&["-hide_banner", "-nostats", "-loglevel", "error"])
                .args(&["-stats_period", "8"])
                // .args(&["-re"]) // read at real-time speed
                .args(&["-stream_loop", "-1"]) // Loop the sample video indefinitely
                .input(&self.input)
                .args(&[
                    "-filter_complex", &filter_complex_foveation,
                ])
                .args(&["-c:v", "hevc_nvenc"])
                .args(&["-preset", "fast"])
                .args(&["-fps_mode", "passthrough"])
                // .args(&["-preset", "llhq"])
                .args(&["-rc", "cbr"])
                .args(&["-b:v", &self.bitrate, "-maxrate", &self.bitrate])
                .args(&["-bufsize", &bufsize_str]) // per-frame window( keep it tight)
                // the throughput distribution will match that of the bitrate target strictly by padding.
                .args(&["-rc-lookahead", "0"])
                .args(&["-g", "0"]) // Disable GOP, intra-refresh instead
                .args(&["-bf", "0"]) // force zero B-frames for PIR to work
                .args(&["-intra-refresh", "1"]) // Enable intra-refresh coding
                // .args(&["-intra-refresh-period", &format!("{:.0}, ", self.gop_size) ])
                .args(&["-movflags", "+frag_keyframe+empty_moov"])
                .args(&["-flush_packets", "1"])
                .args(&["-bsf:v", "hevc_mp4toannexb"])
                .args(&["-an"])
                .args(&["-f", "hevc", "-"]); // output raw HEVC
        } else {
            command
                .hwaccel("cuda")
                .args(&["-ss", &format!("{:.6}", seek_offset)]) // Use high precision; wrapped into video bounds for looping
                .args(&["-t", &chunk_duration_clamped.to_string()]) // Clamped to avoid straddling the loop point
                // .args(&["-re"]) // read at realtime speed
                .args(&["-analyzeduration", "100M"])
                .args(&["-probesize", "100M"])
                .args(&["-threads", "2"])
                .args(&["-hide_banner", "-nostats", "-loglevel", "error"])
                .args(&["-stats_period", "5"])
                .args(&["-stream_loop", "-1"]) // Loop the sample video indefinitely
                .input(&self.input)
                .args(&[
                    "-filter_complex", &filter_complex_foveation,
                ])
                .args(&["-c:v", "hevc_nvenc"])
                .args(&["-preset", "fast"]) // TODO : llhq is preferrable but deprecated on some of the HPC GPUs.
                .args(&["-fps_mode", "passthrough"])
                .args(&["-s", &format!("{}x{}", self.width, self.height)]) // Force input resolution
                .args(&["-flags", "+cgop"]) // 1. Force Closed GOP (No referencing frames outside the GOP)
                // .args(&["-forced-idr", "1"])                     // 2. NVENC specific: Force the start to be an IDR frame. NOTE: not using this, better to make GoP frequency match T_abr
                .args(&["-sc_threshold", "0"]) // 3. Disable scene change detection (keeps GOP strict)
                .args(&["-rc", "cbr"])
                .args(&["-b:v", &self.bitrate, "-maxrate", &self.bitrate])
                .args(&["-bufsize", &bufsize_str]) // per-frame VBV window (keeps it tight)
                .args(&["-rc-lookahead", "0"])
                .args(&["-g", &format!("{:.0}", self.gop_size)]) // using your GOP size constant
                .args(&["-movflags", "+frag_keyframe+empty_moov"])
                .args(&["-flush_packets", "1"])
                .args(&["-bsf:v", "hevc_mp4toannexb"])
                .args(&["-an"])
                .args(&["-f", "hevc", "-"]); // output raw HEVC
        }
        self.chunk_index +=1; 
        // Spawn the ffmpeg process for this chunk.
        let mut child = command.spawn().unwrap();
        let stdout = child.take_stdout().unwrap();
        let mut reader = BufReader::new(stdout);

        if let Some(stderr) = child.take_stderr() {
            let mut err_reader = std::io::BufReader::new(stderr);
            std::thread::spawn(move || {
                for line in err_reader.lines() {
                    match line {
                        Ok(l) => println!("ffmpeg stderr: {}", l),
                        Err(e) => {
                            eprintln!("Error reading ffmpeg stderr: {}", e);
                            break;
                        }
                    }
                }
            });
        }

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
        // ffmpeg exited: nothing more will complete the currently open frame's
        // boundary, so flush it now (discarded if it's only leftover parameter sets).
        if let Some(frame) = self.parser.flush_final_frame() {
            if let Err(e) = self.frame_tx.send(frame) {
                eprintln!("{} Error sending frame: {}", e, self.encoder_str,);
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

    #[inline]
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

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct FaceData {
    pub eye_gazes: [Option<Pose>; 2],
    pub fb_face_expression: Option<Vec<f32>>, // issue: Serialize does not support [f32; 63]
    pub htc_eye_expression: Option<Vec<f32>>,
    pub htc_lip_expression: Option<Vec<f32>>, // issue: Serialize does not support [f32; 37]
}

// Note: face_data does not respect target_timestamp.
#[derive(Serialize, Deserialize, Default, Clone, Debug)]
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

    pub last_tx_time: f32,
    // TaiTime<0>,
    pub last_rx_time: f32,
    // TaiTime<0>,
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
            last_tx_time: 0.0,
            last_rx_time: 0.0,
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
    frame_deadline: Option<TaiTime<0>>,
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

    tx_instant_packet: f32, // used for NADA ABR in XRClient connection loop
    rx_instant_packet: f32,
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
    tx_instant_packet: f32,
    rx_instant_packet: f32,
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

    pub fn get_tx_time_first(&self) -> f32 {
        self.tx_instant_packet
    }
    pub fn get_rx_time_last(&self) -> f32 {
        self.rx_instant_packet
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
    pub video_chunk_duration: f32,
}
#[allow(unused)]
impl StreamSocket {
    pub fn request_stream<T>(
        &self,
        stream_id: u16,
        t0: TaiTime<0>,
        codec_selection: VideoCodec,
        results_path: &str, 
    ) -> StreamSender<T> {
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
            csv_trace: OldCsvTrace::default(),
            tmp_buf: Vec::new(),
            last_lo: Cell::new(0),
            last_hi: Cell::new(1),
            video_chunk_duration: self.video_chunk_duration,
            codec_selection,
            results_path: results_path.to_string(), 
            emu_frame_table: None, 
            // col_cache: HashMap::new(),
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
        let cap = self.lost_shards_deadline_map.len();
        let mut vec_keys = Vec::with_capacity(cap);
        let mut vec_lost = Vec::with_capacity(cap);

        // Drain empties the map while yielding (key, value) pairs.
        for (frame_deadlined, lost_in_frame) in self.lost_shards_deadline_map.drain() {
            // crate::print_red!(
            //     "[Flush deadline {}] Packets lost in frame {}: {:?}",
            //     self.
            //     frame_deadlined,
            //     lost_in_frame
            // );
            vec_keys.push(frame_deadlined);
            vec_lost.push(lost_in_frame);
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

            // Deadline is anchored to when the *server generated the frame* (`tx_r_instant`,
            // stamped once per frame in `StreamSender::send` from the same TaiTime<0> epoch),
            // not to when this shard happened to arrive here. Using arrival time would let
            // network/queueing delay on the first shard silently push the deadline out,
            // giving a frame that already limped in late even more budget than an earlier
            // arrival would get.
            let frame_gen_instant = TaiTime::<0>::EPOCH
                .checked_add(Duration::from_secs_f32(tx_r_instant.max(0.0)));

            self.shard_recv_state.insert(RecvState {
                shard_length,
                stream_id,
                packet_index,
                shards_count,
                shard_index,
                packet_cursor: 0,
                overwritten_data_backup: None,
                should_discard: false,
                frame_deadline: frame_gen_instant.and_then(|t| t.checked_add(DEADLINE_PACKETS_S)),
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

        // print_prettyy!( DebugColor::Orange, "{:.9} [DBG Socket RX {}] F: {}, S:{:2.0}/{:2.0} |deadline_current: {:?}|in_progress_packets: {:?}| indices {:?}| " ,format_elapsed!(now),ip_client ,shard_recv_state_mut.packet_index, shard_recv_state_mut.shard_index, shard_recv_state_mut.shards_count - 1 ,format_elapsed!(shard_recv_state_mut.frame_deadline.unwrap()), components.in_progress_packets.len(), components.in_progress_packets.keys());

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
                    // println!("First fallback");
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
                    // println!(
                    //     "Warning: Creating new emergency buffer - consider increasing buffer pool"
                    // ); // too alar
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
                    deadline: shard_recv_state_mut.frame_deadline,
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

                    if self.prev_frame_rx_instant == TaiTime::EPOCH {
                        frame_interarrival = Duration::ZERO.as_secs_f32(); // prevent very high values at begginning of simulation.
                    } else {
                        frame_interarrival = min_time
                            .checked_duration_since(self.prev_frame_rx_instant)
                            .unwrap_or(Duration::ZERO)
                            .as_secs_f32();
                    }

                    // Use min_time (start-of-frame) consistently on both sides of this
                    // measurement, mirroring the single reference point used on the encoder
                    // side (report_encoded_frame_server). Previously this was overwritten with
                    // max_time (end-of-frame) right after, so every sample actually measured
                    // min_time(N) - max_time(N-1) = (true inter-frame period) - (previous
                    // frame's frame_span) — systematically shrinking frame_interarrival and
                    // inflating the derived rx fps above the real tx fps.
                    self.prev_frame_rx_instant = min_time;

                    all_bytes_in_frame = values.iter().map(|shard| shard.rx_bytes).sum();
                    all_bytes_in_frame_app = values.iter().map(|shard| shard.rx_bytes_app).sum();

                    // println!("SEND COMPLETE PACKET!");

                    // One way delay gradient
                    if let Some(first_shard_stats) = inner_map.get(&0) {
                        if let Some(prev_frame_tx_r_instant) = self.prev_frame_tx_r_instant {
                            self.kalman.ow_delay = frame_interarrival
                                - (first_shard_stats.tx_r_instant - prev_frame_tx_r_instant);

                            self.kalman.last_tx_time = first_shard_stats.tx_r_instant;
                            self.kalman.last_rx_time = prev_frame_tx_r_instant;
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

                tx_instant_packet: self.kalman.last_tx_time, // used for NADA ABR in XRClient connection loop
                rx_instant_packet: self.kalman.last_rx_time, // used for NADA ABR in XRClient connection loop
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

        if ALVR_ORIGINAL_SOCKETRX_BEHAVIOR {
            // Keep only shards with later packet index (using wrapping logic)
            while let Some((idx, inprog)) =
                components.in_progress_packets.iter().find(|(idx, _)| {
                    wrapping_cmp(**idx, shard_recv_state_mut.packet_index) == Ordering::Less
                })
            {
                debug_bgprint!(
                    DebugColor::DarkOrange,
                    "idx {} discarded because {} already found ",
                    idx,
                    shard_recv_state_mut.packet_index
                );
                let mut editprog = inprog.clone();
                let idx = *idx; // fix borrow rule
                let packet = components.in_progress_packets.remove(&idx).unwrap();

                let shards_lost =
                    editprog.num_shards_expected - editprog.received_shard_indices.len();
                self.lost_shards_deadline_map.insert(idx, shards_lost);
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
    pub fn build(self, max_packet_size: usize, video_chunk_duration: f32) -> StreamSocket {
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
                    video_chunk_duration,
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
        video_chunk_duration: f32,
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
            video_chunk_duration,
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
        video_chunk_duration: f32,
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
            video_chunk_duration,
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
        video_chunk_duration: f32,
    ) -> Result<StreamSocket> {
        let (sender, receiver) = buffered_channel();

        Ok(StreamSocketBuilder::Channel(sender, receiver).build(packet_size, video_chunk_duration))
    }

    pub fn accept_from_server_mod(
        server_ip: IpAddr,
        port: u16,
        packet_size: usize,
        video_chunk_duration: f32,
    ) -> Result<StreamSocket> {
        // let (send_socket, receive_socket): (Box<dyn SocketWriter>, Box<dyn SocketReader>) = match self {
        //     StreamSocketBuilder::Channel(sender, receiver) => {
        //         let protocol = SocketProtocol::Channel;
        //         (Box::new(sender), Box::new(receiver))
        //     }
        // };

        let (sender, receiver) = buffered_channel();

        Ok(StreamSocketBuilder::Channel(sender, receiver).build(packet_size, video_chunk_duration))
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

            tx_instant_packet: packet.tx_instant_packet,
            rx_instant_packet: packet.rx_instant_packet,
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
    pub ffmpeg_encoder: Option<Arc<async_std::sync::Mutex<ChunkedEncoder>>>,
    // pub ffmpeg_maxbitrate_encoder: Option<Arc<async_std::sync::Mutex<ChunkedHevcEncoder>>>,

    // Keep the initialization flag:
    pub time_since_last_update: TaiTime<0>,

    csv_trace: OldCsvTrace,
    tmp_buf: Vec<u8>,
    // col_cache: HashMap<u32, usize>,
    last_lo: Cell<usize>,
    last_hi: Cell<usize>,

    pub video_chunk_duration: f32,

    pub codec_selection: VideoCodec,
    pub results_path: String, 
    pub emu_frame_table: Option<Arc<FrameSizeTable>>,

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
    #[inline]
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
        gop_size: usize,
        intra_refresh: bool,
        use_foveation: bool, 
        vbv_perframe: bool, 
        deterministic_frame_sizes_bool: bool, 
        latest_gazes: Vec<[Option<glam::Quat>; 2]>, 
    ) -> Result<Buffer<H>> {
        let _id_frame_files_ref = id_frame + 1;

        // Decide the suffix based on framerate
        let fps_suffix: &'static str = match framerate.round() as u32 {
            60 => "_60fps.mp4",
            90 => "_90fps.mp4",
            120 => "_120fps.mp4",
            _ => "", // default: leave as-is if unexpected fps
        };
        // Compose filename with suffix
        let file_with_fps;

        // if final_file.contains("snow")  || final_file == "swordsmith" || final{
        file_with_fps = format!("{final_file}{fps_suffix}");


        let input_path = get_prefix_path(&format!("video_samples_vmaf/{}", file_with_fps));
        let mut buffer: Vec<u8> = Vec::new();

        if USE_FFMPEG_DEMO {
            if self.ffmpeg_encoder.is_none() {
                // Create a new ChunkedHevcEncoder
                let bitrate_cmd = format!("{:.0}M", current_bitrate_mbps);
                let random_offset = rand::thread_rng().gen_range(1.0..OFFSET_VIDEO);
                // let random_offset = OFFSET_VIDEO;

                let third_octet = get_third_octet(ip).unwrap();

                if self.csv_trace.path.as_os_str().is_empty() {
                    // one CSV per run – put it next to the hevc files, but anywhere is fine
                    let csv_path = get_prefix_path(&format!(
                        "{}/{}/trace_offline_video{}.csv",
                        self.results_path ,name_folder, third_octet,
                    ));

                    let csv_path_emu = get_prefix_path(&format!(
                        "{}/{}/trace_emu_effects{}.csv",
                        self.results_path ,name_folder, third_octet,
                    ));
                    print_green!("Creating OFFLINE CSV at: {csv_path}",);

                    let mut wtr = Writer::from_path(&csv_path)?;
                    let mut wtr2 = Writer::from_path(&csv_path_emu)?;

                    wtr.write_record(&[
                        "OFFSET_VIDEO",
                        "PATH_VIDEO",
                        "IDR_FREQUENCY",
                        // "Intrarefresh_enabled",
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
                        format!("{random_offset:.4}"), // offset used for this run
                        input_path.to_owned(),         // source clip
                        format!("{}", gop_size),
                        // json,
                        "".to_string(), // placeholder timestamp
                        "".to_string(), // placeholder id_f
                        "".to_string(), // placeholder lost
                        "".to_string(),
                    ])?;

                    wtr.flush()?;
                    wtr2.flush()?;
                    self.csv_trace.path = csv_path.into();
                }

                // Assume `selected_codec` is of type VideoCodec::AV1 or VideoCodec::HEVC
                let mut encoder = match self.codec_selection {
                    VideoCodec::HEVC => ChunkedEncoder::HevcSoftware(ChunkedSoftwareHevcEncoder::new(
                        &input_path,
                        WIDTH_ENCODER as u32,
                        HEIGHT_ENCODER as u32,
                        &bitrate_cmd,
                        self.video_chunk_duration as f64, // Chunk duration in seconds
                        format!("[HEVC ENCODER {}]", ip),
                        random_offset,
                        framerate,
                        gop_size,
                        intra_refresh,
                        use_foveation, 
                        vbv_perframe, 
                    )),
                    VideoCodec::AV1 => ChunkedEncoder::Av1(ChunkedAv1Encoder::new(
                        &input_path,
                        WIDTH_ENCODER as u32,
                        HEIGHT_ENCODER as u32,
                        &bitrate_cmd,
                        self.video_chunk_duration as f64, // Chunk duration in seconds
                        format!("[AV1 ENCODER {}]", ip),
                        OFFSET_VIDEO,
                        framerate,
                        gop_size,
                        intra_refresh,
                        use_foveation, 
                        vbv_perframe, 
                    )),
                };

                // Wrap the encoder in an Arc<Mutex<_>>
                let encoder_arc = Arc::new(async_std::sync::Mutex::new(encoder));
                {
                    let mut encoder: async_std::sync::MutexGuard<'_, ChunkedEncoder> =
                        encoder_arc.lock().await;
                    encoder.start_chunking(current_bitrate_mbps, now, latest_gazes.clone()).await;
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
                    }
                    None => {
                        // print_pretty!(
                        //     DebugColor::SaddleBrown,
                        //     "No frame available, restarting encoder",
                        // );
                        encoder.clear_buffers();
                        // Restart chunking
                        encoder.start_chunking(current_bitrate_mbps, now, latest_gazes).await;
                        // Try again after waiting
                        match encoder.next_frame().await {
                            Some(frame) => {
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
        } 
        else {             // non-FFMPEG mode, fast!
            if self.csv_trace.path.as_os_str().is_empty() {
                let third_octet = get_third_octet(ip).unwrap();

                let csv_path_emu = get_prefix_path(&format!(
                    "{}/{}/trace_emu_effects{}.csv",
                    self.results_path ,name_folder, third_octet,
                ));

                let parent_dir = std::path::Path::new(&csv_path_emu)
                    .parent()
                    .ok_or_else(|| anyhow::anyhow!("CSV path has no parent: {}", csv_path_emu))?;

                
                // std::fs::create_dir_all(parent_dir)
                //     .map_err(|e| anyhow::anyhow!("Failed to create results directory: {}", e))?;

                if let Err(e) = std::fs::create_dir_all(parent_dir) {
                    if e.kind() == std::io::ErrorKind::PermissionDenied {
                        crate::print_red!("PERMISSION DENIED for {:?}. Falling back to local ./results", parent_dir);
                        
                        // Construct a fallback path in the current working directory
                        let fallback_path = format!("./results_fallback/{}/{}", name_folder, third_octet);
                        std::fs::create_dir_all(&fallback_path)
                            .map_err(|e2| anyhow::anyhow!("Total failure: Could not create original OR fallback directory. Err: {}", e2))?;
                        
                        // Update the csv_path to use the fallback
                        let new_csv_path = format!("{}/trace_emu_effects{}.csv", fallback_path, third_octet);
                        self.csv_trace.path = new_csv_path.into();
                    } else {
                        return Err(anyhow::anyhow!("Failed to create results directory: {}", e));
                    }
                }
                // Try to atomically create the file and write headers only if we created it.
                match std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true) // <- atomic create, fails if file exists
                    .open(&csv_path_emu)
                {
                    Ok(file) => {
                        // we created the file: write headers and the patterns
                        let mut wtr2 = Writer::from_writer(file);
                        wtr2.write_record(NetworkPattern::csv_headers())?;
                        for emu in network_effects {
                            wtr2.write_record(emu.to_csv_row())?;
                        }
                        wtr2.flush()?;
                        print_green!("Created EMU EFFECTS CSV at (new): {csv_path_emu}",);
                        self.csv_trace.path = csv_path_emu.into();
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                        // another thread/process already created it, just open for append if you need to
                        // or skip entirely since headers already exist
                        let _file = std::fs::OpenOptions::new()
                            .write(true)
                            .append(true)
                            .open(&csv_path_emu)
                            .map_err(|e| anyhow::anyhow!("Failed to open existing CSV: {}", e))?;

                            self.csv_trace.path = csv_path_emu.into(); // update the path and skip this block


                        // print_green!("EMU EFFECTS CSV already exists; opened existing: {csv_path_emu}", );
                    }
                    Err(e) => return Err(anyhow::anyhow!("Failed to create/open CSV: {}", e)),
                }

                // finally set the marker so this instance will not try to create again
            }

            let fps = framerate.round() as u32;

            if deterministic_frame_sizes_bool{
                buffer = generate_fibonacci_video_payload(current_bitrate_mbps, framerate);
            }
            else{
                let bytes_this_frame = if USE_HARDCODED_SIZES_VALIDATION {
                
                let csv_path = format!("csv_framesizes/ALVR_session_framesizes_{}fps_100Mbps.csv", fps);
                let hardcoded_table = Arc::new(
                    HardcodedFrameTable::load(&csv_path).expect("Failed to load hardcoded CSV frame sizes")
                );
                let bytes = hardcoded_table.get_bytes(id_frame); 
                crate::print_dblue!("ALVR VALIDATION MODE: Frame size: {}", bytes); 
                bytes
            } else {
                // --- OLD MODE: Bitrate interpolation ---
                let fps = framerate.round() as u32;
                // let table = get_table(final_file, fps, self.codec_selection)?; // global cached

                let table = if let Some(t) = &self.emu_frame_table {
                    t.clone()
                } else {
                    let t = get_table(final_file, fps, self.codec_selection, vbv_perframe, intra_refresh, use_foveation)?;
                    self.emu_frame_table = Some(t.clone());
                    t
                };


                
                table.bytes_interp_cached(
                    current_bitrate_mbps as f32,
                    id_frame,
                    &self.last_lo,
                    &self.last_hi,
                )
            };

            buffer.clear(); // Sets length to 0, but keeps capacity (no deallocation)
            buffer.resize(bytes_this_frame, 7); // Fills with 7s (for good luck). Very fast if capacity is sufficient.
            }
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

// use once_cell::sync::OnceCell;

//// NEW code for reading CSV of frame sizes, in order to emulate video transmission.
// #[derive(Clone)]
// struct FrameSizeTable {
//     fps: u32,
//     // available Mbps columns, e.g. [5,10,15,...,100]
//     mbps_cols: Vec<u32>,
//     // framesizes[col_idx][frame_idx] -> bytes
//     framesizes: Vec<Vec<usize>>,
// }

// impl FrameSizeTable {
//     fn load(final_file: &str, fps: u32) -> anyhow::Result<Self> {
//         let path = get_prefix_path(&format!(
//             "csv_framesizes/{}_{}fps_fused_framesizes.csv",
//             final_file, fps
//         ));
//         if !std::path::Path::new(&path).exists() {
//             return Err(anyhow::anyhow!("Frame-size CSV not found: {}", path));
//         }

//         let mut rdr = ReaderBuilder::new().has_headers(true).from_path(&path)?;
//         let headers = rdr.headers()?.clone();

//         // Parse Mbps column names just once -> integers
//         // headers[0] is "frame_index"; the rest like "5Mbps", "10Mbps", ...
//         let mut mbps_cols = Vec::new();
//         for h in headers.iter().skip(1) {
//             // fast parse: strip "Mbps" suffix
//             let m = h.trim_end_matches("Mbps")
//                      .parse::<u32>()
//                      .map_err(|_| anyhow::anyhow!("Bad column name: {}", h))?;
//             mbps_cols.push(m);
//         }

//         // Preallocate vectors (one per column)
//         let ncols = mbps_cols.len();
//         let mut framesizes: Vec<Vec<usize>> = vec![Vec::new(); ncols];

//         // Read rows once; push ints (bytes) directly
//         for rec in rdr.records() {
//             let rec = rec?;
//             for (ci, _) in mbps_cols.iter().enumerate() {
//                 let v = rec.get(ci + 1).unwrap_or("0"); // +1 to skip frame_index
//                 let b = v.parse::<usize>().unwrap_or(0);
//                 framesizes[ci].push(b);
//             }
//         }

//         Ok(Self { fps, mbps_cols, framesizes })
//     }

//     #[inline]
//     fn column_for_mbps(&self, mbps: u32) -> Option<usize> {
//         // linear scan is fine for ~20 cols; binary_search if you prefer:
//         self.mbps_cols.iter().position(|&x| x == mbps)
//     }

//     #[inline]
//     fn bytes(&self, col_idx: usize, frame_idx: usize) -> usize {
//         let vec = &self.framesizes[col_idx];
//         if vec.is_empty() { 0 } else { vec[frame_idx % vec.len()] }
//     }
// }

// // Global cache keyed by (final_file,fps)
// static TABLE_CACHE: OnceCell<HashMap<(String, u32), Arc<FrameSizeTable>>> = OnceCell::new();

// fn get_table(final_file: &str, fps: u32) -> anyhow::Result<Arc<FrameSizeTable>> {
//     let map = TABLE_CACHE.get_or_init(HashMap::new);
//     if let Some(t) = map.get(&(final_file.to_string(), fps)) {
//         return Ok(Arc::clone(t));
//     }
//     let table = Arc::new(FrameSizeTable::load(final_file, fps)?);
//     // insert (needs a mutable ref; rebuild a new map to keep OnceCell immutability simple)
//     let mut new_map = map.clone();
//     new_map.insert((final_file.to_string(), fps), Arc::clone(&table));
//     TABLE_CACHE.set(new_map).ok(); // ignore error if already set by a race
//     Ok(table)
// }
#[allow(unused)]
#[derive(Clone)]
pub struct FrameSizeTable {
    _fps: u32,
    codec_str: String,
    mbps_cols: Vec<u32>,            // e.g. [5,10,15,...]
    col_index: HashMap<u32, usize>, // 5 -> 0, 10 -> 1, ...
    // Column-major: framesizes[col_idx][frame_idx] -> bytes
    framesizes: Vec<Vec<u32>>, // use u32 to halve memory on 64-bit
    start_offset: usize,       // <-- ADD THIS FIELD
}

impl FrameSizeTable {
    // Return interpolated frame size (bytes) for arbitrary Mbps
    #[inline(always)]
    fn bytes_interp_cached(
        &self,
        want_mbps: f32,
        frame_idx: usize,
        last_lo: &Cell<usize>,
        last_hi: &Cell<usize>,
    ) -> usize {
        // Get the pre-calculated random offset
        let offset_idx = |idx: usize, v_len: usize| (idx + self.start_offset) % v_len;

        // --- Fast path: exact integer Mbps match
        if let Some(&col_idx) = self.col_index.get(&(want_mbps.round() as u32)) {
            let v = &self.framesizes[col_idx];
            return v[offset_idx(frame_idx, v.len())] as usize; // <-- Use offset
        }

        let mbps_cols = &self.mbps_cols;
        let n = mbps_cols.len();
        if n == 0 {
            return 0;
        }

        // --- Clamp to bounds
        if want_mbps <= mbps_cols[0] as f32 {
            let v = &self.framesizes[0];
            return v[offset_idx(frame_idx, v.len())] as usize; // <-- Use offset
        }
        if want_mbps >= mbps_cols[n - 1] as f32 {
            let v = &self.framesizes[n - 1];
            return v[offset_idx(frame_idx, v.len())] as usize; // <-- Use offset
        }

        // --- Try cached indices
        let mut lo = last_lo.get();
        let mut hi = last_hi.get();

        // Reuse cache if still valid
        if lo < n && hi < n {
            let m0 = mbps_cols[lo] as f32;
            let m1 = mbps_cols[hi] as f32;
            if want_mbps >= m0 && want_mbps <= m1 {
                let f = (want_mbps - m0) / (m1 - m0);
                let v0_vec = &self.framesizes[lo];
                let v1_vec = &self.framesizes[hi];
                let v0 = v0_vec[offset_idx(frame_idx, v0_vec.len())] as f32; // <-- Use offset
                let v1 = v1_vec[offset_idx(frame_idx, v1_vec.len())] as f32; // <-- Use offset
                return ((v0 + f * (v1 - v0)).round() as u32) as usize;
            }
        }

        // --- Binary search fallback if outside cached bracket
        hi = match mbps_cols.binary_search_by(|&v| v.cmp(&(want_mbps as u32))) {
            Ok(i) => i,
            Err(i) => i,
        };
        lo = hi.saturating_sub(1);
        last_lo.set(lo);
        last_hi.set(hi);

        let m0 = mbps_cols[lo] as f32;
        let m1 = mbps_cols[hi] as f32;
        let f = (want_mbps - m0) / (m1 - m0);

        let v0_vec = &self.framesizes[lo];
        let v1_vec = &self.framesizes[hi];
        let v0 = v0_vec[offset_idx(frame_idx, v0_vec.len())] as f32; // <-- Use offset
        let v1 = v1_vec[offset_idx(frame_idx, v1_vec.len())] as f32; // <-- Use offset
        ((v0 + f * (v1 - v0)).round() as u32) as usize
    }

    fn load(final_file: &str, _fps: u32, codec_str: String,
        vbv_perframe: bool, 
        intra_refresh_enabled: bool,
        use_foveation: bool, 
    ) -> anyhow::Result<Self> {
        let path = get_prefix_path(&format!(
            "csv_framesizes/{}_{}_{}fps_vbv{:.0}_IR{:.0}_foveated{:.0}.csv",
            codec_str, final_file, _fps, vbv_perframe as usize, intra_refresh_enabled as usize, use_foveation as usize, 
        ));
        if !std::path::Path::new(&path).exists() {
            return Err(anyhow::anyhow!("Frame-size CSV not found: {}", path));
        }

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path(&path)?;
        let headers = rdr.headers()?.clone();

        let mut mbps_cols = Vec::with_capacity(headers.len().saturating_sub(1));
        for h in headers.iter().skip(1) {
            let m = h
                .trim_end_matches("Mbps")
                .parse::<u32>()
                .map_err(|_| anyhow::anyhow!("Bad column name: {}", h))?;
            mbps_cols.push(m);
        }
        let ncols = mbps_cols.len();

        let mut framesizes: Vec<Vec<u32>> = (0..ncols).map(|_| Vec::new()).collect();

        for rec in rdr.records() {
            let rec = rec?;
            for (ci, _) in mbps_cols.iter().enumerate() {
                // +1 to skip frame_index
                let v = rec.get(ci + 1).unwrap_or("0");
                // if the csv is clean, you can use unwrap_unchecked-like fast paths,
                // but keep it robust first:
                let b = v.parse::<u32>().unwrap_or(0);
                framesizes[ci].push(b);
            }
        }

        let num_frames = framesizes.get(0).map_or(0, |v| v.len());

        let start_offset = if num_frames > 1 {
            let middle_frame = num_frames / 2;
            // gen_range is exclusive of the upper bound: [0, middle_frame)
            if middle_frame > 0 {
                rand::thread_rng().gen_range(0..middle_frame)
            } else {
                0 // Not enough frames to randomize (e.g., num_frames = 1)
            }
        } else {
            0 // No frames or only one frame
        };

        let col_index = mbps_cols
            .iter()
            .enumerate()
            .map(|(i, &m)| (m, i))
            .collect::<HashMap<_, _>>();

        Ok(Self {
            _fps,
            mbps_cols,
            col_index,
            framesizes,
            start_offset,
            codec_str,
        })
    }
}

use dashmap::DashMap;
use once_cell::sync::Lazy;

static TABLE_CACHE: Lazy<DashMap<(String, u32), Arc<FrameSizeTable>>> =
    Lazy::new(|| DashMap::new());

fn get_table(
    final_file: &str,
    fps: u32,
    codec_selection: VideoCodec,
    vbv_perframe: bool, 
    intra_refresh_enabled: bool,
    use_foveation: bool, 

) -> anyhow::Result<Arc<FrameSizeTable>> {
    if let Some(entry) = TABLE_CACHE.get(&(final_file.to_string(), fps)) {
        return Ok(entry.clone());
    }
    let codec_str = format!("{}", codec_selection); // using Display trait to obtain String
                                                    // Double-checked load

    let table = Arc::new(FrameSizeTable::load(final_file, fps, codec_str, vbv_perframe, intra_refresh_enabled, use_foveation, )?);
    let key = (final_file.to_string(), fps);
    let entry = TABLE_CACHE.entry(key).or_insert_with(|| table.clone());
    Ok(entry.clone())
}

#[allow(unused)]
#[inline]
fn fibonacci_payload_exact(size: usize) -> Vec<u8> {
    let mut v = vec![0u8; size];
    if size == 0 {
        return v;
    }
    if size > 1 {
        v[1] = 1;
    }
    // Tight loop; compilers auto-vectorize the addition pipeline well enough.
    for i in 2..size {
        // wrapping to stay in u8
        v[i] = v[i - 1].wrapping_add(v[i - 2]);
    }
    v
}

#[allow(unused)]
pub fn generate_fibonacci_video_payload(current_bitrate_mbps: f32, fps: f32, ) -> Vec<u8> {
    // Calculate the payload size based on bitrate, very simplified linear relationship of bitrate/frame_size based on ALVR test for 90 FPS, assume for other FPS the relationship mantains. 
    let no_bytes_based_bitrate = ((1416.97 * current_bitrate_mbps + -810.06) * 90.0/fps) as usize ;

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

/////// ALT code to use table based on real ALVR frame size distribution at 100 Mbps /////////
#[derive(Clone)]
struct HardcodedFrameTable {
    framesizes: Vec<u32>,
    start_offset: usize,
}

impl HardcodedFrameTable {
    fn load(path: &str) -> anyhow::Result<Self> {
        // Assuming get_prefix_path is in your scope
        let actual_path = get_prefix_path(path);
        
        if !std::path::Path::new(&actual_path).exists() {
            return Err(anyhow::anyhow!("Hardcoded CSV not found: {}", actual_path));
        }

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path(&actual_path)?;
            
        let mut framesizes = Vec::new();

        for rec in rdr.records() {
            let rec = rec?;
            // Assuming the CSV format is: frame_id, size_in_bytes
            // We read index 1. If your CSV only has one column, change this to get(0).
            let size_str = rec.get(1).unwrap_or("0");

            let bytes = size_str.parse::<f32>()
                .map(|f| f.round() as u32) // Use .round() for accuracy or just 'as u32' to truncate
                .unwrap_or(0);

            framesizes.push(bytes);
        }

        let num_frames = framesizes.len();
        let start_offset = if num_frames > 1 {
            let middle_frame = num_frames / 2;
            if middle_frame > 0 {
                rand::thread_rng().gen_range(0..middle_frame)
            } else {
                0
            }
        } else {
            0
        };

        Ok(Self {
            framesizes,
            start_offset,
        })
    }

    #[inline(always)]
    fn get_bytes(&self, frame_idx: usize) -> usize {
        if self.framesizes.is_empty() {
            return 0;
        }
        let offset_idx = (frame_idx + self.start_offset) % self.framesizes.len();
        self.framesizes[offset_idx] as usize
    }
}



