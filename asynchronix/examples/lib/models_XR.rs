use crate::lib::alvr_control_socket::{
    framed_recv_vec, ControlSocketReceiver, ControlSocketSender
};
use crate::lib::gcc_nada_estimator::{self, GccBandwidthEstimator, GCC_INIT_CONFIGURED_BITRATE};
use crate::lib::{alvr_stream_socket::StreamReceiver, HeuristicStats, BATCH_SIZE_CSV};
use async_std::future::pending;
use rand::distributions::Uniform;
use rand::rngs::StdRng;
use rand::{Rng};
use rand::SeedableRng;
use crate::{debug_debug, print_blue, print_brown, print_dblue, print_magenta, taitime_to_f64
    //  print_brown
    };
use image::{ImageBuffer, Rgb};
use image_compare::rgb_hybrid_compare;
use rand::prelude::IteratorRandom;
use rand_distr::{Normal, Distribution};
use crate::lib::alvr_packets::{DeviceMotion, Pose};
use crate::lib::{get_prefix_path, AveragingStrategy, EdcaAc, HevcParser, WindowType, render_text};
use anyhow::Result;
use regex::Regex;
use std::cell::RefCell;
use std::fs::{OpenOptions};
use std::io::{BufReader, Read, Write, BufWriter};
use std::net::{self, Ipv4Addr};
use std::process::Command;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread_local;
use tempfile::TempDir;
use tokio::sync::Semaphore;
use std::path::Path;
use minifb::{Window, WindowOptions};
use std::{fs::File};

use crate::lib::{models_mm1k::NetworkPattern,
    fovoptix::{FOAimdRateControl, FovOptixStruct, * }};
// }; 
use crate::{format_elapsed, print_green};
use crate::lib::{HeaderALVRStream, USE_FFMPEG_DEMO};
use crate::print_pretty;
#[allow(unused)]
use crate::{debug_bgprint, print_prettyy, print_red};
#[allow(unused)]
use crate::{debug_print, print_prettyyyy, print_yellow, print_pink};

use core::f64;
use ffmpeg_sidecar::command::FfmpegCommand;
use glam::{Quat, Vec3};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::net::IpAddr;
use std::thread::yield_now;
use std::time::SystemTime;
use std::time::{Duration, Instant};
use std::{mem, vec};

use crate::lib::alvr_control_socket::ProtoControlSocket;
use crate::lib::alvr_packets::{ClientControlPacket, ClientStatistics, NetworkStatisticsPacket, EverestCommand, NadaStats, };
use crate::lib::alvr_stream_socket::{
    parse_shard_data, ConnectionError, DscpTos, Haptics, ReceiverData, SocketBufferSize,
    SocketProtocol, SocketReader, StreamSender, StreamSocketBuilder, Tracking, VideoPacketHeader, CHUNK_DURATION_F64_S,
};
use crate::lib::alvr_stream_socket::{
    AUDIO, HAPTICS, MAX_HISTORY_SIZE, STATISTICS, TRACKING, VIDEO, FOVOPTIX_BW_PROBE, 
};
use crate::lib::DEBUG_PRINT_ENABLED;
use dashmap::DashMap;
use tai_time::TaiTime;

use asynchronix::model::{Context, Model};
use asynchronix::ports::Output;
use std::cmp::{self, max};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::f64::consts::PI;
use std::future::Future;
use std::sync::RwLock;


use crate::lib::CsvTrace;
use crate::lib::alvr_statistics::StatisticsManager;
use crate::lib::{exponential, AmpduPacket, Coords, DebugColor, MpduPacket, SlidingWindowAverage};
// use crate::lib::INITIAL_BITRATE_MBPS_SIM;
use super::alvr_packets::DeadlineShardlossStatPacket;
use super::alvr_stream_socket::{
    SocketWriter, StreamSocket,  MAX_PACKET_SIZE_RECV,
};
use super::alvr_stream_socket::{CONTROL_STREAM, MAX_DEADLINE_IN_STATS};
use super::get_third_octet;
// use async_process::Child;

use crate::lib::gcc_nada_estimator::*;


fn hide_by_title_with_wmctrl(title: &str) {
    let _ = std::process::Command::new("sh")
        .arg("-lc")
        .arg(format!("wmctrl -r \"{}\" -b add,hidden", title))
        .status();
}

#[cfg(all(unix, not(target_os = "macos")))]
fn send_window_to_background(win: &minifb::Window) {
    // On X11: minimize the window right after creation.
    // This avoids focus-stealing distractions while your script runs.
    unsafe {
        use std::ptr;
        use x11::xlib::{
            XOpenDisplay, XDefaultScreen, XIconifyWindow, XFlush, XCloseDisplay,
            Window as XWindow,
        };

        // minifb returns the OS handle as *mut c_void. For X11 it's the XWindow (an integer)
        // casted to a pointer. Convert back via usize -> XWindow.
        let w: XWindow = (win.get_window_handle() as usize) as XWindow;

        let display = XOpenDisplay(ptr::null());
        if !display.is_null() {
            let screen = XDefaultScreen(display);
            // Ask the WM to iconify (minimize) this window
            XIconifyWindow(display, w, screen);
            XFlush(display);
            XCloseDisplay(display);
        }
    }
}







pub const WIDTH_ENCODER: usize = 3840;
pub const HEIGHT_ENCODER: usize = 2160;

pub const FRAMERATE_WINDOWS: usize = 60;

pub const SCALE_FACTOR_WINDOW: f64 = 0.15;

pub const SHARD_PREFIX_SIZE: usize = mem::size_of::<u32>() // packet length - field itself (4 bytes)
    + mem::size_of::<u16>() // stream ID
    + mem::size_of::<u32>() // packet index
    + mem::size_of::<u32>() // shards count
    + mem::size_of::<u32>() // shards index
    + mem::size_of::<f32>(); // tx relative timestamp

// pub const UPDATE_BITRATE_INTERVAL: Duration = Duration::from_secs(1);
pub const HANDSHAKE_ACTION_TIMEOUT: Duration = Duration::from_secs(2);
pub const MAX_UNREAD_PACKETS: usize = 5; // Applies per stream

pub const CAPACITY_RX_BUFFER: usize = 2000;
pub const STREAMING_RECV_TIMEOUT: Duration = Duration::from_millis(10);
pub const FRAMED_PREFIX_CONTROL_LENGTH: usize = mem::size_of::<u32>();

pub const DECODER_BUFFERING_FRAMES: usize = 3;
pub const BITRATE_UPDATE_INTERVAL: f64 = CHUNK_DURATION_F64_S; 

#[allow(unused)]                                                                                    
pub const TARGET_FRAMES_DECODER_QUEUE: usize = DECODER_BUFFERING_FRAMES; // unused at the moment, 

pub const TARGET_TIMESTAMP_TRACKING: Duration = Duration::from_millis(10);
pub const KEEP_FRAMES_DISK_INDEX: usize = 200;

pub const ALPHA_THROUGHPUT: f32 = 0.1; 


pub const ALPHA_EWMA_FOWD_OBS: f32 = 0.1; 

/// Number of consecutive good matches required to re-establish synchronization

// static _STATISTICS_MANAGER: OptLazy<StatisticsManager> = lazy_mut_none();

use crossbeam::channel::{bounded, unbounded, Receiver, Sender, TryRecvError};

// lazy_static! {
//     static ref REFERENCE_DECODERS: Arc<Mutex<HashMap<IpAddr, SynchronizedDecoder>>> =
//         Arc::new(Mutex::new(HashMap::new()));
// }
#[allow(unused)]
pub struct FramePair {
    decoded: Option<Vec<u32>>,
    reference: Option<Vec<u32>>,
    decoded_raw: Option<Vec<u8>>,
    reference_raw: Option<Vec<u8>>,
    frame_id: usize,
}
#[derive(Clone)]
pub struct PerfectInfoBitrateMessage{
    bitrate_ladder_bps: Option<Vec<f32>>, 
    bitrate_mbps: f32, 
}

use std::sync::OnceLock;

static PRINT_COUNTER: OnceLock<AtomicUsize> = OnceLock::new();

fn get_counter() -> &'static AtomicUsize {
    PRINT_COUNTER.get_or_init(|| AtomicUsize::new(0))
}


pub fn is_keyframe(frame: &[u8]) -> bool {
    // Check for start code
    for i in 0..frame.len().saturating_sub(5) {
        if (frame[i] == 0 && frame[i + 1] == 0 && frame[i + 2] == 1)
            || (frame[i] == 0 && frame[i + 1] == 0 && frame[i + 2] == 0 && frame[i + 3] == 1)
        {
            let start_code_len = if frame[i + 2] == 0 { 4 } else { 3 };
            let nal_header_pos = i + start_code_len;

            if nal_header_pos < frame.len() {
                let nal_header = frame[nal_header_pos];
                let nal_type = (nal_header >> 1) & 0x3F; // Extract bits 1-6 (NAL type)

                // In HEVC, NAL types 16-21 represent IRAP (Intra Random Access Point) pictures
                if (16..=21).contains(&nal_type) {
                    return true;
                }
            }
        }
    }
    false
}


#[allow(unused)]
fn compute_enhanced_frame_similarity(
    frame1: &[u8],
    frame2: &[u8],
    width: usize,
    height: usize,
) -> f64 {
    // Validate that both frames have the expected size (width * height * 3)
    if frame1.len() != frame2.len() || frame1.len() != width * height * 3 {
        return 1.0;
    }

    // Construct an RGB image from the raw byte slice.
    let img1 = ImageBuffer::<Rgb<u8>, _>::from_raw(width as u32, height as u32, frame1.to_vec())
        .expect("Failed to create image 1");
    let img2 = ImageBuffer::<Rgb<u8>, _>::from_raw(width as u32, height as u32, frame2.to_vec())
        .expect("Failed to create image 2");

    // Use the image-compare crate's hybrid comparison method.
    // This method internally converts to YUV, applies MSSIM on Y and RMS on U/V,
    // then combines the differences into a single similarity score.
    let result = rgb_hybrid_compare(&img1, &img2).expect("Images must have the same dimensions");

    // println!("SCORE = {}", 1.0 - result.score);
    1.0 - result.score
}

fn convert_rgb_to_u32(rgb_data: &[u8], width: usize, height: usize) -> Option<Vec<u32>> {
    if rgb_data.len() != width * height * 3 {
        println!(
            "ERROR: Expected rgb_data size {} but got {}",
            width * height * 3,
            rgb_data.len()
        );
        return None;
    }

    let mut pixels = Vec::with_capacity(width * height);
    for chunk in rgb_data.chunks_exact(3) {
        let pixel = ((chunk[0] as u32) << 16) | ((chunk[1] as u32) << 8) | (chunk[2] as u32);
        pixels.push(pixel);
    }

    Some(pixels)
}




/// Standalone function for finding the next NAL start code in a buffer
pub fn find_next_start_code(buffer: &[u8], start_pos: usize) -> Option<usize> {
    for i in start_pos..buffer.len().saturating_sub(3) {
        // Look for 0x000001 or 0x00000001 (3 or 4 byte start codes)
        if (buffer[i] == 0 && buffer[i + 1] == 0 && buffer[i + 2] == 1)
            || (i < buffer.len() - 4
                && buffer[i] == 0
                && buffer[i + 1] == 0
                && buffer[i + 2] == 0
                && buffer[i + 3] == 1)
        {
            return Some(i);
        }
    }
    None
}
#[allow(non_camel_case_types, unused)]
pub struct HevcDecoder {
    frame_rx: crossbeam::channel::Receiver<Vec<u8>>,
    packet_tx: crossbeam::channel::Sender<Vec<u8>>,
    _stdin_handle: std::thread::JoinHandle<()>,
    _stderr_handle: std::thread::JoinHandle<()>,
    width: u32,
    height: u32,
    parser: HevcParser,
    frame_buffer: VecDeque<Vec<u8>>, // Buffer for parsed HEVC frames
    decoded_frames: VecDeque<Vec<u8>>, // Buffer for decoded RGB frames

    ewma_frame_size: f64,
    last_update: Instant,

    pub frames_processed: usize, // Count of frames we've sent to the decoder
    pub keyframes_seen: usize,   // Count of keyframes observed
    pub last_decoded_frame_time: Instant, // Time when we last got a decoded frame
    pub total_bytes_processed: f64, // Total bytes of HEVC data processed
    pub priming_complete: bool,  // Flag to indicate if decoder is primed and ready
    pub expected_frame_size: usize, // Expected size of decoded RGB frames

    max_buffered_frames: usize, // Maximum number of frames to buffer
    min_buffered_frames: usize, 
    decoder_string: String,
    // shared_params: Option<Arc<SharedParameterSetManager>>,
    last_sync_generation: u64,
    force_keyframe_sync: bool,

    recovery_frames: usize,     // Counter for frames to skip during recovery
    pending_clear: bool,        // Flag to indicate decoder state should be reset
    initialization_phase: bool, // Flag for the decoder's initialization phase

    pending_frames: VecDeque<(Vec<u8>, Vec<u32>)>, // (raw, converted pixels)

    pub internal_frame_counter: usize, // Counter for frames successfully decoded *by* ffmpeg

    processing_semaphore: Arc<Semaphore>,

    decoded_frame_counter: usize,
}
#[allow(non_camel_case_types, unused)]
impl HevcDecoder {
    pub fn new(framerate: u32, width: u32, height: u32, decoder_str: &str) -> Self {
        let frame_size = (width as usize) * (height as usize) * 3;

        let decoder_string = decoder_str.to_string();

        let mut child = FfmpegCommand::new()
            // .hwaccel("cuda")
            .args(&["-c:v", "hevc"])     // ✅ force software decoder
            .args(&["-f", "hevc", "-i", "-"])
            // .args(&["-c:v", "libx265"])              // force software HEVC encoder
            // .args(&["-vf", &format!("fps={}", framerate)])
            .args(&["-pix_fmt", "rgb24"])
            .args(&["-tune", "zerolatency"])
            // .args(&["-preset", "ultrafast"])
            .args(&["-vsync", "passthrough"])
            .args(&["-f", "rawvideo", "-"])
            .spawn()
            .unwrap();

        let stdout = child.take_stdout().unwrap();
        let stdin = child.take_stdin().unwrap();
        let stderr = child.take_stderr().unwrap();

        let (frame_tx, frame_rx) = unbounded::<Vec<u8>>();
        let (packet_tx, packet_rx) = bounded::<Vec<u8>>(100);

        // Start stdout reader thread with more explicit error handling
        std::thread::spawn({
            let frame_size = frame_size;
            let frame_tx: crossbeam::channel::Sender<Vec<u8>> = frame_tx.clone(); // Clone for the thread
            let decoder_str_clone = decoder_string.clone();

            move || {
                let mut reader = BufReader::new(stdout);
                let mut buffer = Vec::with_capacity(frame_size * 2);
                let mut chunk = vec![0u8; 4096];

                loop {
                    match reader.read(&mut chunk) {
                        Ok(0) => {
                            println!("{decoder_str_clone} Decoder stdout closed");
                            break;
                        }
                        Ok(n) => {
                            buffer.extend_from_slice(&chunk[..n]);

                            // Print debug info about buffer accumulation
                            // println!("Decoder received {} bytes, buffer size: {}/{}",
                            //     n, buffer.len(), frame_size);

                            while buffer.len() >= frame_size {
                                let frame = buffer.drain(..frame_size).collect::<Vec<u8>>();
                                // frame_tx.send(frame).unwrap();
                                // println!("Sending complete decoded frame of size: {}", frame_size);

                                if let Err(e) = frame_tx.send(frame) {
                                    eprintln!(
                                        "{decoder_str_clone} Decoder frame send error: {}",
                                        e
                                    );
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("{decoder_str_clone} Decoder read error: {}", e);
                            break;
                        }
                    }
                }
                println!("{decoder_str_clone} Decoder stdout reader thread exit");
            }
        });
        let decoder_str_clone2 = decoder_string.clone();

        let stdin_handle = std::thread::spawn(move || {
            let mut writer = stdin;
            for packet in packet_rx {
                // println!("Decoder feeding packet of size: {}", packet.len());
                if let Err(e) = writer.write_all(&packet) {
                    eprintln!("{decoder_str_clone2} Decoder write error: {}", e);
                    break;
                }

                if let Err(e) = writer.flush() {
                    eprintln!("{decoder_str_clone2} Decoder flush error: {}", e);
                    break;
                }
            }
            println!("{decoder_str_clone2} Decoder stdin writer thread exit");
        });

        let decoder_string3 = decoder_string.clone(); // Stderr handler with improved debug output
        let stderr_handle = std::thread::spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut buf = String::new();
            loop {
                buf.clear();
                match reader.read_to_string(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        if !buf.trim().is_empty() {
                            println!("{decoder_string3} Decoder stderr: {} bytes", n);
                            eprint!("{}", buf);
                        }
                    }
                    Err(e) => {
                        eprintln!("{decoder_string3} Decoder stderr read error: {}", e);
                        break;
                    }
                }
            }
            println!("{decoder_string3} Decoder stderr reader thread exit");
        });
        println!(
            "{decoder_str} 📹 HevcDecoder initialized with {}x{} resolution",
            width, height
        );

        Self {
            frame_rx,
            packet_tx,
            _stdin_handle: stdin_handle,
            _stderr_handle: stderr_handle,
            width,
            height,
            parser: HevcParser::new(),
            frame_buffer: VecDeque::new(),
            decoded_frames: VecDeque::new(),
            ewma_frame_size: 0.0,
            last_update: Instant::now(), // use real time here, not simu
            frames_processed: 0,
            keyframes_seen: 0,
            last_decoded_frame_time: Instant::now(),
            total_bytes_processed: 0.0,
            priming_complete: false,
            expected_frame_size: frame_size,
            max_buffered_frames: 50,
            min_buffered_frames: 30, 

            decoder_string: decoder_str.to_string(),

            // shared_params,
            last_sync_generation: 0,
            force_keyframe_sync: false,

            recovery_frames: 0,   // Counter for frames to skip during recovery
            pending_clear: false, // Flag to indicate decoder state should be reset
            initialization_phase: false, // Flag for the decoder's initialization phase
            pending_frames: VecDeque::new(),
            internal_frame_counter: 0,
            // frame_rate_target: FRAMERATE_WINDOWS as f32, 
            // last_frame_time: Instant::now(),
            // frame_interval: Duration::from_secs_f64(1.0 / FRAMERATE_WINDOWS as f64), 
            processing_semaphore: Arc::new(Semaphore::new(3)),  
            decoded_frame_counter: 0, 
        }
    }

    pub fn available_frames(&self) -> usize {
        self.decoded_frames.len()
    }

    // Basic NAL-based keyframe detection
    fn detect_keyframe_nal(&self, buffer: &[u8]) -> bool {
        for i in 0..buffer.len().saturating_sub(5) {
            if (buffer[i] == 0 && buffer[i + 1] == 0 && buffer[i + 2] == 1)
                || (buffer[i] == 0
                    && buffer[i + 1] == 0
                    && buffer[i + 2] == 0
                    && buffer[i + 3] == 1)
            {
                let start_code_len = if buffer[i + 2] == 0 { 4 } else { 3 };
                let nal_header_pos = i + start_code_len;

                if nal_header_pos < buffer.len() {
                    let nal_header = buffer[nal_header_pos];
                    let nal_type = (nal_header >> 1) & 0x3F; // Extract bits 1-6 (NAL type)

                    // In HEVC, NAL types 16-21 represent IRAP (Intra Random Access Point) pictures
                    if (16..=21).contains(&nal_type) {
                        return true;
                    }
                }
            }
        }
        false
    }

    // Enhanced keyframe detection with validation
    pub fn contains_keyframe(&self, buffer: &[u8]) -> bool {
        // Basic HEVC keyframe detection first
        let is_keyframe_nal = self.detect_keyframe_nal(buffer);

        if is_keyframe_nal {
            return true;
        }

        false
    }

    pub fn inject_parameter_sets(
        &mut self,
        vps: Option<Vec<u8>>,
        sps: Option<Vec<u8>>,
        pps: Option<Vec<u8>>,
    ) {
        // Prevent recursive parameter set injection
        static INJECTION_DEPTH: AtomicUsize = AtomicUsize::new(0);

        // Increment depth counter and get current value
        let depth = INJECTION_DEPTH.fetch_add(1, Ordering::SeqCst);

        // Guard against excessive recursion (more than 2 levels deep)
        if depth > 2 {
            println!(
                "{} ⚠️ Preventing recursive parameter set injection (depth: {})",
                self.decoder_string, depth
            );
            INJECTION_DEPTH.fetch_sub(1, Ordering::SeqCst);
            return;
        }

        // Print injection information only for the first level
        if depth == 0 {
            if let Some(vps_data) = &vps {
                println!(
                    "{} Injecting VPS ({} bytes)",
                    self.decoder_string,
                    vps_data.len()
                );
            }

            if let Some(sps_data) = &sps {
                println!(
                    "{} Injecting SPS ({} bytes)",
                    self.decoder_string,
                    sps_data.len()
                );
            }

            if let Some(pps_data) = &pps {
                println!(
                    "{} Injecting PPS ({} bytes)",
                    self.decoder_string,
                    pps_data.len()
                );
            }
        }

        // Temporarily mark parameter sets as injected
        let mut was_processed = false;

        // Directly update parser state rather than sending through packet processing
        if let Some(vps_data) = vps {
            was_processed = true;
            self.parser.update_vps(&vps_data);

            // Only send to ffmpeg decoder if not in a recursive call
            if depth == 0 {
                if let Err(e) = self.packet_tx.send(vps_data) {
                    println!(
                        "{} ERROR: Failed to send VPS to decoder: {}",
                        self.decoder_string, e
                    );
                }
            }
        }

        if let Some(sps_data) = sps {
            was_processed = true;
            self.parser.update_sps(&sps_data);

            if depth == 0 {
                if let Err(e) = self.packet_tx.send(sps_data) {
                    println!(
                        "{} ERROR: Failed to send SPS to decoder: {}",
                        self.decoder_string, e
                    );
                }
            }
        }

        if let Some(pps_data) = pps {
            was_processed = true;
            self.parser.update_pps(&pps_data);

            if depth == 0 {
                if let Err(e) = self.packet_tx.send(pps_data) {
                    println!(
                        "{} ERROR: Failed to send PPS to decoder: {}",
                        self.decoder_string, e
                    );
                }
            }
        }

        // After injecting parameter sets, process any frames in buffer
        if was_processed && depth == 0 {
            self.process_decoded_frames();
        }

        // Decrement depth counter
        INJECTION_DEPTH.fetch_sub(1, Ordering::SeqCst);
    }

    /// Synchronize this decoder with another decoder's parameter sets

    /// Extract complete parameter set packets from a buffer
    /// This is useful when you want to extract the parameter sets as complete NAL units
    /// including the start code, which is necessary for feeding to another decoder
    pub fn extract_complete_parameter_sets(
        &self,
        buffer: &[u8],
    ) -> (Option<Vec<u8>>, Option<Vec<u8>>, Option<Vec<u8>>) {
        let mut temp_parser = HevcParser::new();
        temp_parser.add_data(buffer);

        let mut vps_packet = None;
        let mut sps_packet = None;
        let mut pps_packet = None;

        let mut start_pos = 0;

        // Extract complete NAL units with start codes
        while let Some(pos) = find_next_start_code(buffer, start_pos) {
            // Determine start code length (3 or 4 bytes)
            let start_code_len =
                if pos + 3 < buffer.len() && buffer[pos + 2] == 0 && buffer[pos + 3] == 1 {
                    4
                } else {
                    3
                };

            let nal_header_pos = pos + start_code_len;
            if nal_header_pos >= buffer.len() {
                break;
            }

            // Get the NAL type
            let nal_header = buffer[nal_header_pos];
            let nal_type = (nal_header >> 1) & 0x3F; // Extract bits 1-6 (NAL type)

            // Find the end of this NAL unit (next start code or end of buffer)
            let next_pos =
                find_next_start_code(buffer, pos + start_code_len).unwrap_or(buffer.len());

            // Extract the complete NAL unit with start code
            match nal_type {
                32 => {
                    // VPS
                    vps_packet = Some(buffer[pos..next_pos].to_vec());
                }
                33 => {
                    // SPS
                    sps_packet = Some(buffer[pos..next_pos].to_vec());
                }
                34 => {
                    // PPS
                    pps_packet = Some(buffer[pos..next_pos].to_vec());
                }
                _ => {}
            }

            start_pos = next_pos;
        }

        (vps_packet, sps_packet, pps_packet)
    }

    pub fn is_ready(&self) -> bool {
        // A decoder is ready when:
        // 1. We've seen at least one keyframe
        // 2. We've processed at least 10 frames
        // 3. Priming is marked complete
        self.keyframes_seen >= 1
    }

    pub fn process_packet(&mut self, packet: Vec<u8>) {
        // Track if this packet contains a parameter set
        let mut has_parameter_update = false;

        // Extract parameter sets from this packet
        let (vps, sps, pps) = self.extract_complete_parameter_sets(&packet);
        let has_param_sets = vps.is_some() || sps.is_some() || pps.is_some();

        // if has_param_sets {
        //     has_parameter_update = true;

        //     print_prettyy!(
        //         DebugColor::Cyan,
        //         "{} - Parameter sets found in packet: VPS: {}, SPS: {}, PPS: {}",
        //         self.decoder_string,
        //         vps.as_ref().map_or(0, |v| v.len()),
        //         sps.as_ref().map_or(0, |v| v.len()),
        //         pps.as_ref().map_or(0, |v| v.len()),
        //     );
        // }

        // Record frame metrics
        let frame_size = packet.len() as f64;
        let now = Instant::now();
        self.last_update = now;

        self.total_bytes_processed += frame_size;
        self.frames_processed += 1;

        // Check if this is a keyframe
        let is_keyframe = self.contains_keyframe(&packet);
        if is_keyframe {
            self.keyframes_seen += 1;

            // Reset recovery status on keyframe
            self.recovery_frames = 0;
            self.pending_clear = false;

            print_prettyy!(
                DebugColor::Magenta,
                "{} 🔑 KEYFRAME detected (size: {} bytes)",
                self.decoder_string,
                packet.len(),
            );
        }

        // Calculate smoothing factor for EWMA
        let alpha = 0.1; // Use a fixed alpha for simplicity
        self.ewma_frame_size = alpha * (frame_size as f64) + (1.0 - alpha) * self.ewma_frame_size;

        let _permit = self.processing_semaphore.acquire();
        
        // Add data to the parser
        self.parser.add_data(&packet);

        // Extract frames from the parser and buffer them
        let frames = self.parser.get_frames();
        for frame in frames {
            self.frame_buffer.push_back(frame);
        }

        // If we're in recovery mode, handle differently
        if self.recovery_frames > 0 {
            self.recovery_frames -= 1;

            if is_keyframe {
                // We have a keyframe - go ahead and send to decoder
                if let Err(e) = self.packet_tx.send(packet) {
                    print_prettyy!(
                        DebugColor::Red,
                        "{} ERROR: Failed to send packet to decoder: {}",
                        self.decoder_string,
                        e,
                    );
                }
            } else if self.recovery_frames == 0 {
                // End of recovery period, start sending frames again
                if let Err(e) = self.packet_tx.send(packet) {
                    print_prettyy!(
                        DebugColor::Red,
                        "{} ERROR: Failed to send packet to decoder: {}",
                        self.decoder_string,
                        e,
                    );
                }

                print_prettyy!(
                    DebugColor::Green,
                    "{} - Recovery complete, resuming normal operation",
                    self.decoder_string,
                );
            }
            // Otherwise silently drop frames during recovery
        } else {
            // Forward packet to ffmpeg decoder in normal mode
            if let Err(e) = self.packet_tx.send(packet) {
                print_prettyy!(
                    DebugColor::Red,
                    "{} ERROR: Failed to send packet to decoder: {}",
                    self.decoder_string,
                    e,
                );
            }
        }

        // // After parameter update, enter recovery mode if not already there
        // if has_parameter_update && self.recovery_frames == 0 && !is_keyframe {
        //     self.recovery_frames = 30; // Skip ~30 frames or until next keyframe
        //     print_prettyy!(
        //         DebugColor::Yellow,
        //         "{} - Parameter update detected, entering recovery mode for {} frames",
        //         self.decoder_string,
        //         self.recovery_frames,
        //     );
        // }

        // Check for decoder priming completion
        if !self.priming_complete && self.keyframes_seen >= 2 && self.frames_processed >= 60 {
            print_prettyy!(
                DebugColor::Blue,
                "{} 🚀 Decoder priming complete! Processed {} frames including {} keyframes",
                self.decoder_string,
                self.frames_processed,
                self.keyframes_seen,
            );
            self.priming_complete = true;
        }
    }

    // Better implementation of process_decoded_frames
    // Enhance process_decoded_frames to return number of frames processed
    pub fn process_decoded_frames(&mut self) -> usize {
        let mut frames_received = 0;
        let start_time = Instant::now();
        let max_processing_time = Duration::from_millis(50);

        while start_time.elapsed() < max_processing_time {
            match self.frame_rx.try_recv() {
                Ok(frame) => {
                    frames_received += 1;
                    self.last_decoded_frame_time = Instant::now();

                    if frame.len() == self.expected_frame_size {
                        self.decoded_frames.push_back(frame);
                        self.frames_processed += 1; 
                    } else {
                        println!(
                            "{} ⚠️ Received malformed frame (size={}), expected {}",
                            self.decoder_string,
                            frame.len(),
                            self.expected_frame_size
                        );

                        if frame.len() >= self.expected_frame_size * 9 / 10
                            && frame.len() <= self.expected_frame_size * 11 / 10
                        {
                            self.decoded_frames.push_back(frame);
                        }
                    }
                    if self.decoded_frames.len() <= self.min_buffered_frames{
                        break; 
                    }
                    if self.decoded_frames.len() >= self.max_buffered_frames {
                        break;
                    }
                }
                Err(TryRecvError::Empty) => {
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    println!(
                        "{} 🛑 Decoder output channel disconnected!",
                        self.decoder_string
                    );
                    break;
                }
            }
        }

        frames_received
    }

    /// Extract and print VPS, SPS, and PPS from a packet
    pub fn extract_parameter_sets(&mut self, packet: &[u8]) {
        // Create a temporary parser just for this packet to avoid disturbing the main parser state
        let mut temp_parser = HevcParser::new();
        temp_parser.add_data(packet);

        // Process all possible NAL units in this packet
        while let Some(nal) = temp_parser.next_nal_unit() {
            match nal.nal_type {
                32 => println!(
                    "{} 📋 Found VPS NAL unit (size: {})",
                    self.decoder_string,
                    nal.data.len()
                ),
                33 => println!(
                    "{} 📋 Found SPS NAL unit (size: {})",
                    self.decoder_string,
                    nal.data.len()
                ),
                34 => println!(
                    "{} 📋 Found PPS NAL unit (size: {})",
                    self.decoder_string,
                    nal.data.len()
                ),
                _ => {} // Ignore other NAL types
            }
        }

        // Check if we found any parameter sets
        let (vps, sps, pps) = temp_parser.get_parameter_sets();
        if vps.is_some() || sps.is_some() || pps.is_some() {
            println!("{} 📊 Parameter sets found in packet:", self.decoder_string);
            temp_parser.print_parameter_sets();
        }
    }

    // Add a method to get the parameter sets for synchronization
    pub fn get_parameter_sets(&self) -> (Option<Vec<u8>>, Option<Vec<u8>>, Option<Vec<u8>>) {
        let (vps, sps, pps) = self.parser.get_parameter_sets();

        // Clone the data to return owned values
        (
            vps.map(|v| v.clone()),
            sps.map(|s| s.clone()),
            pps.map(|p| p.clone()),
        )
    }

    fn is_frame_corrupt(&self, frame: &[u8]) -> bool {
        // Early return if frame is empty or clearly too small
        if frame.len() < 1000 {
            return true;
        }

        // During initialization phase, use more sophisticated validation
        if !self.priming_complete || self.frames_processed < 200 {
            // Check RGB distribution - this is the key insight
            // Sample at multiple locations rather than judging the entire frame
            let mut green_dominant_regions: i32 = 0;

            // Sample 9 regions (3x3 grid)
            let width = self.width as usize;
            let height = self.height as usize;
            let stride = width * 3;

            // Define sampling regions and thresholds
            for y_region in 0..3 {
                for x_region in 0..3 {
                    let region_x = width * x_region / 3;
                    let region_y = height * y_region / 3;
                    let region_size = 32; // Sample a 32x32 block

                    let mut green_pixels = 0;
                    let mut total_pixels = 0;

                    // Sample pixels in this region
                    for y_offset in 0..region_size {
                        for x_offset in 0..region_size {
                            let x = region_x + x_offset;
                            let y = region_y + y_offset;

                            if x < width && y < height {
                                let idx = y * stride + x * 3;
                                if idx + 2 < frame.len() {
                                    let r = frame[idx] as u32;
                                    let g = frame[idx + 1] as u32;
                                    let b = frame[idx + 2] as u32;

                                    total_pixels += 1;

                                    // Check for extreme green dominance
                                    if g > r * 3 && g > b * 3 && g > 200 {
                                        green_pixels += 1;
                                    }
                                }
                            }
                        }
                    }

                    // If more than 70% of pixels in this region are extremely green-dominant,
                    // count it as a suspicious region
                    if total_pixels > 0 && (green_pixels * 100 / total_pixels) > 70 {
                        green_dominant_regions += 1;
                    }
                }
            }

            // If at least 4 of the 9 regions are green-dominant, consider it corrupt
            return green_dominant_regions >= 4;
        }

        // For non-initialization frames, use a simpler check
        return false;
    }

    pub fn next_decoded_frame(&mut self) -> Option<(Vec<u8>, Instant)> {
        let inst = Instant::now();

        // Process any newly available frames
        let frames_added = self.process_decoded_frames();

        // Try to get a frame from the buffer
        if let Some(frame) = self.decoded_frames.pop_front() {
            // During initialization, apply stricter validation
            if !self.priming_complete || self.frames_processed < 200 {
                if self.is_frame_corrupt(&frame) {
                    print_prettyy!(
                        DebugColor::Red,
                        "{} - Discarding corrupt frame detected during initialization",
                        self.decoder_string,
                    );

                    // Force entry into recovery mode if we detect corruption
                    if self.recovery_frames == 0 {
                        self.recovery_frames = 5;
                    }

                    return None;
                }
            }
            self.decoded_frame_counter += 1; 
            // Frame is good
            Some((frame, inst))
        } else {
            if frames_added > 0 && !self.decoded_frames.is_empty() {
                // Strange case: we added frames but now buffer is empty?
                print_prettyy!(
                    DebugColor::Yellow,
                    "{} Strange: Added frames but buffer is now empty?",
                    self.decoder_string,
                );
            }

            // No frames available
            None
        }
    }
}

#[allow(non_camel_case_types, unused)]
pub struct SharedParameterSetManager {
    // Core parameter sets
    vps: RwLock<Option<Vec<u8>>>,
    sps: RwLock<Option<Vec<u8>>>,
    pps: RwLock<Option<Vec<u8>>>,

    // Metadata for synchronization
    generation: AtomicU64,
    primary_decoder: String,

    // Keyframe tracking
    last_keyframe_size: AtomicUsize,
    last_keyframe_timestamp: RwLock<Instant>,
    keyframe_count: AtomicUsize,
}
#[allow(non_camel_case_types, unused)]
impl SharedParameterSetManager {
    pub fn new(primary_decoder: &str) -> Self {
        Self {
            vps: RwLock::new(None),
            sps: RwLock::new(None),
            pps: RwLock::new(None),
            generation: AtomicU64::new(0),
            primary_decoder: primary_decoder.to_string(),
            last_keyframe_size: AtomicUsize::new(0),
            last_keyframe_timestamp: RwLock::new(Instant::now()),
            keyframe_count: AtomicUsize::new(0),
        }
    }

    // Update parameter sets from a specific decoder
    pub fn update_from_decoder(
        &self,
        decoder_id: &str,
        vps: Option<Vec<u8>>,
        sps: Option<Vec<u8>>,
        pps: Option<Vec<u8>>,
    ) -> bool {
        // Only accept updates from primary decoder or if we have no sets yet
        let is_primary = decoder_id == self.primary_decoder;
        let should_update = is_primary
            || (self.vps.read().unwrap().is_none()
                && self.sps.read().unwrap().is_none()
                && self.pps.read().unwrap().is_none());

        if should_update {
            let mut updated = false;

            if let Some(vps_data) = vps {
                if vps_data.len() > 8 {
                    // Reasonable minimum size
                    *self.vps.write().unwrap() = Some(vps_data);
                    updated = true;
                }
            }

            if let Some(sps_data) = sps {
                if sps_data.len() > 8 {
                    *self.sps.write().unwrap() = Some(sps_data);
                    updated = true;
                }
            }

            if let Some(pps_data) = pps {
                if pps_data.len() > 4 {
                    *self.pps.write().unwrap() = Some(pps_data);
                    updated = true;
                }
            }

            if updated {
                // Increment generation to notify all decoders
                self.generation.fetch_add(1, Ordering::SeqCst);
                return true;
            }
        }

        false
    }

    // Record a keyframe observation
    pub fn record_keyframe(&self, decoder_id: &str, keyframe_size: usize) {
        // Only primary decoder or first keyframe seen updates the size
        let current_size = self.last_keyframe_size.load(Ordering::SeqCst);
        let should_update = decoder_id == self.primary_decoder || current_size == 0;

        if should_update {
            self.last_keyframe_size
                .store(keyframe_size, Ordering::SeqCst);
            *self.last_keyframe_timestamp.write().unwrap() = Instant::now();
            self.keyframe_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    // Get current parameter sets for a decoder to synchronize with
    pub fn get_parameter_sets(&self) -> (Option<Vec<u8>>, Option<Vec<u8>>, Option<Vec<u8>>) {
        let vps = self.vps.read().unwrap().clone();
        let sps = self.sps.read().unwrap().clone();
        let pps = self.pps.read().unwrap().clone();

        (vps, sps, pps)
    }

    // Get current generation number (increments with each update)
    pub fn get_generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    // Validate a keyframe observation
    pub fn validate_keyframe(&self, keyframe_size: usize) -> bool {
        let reference_size = self.last_keyframe_size.load(Ordering::SeqCst);

        // If we have no reference yet, accept anything
        if reference_size == 0 {
            return true;
        }

        // Check if size is within reasonable bounds
        let size_ratio = keyframe_size as f64 / reference_size as f64;
        if size_ratio < 0.5 || size_ratio > 2.0 {
            return false;
        }

        true
    }
}

// #[derive(Clone)]   // TODO: Use for modelling "Adaptive" mode of ALVR. Encoding latencies are tricky to get from chunks, could be modelled directly as some distribution over T_enc_chunk/N_frames_chunk
                      // ** Might want to look into libavcodec instead of FFMPEG. 
// pub struct EncoderLatencyLimiter {
//     pub max_saturation_multiplier: f32,
// }
// #[derive(Clone)]
// pub struct DecoderLatencyLimiter {
//     pub max_decoder_latency_ms: u64,
//     pub latency_overstep_frames: usize,
//     pub latency_overstep_multiplier: f32,
// }



// #[derive(Clone, PartialEq, Debug)]
#[allow(unused)]
pub enum BitrateMode {
    ConstantMbps(f32),
        EVeREst{
            // d_upper: f32, 
            // d_lower: f32, 
            bitrate_ladder_mbps: Vec<f32>,
        },
        NestVr{
            averaging_strategy: AveragingStrategy,            
            max_bitrate_mbps: f32,

            min_bitrate_mbps: f32,

            initial_bitrate_mbps: f32,

            nest_vr_profile: ProfileConfig,

        },

        ReinforcementLearner {
            bitrate_ladder_mbps: Vec<f32>, 
            step_interval: Duration, 
            connector: Arc<Mutex<Box<dyn RLConnector + Send>>>,
            last_action_idx: Arc<Mutex<usize>>,
            last_decision_instant: Arc<Mutex<TaiTime<0>>>,
            pending_obs: Arc<Mutex<Option<RLObservationVector>>>,
            action_space: ActionSpace, 
        }, 
        GCCPort{
            gcc_estimator: GccBandwidthEstimator, 
            framerate: f64 // to reset without needing to store framerate in parent class. 
        },

        NADACiscoPort{
            // last_bitrate_mbps: f64, 
        }, 

        FovOptixPort{
            last_bitrate_mbps: f32,   // FovOptix requires additional network probing, TODO. 
        },
}

impl BitrateMode{
    fn variant_name(&self) -> String {
        match self {
            BitrateMode::ConstantMbps(val) =>       format!("CBR {} Mbps", val),      
            BitrateMode::EVeREst { .. } =>                "EVeREst-Intra".to_string(),
            BitrateMode::NestVr { .. } =>                 "NeSt-VR".to_string(), 
            BitrateMode::ReinforcementLearner { .. } =>   "ReinforcementLearner".to_string(),
            BitrateMode::GCCPort { .. } =>                 "GCC Port".to_string(),   
            BitrateMode::NADACiscoPort {  } =>             "NADA Port".to_string(), 
            BitrateMode::FovOptixPort { .. }        =>    "FovOptix Port".to_string(), 
        }
    }
}
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Default)]
pub struct RLObservation {
    pub t_elapsed_s: f32, 
    pub last_target_bitrate_mbps: f32, 
    pub rtt_ms_avg_s: f32, 
    pub rtt_ms_std_s: f32, 

    // pub frame_size_mb_avg_s: f32, 
    pub bandwidth_mbps_avg_s: f32,
    pub bandwidth_mbps_std_s: f32, 
    pub frame_interarrival_avg_ms: f32, 
    pub frame_interarrival_std_ms: f32, 

    pub flr_avg_s: f32, 
    pub pl_sum_period: usize, 
    pub frame_size_avg_bytes: f32, 

    pub ow_delay_period_ewma: f32, 
    pub f_ow_delay_period_ewma: f32, 
    // pub buffer_level_avg_s: f32, 
    pub rebuffer_event_sum: u8, 

}


impl RLObservation {
    /// Converts the struct into a vector of f32s in a defined order.
    /// This is the key to preserving order across the language boundary.
    pub fn to_vec(&self) -> Vec<f32> {
        vec![
            self.t_elapsed_s,
            self.last_target_bitrate_mbps,
            self.rtt_ms_avg_s,
            self.rtt_ms_std_s,
            self.bandwidth_mbps_avg_s,
            self.bandwidth_mbps_std_s,
            self.frame_interarrival_avg_ms,
            self.frame_interarrival_std_ms,
            self.flr_avg_s,
            self.pl_sum_period as f32,

            self.frame_size_avg_bytes, 

            self.ow_delay_period_ewma, 
            self.f_ow_delay_period_ewma, 

            self.rebuffer_event_sum as f32, // Cast u8 to f32
        ]
    }
}




#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct RLObservationVector{
    observations: Vec<RLObservation>, 
    max_len: u8, 
    FEAT_DIM: usize, 
    
}

impl RLObservationVector{
    pub fn new(max_len: u8) -> Self { 
        // debug_assert_eq!(FEAT_DIM, RLObservation::default().to_vec().len());
        Self{ observations: Vec::new(), max_len, FEAT_DIM: 0, }}

    pub fn push(&mut self, o: RLObservation){
        
        if self.FEAT_DIM == 0{
            self.FEAT_DIM = o.to_vec().len(); 
        }
        
        
        self.observations.push(o); 
        if self.observations.len() as u8 > self.max_len {
            self.observations.remove(0); 
        }
    } 

    pub fn seq_len(&self) -> usize {
        self.observations.len()
    }

    /// Return a flat Vec<f32> of length (max_len * FEAT_DIM), left-padded with zeros.
    /// Layout: [o_{t-k+1}, ..., o_t] row-major, zeros for missing prefix.
    pub fn as_flat_padded(&self) -> Vec<f32> {
        let cap = self.max_len as usize;
        let mut out = vec![0.0f32; cap * self.FEAT_DIM];

        // copy rows to the tail to keep left padding at the front
        let len = self.observations.len();
        let start_row = cap.saturating_sub(len);
        for (i, o) in self.observations.iter().enumerate() {
            let row = o.to_vec();                    // len == FEAT_DIM
            let dst = (start_row + i) * self.FEAT_DIM;
            out[dst..dst + self.FEAT_DIM].copy_from_slice(&row[..self.FEAT_DIM]);
        }
        out
    }
}



#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RLTransition {
    pub sim_id: String,
    pub prev_obs:  Vec<f32>,
    pub action: usize,                  // snapped index (if continuous used)
    pub reward: f32,
    pub next_obs:  Vec<f32>,
    pub done: bool,
    #[serde(default)]
    pub cont_action_mbps: Option<f32>,  // NEW: raw continuous action, if any
}



// pub struct RLStep {
    // pub obs: RLObservationVector, 
    // pub reward: f32, 
    // pub done: bool, 
// }
pub trait RLConnector {
    fn select_action(&mut self, obs: &RLObservation) -> RLAction;
    fn post_transition(&mut self, transition: &RLTransition);
    fn reset_window(&mut self);
}

const RL_WINDOW_OBSERVATION_SIZE: usize = 5; 

#[derive(Debug, Clone)]
pub struct ObsWindow {
    buf: VecDeque<Vec<f32>>,
    cap: usize,
    FEAT_DIM: usize, // keep in sync with RLObservation::to_vec().len()

}

impl ObsWindow {
    pub fn new(cap: usize) -> Self {
        Self { buf: VecDeque::with_capacity(cap), cap, FEAT_DIM: 0,  }
    }

    pub fn clear(&mut self) { self.buf.clear(); }

    pub fn push_obs(&mut self, obs: &RLObservation) {

        if self.FEAT_DIM == 0{
            self.FEAT_DIM = obs.to_vec().len(); 
        }

        if self.buf.len() == self.cap {
            self.buf.pop_front();
        }
        self.buf.push_back(obs.to_vec());
    }

    /// Current logical sequence length (<= cap)
    pub fn seq_len(&self) -> usize { self.buf.len() }

    /// Return a flattened window of size (cap * FEAT_DIM), left-padded with zeros.
    /// Layout: [o_{t-k+1}, ..., o_{t}] row-major.
    pub fn as_flat_padded(&self) -> Vec<f32> {
        let mut out = vec![0.0f32; self.cap * self.FEAT_DIM];
        // copy the existing rows to the tail of out to keep left padding zeros
        let start_row = self.cap - self.buf.len();
        for (i, row) in self.buf.iter().enumerate() {
            let dst = (start_row + i) * self.FEAT_DIM;
            out[dst..dst + self.FEAT_DIM].copy_from_slice(&row[..self.FEAT_DIM]);
        }
        out
    }
}

#[derive(Serialize, Deserialize)]
pub struct RLRequestRNN {
    pub sim_id: String,
    pub obs_flat: Vec<f32>, // length = window_len * FEAT_DIM
    pub seq_len: u8,        // how many real steps (<= window_len)
    pub feat_dim: u8,       // = FEAT_DIM
    pub window_len: u8,     // fixed capacity N
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum RLResponse {
    Discrete { action_idx: usize },
    Continuous { bitrate_mbps: f32 },
}



#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ActionSpace {
    /// Classic discrete ladder indices: 0..N-1
    Discrete,

    /// Continuous action in Mbps (with optional snapping onto the ladder)
    Continuous {
        min_mbps: f32,
        max_mbps: f32,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RLAction {
    Discrete(usize),        // ladder index
    ContinuousMbps(f32),    // raw Mbps
}
fn idx_to_mbps(ladder: &[f32], idx: usize) -> (usize, f32) {
    let i = idx.min(ladder.len().saturating_sub(1));
    (i, ladder[i])
}

fn nearest_idx(ladder: &[f32], mbps: f32) -> usize {
    let mut best_i = 0usize;
    let mut best_d = f32::INFINITY;
    for (i, &r) in ladder.iter().enumerate() {
        let d = (r - mbps).abs();
        if d < best_d {
            best_d = d; best_i = i;
        }
    }
    best_i
}

fn snap_to_ladder(ladder: &[f32], mbps: f32) -> (usize, f32) {
        let i = nearest_idx(ladder, mbps);
        (i, ladder[i])
        }

pub struct ZmqConnector{
    action_socket: zmq::Socket, // REQ socket for blocking action selection
    step_socket: zmq::Socket,   // PUSH socket for sending (obs, reward, done) steps
    sim_id: String, 
    window: ObsWindow, 
}
impl ZmqConnector {
    // pub fn new(action_ep: &str, reward_ep: &str, ctx: &zmq::Context, simu_id: &str) -> Self {
    //     // --- Action Socket (DEALER) ---
    //     // let action_socket = ctx.socket(zmq::DEALER).expect("Failed to create DEALER socket");
    //     // action_socket
    //     //     .set_identity(simu_id.as_bytes())
    //     //     .expect("Failed to set DEALER identity");


    //     let action_socket = ctx.socket(zmq::DEALER).unwrap();
    //     action_socket.set_identity(simu_id.as_bytes()).unwrap();


    //     action_socket
    //         .set_rcvtimeo(60_000)
    //         .expect("Failed to set receive timeout");
       
    //     action_socket.connect(&action_ep).unwrap();
    
    //     println!("[ZmqConnector] Action DEALER connected to {} as {}", action_ep, simu_id);

    //     // --- Reward Socket (PUSH) ---
    //     let reward_socket = ctx.socket(zmq::PUSH).expect("Failed to create PUSH socket");
    //     reward_socket
    //         .connect(reward_ep)
    //         .expect("Failed to connect PUSH socket");
    //     println!("[ZmqConnector] Reward PUSH connected to {}", reward_ep);

    //     Self {
    //         action_socket,
    //         step_socket: reward_socket,
    //         sim_id: simu_id.to_string(),
    //         window: ObsWindow::new(window_len),
    //     }
    // }
    pub fn new(action_ep: &str, reward_ep: &str, ctx: &zmq::Context, simu_id: &str, window_len: usize) -> Self {
            let action_socket = ctx.socket(zmq::DEALER).unwrap();
            action_socket.set_identity(simu_id.as_bytes()).unwrap();
            action_socket.set_rcvtimeo(60_000).unwrap();
            action_socket.connect(action_ep).unwrap();
            println!("[ZmqConnector] Action DEALER connected to {} as {}", action_ep, simu_id);

            let reward_socket = ctx.socket(zmq::PUSH).unwrap();
            reward_socket.connect(reward_ep).unwrap();
            println!("[ZmqConnector] Reward PUSH connected to {}", reward_ep);

            Self {
                action_socket,
                step_socket: reward_socket,
                sim_id: simu_id.to_string(),
                window: ObsWindow::new(window_len),
            }
        }

}

#[derive(Serialize, Deserialize)]
pub struct RLRequest{ pub obs: Vec<f32>}
// #[derive(Serialize, Deserialize)]
// pub struct RLResponse{pub action_idx: usize}

impl RLConnector for ZmqConnector {
    fn select_action(&mut self, obs: &RLObservation) -> RLAction {
        // 1) Build prev window (no push yet)
        let prev_flat = self.window.as_flat_padded();

        let feat_dim = obs.to_vec().len(); 


        let req = RLRequestRNN {
            sim_id: self.sim_id.clone(),
            obs_flat: prev_flat,
            seq_len: self.window.seq_len() as u8,
            feat_dim: feat_dim as u8,
            window_len: self.window.cap as u8,
        };
        let request_json = serde_json::to_string(&req).expect("serialize RLRequestRNN");
        self.action_socket.send(request_json.as_bytes(), 0).expect("send obs window");

        // 2) recv action (discrete or continuous)
        let response_bytes = self.action_socket.recv_bytes(0).unwrap_or_else(|e| {
            if let zmq::Error::EAGAIN = e {
                eprintln!("RUST ERROR: Timed out waiting for action from Python agent!");
                panic!("ZMQ Timeout");
            }
            panic!("ZMQ Error: {}", e);
        });

        let parsed: RLResponse = serde_json::from_slice(&response_bytes).expect("deserialize RLResponse");

        // 3) push CURRENT obs now
        self.window.push_obs(obs);

        match parsed {
            RLResponse::Discrete { action_idx } => RLAction::Discrete(action_idx),
            RLResponse::Continuous { bitrate_mbps } => RLAction::ContinuousMbps(bitrate_mbps),
        }
    }

    fn post_transition(&mut self, transition: &RLTransition) {
        let transition_json = serde_json::to_string(transition).expect("Failed to serialize RLTransition");
        // println!(
        //     "Sending transition: sim_id {}, action {}, reward {:.2}",
        //     transition.sim_id, transition.action, transition.reward
        // );
        self.step_socket.send(transition_json.as_bytes(), 0).expect("Failed to send transition");
    }
    fn reset_window(&mut self) { self.window.clear(); }

}




#[allow(unused)]
#[derive(Clone, PartialEq)]
pub enum NestVrProfile {
    Custom {
        update_interval_nestvr_s: f32,
        bitrate_step_count: usize,
        bitrate_inc_steps: usize,
        bitrate_dec_steps: usize,
        rtt_adj_prob: f32,
        bitrate_inc_prob: f32,
        nfr_thresh: f32,
        rtt_thresh_ms: f32,
        capacity_scaling_factor: f32,
    },
    Balanced,
    Speedy,
    Anxious,
}

#[allow(unused)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProfileConfig {
    pub update_interval_nestvr_s: f32,

    pub max_bitrate_mbps: f32,
    pub min_bitrate_mbps: f32,
    pub initial_bitrate_mbps: f32,

    pub bitrate_step_count: usize,
    pub bitrate_inc_steps: usize,
    pub bitrate_dec_steps: usize,

    pub rtt_adj_prob: f32,
    pub bitrate_inc_prob: f32,

    pub nfr_thresh: f32,
    pub rtt_thresh_ms: f32,

    pub capacity_scaling_factor: f32,
}

impl Default for ProfileConfig {
    fn default() -> Self {
        ProfileConfig {
            update_interval_nestvr_s: 1.,

            max_bitrate_mbps: 100.,
            min_bitrate_mbps: 10.,
            initial_bitrate_mbps: 50.,

            bitrate_step_count: 9,
            bitrate_inc_steps: 1,
            bitrate_dec_steps: 1,

            rtt_adj_prob: 1.0,
            bitrate_inc_prob: 0.25,

            nfr_thresh: 0.99,
            rtt_thresh_ms: 22.,

            capacity_scaling_factor: 0.9,
        }
    }
}


#[derive(Clone)]
pub struct TimedVecFLR{
    vec_flr: VecDeque<(f32,usize)>,
    vec_shard_loss: VecDeque<(f32, usize)>, 
    period: f32, // how long to keep values 
}

impl TimedVecFLR{
    pub fn new(period: f32) -> Self {
        Self {
            vec_flr: VecDeque::new(),
            vec_shard_loss: VecDeque::new(),
            period,
        }
    }
    pub fn push_new(&mut self, fl_report: usize, sl_report: usize, time_f32: f32) {
        // Insert new values
        self.vec_flr.push_back((time_f32, fl_report));
        self.vec_shard_loss.push_back((time_f32, sl_report));

        // Prune old values outside the time window
        let cutoff = time_f32 - self.period;

        self.prune_old(cutoff);
    }


      fn prune_old(&mut self, cutoff: f32) {
        while let Some(&(t, _)) = self.vec_flr.front() {
            if t < cutoff {
                self.vec_flr.pop_front();
            } else {
                break;
            }
        }

        while let Some(&(t, _)) = self.vec_shard_loss.front() {
            if t < cutoff {
                self.vec_shard_loss.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn sum_flr(&mut self, time_f32: f32) -> usize {
        let cutoff = time_f32 - self.period;

        // Remove outdated entries
        self.prune_old(cutoff);

        // Sum *and remove* all currently stored FLR values (only once)
        let sum: usize = self.vec_flr.drain(..).map(|(_, v)| v).sum();
        sum
    }


    pub fn sum_shard_loss(&mut self, time_f32: f32) -> usize {
        let cutoff: f32 = time_f32 - self.period;
        self.prune_old(cutoff);
        self.vec_shard_loss.drain(..).map(|(_, v)| v).sum()
    }

}



#[derive(Clone)]
pub struct TimedVecBuffer{
    vec_buflevel: VecDeque<(f32,usize)>,
    period: f32, // how long to keep values 
}

impl TimedVecBuffer{
    pub fn new(period: f32) -> Self {
        Self {
            vec_buflevel: VecDeque::new(),
            period,
        }
    }
    pub fn push_new(&mut self, buflevel_report: usize, time_f32: f32) {
        // Insert new values
        self.vec_buflevel.push_back((time_f32, buflevel_report));

        // Prune old values outside the time window
        let cutoff = time_f32 - self.period;

        while let Some(&(t, _)) = self.vec_buflevel.front() {
            if t < cutoff {
                self.vec_buflevel.pop_front();
            } else {
                break;
            }
        }
    }
    pub fn avg_buffer_level_period(&self) -> f32 {
        if self.vec_buflevel.is_empty() {
            return 0.0;
        }

        let sum: f32 = self.vec_buflevel.iter().map(|&(_, v)| v as f32).sum();
        sum / self.vec_buflevel.len() as f32
    }

}



#[allow(unused)]
// #[derive(Clone)]
pub struct BitrateManager {
    last_frame_instant: TaiTime<0>,
    last_update_instant: TaiTime<0>,

    pub bitrate_mode: BitrateMode,
    frame_index: usize,

    frame_interval_average: SlidingWindowAverage<f32>,
    // encoder_latency_average: SlidingWindowAverage<f32>,
    // network_latency_average: SlidingWindowAverage<f32>,

    bitrate_average_mbps: SlidingWindowAverage<f32>,

    update_interval_s: Duration,

    rtt_average: SlidingWindowAverage<f32>,
    peak_throughput_average: SlidingWindowAverage<f32>,
    frame_interarrival_average: SlidingWindowAverage<f32>,
    everest_last_capacity: f32, 
    everest_last_throughput: f32, 

    everest_capacity_ewma: f32, 
    everest_throughput_ewma: f32, 
    everest_last_dshort: f32, 
    everest_last_dlong: f32, 
    everest_time_last_capacity_update: TaiTime<0>, 
    everest_time_last_throughput_update: TaiTime<0>, 
    everest_last_order: EverestCommand, 

    last_target_bitrate_bps: f32,


    bitrate_ladder_bps: Option<Vec<f32>>, 
    bitrate_step_size_bps_nest: f32, 
    flr_shardloss_count: TimedVecFLR,  
    jitbuf_avg_count: TimedVecBuffer,
    last_rebuffer_avg_sum: u8,   
    t_end_simulation: f64, 
    sim_unique_string: String, 
    
    
    pub last_nada_target_bitrate_mbps: Option<f64>, //updated on receive of NADA feedback data.  

    aimd_manager: Option<Arc<Mutex<FOAimdRateControl>>>, 

    framerate: f32, 

    ewma_fowd: f32, 
    ewma_owd:  f32, 
    bytes_size_avg: SlidingWindowAverage<f32>, 
}



impl BitrateManager {
    
     pub fn new(max_history_size: usize, initial_framerate: f32, initial_bitrate_mbps: f32, abr_enabled: usize, nest_vr_profile: &NestVrProfile, t_end_simu: f64, ip_server: IpAddr, sim_unique_string: &str, fps: f32, ) -> Self {
        
        let decrement: usize = match nest_vr_profile {
            NestVrProfile::Anxious => {10}, 
            NestVrProfile::Balanced => {1},
            NestVrProfile::Speedy => {2}, 
            NestVrProfile::Custom{..} => {1}, 
        };         
        
        let mut bitrate_ladder_std_bps = Vec::new(); 
        let bitrate_step_count = 9; 

        let max_mbps = MAX_MBPS_LADDER; 
        let min_mbps = 10.0; 

        let (min_bps, max_bps) = ( min_mbps * 1e6, max_mbps * 1e6); 
        // let initial_bitrate_mbps = 50.0; 

        let bitrate_step_size_bps_nest = (max_bps - min_bps) / bitrate_step_count as f32;


        let bitrate_mode = match abr_enabled{
            1 =>  {  // NeSt-VR
                
                    if max_bps != 0.0 && min_bps != 0.0 {
                        let mut vec_bitrates = Vec::new();

                        let bitrate_step_size_bps = (max_bps - min_bps) / bitrate_step_count as f32;

                        let mut last_value = min_bps;

                        vec_bitrates.push(min_bps); // first bitrate is min
                        
                        for _ in 0..bitrate_step_count {
                            last_value += bitrate_step_size_bps;
                            vec_bitrates.push(last_value);
                        }

                        bitrate_ladder_std_bps = vec_bitrates.clone(); 

                        // let bitrate_step_size_bps = bitrate_step_size_bps;
                            
                        // let last_target_bitrate_bps = upper_bound_bitrate(
                        //     initial_bitrate_mbps * 1e6,
                        //     &vec_bitrates, 
                        // );
                    }
                BitrateMode::NestVr { 
                    //     NestVr{
                        averaging_strategy: AveragingStrategy::SimpleWindowAverage { window_type: WindowType::BySeconds { sliding_window_secs: Some(BITRATE_UPDATE_INTERVAL as f32) } },            
                        max_bitrate_mbps: max_mbps,
                        min_bitrate_mbps: min_mbps,
                        initial_bitrate_mbps,
                        nest_vr_profile: ProfileConfig {
                                            update_interval_nestvr_s: BITRATE_UPDATE_INTERVAL as f32, 
                                            max_bitrate_mbps: max_mbps,
                                            min_bitrate_mbps: min_mbps,
                                            initial_bitrate_mbps: initial_bitrate_mbps,

                                            bitrate_step_count, 
                                            bitrate_inc_steps: 1, 
                                            bitrate_dec_steps: decrement, 

                                            rtt_adj_prob: 1.0,
                                            bitrate_inc_prob: 0.25, 
                                            nfr_thresh: 0.99,
                                            rtt_thresh_ms: 22.0, 
                                            capacity_scaling_factor: 0.9,},            
                    }
                }
            2 => {  
                    let values_original = [10.0, 20.0, 40.0, 60.0, 80.0, MAX_MBPS_LADDER]; 
                    let mut bitrate_ladder_mbps = Vec::new(); 
                    for value in values_original.iter(){
                        bitrate_ladder_mbps.push(*value as f32); 
                        bitrate_ladder_std_bps.push(*value * 1e6)
                    }                 
                    
                    BitrateMode::EVeREst { bitrate_ladder_mbps }
                }
            3 => { // RL 
                let ladder_mbps = (10..=MAX_MBPS_LADDER as usize).step_by(5).map(|x| x as f32).collect::<Vec<_>>();
                let ctx = zmq::Context::new();
                // print_yellow!("Ladder of Mbps values: {:?}", ladder_mbps); 

                let action_ep  = std::env::var("ZMQ_ACTION_EP").unwrap_or("ipc:///tmp/xr_default_action".into());
                let reward_ep  = std::env::var("ZMQ_STEP_EP").unwrap_or("ipc:///tmp/xr_default_step".into());

                let action_space = ActionSpace::Continuous { min_mbps: (1.0), max_mbps: (100.0) }; 


                BitrateMode::ReinforcementLearner {
                    bitrate_ladder_mbps: ladder_mbps,
                    step_interval: Duration::from_secs_f32(BITRATE_UPDATE_INTERVAL as f32),
                    connector: Arc::new(Mutex::new(Box::new(ZmqConnector::new(&action_ep, &reward_ep,  &ctx, sim_unique_string , RL_WINDOW_OBSERVATION_SIZE)))),
                    last_action_idx: Arc::new(Mutex::new(0)),
                    last_decision_instant: Arc::new(Mutex::new(TaiTime::EPOCH)),
                    pending_obs: Arc::new(Mutex::new(Some(RLObservationVector::new(8)))),
                    action_space, 
                } 
            }

            4 => { // GCC estimator. 

                let gcc_estimator = GccBandwidthEstimator::new(initial_framerate as f64); // make period be adaptive to framerate. 
                BitrateMode::GCCPort { gcc_estimator, framerate: initial_framerate as f64}
            }
            5 => {
                BitrateMode::NADACiscoPort{ }
            }
            6 => {
                BitrateMode::FovOptixPort { last_bitrate_mbps : initial_bitrate_mbps }
            }


            _ => BitrateMode::ConstantMbps(initial_bitrate_mbps)
        };          

        print_green!("Server {} has BitrateMode => {:?}", ip_server, bitrate_mode.variant_name());

        let flr_vec: TimedVecFLR = TimedVecFLR::new( 1.0 as f32); // let's take FLR 1 sec sliding window. TODO: input arg 
        let buflevel_vec =  TimedVecBuffer::new( BITRATE_UPDATE_INTERVAL as f32); 

        Self {
            last_frame_instant: TaiTime::EPOCH,
            last_update_instant: TaiTime::EPOCH,

            frame_index: 0,
            frame_interval_average: SlidingWindowAverage::new(0.0, max_history_size),
            // encoder_latency_average: SlidingWindowAverage::new(0.0, max_history_size), // Unused in this simulator. 
            // network_latency_average: SlidingWindowAverage::new(0.0, max_history_size),

            bitrate_average_mbps: SlidingWindowAverage::new(initial_bitrate_mbps, max_history_size),
            // last_target_bitrate_mbps: initial_bitrate_mbps,
            update_interval_s: Duration::from_secs_f64(BITRATE_UPDATE_INTERVAL),

            rtt_average: SlidingWindowAverage::new(Duration::from_millis(5).as_secs_f32(), max_history_size),
            peak_throughput_average: SlidingWindowAverage::new(300E6, max_history_size),
            frame_interarrival_average: SlidingWindowAverage::new(
                1. / initial_framerate,
                max_history_size,
            ),
            // abr_enabled_ev: abr_enabled, 
            everest_last_capacity: 0.0, 
            everest_last_throughput: 0.0, 
            everest_last_dlong: 0.0, 
            everest_last_dshort: 0.0,  
            everest_capacity_ewma: 0.0, 
            everest_throughput_ewma: 0.0, 
            everest_time_last_capacity_update: TaiTime::EPOCH, 
            everest_time_last_throughput_update: TaiTime::EPOCH, 
            bitrate_mode,
            last_target_bitrate_bps: initial_bitrate_mbps * 1e6,
            bitrate_ladder_bps: Some(bitrate_ladder_std_bps) , 
            bitrate_step_size_bps_nest, 
            everest_last_order: EverestCommand::Continue, 
            flr_shardloss_count: flr_vec, 
            jitbuf_avg_count: buflevel_vec, 
            last_rebuffer_avg_sum: 0, 
            t_end_simulation: t_end_simu, 
            sim_unique_string: sim_unique_string.to_string(), 

            last_nada_target_bitrate_mbps: None, 
            aimd_manager: None, 
            framerate: fps, 

            ewma_fowd: 0.0, 
            ewma_owd: 0.0, 
            bytes_size_avg: SlidingWindowAverage::new(initial_bitrate_mbps / initial_framerate * 1e6,
                        max_history_size )
        }
    }

      pub fn reset(&mut self) {
        // Reset timestamps and counters
        self.last_frame_instant = TaiTime::EPOCH;
        self.last_update_instant = TaiTime::EPOCH;
        self.frame_index = 0;

        // Clear sliding window averages
        self.frame_interval_average.clear();
        // self.encoder_latency_average.clear();
        // self.network_latency_average.clear();
        self.rtt_average.clear();
        self.peak_throughput_average.clear();
        self.frame_interarrival_average.clear();
        self.bitrate_average_mbps.clear();
        self.ewma_owd = 0.0; 
        self.ewma_fowd = 0.0; 
        self.bytes_size_avg.clear(); 


        // Reset bitrate state
        match &mut self.bitrate_mode {
            BitrateMode::ConstantMbps(init_mbps) => {
                self.last_target_bitrate_bps = *init_mbps * 1e6;
            }
            BitrateMode::EVeREst { bitrate_ladder_mbps } => {
                self.last_target_bitrate_bps = bitrate_ladder_mbps[0] * 1e6;
            }
            BitrateMode::NestVr { min_bitrate_mbps, .. } => {
                self.last_target_bitrate_bps = *min_bitrate_mbps * 1e6;
            }
            BitrateMode::ReinforcementLearner {
                last_action_idx,
                last_decision_instant,
                pending_obs,
                bitrate_ladder_mbps,
                ..
            } => {
                self.last_target_bitrate_bps = bitrate_ladder_mbps[0] * 1e6;
                *last_action_idx.lock().unwrap() = 0;
                *last_decision_instant.lock().unwrap() = TaiTime::EPOCH;
                *pending_obs.lock().unwrap() = Some(RLObservationVector::new(8));
            }
            BitrateMode::GCCPort { gcc_estimator , framerate} => {
                self.last_target_bitrate_bps = GCC_INIT_CONFIGURED_BITRATE as f32 * 1e6;
                *gcc_estimator = GccBandwidthEstimator::new(*framerate);
            }

            BitrateMode::NADACiscoPort { ..  } => {

                self.last_target_bitrate_bps = NADA_INITIAL_RATE as f32; // in bps
                // last_order_bitrate_mbps = 
                // }
            }
            
            BitrateMode::FovOptixPort { .. } => {
                if self.aimd_manager.is_none(){
                    self.aimd_manager = Some(Arc::new(Mutex::new(FOAimdRateControl::new(true)))); 
                }
            }
        }

        // Reset Everest/Nest state
        self.everest_last_capacity = 0.0;
        self.everest_last_throughput = 0.0;
        self.everest_last_dlong = 0.0;
        self.everest_last_dshort = 0.0;
        self.everest_capacity_ewma = 0.0;
        self.everest_throughput_ewma = 0.0;
        self.everest_time_last_capacity_update = TaiTime::EPOCH;
        self.everest_time_last_throughput_update = TaiTime::EPOCH;
        self.everest_last_order = EverestCommand::Continue;

        crate::print_blue!(
            "[BitrateManager] Reset complete -> bitrate = {:.2} Mbps",
            self.last_target_bitrate_bps / 1e6
        );
    }     

    pub fn report_encoded_frame_server(&mut self, now: TaiTime<0>) {
        print_prettyy!(
            DebugColor::Purple,
            "[bitrateManager] submitted encoded frame. avg_fps = {}",
            1.0 / self.frame_interval_average.get_average()
        );
        if self.last_frame_instant != TaiTime::EPOCH {
            let dur = now.duration_since(self.last_frame_instant).as_secs_f32();
            self.frame_interval_average.submit_sample(dur);
        }
        self.last_frame_instant = now;
    }

    pub fn report_network_statistics_abr(
        &mut self,
        network_rtt: Duration,
        peak_throughput_bps: f32,
        frame_interarrival_s: f32,
        network_stats: NetworkStatisticsPacket, 
        now: TaiTime<0>,
        send_instant: TaiTime<0>, 
    ) {


        match &mut self.bitrate_mode{
            
            BitrateMode::GCCPort { gcc_estimator, ..} => {

                let current_frame_send_timestamp = taitime_to_f64!(send_instant) * 1e6; // input units: micros 
                let current_frame_arrival_timestamp = taitime_to_f64!(now) * 1e6;       // input units: micros 
                let current_frame_size = network_stats.bytes_in_frame; 
                let _target_bitrate_bps = gcc_estimator.Update(current_frame_send_timestamp , current_frame_arrival_timestamp, current_frame_size as i64, now); 
                // target now unused, then retrieved during one_pass_abr()  
            }
            
            _ => {}, 
        }


        let fowd = network_stats.filtered_ow_delay; 
        let owd = network_stats.ow_delay; 
        
        self.ewma_fowd = ALPHA_EWMA_FOWD_OBS * fowd + (1.0 - ALPHA_EWMA_FOWD_OBS) * self.ewma_fowd;   // used for RL observations   
        self.ewma_owd = ALPHA_EWMA_FOWD_OBS * owd + (1.0 - ALPHA_EWMA_FOWD_OBS) * self.ewma_owd;      // used for RL observations

        self.rtt_average.submit_sample(network_rtt.as_secs_f32());

        self.peak_throughput_average.submit_sample(peak_throughput_bps);
        self.frame_interarrival_average.submit_sample(frame_interarrival_s);
        self.bytes_size_avg.submit_sample(network_stats.bytes_in_frame as f32);

        self.jitbuf_avg_count.push_new(network_stats.buffer_level_decoder as usize, now.duration_since(TaiTime::EPOCH).as_secs_f32());
        self.last_rebuffer_avg_sum = network_stats.rebuffering_events_last_s; // discrete, no averaging. It's counted on the XRClient and sent over UL messages.


        /// EVEREST METRICS COMPUTE ///////
        const T_USER_WIN : f32 = 5.0; // from original Everest paper 

        let everest_capacity_sample = network_stats.everest_capacity_update; 
        let everest_throughput_sample = network_stats.everest_throughput_update; 

        if everest_capacity_sample > 0.0 { // we only get one or the other, which is computed is based on frame size. They are initialized to -1.0, only positive samples count 
            self.everest_last_capacity = everest_capacity_sample;  
            let last_c_f32 = now.duration_since(self.everest_time_last_capacity_update).as_secs_f32(); 
            
            self.everest_capacity_ewma = last_c_f32 / T_USER_WIN * everest_capacity_sample + (1.0 - last_c_f32 / T_USER_WIN ) * self.everest_capacity_ewma; 
            self.everest_time_last_capacity_update = now; 
        }

        if everest_throughput_sample > 0.0{
            self.everest_last_throughput = everest_throughput_sample;  
            let last_t_f32 = now.duration_since(self.everest_time_last_throughput_update).as_secs_f32(); 
            
            self.everest_throughput_ewma = last_t_f32 / T_USER_WIN * everest_throughput_sample + (1.0 - last_t_f32 / T_USER_WIN ) * self.everest_throughput_ewma;  
            self.everest_time_last_throughput_update = now; 
        } 

        self.everest_last_dshort = network_stats.everest_dshort; 
        self.everest_last_dlong = network_stats.everest_dlong; 
        self.everest_last_order = network_stats.everest_command; 
        
        // if matches!(self.bitrate_mode , BitrateMode::EVeREst{ .. }) && now.duration_since(self.last_update_instant) >= Duration::from_secs_f64(BITRATE_UPDATE_INTERVAL) {
        //     print_pink!("Everest Stats:\nCapacity={:.4} mbps,\nThroughput={:.4} mbps,\nD_short={},\nD_long={},\n\n",self.everest_capacity_ewma / 1e6, self.everest_throughput_ewma / 1e6,  network_stats.everest_dshort, network_stats.everest_dlong,  ); 
        // }
    }   

    pub fn report_shard_and_frame_loss(&mut self, fl: usize, sl: usize, timestep_f32: f32 ,){
        

        // println!("{}%%%%%%%% -> Pushing F: [{}], SL: {}  ", timestep_f32 , fl, sl); 
        
        self.flr_shardloss_count.push_new(fl, sl, timestep_f32);
    }

    pub fn one_pass_abr(&mut self, now: TaiTime<0>, ip_server: IpAddr) -> f32 {
        const TIME_WARMUP_ABR: u64 = 5; 

        if now.duration_since(TaiTime::EPOCH) < Duration::from_secs(TIME_WARMUP_ABR){
            println!("No ABR (warmup) {} -> {}. Mode: {}", format_elapsed!(now), TIME_WARMUP_ABR, self.bitrate_mode.variant_name()); 
            let bitrate_bps = self.last_target_bitrate_bps; 
            bitrate_bps 
        }
        else{
            // println!("One pass ABR"); 

            let obs= self.build_rl_observation(now); // do it here so borrow checker is happy

            if let BitrateMode::GCCPort { ref mut gcc_estimator , ..} = self.bitrate_mode {
                let bitrate_bps = gcc_estimator.get_target_bitrate_bps();
                self.last_target_bitrate_bps = bitrate_bps as f32;  // Done here because only in this case we need mut access to gcc_estimator. 
            }

            let bitrate_bps = match &self.bitrate_mode { // match all other cases. 
                BitrateMode::ConstantMbps(bitrate_mbps) => {
                    self.last_target_bitrate_bps = *bitrate_mbps as f32 * 1E6;
                    // self.last_target_bitrate_mbps = *bitrate_mbps as f32;

                    print_prettyy!(DebugColor::Navy, "[{}] CBR -> Bitrate = {} Mbps", ip_server ,bitrate_mbps);

                    *bitrate_mbps as f32 * 1e6
                }

                BitrateMode::EVeREst { bitrate_ladder_mbps }
                    => {
                        let mut bitrate_bps = self.last_target_bitrate_bps; 
                        // print_red!("bitrate first: {} Mbps", bitrate_bps / 1e6);  
                        
                        let current_mbps = (self.last_target_bitrate_bps as f32) / 1e6;
                        let new_mbps = match self.everest_last_order {
                            EverestCommand::Continue => {
                                // stay on the same rung (or the closest one)
                                bitrate_ladder_mbps
                                    .iter()
                                    .find(|&&x| (x - current_mbps).abs() < std::f32::EPSILON)
                                    .copied()
                                    .unwrap_or(current_mbps)
                            }
                            EverestCommand::SpeedUp => {
                                // first entry strictly greater than current
                                bitrate_ladder_mbps
                                    .iter()
                                    .find(|&&x| x > current_mbps)
                                    .copied()
                                    .unwrap_or(*bitrate_ladder_mbps.last().unwrap())
                            }
                            EverestCommand::SlowDown => {
                                // last entry strictly less than current
                                bitrate_ladder_mbps
                                    .iter()
                                    .rfind(|&&x| x < current_mbps)
                                    .copied()
                                    .unwrap_or(bitrate_ladder_mbps[0])
                            }
                        };
                        bitrate_bps = new_mbps * 1e6; 

                        let n_users = (self.everest_capacity_ewma / self.everest_throughput_ewma ).ceil() as usize ; 
                        let capacity_margin_bps = self.everest_capacity_ewma / (n_users as f32 + 1.0);  

                        bitrate_bps = f32::min(capacity_margin_bps, bitrate_bps ); 
                        if let Some(ladder) = &self.bitrate_ladder_bps{
                            bitrate_bps = upper_bound_bitrate(bitrate_bps, ladder);   
                        }
                        else{
                            print_red!( "t: {:.6} -> no bitrate ladder? ", format_elapsed!(now)); 
                        }
                        // if now.duration_since(self.last_update_instant) >= Duration::from_secs_f64(BITRATE_UPDATE_INTERVAL){
                        //     print_pink!("[Everest {}] N_users=  == {}, Capacity_margin={}\nBitrate={:.3}", ip_server, n_users, capacity_margin_bps/1e6, bitrate_bps / 1e6); 

                        // }
                        self.last_target_bitrate_bps = bitrate_bps; 
                        
                        bitrate_bps
                    }

                BitrateMode::NestVr {
                    max_bitrate_mbps,
                    min_bitrate_mbps,
                    // initial_bitrate_mbps,
                    nest_vr_profile,
                    ..
                } => {
                    
                    // print_pink!(
                    //     // DebugColor::Purple,
                    //     "{} ONE PASS OF NEST-VR! Bitrate: {} Mbps",
                    //     format_elapsed!(now), self.last_target_bitrate_bps / 1e6,  
                    // );

                    let (max_bps, min_bps) = (max_bitrate_mbps * 1e6, min_bitrate_mbps * 1e6); 

                    let profile_config = nest_vr_profile; 
                    // Sample from uniform distribution
                    let mut rng = rand::thread_rng();
                    let uniform_dist = Uniform::new(0.0, 1.0);

                    let r_rtt = rng.sample(uniform_dist);
                    let r_inc = rng.sample(uniform_dist);

                    let frame_interval_s = f32::max(self.frame_interval_average.get_average(), 1e-9);

                    let fps_tx_avg = if frame_interval_s != 0.0 {
                        1.0 / frame_interval_s
                    } else {
                        0.0
                    };

                    let fps_rx_avg = if self.frame_interarrival_average.get_average() != 0.0 {
                        1.0 / f32::max(1e-9, self.frame_interarrival_average.get_average()) 
                    } else {
                        0.0
                    };

                    let nfr_avg = fps_rx_avg / fps_tx_avg;
                    let rtt_avg_ms = self.rtt_average.get_average() * 1000.0;

                    let estimated_capacity_bps = f32::max(self.peak_throughput_average.get_average(), 1e-9);

                    let mut bitrate_bps: f32 = self.last_target_bitrate_bps;

                    // print_yellow!("nfr_avg = {}, rtt_avg = {} ms, r_inc = {}, r_rtt = {}, STEP SIZE = {} Mbps", nfr_avg, rtt_avg_ms, r_inc, r_rtt, self.bitrate_step_size_bps_nest / 1e6 ); 

                    if nfr_avg < profile_config.nfr_thresh {
                        // decrease
                        // print_yellow!("decrease (nfr_thresh)",); 

                        bitrate_bps -=
                            profile_config.bitrate_dec_steps as f32 * self.bitrate_step_size_bps_nest;
                    } else {
                        if rtt_avg_ms > profile_config.rtt_thresh_ms {
                            if r_rtt <= profile_config.rtt_adj_prob {
                                // decrease
                                // print_yellow!("decrease (rtt prob)",); 

                                bitrate_bps -= profile_config.bitrate_dec_steps as f32
                                    * self.bitrate_step_size_bps_nest;
                            }
                        } else {
                            if r_inc <= profile_config.bitrate_inc_prob {
                                // increase
                                // print_yellow!("INCREASE (rtt prob)",); 

                                bitrate_bps += profile_config.bitrate_inc_steps as f32
                                    * self.bitrate_step_size_bps_nest;
                            }
                        }
                    }
                    // print_pink!("bitrate after Nest: {} Mbps", f32::min( f32::max(bitrate_bps / 1e6, *min_bitrate_mbps), *max_bitrate_mbps )); 
                    // Ensure bitrate is below the estimated network capacity
                    let capacity_upper_limit =
                        profile_config.capacity_scaling_factor * estimated_capacity_bps;

                    bitrate_bps = f32::min(bitrate_bps, capacity_upper_limit);
                    // Ensure bitrate is always within the configured range
                    bitrate_bps = minmax_bitrate(bitrate_bps, max_bps, min_bps);

                    // print_red!("bitrate ladder: {:?}", self.bitrate_ladder_bps); 

                    bitrate_bps =
                        upper_bound_bitrate(bitrate_bps, &self.bitrate_ladder_bps.clone().unwrap());

                    let heur_stats = HeuristicStats {
                        bitrate_step_count: profile_config.bitrate_step_count,

                        bitrate_dec_steps: profile_config.bitrate_dec_steps,
                        bitrate_inc_steps: profile_config.bitrate_inc_steps,

                        bitrate_step_size_mbps: self.bitrate_step_size_bps_nest / 1e6,

                        r_rtt: r_rtt,
                        r_inc: r_inc,

                        rtt_adj_prob: profile_config.rtt_adj_prob,
                        bitrate_inc_prob: profile_config.bitrate_inc_prob,

                        fps_tx_avg: fps_tx_avg,
                        fps_rx_avg: fps_rx_avg,

                        nfr_avg: nfr_avg,
                        rtt_avg_ms: rtt_avg_ms,

                        nfr_thresh: profile_config.nfr_thresh,
                        rtt_thresh_ms: profile_config.rtt_thresh_ms,

                        requested_bitrate_mbps: bitrate_bps / 1e6,
                        estimated_capacity_mbps: estimated_capacity_bps / 1e6, 
                    };

                    // print_pink!(
                    //     // DebugColor::Purple,
                    //     "[{}]NeSt-VR STATS-------: {:#?}",
                    //     ip_server, 
                    //     heur_stats
                    // );
                    self.last_target_bitrate_bps = bitrate_bps;
                    // self.last_target_bitrate_mbps = bitrate_bps / 1E6; 
                    bitrate_bps
                }
            
                BitrateMode::ReinforcementLearner {
                    bitrate_ladder_mbps,
                    step_interval,
                    connector,
                    last_action_idx,
                    last_decision_instant,
                    pending_obs,
                    action_space,
                } => {
                    // Respect step interval
                    if now.duration_since(*last_decision_instant.lock().unwrap()) < *step_interval {
                        return self.last_target_bitrate_bps;
                    }

                    // Build current obs (already computed above as `obs`)
                    let current_obs = obs.clone();

                    // Take history out, or create new
                    let mut history = pending_obs
                        .lock().unwrap()
                        .take()
                        .unwrap_or_else(|| RLObservationVector::new(RL_WINDOW_OBSERVATION_SIZE as u8));

                    // Window BEFORE pushing current obs
                    let prev_win_flat = history.as_flat_padded();

                    // Ask the agent
                    let action = connector.lock().unwrap().select_action(&current_obs);

                    // Reward/Done
                    let reward = self.rl_naive_reward_function(&current_obs);
                    let done = now.duration_since(TaiTime::EPOCH).as_secs_f64() >= self.t_end_simulation;
                    let prev_idx_logged = *last_action_idx.lock().unwrap();

                    // Push current obs, compute next window
                    history.push(current_obs.clone());
                    let next_win_flat = history.as_flat_padded();

                    // Map action -> (final_mbps, snapped_idx)
                    let (final_mbps, snapped_idx, cont_raw_opt) = match (action_space, action) {
                        (ActionSpace::Discrete, RLAction::Discrete(i)) => {
                            let (idx, mbps) = idx_to_mbps(bitrate_ladder_mbps, i);
                            (mbps, idx, None)
                        }
                        (ActionSpace::Discrete, RLAction::ContinuousMbps(v)) => {
                            // Defensive: if Python sends continuous while env expects discrete,
                            // snap to nearest rung.
                            let (idx, mbps) = snap_to_ladder(bitrate_ladder_mbps, v);
                            (mbps, idx, Some(v))
                        }
                        (ActionSpace::Continuous { min_mbps, max_mbps }, RLAction::Discrete(i)) => {
                            // Map ladder index to evenly-spaced value over [min,max]
                            let n = bitrate_ladder_mbps.len().max(2);
                            let alpha = (i as f32) / ((n - 1) as f32);
                            let raw = (min_mbps + alpha * (max_mbps - min_mbps)).clamp(*min_mbps, *max_mbps);
                            let idx = nearest_idx(bitrate_ladder_mbps, raw);
                            (raw, idx, Some(raw))
                        }
                        (ActionSpace::Continuous { min_mbps, max_mbps }, RLAction::ContinuousMbps(v)) => {
                            let raw = v.clamp(*min_mbps, *max_mbps);
                            
                                let idx = nearest_idx(bitrate_ladder_mbps, raw);
                                (raw, idx, Some(raw))
                            
                        }
                    };

                    // Post transition (send snapped index for compatibility + optional raw)
                    let transition = RLTransition {
                        sim_id: self.sim_unique_string.clone(),
                        prev_obs: prev_win_flat,
                        action: prev_idx_logged,
                        reward,
                        next_obs: next_win_flat,
                        done,
                        cont_action_mbps: cont_raw_opt,
                    };
                    connector.lock().unwrap().post_transition(&transition);

                    // Store updated history / action / instant
                    if done {
                        *pending_obs.lock().unwrap() = Some(RLObservationVector::new(RL_WINDOW_OBSERVATION_SIZE as u8));
                    } else {
                        *pending_obs.lock().unwrap() = Some(history);
                    }
                    *last_action_idx.lock().unwrap() = snapped_idx;
                    *last_decision_instant.lock().unwrap() = now;

                    // Apply bitrate
                    self.last_target_bitrate_bps = final_mbps * 1e6;

                    match cont_raw_opt {
                        Some(raw) => {print_blue!(
                            "[{} RL {}] Action: raw={:.2} Mbps -> final={:.2} Mbps (idx={})",
                            format_elapsed!(now), ip_server, raw, final_mbps, snapped_idx
                            );
                        },
                        None => {print_blue!(
                            "[{} RL {}] Action: idx={} -> final={:.2} Mbps",
                            format_elapsed!(now), ip_server, snapped_idx, final_mbps
                            );
                        },
                    }

                    self.last_target_bitrate_bps
                }

                BitrateMode::NADACiscoPort {} => {
                    if let Some(last_order_bitrate_mbps ) = self.last_nada_target_bitrate_mbps{
                        let bitrate_bps = last_order_bitrate_mbps * 1e6; 
                        self.last_target_bitrate_bps = bitrate_bps as f32; 

                        self.last_target_bitrate_bps
                    }
                    else{
                        panic!("WHATS HAPPPPPPPPPPENING CATCH!"); 
                        self.last_target_bitrate_bps 
                    }
                }, 

                BitrateMode::FovOptixPort { .. } => {

                    // println!("Value is configured BEFORE one pass ABR: {:.2} Mbps", self.last_target_bitrate_bps / 1e6); 
                    self.last_target_bitrate_bps
                }

                // BitrateMode::FovOptixPort { last_bitrate_mbps }{

                //     if let Some(guard) = self.aimd_manager.unwrap().lock().unwrap(); 
                //     {
                //         bitrate_bps=guard.flag_for_qp;
                //         let nol=guard.normalize_delta;
                //         let tps=guard.current_bitrate_/72./8.;
                //     }
                // }
                _ => {
                    self.last_target_bitrate_bps
                }
            };
            print_prettyy!(
                DebugColor::Purple,
                " Bitrate chosen -> {:.3} mbps  (last = {:.2})",
                bitrate_bps / 1e6,
                self.last_target_bitrate_bps / 1e6
            );
            bitrate_bps
        }
    }


    pub fn build_rl_observation (&mut self, now: TaiTime<0>) -> RLObservation{
        match self.bitrate_mode{
            BitrateMode::ReinforcementLearner { .. } => {} //do nothing
            _ => { return RLObservation::default();}       // return early. 
        }; 
        let t_elapsed_s = now.duration_since(TaiTime::EPOCH).as_secs_f32(); 
        let last_target_bitrate_mbps = self.last_target_bitrate_bps * 1e-6; 
        let rtt_ms_avg_s = self.rtt_average.get_average() * 1000.0; 
        let rtt_ms_std_s = self.rtt_average.get_std() * 1000.0; 

        let bandwidth_mbps_avg_s = self.peak_throughput_average.get_average() * 1e-6; 
        let bandwidth_mbps_std_s =      self.peak_throughput_average.get_std() * 1e-6; 
        let frame_interarrival_avg_ms = self.frame_interarrival_average.get_average() * 1000.0; 
        let frame_interarrival_std_ms = self.frame_interarrival_average.get_std() * 1000.0; 

        // let flr_avg_s = (self.flr_shardloss_count.sum_flr(now.duration_since(TaiTime::EPOCH).as_secs_f32() ) as f32 / 
        //         (1.0 / self.framerate )) ; // percentage according to encoded frames window average, 
        //  
        let window_s = self.flr_shardloss_count.period; // the window period (e.g., 1 s)
        let frames_sent_expectation = self.framerate * window_s;
        let t_elapsed = now.duration_since(TaiTime::EPOCH).as_secs_f32(); 
        let frames_lost = self.flr_shardloss_count.sum_flr(t_elapsed) as f32;
        let pl_lost_period    = self.flr_shardloss_count.sum_shard_loss(t_elapsed); 

        // normalize to a ratio [0.0, 1.5]
        let flr_avg_s = (frames_lost / frames_sent_expectation ).min(1.5); // (not in the same period though, watch out). Saturate at 1.5 to not make ultralarge

        // let buffer_level_avg_s = self.jitbuf_avg_count.avg_buffer_level_period(); 
        let rebuffer_event_sum = self.last_rebuffer_avg_sum; 

        RLObservation{
            t_elapsed_s,
            last_target_bitrate_mbps,
            rtt_ms_avg_s,
            rtt_ms_std_s, 
            bandwidth_mbps_avg_s,
            bandwidth_mbps_std_s,
            frame_interarrival_avg_ms,
            frame_interarrival_std_ms,  
            flr_avg_s,
            pl_sum_period: pl_lost_period, 
            frame_size_avg_bytes: self.bytes_size_avg.get_average(), 
            ow_delay_period_ewma: self.ewma_owd, 
            f_ow_delay_period_ewma: self.ewma_fowd, 
            // buffer_level_avg_s, 
            rebuffer_event_sum, 
        }

    }

    pub fn vmaf_manual_function( &self, bitrate_mbps: f32 ) -> f32{ // values obtained empirically by scipy curve_fit via VMAF on bitrate ladder
                             // Snow sample, intra-refresh against 100 Mbps (median fit)
        100.0 - 89.40 * (-0.0615 * bitrate_mbps).exp()
    }

    pub fn rl_naive_reward_function(&self, obs: &RLObservation) -> f32 { // only first term is positive, others are penalties. 

        let alpha = 0.01; // bitrate 0 to 100 -> 0 to 1 
        let beta = 1.0;   // flr 0 to 1
        let gamma = - 0.04;       // rtt ~2 to 50 ms -> 0 to - 2
        let omega = - 1.0 / 90.0 ;     // rebuffering events: 90 -> -1 too 

        let bitrate_term = alpha * self.vmaf_manual_function(obs.last_target_bitrate_mbps);
        let flr_term = beta * (1.0 - obs.flr_avg_s).max(-3.0); // bound negative rewards. 
        let rtt_term = gamma * obs.rtt_ms_avg_s;
        let rebuffer_term = omega * obs.rebuffer_event_sum as f32;  

        let mut reward = bitrate_term + flr_term + rtt_term + rebuffer_term as f32; 
        reward = f32::max(reward, -1.0); // clip rewards to 0 
        reward
    }

    pub fn rl_reward_function(&self, obs: &RLObservation) -> f32 {
        let alpha = 0.05; // bitrate 0 to 100 -> 0 to 1 

        println!("#######################\nrtt_ms:{} , flr: {}  ######################\n", obs.rtt_ms_avg_s, obs.flr_avg_s); 

        let reward = if obs.flr_avg_s <= 0.05 {
            if obs.rtt_ms_avg_s <= 40.0 { // let's use this manual MTP threshold
                self.vmaf_manual_function(obs.last_target_bitrate_mbps) * alpha
            }
            else{
                0.0}
        }
        else{0.0}; 
        reward
    }
}

// static BITRATE_MANAGER: Lazy<Mutex<BitrateManager>> =
//     Lazy::new(|| Mutex::new(BitrateManager::new(256, 60.0, 30.0)));

#[allow(dead_code)]
pub type OptLazy<T> = Lazy<Mutex<Option<T>>>;

#[allow(dead_code)]
pub const fn lazy_mut_none<T>() -> OptLazy<T> {
    Lazy::new(|| Mutex::new(None))
}


#[derive(Default)]
struct TrackingData {
    v_timestamp: Vec<String>,
    v_device_id: Vec<u64>,
    v_pos_x: Vec<f32>,
    v_pos_y: Vec<f32>,
    v_pos_z: Vec<f32>,
    v_interarrival: Vec<f32>,
}

pub struct CsvTracking {
    csv_data: Arc<Mutex<TrackingData>>,
    writer: Arc<Mutex<BufWriter<std::fs::File>>>,
    batch_size: usize,
}

impl CsvTracking {
    pub fn new(folder_name: &str, num_id: u8) -> std::io::Result<Self> {
        let dir = format!("Results/{}", folder_name);
        let _ = std::fs::create_dir_all(&dir);

        let file_path = format!("{}/TRACKING_stats{}.csv", dir, num_id);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;
        let mut buf = BufWriter::new(file);

        // Write header if file is empty
        if Path::new(&file_path).metadata()?.len() == 0 {
            buf.write_all(
                b"timestamp,device_id,pos_x,pos_y,pos_z,interarrival_ms\n",
            )?;
            buf.flush()?;
        }

        Ok(Self {
            csv_data: Arc::new(Mutex::new(TrackingData::default())),
            writer: Arc::new(Mutex::new(buf)),
            batch_size: 100,
        })
    }

    pub fn update_stats(
        &self,
        now: TaiTime<0>,
        device_id: u64,
        pos: [f32; 3],
        interarrival_ms: f32,
    ) {
        let ts_str = format_elapsed!(now);

        {
            let mut data = self.csv_data.lock().unwrap();
            data.v_timestamp.push(ts_str);
            data.v_device_id.push(device_id);
            data.v_pos_x.push(pos[0]);
            data.v_pos_y.push(pos[1]);
            data.v_pos_z.push(pos[2]);
            data.v_interarrival.push(interarrival_ms);

            if data.v_timestamp.len() >= self.batch_size {
                // flush while holding data, but drop it before writing
                drop(data);
                if let Err(e) = self.flush_batch() {
                    eprintln!("[TRACKING] Error flushing batch: {}", e);
                }
            }
        }
    }

    pub fn flush_batch(&self) -> std::io::Result<()> {
        let mut data = self.csv_data.lock().unwrap();
        let mut writer = self.writer.lock().unwrap();

        for i in 0..data.v_timestamp.len() {
            let row = format!(
                "{},{},{},{},{},{}\n",
                data.v_timestamp[i],
                data.v_device_id[i],
                data.v_pos_x[i],
                data.v_pos_y[i],
                data.v_pos_z[i],
                data.v_interarrival[i]
            );
            writer.write_all(row.as_bytes())?;
        }
        writer.flush()?;

        // Clear buffer
        data.v_timestamp.clear();
        data.v_device_id.clear();
        data.v_pos_x.clear();
        data.v_pos_y.clear();
        data.v_pos_z.clear();
        data.v_interarrival.clear();

        Ok(())
    }
}


#[derive(Debug)]
struct TrackingLog {
    device_id: u64,
    position: Vec3,
    orientation: Quat,
    linear_velocity: Vec3,
}

#[allow(unused)]
pub struct XRServer {
    pub ip_self: IpAddr,
    pub ip_client: IpAddr,
    pub t_0: TaiTime<0>,
    pub bitrate_manager: BitrateManager,
    pub video_app_sender: Option<StreamSender<VideoPacketHeader>>,
    pub audio_app_sender: Option<StreamSender<()>>,
    pub bw_probe_sender: Option<StreamSender<()>>,                  // Optional, only used by FovOptix to estimate current BW via active probing. 
    pub bw_probe_receiver: Option<StreamReceiver<()>>, 

    pub tracking_app_receiver: Option<StreamReceiver<Tracking>>,
    pub statistics_app_receiver: Option<StreamReceiver<ClientStatistics>>,

    pub control_socket_sender: Option<ControlSocketSender<ClientControlPacket>>,
    pub control_socket_receiver: Option<ControlSocketReceiver<ClientControlPacket>>,
    pub outport_videoapp_network: Output<MpduPacket>,
    pub is_streaming: bool,
    pub fps: f32,
    pub frames_sent_counter: usize,
    pub map_rtt: Arc<DashMap<u32, TaiTime<0>>>,
    pub STATISTICS_MANAGER: StatisticsManager,
    pub name_folder: String,
    pub network_effects: Vec<NetworkPattern>, 

    pub video_sample_filename: String, 
    pub gop_size: usize, 
    pub intra_refresh: bool, 
    pub abr_enabled: usize, 

    pub output_perfect_information_bitrate: Output<PerfectInfoBitrateMessage>, 
    pub last_tracking_rx_instant: TaiTime<0>, 

    pub csv_tracking: CsvTracking, 
    pub sim_unique_string: String, 

    pub nada_sender: Option<Arc<Mutex<NadaSender>>>, 

    pub fov_optix_manager : Option<Arc<Mutex<FovOptixStruct>>> , 

}
#[allow(unused)]
impl XRServer {
    pub fn new(
        ip_self: IpAddr,
        ip_client: IpAddr,
        t0_sim: TaiTime<0>,
        frame_rate: f32,
        initial_bitrate_mbps: f32,
        name_folder: &str,
        effects: &[NetworkPattern], 
        file_name_video: &str,
        gop_size: usize, 
        intra_refresh: bool, 
        abr_enabled: usize, 
        nest_vr_profile: &NestVrProfile, 
        t_end_simu: f64, 
        sim_unique_string: &str, 

    ) -> Self {
        let system_time = SystemTime::UNIX_EPOCH;


        let mut final_file; 

        if file_name_video.contains("randomVid"){

            let random_file_list = ["garp4k",  "snow", "assemble", "cut_video", "furbo"];
            let choice_random = random_file_list.iter().choose(&mut rand::thread_rng());
            final_file = match choice_random {
                Some(file) => file,
                None => {
                    "cut_video"
                    // println!("No files to choose from");
                }
            };

        }
        else{
            final_file = file_name_video; 
        }

        let num = crate::lib::get_4_octet(ip_self);
        let history_interval = BITRATE_UPDATE_INTERVAL;

        let nada_sender = if abr_enabled == 5{
            Some(Arc::new(Mutex::new(NadaSender::new(t0_sim))))
        }
        else{
            None
        }; 

        let fovoptix_struct: Option<Arc<Mutex<FovOptixStruct>>> = if abr_enabled == 6 {
            // let interarrival_man = FOInterArrival::new(60, 0.001);  // completely unused... 
            let trendline_man = FOTrendlineEstimator::new(); 
            let aimd_man =    FOAimdRateControl::new(true); 

            let bitrate_est_man = FOBitrateEstimator::new(); 
            let ratecontrol_man= FORateControlInput::new(FOBandwidthUsage::kBwNormal, Some((30. * 1024. * 1024.))); 


            Some(Arc::new(Mutex::new( FovOptixStruct::new(
                trendline_man, 
                aimd_man, 
                bitrate_est_man, 
                ratecontrol_man, 
                initial_bitrate_mbps as f64 )
            )))
        }
        else{
            None
        }; 


        Self {
            ip_self,
            ip_client,
            t_0: t0_sim,

            bitrate_manager: BitrateManager::new(
                MAX_HISTORY_SIZE,
                frame_rate,
                initial_bitrate_mbps,
                abr_enabled, 
                nest_vr_profile, 
                t_end_simu, 
                ip_self, 
                sim_unique_string, 
                frame_rate, 
            ),

            video_app_sender: None,
            audio_app_sender: None, 
            bw_probe_sender: None,  // Optional, only used by FovOptix to estimate current BW via active probing. 
            bw_probe_receiver: None, 

            tracking_app_receiver: None,
            statistics_app_receiver: None,

            control_socket_sender: None,
            control_socket_receiver: None,
            outport_videoapp_network: Output::default(),
            // output_haptics: Output::default(),
            is_streaming: false,
            fps: frame_rate,
            // sockets,
            frames_sent_counter: 0,
            name_folder: name_folder.to_string(),

            map_rtt: Arc::new(DashMap::new()),
            STATISTICS_MANAGER: StatisticsManager::new(
                MAX_HISTORY_SIZE,
                Duration::from_secs_f32(1.0 / frame_rate),
                0.0,
                name_folder,
                ip_self,
            ),

            network_effects: effects.to_vec() ,  
            video_sample_filename: final_file.to_string(), 
            gop_size, 
            intra_refresh, 
            abr_enabled, 
            output_perfect_information_bitrate: Output::default(),
            
            last_tracking_rx_instant: t0_sim, 
            csv_tracking: CsvTracking::new(name_folder, num).unwrap(), 
            sim_unique_string: sim_unique_string.to_string(), 
            nada_sender, 
            fov_optix_manager: fovoptix_struct, 
            
        }
    }

    pub fn session_end(&mut self, _delay: f64, context: &Context<Self>) {
        let now = context.scheduler.time();
        print_red!(
            "[XRServer {}] Ending session at {:.8}s",
            self.ip_self,
            format_elapsed!(now), 
        );

        // Stop streaming
        self.is_streaming = false;

        let obs = self.bitrate_manager.build_rl_observation(now); // only return something if RL mode activated

        match &self.bitrate_manager.bitrate_mode {
            BitrateMode::ReinforcementLearner { pending_obs, connector, last_action_idx, .. } => {
                println!("SESSION ENDDD!");
                let mut con = connector.lock().unwrap();  // lock ONCE

                let current_obs = obs.clone();

                // history
                let mut history_opt = pending_obs.lock().unwrap().take();
                let mut history = history_opt
                    .unwrap_or_else(|| RLObservationVector::new(RL_WINDOW_OBSERVATION_SIZE as u8));

                // prev window BEFORE pushing current_obs
                let prev_win_flat = history.as_flat_padded();

                let reward = self.bitrate_manager.rl_naive_reward_function(&current_obs);
                let prev_action = *last_action_idx.lock().unwrap();

                // push and build next window
                history.push(current_obs);
                let next_win_flat = history.as_flat_padded();
                // println!("SESSION ENDDD2!");

                // FINAL transition (windowed)
                let transition = RLTransition {
                    sim_id: self.sim_unique_string.clone(),
                    prev_obs: prev_win_flat,
                    action: prev_action,
                    reward,
                    next_obs: next_win_flat,
                    done: true,
                    cont_action_mbps: Some(self.bitrate_manager.last_target_bitrate_bps / 1e6)
                };
                con.post_transition(&transition);     // <--- use `con`, don't re-lock
                // print_red!("[RL] POSTING FINAL TRANSITION);

                con.reset_window();                   // <--- use `con`, don't re-lock

                // Reset history window for next episode
                *pending_obs.lock().unwrap() = Some(RLObservationVector::new(RL_WINDOW_OBSERVATION_SIZE as u8));
                // println!("SESSION ENDDD3!");
                drop(con); // releases the lock
            }
            _ => {}
        }


        // Drop or reset senders/receivers
        self.video_app_sender = None;
        self.audio_app_sender = None;
        self.tracking_app_receiver = None;
        self.statistics_app_receiver = None;
        self.control_socket_sender = None;
        self.control_socket_receiver = None;

        // Reset counters/trackers
        self.frames_sent_counter = 0;
        self.map_rtt.clear();

        // reset bitrate manager & statistics manager
        self.bitrate_manager.reset();
        self.STATISTICS_MANAGER.clear();
        
    }

    pub fn handle_control_packet(&mut self, packet: ClientControlPacket, now: TaiTime<0>) {
        if let Some(mut protorecv) = self.control_socket_receiver.clone() {
            // let packet = protorecv.recv(STREAMING_RECV_TIMEOUT).unwrap();
            let map_clone: Arc<DashMap<u32, TaiTime<0>>> = Arc::clone(&self.map_rtt);

            match packet {
                ClientControlPacket::NetworkStatistics(network_stats) => {
                    debug_debug!(DebugColor:: Teal, "{:.9}[DBG SERVER STATS]- Received stats for frame {:2.0}: \nNetwork stats:\n\t\t{:#?}",now.duration_since(self.t_0).as_secs_f64(), network_stats.frame_index,network_stats);

                    // let mut map_rtt_lock = map_clone.write().unwrap();
                    let frame_id = network_stats.frame_index as u32;
                    let rtt: Duration;
                    // if let send_instant = map_clone.get(&frame_id).unwrap()
                    if let Some((_, send_instant)) = map_clone.remove(&frame_id) {  
                        if let Some(foman) = self.fov_optix_manager.clone(){ // equivalent to matching for FovOptix

                            let mut guard = foman.lock().unwrap(); 
                            let val_rx = guard.last_reported_frame_rx_client_timestamp; 
                            
                            let send_instant_s = send_instant.duration_since(TaiTime::EPOCH).as_secs_f32(); 
                            guard.report_fovoptix_stats_server(network_stats.clone(), send_instant_s, val_rx, );
                        }
                        
                        rtt = now.duration_since(send_instant);
                        debug_bgprint!(DebugColor::Teal, "RTT = {:.9}", rtt.as_secs_f64());


                        let netstats= network_stats.clone(); 

                        let (peak_network_throughput_bps, frame_interarrival_s) =
                            self.STATISTICS_MANAGER.report_network_statistics(
                                network_stats.clone(),
                                rtt,
                                now,
                                self.bitrate_manager.last_target_bitrate_bps,
                            );

                        // BITRATE_MANAGER.lock().report_network_statistics
                        self.bitrate_manager.report_network_statistics_abr(
                            rtt,
                            peak_network_throughput_bps,
                            frame_interarrival_s,
                            network_stats,
                            now,  
                            send_instant, 
                        );

                        if netstats.nada_stats.nada_feedback{                                              // Should be equivalent to matching bitrate_mode to NADAPort
                            let client_stats = netstats.nada_stats.clone(); 
                            let feedback_report  =  NADAFeedbackReport::new(client_stats.nada_rmode, client_stats.nada_xcurr, client_stats.nada_recv, client_stats.d_queue, client_stats.d_tilde, client_stats.plr);
                            if let Some(nada_sender_arc) = self.nada_sender.as_mut(){
                                let mut guard = nada_sender_arc.lock().unwrap(); 
                                guard.update_on_receive_feedback(send_instant.duration_since(TaiTime::EPOCH).as_micros() as i64, feedback_report, self.fps as f64);
                                self.bitrate_manager.last_nada_target_bitrate_mbps = Some(guard.get_target_bitrate() as f64 / 1024.0 / 1024.0) ;
                            }
                        }
                 
                        // println!("SEND INSTANT: {}, now: {}, rtt: {}", format_elapsed!(send_instant), format_elapsed!(now), rtt.as_secs_f32());
                    } else {
                        println!("frame {} RTT ZEROO!!!!!", network_stats.frame_index);
                        rtt = Duration::ZERO;
                    }

                    
                }
                ClientControlPacket::DeadlineShardLossStat(inner) => {
                    let frames_lost = inner.frame_indexes;
                    let shards_lost = inner.shards_lost;

                    for (frame, shard) in frames_lost.iter().zip(shards_lost.iter()) {
                        print_pretty!(
                            DebugColor::Red,
                            "[DBG_DEAD_RX server {}] Frame {} lost {} shards",
                            self.ip_self,
                            frame,
                            shard
                        );
                        let time_elapsed = now.duration_since(TaiTime::EPOCH).as_secs_f32(); 

                        self.bitrate_manager.report_shard_and_frame_loss(1 as usize, *shard, time_elapsed); // report one frame lost, and nº of video shards lost
                    }
                }

                _ => {
                    println!("UNEXPECTED CONTROL PACKET RECEIVED!!");
                }
            }
        }
    }
    fn decode_tracking_from_bytes(&self, buf: &[u8]) -> Result<Tracking> {
        anyhow::ensure!(buf.len() >= SHARD_PREFIX_SIZE, "buffer too small");

        let serialized = &buf[SHARD_PREFIX_SIZE..];
        let track: Tracking = bincode::deserialize::<Tracking>(serialized)?;
            // .unwrap()?; 
        Ok(track)
    }
    pub async fn in_from_network(&mut self, frame: TimedFrame) {
        let packet_vec = frame.vec;
        let now = frame.timestamp;

        for packet in packet_vec {
            let header = packet.header_alvr.clone();
            let buffer = packet.data_inner.clone();
            // println!("XRServer IN NETWORK. Header: {:?}, buffer_len = {}", header, buffer.len());

            match header.stream_id {

                FOVOPTIX_BW_PROBE => {
                    // The raw bytes as received from network
                    let buf = &packet.data_inner;

                    // --- Step 1: skip SHARD_PREFIX_SIZE ---
                    // (must match the same constant used in sender)
                    let start_hdr = SHARD_PREFIX_SIZE;

                    // --- Step 2: figure out where the header ends ---
                    // The header size can be deduced from the type
                    // let hdr_size = std::mem::size_of::<BwProbeHeader>(); // or use serialized_size if you prefer
                    let hdr_size = bincode::serialized_size(&BwProbeHeader {
                        seq: 0,
                        tx_instant_us: 0,
                        rx_instant_s_video_frame: 0., 
                        payload_len: 0,
                    }).unwrap() as usize;

                    // --- Step 3: slice and deserialize ---
                    let hdr_slice = &buf[start_hdr..start_hdr + hdr_size];
                    let recv_hdr: BwProbeHeader = bincode::deserialize(hdr_slice)
                        .expect("Failed to deserialize BwProbeHeader");
                    // --- Step 4: (optional) extract payload ---
                    let payload_start: usize = start_hdr + hdr_size;
                    let payload_end = payload_start + recv_hdr.payload_len as usize;
                    let payload_slice = &buf[payload_start..payload_end];

                    let now_us = now.duration_since(TaiTime::EPOCH).as_micros() as i128;
                    let time_elapsed_probing: i128 = now_us - recv_hdr.tx_instant_us;
                    
                    let payload_size = buf.len(); 

                    let bandwidth_sample_bps = (payload_size as f32 * 8.0 * 1_000_000.0) / time_elapsed_probing as f32;
                    // println!("Available bandwidth: {} bps", bandwidth_sample_bps);

                    if let Some(foman) = self.fov_optix_manager.clone().as_mut(){
                        let mut guard = foman.lock().unwrap(); 

                        guard.last_bw_bps = bandwidth_sample_bps as f64 ; //store last sample, as FovOptix does in global in connection loop                         
                        guard.last_reported_frame_rx_client_timestamp = recv_hdr.rx_instant_s_video_frame; // probe from client contains absolute timestamp of last RX frame as f32 
                        // guard.last_frame_tx_timestamp_micros =; 
                        drop(guard); 
                    }
                    else{
                        panic!("UNDEFINED?"); 
                    }
                    // calculate the inter-arrival time     

                    // print_red!(
                    //     "[BW_PROBE] seq={} RTT={:.3}ms payload={}B",
                    //     recv_hdr.seq,
                    //     (rtt_ns as f64) / 1e6,
                    //     recv_hdr.payload_len
                    // );
                }
                TRACKING => {
                    if let Some(sock) = self.tracking_app_receiver.clone() {
                        let sender = sock.network_app_interface.lock().unwrap().send(&buffer);
                        let mut new_buffer: Vec<u8> = vec![0; MAX_PACKET_SIZE_RECV];
                        let receiver = sock.inner.lock().unwrap().recv(&mut new_buffer);

                         match self.decode_tracking_from_bytes(&new_buffer) {
                            Ok(track) => {
                                for (device_id, motion) in track.device_motions {
                                    let log_entry = TrackingLog {
                                        device_id,
                                        position: motion.pose.position,
                                        orientation: motion.pose.orientation,
                                        linear_velocity: motion.linear_velocity,
                                    };

                                    // Just print for now
                                    // println!(
                                    //     "[TRACKING] device={} pos={:?} vel={:?}",
                                    //     log_entry.device_id,
                                    //     log_entry.position,
                                    //     // log_entry.orientation,
                                    //     log_entry.linear_velocity
                                    // );
                                    let interarrival_tracking_ms = now.duration_since(self.last_tracking_rx_instant).as_secs_f32() * 1000.0; 
                                    self.csv_tracking.update_stats(
                                        now,
                                        log_entry.device_id,
                                        [
                                            log_entry.position.x,
                                            log_entry.position.y,
                                            log_entry.position.z,
                                        ],
                                        interarrival_tracking_ms,
                                    );
                                    self.last_tracking_rx_instant = now; 



                                    // later: push to CSV logger
                                    // self.csv_logger.write_tracking(&log_entry);
                                }
                            }
                            Err(e) => {
                                eprintln!("[TRACKING] decode error: {e:?}");
                            }
                         }
                        // println!("TODO: THE REST of tracking server!!");
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
                        let stats: ClientControlPacket =
                            framed_recv_vec(&packet.data_inner).unwrap();
                        // println!("STATS IS {:?}", stats);

                        if let Ok(()) = sock.send(&stats){
                            
                        }
                        else{
                            print_red!("[ERROR] Socket error control stream!", ); 
                        }

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

            // debug_print!(
            //     DebugColor::DarkGreen,
            //     "{}[DBG XR_SERVER {}] Sending to network the following packets:",
            //     self.ip_self,
            //     elapsed.as_secs_f64(),
            // );
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
                                    9 => "BW probe", 
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

                                if packet.header_alvr.shard_index == 0 {
                                    // println!(
                                    //     "{:.9}-Server {} sending {:#?}",
                                    //     now.duration_since(self.t_0).as_secs_f64(),
                                    //     self.ip_self,
                                    //     packet.header_alvr
                                    // );
                                }
                                // if stream_id == VIDEO || stream_id == AUDIO {
                                    
                                    if stream_id == VIDEO{
                                        packet.edca_ac = EdcaAc::Video; 
                                    }
                                    else if stream_id == AUDIO {
                                        packet.edca_ac = EdcaAc::Voice; 
                                    }
                                    else if stream_id == FOVOPTIX_BW_PROBE{
                                        packet.edca_ac = EdcaAc::BestEffort; 
                                    }
                                    self.outport_videoapp_network.send(packet).await;
                                // }

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


    pub fn generate_audio_frame<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let now = context.scheduler.time();

            if let Some(mut sender) = self.audio_app_sender.clone() {
                // 1) how big is our "two empties" payload?
                let payload_len = 1400 + 600; 

                // 2) compute the hidden prefix so fragmentation/sharding still lines up
                let header = VideoPacketHeader::new(Duration::from_secs(1), false);
                let hsize = bincode::serialized_size(&header).unwrap() as usize;
                let hidden_offset = SHARD_PREFIX_SIZE + hsize;

                // 3) allocate one big vec = prefix + payload
                let mut raw = vec![0u8; hidden_offset + payload_len];

                // 4) (optional) encode your header into the reserved space
                let header_bytes = bincode::serialize(&header).unwrap();
                raw[SHARD_PREFIX_SIZE .. SHARD_PREFIX_SIZE + hsize]
                    .copy_from_slice(&header_bytes);

                // 5) wrap it—length is _only_ the payload
                let buf = crate::lib::alvr_stream_socket::Buffer {
                    inner: raw,
                    hidden_offset,
                    length: payload_len,
                    _phantom: std::marker::PhantomData::<()>,
                };

                // 6) send + handle the app‐recv path
                let _ = sender.send(buf, now); 
                let arc_receiver = sender.app_network_interface.clone();
                let buffer: Vec<u8> = vec![0; CAPACITY_RX_BUFFER];
                XRServer::read_app_send_network_interface(
                    self, (), now, buffer, arc_receiver
                ).await;
            }

            // 7) schedule next in 10 ms
            context
                .scheduler
                .schedule_event(Duration::from_millis(10),
                                Self::generate_audio_frame, ())
                .unwrap();
        }
    }

    pub fn generate_FO_bandwidth_probe<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {

            let now = context.scheduler.time();
            // print_dblue!("{} GENERATING PROBE", format_elapsed!(now));

            // Run only if we have the FovOptix managers AND a probe sender
            if let (Some(fov_man), Some(mut sender)) = (self.fov_optix_manager.as_ref(), self.bw_probe_sender.clone()) {
                // ---- take what we need without holding the lock across await ----
                let (seq, bw_sent_map_arc) = {
                    let mut guard = fov_man.lock().unwrap();
                    let seq = guard.bw_seq;
                    let map_arc = guard.bw_sent_map.clone(); // Arc<DashMap<...>>
                    guard.bw_seq = guard.bw_seq.wrapping_add(1);
                    (seq, map_arc)
                }; // drop(guard) here before any await

                // 1) tiny payload (~100 bytes)
                let payload_len: usize = 100;

                // 2) build the probe header
                let hdr = BwProbeHeader {
                    seq,
                    tx_instant_us: now.duration_since(TaiTime::EPOCH).as_micros() as i128,
                    rx_instant_s_video_frame: 0., 
                    payload_len: payload_len as u32,
                };

                // 3) compute hidden prefix size (SHARD_PREFIX + serialized header)
                let hsize = bincode::serialized_size(&hdr).unwrap() as usize;
                let hidden_offset = SHARD_PREFIX_SIZE + hsize;

                // 4) allocate buffer = hidden area + payload
                let mut raw = vec![0u8; hidden_offset + payload_len];

                // 5) encode BwProbeHeader into hidden area
                let hdr_bytes = bincode::serialize(&hdr).unwrap();
                raw[SHARD_PREFIX_SIZE .. SHARD_PREFIX_SIZE + hsize].copy_from_slice(&hdr_bytes);

                // 6) wrap it as Buffer<()> to match StreamSender<()> 
                let buf = crate::lib::alvr_stream_socket::Buffer {
                    inner: raw,
                    hidden_offset,
                    length: payload_len,
                    _phantom: std::marker::PhantomData::<()> // IMPORTANT
                };

                // 7) remember send time for RTT/goodput on echo
                bw_sent_map_arc.insert(seq, now);

                // 8) send + forward the app→network side
                let _ = sender.send(buf, now);
                let arc_reader = sender.app_network_interface.clone();
                let rx_buf: Vec<u8> = vec![0; CAPACITY_RX_BUFFER];
                XRServer::read_app_send_network_interface(self, (), now, rx_buf, arc_reader).await;
            }

            // 9) schedule next probe (tune period as needed)
            context.scheduler
                .schedule_event(std::time::Duration::from_millis(20), Self::generate_FO_bandwidth_probe, ())
                .unwrap();
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

            let map_clone: Arc<DashMap<u32, TaiTime<0>>> = Arc::clone(&self.map_rtt);
            
            if self.video_app_sender.is_none(){ // This happens when we end a session but the next frame was already scheduled. 
                        // println!("CATCH NO VIDEO APP SENDER");  
                        return;
                    }
            
            self.video_app_sender.as_mut().unwrap().next_packet_index =
                self.frames_sent_counter as u32;

            // STEP 1: DEBUG VIDEO
            if let Some(mut send_socket) = self.video_app_sender.clone() {
                let is_idr = false;
                let header = VideoPacketHeader::new(Duration::from_secs(1), is_idr);

                // self.bitrate_manager.report_timestamp_change_bitrate(now);   // for programatically changing CBR bitrate
                
                let duration_abr = Duration::from_secs_f64(BITRATE_UPDATE_INTERVAL); 

                // print_red!("Duration of ABR {:.4}", duration_abr.as_secs_f32()); 

                let count = get_counter().fetch_add(1, Ordering::Relaxed);


                
                if !matches!(self.bitrate_manager.bitrate_mode , BitrateMode::EVeREst{ .. }) || // One pass every BITRATE_UPDATE_INTERVAL
                    !matches!(self.bitrate_manager.bitrate_mode , BitrateMode::GCCPort{ .. }) || !matches!(self.bitrate_manager.bitrate_mode, BitrateMode::FovOptixPort{..}) 
                {  
                    if (now.duration_since(self.bitrate_manager.last_update_instant) >= duration_abr){
                       
                        let last_bitrate_mbps = self.bitrate_manager.one_pass_abr(now, self.ip_self) / 1e6;
                        self.bitrate_manager.last_update_instant = now;
                        

                        let perfect_info_message = PerfectInfoBitrateMessage{bitrate_ladder_bps: self.bitrate_manager.bitrate_ladder_bps.clone(),  bitrate_mbps: last_bitrate_mbps }; 

                        self.output_perfect_information_bitrate.send(perfect_info_message).await; // Client knows the bitrate ladder, needed for thresholds computing in HMD. 
                        
                        // self.bitrate_manager.last_target_bitrate_mbps = last_bitrate_mbps; 

                        // print_green!("[{}]  Current bitrate: {} Mbps", self.ip_self, self.bitrate_manager.last_target_bitrate_mbps); 
                    }
                }                
                else{ // EveRest classic, GCC, FovOptix are applied per-frame. 

                    // println!()
                    if matches!(self.bitrate_manager.bitrate_mode , BitrateMode::FovOptixPort { ..}){
                        println!("INSIDE FOVOPTIXIXXX");
                        if let Some(man) = self.fov_optix_manager.as_mut(){

                            let mut guard = man.lock().unwrap(); 

                            let bitrate_bps = guard.aimd.flag_for_qp; 
                            let nol = guard.aimd.normalize_delta; 
                            let tps =      guard.aimd.current_bitrate_ / 72.0 / 8.0 ; // Is this only valid for 72 FPS? :/      

                            println!("[FOVOPTIX] Bitrate Mbps : {:.2} Mbps", bitrate_bps as f32 / 1e6 ); 


                            panic!("CHECK FovOptix paper and VideoEncoderNVENC: Actual usage of nol and tps"); 
                            self.bitrate_manager.last_target_bitrate_bps = bitrate_bps as f32; 
                        }
                    }

                    let last_bitrate_mbps = self.bitrate_manager.one_pass_abr(now, self.ip_self) / 1e6;
                    self.bitrate_manager.last_update_instant = now;
                    
                    let perfect_info_message = PerfectInfoBitrateMessage{bitrate_ladder_bps: self.bitrate_manager.bitrate_ladder_bps.clone(),  bitrate_mbps: last_bitrate_mbps }; 
                    self.output_perfect_information_bitrate.send(perfect_info_message).await;  // Client knows the bitrate ladder, needed for thresholds computing in HMD. 

                    // self.bitrate_manager.last_target_bitrate_mbps = last_bitrate_mbps;   
                }




                if count % 80 == 0 {
                    print_green!("{} [{}]  Current bitrate: {} Mbps", format_elapsed!(now), self.ip_self, self.bitrate_manager.last_target_bitrate_bps / 1e6); 
                }

               
                let current_bitrate_mbps: f32 = self.bitrate_manager.last_target_bitrate_bps / 1e6;

                // let max_bitrate_ladder_mbps: f32 = match self.bitrate_manager.bitrate_mode { // only useful for online VQ analysis 
                //     BitrateMode::NestVr {
                //         max_bitrate_mbps, ..
                //     } => max_bitrate_mbps, // Extract max_bitrate_mbps
                //     _ => 100.0,
                // };


                let mut buffer_emu = send_socket // generate the actual video frame data
                    .get_buffer_emu(
                        &header,
                        current_bitrate_mbps,
                        now,
                        self.ip_self,
                        self.frames_sent_counter,
                        &self.name_folder,
                        // max_bitrate_ladder_mbps,
                        &self.network_effects, 
                        &self.video_sample_filename, 
                        self.fps, 
                        self.gop_size, 
                        self.intra_refresh, 
                    )
                    .await
                    .unwrap();

                if let Some(encoder_init) = send_socket.clone().ffmpeg_encoder {
                    self.video_app_sender.as_mut().unwrap().ffmpeg_encoder = Some(encoder_init);
                    // println!("ENCODER INITIALIZED");
                }
               
                // Use DashMap's thread-safe `insert` API instead of write locks   
                let frame_tracker_map = send_socket.get_frame_tracker_map(); 
                    frame_tracker_map.into_iter().for_each(|(key, value)| {
                    map_clone.insert(key, value);
                });

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

                let normal: Normal<f32> = Normal::new(0.0, 0.001).unwrap(); // σ = 0.001s
                let epsilon = normal.sample(&mut rand::thread_rng());

                let ideal = 1.0 / (self.fps as f32);
                let floor = 0.5 * ideal; 

                let dt = (ideal + epsilon).max(floor);
                let time_until_next_frame = Duration::from_secs_f32(dt);

                self.bitrate_manager.report_encoded_frame_server(now);

                context
                    .scheduler
                    .schedule_event(time_until_next_frame, Self::generate_video_frame, ())
                    .unwrap();
            }
            self.frames_sent_counter += 1;
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
            
            self.audio_app_sender = Some(stream_socket.request_stream(AUDIO, self.t_0)); 


            if matches!(self.bitrate_manager.bitrate_mode, BitrateMode::FovOptixPort { .. }) {
                self.bw_probe_sender = Some(stream_socket.request_stream(FOVOPTIX_BW_PROBE, self.t_0)); 
                self.bw_probe_receiver = Some(stream_socket.subscribe_to_stream(FOVOPTIX_BW_PROBE, MAX_UNREAD_PACKETS)); 

                XRServer::generate_FO_bandwidth_probe(self, (), context).await; 
            }
            self.tracking_app_receiver =
                Some(stream_socket.subscribe_to_stream::<Tracking>(TRACKING, MAX_UNREAD_PACKETS));
            self.statistics_app_receiver = Some(
                stream_socket
                    .subscribe_to_stream::<ClientStatistics>(STATISTICS, MAX_UNREAD_PACKETS),
            );

            // self.control_receiver = Some(
            //     stream_socket.subscribe_to_stream::<()>(CONTROL_STREAM,  MAX_UNREAD_PACKETS),
            // );

            let (proto_socket, _) = ProtoControlSocket::connect(STREAMING_RECV_TIMEOUT).unwrap();

            let (mut control_sender, mut control_receiver): (
                ControlSocketSender<ClientControlPacket>,
                ControlSocketReceiver<ClientControlPacket>,
            ) = proto_socket.split().unwrap();

            self.control_socket_sender = Some(control_sender);
            self.control_socket_receiver = Some(control_receiver);

            XRServer::generate_video_frame(self, (), context).await;
            XRServer::generate_audio_frame(self, (), context).await; 

       
            // STEP 2: DO SAME FOR HAPTICS using context.scheduler!
            // TODO!
        }
    }
}

impl Model for XRServer {}

pub struct DroppingVecDeque<T> {
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
    pub fn clear(&mut self){
        self.deque.clear(); 
        self.dropped_frame_counter  = 0; 
        self.ok_dequed_frame_counter = 0; 
        self.enqued_frame_counter   = 0; 

    }
    fn push(&mut self, item: T) {
        // If we are at capacity, pop the oldest frame from the front
        self.enqued_frame_counter += 1;
        debug_print!(
            DebugColor::Gold,
            "[VecDecoder] Pushing frame {}, decoder_length: {}, max: {},",
            self.enqued_frame_counter,
            self.deque.len(),
            self.capacity,
        );

        if self.deque.len() == self.capacity {
            self.deque.pop_front();
            self.dropped_frame_counter += 1;
            // println!("DROPPED A FRAME IN DECODER!!");
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

// #[derive(Debug, Clone)]
// struct FrameData {
//     _ref_rgb: Vec<u8>,
//     _lossy_rgb: Vec<u8>,
//     _timestamp_ms: f64,
//     _frame_number: u64,
// }
// struct FrameGroup {
//     _frames: Vec<FrameData>,
// }
// #[derive(serde::Serialize)]
// struct FrameMetrics {
//     frame_number: u64,
//     timestamp_ms: f64,
//     vmaf: f64,
//     psnr: f64,
//     ssim: f64,
// }
#[derive(Debug, Clone)]
#[allow(unused)]
struct FrameData {
    path: String,
    timestamp_ms: f64,
    frame_number: u64,
}

struct FrameGroup {
    _frames: Vec<FrameData>,
}

#[derive(serde::Serialize)]
struct FrameMetrics {
    frame_number: u64,
    timestamp_ms: f64,
    vmaf: f64,
    psnr: f64,
    ssim: f64,
}



#[derive(Clone)]
struct MetricsLogger {
    writer: Arc<Mutex<csv::Writer<File>>>,
    name_folder: String,
}

impl MetricsLogger {
    fn new(ip: IpAddr, name_folder: &str) -> Result<Self> {
        let mut value = 99;
        if let IpAddr::V4(ip4) = ip {
            let octets = ip4.octets();
            value = octets[2]
        }

        let file = File::create(format!(
            "Results/{}/VMAF_metrics_{}.csv",
            name_folder, value
        ))?;
        let writer = csv::Writer::from_writer(file);
        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            name_folder: name_folder.to_string(),
        })
    }

    pub fn process_frame_metrics(
        &self,
        frame_number: u64,
        timestamp_ms: f64,
        ref_path: &str,
        lossy_path: &str,
        ip_client: IpAddr, 
    )  {
        // Create a temporary directory for processing
        let temp_dir = TempDir::new().unwrap();

        // Convert RGB frames to Y4M format (better for VMAF processing)
        let ref_y4m = temp_dir
            .path()
            .join("reference.y4m")
            .to_string_lossy()
            .to_string();
        let lossy_y4m = temp_dir
            .path()
            .join("lossy.y4m")
            .to_string_lossy()
            .to_string();

        // Convert reference frame to Y4M
        let _ref_status = Command::new("ffmpeg")
            .args(&[
                "-hwaccel",
                "cuda",
                "-loglevel",
                "error", // Add this line to reduce verbosity
                "-y",
                "-f",
                "rawvideo",
                "-pixel_format",
                "rgb24",
                "-video_size",
                &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
                "-i",
                ref_path,
                "-pix_fmt",
                "yuv420p",
                &ref_y4m,
            ])
            .status().unwrap();

        // if !ref_status.success() {
        //     return Err(anyhow::anyhow!("Failed to convert reference frame to Y4M"));
        // }

        // Convert lossy frame to Y4M
        let _lossy_status = Command::new("ffmpeg")
            .args(&[
                "-hwaccel",
                "cuda",
                "-loglevel",
                "error", // Add this line to reduce verbosity
                "-y",
                "-f",
                "rawvideo",
                "-pixel_format",
                "rgb24",
                "-video_size",
                &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
                "-i",
                lossy_path,
                "-pix_fmt",
                "yuv420p",
                &lossy_y4m,
            ])
            .status().unwrap();

        // if !lossy_status.success() {
        //     return Err(anyhow::anyhow!("Failed to convert lossy frame to Y4M"));
        // }

        // Create the Sink_for_video directory within the temp directory
        let video_sink_dir = temp_dir.path().join(&self.name_folder).join("Sink_for_video");
        std::fs::create_dir_all(&video_sink_dir).unwrap();

        // Set up paths correctly
        let vmaf_json = video_sink_dir
            .join("vmaf.json")
            .to_string_lossy()
            .to_string();
        let psnr_log = video_sink_dir
            .join("psnr.log")
            .to_string_lossy()
            .to_string();
        let ssim_log = video_sink_dir
            .join("ssim.log")
            .to_string_lossy()
            .to_string();

        // Calculate all metrics in a single ffmpeg call
        let _metrics_status = Command::new("ffmpeg")
            .args(&[
                "-hwaccel",
                "cuda",
                "-loglevel",
                "error", // Add this line to reduce verbosity
                "-i",
                &ref_y4m,
                "-i",
                &lossy_y4m,
                "-filter_complex",
                &format!("[0:v][1:v]libvmaf=log_fmt=json:log_path={}", vmaf_json),
                "-filter_complex",
                &format!("[0:v][1:v]psnr=stats_file={}", psnr_log),
                "-filter_complex",
                &format!("[0:v][1:v]ssim=stats_file={}", ssim_log),
                "-f",
                "null",
                "-",
            ])
            .status().unwrap();

        // if !metrics_status.success() {
        //     return Err(anyhow::anyhow!("Failed to calculate video metrics"));
        // }

        // Parse VMAF score
        let mut vmaf_score = 0.0;
        if let Ok(vmaf_content) = std::fs::read_to_string(&vmaf_json) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&vmaf_content) {
                if let Some(score) = json_value["pooled_metrics"]["vmaf"]["mean"].as_f64() {
                    vmaf_score = score;
                } else if let Some(frames) = json_value["frames"].as_array() {
                    if let Some(first_frame) = frames.first() {
                        if let Some(score) = first_frame["metrics"]["vmaf"].as_f64() {
                            vmaf_score = score;
                        }
                    }
                }
            }
        }

        // Parse PSNR score
        let mut psnr_avg = 0.0;
        if let Ok(psnr_content) = std::fs::read_to_string(&psnr_log) {
            if let Some(avg_idx) = psnr_content.find("psnr_avg:") {
                let remaining = &psnr_content[avg_idx + 9..];
                let end_idx = remaining.find(" ").unwrap_or(10);
                let avg_str = &remaining[..end_idx];
                if let Ok(value) = avg_str.trim().parse::<f64>() {
                    psnr_avg = value;
                }
            }
        }

        // Parse SSIM score
        let mut ssim_score = 0.0;
        if let Ok(ssim_content) = std::fs::read_to_string(&ssim_log) {
            if let Some(all_idx) = ssim_content.find("All:") {
                let remaining = &ssim_content[all_idx + 4..];
                let end_idx = remaining.find(" ").unwrap_or(10);
                let all_str = &remaining[..end_idx];
                if let Ok(value) = all_str.trim().parse::<f64>() {
                    ssim_score = value;
                }
            }
        }

        // Print debug info
        print_green!(
            "T: {:.3} [{}]| Frame {}: VMAF = {:.2}, PSNR = {:.2}, SSIM = {:.4}",
            timestamp_ms,
            ip_client,
            frame_number,
            vmaf_score,
            psnr_avg,
            ssim_score
        );

        let metrics = FrameMetrics {
            frame_number: frame_number,
            timestamp_ms: timestamp_ms,
            vmaf: vmaf_score,
            psnr: psnr_avg,
            ssim: ssim_score,
        };

        // Log the metrics
        self.log_metrics(&metrics).unwrap();

        // Ok(())
    }

    

    fn log_metrics(&self, metrics: &FrameMetrics) -> Result<()> {
        // Get a single mutex guard and use it for both operations
        let mut guard = self.writer.lock().unwrap();

        // Now use the guard directly for both operations
        guard.serialize(metrics)?;
        guard.flush()?;

        Ok(())
    }
}
// Add to your struct




#[derive(Clone)]
pub struct TimedRebufferCounter{
    vec_buflevel: VecDeque<(f32,usize)>,
    period: f32, // how long to keep values 
}

impl TimedRebufferCounter{
    pub fn new(period: f32) -> Self {
        Self {
            vec_buflevel: VecDeque::new(),
            period,
        }
    }
    pub fn add_one(&mut self, now: TaiTime<0>) {
        // Insert new values
        let time_f32 = now.duration_since(TaiTime::EPOCH).as_secs_f32(); 
        self.vec_buflevel.push_back((time_f32, 1)); // one rebuffer event per call

        // Prune old values outside the time window
        let cutoff = time_f32 - self.period;

        while let Some(&(t, _)) = self.vec_buflevel.front() {
            if t < cutoff {
                self.vec_buflevel.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn sum_in_period(&self) -> usize {
        self.vec_buflevel.iter().map(|&(_, v)| v).sum()
    }

}

#[allow(unused)]
pub struct XRClient {
    pub decoder_queue: DroppingVecDeque<(usize, Vec<u8>)>,

    pub outport_tracking_network: Output<MpduPacket>,

    pub input_app_video: Option<StreamReceiver<VideoPacketHeader>>,
    pub input_app_audio: Option<StreamReceiver<()>>,
    pub input_app_haptics: Option<StreamReceiver<Haptics>>,

    pub input_app_bw_probe: Option<StreamReceiver<()>>, 

    pub output_app_tracking_sender: Option<StreamSender<Tracking>>,

    pub output_app_bw_probe_back: Option<StreamSender<()>>, 

    pub out_video_decoded: Output<Vec<u8>>,

    pub framerate: f32,


    pub output_app_network: Output<MpduPacket>,

    pub current_coordinates_tracking: Vec3,
    pub last_coordinates_tracking: Vec3, 
    pub is_streaming: bool,

    pub frames_dropped_counter: usize,
    pub server_ip: IpAddr,

    pub streamsocket_clone: Option<StreamSocket>,

    pub decoded_frame_index: usize,
    pub t_0: TaiTime<0>,

    pub last_tracking_time: TaiTime<0>,
    pub last_decoded_frame_instant: TaiTime<0>,
    pub last_rx_frame_instant: TaiTime<0>, 

    // pub has_decoder: Option<bool>,
    // pub decoder_arc: Option<Arc<tokMutex<HevcDecoder>>>,

    // pub ref_decoder_arc: Option<Arc<tokMutex<HevcDecoder>>>,
    original_decoder: Option<Arc<Mutex<HevcDecoder>>>,

    pub is_decoder_ready: bool, // internal of FFMPEG
    pub jitter_buffer_warmup_ready: bool, // of actual VR Client application
    // pub is_ref_decoder_ready: bool,
    // Add these new fields:
    initialization_buffer: Vec<Vec<u8>>, // Buffer to hold initial frames

    min_buffered_frames: usize, // Minimum frames to buffer before decoding
    dec_saw_keyframe: bool,
    ref_saw_keyframe: bool,

    dec_saw_keyframe_last_t: TaiTime<0>,
    ref_saw_keyframe_last_t: TaiTime<0>,

    stream_offset: f64,

    metrics_logger: Option<MetricsLogger>,
    current_frame_group: Option<FrameGroup>,
    group_tx: Option<Sender<FrameGroup>>,
    enable_batch_processing: bool,
    cleanup_interval: std::time::Duration,
    group_rx: Option<Receiver<FrameGroup>>,

    last_processed_frame_id: usize, // Keep track of the last processed frame ID
    missing_frames_buffer: HashMap<usize, bool>, // Track missing frames

    last_displayed_frame_id: usize,

    // Add this to your struct
    frame_pairs: HashMap<usize, FramePair>,
    ref_max_pairs: HashMap<usize, FramePair>,
    last_displayed_pair_id: usize,

    last_keyframe_id: usize,

    name_folder: String,
    frame_batch: Vec<(usize, Vec<u8>, Vec<u8>, f64)>, // (frame_id, sample, ref_sample, timestamp)
    last_batch_process_time: TaiTime<0>,

    shared_params: Option<Arc<SharedParameterSetManager>>,
    channel_tx_vmaf: Sender<(Vec<u8>, Vec<u8>, usize)>,
    channel_rx_vmaf: Receiver<(Vec<u8>, Vec<u8>, usize)>,
    test: String,


    lost_ids_reference_buffer: VecDeque<u32>, 
    lost_frames_buffer : LostFramesBuffer,
    // pub visualize_decoder_window: Option<Window>,


    vmaf_frame_buffer: VecDeque<(Vec<u8>, Vec<u8>, TaiTime<0>, usize, IpAddr)>, // (sample, ref_sample, timestamp, frame_id, ip)
    vmaf_batch_size: usize,      



    file_id_offset: i64,
    /// During init we collect a few (id, frame_data) pairs for calibration
    init_buffer_ids:       Vec<usize>,
    init_buffer_frames:    Vec<Vec<u8>>,


    offline_csv_trace: CsvTrace, 
    last_seen_id: usize, 

    last_throughput_avg: f32, 
    last_bitrate_perfect_info_update_mbps: f32,
    bitrate_ladder_perfect_info_update: Vec<f32>, 

    frame_size_exp_avg: f32, 
    d_short_exp_avg: f32, 
    d_long_exp_avg: f32, 

    everest_enabled: bool, 

    rebuffer_event_counter: TimedRebufferCounter, 
    sim_unique_string: String, // for logging
    bm_string: String, 


    nada_receiver: Option<Arc<Mutex<NadaReceiver>>>,

    abr_mode: usize, 
    // everest_capacity_vec: Vec<f32>, 
    // everest_throughput_vec: Vec<f32>, 
}

#[allow(unused)]
impl XRClient {
    pub fn new(
        server_ip: IpAddr,
        fps: f32,
        now: TaiTime<0>,
        name_folder: &str,
        test: &str,
        // everest_enabled: bool,  // todo, match on abr_mode == 2 instead
        abr_mode: usize, 
        simu_id: &str, // for logging
        bm_str: &str,  // for logging 
    ) -> Self {
        let (vmaf_tx, vmaf_rx) = bounded(10);
        let (group_tx, group_rx) = bounded(10); // Buffer up to 5 groups
        let synchronized_throttle = Arc::new(Semaphore::new(0));
        
        let nada_receiver = if abr_mode == 5 { // ONLY for NADA (nest>1, everest>2, RL>3, Gcc>4,NADA>5 )
            Some(Arc::new(Mutex::new(NadaReceiver::new())) )
        }
        else{
            None
        }; 

        let everest_enabled = abr_mode == 2; 
        
        Self {
            decoder_queue: DroppingVecDeque::new(DECODER_BUFFERING_FRAMES),
            outport_tracking_network: Output::default(),
            input_app_video: None,
            input_app_audio: None,
            input_app_haptics: None,
            input_app_bw_probe: None, 
            output_app_bw_probe_back: None, 

            output_app_tracking_sender: None,
            out_video_decoded: Output::default(),
            framerate: fps,
            output_app_network: Output::default(),
            // output_tracking: Output::default(),
            current_coordinates_tracking: Vec3::ZERO,
            last_coordinates_tracking: Vec3::ZERO,

            is_streaming: false,
            frames_dropped_counter: 0,
            server_ip,
            streamsocket_clone: None,
            decoded_frame_index: 0,
            t_0: now,
            last_tracking_time: now,
            last_decoded_frame_instant: TaiTime::EPOCH,
            last_rx_frame_instant: TaiTime::EPOCH,

            // Add these new fields:
            initialization_buffer: Vec::new(), // Buffer to hold initial frames
            is_decoder_ready: false, // Flag to track if FFMPEG decoder is ready
            jitter_buffer_warmup_ready: false, // to buffer at least N frames before starting to show at first. 
            min_buffered_frames: 20, // Minimum frames to buffer before decoding
            dec_saw_keyframe: false,
            ref_saw_keyframe: false,

            dec_saw_keyframe_last_t: TaiTime::EPOCH,
            ref_saw_keyframe_last_t: TaiTime::EPOCH,
            stream_offset: 0.0,

            metrics_logger: None,
            current_frame_group: None,
            group_tx: Some(group_tx),
            group_rx: Some(group_rx),
            enable_batch_processing: false,
            cleanup_interval: std::time::Duration::from_secs(8), // Clea

            last_processed_frame_id: 0,
            missing_frames_buffer: HashMap::new(),
            last_displayed_frame_id: 0,

            frame_pairs: HashMap::new(),
            ref_max_pairs: HashMap::new(),
            last_displayed_pair_id: 0,
            last_keyframe_id: 0,
            name_folder: name_folder.to_string(),

            frame_batch: Vec::new(),
            last_batch_process_time: TaiTime::EPOCH,
            shared_params: None,

            channel_tx_vmaf: vmaf_tx,
            channel_rx_vmaf: vmaf_rx,
            test: test.to_string(),

            lost_ids_reference_buffer: VecDeque::new(), 
            lost_frames_buffer : LostFramesBuffer::new(4),
            vmaf_frame_buffer: VecDeque::new(),
            vmaf_batch_size: 2, // Default batch size

            file_id_offset:      0,
            init_buffer_ids:       Vec::new(),
            init_buffer_frames:    Vec::new(),

            offline_csv_trace: CsvTrace::default(), 
            last_seen_id: 0, 
            last_throughput_avg: 0.0, 

            original_decoder: None, 
            last_bitrate_perfect_info_update_mbps: 0.0, 

            frame_size_exp_avg: 0.0, 
            d_short_exp_avg: 0.0,
            d_long_exp_avg: 0.0, 
            bitrate_ladder_perfect_info_update: Vec::new(), 
            everest_enabled, 

            rebuffer_event_counter: TimedRebufferCounter::new( BITRATE_UPDATE_INTERVAL as f32), 
            sim_unique_string: simu_id.to_string(), //for logging                
            bm_string: bm_str.to_string(),          //for logging   

            nada_receiver,  
            abr_mode, 
             
        }
    }

        pub async fn session_end(&mut self, pause_time: f64, context: &Context<Self>) {
            let now = context.scheduler.time();
            println!("[XRClient {}] Ending session at {:.8}s", self.server_ip, format_elapsed!(now));

            self.is_streaming = false;
            self.input_app_video = None;
            self.input_app_audio = None;
            self.input_app_haptics = None;
            self.output_app_tracking_sender = None;
            self.streamsocket_clone = None;
            self.decoder_queue.clear();
            self.jitter_buffer_warmup_ready = false; 

            // Schedule reboot after pause_time
            let delay = Duration::from_secs_f64(pause_time);
            
            print_magenta!("[session_end_schedule] delay: {}, now + delay: {} ", delay.as_secs_f64(), format_elapsed!(now + delay), ); 
            
            let epsilon = Duration::from_nanos(1);
            let target = now + delay.max(epsilon);

            context.scheduler.schedule_event(target, Self::session_reboot, ()).unwrap();
    }

    pub async fn session_reboot(&mut self, _: (), context: &Context<Self>) {
        let now = context.scheduler.time();
        println!("[XRClient {}] Rebooting session at {:.7}s", self.server_ip, format_elapsed!(now));

        let packet_size = 1400; // or pass from args/config
        self.t_0 = now;
        self.last_tracking_time = now;
        self.is_decoder_ready = false;
        self.decoder_queue.clear();

        // Re-establish streams
        self.configure_streams(packet_size, context).await;

        // Restart periodic tasks
        context.scheduler
            .schedule_event(Duration::from_millis(10), Self::video_receive_thread, ())
            .unwrap();

        // context.scheduler 
        //     .schedule_event(Duration::from_secs_f64(1.0 / self.framerate as f64), Self::vsync, () )
        //     .unwrap();

    }


    pub async fn configure_streams(&mut self, packet_size: usize, context: &Context<Self>) {
        // obtained by printing debug. We're using channel for purposes of mpsc for separate client and server processes, and separating the network interface of each.
        let stream_port: u16 = 9944;
        let stream_protocol: SocketProtocol = SocketProtocol::Channel;
        let dscp: Option<DscpTos> = None;
        let server_send_buffer_bytes: SocketBufferSize = SocketBufferSize::Maximum;
        let client_recv_buffer_bytes: SocketBufferSize = SocketBufferSize::Maximum;
        let client_send_buffer_bytes: SocketBufferSize = SocketBufferSize::Maximum;
        let server_recv_buffer_bytes: SocketBufferSize = SocketBufferSize::Maximum;
        // let packet_size: i32 = 1400;

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

            if self.abr_mode == 6 { // FovOptix only
                println!("CONFIIG 6"); 
                self.input_app_bw_probe = 
                    Some(stream_socket.subscribe_to_stream(FOVOPTIX_BW_PROBE, MAX_UNREAD_PACKETS)); 

                self.output_app_bw_probe_back = 
                    Some(stream_socket.request_stream( FOVOPTIX_BW_PROBE, self.t_0)); // way back for bw probing packets. 
            }

            self.streamsocket_clone = Some(stream_socket.clone());


            self.output_app_tracking_sender =
                Some(stream_socket.request_stream(TRACKING, self.t_0));

            XRClient::generate_tracking_data(self, (), context).await;
        }
    }

    pub fn generate_tracking_data<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        const HEAD_ID: u64 = 555;

        async move {
            let now = context.scheduler.time();        
            let mut position_offset = Vec3::ZERO;

            // let mut loop_deadline = now;
            let mut random_position_deadlne = now;

            // if let Some(tracking_send_socket) = self.output_app_tracking_sender.clone() {
            if self.is_streaming {
            
                let actual_position = self.current_coordinates_tracking;
                let last_position = self.last_coordinates_tracking;  
                
                let delta = actual_position - last_position; 

                let dt = now.duration_since(self.last_tracking_time); 
                let linear_velocity = if dt.as_secs_f32() > 0.0 {
                    delta / dt.as_secs_f32()
                } else {
                    Vec3::ZERO
                };

                let orientation = if delta.length_squared() > 1e-9 {
                    let yaw = delta.y.atan2(delta.x); 
                    Quat::from_rotation_y(yaw)
                }
                else{
                    Quat::IDENTITY
                }; 
                 // --- tracking packet ---
                let track = Tracking {
                    target_timestamp: TARGET_TIMESTAMP_TRACKING,
                    device_motions: vec![(
                        HEAD_ID,
                        DeviceMotion {
                            pose: Pose {
                                orientation,
                                position: actual_position,
                            },
                            linear_velocity,
                            angular_velocity: Vec3::ZERO, // could derive from orientation diff if needed (not needed)
                        },
                    )],
                    ..Default::default()
                };
                // print_yellow!("Generating tracking packet| dt: {}, data: {:#?}", dt.as_secs_f32(), track.device_motions); 


                if let Some(mut sender) = self.output_app_tracking_sender.clone() {
                    let arc_inner_app_receiver = sender.app_network_interface.clone();

                    let send_result = sender.send_header_tracking(&track, now);
                    let buffer: Vec<u8> = vec![0; CAPACITY_RX_BUFFER];

                    XRClient::read_app_send_network_interface(
                        self,
                        (),
                        now,
                        buffer,
                        arc_inner_app_receiver,
                    )
                    .await; // FUNCTION TO HANDLE NETWORK PACKETS!
                }

                // println!("CLIENT FRAMERATE = {}", self.framerate);
                let loop_deadline = Duration::from_secs_f32(1.0 / self.framerate / 3.0);

                self.last_coordinates_tracking = self.current_coordinates_tracking; 
                self.last_tracking_time = now;

                context
                    .scheduler
                    .schedule_event(loop_deadline, Self::generate_tracking_data, ())
                    .unwrap();
            }
        }
    }

    fn coords_to_vec3(&mut self, coords: Coords) -> Vec3 {
        let vecc = Vec3::new(coords.x as f32, coords.y as f32, coords.z as f32);
        vecc
    }
    fn vec3_to_coords(&mut self, vec: Vec3) -> Coords {
        let vecc = Coords::with_coords(vec.x as f64, vec.y as f64, vec.z as f64);
        vecc
    }

    // pub async fn send_tracking(&mut self, tracking: Tracking, now: TaiTime<0>) {
    //     if let Some(mut sender) = self.output_app_tracking_sender.clone() {
    //         let arc_inner_app_receiver = sender.app_network_interface.clone();

    //         let send_result = sender.send_header_tracking(&tracking, now);
    //         let buffer: Vec<u8> = vec![0; CAPACITY_RX_BUFFER];

    //         XRClient::read_app_send_network_interface(
    //             self,
    //             (),
    //             now,
    //             buffer,
    //             arc_inner_app_receiver,
    //         )
    //         .await; // FUNCTION TO HANDLE NETWORK PACKETS!
    //     }
    // }


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

            // debug_print!(
            //     DebugColor::DarkGreen,
            //     "{}[DBG XR_SERVER {}] Sending to network the following packets:",
            //     self.ip_self,
            //     elapsed.as_secs_f64(),
            // );
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
                                    9 => "BW probe", 
                                    _ => "?? IDK",
                                };
                                elapsed = now.duration_since(self.t_0);

                                let elapsed_tracking =
                                    now.duration_since(self.last_tracking_time).as_secs_f32();

                                // println!("[Client {} read ]: {} packet ", self.server_ip, str_id); 
                                // if stream_id == TRACKING {
                                //     print_yellow!(
                                //         "{} UL TRACKING [{}]-> Δt_tracking:{:.4} |length: {}| Stream ID: {}|",
                                //         format_elapsed!(now),
                                //         self.server_ip, 
                                //         elapsed_tracking,
                                //         packet_length,
                                //         str_id,
                                //     );
                                // }

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

                                // if packet.header_alvr.shard_index == 0 {
                                //     println!(
                                //         "{:.9}-CLIENT {} sending {:#?}",
                                //         now.duration_since(self.t_0).as_secs_f64(),
                                //         self.server_ip,
                                //         packet.header_alvr
                                //     );
                                // }
                                if stream_id == TRACKING {
                                    packet.edca_ac = EdcaAc::Voice; // explanation: While small, these packets are most important to be timely for rendering. 
                                    self.outport_tracking_network.send(packet).await

                                }
                                else if stream_id == FOVOPTIX_BW_PROBE{
                                    packet.edca_ac = EdcaAc::BestEffort; // Should not block actual VR traffic.. 
                                    self.outport_tracking_network.send(packet).await
                                }

                            } else {
                                println!(
                                    "{}",
                                    DebugColor::DarkGreen.to_color_fn()(String::from(
                                        "Failed to parse shard data, stopping."
                                    ))
                                );
                                stop = true;
                                // panic!("IS THIS HAPPENING EVER?"); // Hasn't happened since ever, erasing for cleaning
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

    pub async fn framed_send(
        &mut self,
        packet: &ClientControlPacket,
        context: &Context<Self>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // println!("FRAMEDSEND!");

        let mut buffer = vec![0; MAX_PACKET_SIZE_RECV];

        let serialized_size = bincode::serialized_size(&packet)? as usize;
        let packet_size = serialized_size + FRAMED_PREFIX_CONTROL_LENGTH;

        // println!("Framed send!");

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
        packetz.header_alvr.next_packet_index = 2;

        if matches!(packet, ClientControlPacket::NetworkStatistics(..)){
            packetz.edca_ac = EdcaAc::Video; 
        }
        else if matches!(packet, ClientControlPacket::DeadlineShardLossStat(..)) {
            packetz.edca_ac = EdcaAc::BestEffort; 
        }

        context
            .scheduler
            .schedule_event(
                Duration::from_nanos(10),
                Self::output_app_network_send,
                packetz,
            )
            .unwrap();
        Ok(())
    }

    pub async fn output_app_network_send(&mut self, packet: MpduPacket) {
        self.output_app_network.send(packet).await; // send to input_XR_app of STA
    }

    pub async fn output_control(
        &mut self,
        packet: ClientControlPacket,
        context: &Context<Self>,
    ) -> () {
        // Sends directly TCP packets related to Control.
        // println!("output_control");
        let pack = packet.clone();
        match packet {

            ClientControlPacket::NetworkStatistics(inner) => {
                // println!("sending stats packet!");
                let result = Self::framed_send(self, &pack, context).await;
                // println!("result of output control: {:?}", result);

            }
            ClientControlPacket::DeadlineShardLossStat(inner) => {
                // println!("Shardloss packet sent");
                let result = Self::framed_send(self, &pack, context).await;
            }
            _ => eprintln!("Uncovered match case!!"),
        }
        ()
    }

    pub fn report_frame_lost(
        &mut self, 
        mut frames: Vec<u32>,
        mut shards_lost: Vec<usize>,
        context: &Context<Self>,
    ) {
        frames.truncate(MAX_DEADLINE_IN_STATS);
        shards_lost.truncate(MAX_DEADLINE_IN_STATS);

        // println!("REPORT FRAME LOSt");
        let net = DeadlineShardlossStatPacket {
            frame_indexes: frames.clone(),
            shards_lost: shards_lost,
            // edca_ac: EdcaAc::BestEffort, // Non-crutial to be received timely, we don't want it to interfere with UL tracking. 
        };

        for frame in frames{
            // print_red!("[DBGGGY] MARKING FRAME {} for SKIPPING in REF DECODER", frame); 
            self.lost_ids_reference_buffer.push_back(frame); 
        }        
        

        context
            .scheduler
            .schedule_event(
                Duration::from_nanos(10),
                Self::output_control,
                ClientControlPacket::DeadlineShardLossStat(net),
            )
            .unwrap();
        
    }
    

    pub fn bw_probe_receive_thread<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let now = context.scheduler.time();

            // print_dblue!("{} RECEIVING PROBE MESSAGE", format_elapsed!(now));
            // Only proceed if both ends exist
            if let (Some(mut receiver), Some(mut sender)) =
                (self.input_app_bw_probe.clone(), self.output_app_bw_probe_back.clone())
            {
                let data: ReceiverData<()> = match receiver.recv(STREAMING_RECV_TIMEOUT) {
                    Ok(d) => d,
                    Err(ConnectionError::TryAgain(_)) => return,
                    Err(ConnectionError::Other(_)) => return,
                };

                // Extract the raw bytes and meta information
                // let (raw_bytes, hidden_offset, length) = data.get_buffer(); 
                
                // let buf = &packet.data_inner; 
                let raw = data.get_buffer();
                let buf_len = raw.len();

                // --- Step 1: constants ---
                let start_hdr = SHARD_PREFIX_SIZE;
                let hdr_size = bincode::serialized_size(&BwProbeHeader {
                    seq: 0,
                    tx_instant_us: 0,
                    rx_instant_s_video_frame: 0.,
                    payload_len: 0,
                }).unwrap() as usize;

                // --- Step 2: bounds check ---
                if buf_len < start_hdr + hdr_size {
                    eprintln!("[BW_PROBE] Too short buffer: {} bytes", buf_len);
                    return;
                }

                // --- Step 3: deserialize header ---
                let hdr_slice = &raw[start_hdr..start_hdr + hdr_size];
                let mut hdr: BwProbeHeader =
                    bincode::deserialize(hdr_slice).expect("Failed to deserialize BwProbeHeader");
                
                hdr.rx_instant_s_video_frame = self.last_rx_frame_instant.duration_since(TaiTime::EPOCH).as_secs_f32(); // TAG PROBE WITH RX_TIME of LAST VIDEO FRAME

                // --- Step 5: re-serialize header ---
                let new_hdr_bytes = bincode::serialize(&hdr).unwrap();
                let mut updated_raw = raw.clone();
                updated_raw[start_hdr..start_hdr + new_hdr_bytes.len()].copy_from_slice(&new_hdr_bytes);

                // --- Step 6: (optional) extract payload slice ---
                let payload_start = start_hdr + hdr_size;
                let payload_end = payload_start + hdr.payload_len as usize;
                let payload_slice = &updated_raw[payload_start.min(buf_len)..payload_end.min(buf_len)];

                // --- Step 7: wrap and send back ---
                let buf = crate::lib::alvr_stream_socket::Buffer {
                    inner: updated_raw,
                    hidden_offset: SHARD_PREFIX_SIZE,
                    length: buf_len - SHARD_PREFIX_SIZE,
                    _phantom: std::marker::PhantomData::<()>,
                };

                // Send back (echo)
                let _ = sender.send(buf, now);
                // Forward app→network to actually put the echo on the simulated link
                let arc_inner_app_receiver = sender.app_network_interface.clone();
                let buffer: Vec<u8> = vec![0; CAPACITY_RX_BUFFER];

                XRClient::read_app_send_network_interface(
                    self,
                    (),
                    now,
                    buffer,
                    arc_inner_app_receiver,
                )
                .await;

                // println!(
                //     // DebugColor::Cyan,
                //     "[BW_ECHO] Client {} echoed probe back at {:.6}s",
                //     self.server_ip,
                //     format_elapsed!(now)
                // );
            }
        }
    }

    pub fn video_receive_thread<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let now = context.scheduler.time(); 
            if self.is_streaming {
                // println!("RECEIVING VIDEO!!");
                if let Some(mut receiver) = self.input_app_video.clone() {
                    let (frames_lost, shards_lost): (Vec<u32>, Vec<usize>);

                    if let Some(mut ssocket) = self.streamsocket_clone.as_mut() {
                        let mut counter = 0;
                        (frames_lost, shards_lost) =
                            StreamSocket::flush_shards_lost_deadline(&mut ssocket);

                        if !frames_lost.is_empty() {
                            // println!(
                            //     "{} [{}] - FRAMES LOST {:?}, SHARDS LOST {:?}",
                            //     format_elapsed!(now), 
                            //     self.server_ip, 
                            //     &frames_lost[..],
                            //     &shards_lost[..]
                            // );
                            self.report_frame_lost(frames_lost, shards_lost, context);
                        }
                    }

                    let data: ReceiverData<VideoPacketHeader> =
                        match receiver.recv(STREAMING_RECV_TIMEOUT) {
                            Ok(data) => data,
                            Err(ConnectionError::TryAgain(_)) => return,
                            Err(ConnectionError::Other(_)) => return,
                        };

                    let mut packets_lost_deadline = 0;
                    let frame_id = data.get_frame_index();

                    let Ok((nal)) = data.get() else {
                        println!("UNABLE TO GET HEADER NAL? ");
                        return;
                    };

                    let sized_vec = nal[..20.min(nal.len())].to_vec();                    

                    ///////////////////////////////////////////////
                    // pub const EVEREST_ENABLED : bool = false; // EVEREST STATS CLIENT
                    let mut everest_throughput: f32 = -1.0;     // initialize, if negative then on rx don't count 
                    let mut everest_capacity: f32 = -1.0;       // (only one measure per frame of either)

                    let mut command_abr_everest = EverestCommand::Continue; 


                    if self.everest_enabled {
                        pub const EVEREST_CLASSIC : bool = false; 
                        if self.frame_size_exp_avg == 0.0 { 
                            self.frame_size_exp_avg = data.get_bytes_in_frame() as f32; // initialize avg only on first value
                        }
                        if self.d_short_exp_avg == 0.0 {
                            self.d_short_exp_avg = data.get_frame_span() *  data.get_frame_interarrival() / T_SHORT_EVEREST_S; 
                        }
                        if self.d_long_exp_avg == 0.0 {
                            self.d_long_exp_avg = data.get_frame_span() *  data.get_frame_interarrival() / T_LONG_EVEREST_S; 
                        }
                        pub const MPDU_MAX_SIZE: u32 = 1500; 
                        pub const THETA_EWMA: f32 = 0.01;  // we want the long term expectation for comparison of individual frame sizes. 

                        pub const T_SHORT_EVEREST_S: f32 = 1.0; 
                        pub const T_LONG_EVEREST_S: f32 = 5.0; 
          
                        let frame_size_bytes = data.get_bytes_in_frame() as f32; 
                        let frame_span = data.get_frame_span(); 

                        if frame_span != 0.0 { // prevent division by zero
                            if EVEREST_CLASSIC { 
                                if is_keyframe(&nal){
                                    everest_throughput = frame_size_bytes * 8.0 / frame_span; 
                                }
                                else{                   
                                    let frame_size_mtu_portion = (frame_size_bytes as u32/ MPDU_MAX_SIZE ) as f32 * MPDU_MAX_SIZE as f32; // just the part with full packets of MTU
                                    // let remainder_size =    data.get_bytes_in_frame() % 1500 ;
                                    if frame_span != 0.0 {
                                        everest_capacity = frame_size_mtu_portion * 8.0 / frame_span; 
                                    }
                                }
                            }
                            else{ // EVEREST-Intra
                                self.frame_size_exp_avg = ( THETA_EWMA * frame_size_bytes )  + ( 1.0 - THETA_EWMA ) * self.frame_size_exp_avg ; 
                                
                                if frame_size_bytes > self.frame_size_exp_avg {
                                    everest_throughput = frame_size_bytes * 8.0  / frame_span; 
                                
                                }
                                else{
                                    let frame_size_mtu_portion = (frame_size_bytes as u32/ MPDU_MAX_SIZE ) as f32 * MPDU_MAX_SIZE as f32;  // just the part with full packets of MTU
                                    everest_capacity = (frame_size_mtu_portion * 8.0 ) / frame_span ;    

                                    // print_yellow!("Capacity ev: L / deltaT = {} ({}) / {} = {}", frame_size_mtu_portion, frame_size_bytes, frame_span, everest_capacity); 
                                }
                            }
                        }

                        let interarrival = data.get_frame_interarrival(); 
                        self.d_short_exp_avg = (interarrival / T_SHORT_EVEREST_S * frame_span )  + (1.0 - interarrival/ T_SHORT_EVEREST_S) * self.d_short_exp_avg; 
                        self.d_long_exp_avg =  (interarrival / T_LONG_EVEREST_S  * frame_span ) + (1.0 - interarrival/ T_LONG_EVEREST_S) * self.d_long_exp_avg; 
                        
                        // let d_lower_everest =  
                        let mut bitrate_mbps = self.last_bitrate_perfect_info_update_mbps; 
                        let bitrate_bps_comp = bitrate_mbps * 1e6; 

                        if !self.bitrate_ladder_perfect_info_update.is_empty(){
                            
                            let &value_b2 = self.bitrate_ladder_perfect_info_update.iter().find(|&&x| x > bitrate_bps_comp).unwrap_or_else(|| {
                                // if nothing higher, use the highest available:
                                self
                                    .bitrate_ladder_perfect_info_update
                                    .last()
                                    .unwrap_or(&bitrate_bps_comp)
                            });
                            let d_lower_everest = bitrate_bps_comp / value_b2 * (1.0 / self.framerate);   // IFT in average or expectation from fps? assuming FPS
                            
                            // print_yellow!("b1 = {}, b2 = {} , 1/FPS = {}", bitrate_bps_comp, value_b2, 1.0/self.framerate); 
                            
                            let d_upper_everest = 1.0 / self.framerate; 

                            const T_LOW_EVEREST_S: f32 = 0.005; 
                            const T_HIGH_EVEREST_S: f32 = 0.020;   


                            if self.d_short_exp_avg >= d_upper_everest 
                            {
                                self.d_short_exp_avg = T_LOW_EVEREST_S; 
                                command_abr_everest = EverestCommand::SlowDown; 
                            }
                            if self.d_long_exp_avg < d_lower_everest{
                                self.d_long_exp_avg = T_HIGH_EVEREST_S; 
                                command_abr_everest = EverestCommand::SpeedUp; 
                            }

                            // crate::print_blue!("[CLIENT EVEREST ]------------------------------\nIs D_short({}) >= D_upper({})? -> {}\nIs D_long({}) < D_lower({})? -> {}\nCMD={:?}",
                            //          self.d_short_exp_avg, d_upper_everest, self.d_short_exp_avg >= d_upper_everest , self.d_long_exp_avg, d_lower_everest,  self.d_long_exp_avg < d_lower_everest, command_abr_everest ); 


                        }
                    }
                    
                    
                    
                    //////////////////////////////////////////////  // NADA rcv loop upon succesfully receiving a full frame. 
                    let mut nada_stats: NadaStats = NadaStats::default(); 

                    if let Some( nada_receiver_in) = self.nada_receiver.as_mut(){
                            
                        let mut nada_receiver = nada_receiver_in.lock().unwrap(); 

                        let frame_send_timestamp =     data.get_tx_time_first();  // as secs; 
                        let frame_recv_timestamp =     data.get_rx_time_last();  //as secs; 
                        let size = data.get_bytes_in_frame() as usize; 

                        let micros_send_ts = (frame_send_timestamp * 1_000_000.0).round() as i64;
                        let micros_rcv_ts =  (frame_recv_timestamp * 1_000_000.0).round() as i64; 
                        
                        nada_receiver.compute_oneway_delay(micros_send_ts, micros_rcv_ts); //inputs as micros
                        nada_receiver.update_receive_loss_rate(size);
                        let is_feedback_on = nada_receiver.time_to_report_feedback(now, false, false);

                        //if there is a feedback to report
                        if is_feedback_on{
                            //send RTCP feedback report containing values of: rmode, x_curr, and r_recv
                            nada_stats.nada_feedback = true;
                            nada_stats.nada_xcurr = nada_receiver.x_curr;
                            nada_stats.nada_rmode = match nada_receiver.rmode {
                                RateUpdateMode::AcceleratedRampUp => 0,
                                RateUpdateMode::GradualUpdate => 1,
                                _ => 1,
                            };
                            nada_stats.nada_recv = nada_receiver.r_recv;

                            //To Debug NADA Receiver, report values of: t_last, d_fwd, d_tilde, d_queue, p_loss
                            nada_stats.plr = nada_receiver.p_loss;
                            nada_stats.d_tilde = nada_receiver.d_tilde;
                            nada_stats.d_queue = nada_receiver.d_queue;

                            //update t_last = t_curr
                            nada_receiver.update_t_last(now);
                        }else{
                            nada_stats.nada_feedback = false;
                        }
                    }

                    ///////////////////////////////////////////////// NADA STATS END
                    let net = NetworkStatisticsPacket {
                        // Frame specific metrics
                        frame_index: frame_id as i32, // index of the current frame
                        frame_span: data.get_frame_span(), // duration of the current frame
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
                        everest_capacity_update: everest_capacity, 
                        everest_throughput_update: everest_throughput, 
                        everest_dshort: self.d_short_exp_avg, 
                        everest_dlong: self.d_long_exp_avg, 
                        everest_command: command_abr_everest,
                        buffer_level_decoder: self.decoder_queue.len() as u8,  
                        rebuffering_events_last_s: self.rebuffer_event_counter.sum_in_period() as u8, // should be impossible to overflow unless FPS > 256 (not planned, makes no sense) 
                        // edca_ac: EdcaAc::Video, // Explanation: Given we're computing the VF-RTT of video packets based on arrivals, let's assume this AC for UL to get the same 'treatment' by EDCA.  
                        nada_stats, 
                    };

                    if self.last_throughput_avg == 0.0 {
                        self.last_throughput_avg = net.bytes_in_frame as f32 / net.frame_interarrival;  
                    }
                    else{
                        let throughput_now = net.bytes_in_frame as f32 / net.frame_interarrival;  
                        self.last_throughput_avg = ALPHA_THROUGHPUT * throughput_now + ( 1.0  - ALPHA_THROUGHPUT ) * self.last_throughput_avg;
                    } 
                    // println!("[CLIENT] Sending networkstats packet in UL: {:#?}", net);

                    // send frame and network statistics for every reconstructed video frame
                    debug_print!(
                        DebugColor::Gold,
                        "[DBG Client {} RX frame] Frame {:2.0} received, sending stats packet in UL",
                        self.server_ip,
                        data.get_frame_index()
                    );

                    context
                        .scheduler
                        .schedule_event(
                            Duration::from_nanos(10),
                            Self::output_control,
                            ClientControlPacket::NetworkStatistics(net),
                        )
                        .unwrap();
                    // self.output_control(ClientControlPacket::NetworkStatistics(net)).await;

          

                    debug_print!(
                        DebugColor::Gold,
                        "[DEBUG DECODE] NAL first 20 bytes: {:?}",
                        sized_vec
                    );

                    self.decoder_queue.push((frame_id as usize, nal.to_vec()));

                    ()
                }
                // if let Some(stats) = &mut *STATISTICS_MANAGER.lock() {
                //     stats.report_video_packet_received(header.timestamp);
                //     }
                // }
            }
        }
    }


    // Function to convert YUV420p to RGB
    pub fn yuv420_to_rgb(yuv: &[u8], width: usize, height: usize) -> Vec<u8> {
        let mut rgb = Vec::with_capacity(width * height * 3);

        let y_size = width * height;
        let uv_size = (width / 2) * (height / 2);

        let y_plane = &yuv[0..y_size];
        let u_plane = &yuv[y_size..y_size + uv_size];
        let v_plane = &yuv[y_size + uv_size..];

        for i in 0..height {
            for j in 0..width {
                let y_index = i * width + j;
                let u_index = ((i / 2) * (width / 2)) + (j / 2);
                let v_index = ((i / 2) * (width / 2)) + (j / 2);

                let y = y_plane[y_index] as f32;
                let u = u_plane[u_index] as f32 - 128.0;
                let v = v_plane[v_index] as f32 - 128.0;

                // RGB conversion formula
                let r = (y + 1.402 * v).max(0.0).min(255.0) as u8;
                let g = (y - 0.344136 * u - 0.714136 * v).max(0.0).min(255.0) as u8;
                let b = (y + 1.772 * u).max(0.0).min(255.0) as u8;

                rgb.push(r);
                rgb.push(g);
                rgb.push(b);
            }
        }

        rgb
    }
    // Generate decoder key from client IP and stream type

    fn get_decoder_key(&self, client_ip: IpAddr, is_max_bitrate: bool) -> IpAddr {
        if is_max_bitrate {
            // Use original IP for max bitrate streams
            return client_ip;
        }

        match client_ip {
            IpAddr::V4(ipv4) => {
                let mut octets = ipv4.octets();
                // Modify last octet to create a virtual IP for regular streams
                // Adding 100 creates sufficient separation while staying within valid IPv4 range
                octets[3] = octets[3].saturating_add(100);
                IpAddr::V4(Ipv4Addr::from(octets))
            }
            _ => {
                println!("use ipv4 for now!! warning");
                client_ip
            }
        }
    }

    async fn cleanup_old_frames_vmaf(&mut self, now: TaiTime<0>, current_frame_id: usize, ip: IpAddr) -> Result<()> {
        // Only clean up frames that are at least 100 frames behind
        if current_frame_id <= KEEP_FRAMES_DISK_INDEX {
            return Ok(());
        }

        let oldest_frame_to_keep = current_frame_id - KEEP_FRAMES_DISK_INDEX;
        // let base_dir = &format!("Sink_for_video/{}", &self.name_folder);

        // // Define paths to reference and lossy directories
        // let ref_dir = format!("{}/{}/reference_rgb", base_dir, ip);
        // let lossy_dir = format!("{}/{}/lossy_rgb", base_dir, ip);

        self.flush_vmaf_buffer(now).await?;


        // Function to remove older frames from a directory
        let remove_old_frames = |dir: &str| -> Result<()> {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.filter_map(Result::ok) {
                    let path = entry.path();
                    if let Some(filename) = path.file_name().and_then(|f| f.to_str()) {
                        // Parse frame number from filename (e.g., "frame_0042.rgb")
                        if let Some(frame_str) = filename
                            .strip_prefix("frame_")
                            .and_then(|s| s.strip_suffix(".rgb"))
                        {
                            if let Ok(frame_num) = frame_str.parse::<usize>() {
                                if frame_num < oldest_frame_to_keep {
                                    if let Err(e) = std::fs::remove_file(&path) {
                                        eprintln!(
                                            "Failed to remove old frame {}: {}",
                                            path.display(),
                                            e
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        };

        // // Clean up both directories
        // remove_old_frames(&ref_dir)?;
        // remove_old_frames(&lossy_dir)?;

        if current_frame_id % KEEP_FRAMES_DISK_INDEX == 0 {
            println!("Cleaned up frames older than {}", oldest_frame_to_keep);
        }

        Ok(())
    }


    
    pub async fn vmaf_analysis(
        &mut self,
        sample: Vec<u8>,
        ref_sample: Vec<u8>,
        now: TaiTime<0>,
        frame_id: usize,
        ip: IpAddr,
    ) -> Result<()> {
        // Skip if either sample is empty
        if sample.is_empty() || ref_sample.is_empty() {
            println!(
                "Skipping VMAF analysis for frame {} - sample sizes: {}, ref: {}",
                frame_id,
                sample.len(),
                ref_sample.len()
            );
            return Ok(());
        }

        // Add current frame to buffer
        self.vmaf_frame_buffer.push_back((sample, ref_sample, now, frame_id, ip));
        
        // Only process if we have enough frames
        if self.vmaf_frame_buffer.len() < self.vmaf_batch_size {
            return Ok(());
        }
        
        // Process the accumulated frames
        println!("Processing batch of {} frames for VMAF analysis", self.vmaf_frame_buffer.len());
        
        // Ensure metrics logger is initialized
        if self.metrics_logger.is_none() {
            println!("INITIALIZING LOGGER IN FOLDER: {}", self.name_folder);

            match MetricsLogger::new(ip, &self.name_folder) {
                Ok(logger) => {
                    println!("Initialized metrics logger for VMAF analysis");
                    self.metrics_logger = Some(logger);
                }
                Err(e) => {
                    eprintln!("Failed to initialize metrics logger: {}", e);
                    self.vmaf_frame_buffer.clear(); // Clear buffer on error
                    return Ok(());
                }
            }
        }
        
        // Process all frames in buffer
        while !self.vmaf_frame_buffer.is_empty() {
            // Take one frame from the buffer
            if let Some((frame_sample, frame_ref, frame_time, frame_id, frame_ip)) = self.vmaf_frame_buffer.pop_front() {
                // Create directories for temporary storage if they don't exist
                let base_dir = format!("Sink_for_video/{}", &self.name_folder);
                if let Err(e) = std::fs::create_dir_all(&base_dir) {
                    eprintln!("Failed to create directory {}: {}", base_dir, e);
                    continue;
                }

                // Save frames to temporary files
                let ref_path = format!(
                    "{}/{}/reference_rgb/frame_{:04}.rgb",
                    base_dir, frame_ip, frame_id
                );
                let lossy_path = format!(
                    "{}/{}/lossy_rgb/frame_{:04}.rgb", 
                    base_dir, frame_ip, frame_id
                );
                
                // Create parent directories
                if let Err(e) = std::fs::create_dir_all(format!("{}/{}/reference_rgb", base_dir, frame_ip)) {
                    eprintln!("Failed to create reference directory: {}", e);
                    continue;
                }
                if let Err(e) = std::fs::create_dir_all(format!("{}/{}/lossy_rgb", base_dir, frame_ip)) {
                    eprintln!("Failed to create lossy directory: {}", e);
                    continue;
                }

                // Write frames to disk
                if let Err(e) = std::fs::write(&ref_path, &frame_ref) {
                    eprintln!("Failed to write reference frame: {}", e);
                    continue;
                }
                if let Err(e) = std::fs::write(&lossy_path, &frame_sample) {
                    eprintln!("Failed to write lossy frame: {}", e);
                    continue;
                }

                let timestamp_ms = frame_time.duration_since(self.t_0).as_secs_f64();

                // Process frame metrics
                if let Some(logger) = &self.metrics_logger {
                    logger
                        .process_frame_metrics(
                            frame_id as u64,
                            timestamp_ms,
                            &ref_path,
                            &lossy_path,
                            frame_ip,
                        ); 
                }
            }
        }
        
        Ok(())
    }

 
    pub async fn flush_vmaf_buffer(&mut self, now: TaiTime<0>) -> Result<()> {
        // If there are any frames left in the buffer, process them
        if !self.vmaf_frame_buffer.is_empty() {
            println!("Flushing {} remaining frames in VMAF buffer", self.vmaf_frame_buffer.len());
            
            // Get IP from the first frame in buffer (or use a default)
            let default_ip = IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1));
            let ip = self.vmaf_frame_buffer.front().map(|f| f.4).unwrap_or(default_ip);
            
            // Call vmaf_analysis with empty frames to trigger processing of buffer
            self.vmaf_analysis(Vec::new(), Vec::new(), now , 0, ip).await?;
        }
        
        Ok(())
    }

    

    pub fn vsync<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let now = context.scheduler.time();
            let T_vsync = Duration::from_secs_f64(1.0 / self.framerate as f64);
            let mut lost_frames_aux =self.lost_ids_reference_buffer.clone(); // Keep for passing to display, but its population logic might need review

            if self.original_decoder.is_none() && USE_FFMPEG_DEMO{
                self.original_decoder = Some(
                    Arc::new( Mutex::new( HevcDecoder::new(
                        self.framerate as u32,
                        WIDTH_ENCODER as u32,
                        HEIGHT_ENCODER as u32,
                        &format!("[SINGLE DECODER {}]", self.server_ip), 
                    )))); 
            }

            let third_octet = get_third_octet(self.server_ip).unwrap();                 


            let csv_path = get_prefix_path(&format!(
                "Results/{}/trace_offline_video{}.csv",
                self.name_folder,
                third_octet,
                // format_elapsed!(now), 
            ));


            // --------------Initialize offline CSV tracker for frames ------------- 
           if self.offline_csv_trace.writer.is_none() && USE_FFMPEG_DEMO {
                if !Path::new(&csv_path).exists() {
                    // Encoder hasn’t created the file yet – keep your wait/log if you want
                    println!("waiting until offline CSV created");
                } else {
                    print_green!("Read from: {}", csv_path);

                    // just set the path and let CsvTrace open (append) with BufWriter
                    self.offline_csv_trace.path = csv_path.clone().into();
                    self.offline_csv_trace
                        .init_writer()
                        .expect("CSV trace created by encoder is missing!");
                }
            }

            // Clean up older processed frames from tracking buffer
            let current_last_processed = self.last_processed_frame_id;
            self.missing_frames_buffer.retain(|&id, &mut processed| {
                !processed || id.saturating_sub(current_last_processed) <= 100 // Avoid underflow
            });         

            if !self.jitter_buffer_warmup_ready {
                if self.decoder_queue.len() < TARGET_FRAMES_DECODER_QUEUE {
                    // do nothing
                }
                else {
                    self.jitter_buffer_warmup_ready = true; 
                    print_yellow!("Jitter buffer ready!!", ); 
                }
            }
            else
            {
            // Process the next frame if available from the regular stream queue
                if let Some((id_f, video_frame)) = self.decoder_queue.pop() {


                    // crate::print_magenta!(
                    //                 // DebugColor::Violet,
                    //                 "{} - [DBG VSYNC {}] Frame id {} processing. Size: {}, Queue len: {}", 
                    //                 format_elapsed!(now),
                    //                 self.server_ip,
                    //                 id_f,
                    //                 video_frame.len(),
                    //                 self.decoder_queue.len(),
                    //                 // interarrival.as_secs_f32(),
                    //             );

                    let lost = if self.last_seen_id != 0 && id_f != self.last_seen_id + 1 { 1 } else { 0 };
                    self.last_seen_id = id_f;

                    let timestamp = now.duration_since(self.t_0).as_secs_f64();           // TaiTime -> f64 seconds

                    
                    if !Path::new(&csv_path).exists()  {
                        // panic!("CSV trace still missing after {}ms: {}", max_wait_ms, csv_path);
                    }
                    else{

                        // emu effects part here? 

                    self.offline_csv_trace
                            .write_record(&[
                                "", // offset (preamble only)
                                "", // source (preamble only)
                                "", // IDR_freq (preamble only)
                                &format!("{:.6}", timestamp),
                                &id_f.to_string(),
                                &lost.to_string(),
                                &format!("{:.3}", self.last_throughput_avg),
                            ])
                            .await
                            .expect("failed to append offline csv row");

                        // Optional cheap periodic flush (avoid flushing every row)
                        if id_f % BATCH_SIZE_CSV == 0 {
                            let _ = self.offline_csv_trace.flush().await;
                        }
                    }
                    let mut ip_client = self.server_ip; 
                    if let IpAddr::V4(ip4) = ip_client {
                        let mut octets = ip4.octets();
                        if octets[3] == 2 {
                            octets[3] = 1; // Change last byte from 2 to 1
                            ip_client = IpAddr::V4(std::net::Ipv4Addr::from(octets));
                        }
                    }
                

                    if id_f > self.last_processed_frame_id + 1 {
                        let missing_start = self.last_processed_frame_id + 1;
                        let missing_end = id_f - 1; // Inclusive end

                        print_pretty!(
                            DebugColor::Red,
                            "{} Detected missing regular frames between {} and {}",
                            ip_client, missing_start, missing_end,
                        );

                        for missing_id in missing_start..=missing_end { // Iterate inclusive
                            if !self.missing_frames_buffer.contains_key(&missing_id) {
                                print_pretty!(
                                    DebugColor::DarkOrange,
                                    "{} Added missing frame {} to tracking system",
                                    ip_client, missing_id,
                                );
                                self.missing_frames_buffer.insert(missing_id, false); // Mark as not processed yet
                            }

                            // Check if already processed (e.g., by a previous recovery attempt)
                                if *self.missing_frames_buffer.get(&missing_id).unwrap_or(&false) {
                                print_pretty!(DebugColor::Purple, "Missing frame {} already processed, skipping", missing_id);
                                continue;
                                }
                                // Mark as processed in the tracking buffer *after* attempting to process
                                self.missing_frames_buffer.insert(missing_id, true);
                        }
                    } // End of missing frame recovery


                    // Update last processed frame ID for the *regular* stream continuity check
                    self.last_processed_frame_id = id_f;

                    // Keyframe detection (keep as is)
                    if is_keyframe(&video_frame) {
                        self.dec_saw_keyframe = true;
                        print_pretty!(
                            DebugColor::Magenta,
                            "*** KEYFRAME DETECTED IN REGULAR STREAM *** Size: {} bytes",
                            video_frame.len(),
                        );
                        self.dec_saw_keyframe_last_t = now;
                    }

                    if !self.is_decoder_ready && USE_FFMPEG_DEMO { 

                        if !video_frame.is_empty() { self.initialization_buffer.push(video_frame.clone()); }
                        
                        let has_enough_frames = self.initialization_buffer.len() >= self.min_buffered_frames;
                        if is_keyframe(&video_frame) { self.dec_saw_keyframe = true; self.dec_saw_keyframe_last_t = now; }

                        if has_enough_frames && self.dec_saw_keyframe {
                            print_pretty!(DebugColor::Cyan, "Decoder initialization criteria met! Buffered {} frames", self.initialization_buffer.len());

                            print_pretty!(DebugColor::Cyan, "Initialization processing complete.", );
                            self.is_decoder_ready = true;
                            self.initialization_buffer.clear();

                        } else { /* ... log buffering status ... */ }

                    } // End of initialization logic


                    if self.is_decoder_ready && USE_FFMPEG_DEMO {
                        if !video_frame.is_empty() {
                            
                            if let Some(interarrival) = now.checked_duration_since(self.last_decoded_frame_instant) {
                                let miin: usize = usize::min(video_frame.len(), 50);
                                // crate::print_magenta!(
                                //     // DebugColor::Violet,
                                //     "{} - [DBG VSYNC {}] Frame id {} processing. Size: {}, Queue len: {}, Interarrival: {:.4}s", 
                                //     format_elapsed!(now),
                                //     ip_client,
                                //     id_f,
                                //     video_frame.len(),
                                //     self.decoder_queue.len(),
                                //     interarrival.as_secs_f32(),
                                // );
                            }

                            if let Some(decoder_arc) = self.original_decoder.clone(){
                                let mut decoder = decoder_arc.lock().unwrap();

                                // Process the current frame pair using process_packets
                                print_pretty!(DebugColor::Cyan, "Processing frame #{} ", id_f);
                                decoder.process_packet(video_frame.clone());


                                // Try to get a synchronized frame pair immediately after processing
                                // This might yield 0, 1 or more pairs depending on internal buffering and state
                                while let Some((frame, _)) = decoder.next_decoded_frame() {
                                    // print_pretty!(
                                    //     DebugColor::Green,
                                    //     "Retrieved frame #{}",
                                    //     decoder.decoded_frame_counter,
                                    // );

                                    // Display synchronized frame pair (keep display logic)
                                    thread_local! {
                                        static DISPLAY_WINDOWS: RefCell<HashMap<IpAddr, Window>> = RefCell::new(HashMap::new());
                                    }

                                    DISPLAY_WINDOWS.with(|windows_cell| {
                                        let mut windows = windows_cell.borrow_mut();
                                        // Ensure window exists (keep window creation logic)
                                        if !windows.contains_key(&self.server_ip) { 
                                            
                                                let window_title = format!("{} - Frame Display [{}]", format_elapsed!(now), self.server_ip);
                                                let window_width = (WIDTH_ENCODER as f64 * SCALE_FACTOR_WINDOW ) as usize;
                                                let window_height = (HEIGHT_ENCODER as f64 * SCALE_FACTOR_WINDOW) as usize;
                                                match Window::new(
                                                    &window_title,
                                                    window_width,
                                                    window_height,
                                                    WindowOptions::default()
                                                ) {
                                                    Ok(window) => {
                                                        // hide_by_title_with_wmctrl(&window_title);
                                                        windows.insert(self.server_ip.clone(), window);
                                                        print_pretty!(DebugColor::Green,
                                                            "Created display window for {} ({} x {})", 
                                                            self.server_ip, window_width, window_height,);
                                                    },
                                                    Err(e) => {
                                                        print_pretty!(DebugColor::Red,
                                                            "Failed to create display window: {}", 
                                                            e,);
                                                    }
                                                }   
                                            }

                                        if let Some(window) = windows.get_mut(&self.server_ip) {
                                            // Calculate similarity (keep calculation)
                                        
                                            let bitrate_sample_mbps = extract_br_value(&self.name_folder).unwrap_or(0.0);

                                            // Pass the current SyncState to the display function
                                            self.lost_ids_reference_buffer = VecDeque::new(); 
                                            
                                            // let slice: &[u32] = lost_frames_aux.make_contiguous();
                                            let display_result = display_single_frame_with_info(
                                                &frame,
                                                &self.server_ip,
                                                decoder.decoded_frame_counter,
                                                window,
                                                now,
                                                self.last_bitrate_perfect_info_update_mbps,
                                                lost_frames_aux.clone(),
                                                &mut self.lost_frames_buffer, // Pass mutable lost frames buffer if needed
                                                &self.test,
                                                &self.bm_string, 
                                                &self.sim_unique_string, 

                                            );
                                        
                                            lost_frames_aux = VecDeque::new(); 

                                            if display_result {      
                                                print_pretty!(DebugColor::Green,
                                                "Successfully displayed frame #{}", 
                                                decoder.decoded_frame_counter);                             
                                            }
                                            else {
                                                print_pretty!(DebugColor::Red,
                                                "Failed to display frame #{}", 
                                                decoder.decoded_frame_counter,);
                                        }                                            
                                        } // End if let Some(window)
                                    }); // End DISPLAY_WINDOWS.with
                                } // End while let Some(frame_pair)

                            } else {
                                print_pretty!(DebugColor::Red, "Synchronized decoder not initialized!", );
                            }
                        } else {
                            // Log cases where frames might be empty if unexpected
                            if video_frame.is_empty() { print_pretty!(DebugColor::Yellow, "Received empty regular frame #{}", id_f); }
                        }
                    } // End if self.is_decoder_ready

                    // Update timestamp and send placeholder output (keep as is)
                    self.last_decoded_frame_instant = now;
                    self.out_video_decoded.send(video_frame[0..10.min(video_frame.len())].to_vec()).await;

                } else { // Decoder queue was empty
                    // print_brown!("{} - [{}] REBUFFER EVENT!!", format_elapsed!(now), self.server_ip ); 
                    self.rebuffer_event_counter.add_one(now); 

                } // End if let Some((id_f, video_frame))
            }

            // print_red!("{} - Scheduling VSYNC at {}", format_elapsed!(now), format_elapsed!(now + T_vsync));
            context.scheduler.schedule_event(T_vsync, Self::vsync, ()).unwrap();
        }
    }  

    pub async fn input_perfect_information_bitrate(&mut self, bitrate_msg: PerfectInfoBitrateMessage, context: &Context<Self>) {
        
        let now = context.scheduler.time(); 

        let bitrate = bitrate_msg.bitrate_mbps; 
        // if self.bitrate_ladder_perfect_info_update.is_none(){
        if let Some(veccc) = bitrate_msg.bitrate_ladder_bps{
            self.bitrate_ladder_perfect_info_update = veccc; 
        }
        // crate::print_brown!("{} - perfect bitrate input: {} | Bitrate ladder : {:#?}", format_elapsed!(now), bitrate, self.bitrate_ladder_perfect_info_update); 
        self.last_bitrate_perfect_info_update_mbps = bitrate; 
    }


    pub async fn input_coordinates_STA(&mut self, coords: Coords, context: &Context<Self>){
        self.current_coordinates_tracking = self.coords_to_vec3(coords); // not much else to do, tracking output will just use the last value 3*FPS. Angular velocities might be considered in future, but not yet; TODO 
    }

    pub async fn in_from_network(&mut self, frame: TimedFrame, context: &Context<Self>) {
        let packet_vec = frame.vec;
        let now = frame.timestamp;

        for packet in packet_vec {
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
                        // println!("receiver: {:?}", receiver);
                        
                        // println!("Received audio packet: {:?}", header); // We do nothing with received audio packets at time,
                                                                            // it's just empty data being sent every 10ms following structure of ALVR game audio transmissions. 
                    }

                }
                VIDEO => {
                    if let Some(sock) = self.input_app_video.clone() {
                        // println!("app lock");
                        let _sender = sock.network_app_interface.lock().unwrap().send(&buffer); // We send the packet from network to the application, where it needs to be now read and passed to the application!

                        if let Some(mut ssocket) = self.streamsocket_clone.as_mut() {
                            
                            self.last_rx_frame_instant = now; 
                            
                            let _resulllt = StreamSocket::recv(
                                &mut ssocket,
                                self.server_ip,
                                sock.inner,
                                context,
                            );
                        }
                    } else {
                        print!(".");
                    }
                }

                FOVOPTIX_BW_PROBE => {
                    // println!("Client: RECEIVED PROBE!"); 
                    if let Some(sock) = self.input_app_bw_probe.clone() {
                        let _sender = sock.network_app_interface.lock().unwrap().send(&buffer); 

                        if let Some(mut ssocket) = self.streamsocket_clone.as_mut() {

                            let _resulllt = StreamSocket::recv(
                                &mut ssocket,
                                self.server_ip,
                                sock.inner,
                                context); 
                        
                            // println!("Client: RECEIVED PROBE!"); 
                            context
                                .scheduler
                                .schedule_event(Duration::from_nanos(10), Self::bw_probe_receive_thread, ())
                                .unwrap();                        
                        }
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

// Enhanced struct to manage the text buffer with timing information
#[derive(Clone)]
struct LostFramesMessage {
    text: String,
    timestamp: Instant,
    fade_duration: Duration, // Time after which the message starts fading
}
#[derive(Clone)]
pub struct LostFramesBuffer {
    messages: Vec<LostFramesMessage>,
    max_size: usize,
}

impl LostFramesBuffer {
    fn new(size: usize) -> Self {
        LostFramesBuffer {
            messages: Vec::with_capacity(size),
            max_size: size,
        }
    }


    fn add_message(&mut self, message: String) {
        if self.messages.len() >= self.max_size {
            // Remove the oldest message
            self.messages.remove(0);
        }
        
        // Add the new message with the current timestamp
        self.messages.push(LostFramesMessage {
            text: message,
            timestamp: Instant::now(),
            fade_duration: Duration::from_secs(5),
        });
    }
    

}

// Function to calculate alpha (opacity) based on message age
fn calculate_opacity(message: &LostFramesMessage) -> u32 {
    let now = Instant::now();
    let age = now.duration_since(message.timestamp);
    
    // If message is newer than fade_duration, full opacity
    if age <= message.fade_duration {
        return 255;
    }
    
    // Calculate fading over the next 2 seconds after fade_duration
    let fade_time = Duration::from_secs(40);
    let fade_age = age - message.fade_duration;
    
    if fade_age >= fade_time {
        // Message is completely faded out
        return 0;
    }
    
    // Linear fade from 255 to 0 over fade_time
    let fade_ratio = 1.0 - (fade_age.as_millis() as f64 / fade_time.as_millis() as f64);
    (fade_ratio * 255.0) as u32
}

// Apply alpha value to a color
fn apply_alpha(color: u32, alpha: u32) -> u32 {
    // Extract RGB components
    let r = (color >> 16) & 0xFF;
    let g = (color >> 8) & 0xFF;
    let b = color & 0xFF;
    
    // Apply alpha (simple linear blending with black background)
    let r = (r * alpha) / 255;
    let g = (g * alpha) / 255;
    let b = (b * alpha) / 255;
    
    // Recompose color
    (r << 16) | (g << 8) | b
}

// Modified render_text function to support alpha
fn render_text_with_alpha(
    buffer: &mut [u32],
    text: &str,
    x: usize,
    y: usize,
    stride: usize,
    color: u32,
    scale: usize,
    alpha: u32,
) {
    // Apply alpha to the color
    let alpha_color = apply_alpha(color, alpha);
    
    // Call the original render_text function with the modified color
    render_text(buffer, text, x, y, stride, alpha_color, scale);
}
/// Display exactly one decoded RGB frame (no sync/state info, just the index).
///
/// - `raw_frame`: the contiguous RGB byte buffer from `HevcDecoder`
/// - `server_ip`: used for title bar
/// - `frame_id`: index you want to show
/// - `window`: your pre-created `minifb::Window`
/// - `now`: timestamp, if you need it in the title (optional)
/// Display one decoded RGB frame, plus bitrate and lost-packets info.
/// Display exactly one decoded RGB frame (with sync/state info, bitrate, and lost-frames overlay)
pub fn display_single_frame_with_info(
    raw_frame: &[u8],
    server_ip: &IpAddr,
    frame_id: usize,
    window: &mut Window,
    now: TaiTime<0>,
    bitrate_mbps: f32,
    lost_frames: VecDeque<u32>,
    lost_frames_buffer: &mut LostFramesBuffer,
    test: &str,
    bm: &str, 
    string_id: &str, 
) -> bool {
    // 1) Convert raw RGB bytes → u32 pixel buffer
    let pixels = match convert_rgb_to_u32(raw_frame, WIDTH_ENCODER, HEIGHT_ENCODER) {
        Some(p) => p,
        None => {
            eprintln!("ERROR: Failed to convert RGB for frame #{}", frame_id);
            return false;
        }
    };

    // 2) Compute scaled dimensions
    let scaled_w = (WIDTH_ENCODER as f64 * SCALE_FACTOR_WINDOW) as usize;
    let scaled_h = (HEIGHT_ENCODER as f64 * SCALE_FACTOR_WINDOW) as usize;

    // 3) Allocate window buffer
    let mut buffer = vec![0u32; scaled_w * scaled_h];

    // 4) Nearest-neighbor resize
    for y in 0..scaled_h {
        for x in 0..scaled_w {
            let sx = x * WIDTH_ENCODER as usize / scaled_w;
            let sy = y * HEIGHT_ENCODER as usize / scaled_h;
            buffer[y * scaled_w + x] = pixels[sy * WIDTH_ENCODER as usize + sx];
        }
    }

    // 5) Draw text overlays (frame index and bitrate)
    let margin = 10;
    let line_h = 20;
    let line_hh = 40; 
    
    render_text(&mut buffer, 
        &format!("FRAME #{}", 
        frame_id), 
        margin,
        margin,
        scaled_w,
        0x00FF00,
        2);
    render_text(
        &mut buffer,
        &format!("Bitrate: {:.2} Mbps", bitrate_mbps),
        margin,
        margin + line_h,
        scaled_w,
        0x00FF00,
        2,
    );
    render_text(
        &mut buffer,
        &format!("{}", bm ),
        margin,
        margin + line_hh,
        scaled_w,
        0x00FF00,
        2,
    );
     



    // 6) Handle new lost-frames and add to buffer
    if !lost_frames.is_empty() {
        let msg = format!("T: {:5.5} LOST FRAMES: {:?}", format_elapsed!(now), lost_frames);
        lost_frames_buffer.add_message(msg);
    }

    // 7) Render the rolling lost-frames messages (up to 3) with fading
    for (i, entry) in lost_frames_buffer.messages.iter().rev().enumerate() {
        let opacity = calculate_opacity(entry);
        if opacity == 0 {
            continue;
        }
        // stack from bottom
        let y_pos = scaled_h as i32 - 80 + (i as i32 * 22);
        if y_pos > 0 {
            render_text_with_alpha(
                &mut buffer,
                &entry.text,
                margin,
                y_pos as usize,
                scaled_w,
                0xFF0000,
                2,
                opacity,
            );
        }
    }
    // 8) Update title with timestamp and test identifier
    let title = format!(
        "{} | {} -- Frame #{} @ {:.2}s - Test: {}",
        string_id, 
        server_ip,
        frame_id,
        now.duration_since(TaiTime::EPOCH).as_secs_f64(),
        test,
    );
    window.set_title(&title);

    // 9) Blit to screen
    match window.update_with_buffer(&buffer, scaled_w, scaled_h) {
        Ok(_) => true,
        Err(e) => {
            eprintln!("Window update failed for frame #{}: {}", frame_id, e);
            false
        }
    }
}


#[allow(unused)] //Looks useless, is mainly defined for type matching compatibility between XRServer/XRclient for StreamSocket
pub trait XRDevice {
    fn some_shared_method(&self);
}
#[allow(unused)] //Looks useless, is mainly defined for type matching compatibility between XRServer/XRclient for StreamSocket
impl XRDevice for XRClient {
    fn some_shared_method(&self) {
        println!("WOWWWWW");
    }
}
#[allow(unused)] //Looks useless, is mainly defined for type matching compatibility between XRServer/XRclient for StreamSocket
impl XRDevice for XRServer {
    fn some_shared_method(&self) {
        println!("WOWZA!!");
    }
}
#[derive(Clone)]
pub struct TimedFrame {
    vec: Vec<MpduPacket>,
    timestamp: TaiTime<0>,  // this timestamp corresponds to the receive instant of an A-MPDU by any STA
}

#[allow(non_camel_case_types)]
#[allow(unused)]
// #[derive(Clone)]
pub struct STA_extended {
    // extended class to PoissonGen
    pub output_network_port: Output<MpduPacket>,

    pub outport_coords_xrclient: Output<Coords>, // only used so the XRClient can know its coordinates in real time, will be input in Tracking packets! 

    pub to_app_socket: Output<TimedFrame>,
    // pub to_app_socket_end_ampdu: Output<bool>,
    pub sta_id: i32,
    pub destination_id: i32,

    pub arrival_rate_BG: f64,
    pub mean_length_packets_BG: f64,
    pub num_packets_sent: usize,
    pub received_packet_counter: usize,

    pub sta_coordinates: Coords,
    pub orig_sta_coordinates: Coords, 
    pub does_sta_tx: bool,

    pub is_bg_sta: bool,

    pub t_0: TaiTime<0>,
}
#[allow(unused)]
impl STA_extended {
    pub fn new(
        // arrival_rate_bps: f64,
        mean_length_BG: f64,
        src: i32,
        dest: i32,
        coordinates: Coords,
        does_sta_transmit: bool,
        t0_sim: TaiTime<0>,
        is_bg_sta: bool,
        arrival_rate_BG: f64,
    ) -> Self {
        let arrival_rate_BG_packets = arrival_rate_BG / mean_length_BG;

        println!("\n*************************************************");
        println!("[DEBUG STA{}]\tCoordinates: {:?}\n\tDestination: STA{} | L_BG: {:.3}, RATE_BG: {:.3} Mbps, is_BG_STA {}",
                            src, coordinates, dest, mean_length_BG,   arrival_rate_BG, is_bg_sta);

        Self {
            output_network_port: Default::default(),
            outport_coords_xrclient: Default::default(),  
            to_app_socket: Default::default(),
            // to_app_socket_end_ampdu: Default::default(),
            sta_id: src,
            destination_id: dest,
            arrival_rate_BG: arrival_rate_BG_packets,
            mean_length_packets_BG: mean_length_BG,
            num_packets_sent: 0,
            sta_coordinates: coordinates,
            orig_sta_coordinates: coordinates, 
            received_packet_counter: 0,
            does_sta_tx: does_sta_transmit,
            t_0: t0_sim,
            is_bg_sta,
        }
    }


    // To simulate the channel changes, simulate the HMD moving at a
    // constant speed of 5 m/s according to a random direction model within 1m² around
    // initial position. In this way, we approximate the channel changes caused
    // by a VR gamer standing still but rapidly moving around. src: How to model cloud VR, khorov et al. 
 pub fn move_coordinates_everest<'a>(&'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move{

            const LIMIT_MOVEMENT_RADIUS :f64 = 1.0; // circle of 1m radius. 
            let delta_t = 0.01; //  is reasonable? 

            // Step length = speed * delta_t
            let step = 5.0 * delta_t;

            {              
                let mut rng = rand::thread_rng(); // rng needs to be scoped ( {...} ) so that future is Send or sth. 

                // Pick a random direction in 2D plane (azimuth only)
                let theta = rng.gen_range(0.0..2.0 * PI);
                let dx = step * theta.cos();
                let dy = step * theta.sin();

                // println!("[MOVE COORDS] Before: {:?}", self.sta_coordinates);

                // New candidate position
                let new_x = self.sta_coordinates.x + dx;
                let new_y = self.sta_coordinates.y + dy;

                // Boundaries: within ±0.5 m around initial position
                let min_x = self.orig_sta_coordinates.x - LIMIT_MOVEMENT_RADIUS;
                let max_x = self.orig_sta_coordinates.x + LIMIT_MOVEMENT_RADIUS;
                let min_y = self.orig_sta_coordinates.y - LIMIT_MOVEMENT_RADIUS;
                let max_y = self.orig_sta_coordinates.y + LIMIT_MOVEMENT_RADIUS;

                // Reflect if out of bounds
                self.sta_coordinates.x = if new_x < min_x {
                    min_x + (min_x - new_x) // reflect back
                } else if new_x > max_x {
                    max_x - (new_x - max_x)
                } else {
                    new_x
                };

                self.sta_coordinates.y = if new_y < min_y {
                    min_y + (min_y - new_y)
                } else if new_y > max_y {
                    max_y - (new_y - max_y)
                } else {
                    new_y
                };
            }
            // z stays constant (HMD height)
            // println!("Coordinates After dt: {:?}", self.sta_coordinates);
            
            self.outport_coords_xrclient.send( self.sta_coordinates.clone()).await; 

            context
                .scheduler
                .schedule_event(Duration::from_secs_f64(delta_t), Self::move_coordinates_everest, () , )
                .unwrap();
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

        packet.length_packet = (packet.header_alvr.packet_length + 100) as usize;
        // println!("Length packet XR {}", packet.length_packet);

        packet.packet_id = self.num_packets_sent;

        packet.sta_src_id = self.sta_id;
        packet.sta_dest_id = self.destination_id;

        packet.sta_src_coords = self.sta_coordinates;

        // println!("STA IN: packet.src_id = {}, packet.sta_dest_id = {}\n Coords src: {:?}", packet.sta_src_id, packet.sta_dest_id, packet.sta_src_coords);
        context
            .scheduler
            .schedule_event(Duration::from_nanos(10), Self::send_packet_wireless, packet)
            .unwrap();
        // self.output_network_port.send(packet).await;
        self.num_packets_sent += 1;
    }
    pub async fn send_packet_wireless(&mut self, packet: MpduPacket) {
        self.output_network_port.send(packet).await;
    }

    pub async fn input_wireless(&mut self, ampdu_packet: AmpduPacket, context: &Context<Self>) {
        let mut packet_batch = Vec::new(); // Create a batch to hold packets
        let now: TaiTime<0> = context.scheduler.time();
        // println!("INPUT WIRELESS: STA{} received AMPDU from STA{}, dest: {}", self.sta_id, ampdu_packet.sta_src_id, ampdu_packet.sta_dest_id);
        if ampdu_packet.sta_dest_id == self.sta_id {
            // make sure we ignore packets not corresponding to STA
            for packet in ampdu_packet.mpdu_packets {
                // iterate through whole AMPDU

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
            let frame = TimedFrame {
                vec: packet_batch,
                timestamp: now,
            };
            context
                .scheduler
                .schedule_event(Duration::from_nanos(10), Self::to_app_socket_send, frame)
                .unwrap();
            // Send the batch to the app socket in one go
            // self.to_app_socket.send(packet_batch).await;
        }
        yield_now();
    }
    pub async fn to_app_socket_send(&mut self, frame: TimedFrame) {
        self.to_app_socket.send(frame).await;
    }

    pub fn send_packet_BG<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let mut rng = StdRng::seed_from_u64(42);

            if self.does_sta_tx && self.is_bg_sta {
                // if STA is "TX type"         (and not "RX only")

                let mut packet = MpduPacket::new();


                let mut time_interarrival =
                    Duration::from_secs_f64(exponential(1.0 / self.arrival_rate_BG, &mut rng));

                time_interarrival = max(time_interarrival, Duration::from_nanos(1));

                // let len_random = exponential(self.mean_length_packets_BG as f64) as usize;
                let len_random = self.mean_length_packets_BG as usize;

                packet.length_packet = cmp::max(1, len_random);
                packet.packet_id = self.num_packets_sent;

                packet.sta_src_id = self.sta_id;
                packet.sta_dest_id = self.destination_id;

                packet.sta_src_coords = self.sta_coordinates;

                debug_print!(
                    DebugColor::Blue,
                    "{} [TGAPP{}] Packet {} generated, destination STA {}, self.coords = {:?}",
                    format_elapsed!(context.scheduler.time()),
                    self.sta_id,
                    packet.packet_id,
                    packet.sta_dest_id,
                    self.sta_coordinates,
                );

                // self.output_network_port.send(packet).await;
                context
                    .scheduler
                    .schedule_event(Duration::from_nanos(10), Self::send_packet_wireless, packet)
                    .unwrap();

                self.num_packets_sent += 1;

                context // reschedule this function
                    .scheduler
                    .schedule_event(time_interarrival, Self::send_packet_BG, ())
                    .unwrap();
            }
        }
    }
}

impl Model for STA_extended {}

pub fn extract_br_value(input: &str) -> Option<f32> {
    let re = Regex::new(r"Br(\d+\.\d+)").unwrap(); // Regex to match "Br" followed by a float.

    if let Some(captures) = re.captures(input) {
        captures.get(1).map(|m| m.as_str().parse::<f32>().unwrap())
    } else {
        None
    }
}

pub fn upper_bound_bitrate(bitrate_bps: f32, bitrate_ladder: &Vec<f32>) -> f32 {
                    // Perform binary search to find the largest value less than or equal to `bitrate_bps`
                    match bitrate_ladder
                        .binary_search_by(|x| x.partial_cmp(&bitrate_bps).unwrap_or(std::cmp::Ordering::Less))
                    {
                        Ok(index) => bitrate_ladder[index], // Exact match found
                        Err(index) => {
                            // If not found, `index` is where the value would be inserted to maintain sorted order
                            if index == 0 {
                                // If `bitrate_bps` is smaller than the first element, return the first element
                                bitrate_ladder.first().copied().unwrap_or(bitrate_bps)
                            } else {
                                // Otherwise, return the element just before the insertion point (i.e., the largest <= bitrate_bps)
                                bitrate_ladder[index - 1]
                            }
                        }
                    }
                }
 
pub fn minmax_bitrate(
    bitrate_bps: f32,
    max_bitrate_bps: f32,
    min_bitrate_bps: f32,
) -> f32 {
    let mut bitrate = bitrate_bps;
    bitrate = f32::min(bitrate, max_bitrate_bps);
    bitrate = f32::max(bitrate, min_bitrate_bps);


    // println!("minmax: bitrate_mbps_orig: {}, final {}", bitrate_bps/1e6, bitrate/1e6); 


    bitrate
}