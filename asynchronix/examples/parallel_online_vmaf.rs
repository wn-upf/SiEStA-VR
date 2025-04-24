

pub const INTRAREFRESH_ENABLED: bool = false; 

pub const FRAMERATE_WINDOWS: usize = 60; 
pub const INITIAL_FRAMERATE_FPS: f32 = 90.0; 
pub const IDR_FRAME_SIZE_GOP: usize = 30; 

pub const WIDTH_ENCODER: usize = 1920; 
pub const HEIGHT_ENCODER: usize = 1080; 
use tokio::sync::Semaphore;

// static METRIC_SLOTS: Lazy<Semaphore> = Lazy::new(|| Semaphore::const_new(30)); // ≤4 frames in flight
use asynchronix::model::Context;
use crossbeam::channel::{bounded, unbounded, Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::collections::HashMap;
use std::io::{Read, Write};
#[allow(unused_imports)]
#[allow(dead_code)]
use std::process::{Child, Command, Stdio};
use colored::Colorize;
use std::sync::{Arc, Mutex};
use ffmpeg_sidecar::command::FfmpegCommand;
use rand::seq::IteratorRandom;
use std::io::BufReader;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use anyhow::Result;
use async_std::task;
use minifb::{Key, Scale, Window, WindowOptions};
use rand::Rng;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::{ VecDeque};
use std::error::Error;
use std::fs::File;
use std::hash::Hash;
use std::io::{ BufWriter};
use std::process::{ChildStdin, ChildStdout};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use std::cell::RefCell;
use std::fmt::{self, Debug};
use std::{
    collections::{HashSet,},
    io,
    marker::PhantomData,
    mem,
    // net::{TcpListener, UdpSocket},
}; 

macro_rules! print_prettyy {
    ($color:expr, $fmt:expr, $($arg:tt)*) => {
        // if true {
            let msg = format!($fmt, $($arg)*);
            println!("{}", $color.to_background_fn()(msg));
        // }
    };
}

use std::net::IpAddr;
// use tokio::sync::Semaphore;
use std::result::Result::Ok;
use tai_time::TaiTime;
use std::{
    fs::{ OpenOptions},
    path::PathBuf,
};
use csv::Writer;
use rand::distributions::Uniform;
use rand::rngs::StdRng;
use rand::SeedableRng;
use image::{ImageBuffer, Rgb};
use image_compare::rgb_hybrid_compare;
use regex::Regex;
use std::net::Ipv4Addr;
use std::thread_local;
use std::path::Path;
use core::f64;
use glam::{Quat, Vec3};
use once_cell::sync::Lazy;
use std::time::SystemTime;
use std::{ vec};
use dashmap::DashMap;
use asynchronix::model::{ Model};
use asynchronix::ports::Output;
use std::cmp::{self, max};
use std::f64::consts::PI;
use std::future::Future;
use std::sync::RwLock;

use lazy_static::lazy_static;

#[derive(Debug, Clone)]
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


#[macro_export]
macro_rules! print_greennn {
    ($fmt:expr, $($arg:tt)*) => {
        let msg = format!($fmt, $($arg)*);
        println!("{}", DebugColor::ForestGreen.to_background_fn()(msg));
    };
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
        // create it (and any missing parents) if it doesn't exist
        let dir =format!("Results/{}", name_folder); 
        std::fs::create_dir_all(&dir)?;
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
    pub fn new_for_trace(
        scenario: &str,
        trace_idx: usize,
    ) -> Result<Self> {
        let dir = format!("Results/{}", scenario);
        std::fs::create_dir_all(&dir)?;
        let path = format!("{}/VMAF_metrics_{}.csv", dir, trace_idx);
        let file = std::fs::File::create(path)?;
        Ok(Self {
            writer: Arc::new(std::sync::Mutex::new(csv::Writer::from_writer(file))),
            name_folder: scenario.to_string(),
        })
    }
   

    pub async fn process_frame_buffers(
        &self,
        frame_number: u64,
        timestamp_ms: f64,
        ref_buf: &[u8],
        lossy_buf: &[u8],
        ip_client: IpAddr,
    ) -> Result<()> {

        // 1) create a new tempdir for *this* frame
        let temp_dir = TempDir::new()?;
        let ref_path = temp_dir.path().join("ref.rgb");
        let lossy_path = temp_dir.path().join("lossy.rgb");

        // 2) dump the in-memory RGB into raw files
        std::fs::write(&ref_path, ref_buf)?;
        std::fs::write(&lossy_path, lossy_buf)?;
        println!("Processing!"); 
        // 3) call your existing pipeline
        self.process_frame_metrics(
            frame_number,
            timestamp_ms,
            ref_path.to_str().unwrap(),
            lossy_path.to_str().unwrap(),
            ip_client,
        );
        Ok(())
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
        let ref_status = Command::new("ffmpeg")
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
        let lossy_status = Command::new("ffmpeg")
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
        let metrics_status = Command::new("ffmpeg")
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
        print_greennn!( 
            // DebugColor::Green, 
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

use std::time::Instant; 

#[derive(Clone)]
pub struct NalUnit {
    pub nal_type: u8,
    pub data: Vec<u8>,
    pub is_keyframe: bool,
}

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

#[derive(Debug, Clone)]
struct FrameInfo {
    id:       u32,
    lost:     bool,
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
    id_queue: VecDeque<u32>,
}
#[allow(non_camel_case_types, unused)]
impl HevcDecoder {
    pub fn new(framerate: u32, width: u32, height: u32, decoder_str: &str) -> Self {
        let frame_size = (width as usize) * (height as usize) * 3;

        let decoder_string = decoder_str.to_string();

        let mut child = FfmpegCommand::new()
            .hwaccel("cuda")
            .args(&["-f", "hevc", "-i", "-"])
            .args(&["-vf", &format!("fps={}", framerate)])
            .args(&["-pix_fmt", "rgb24"])
            .args(&["-tune", "zerolatency"])
            // .args(&["-preset", "ultrafast"])
            // .args(&["-vsync", "passthrough"])
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
            id_queue: VecDeque::new(), 
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

    pub fn process_packet(&mut self, packet: Vec<u8>, id: u32) {
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
        self.id_queue.push_back(id);

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

    pub fn next_decoded_frame(&mut self) -> Option<(Vec<u8>, u32, Instant)> {
        let ts = Instant::now();
        let _ = self.process_decoded_frames();

        if let Some(frame) = self.decoded_frames.pop_front() {
            let id = self.id_queue
                .pop_front()
                .expect("decoder out‐of‐sync: id_queue empty");
            Some((frame, id, ts))
        } else {
            None
        }
    }
}

pub struct HevcParser {
    buffer: Vec<u8>,
    // Store the most recent parameter sets
    vps: Option<Vec<u8>>,
    sps: Option<Vec<u8>>,
    pps: Option<Vec<u8>>,
}
#[allow(dead_code)]
impl HevcParser {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            vps: None,
            sps: None,
            pps: None,
        }
    }

    /// Add more encoded data to the parser buffer
    pub fn add_data(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Find the next NAL unit start code in the buffer
    fn find_next_start_code(&self, start_pos: usize) -> Option<usize> {
        if !self.buffer.is_empty() {
            for i in start_pos..self.buffer.len() - 3 {
                // Look for 0x000001 or 0x00000001 (3 or 4 byte start codes)
                if (self.buffer[i] == 0 && self.buffer[i + 1] == 0 && self.buffer[i + 2] == 1)
                    || (i < self.buffer.len() - 4
                        && self.buffer[i] == 0
                        && self.buffer[i + 1] == 0
                        && self.buffer[i + 2] == 0
                        && self.buffer[i + 3] == 1)
                {
                    return Some(i);
                }
            }
            None
        } else {
            // println!("PARSER BUFFER EMPTTTTTTTTTTTY");
            None
        }
    }

    // pub fn clear(&mut self) {
    //     println!("Clearing parser buffer: {} bytes", self.buffer.len());
    //     self.buffer.clear();
    // }

    /// Extract the next complete NAL unit from the buffer
    pub fn next_nal_unit(&mut self) -> Option<NalUnit> {
        // Find the first start code
        let start_pos = self.find_next_start_code(0)?;

        // Determine start code length (3 or 4 bytes)
        let start_code_len = if start_pos + 3 < self.buffer.len()
            && self.buffer[start_pos + 2] == 0
            && self.buffer[start_pos + 3] == 1
        {
            4
        } else {
            3
        };

        // Find the next start code
        let next_start = self.find_next_start_code(start_pos + start_code_len);

        let (nal_end, has_next) = match next_start {
            Some(pos) => (pos, true),
            None => (self.buffer.len(), false),
        };

        // If we don't have a complete NAL unit yet, wait for more data
        if !has_next {
            return None;
        }

        // Extract NAL header and determine NAL type
        let nal_header_pos = start_pos + start_code_len;
        if nal_header_pos >= self.buffer.len() {
            return None;
        }

        let nal_header = self.buffer[nal_header_pos];
        let nal_type = (nal_header >> 1) & 0x3F; // Extract bits 1-6 (NAL type)

        // Extract the complete NAL unit data (including header)
        let nal_data = self.buffer[nal_header_pos..nal_end].to_vec();

        // Store parameter sets based on NAL type
        match nal_type {
            32 => {
                // VPS
                let full_nal = self.create_full_nal(&self.buffer[start_pos..nal_end]);
                self.vps = Some(full_nal);
            }
            33 => {
                // SPS
                let full_nal = self.create_full_nal(&self.buffer[start_pos..nal_end]);
                self.sps = Some(full_nal);
            }
            34 => {
                // PPS
                let full_nal = self.create_full_nal(&self.buffer[start_pos..nal_end]);
                self.pps = Some(full_nal);
            }
            _ => {}
        }

        // Remove the processed NAL unit from the buffer
        self.buffer.drain(0..nal_end);

        // Determine if this is a keyframe (I-frame)
        // In HEVC, NAL types 16-21 represent IRAP (Intra Random Access Point) pictures
        let is_keyframe = (16..=21).contains(&nal_type);

        Some(NalUnit {
            nal_type,
            data: nal_data,
            is_keyframe,
        })
    }

    // Helper to create a full NAL unit with start code
    fn create_full_nal(&self, data: &[u8]) -> Vec<u8> {
        let mut nal = Vec::with_capacity(data.len());
        nal.extend_from_slice(data);
        nal
    }

    /// Get all complete frames currently in the buffer
    pub fn get_frames(&mut self) -> Vec<Vec<u8>> {
        let mut frames = Vec::new();
        let mut current_frame = Vec::new();
        let mut saw_vcl = false;

        while let Some(nal) = self.next_nal_unit() {
            // VCL NAL units (0-31) contain the actual picture data
            let is_vcl = nal.nal_type <= 31;

            // If we see a VCL NAL and already saw one before, it's a new frame
            if is_vcl && saw_vcl {
                if !current_frame.is_empty() {
                    frames.push(current_frame);
                    current_frame = Vec::new();
                }
                saw_vcl = false;
            }

            if is_vcl {
                saw_vcl = true;
            }

            // Add start code and NAL data to current frame
            current_frame.extend_from_slice(&[0, 0, 0, 1]);
            current_frame.extend_from_slice(&nal.data);
        }

        // Add the last frame if it's not empty
        if !current_frame.is_empty() {
            frames.push(current_frame);
        }

        frames
    }

    // New methods to access parameter sets

    /// Get the current VPS (Video Parameter Set)
    pub fn get_vps(&self) -> Option<&Vec<u8>> {
        self.vps.as_ref()
    }

    /// Get the current SPS (Sequence Parameter Set)
    pub fn get_sps(&self) -> Option<&Vec<u8>> {
        self.sps.as_ref()
    }

    /// Get the current PPS (Picture Parameter Set)
    pub fn get_pps(&self) -> Option<&Vec<u8>> {
        self.pps.as_ref()
    }

    pub fn update_vps(&mut self, vps: &Vec<u8>) {
        self.vps = Some(vps.clone());
    }
    pub fn update_sps(&mut self, sps: &Vec<u8>) {
        self.sps = Some(sps.clone());
    }
    pub fn update_pps(&mut self, pps: &Vec<u8>) {
        self.pps = Some(pps.clone());
    }

    /// Get all parameter sets as a tuple
    pub fn get_parameter_sets(&self) -> (Option<&Vec<u8>>, Option<&Vec<u8>>, Option<&Vec<u8>>) {
        (self.vps.as_ref(), self.sps.as_ref(), self.pps.as_ref())
    }

    /// Print parameter sets as hex strings
    pub fn print_parameter_sets(&self) {
        if let Some(vps) = &self.vps {
            println!("VPS ({}): {}", vps.len(), self.format_hex(vps));
        } else {
            println!("VPS: Not found");
        }

        if let Some(sps) = &self.sps {
            println!("SPS ({}): {}", sps.len(), self.format_hex(sps));
        } else {
            println!("SPS: Not found");
        }

        if let Some(pps) = &self.pps {
            println!("PPS ({}): {}", pps.len(), self.format_hex(pps));
        } else {
            println!("PPS: Not found");
        }
    }

    /// Format bytes as hex string with limited length
    fn format_hex(&self, data: &[u8]) -> String {
        let max_display = 48; // Show at most 48 bytes
        let mut result = String::new();

        for (i, byte) in data.iter().enumerate() {
            if i >= max_display {
                result.push_str("...");
                break;
            }
            result.push_str(&format!("{:02x}", byte));
            if i % 4 == 3 && i + 1 < std::cmp::min(data.len(), max_display) {
                result.push(' ');
            }
        }

        result
    }
}

// use super::alvr_packets::NetworkStatisticsPacket;

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
        let bitrate_adjusted_fps = bitrate_mbps * FRAMERATE_WINDOWS as f32 / INITIAL_FRAMERATE_FPS;
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

use csv::StringRecord;

fn render_text(
    buffer: &mut [u32],
    text: &str,
    x: usize,
    y: usize,
    stride: usize,
    color: u32,
    scale: usize,
) {
    // Simple 5x7 pixel font (common for basic bitmap fonts)
    // Each character is represented as an array of 7 bytes, where each byte represents a row
    // and the bits in each byte represent the pixels in that row
    const FONT_WIDTH: usize = 5;
    const FONT_HEIGHT: usize = 7;
    const CHAR_SPACING: usize = 1;

    // Apply scaling
    let scaled_font_width = FONT_WIDTH * scale;
    let scaled_char_spacing = CHAR_SPACING * scale;

    // Define a simple bitmap font (only uppercase letters and some basic characters)
    // Each character is 5x7 pixels
    let font = [
        // Space
        [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        // !
        [0x04, 0x04, 0x04, 0x04, 0x00, 0x04, 0x00],
        // "
        [0x0A, 0x0A, 0x00, 0x00, 0x00, 0x00, 0x00],
        // #
        [0x0A, 0x0A, 0x1F, 0x0A, 0x1F, 0x0A, 0x0A],
        // $
        [0x04, 0x0F, 0x14, 0x0E, 0x05, 0x1E, 0x04],
        // %
        [0x18, 0x19, 0x02, 0x04, 0x08, 0x13, 0x03],
        // &
        [0x0C, 0x12, 0x14, 0x08, 0x15, 0x12, 0x0D],
        // '
        [0x0C, 0x04, 0x08, 0x00, 0x00, 0x00, 0x00],
        // (
        [0x02, 0x04, 0x08, 0x08, 0x08, 0x04, 0x02],
        // )
        [0x08, 0x04, 0x02, 0x02, 0x02, 0x04, 0x08],
        // *
        [0x00, 0x04, 0x15, 0x0E, 0x15, 0x04, 0x00],
        // +
        [0x00, 0x04, 0x04, 0x1F, 0x04, 0x04, 0x00],
        // ,
        [0x00, 0x00, 0x00, 0x00, 0x0C, 0x04, 0x08],
        // -
        [0x00, 0x00, 0x00, 0x1F, 0x00, 0x00, 0x00],
        // .
        [0x00, 0x00, 0x00, 0x00, 0x00, 0x0C, 0x0C],
        // /
        [0x00, 0x01, 0x02, 0x04, 0x08, 0x10, 0x00],
        // 0
        [0x0E, 0x11, 0x13, 0x15, 0x19, 0x11, 0x0E],
        // 1
        [0x04, 0x0C, 0x04, 0x04, 0x04, 0x04, 0x0E],
        // 2
        [0x0E, 0x11, 0x01, 0x02, 0x04, 0x08, 0x1F],
        // 3
        [0x1F, 0x02, 0x04, 0x02, 0x01, 0x11, 0x0E],
        // 4
        [0x02, 0x06, 0x0A, 0x12, 0x1F, 0x02, 0x02],
        // 5
        [0x1F, 0x10, 0x1E, 0x01, 0x01, 0x11, 0x0E],
        // 6
        [0x06, 0x08, 0x10, 0x1E, 0x11, 0x11, 0x0E],
        // 7
        [0x1F, 0x01, 0x02, 0x04, 0x08, 0x08, 0x08],
        // 8
        [0x0E, 0x11, 0x11, 0x0E, 0x11, 0x11, 0x0E],
        // 9
        [0x0E, 0x11, 0x11, 0x0F, 0x01, 0x02, 0x0C],
        // :
        [0x00, 0x0C, 0x0C, 0x00, 0x0C, 0x0C, 0x00],
        // ;
        [0x00, 0x0C, 0x0C, 0x00, 0x0C, 0x04, 0x08],
        // <
        [0x02, 0x04, 0x08, 0x10, 0x08, 0x04, 0x02],
        // =
        [0x00, 0x00, 0x1F, 0x00, 0x1F, 0x00, 0x00],
        // >
        [0x08, 0x04, 0x02, 0x01, 0x02, 0x04, 0x08],
        // ?
        [0x0E, 0x11, 0x01, 0x02, 0x04, 0x00, 0x04],
        // @
        [0x0E, 0x11, 0x01, 0x0D, 0x15, 0x15, 0x0E],
        // A
        [0x0E, 0x11, 0x11, 0x11, 0x1F, 0x11, 0x11],
        // B
        [0x1E, 0x11, 0x11, 0x1E, 0x11, 0x11, 0x1E],
        // C
        [0x0E, 0x11, 0x10, 0x10, 0x10, 0x11, 0x0E],
        // D
        [0x1C, 0x12, 0x11, 0x11, 0x11, 0x12, 0x1C],
        // E
        [0x1F, 0x10, 0x10, 0x1E, 0x10, 0x10, 0x1F],
        // F
        [0x1F, 0x10, 0x10, 0x1E, 0x10, 0x10, 0x10],
        // G
        [0x0E, 0x11, 0x10, 0x17, 0x11, 0x11, 0x0F],
        // H
        [0x11, 0x11, 0x11, 0x1F, 0x11, 0x11, 0x11],
        // I
        [0x0E, 0x04, 0x04, 0x04, 0x04, 0x04, 0x0E],
        // J
        [0x07, 0x02, 0x02, 0x02, 0x02, 0x12, 0x0C],
        // K
        [0x11, 0x12, 0x14, 0x18, 0x14, 0x12, 0x11],
        // L
        [0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x1F],
        // M
        [0x11, 0x1B, 0x15, 0x15, 0x11, 0x11, 0x11],
        // N
        [0x11, 0x11, 0x19, 0x15, 0x13, 0x11, 0x11],
        // O
        [0x0E, 0x11, 0x11, 0x11, 0x11, 0x11, 0x0E],
        // P
        [0x1E, 0x11, 0x11, 0x1E, 0x10, 0x10, 0x10],
        // Q
        [0x0E, 0x11, 0x11, 0x11, 0x15, 0x12, 0x0D],
        // R
        [0x1E, 0x11, 0x11, 0x1E, 0x14, 0x12, 0x11],
        // S
        [0x0F, 0x10, 0x10, 0x0E, 0x01, 0x01, 0x1E],
        // T
        [0x1F, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04],
        // U
        [0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x0E],
        // V
        [0x11, 0x11, 0x11, 0x11, 0x11, 0x0A, 0x04],
        // W
        [0x11, 0x11, 0x11, 0x15, 0x15, 0x15, 0x0A],
        // X
        [0x11, 0x11, 0x0A, 0x04, 0x0A, 0x11, 0x11],
        // Y
        [0x11, 0x11, 0x11, 0x0A, 0x04, 0x04, 0x04],
        // Z
        [0x1F, 0x01, 0x02, 0x04, 0x08, 0x10, 0x1F],
        // [
        [0x0E, 0x08, 0x08, 0x08, 0x08, 0x08, 0x0E],
        // \
        [0x00, 0x10, 0x08, 0x04, 0x02, 0x01, 0x00],
        // ]
        [0x0E, 0x02, 0x02, 0x02, 0x02, 0x02, 0x0E],
        // ^
        [0x04, 0x0A, 0x11, 0x00, 0x00, 0x00, 0x00],
        // _
        [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1F],
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
            'a'..='z' => (c as usize) - ('a' as usize) + 33, // Map lowercase to uppercase
            '[' => 59,
            '\\' => 60,
            ']' => 61,
            '^' => 62,
            '_' => 63,
            _ => 0, // Default to space for unknown characters
        };

        // Draw the character with scaling
        for row in 0..FONT_HEIGHT {
            for scaled_row in 0..scale {
                let buffer_y = y + (row * scale) + scaled_row;

                for col in 0..FONT_WIDTH {
                    // Check if the current pixel is set in the font bitmap
                    if (font[index][row] & (1 << (FONT_WIDTH - 1 - col))) != 0 {
                        for scaled_col in 0..scale {
                            let buffer_x = char_x + (col * scale) + scaled_col;

                            // Calculate buffer index and check bounds
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

        // Move to the next character position
        char_x += scaled_font_width + scaled_char_spacing;
    }
}


fn make_encoder_task(
    tag: usize,
    bitrate_mbps: f32,
    trace: Arc<Vec<FrameInfo>>,
    tx: Sender<(usize, u32, Vec<u8>)>,
    video_path: String,
    offset_video: f64,
    simulate_loss: bool,
) {
    task::spawn(async move {
        // 1️⃣ Create your encoder
        let mut enc = ChunkedHevcEncoder::new(
            &video_path,
            1920,
            1080,
            &format!("{bitrate_mbps}M"),
            1.0,
            format!("ENC{}M", bitrate_mbps),
            offset_video,
        );

        // 2️⃣ Iterate until we’ve produced every ID in the trace
        let mut produced = 0;
        while produced < trace.len() {
            // (re)fill the encoder’s internal queue
            enc.start_chunking(bitrate_mbps).await;
            std::thread::sleep(Duration::from_millis(10000)); 
            // drain all frames this chunk produced (but never overrun our trace)
            while produced < trace.len() {
                // try to grab the next packet
                if let Some(pkt) = enc.next_frame().await {
                    let info = &trace[produced];
                    produced += 1;

                    std::thread::sleep(Duration::from_millis(600)); 


                    // simulate loss only on the “low” path
                    if !simulate_loss || !info.lost {
                        if tx.send((tag, info.id, pkt)).is_err() {
                            // receiver hung up → terminate task
                            return;
                        }
                    }
                } else {
                    // no more frames in this chunk → go back to start_chunking()
                    break;
                }
            }
        }
        // when this async block returns, `enc` is dropped and ffmpeg subprocesses exit
    });
}
/* ---------------------------  MAIN  ---------------------------------- */
/* convert “RGBRGB…” byte slice to Vec<u32> suitable for minifb */
fn rgb_to_u32(src: &[u8]) -> Vec<u32> {
    src.chunks_exact(3)
       .map(|px| ((px[0] as u32) << 16) | ((px[1] as u32) << 8) | px[2] as u32)
       .collect()
}

/* nearest-neighbour down-scale to (w_out,h_out) */
fn resize_nn(buf: &[u32], w_in: usize, h_in: usize,
             w_out: usize, h_out: usize) -> Vec<u32>
{
    let mut out = vec![0u32; w_out * h_out];
    for y in 0..h_out {
        let src_y = y * h_in / h_out;
        for x in 0..w_out {
            let src_x = x * w_in / w_out;
            out[y*w_out + x] = buf[src_y*w_in + src_x];
        }
    }
    out
}


/* similarity in [0.0,1.0]  (1.0 ⇒ perfect match) */
fn similarity_rgb_hybrid(a: &[u8], b: &[u8], w: usize, h: usize) -> f64 {
    let img1 = ImageBuffer::<Rgb<u8>, _>::from_raw(w as u32, h as u32, a.to_vec()).unwrap();
    let img2 = ImageBuffer::<Rgb<u8>, _>::from_raw(w as u32, h as u32, b.to_vec()).unwrap();
    1.0 - rgb_hybrid_compare(&img1, &img2).unwrap().score
}
type Packet = (usize, u32, Vec<u8>);

fn make_low_task(
    trace: Arc<Vec<FrameInfo>>,
    mut rx_raw: Receiver<Vec<u8>>,
    tx: Sender<Packet>,
) {
    task::spawn(async move {
        for row in trace.iter() {
            // pull one packet for this CSV row
            let pkt = loop { if let Ok(p) = rx_raw.try_recv() { break p } };
            if !row.lost {
                tx.send((0, row.id, pkt)).ok();      // only good frames go out
            }
        }
    });
}
fn make_ref_task(
    trace: Arc<Vec<FrameInfo>>,
    mut rx_raw: Receiver<Vec<u8>>,
    tx: Sender<Packet>,
) {
    task::spawn(async move {
        for row in trace.iter() {
            let pkt = loop { if let Ok(p) = rx_raw.try_recv() { break p } };
            tx.send((1, row.id, pkt)).ok();          // never skipped
        }
    });
}


/* draw the two half-frames *plus* the ID text */
fn draw_pair(
    window:  &mut Window,
    rgb_l:   &[u8],
    rgb_r:   &[u8],
    id:      u32,
    sim:     f64,
) -> Result<()> {
    const W: usize = 1920;
    const H: usize = 1080;
    const SCALE: f64 = 0.28;
    let sw = (W as f64 * SCALE) as usize;
    let sh = (H as f64 * SCALE) as usize;
    let ww = sw * 2 + 10;

    let left  = resize_nn(&rgb_to_u32(rgb_l),  W, H, sw, sh);
    let right = resize_nn(&rgb_to_u32(rgb_r),  W, H, sw, sh);

    let mut buf = vec![0u32; ww * sh];
    for y in 0..sh {
        let dst = y * ww;
        buf[dst..dst+sw].copy_from_slice(&left[y*sw..(y+1)*sw]);
        buf[dst+sw+10..dst+sw+10+sw].copy_from_slice(&right[y*sw..(y+1)*sw]);
    }

    let y_lbl = sh - 40;
    render_text(&mut buf, &format!("#{}", id), 10,           y_lbl, ww, 0xFFAA00, 2);
    render_text(&mut buf, &format!("#{}", id), sw + 20,      y_lbl, ww, 0xFFAA00, 2);

    window.set_title(&format!("ID {}  –  sim {:.1} %", id, sim*100.0));
    window.update_with_buffer(&buf, ww, sh)?;
    Ok(())
}


/// Run your entire “main async block” on one trace CSV
pub async fn process_trace(
    trace_csv: PathBuf,
    ip: IpAddr,
) -> Result<()> {
    // 1) extract the “scenario” folder name and the trace index
    let file_name = trace_csv.file_name().unwrap().to_string_lossy();
    let caps = Regex::new(r"trace_offline_video(\d+)\.csv$")?
        .captures(&file_name)
        .expect("filename didn’t match");
    let trace_idx: usize = caps[1].parse()?;

    let scenario = trace_csv.parent()
        .and_then(|p| p.file_name())
        .unwrap()
        .to_string_lossy();

    // 2) build a logger that writes to Results/<scenario>/VMAF_metrics_<idx>.csv
    let metric = MetricsLogger::new_for_trace(&scenario, trace_idx)?;

    // 3) copy in your CSV‐parsing + FFmpeg + VMAF loop here, but
    //    • replace the hard-coded `trace_path = "..."`
    //      with `trace_csv.to_str().unwrap()`
    //    • use this `metric` instead of re-creating the logger
    //
    //    For example (pseudo):
    //
    //    let mut rdr = csv::ReaderBuilder::new()
    //        .has_headers(true)
    //        .from_path(&trace_csv)?;
    //    // … build your `trace: Arc<Vec<FrameInfo>>` …
    //    // … spawn encoder tasks …
    //    // … do your decode loop, calling
    //    //      metric.process_frame_buffers(…) …
    //
    //    Everything else stays exactly the same.

    let mut counter_frames: usize = 0; 
    /* 1. ─ parse CSV ─────────────────────────────────────── */
    // After you open the ReaderBuilder…

    let mut low_buf = HashMap::<u32, Vec<u8>>::new();
    let mut ref_buf = HashMap::<u32, Vec<u8>>::new();
    // let trace_path = "/home/boris/Desktop/Rust_MG1/asynchronix/Results/sim_T70_D2_Br20.0_PL0.100_NXR1_NBG0_UL0_PL/trace_offline_video0.csv"; 
    let trace_path =  trace_csv.to_str().unwrap();


    println!("Test1"); 
    let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1));
    // let metric: MetricsLogger = MetricsLogger::new(ip, "scenario")?;

    
    let start = Instant::now(); 
    // 1. ─ parse CSV ───────────────────────────────────────
    let mut raw_ids     = Vec::new();
    let mut raw_ts = Vec::new(); 
    let mut path_video  = None::<String>;
    let mut offset_video= None::<f64>;
    let mut idr_frequency = None::<u32>;
    let mut throughput_data = Vec::<f64>::new();

    // let trace_path = "/…/trace_offline_video0.csv";
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(trace_path)?;

    for rec in rdr.records() {
        let rec = rec?;

        // first non‐empty row → OFFSET_VIDEO (col 0), PATH_VIDEO (col 1), IDR_FREQUENCY (col 2)
        if path_video.is_none() {
            if let (Some(o), Some(p), Some(i)) =
                (rec.get(0), rec.get(1), rec.get(2))
            {
                if !o.trim().is_empty()
                 && !p.trim().is_empty()
                 && !i.trim().is_empty()
                {
                    offset_video    = Some(o.trim().parse()?);
                    path_video      = Some(p.trim().to_string());
                    idr_frequency   = Some(i.trim().parse()?);
                    continue;
                }
            }
        }

        // subsequent rows → timestamp(col 3), ID_frame(col 4), Lost(col 5), Throughput(col 6)
        if let (Some(_ts), Some(id_s), Some(lost_s), Some(tp_s)) =
            (rec.get(3), rec.get(4), rec.get(5), rec.get(6))
        {
            // skip any empty/data‐garbage rows
            if id_s.trim().is_empty() { continue; }

            let id:    u32   = id_s.trim().parse()?;
            let lost:  bool  = lost_s.trim().parse::<u32>()? != 0;
            let tp:    f64   = tp_s.trim().parse()?;
            let ts:     f64 = _ts.trim().parse()?; 

            raw_ids.push(id);
            raw_ts.push(ts); 
            throughput_data.push(tp);

            // if you want to track lost in the CSV pass-through you could also
            // store it alongside id here, or later when you build FrameInfo[]
        }
    }

    // sanity‐check & unwrap
    let video = path_video
        .clone()
        .ok_or_else(|| anyhow::anyhow!("missing video path"))?;
    let offset = offset_video
        .ok_or_else(|| anyhow::anyhow!("missing offset"))?;
    let idr    = idr_frequency
        .ok_or_else(|| anyhow::anyhow!("missing IDR_FREQUENCY"))?;
    let ts_map: HashMap<u32,f64> = raw_ids.iter().cloned().zip(raw_ts.iter().cloned()).collect();


    println!(
        "Video={}\n, offset={} s\n, IDR_FREQ={}fps\n, read {} frames\n, {} throughput samples\n",
        video,
        offset,
        idr,
        raw_ids.len(),
        throughput_data.len()
    );


        // sanity‐check
        let video = path_video.clone().ok_or_else(|| anyhow::anyhow!("missing video path"))?;
        let offset = offset_video.ok_or_else(|| anyhow::anyhow!("missing offset"))?;

        raw_ids.sort_unstable();
        raw_ids.dedup();
        let &min_id = raw_ids.first().unwrap();
        let &max_id = raw_ids.last().unwrap();
        let id_set: HashSet<_> = raw_ids.into_iter().collect();

        // build a complete sequence, marking any missing `id` as lost
        let mut trace = Vec::with_capacity((max_id - min_id + 1) as usize);
        for id in min_id..=max_id {
            trace.push(FrameInfo {
            id,
            lost: !id_set.contains(&id),
            });
        }
        let trace = Arc::new(trace);
        println!("Rebuilt trace with {} total packets ({} lost)", 
                trace.len(),
                trace.iter().filter(|f| f.lost).count()
        );
        println!("Test4"); 

        let path_video = path_video.clone().ok_or_else(|| anyhow::anyhow!("PATH_VIDEO missing"))?;
        let offset_video = offset_video.ok_or_else(|| anyhow::anyhow!("OFFSET_VIDEO missing"))?;
        let trace = Arc::new(trace);
        println!("Loaded {} rows – path=\"{}\" offset={}", trace.len(), path_video, offset_video);

        /* 2. ─ shared channel + encoder tasks ───────────────── */
        // make_encoder_task(1, 100.0, Arc::clone(&trace), tx.clone(), path_video.clone(), offset_video);
          // central dispatcher: now (tag, id, Vec<u8>)
        let (tx, rx) = unbounded::<(usize, u32, Vec<u8>)>();
        // low-bitrate path (30 Mbps) → simulate loss
        make_encoder_task(
            0,
            30.0,
            Arc::clone(&trace),
            tx.clone(),
            path_video.clone(),
            offset_video,
            /* simulate_loss = */ true,
        );

        // high-bitrate reference (100 Mbps) → perfect
        make_encoder_task(
            1,
            100.0,
            Arc::clone(&trace),
            tx.clone(),
            path_video.clone(),
            offset_video,
            /* simulate_loss = */ false,
        );

        
        let mut dec_low  = HevcDecoder::new(60, 1920, 1080, "LOW");
        let mut dec_ref  = HevcDecoder::new(60, 1920, 1080, "REF");
        
        let mut want_id = None::<u32>;   // next id we try to pair
        

        /* 4. ─ create window ONCE ───────────────────────────── */
        const W: usize = 1920;
        const H: usize = 1080;
        const SCALE: f64 = 0.28;
        let scaled_w = (W as f64 * SCALE) as usize;
        let scaled_h = (H as f64 * SCALE) as usize;

        let title_color      = 0x00FF00;   // bright green
        let frame_id_color   = 0xFFAA00;   // orange ⇒ easy to read on video
        let y_title          = 10;                          // first text row
        let y_frame_number   = scaled_h - 40;          // last text row

        let win_w = scaled_w * 2 + 10;
        let mut window = Window::new(
            "Frame-sync offline",
            win_w,
            scaled_h,
            WindowOptions {
                scale: Scale::X1,         // we already down-scale manually
                ..WindowOptions::default()
            },
        )?;

        /* 5. ─ main dispatch loop ───────────────────────────── */
        let expected = trace.len();
        let mut seen_pairs = 0;
        let (mut seen30, mut seen100) = (0usize, 0usize);


        
        while window.is_open() {

            while let Ok((tag, id, pkt)) = rx.try_recv() {
                match tag {
                    0 => dec_low.process_packet(pkt, id),
                    1 => dec_ref.process_packet(pkt, id),
                    _ => {}
                }
            }
            // — non‐blocking receive & decode for low
            if let Some((rgb_l, id_l, _ts)) = dec_low.next_decoded_frame() {
                if let Some(rgb_r) = ref_buf.remove(&id_l) {
                    // we already have the matching ref frame
                    let sim = similarity_rgb_hybrid(&rgb_l, &rgb_r, 1920, 1080);
                     // compute timestamp relative to program start
                    // let timestamp_ms = start.elapsed().as_secs_f64();
                    let ts_ms = ts_map.get(&id_l).unwrap_or(&0.0);


                    // log VMAF/PSNR/SSIM into CSV
                    metric.process_frame_buffers(
                        id_l as u64,
                        *ts_ms,
                        &rgb_r,
                        &rgb_l,
                        ip,
                    ).await?;
                    // metric.clone().spawn_metrics(id_l as u64, timestamp_ms, rgb_r.clone(), rgb_l.clone(), ip);

                    draw_pair(&mut window, &rgb_l, &rgb_r, id_l, sim)?;
                    seen_pairs += 1;
                } else {
                    // stash low until its ref arrives
                    low_buf.insert(id_l, rgb_l);
                }
            }

            // — non‐blocking receive & decode for ref
            if let Some((rgb_r, id_r, _ts)) = dec_ref.next_decoded_frame() {
                if let Some(rgb_l) = low_buf.remove(&id_r) {
                    // we already have the matching low frame
                    let sim = similarity_rgb_hybrid(&rgb_l, &rgb_r, 1920, 1080);
                    draw_pair(&mut window, &rgb_l, &rgb_r, id_r, sim)?;
                } else {
                    // stash ref until its low arrives
                    ref_buf.insert(id_r, rgb_r);
                }
            }
            if seen_pairs >= expected {
                break;
              }

            // ALWAYS update the window so it stays responsive
            window.update();
        }


    Ok(())
}

use walkdir::WalkDir;


#[tokio::main]
async fn main() -> Result<()> {
    // 1) Prepare your IP (or however you choose it)
    let ip: IpAddr = "192.168.0.1".parse().unwrap();

    // 2) Regex for matching “trace_offline_video<idx>.csv”
    let trace_re = Regex::new(r"^trace_offline_video\d+\.csv$")?;

    // 3) Recurse under “Results/”
    for entry in WalkDir::new("Results/").into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.is_file() {
            if let Some(fname) = path.file_name().and_then(|s| s.to_str()) {
                if trace_re.is_match(fname) {
                    println!("→ scheduling {:?}", path);
                    // clone the path for the async task
                    let trace_path = PathBuf::from(path);
                    let ip_clone   = ip.clone();
                    // run them *sequentially* or `tokio::spawn` if you want them in parallel
                    process_trace(trace_path, ip_clone).await?;
                }
            }
        }
    }
    

    Ok(())
}
