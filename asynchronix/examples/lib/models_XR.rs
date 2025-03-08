use crate::lib::alvr_control_socket::{
    framed_recv, framed_recv_vec, ControlSocketReceiver, ControlSocketSender,
};
use crate::lib::alvr_stream_socket::{Buffer, StreamReceiver};
use rand_distr::{Distribution, Normal};
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::{
    fs::write,
    io,
    process::{ChildStdin, ChildStdout, Stdio},
};
use anyhow::Result;
use async_std::stream::StreamExt; // Add this import to fix the .next() error

use tempfile::TempDir;
use tokio::sync::Mutex as tokMutex; 
use crate::lib::HevcParser;
use crate::lib::alvr_packets::{DeviceMotion, Pose}; 
use std::cell::RefCell;
use std::error::Error;
use std::fs;
use std::io::{BufReader, BufWriter, Read, Write};
use std::process::{Child, Command};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread_local;

use futures::io::{AsyncReadExt, AsyncWriteExt};
use minifb::Key;

use minifb::{Window, WindowOptions};
use std::{fs::File, thread, write};

use crate::debug_bgprint;
use crate::debug_print;
use crate::format_elapsed;
use crate::lib::{HeaderALVRStream, USE_FFMPEG, USE_VMAF};
use crate::print_pretty;
use core::{f64, net};
use ffmpeg_sidecar::command::FfmpegCommand;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::net::IpAddr;
use std::{mem, vec};
use std::thread::yield_now;
use std::time::{Duration, Instant};
use std::time::SystemTime;
use once_cell::sync::Lazy;
use glam::{Vec3, Quat};

use std::sync::mpsc::{};
use minifb_fonts::font6x8;

use crate::lib::alvr_packets::{ClientControlPacket, ClientStatistics, NetworkStatisticsPacket};
use crate::lib::alvr_stream_socket::{
    parse_shard_data, ConnectionError, DscpTos, Haptics, ReceiverData, SocketBufferSize,
    SocketProtocol, SocketReader, StreamSender, StreamSocketBuilder, Tracking, VideoPacketHeader,
};
use crate::lib::alvr_control_socket::ProtoControlSocket;
use tai_time::TaiTime;
use crate::lib::alvr_stream_socket::{ AUDIO, HAPTICS, INITIAL_FRAMERATE_FPS, MAX_HISTORY_SIZE, STATISTICS, TRACKING, VIDEO};
use crate::lib::DEBUG_PRINT_ENABLED;
use dashmap::DashMap;
use minifb::Scale;

use std::cmp::{self, max};
use std::collections::VecDeque;
use std::f64::consts::PI;
use std::future::Future;
use asynchronix::model::{Context, Model};
use asynchronix::ports::Output;
use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;


use crate::lib::{exponential, AmpduPacket, Coords, DebugColor, MpduPacket, SlidingWindowAverage};
use crate::lib::alvr_statistics::StatisticsManager;
// use crate::lib::INITIAL_BITRATE_MBPS_SIM;
use super::alvr_packets::DeadlineShardlossStatPacket;
use super::alvr_stream_socket::{SocketWriter, StreamSocket, IDR_FRAME_SIZE_GOP, MAX_PACKET_SIZE_RECV};
use super::alvr_stream_socket::{CONTROL_STREAM, MAX_DEADLINE_IN_STATS};
use super::{SlidingWindowTimely, _INITIAL_BITRATE_MBPS_SIM};
// use async_process::Child;
use lazy_static::lazy_static;
use crate::lib::alvr_control_socket::{ControlPacketType};
use tokio::task;

use serde::Deserialize;

pub const WIDTH_ENCODER: usize = 1920;
pub const HEIGHT_ENCODER: usize = 1080;


pub const FRAMERATE_WINDOWS: usize =  60;  

pub const SCALE_FACTOR_WINDOW: f64 = 0.65;

static STATISTICS_MANAGER: OptLazy<StatisticsManager> = lazy_mut_none();

pub const SHARD_PREFIX_SIZE: usize = mem::size_of::<u32>() // packet length - field itself (4 bytes)
    + mem::size_of::<u16>() // stream ID
    + mem::size_of::<u32>() // packet index
    + mem::size_of::<u32>() // shards count
    + mem::size_of::<u32>() // shards index
    + mem::size_of::<f32>(); // tx relative timestamp





type InstantMap = Arc<RwLock<HashMap<u32, TaiTime<0>>>>;
// Static ffmpeg resources
static FFMPEG_COMMAND: OnceLock<Arc<Mutex<FfmpegCommand>>> = OnceLock::new();
static FFMPEG_CHILD: OnceLock<Arc<Mutex<Option<(ChildStdin, BufReader<ChildStdout>)>>>> =
    OnceLock::new();



pub const UPDATE_BITRATE_INTERVAL: Duration = Duration::from_secs(1);
pub const HANDSHAKE_ACTION_TIMEOUT: Duration = Duration::from_secs(2);
pub const MAX_UNREAD_PACKETS: usize = 5; // Applies per stream

pub const CAPACITY_RX_BUFFER: usize = 2000;
pub const STREAMING_RECV_TIMEOUT: Duration = Duration::from_millis(10);
pub const FRAMED_PREFIX_CONTROL_LENGTH: usize = mem::size_of::<u32>();

pub const DECODER_BUFFERING_FRAMES: usize = 10;
pub const TARGET_FRAMES_DECODER_QUEUE: usize = DECODER_BUFFERING_FRAMES/2;


pub const VMAF_FRAME_GROUP_SIZE: usize = 10;
pub const TARGET_TIMESTAMP_TRACKING: Duration = Duration::from_millis(10); 


// static _STATISTICS_MANAGER: OptLazy<StatisticsManager> = lazy_mut_none();

use crossbeam::channel::{Receiver, unbounded, bounded, Sender, TryRecvError};  



lazy_static! {
    static ref REFERENCE_DECODERS: Arc<Mutex<HashMap<IpAddr, HevcDecoder>>> = 
        Arc::new(Mutex::new(HashMap::new()));
}

struct FramePair {
    decoded: Option<Vec<u32>>,
    reference: Option<Vec<u32>>,
    frame_id: usize,
    timestamp: Instant,
}

fn render_text(buffer: &mut [u32], text: &str, x: usize, y: usize, stride: usize, color: u32, scale: usize) {
    // Simple 5x7 pixel font (common for basic bitmap fonts)
    // Each character is represented as an array of 7 bytes, where each byte represents a row
    // and the bits in each byte represent the pixels in that row
    const FONT_WIDTH: usize = 5;
    const FONT_HEIGHT: usize = 7;
    const CHAR_SPACING: usize = 1;
    
    // Apply scaling
    let scaled_font_width = FONT_WIDTH * scale;
    let scaled_font_height = FONT_HEIGHT * scale;
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
        [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1F]
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

fn resize_buffer(buffer: &[u32], orig_width: usize, orig_height: usize, new_width: usize, new_height: usize) -> Vec<u32> {
    let mut scaled = vec![0u32; new_width * new_height];
    
    // Calculate scaling ratios (use floating point for better precision)
    let x_ratio = orig_width as f64 / new_width as f64;
    let y_ratio = orig_height as f64 / new_height as f64;

    for y in 0..new_height {
        for x in 0..new_width {
            // Calculate source pixel coordinates
            let src_x = (x as f64 * x_ratio).floor() as usize;
            let src_y = (y as f64 * y_ratio).floor() as usize;
            
            // Ensure we don't go out of bounds
            let src_x = src_x.min(orig_width - 1);
            let src_y = src_y.min(orig_height - 1);
            
            // Get source pixel and store in destination buffer
            scaled[y * new_width + x] = buffer[src_y * orig_width + src_x];
        }
    }
    
    scaled
}

fn convert_rgb_to_u32(rgb_data: &[u8], width: usize, height: usize) -> Option<Vec<u32>> {
    if rgb_data.len() != width * height * 3 {
        println!("ERROR: Expected rgb_data size {} but got {}", 
                width * height * 3, rgb_data.len());
        return None;
    }
    
    let mut pixels = Vec::with_capacity(width * height);
    for chunk in rgb_data.chunks_exact(3) {
        let pixel = ((chunk[0] as u32) << 16) | 
                   ((chunk[1] as u32) << 8) | 
                   (chunk[2] as u32);
        pixels.push(pixel);
    }
    
    Some(pixels)
}

pub struct HevcDecoder {
    frame_rx: Receiver<Vec<u8>>,
    packet_tx: Sender<Vec<u8>>,
    _stdin_handle: std::thread::JoinHandle<()>,
    _stderr_handle: std::thread::JoinHandle<()>,
    width: u32,
    height: u32,
    parser: HevcParser,
    frame_buffer: VecDeque<Vec<u8>>,  // Buffer for parsed HEVC frames
    decoded_frames: VecDeque<Vec<u8>>, // Buffer for decoded RGB frames

    ewma_frame_size: f64,
    last_update: Instant, 

    pub frames_processed: usize,           // Count of frames we've sent to the decoder
    pub keyframes_seen: usize,             // Count of keyframes observed
    pub last_decoded_frame_time: Instant,  // Time when we last got a decoded frame
    pub total_bytes_processed: f64,      // Total bytes of HEVC data processed
    pub priming_complete: bool,            // Flag to indicate if decoder is primed and ready
    pub expected_frame_size: usize,        // Expected size of decoded RGB frames

    max_buffered_frames: usize,        // Maximum number of frames to buffer

    decoder_string: String, 


}


impl HevcDecoder {
    pub fn new(framerate: u32, width: u32, height: u32, decoder_str: &str) -> Self  {
        let frame_size = (width as usize) * (height as usize) * 3;

        let decoder_string = decoder_str.to_string();

        let mut child = FfmpegCommand::new()
            .hwaccel("cuda")
            .args(&["-f", "hevc", "-i", "-"])
            .args(&["-vf", &format!("fps={}", framerate)])
            .args(&["-pix_fmt", "rgb24"])

            // .args(&["-flags", "+low_delay"])
            // .args(&["-fflags", "+nobuffer+flush_packets"])
            .args(&["-f", "rawvideo", "-"])
            .spawn().unwrap();

        let stdout = child.take_stdout().unwrap();
        let stdin = child.take_stdin().unwrap();
        let stderr = child.take_stderr().unwrap();
        

        let (frame_tx, frame_rx) = unbounded::<Vec<u8>>();
        let (packet_tx, packet_rx) = bounded::<Vec<u8>>(100);



        // Start stdout reader thread with more explicit error handling
        std::thread::spawn({
            let frame_size = frame_size;
            let frame_tx = frame_tx.clone(); // Clone for the thread
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
                        },
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
                                    eprintln!("{decoder_str_clone} Decoder frame send error: {}", e);
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
        

        let decoder_string3 = decoder_string.clone();        // Stderr handler with improved debug output
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
                    },
                    Err(e) => {
                        eprintln!("{decoder_string3} Decoder stderr read error: {}", e);
                        break;
                    }
                }
            }
            println!("{decoder_string3} Decoder stderr reader thread exit");
        });
        println!("{decoder_str} 📹 HevcDecoder initialized with {}x{} resolution", width, height);

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
            max_buffered_frames: 10, 

            decoder_string: decoder_str.clone().to_string(), 
        }
    }

    pub fn is_ready(&self) -> bool {
        // A decoder is ready when:
        // 1. We've seen at least one keyframe
        // 2. We've processed at least 10 frames
        // 3. Priming is marked complete
        self.keyframes_seen >= 1
    }

    // New function to check if a frame contains valid HEVC data
    fn is_valid_hevc_frame(frame: &[u8]) -> bool {
        // Check for HEVC start code (0x000001 or 0x00000001)
        for i in 0..frame.len().saturating_sub(4) {
            if (frame[i] == 0 && frame[i + 1] == 0 && frame[i + 2] == 1) || 
               (frame[i] == 0 && frame[i + 1] == 0 && frame[i + 2] == 0 && frame[i + 3] == 1) {
                return true;
            }
        }
        false
    }

     // Check if a buffer contains a keyframe
     pub fn contains_keyframe(&self, buffer: &[u8]) -> bool {
        // For HEVC, keyframes are signaled by NAL types 16-21 (IRAP pictures)
        for i in 0..buffer.len().saturating_sub(5) {
            if (buffer[i] == 0 && buffer[i + 1] == 0 && buffer[i + 2] == 1) || 
               (buffer[i] == 0 && buffer[i + 1] == 0 && buffer[i + 2] == 0 && buffer[i + 3] == 1) {
                
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

    // Process incoming encoded packets with improved error handling
    pub fn process_packet(&mut self, packet: Vec<u8>) {
        // Add packet data to the parser
        // println!("🎬 Processing packet of size {} bytes (total: {} frames)", 
        //         packet.len(), self.frames_processed);



        let frame_size = packet.len() as f64;
        let now = Instant::now();
        let delta_t = now.duration_since(self.last_update).as_secs_f64();
        self.last_update = now;


        self.total_bytes_processed += frame_size;
        self.frames_processed += 1;
        
        // Check if this is a keyframe
        let is_keyframe = self.contains_keyframe(&packet);
        if is_keyframe {
            self.keyframes_seen += 1;
            // println!("🔑 KEYFRAME detected! Size: {}, Frame #{}, Total keyframes: {}", 
                    // frame_size, self.frames_processed, self.keyframes_seen);
        }

        // Calculate smoothing factor α
        let now = Instant::now();
        let delta_t = now.duration_since(self.last_update).as_secs_f64();
        self.last_update = now;
        let temporal_constant :f64 = 1.0;  // 1 second EWMA

        let alpha = temporal_constant - (-delta_t / temporal_constant).exp();
        self.ewma_frame_size = alpha * (frame_size as f64) + (1.0 - alpha) * self.ewma_frame_size;
        // println!("Parsing frame #{}: size={}, keyframe={}, EWMA size={:.2}", 
            // self.frames_processed, frame_size, is_keyframe, self.ewma_frame_size);

        self.parser.add_data(&packet);
        
        // Extract frames from the parser and buffer them
        let frames = self.parser.get_frames();
        for frame in frames {
            self.frame_buffer.push_back(frame);
        }
        
        // Forward t  // Forward packet to ffmpeg decoder
        if let Err(e) = self.packet_tx.send(packet) {
            println!("{} ERROR: Failed to send packet to decoder: {}", self.decoder_string, e);
            return;
        }
        
        // If we've processed enough frames, consider the decoder primed
        if !self.priming_complete && self.keyframes_seen >= 1 && self.frames_processed >= 5 {
            println!("{} 🚀 Decoder priming complete! Processed {} frames including {} keyframes",
                    self.decoder_string ,self.frames_processed, self.keyframes_seen);
            self.priming_complete = true;
        }
        
        // Ok(())
    }

        // Your existing method to get raw frames from ffmpeg
    // Modified try_next_decoded_frame with timeout
    fn try_next_decoded_frame_with_timeout(&self, timeout_ms: u64) -> Option<Vec<u8>> {
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_millis(timeout_ms);
        
        while start.elapsed() < timeout {
            match self.frame_rx.try_recv() {
                Ok(frame) => return Some(frame),
                Err(TryRecvError::Empty) => {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                    continue;
                },
                Err(TryRecvError::Disconnected) => {
                    println!("{} Decoder frame channel disconnected", self.decoder_string);
                    return None;
                }
            }
        }
        None
    }


        // Better implementation of process_decoded_frames
    pub fn process_decoded_frames(&mut self) -> usize {
        let mut frames_received = 0;
        let start_time = Instant::now();
        let max_processing_time = Duration::from_millis(50);  // Prevent blocking too long
        
        // Process frames with a time limit
        while start_time.elapsed() < max_processing_time {
            match self.frame_rx.try_recv() {
                Ok(frame) => {
                    frames_received += 1;
                    self.last_decoded_frame_time = Instant::now();
                    
                    // Check frame is the expected size
                    if frame.len() == self.expected_frame_size {
                        self.decoded_frames.push_back(frame);
                    } else {
                        println!("{} ⚠️ Received malformed frame (size={}), expected {}", 
                            self.decoder_string,frame.len(), self.expected_frame_size);
                        // Only add if it's close - this helps avoid complete corruption
                        if frame.len() >= self.expected_frame_size * 9 / 10 && 
                        frame.len() <= self.expected_frame_size * 11 / 10 {
                            self.decoded_frames.push_back(frame);
                        }
                    }
                    
                    // Don't buffer too many frames - it causes delay
                    if self.decoded_frames.len() >= self.max_buffered_frames/2 {
                        break;
                    }
                },
                Err(TryRecvError::Empty) => {
                    // No more frames available now
                    break;
                },
                Err(TryRecvError::Disconnected) => {
                    println!("{} 🛑 Decoder output channel disconnected!", self.decoder_string);
                    // Trigger restart at next opportunity
                    // self.needs_restart = true;
                    break;
                }
            }
        }
        
        if frames_received > 0 {
            // println!("✅ Added {} frames to decoded buffer, now has {} frames (in {}ms)",
                    // frames_received, self.decoded_frames.len(), start_time.elapsed().as_millis());
        }
        
        frames_received
    }
    
        // Get the next available encoded frame
        pub fn next_encoded_frame(&mut self) -> Option<Vec<u8>> {
            self.frame_buffer.pop_front()
        }
    
        // Get the next available decoded RGB frame
        pub fn next_decoded_frame(&mut self) -> Option<Vec<u8>> {
            // First try to process any newly available frames
            let frames_added = self.process_decoded_frames();
            
            // Then try to get a frame from the buffer
            if let Some(frame) = self.decoded_frames.pop_front() {
                // println!("🖼️ Returning decoded frame of size: {} bytes", frame.len());
                Some(frame)
            } else {
                if frames_added > 0 {
                    println!("{} Strange: Added frames but buffer is now empty?", self.decoder_string);
                } else if self.priming_complete {
                    println!("\n\n******************************No decoded frames available (buffer empty)");
                } else {
                    println!("{} Decoder still priming ({}/{} frames processed)", self.decoder_string, 
                            self.frames_processed, 5);
                }
                None
            }
        }
        
    pub fn try_next_frame(&self) -> Option<Vec<u8>> {
        // println!("TRY READ FRAME"); // ADDED LOGGING
        match self.frame_rx.try_recv() {
            Ok(frame) => {
                // println!("Frame received!"); // ADDED LOGGING
                Some(frame)
            },
            Err(TryRecvError::Empty) => {
                println!("{}  No frame available yet.", self.decoder_string); // ADDED LOGGING
                None
            },
            Err(TryRecvError::Disconnected) => {println!("WARNING! {} frame channel disconnected", self.decoder_string);
                                                 None
                                                } ,
        }
    }
}



#[derive(Serialize, Deserialize, Clone, Debug, Copy, Default)]
pub struct HeuristicStats {
        pub frame_interval_s: f32,
        pub server_fps: f32,
        pub steps_bps: f32,
    
        pub network_heur_fps: f32,
        pub rtt_avg_heur_s: f32,
        pub random_prob: f32,
    
        pub threshold_fps: f32,
        pub threshold_rtt_s: f32,
        pub threshold_u: f32,
    
        pub requested_bitrate_bps: f32,
    }
#[derive(Clone)]
pub struct EncoderLatencyLimiter{
    pub max_saturation_multiplier: f32, 
}
#[derive(Clone)]
pub struct DecoderLatencyLimiter{
    pub max_decoder_latency_ms: u64, 
    pub latency_overstep_frames: usize,
    pub latency_overstep_multiplier: f32,
}
#[derive(Clone)]
pub enum BitrateMode {
    ConstantMbps(u64),
    // Adaptive {
    //     saturation_multiplier: f32,
    //     max_bitrate_mbps: u64,
    //     min_bitrate_mbps: u64,
    //     max_network_latency_ms: u64,
    //     encoder_latency_limiter: EncoderLatencyLimiter,
    //     decoder_latency_limiter: DecoderLatencyLimiter, 
    // },
    NestVr {
        update_interval_nestvr_s: f32,
        max_bitrate_mbps: f32,
        min_bitrate_mbps: f32,
        initial_bitrate_mbps: f32,
        step_size_mbps: f32,
        capacity_scaling_factor: f32,
        rtt_explor_prob: f32,
        nfr_thresh: f32,
        rtt_thresh_scaling_factor: f32,
    }
}

#[allow(unused)]
#[derive(Clone)]
pub struct BitrateManager {
    last_frame_instant: Instant,
    last_update_instant: Instant,

    pub bitrate_mode: BitrateMode, 
    frame_index: usize,

    frame_interval_average: SlidingWindowAverage<Duration>,
    encoder_latency_average: SlidingWindowAverage<Duration>,
    network_latency_average: SlidingWindowAverage<Duration>,

    bitrate_average_mbps: SlidingWindowAverage<f32>,

    pub last_target_bitrate_mbps: f32,
    update_interval_s: Duration,

    rtt_average: SlidingWindowAverage<Duration>,
    peak_throughput_average: SlidingWindowAverage<f32>,
    frame_interarrival_average: SlidingWindowAverage<f32>,

    last_target_bitrate_bps : f32, 
    
}

impl BitrateManager {
    pub fn report_network_statistics(
        &mut self,
        network_rtt: Duration,
        peak_throughput_bps: f32,
        frame_interarrival_s: f32,
    ) {
        self.rtt_average.submit_sample(network_rtt);

        self.peak_throughput_average
            .submit_sample(peak_throughput_bps);

        self.frame_interarrival_average
            .submit_sample(frame_interarrival_s);
    }

    pub fn one_pass_abr(&mut self, ) -> f32 {
        let bitrate_bps = match self.bitrate_mode{
            BitrateMode::ConstantMbps(bitrate_mbps) => bitrate_mbps as f32 * 1e6,

            BitrateMode::NestVr {
                max_bitrate_mbps,
                min_bitrate_mbps,
                initial_bitrate_mbps,
                step_size_mbps,
                capacity_scaling_factor,
                rtt_explor_prob,
                nfr_thresh,
                rtt_thresh_scaling_factor,
                ..
            } => {
                fn floor_to_nearest_mult_from_initial(value: f32, step: f32, initial: f32) -> f32 {
                    initial + ((value - initial) / step).floor() * step
                }

                fn minmax_bitrate(
                    bitrate_bps: f32,
                    max_bitrate_mbps: f32,
                    min_bitrate_mbps: f32,
                ) -> f32 {
                    let mut bitrate = bitrate_bps;
                    bitrate = f32::min(bitrate, max_bitrate_mbps * 1e6);
                    bitrate = f32::max(bitrate, min_bitrate_mbps * 1e6);

                    bitrate
                }

                // Sample from uniform distribution
                let mut rng = rand::thread_rng();
                let uniform_dist = Uniform::new(0.0, 1.0);
                let random_prob = rng.sample(uniform_dist);

                let mut bitrate_bps: f32 = self.last_target_bitrate_bps;

                let frame_interval_s = self.frame_interval_average.get_average().as_secs_f32();
                let rtt_avg_heur_s = self.rtt_average.get_average().as_secs_f32();

                let server_fps = if frame_interval_s != 0.0 {
                    1.0 / frame_interval_s
                } else {
                    0.0
                };
                let heur_fps = if self.frame_interarrival_average.get_average() != 0.0 {
                    1.0 / self.frame_interarrival_average.get_average()
                } else {
                    0.0
                };

                let estimated_capacity_bps = self.peak_throughput_average.get_average();
                let steps_bps = step_size_mbps * 1E6;

                let threshold_fps = nfr_thresh * server_fps;
                let threshold_rtt = frame_interval_s * rtt_thresh_scaling_factor;
                let threshold_u = rtt_explor_prob;

                if heur_fps >= threshold_fps {
                    if rtt_avg_heur_s > threshold_rtt {
                        if random_prob >= threshold_u {
                            bitrate_bps -= steps_bps; // decrease bitrate by 1 step
                        }
                    } else {
                        if random_prob <= threshold_u {
                            bitrate_bps += steps_bps; // increase bitrate by 1 step
                        }
                    }
                } else {
                    bitrate_bps -= steps_bps; // decrease bitrate by 1 step
                }

                // Ensure bitrate is within allowed range
                bitrate_bps = minmax_bitrate(bitrate_bps, max_bitrate_mbps, min_bitrate_mbps);

                // Ensure bitrate is below the estimated network capacity
                let capacity_upper_limit = capacity_scaling_factor * estimated_capacity_bps;
                bitrate_bps = floor_to_nearest_mult_from_initial(
                    f32::min(bitrate_bps, capacity_upper_limit),
                    steps_bps,
                    initial_bitrate_mbps * 1E6,
                );

                let heur_stats = HeuristicStats {
                    frame_interval_s: frame_interval_s,
                    server_fps: server_fps, // fps_tx
                    steps_bps: steps_bps,

                    network_heur_fps: heur_fps, // fps_rx
                    rtt_avg_heur_s: rtt_avg_heur_s,
                    random_prob: random_prob,

                    threshold_fps: threshold_fps,
                    threshold_rtt_s: threshold_rtt,
                    threshold_u: threshold_u,

                    requested_bitrate_bps: bitrate_bps,
                };

                debug_bgprint!(DebugColor::Purple , " ------NeSt-VR STATS-------: {:#?}", heur_stats); 

                // alvr_events::send_event(EventType::HeuristicStats(heur_stats));

                // if let Switch::Enabled(max) = max_bitrate_mbps {
                //     let maxi = *max as f32 * 1e6;
                //     stats.manual_max_bps = Some(maxi);
                // }
                // if let Switch::Enabled(min) = min_bitrate_mbps {
                //     let mini = *min as f32 * 1e6;
                //     stats.manual_min_bps = Some(mini);
                // }

                self.last_target_bitrate_bps = bitrate_bps; 
                bitrate_bps
            }

        }; 
        debug_bgprint!(DebugColor::Purple , " Bitrate chosen -> {:.3} mbps", bitrate_bps / 1e6); 
        bitrate_bps
    }




    pub fn report_timestamp_change_bitrate(&mut self, now: TaiTime<0>) {
        let dur = now.duration_since(TaiTime::EPOCH).as_secs_f64();
        // TODO: ACTUAL IMPLEMENTATION OF ABR, now just:

        // if dur < 5.0{
        //     self.last_target_bitrate_mbps = 10.0;
        // }
        // else if 5.0 <= dur && dur < 10.0 {
        //     self.last_target_bitrate_mbps = 0.01;
        // }
        if 10.0 <= dur && dur < 1000.0 {
            // self.last_target_bitrate_mbps = 10.0; // just CBR for now
        }
        // } else if 12.0 <= dur && dur < 25.0 {
        //     self.last_target_bitrate_mbps = 0.9;
        // } else if 25.0 <= dur && dur < 30.0 {
        //     self.last_target_bitrate_mbps = 10.0;
        // } else if 35.0 <= dur && dur < 45.0 {
        //     self.last_target_bitrate_mbps = 0.2;
        // } else if 45.0 <= dur && dur < 55.0 {
        //     self.last_target_bitrate_mbps = 10.0;
        // } else if 55.0 <= dur && dur < 65.0 {
        //     self.last_target_bitrate_mbps = 0.5;
        // } else if 65.0 <= dur && dur < 75.0 {
        //     self.last_target_bitrate_mbps = 10.0;
        // } else if 75.0 <= dur && dur < 85.0 {
        //     self.last_target_bitrate_mbps = 1.0;
        
        debug_bgprint!(
            DebugColor::Tan,
            "t = {}, [DBG bitrate set] {} Mbps",
            dur,
            self.last_target_bitrate_mbps,
        );
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

impl BitrateManager {
    // TODO: Add method for CBR
    pub fn new(max_history_size: usize, initial_framerate: f32, initial_bitrate_mbps: f32) -> Self {
        Self {
            last_frame_instant: Instant::now(),
            last_update_instant: Instant::now(),

            frame_index: 0,

            frame_interval_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),
            encoder_latency_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),
            network_latency_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),

            bitrate_average_mbps: SlidingWindowAverage::new(initial_bitrate_mbps, max_history_size),
            last_target_bitrate_mbps: initial_bitrate_mbps,
            update_interval_s: UPDATE_BITRATE_INTERVAL,

            rtt_average: SlidingWindowAverage::new(Duration::from_millis(5), max_history_size),
            peak_throughput_average: SlidingWindowAverage::new(300E6, max_history_size),
            frame_interarrival_average: SlidingWindowAverage::new(
                1. / initial_framerate,
                max_history_size,
            ),

            bitrate_mode: BitrateMode::ConstantMbps(_INITIAL_BITRATE_MBPS_SIM as u64),   // ONLY CBR FOR NOW!!!
            last_target_bitrate_bps: 0.0, 
        }
    }
}
#[allow(unused)]
pub struct XRServer {
    pub ip_self: IpAddr,
    pub ip_client: IpAddr,

    pub t_0: TaiTime<0>,
    pub bitrate_manager: BitrateManager,

    pub video_app_sender: Option<StreamSender<VideoPacketHeader>>,

    pub tracking_app_receiver: Option<StreamReceiver<Tracking>>,
    pub statistics_app_receiver: Option<StreamReceiver<ClientStatistics>>,

    pub control_socket_sender: Option<ControlSocketSender<ClientControlPacket>>,
    pub control_socket_receiver: Option<ControlSocketReceiver<ClientControlPacket>>,

    pub outport_videoapp_network: Output<MpduPacket>, // ONLY VIDEO FOR NOW!

    pub is_streaming: bool,

    pub fps: f32,

    // pub sockets: SimRuntimeSockets,
    pub frames_sent_counter: usize,

    pub map_rtt: Arc<DashMap<u32, TaiTime<0>>>,
    pub STATISTICS_MANAGER: StatisticsManager,
}
#[allow(unused)]
impl XRServer {
    pub fn new(
        ip_self: IpAddr,
        ip_client: IpAddr,
        t0_sim: TaiTime<0>,
        frame_rate: f32,
        initial_bitrate: f32,
        name_folder: &str,
    ) -> Self {
        let system_time = SystemTime::UNIX_EPOCH;
        Self {
            ip_self,
            ip_client,
            t_0: t0_sim,
            bitrate_manager: BitrateManager::new(
                MAX_HISTORY_SIZE,
                INITIAL_FRAMERATE_FPS,
                initial_bitrate,
            ),

            video_app_sender: None,

            tracking_app_receiver: None,
            statistics_app_receiver: None,

            control_socket_sender: None,
            control_socket_receiver: None,
            outport_videoapp_network: Output::default(),
            // output_audio: Output::default(),
            // output_haptics: Output::default(),
            is_streaming: false,
            fps: frame_rate,
            // sockets,
            frames_sent_counter: 0,

            map_rtt: Arc::new(DashMap::new()),
            STATISTICS_MANAGER: StatisticsManager::new(
                MAX_HISTORY_SIZE,
                Duration::from_secs_f32(1.0 / frame_rate),
                0.0,
                name_folder,
                ip_self, 
            ),
        }
    }

    pub fn handle_control_packet(&mut self, packet: ClientControlPacket, now: TaiTime<0>) {
        if let Some(mut protorecv) = self.control_socket_receiver.clone() {
            // let packet = protorecv.recv(STREAMING_RECV_TIMEOUT).unwrap();
            let map_clone: Arc<DashMap<u32, TaiTime<0>>> = Arc::clone(&self.map_rtt);

            match packet {
                ClientControlPacket::NetworkStatistics(network_stats) => {
                    // debug_bgprint!(DebugColor:: Teal, "{:.9}[DBG SERVER STATS]- Received stats for frame {:2.0}: \nNetwork stats:\n\t\t{:#?}",now.duration_since(self.t_0).as_secs_f64(), network_stats.frame_index,network_stats);

                    // let mut map_rtt_lock = map_clone.write().unwrap();
                    let frame_id = network_stats.frame_index as u32;
                    let rtt: Duration;
                    // if let send_instant = map_clone.get(&frame_id).unwrap()
                    if let Some(( _, send_instant)) = map_clone.remove(&frame_id) {
                        rtt = now.duration_since(send_instant);
                        // println!("SEND INSTANT: {}, now: {}, rtt: {}", format_elapsed!(send_instant), format_elapsed!(now), rtt.as_secs_f32());
                    } else {
                        println!(
                            "frame {} RTT ZEROO!!!!!",
                            network_stats.frame_index
                        );
                        rtt = Duration::ZERO;
                    }

                    // // Before removing an entry, check whether its lifetime has exceeded the expected range.
                    // if let Some(send_instant) = map_rtt_lock.remove(&frame_id) {// Process RTT normally
                    //         rtt = now.duration_since(send_instant);
                    // } else {
                    //     println!("Frame {} missing in map_rtt, possible packet loss or eviction!", frame_id);
                    //     rtt = Duration::ZERO;
                    // }
                    // if let Some(send_instant) = hashmap.remove(&frame_id) {
                    //     rtt = now.duration_since(send_instant);
                    //     println!("rtt = {:.9}", rtt.as_secs_f64());
                    // }
                    // else {
                    //     println!("frame {} RTT ZEROOOOOOOOOOOOOO!!!!!!!!!!!!!!!!!!!!!!!!!",  network_stats.frame_index);
                    //     rtt = Duration::ZERO;
                    // }

                    debug_bgprint!(DebugColor::Teal, "RTT = {:.9}", rtt.as_secs_f64());

                    let (peak_network_throughput_bps, frame_interarrival_s) = self
                        .STATISTICS_MANAGER
                        .report_network_statistics(network_stats, rtt, now);

                    // BITRATE_MANAGER.lock().report_network_statistics
                    self.bitrate_manager.report_network_statistics(
                        rtt,
                        peak_network_throughput_bps,
                        frame_interarrival_s,
                    );
                }
                ClientControlPacket::DeadlineShardLossStat(inner) => {
                    let frames_lost = inner.frame_indexes;
                    let shards_lost = inner.shards_lost;

                    for (frame, shard) in frames_lost.iter().zip(shards_lost.iter()) {
                        print_pretty!(DebugColor::Red ,"[DBG_DEAD_RX server {}] Frame {} lost {} shards", self.ip_self, frame, shard);
                    }
                }

                _ => {
                    println!("UNEXPECTED CONTROL PACKET RECEIVED!!");
                }
            }
        }
    }

    pub async fn in_from_network(&mut self, frame: TimedFrame) {
        let packet_vec = frame.vec;
        let now = frame.timestamp;

        for packet in packet_vec {
            let header = packet.header_alvr.clone();
            let buffer = packet.data_inner.clone();
            // println!("XRServer IN NETWORK. Header: {:?}, buffer_len = {}", header, buffer.len());

            match header.stream_id {
                TRACKING => {
                    if let Some(sock) = self.tracking_app_receiver.clone() {
                        let sender = sock.network_app_interface.lock().unwrap().send(&buffer);
                        let mut new_buffer: Vec<u8> = vec![0; MAX_PACKET_SIZE_RECV];
                        let receiver = sock.inner.lock().unwrap().recv(&mut new_buffer);

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

                        let results = sock.send(&stats);

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
                            // TODO: CHECK WITH WIRESHARK ENCAPSULATION OF PACKET
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
                                if stream_id == VIDEO{
                                    self.outport_videoapp_network.send(packet).await;
                                }
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

    // pub fn generate_video_frame(&mut self, context: &Context<Self> ){

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

    pub fn generate_video_frame<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let now = context.scheduler.time();
            // let map_clone: Arc<RwLock<HashMap<u32, TaiTime<0>>>> = Arc::clone(&self.map_rtt);

            let map_clone: Arc<DashMap<u32, TaiTime<0>>> = Arc::clone(&self.map_rtt);
            self.video_app_sender.as_mut().unwrap().next_packet_index =
                self.frames_sent_counter as u32;
            
            // STEP 1: DEBUG VIDEO
            if let Some(mut send_socket) = self.video_app_sender.clone() {
                let is_idr = false;
                let header = VideoPacketHeader::new(Duration::from_secs(1), is_idr);

                self.bitrate_manager.report_timestamp_change_bitrate(now);   // for programatically changing CBR bitrate
                let current_bitrate_mbps: f32 = self.bitrate_manager.last_target_bitrate_mbps;
                // 
                // let current_bitrate_mbps: f32 = self.bitrate_manager.one_pass_abr(); // for ABR bitrates

                let mut buffer_emu =
                    send_socket // generate the actual video frame data
                        .get_buffer_emu(&header, current_bitrate_mbps, now, self.ip_self,  self.frames_sent_counter)
                        .await.unwrap();

                if let Some(encoder_init) = send_socket.clone().ffmpeg_encoder{
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

                let normal = Normal::new(0.0, 2.0).unwrap(); // Mean = 0, Std dev = 5
                let epsilon = normal.sample(&mut rand::thread_rng()); // Random Gaussian value
                
                let time_until_next_frame = Duration::from_secs_f32(1.0 / (self.fps + epsilon));

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
            // STEP 2: DO SAME FOR REST OF PACKETS (VIDEO; HAPTICS) and loop using context.scheduler!
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





#[derive(Debug, Clone)]
struct FrameData {
    ref_rgb: Vec<u8>,
    lossy_rgb: Vec<u8>,
    timestamp_ms: f64,
    frame_number: u64,
}
struct FrameGroup {
    frames: Vec<FrameData>,
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
}
impl MetricsLogger {
    fn new(ip: IpAddr) -> Result<Self> {
        let file = File::create(format!("Video_Sink/{}/metrics.csv", ip))?;
        let writer = csv::Writer::from_writer(file);
        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    pub async fn process_frame_metrics(
        &self,
        frame_number: u64,
        timestamp_ms: f64,
        ref_path: &str,
        lossy_path: &str
    ) -> Result<()> {
        // Create a temporary directory for processing
        let temp_dir = TempDir::new()?;
        
        // Convert RGB frames to Y4M format (better for VMAF processing)
        let ref_y4m = temp_dir.path().join("reference.y4m").to_string_lossy().to_string();
        let lossy_y4m = temp_dir.path().join("lossy.y4m").to_string_lossy().to_string();
        
        // Convert reference frame to Y4M
        let ref_status = Command::new("ffmpeg")
            .args(&[
                "-y",
                "-f", "rawvideo",
                "-pixel_format", "rgb24",
                "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
                "-i", ref_path,
                "-pix_fmt", "yuv420p",
                &ref_y4m
            ])
            .status()?;
        
        if !ref_status.success() {
            return Err(anyhow::anyhow!("Failed to convert reference frame to Y4M"));
        }
        
        // Convert lossy frame to Y4M
        let lossy_status = Command::new("ffmpeg")
            .args(&[
                "-y",
                "-f", "rawvideo",
                "-pixel_format", "rgb24",
                "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
                "-i", lossy_path,
                "-pix_fmt", "yuv420p",
                &lossy_y4m
            ])
            .status()?;
        
        if !lossy_status.success() {
            return Err(anyhow::anyhow!("Failed to convert lossy frame to Y4M"));
        }
                
            // Create the Video_Sink directory within the temp directory
        let video_sink_dir = temp_dir.path().join("Video_Sink");
        std::fs::create_dir_all(&video_sink_dir)?;

        // Set up paths correctly
        let vmaf_json = video_sink_dir.join("vmaf.json").to_string_lossy().to_string();
        let psnr_log = video_sink_dir.join("psnr.log").to_string_lossy().to_string();
        let ssim_log = video_sink_dir.join("ssim.log").to_string_lossy().to_string();

        // Calculate all metrics in a single ffmpeg call
        let metrics_status = Command::new("ffmpeg")
            .args(&[
                "-i", &ref_y4m,
                "-i", &lossy_y4m,
                "-filter_complex", &format!("[0:v][1:v]libvmaf=log_fmt=json:log_path={}", vmaf_json),
                "-filter_complex", &format!("[0:v][1:v]psnr=stats_file={}", psnr_log),
                "-filter_complex", &format!("[0:v][1:v]ssim=stats_file={}", ssim_log),
                "-f", "null", "-"
            ])
            .status()?;

        if !metrics_status.success() {
            return Err(anyhow::anyhow!("Failed to calculate video metrics"));
        }
                
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
                let remaining = &psnr_content[avg_idx+9..];
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
                let remaining = &ssim_content[all_idx+4..];
                let end_idx = remaining.find(" ").unwrap_or(10);
                let all_str = &remaining[..end_idx];
                if let Ok(value) = all_str.trim().parse::<f64>() {
                    ssim_score = value;
                }
            }
        }
        
        // Print debug info
        print_pretty!(DebugColor::Cyan, "Frame {}: VMAF = {:.2}, PSNR = {:.2}, SSIM = {:.4}", 
                frame_number, vmaf_score, psnr_avg, ssim_score);

        let metrics = FrameMetrics {
            frame_number: frame_number,
            timestamp_ms: timestamp_ms,
            vmaf: vmaf_score,
            psnr: psnr_avg,
            ssim: ssim_score,
        };
            
        // Log the metrics
        self.log_metrics(&metrics).await?;
                

        Ok(())
    }


    async fn log_metrics(&self, metrics: &FrameMetrics) -> Result<()> {
        // Get a single mutex guard and use it for both operations
        let mut guard = self.writer.lock().unwrap();
        
        // Now use the guard directly for both operations
        guard.serialize(metrics)?;
        guard.flush()?;
        
        Ok(())
    }
}
    // Add to your struct
    struct KeyframeSyncState {
        last_keyframe_id: usize,
        keyframe_timestamps: HashMap<usize, Instant>,
        frames_since_keyframe: usize,
        resync_requested: bool,
        max_drift_frames: usize,
    }

    impl Default for KeyframeSyncState {
        fn default() -> Self {
            Self {
                last_keyframe_id: 0,
                keyframe_timestamps: HashMap::new(),
                frames_since_keyframe: 0,
                resync_requested: false,
                max_drift_frames: 10, // Maximum allowed drift before forced resync
            }
        }
    }

#[allow(unused)]
pub struct XRClient {
    pub decoder_queue: DroppingVecDeque<(usize,Vec<u8>)>,

    pub outport_tracking_network: Output<MpduPacket>,

    pub input_app_video: Option<StreamReceiver<VideoPacketHeader>>,
    pub input_app_audio: Option<StreamReceiver<()>>,
    pub input_app_haptics: Option<StreamReceiver<Haptics>>,

    pub output_app_tracking_sender: Option<StreamSender<Tracking>>,  

    pub out_video_decoded: Output<Vec<u8>>,

    pub framerate: f32,
    pub last_decoded_frame_instant: TaiTime<0>,

    pub output_app_network: Output<MpduPacket>,

    pub coordinates: Coords,
    pub is_streaming: bool,

    pub frames_dropped_counter: usize,
    pub server_ip: IpAddr,

    pub streamsocket_clone: Option<StreamSocket>,

    pub decoded_frame_index: usize,
    pub t_0: TaiTime<0>, 


    pub last_tracking_time: TaiTime<0>, 

    // pub has_decoder: Option<bool>, 
    pub decoder_arc: Option<Arc<tokMutex<HevcDecoder>>>, 

    pub ref_decoder_arc: Option<Arc<tokMutex<HevcDecoder>>>, 

    pub is_decoder_ready: bool, 
    pub is_ref_decoder_ready: bool, 
    // Add these new fields:
    initialization_buffer: Vec<Vec<u8>>,  // Buffer to hold initial frames
    initialization_buffer_ref: Vec<Vec<u8>>, 

    min_buffered_frames: usize,           // Minimum frames to buffer before decoding
    dec_saw_keyframe: bool,     
    ref_saw_keyframe: bool, 

    dec_saw_keyframe_last_t: TaiTime<0>, 
    ref_saw_keyframe_last_t: TaiTime<0>, 

    stream_offset: f64, 

    metrics_logger: Option<MetricsLogger>,
    current_frame_group: Option<FrameGroup>,
    group_tx: Option<Sender<FrameGroup>>,
    enable_batch_processing: bool,
    last_cleanup_time: TaiTime<0>,
    cleanup_interval: std::time::Duration,
    group_rx: Option<Receiver<FrameGroup>>, 

    
    last_processed_frame_id: usize, // Keep track of the last processed frame ID
    missing_frames_buffer: HashMap<usize, bool>, // Track missing frames

    last_displayed_frame_id: usize, 

        // Add this to your struct
    frame_pairs: HashMap<usize, FramePair>,
    last_displayed_pair_id: usize,
    display_queue: VecDeque<usize>, // Queue of frame IDs ready to display


    keyframe_sync_state: KeyframeSyncState, 
    last_keyframe_id: usize, 



    // pub visualize_decoder_window: Option<Window>,
}
#[allow(unused)]
impl XRClient {
    pub fn new(server_ip: IpAddr, fps: f32, now: TaiTime<0>) -> Self {
        
        let (group_tx, group_rx) = bounded(5); // Buffer up to 5 groups

        Self {
            decoder_queue: DroppingVecDeque::new(DECODER_BUFFERING_FRAMES),
            outport_tracking_network: Output::default(),
            input_app_video: None,
            input_app_audio: None,
            input_app_haptics: None,

            output_app_tracking_sender: None, 
            out_video_decoded: Output::default(),
            framerate: fps,
            last_decoded_frame_instant: TaiTime::EPOCH,
            output_app_network: Output::default(),
            // output_tracking: Output::default(),
            coordinates: Coords::new(),
            is_streaming: false,
            frames_dropped_counter: 0,
            server_ip,
            streamsocket_clone: None,
            decoded_frame_index: 0,
            t_0: now, 
            last_tracking_time: now, 
            decoder_arc: None, 
            ref_decoder_arc: None, 

                        // Add these new fields:
            initialization_buffer: Vec::new(),   // Buffer to hold initial frames
            initialization_buffer_ref: Vec::new(), 
            is_decoder_ready: false,                  // Flag to track if decoder is ready
            is_ref_decoder_ready: false, 
            min_buffered_frames: 10,           // Minimum frames to buffer before decoding
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
            last_cleanup_time: now, // Use your simulator's initial time
            cleanup_interval: std::time::Duration::from_secs(8), // Clea

            last_processed_frame_id: 0,
            missing_frames_buffer: HashMap::new(),
            last_displayed_frame_id: 0, 

            frame_pairs: HashMap::new(), 
            last_displayed_pair_id: 0, 
            display_queue: VecDeque::new(), 
            keyframe_sync_state: KeyframeSyncState::default(),
            last_keyframe_id: 0, 
            // visualize_decoder_window: None,
        }
    }

    pub async fn configure_streams(&mut self, packet_size: usize ,context: &Context<Self> ) {
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
            self.streamsocket_clone = Some(stream_socket.clone());

            self.output_app_tracking_sender = Some(stream_socket.request_stream(TRACKING, self.t_0));
            

            // if self.decoder_arc.is_none(){
            //     // self.has_decoder = Some(true); 
            //     println!("INITIALIZING DECODER: FPS: {:.1}", self.framerate); 
            //     self.decoder_arc = Some(Arc::new(tokMutex::new( HevcDecoder::new(self.framerate as u32, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32))   ) ); 
            // }

            XRClient::generate_tracking_data(self, (), context).await;

            // {
            //     // retrieve video packets in RX buffer
            //     if let Some(rx_socket) = self.input_app_video.clone(){

            //             let  buffer_tx: Vec<u8> = vec![0; CAPACITY_RX_BUFFER];
            //             let  buffer_rx = buffer_tx.clone();

            //             let arc_receiver = rx_socket.network_app_interface.clone();

            //             XRClient::read_network_interface_to_app(self, (), context, buffer_rx, arc_receiver);

            //             let mut buffer_app: Vec<u8> = vec![0;MAX_PACKET_SIZE_RECV];
            //             let data_app = rx_socket.inner.lock().unwrap().recv(&mut buffer_app);

            //             println!("DATA OF APP: {:?}", &buffer_app[0..100]);
            //     }
            // }
        }
    }

    pub fn generate_tracking_data<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        
        const HEAD_ID : u64 = 555; 

        async move {
            let now = context.scheduler.time(); 

            let mut position_offset = Vec3::ZERO; 

            // let mut loop_deadline = now; 
            let mut random_position_deadlne = now; 

            // if let Some(tracking_send_socket) = self.output_app_tracking_sender.clone() {
            if self.is_streaming{

                let mut rng = StdRng::from_entropy(); 

                let yaw: f32 = rng.gen_range((-PI as f32)..(PI as f32));
                let pitch: f32 = rng.gen_range((-PI as f32)..(PI as f32));

                let orientation = Quat::from_rotation_y(yaw) * Quat::from_rotation_x(pitch); 
                let position_offset  = (Vec3::new(rand::random(), rand::random(), rand::random())
                - Vec3::ONE / 0.5)
                * 1.0;
                let position = Vec3::new(0.0, 1.82, 0.0) + position_offset;

                let track = Tracking {
                        target_timestamp: TARGET_TIMESTAMP_TRACKING, 
                        device_motions: vec![(
                            HEAD_ID,
                            DeviceMotion {
                                pose: Pose {
                                    orientation,
                                    position,
                                },
                                linear_velocity: Vec3::ZERO,
                                angular_velocity: Vec3::ZERO,
                            },
                        )],
                        ..Default::default()
                    }; 

                if let Some(mut sender) = self.output_app_tracking_sender.clone() {
            
                    let arc_inner_app_receiver = sender.app_network_interface.clone(); 
                    
                    let send_result = sender.send_header_tracking(&track, now);
                    let buffer: Vec<u8> = vec![0; CAPACITY_RX_BUFFER];
        
                    XRClient::read_app_send_network_interface(self, (), now, buffer, arc_inner_app_receiver).await; // FUNCTION TO HANDLE NETWORK PACKETS!
                }    
                
                // println!("CLIENT FRAMERATE = {}", self.framerate); 
                let loop_deadline = Duration::from_secs_f32(1.0/self.framerate / 3.0); 

                context.scheduler.schedule_event(loop_deadline, Self::generate_tracking_data, ()).unwrap();
            }       
        } 
    }

    pub async fn send_tracking(&mut self, tracking: Tracking, now:TaiTime<0> ) {
        if let Some(mut sender) = self.output_app_tracking_sender.clone() {
            
            let arc_inner_app_receiver = sender.app_network_interface.clone(); 
            
            let send_result = sender.send_header_tracking(&tracking, now);
            let buffer: Vec<u8> = vec![0; CAPACITY_RX_BUFFER];

            XRClient::read_app_send_network_interface(self, (), now, buffer, arc_inner_app_receiver).await; // FUNCTION TO HANDLE NETWORK PACKETS!
        
        
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
                            // TODO: CHECK WITH WIRESHARK ENCAPSULATION OF PACKET
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
                                    _ => "?? IDK",
                                };
                                elapsed = now.duration_since(self.t_0);
                                

                                let elapsed_tracking = now.duration_since(self.last_tracking_time).as_secs_f32(); 
                                
                                if stream_id == TRACKING{
                                    debug_print!(
                                        DebugColor::ForestGreen,
                                        "{} UL TRACKING -> Δt_tracking:{:.4} |length: {}| Stream ID: {}|",
                                        format_elapsed!(now), 
                                        elapsed_tracking, 
                                        packet_length,
                                        str_id,
                                    );
                                }
                                self.last_tracking_time = now; 

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
                                if stream_id ==TRACKING{
                                    self.outport_tracking_network.send(packet).await;
                                }

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


    pub async fn framed_send<S: Serialize>(
        &mut self,
        packet: &S,
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
        // Sends directly TCP packets related to Control. For now, just NetworkStatistics
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
        mut frames: Vec<u32>,
        mut shards_lost: Vec<usize>,
        context: &Context<Self>,
    ) {
        frames.truncate(MAX_DEADLINE_IN_STATS);
        shards_lost.truncate(MAX_DEADLINE_IN_STATS);

        // println!("REPORT FRAME LOSt");
        let net = DeadlineShardlossStatPacket {
            frame_indexes: frames,
            shards_lost: shards_lost,
        };
        context
            .scheduler
            .schedule_event(
                Duration::from_nanos(10),
                Self::output_control,
                ClientControlPacket::DeadlineShardLossStat(net),
            )
            .unwrap();
    }
    pub fn video_receive_thread<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
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
                            //     "FRAMES LOST {:?}, SHARDS LOST {:?}",
                            //     &frames_lost[..],
                            //     &shards_lost[..]
                            // );
                            XRClient::report_frame_lost(frames_lost, shards_lost, context);
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

                    let net = NetworkStatisticsPacket {
                        // Frame specific metrics
                        frame_index: frame_id as i32, // index of the current frame
                        frame_span: data.get_frame_span(),          // duration of the current frame

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
                        // tx_instant: data.get_tx_instant(),
                    };
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

                    let Ok((nal)) = data.get() else {
                        println!("UNABLE TO GET HEADER NAL? ");
                        return;
                    };
                    let sized_vec = nal[..20.min(nal.len())].to_vec();

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
    fn convert_rgb_to_u32(rgb_data: &[u8], width: usize, height: usize) -> Option<Vec<u32>> {
        if rgb_data.len() != width * height * 3 {
            println!("ERROR: Expected rgb_data size {} but got {}", 
                    width * height * 3, rgb_data.len());
            return None;
        }
        
        let mut pixels = Vec::with_capacity(width * height);
        for chunk in rgb_data.chunks_exact(3) {
            let pixel = ((chunk[0] as u32) << 16) | 
                       ((chunk[1] as u32) << 8) | 
                       (chunk[2] as u32);
            pixels.push(pixel);
        }
        
        Some(pixels)
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

    pub async fn cleanup_old_frames(&mut self, now: TaiTime<0>, ip: IpAddr) {
        // Only run cleanup at specified intervals
        let elapsed = now.duration_since(self.last_cleanup_time);
        if elapsed < self.cleanup_interval {
            return;
        }
        
        print_pretty!(DebugColor::DarkRed, "Cleaning up old VMAF analysis frames", );
        self.last_cleanup_time = now;
        
        // Calculate cutoff time (current time - retention period)
        let retention_period = self.cleanup_interval;  // Keep files for N seconds
        let cutoff_time = now - retention_period;
        
        let ref_path = format!("Video_Sink/{}/reference_rgb", ip);
        let lossy_path = format!("Video_Sink/{}/lossy_rgb", ip);

        // Clean up directories
        for dir_name in &[ ref_path, lossy_path] {
            if let Ok(mut entries) = std::fs::read_dir(dir_name) {
                while let Some(Ok(entry)) = entries.next() {
                    if let Ok(metadata) = entry.metadata() {
                        if let Ok(modified) = metadata.modified() {


                            let modified_time = std::time::SystemTime::from(modified);
                            let now_systime = std::time::SystemTime::now(); 
                            // let now_systime = std::time::SystemTime::from(now.to_system_time(32).unwrap());
                            
                            if modified_time < now_systime - retention_period {
                                if let Err(e) = std::fs::remove_file(entry.path()) {
                                    eprintln!("Failed to delete temporary file {}: {}", 
                                        entry.path().display(), e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }


    pub async fn decode_hevc_to_rgb2(&mut self, encoded_buffer: Vec<u8>, frame_index: usize, client_ip: IpAddr) -> (Vec<u8>, Vec<u32>) {
        // Validate input
        let rgb_path = format!("Video_Sink/{}/hevc_ref/{}.rgb", client_ip, frame_index);
        if std::path::Path::new(&rgb_path).exists() {
            match fs::read(&rgb_path) {
                Ok(rgb_data) if !rgb_data.is_empty() => {
                    if let Some(pixels) = convert_rgb_to_u32(&rgb_data, WIDTH_ENCODER, HEIGHT_ENCODER) {
                        return (rgb_data, pixels);
                    }
                }
                _ => {}
            }
        }
        
        let encoded_length = encoded_buffer.len();
        if encoded_buffer.is_empty() {
            println!("WARNING: Empty encoded buffer received!");
            return (Vec::new(), Vec::new());
        }
        
        {
            let mut decoders = REFERENCE_DECODERS.lock().unwrap();
            if !decoders.contains_key(&client_ip) {
                print_pretty!(DebugColor::Cyan, 
                    "Initializing reference decoder for client {}", client_ip);
                decoders.insert(
                    client_ip.clone(), 
                    HevcDecoder::new(FRAMERATE_WINDOWS as u32, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32, &format!("[REF_DECODER {}]", client_ip))
                );
            }
        }
        
        // Now use the decoder - second lock scope to minimize lock time
        let result = {
            let mut decoders = REFERENCE_DECODERS.lock().unwrap();
            if let Some(decoder) = decoders.get_mut(&client_ip) {
                // Check if this is a keyframe for logging
                let is_keyframe = decoder.contains_keyframe(&encoded_buffer);
                let frame_display = if is_keyframe { "KEYFRAME" } else { "frame" };
                print_pretty!(DebugColor::Cyan, 
                    "{} - Decoding reference HEVC {} #{} of size: {} bytes",
                    client_ip,frame_display, frame_index, encoded_length
                );
                
                // Process the frame
                decoder.process_packet(encoded_buffer);
                
                // Process any decoded frames and don't wait too long
                // This is a non-blocking call
                let frames_count = decoder.process_decoded_frames();
                // print_pretty!(DebugColor::Cyan, "Processed {} reference frames", frames_count);
                
                // Try to get a decoded frame
                if let Some(frame) = decoder.next_decoded_frame() {
                    // Convert to RGB
                    let sample = frame.clone();
                    if let Some(pixels) = convert_rgb_to_u32(&frame, WIDTH_ENCODER, HEIGHT_ENCODER) {
                        // print_pretty!(DebugColor::Green, "Successfully decoded reference frame #{}", frame_index);
                        (sample, pixels)
                    } else {
                        print_pretty!(DebugColor::Red, "Failed to convert decoded frame to RGB", );
                        (Vec::new(), Vec::new())
                    }
                } else {
                    // Don't consider this an error during the priming phase
                    if !decoder.priming_complete {
                        print_pretty!(DebugColor::Yellow, 
                            "Reference decoder still priming (processed: {}, keyframes: {})",
                            decoder.frames_processed, decoder.keyframes_seen
                        );
                    } else {
                        print_pretty!(DebugColor::Yellow, "No decoded reference frame available yet", );
                    }
                    (Vec::new(), Vec::new())
                }
            } else {
                print_pretty!(DebugColor::Red, "ERROR: Reference decoder initialization failed", );
                (Vec::new(), Vec::new())
            }
        };
        
        // Return the result
        result
    }



    pub async fn decode_hevc_to_rgb(&mut self, encoded_buffer: Vec<u8>, frame_index: usize, ip: IpAddr) -> (Vec<u8>, Vec<u32>)  {
        // Validate input
        let encoded_length = encoded_buffer.len();
        
        if encoded_buffer.is_empty() {
            println!("WARNING: Empty encoded buffer received!");
            return (Vec::new(), Vec::new());
        }
        
        // Ensure decoder is initialized
        if self.decoder_arc.is_none() {
            println!("Initializing decoder on first frame");
            let decoder = HevcDecoder::new( FRAMERATE_WINDOWS as u32, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32, &format!("[MAIN_DECODER {}]", ip)); 
            self.decoder_arc = Some(Arc::new(tokMutex::new(decoder)));
        }
        
        // Access the decoder
        if let Some(decoder_arc) = &self.decoder_arc {
            let mut decoder_guard = decoder_arc.lock().await;
            let decoder = &mut *decoder_guard;
            
            // Check if this is a keyframe for logging
            let is_keyframe = decoder.contains_keyframe(&encoded_buffer);
            let frame_display = if is_keyframe { "KEYFRAME" } else { "frame" };
            
            // println!("Decoding HEVC {} #{} of size: {} bytes", 
            //          frame_display, frame_index, encoded_length);
             // Check if decoder is ready
            
  
            // Process the frame
            decoder.process_packet(encoded_buffer);
            
            // Process any decoded frames
            decoder.process_decoded_frames();

            // Try to get a decoded frame
            if let Some(frame) = decoder.next_decoded_frame() {
                // Convert to RGB
                let sample = frame.clone(); 
                if let Some(pixels) = convert_rgb_to_u32(&frame, WIDTH_ENCODER, HEIGHT_ENCODER) {
                    // println!("✅ Successfully decoded and converted frame #{}", frame_index);
                    self.is_decoder_ready = true; 
                    return (sample, pixels);
                } else {
                    println!("ERROR: Failed to convert decoded frame to RGB");
                    return (Vec::new(), Vec::new());
                }
            } else {
                // Don't consider this an error during the priming phase
                if !decoder.priming_complete {
                    println!("Decoder still priming, frame buffered (processed: {}, keyframes: {})",
                             decoder.frames_processed, decoder.keyframes_seen);
                } else {
                    println!("No decoded frame available yet");
                }
                return (Vec::new(), Vec::new());
            }
        } else {
            println!("ERROR: Decoder not initialized properly");
            return (Vec::new(), Vec::new());
        }
    }

    pub async fn vmaf_analysis(&mut self, sample: Vec<u8>, ref_sample: Vec<u8>, now: TaiTime<0>, frame_id: usize, ip: IpAddr) -> Result<()> {
        // Skip if either sample is empty

        println!("VMAF analysis - Current frame size: {}, Reference frame size: {}", 
            sample.len(), ref_sample.len());
            println!("VMAF analysis - Current frame size: {}, Reference frame size: {}", 
            sample.len(), ref_sample.len());
        
        if sample.is_empty() || ref_sample.is_empty() {
            println!("Skipping VMAF analysis for frame {} - sample sizes: {}, ref: {}", 
                        frame_id, sample.len(), ref_sample.len());
            return Ok(());  // Return early, don't try to process empty frames
        }
    
        // Ensure metrics logger is initialized
        if self.metrics_logger.is_none() {
            match MetricsLogger::new(ip) {
                Ok(logger) => {
                    println!("Initialized metrics logger for VMAF analysis");
                    self.metrics_logger = Some(logger);
                },
                Err(e) => {
                    eprintln!("Failed to initialize metrics logger: {}", e);
                    return Ok(());
                }
            }
        }

        print_pretty!(DebugColor::ForestGreen, "Inside VMAF analysis? ", ); 
    
        // Create directories for temporary storage if they don't exist
        let base_dir = "Video_Sink";
        if let Err(e) = std::fs::create_dir_all(base_dir) {
            eprintln!("Failed to create directory {}: {}", base_dir, e);
            return Ok(());
        }
    
        // Save frames to temporary files
        let ref_path = format!("{}/{}/reference_rgb/frame_{:04}.rgb", base_dir, ip, frame_id);
        let lossy_path = format!("{}/{}/lossy_rgb/frame_{:04}.rgb", base_dir, ip, frame_id);
        print_pretty!(DebugColor::ForestGreen, "Inside VMAF analysis - Writing frames to disk", );

        // Create parent directories
        if let Err(e) = std::fs::create_dir_all(format!("{}/{}/reference_rgb", base_dir, ip)) {
            eprintln!("Failed to create reference directory: {}", e);
            return Ok(());
        }
        if let Err(e) = std::fs::create_dir_all(format!("{}/{}/lossy_rgb", base_dir, ip)) {
            eprintln!("Failed to create lossy directory: {}", e);
            return Ok(());
        }
    
        // Write frames to disk
        if let Err(e) = std::fs::write(&ref_path, &ref_sample) {
            eprintln!("Failed to write reference frame: {}", e);
            return Ok(());
        }
        if let Err(e) = std::fs::write(&lossy_path, &sample) {
            eprintln!("Failed to write lossy frame: {}", e);
            return Ok(());
        }
        print_pretty!(DebugColor::ForestGreen, "Inside VMAF analysis - Frame files written", );
    
        // Calculate timestamp in milliseconds - convert to f64 as required by process_frame_metrics
        let timestamp_ms = now.duration_since(self.t_0).as_secs_f64() * 1000.0;
        // print_pretty!(DebugColor::ForestGreen, "Inside VMAF analysis 2222 ? ", ); 

        // Process frame metrics
        if let Some(logger) = &self.metrics_logger {
            match logger.process_frame_metrics(
                frame_id as u64,
                timestamp_ms, // This is now f64 as expected
                &ref_path,
                &lossy_path
            ).await {
                Ok(_) => {
                    if frame_id % 10 == 0 {
                        println!("Processed VMAF analysis for frame {}", frame_id);
                    }
                },
                Err(e) => {
                    eprintln!("Error in VMAF analysis for frame {}: {}", frame_id, e);
                }
            }
        }
        
        // Add to frame group for batch processing if enabled
        if self.enable_batch_processing {
            if self.current_frame_group.is_none() {
                self.current_frame_group = Some(FrameGroup { 
                    frames: Vec::with_capacity(VMAF_FRAME_GROUP_SIZE) 
                });
            }
    
            if let Some(group) = &mut self.current_frame_group {
                group.frames.push(FrameData {
                    ref_rgb: ref_sample,
                    lossy_rgb: sample,
                    timestamp_ms: timestamp_ms, // Now using f64
                    frame_number: frame_id as u64,
                });
    
                // Send group when full
                if group.frames.len() >= VMAF_FRAME_GROUP_SIZE {
                    if let Some(tx) = &self.group_tx {
                        // Create a new group to send
                        let frames_to_send = std::mem::replace(&mut group.frames, Vec::with_capacity(VMAF_FRAME_GROUP_SIZE));
                        let group_to_send = FrameGroup { frames: frames_to_send };
                        
                        if let Err(e) = tx.send(group_to_send) {
                            eprintln!("Error sending frame group: {}", e);
                        }
                    }
                }
            }
        }
        print_pretty!(DebugColor::ForestGreen, "Inside VMAF analysis 33333333333333 ? ", ); 

    
        // Update clean-up timer
        self.last_cleanup_time = now;
        
        Ok(())
    }


    // Helper method to check if a frame contains a keyframe
    fn is_keyframe(&self, frame: &[u8]) -> bool {
        // Check for start code
        for i in 0..frame.len().saturating_sub(5) {
            if (frame[i] == 0 && frame[i + 1] == 0 && frame[i + 2] == 1) || 
               (frame[i] == 0 && frame[i + 1] == 0 && frame[i + 2] == 0 && frame[i + 3] == 1) {
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

    pub fn vsync<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {

                    // local Helper function to display synchronized frame pairs, it's kinda wrong/ugly but works for now
                    fn display_frame_pair(pair: &FramePair, server_ip: &IpAddr, display_frame_id: usize, now: TaiTime<0>) {
                        let decoded = pair.decoded.as_ref().unwrap();
                        let reference = pair.reference.as_ref().unwrap();

                        println!("Lengths of decoded and reference: {} | {}", decoded.len(), reference.len()); 
                        if decoded.len() == 0 || reference.len() == 0 {
                            println!("One of the frame pairs is missing, skip!"); 
                            return; 
                        }

                        let scale_factor = SCALE_FACTOR_WINDOW;
                        let scaled_width = (WIDTH_ENCODER as f64 * scale_factor) as usize;
                        let scaled_height = (HEIGHT_ENCODER as f64 * scale_factor) as usize;
                        
                        // Create a wider window to hold both frames with a separator
                        let window_width = scaled_width * 2 + 10;
                        let window_title = format!("{} - Frame Compare {}", format_elapsed!(now),server_ip);
                        
                        // Create combined buffer
                        let mut combined_buffer = vec![0u32; window_width * scaled_height];
                        
                        // Scale and combine the frames
                        let scaled_current = resize_buffer(decoded, WIDTH_ENCODER, HEIGHT_ENCODER, scaled_width, scaled_height);
                        let scaled_reference: Vec<u32> = resize_buffer(reference, WIDTH_ENCODER, HEIGHT_ENCODER, scaled_width, scaled_height);
                        
                        // Copy the scaled current frame to the left side
                        for y in 0..scaled_height {
                            for x in 0..scaled_width {
                                combined_buffer[y * window_width + x] = scaled_current[y * scaled_width + x];
                            }
                        }
                        
                        // Draw separator line
                        for y in 0..scaled_height {
                            for x in 0..10 {
                                combined_buffer[y * window_width + scaled_width + x] = 0x808080;
                            }
                        }
                        
                        // Copy the scaled reference frame to the right side
                        for y in 0..scaled_height {
                            for x in 0..scaled_width {
                                combined_buffer[y * window_width + scaled_width + 10 + x] = scaled_reference[y * scaled_width + x];
                            }
                        }
                        
                        // Add text overlays0xFF0033
                        let color = 0xFF0033; // red
                        let text_color = 0x00FF00; // green
                        

                        // Mark decoded and reference sides
                        render_text(&mut combined_buffer, "DECODED FRAME", 10, 10, window_width, text_color, 2);
                        render_text(&mut combined_buffer, "REFERENCE FRAME", scaled_width + 15, 10, window_width, text_color, 2);
                        
                        // Frame info
                        let frame_info = format!("FRAME #{}", display_frame_id);
                        render_text(&mut combined_buffer, &frame_info, 
                                  (window_width - frame_info.len() * 6 * 3) / 2,
                                  scaled_height - 25, window_width, color, 3);
                        
                        println!("Displaying synced frame #{} (window size: {}x{})", 
                                 display_frame_id, window_width, scaled_height);
                        
                        // Update the display window with improved window creation logic
                        DISPLAY_WINDOWS.with(|windows_cell| {
                            let mut windows = windows_cell.borrow_mut();
                            
                            // Create window if it doesn't exist yet
                            if !windows.contains_key(server_ip) {
                                println!("Creating new window for {}", server_ip);
                                let window = Window::new(
                                    &window_title,
                                    window_width,
                                    scaled_height,
                                    WindowOptions::default()
                                );
                                
                                if let Ok(new_window) = window {
                                    windows.insert(server_ip.clone(), new_window);
                                    println!("Successfully created window!");
                                } else {
                                    println!("Failed to create window for {}", server_ip);
                                    return; // Exit early if window creation failed
                                }
                            }
                            
                            // Update the window with the combined buffer
                            if let Some(window) = windows.get_mut(server_ip) {
                                window.set_title(&format!("{} | Frame Compare #{} - {}", format_elapsed!(now), display_frame_id, server_ip));
                                
                                if let Err(e) = window.update_with_buffer(&combined_buffer, window_width, scaled_height) {
                                    println!("Failed to update window buffer: {}", e);
                                } else {
                                    println!("Successfully updated window with frame #{}", display_frame_id);
                                }
                            } else {
                                println!("Window not found for {}", server_ip);
                            }
                        });
                    }


                let now = context.scheduler.time();
                let mut T_vsync = Duration::from_secs_f64(1.0 / self.framerate as f64);
                
                let mut is_frame_lost = false; 
                // Use a HashMap to store windows, keyed by server_ip
                thread_local! {
                    static DISPLAY_WINDOWS: RefCell<HashMap<IpAddr, Window>> = RefCell::new(HashMap::new());
                }
                self.missing_frames_buffer.retain(|&id, &mut processed| {
                    !processed || id > self.last_processed_frame_id - 1000
                });

                
                if let Some((id_f, video_frame)) = self.decoder_queue.pop() {
                    let subsample = video_frame[0..10.min(video_frame.len())].to_vec();
                    
                    let mut ip_client = self.server_ip.clone();

                    if let IpAddr::V4(mut ip4) = ip_client {
                        let mut octets = ip4.octets();
                        if octets[3] == 2 {
                            octets[3] = 1; // Change last byte from 2 to 1
                            ip_client = IpAddr::V4(std::net::Ipv4Addr::from(octets));
                        }
                    }
                    print_pretty!(DebugColor::ForestGreen, "{} Extracting ref frame {}",ip_client , id_f);                                                               

                                        // Before decoding the reference frame, ensure decoder consistency

                    if id_f > self.last_processed_frame_id + 1 {

                        let missing_start = self.last_processed_frame_id + 1;
                        let missing_end = id_f - 1;
                        
                        print_pretty!(DebugColor::Red, 
                            "{} Detected missing frames between {} and {}", 
                            ip_client, missing_start, missing_end);
                        

                        for missing_id in (self.last_processed_frame_id + 1)..id_f {
                            if !self.missing_frames_buffer.contains_key(&missing_id) {
                                print_pretty!(DebugColor::DarkOrange, 
                                    "{} Added missing frame {} to tracking system", ip_client, missing_id); 
                                self.missing_frames_buffer.insert(missing_id,  false);
                            }
                        }
                        
                        let mut next_frame_id = self.last_processed_frame_id + 1;
                        let process_limit = id_f + 3000;

                        while next_frame_id < process_limit {
                            if let Some(frame_state) = self.missing_frames_buffer.get(&next_frame_id) {
                                if *frame_state == true {
                                    // Already processed, move to next
                                    println!("continue, Next frame id = {}",next_frame_id ); 
                                    next_frame_id += 1;
                                    continue;
                                }
                            }
                            let hevc_path: String = format!("Video_Sink/{}/hevc_ref/{}.hevc", ip_client, next_frame_id);
                            // println!("[DBG1] Processing frame ID: {}", next_frame_id); 
                            if std::path::Path::new(&hevc_path).exists(){
                                if let Ok(hevc_data) =  fs::read(&hevc_path) {
                                    if !hevc_data.is_empty(){
                                        print_pretty!(DebugColor::Lime, "DECODING REFERENCE FRAME {}", next_frame_id, ); 
                                        let (rgb_ref_frame, _) = self.decode_hevc_to_rgb2(hevc_data, next_frame_id, ip_client).await;         
                                        if !rgb_ref_frame.is_empty() {
                                            let rgb_path = format!("Video_Sink/{}/hevc_ref/{}.rgb", ip_client, next_frame_id);                                                                                     // Save the decoded RGB file
                                            if let Err(e) = std::fs::write(&rgb_path, &rgb_ref_frame) {
                                                print_pretty!(DebugColor::Red, 
                                                    "Failed to save RGB for frame #{}: {}", next_frame_id, e);
                                            } else {
                                                // Mark as processed
                                                self.missing_frames_buffer.insert(next_frame_id, true);
                                                print_pretty!(DebugColor::Green, 
                                                    "Successfully processed missing frame #{}", next_frame_id);
                                            }
                                        }
                                    }
                                }
                            }     
                            // println!("[DBG2] Finished processing {}", next_frame_id); 
                            next_frame_id +=1;                    
                        } //end while
                    }
                    self.last_processed_frame_id = id_f;

                    // Reference frame handling
                    let ref_path: String = format!("Video_Sink/{}/hevc_ref/{}.rgb", ip_client, id_f); 
                    let hevc_file_path: String = format!("Video_Sink/{}/hevc_ref/{}.hevc", ip_client, id_f);

                    let mut retries = 10;
                    let ref_frame = loop {
                        match fs::read(&hevc_file_path) {
                            Ok(data) => break data, // Successfully read file
                            Err(_) if retries > 0 => {
                                thread::sleep(Duration::from_millis(100)); // Wait 100ms before retrying
                                retries -= 1;
                            }
                            Err(_) => break Vec::new() // do nothing 
                        }
                    };

                    // Keyframe detection
                    if self.is_keyframe(&video_frame) {
                        self.dec_saw_keyframe = true;
                        println!("*** KEYFRAME DETECTED DEC *** Size: {}", video_frame.len());

                        self.dec_saw_keyframe_last_t = now;
                        
             
                        // self.flush_decoders_and_buffers();
                    }

                               
                    // Buffering phase logic (existing code)
                    if !self.is_decoder_ready {
                        // Add frame to initialization buffer
                        self.initialization_buffer.push(video_frame.clone());
                        self.initialization_buffer_ref.push(ref_frame.clone()); 
                        
                        // Check if we're ready to start decoding
                        let has_enough_frames = self.initialization_buffer.len() >= self.min_buffered_frames;
                        
                        if has_enough_frames && self.dec_saw_keyframe {
                            print_pretty!(DebugColor::DarkBlue, "Decoder initialization complete! Buffered {} frames!!!",
                                    self.initialization_buffer.len());
                                    print_pretty!(DebugColor::DarkBlue, "REFERENCE decoder initialization complete! Buffered {} frames!!!",
                                    self.initialization_buffer_ref.len());
                            
                            // Process all buffered frames
                            if let Some(decoder_arc) = &self.decoder_arc {
                                let mut decoder_guard = decoder_arc.lock().await;
                                
                                for frame in &self.initialization_buffer {
                                    decoder_guard.process_packet(frame.clone());
                                    
                                    
                                    decoder_guard.process_decoded_frames(); 
                                }

                                let mut decoders = REFERENCE_DECODERS.lock().unwrap();
                                if let Some(decoder_guard_ref) = decoders.get_mut(&ip_client) {

                                    for frame in &self.initialization_buffer_ref{
                                        decoder_guard_ref.process_packet(frame.clone());
                                        
                                        decoder_guard_ref.process_decoded_frames(); 
                                    }
                                }
                            }
                            
                            // Mark decoder as ready and clear buffer
                            self.is_decoder_ready = true;
                            self.is_ref_decoder_ready = true; 
                            self.initialization_buffer.clear();
                            self.initialization_buffer_ref.clear();


                        } else {
                            println!("Buffering frame {} of {} (keyframe: {})", 
                                    self.initialization_buffer.len(), 
                                    self.min_buffered_frames,
                                    self.dec_saw_keyframe);
                        }
                    } else {
                        // Normal decoding phase
                        if let Some(interarrival) = now.checked_duration_since(self.last_decoded_frame_instant) {
                            let miin: usize = usize::min(video_frame.len(), 50);
                            print_pretty!(
                                DebugColor::Violet,
                                "{} - [DBG VSYNC {}] Frame id {} decoded OK! Size frame: {} ,Q: {}, Interarrival: {},  ok: {} | dropped: {}|\nData: {:?}", 
                                format_elapsed!(now), 
                                self.server_ip, 
                                id_f, 
                                video_frame.len(),
                                self.decoder_queue.len(),
                                interarrival.as_secs_f32(),
                                self.decoder_queue.ok_dequed_frame_counter,
                                self.decoder_queue.dropped_frame_counter,
                                &video_frame[0..miin]
                            );

                            if USE_FFMPEG == true {
                                let (rgb_ref_frame, ref_pixels) = self.decode_hevc_to_rgb2(ref_frame.clone(), id_f, ip_client).await; 
                                
                                let (rgb, frame) =                self.decode_hevc_to_rgb(video_frame.clone(), id_f, ip_client).await;


                                if !rgb_ref_frame.is_empty(){
                                    std::fs::write(&ref_path, rgb_ref_frame.clone());
                                }
                                if self.is_keyframe(&ref_frame){
                                    self.ref_saw_keyframe = true; 
                                    println!("*** KEYFRAME DETECTED REF *** Size: {}", ref_frame.len());
                                    self.ref_saw_keyframe_last_t = now;                                
                                } 

                                if now.duration_since(self.t_0) >= Duration::from_secs(13)  // just some starting time before cleaning. 
                                {
                                    self.cleanup_old_frames(now, ip_client).await; 
                                }

                                // When processing a decoded frame
                                if !frame.is_empty() {

                                    // Now check if we have a reference frame for this adjusted ID
                                    if !ref_pixels.is_empty() {
                                        if let Some(pair) = self.frame_pairs.get_mut(&id_f) {
                                            pair.reference = Some(ref_pixels.clone());
                                            print_pretty!(DebugColor::Yellow, "Updated reference for pair #{}, len = {}", id_f, ref_pixels.len());
                                        }
                                    }
                                    else if let Ok(rgb_ref_frame) = std::fs::read(&ref_path) {
                                            if let Some(pair) = self.frame_pairs.get_mut(&id_f) {
                                                pair.reference = Some(ref_pixels.clone());
                                                print_pretty!(DebugColor::Yellow, "Updated reference (from file) for pair #{}, len = {}", id_f, ref_pixels.len());
                                            
                                            }
                                    }
                                        // Create a frame pair with both frames
                                    let pair = FramePair {
                                        decoded: Some(frame.clone()),
                                        reference: Some(ref_pixels.clone()),
                                        frame_id: id_f,
                                        timestamp: Instant::now(),
           
                                    };
                                    print_pretty!(DebugColor::Lavender, "Inserting frame {} : decoded size = {}, ref size = {} ", id_f, frame.len(), ref_pixels.len()); 
                                        
                                    self.frame_pairs.insert(id_f, pair);
                                    
                                    // Perform VMAF analysis on directly matched frames
                                    if !rgb.is_empty() && !rgb_ref_frame.is_empty() && USE_VMAF {
                                    self.vmaf_analysis(
                                        rgb.clone(),
                                        rgb_ref_frame.clone(),
                                        now,
                                        id_f,
                                        ip_client
                                    ).await.unwrap_or_else(|e| {
                                        eprintln!("VMAF analysis error: {}", e);
                                    });
                                }


                                    // Display synchronized pair if both parts are available
                                    // Check both original and adjusted IDs for complete pairs
                                    if let Some(pair) = self.frame_pairs.get(&id_f) {
                                        if pair.decoded.is_some() && pair.reference.is_some()  {
                                            
                                            // Now display the synchronized pair
                                            
                                            
                                            display_frame_pair(pair, &self.server_ip, id_f, now);
                                            self.last_displayed_pair_id = id_f;
                                            // print_pretty!(DebugColor::Magenta, "{} Displayed synchronized frame #{} (offset applied)", id_f);
                                            
                                            // Cleanup old pairs to avoid memory leaks
                                            self.frame_pairs.retain(|&id, _| 
                                                id >= self.last_displayed_pair_id.saturating_sub(3000));
                                            
                 
                                        }
                                    }
                                    
                                }else {
                                    println!("Empty frame received, skipping display update");
                                }
                            }
                        }
                    }

                    self.last_decoded_frame_instant = now;
                    self.out_video_decoded.send(subsample).await;
                } else {
                    println!(
                        "Decoder queue is empty! |  queue len: {}, T_VSYNC: {:.3} ms",
                        self.decoder_queue.len(),
                        T_vsync.as_secs_f32() * 1000.0
                    );
                }

                // // T_vsync adjustment logic (existing code)
                if self.decoder_queue.len() < TARGET_FRAMES_DECODER_QUEUE {
                    T_vsync = T_vsync.mul_f64(2.0);
                    print_pretty!(
                        DebugColor::Violet,
                        "[DBG VSYNC] Doubling time ({}) until frame deque due to length ({}) UNDER target ({})",
                        T_vsync.as_secs_f32(),
                        self.decoder_queue.len(),
                        TARGET_FRAMES_DECODER_QUEUE
                    );
                } 
                //  if self.decoder_queue.len() > TARGET_FRAMES_DECODER_QUEUE {
                //     T_vsync = T_vsync.mul_f64(0.5);
                //     print_pretty!(
                //         DebugColor::Violet,
                //         "[DBG VSYNC] Dividing time ({}) until frame deque due to length ({}) OVER target ({})",
                //         T_vsync.as_secs_f32(),
                //         self.decoder_queue.len(),
                //         TARGET_FRAMES_DECODER_QUEUE
                //     );
                // }

                context
                    .scheduler
                    .schedule_event(T_vsync, Self::vsync, ())
                    .unwrap();
            }
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
                        println!("receiver: {:?}", receiver);
                    }
                }
                VIDEO => {
                    if let Some(sock) = self.input_app_video.clone() {
                        // println!("app lock");
                        let _sender = sock.network_app_interface.lock().unwrap().send(&buffer); // We send the packet from network to the application, where it needs to be now read and passed to the application!
                                                                                                // println!("reader lock");

                        if let Some(mut ssocket) = self.streamsocket_clone.as_mut() {
                            let _resulllt = StreamSocket::recv(&mut ssocket, self.server_ip, sock.inner, context);

                            // println!("result of sender {:?}", sender );
                            // println!("Result of reader? {:?}" , resulllt);
                        }
                    } else {
                        println!("NO SOME??");
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

pub struct SinkVideo_XR {
    pub counter_decoded: usize,
    pub last_update: TaiTime<0>,
    pub window_timed_fps: SlidingWindowTimely<f64>,
}
impl SinkVideo_XR {
    pub fn new() -> Self {
        Self {
            counter_decoded: (0),
            last_update: TaiTime::EPOCH,
            window_timed_fps: SlidingWindowTimely::new(1.0 / 60.0, 16., 1.0),
        }
    }
    pub async fn in_video(&mut self, input: Vec<u8>, context: &Context<Self>) {
        // Select only the first N bytes
        let now = context.scheduler.time();
        let n = 10;
        let first_n_bytes = &input[..n.min(input.len())];

        if let Some(user_interarrival) = now.checked_duration_since(self.last_update) {
            self.window_timed_fps.submit_sample(
                user_interarrival.as_secs_f64(),
                user_interarrival.as_secs_f32(),
            );
            let avg_samples = self.window_timed_fps.get_interval_buffer_mean();
            let length_samples = self.window_timed_fps.get_length();

            debug_bgprint!(DebugColor::Cyan, "[USER HMD] Reproducing video! {:?}. video_interarrival: {}, avg_fps: {}, ({:2.0} samples)", first_n_bytes, user_interarrival.as_secs_f32(), 1.0 / avg_samples,length_samples );
        }
        self.last_update = now;
        return;
    }
}
impl Model for SinkVideo_XR {}

pub trait XRDevice {
    fn some_shared_method(&self);
}
impl XRDevice for XRClient {
    fn some_shared_method(&self) {
        println!("WOWWWWW");
    }
}
impl XRDevice for XRServer {
    fn some_shared_method(&self) {
        println!("WOWZA!!");
    }
}
#[derive(Clone)]
pub struct TimedFrame {
    vec: Vec<MpduPacket>,
    timestamp: TaiTime<0>,
}

#[allow(non_camel_case_types)]
#[allow(unused)]

pub struct STA_extended {
    // extended class to PoissonGen
    pub output_network_port: Output<MpduPacket>,

    pub to_app_socket: Output<TimedFrame>,
    // pub to_app_socket_end_ampdu: Output<bool>,
    pub sta_id: i32,
    pub destination_id: i32,

    pub arrival_rate_BG: f64,
    pub mean_length_packets_BG: f64,
    pub num_packets_sent: usize,
    pub received_packet_counter: usize,

    pub sta_coordinates: Coords,
    pub does_sta_tx: bool,

    pub is_bg_sta: bool,

    pub t_0: TaiTime<0>,
}
#[allow(unused)]
impl STA_extended {
    pub fn new(
        arrival_rate_bps: f64,
        mean_length: f64,
        src: i32,
        dest: i32,
        coordinates: Coords,
        does_sta_transmit: bool,
        rate_service_bps: f64,
        t0_sim: TaiTime<0>,
        is_bg_sta: bool,
        arrival_rate_BG: f64,
    ) -> Self {
        let arrival_rate_BG_packets = arrival_rate_BG / mean_length;

        println!("\n*************************************************");
        println!("[DEBUG STA{}]\tCoordinates: {:?}\n\tDestination: STA{} | RATE_IN: {:.3} Mbps, Rate_service: {:.3} (packs/s),\n\t arrival_rate_BG (pack/s): {:.3}, Departure_rate: {:.3},  L = {}, is_BG_STA {}",
                            src, coordinates, dest, arrival_rate_bps/1E6, rate_service_bps / 1E6 , arrival_rate_BG, rate_service_bps / mean_length , mean_length, is_bg_sta);

        Self {
            output_network_port: Default::default(),

            to_app_socket: Default::default(),
            // to_app_socket_end_ampdu: Default::default(),
            sta_id: src,
            destination_id: dest,
            arrival_rate_BG: arrival_rate_BG_packets,
            mean_length_packets_BG: mean_length,
            num_packets_sent: 0,
            sta_coordinates: coordinates,

            received_packet_counter: 0,
            does_sta_tx: does_sta_transmit,
            t_0: t0_sim,
            is_bg_sta,
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
        let now = context.scheduler.time();
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
            if self.does_sta_tx && self.is_bg_sta {
                // if STA is "TX type"         (and not "RX only")

                let mut packet = MpduPacket::new();

                let mut time_interarrival =
                    Duration::from_secs_f64(exponential(1.0 / self.arrival_rate_BG));

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


// Update the process_group function to use the enhanced method
async fn process_group(group: FrameGroup, logger: &MetricsLogger) -> Result<()> {
    let temp_dir = TempDir::new()?;
    
    println!("Processing group with {} frames", group.frames.len());
    
    for frame in &group.frames {
        // Save frames to temporary files
        let ref_path = temp_dir.path().join(format!("ref_{}.rgb", frame.frame_number)).to_string_lossy().to_string();
        let lossy_path = temp_dir.path().join(format!("lossy_{}.rgb", frame.frame_number)).to_string_lossy().to_string();
        
        std::fs::write(&ref_path, &frame.ref_rgb)?;
        std::fs::write(&lossy_path, &frame.lossy_rgb)?;
        
        // Process and log metrics for this frame
        match logger.process_frame_metrics(
            frame.frame_number,
            frame.timestamp_ms,
            &ref_path,
            &lossy_path
        ).await {
            Ok(_) => {
                // Successfully processed
                println!("Processed frame {}", frame.frame_number);
            },
            Err(e) => {
                eprintln!("Error processing metrics for frame {}: {}", frame.frame_number, e);
            }
        }
    }
    
    println!("Group processing complete");
    Ok(())
}