use asynchronix::model::Context;
use crossbeam::channel::{unbounded, Receiver, RecvTimeoutError, Sender, TryRecvError};
// use futures_util::stream::empty;
use std::io::{Read, Write};
#[allow(unused_imports)]
#[allow(dead_code)]
use std::process::{Child, Command, Stdio};
use std::os::unix::io::AsRawFd;
use nix::fcntl;
use nix::fcntl::{fcntl, FcntlArg, OFlag};
use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex};
// use tokio::io::{AsyncReadExt, BufReader};
use std::io::{BufReader};
use crate::DebugColor;
use lazy_static::lazy_static;
use std::thread;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::process::ChildStdout;
use ffmpeg_sidecar::command::FfmpegCommand;
use tokio::io::{AsyncReadExt, BufReader as tokBufReader, AsyncBufReadExt}; // Import AsyncBufReadExt
use std::io::{BufRead};
use std::path::PathBuf;
use std::fs;
use tokio::sync::Mutex as tokMutex; 
use std::sync::atomic::AtomicBool; 

use std::sync::atomic::Ordering as AtOrdering;
// lazy_static! {
//     // Global static encoder instance
//     static ref HEVC_ENCODER: Mutex<Option<HevcEncoder>> = Mutex::new(None);
// }

use tokio::time::{sleep, Duration as Durtokio};


use crate::{lib::DEBUG_PRINT_ENABLED, lib::USE_FFMPEG, print_pretty};

use crate::{debug_bgprint, format_elapsed};
pub const DEADLINE_PACKETS_S: Duration = Duration::from_millis(100);
pub const MAX_DEADLINE_IN_STATS: usize = 10;
pub const OFFSET_VIDEO: f64 = 350.0;


pub const CHUNK_SIZE_FRAMES: usize = 300; 
pub const IDR_FRAME_SIZE_GOP: usize = 90; 


use rand::Rng;
use std::cell::RefCell;
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

use crate::debug_print;
use crate::lib::models_XR::{
    XRDevice,
    XRServer, // ,XRClient
    HEIGHT_ENCODER,
    WIDTH_ENCODER,
};

use crate::lib::models_XR::SHARD_PREFIX_SIZE;
// use crate::lib::DebugColor;
use anyhow::{anyhow, Result};
use glam::{Quat, Vec3};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::error::Error;
// use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr};

use std::result::Result::Ok;
use tai_time::TaiTime;

use crate::lib::alvr_packets::{DeviceMotion, Pose}; 


// use super::alvr_packets::NetworkStatisticsPacket;

// pub const UPDATE_BITRATE_INTERVAL: Duration = Duration::from_secs(1);
pub const MAX_HISTORY_SIZE: usize = 256;
pub const INITIAL_FRAMERATE_FPS: f32 = 90.0;

pub const MAX_PACKET_SIZE_RECV: usize = 2000 * 8;
pub const TRACKING: u16 = 0;
pub const HAPTICS: u16 = 1;
pub const AUDIO: u16 = 2;
pub const VIDEO: u16 = 3;
pub const STATISTICS: u16 = 4;

pub const CONTROL_STREAM: u16 = 5;

pub const _SERVER_DISCONNECTED_MESSAGE: &str = "The streamer has disconnected.";


/// Converts raw RGB byte data (3 bytes per pixel) into a Vec<u32> pixel buffer
/// where each pixel is represented as 0xRRGGBB.
fn convert_rgb_to_u32(rgb_data: &[u8], width: usize, height: usize) -> Vec<u32> {
    let expected_len = width * height * 3;
    if rgb_data.len() != expected_len {
        eprintln!(
            "Unexpected RGB data length. Expected {}, got {}",
            expected_len,
            rgb_data.len()
        );
        return Vec::new();
    }
    
    let mut pixels = Vec::with_capacity(width * height);
    for chunk in rgb_data.chunks_exact(3) {
        let pixel = ((chunk[0] as u32) << 16) | 
                   ((chunk[1] as u32) << 8) | 
                   (chunk[2] as u32);
        pixels.push(pixel);
    }

    if !pixels.is_empty() {
        // println!("First 5 pixels: {:x} {:x} {:x} {:x} {:x}", 
        //     pixels[0], pixels[1], pixels[2], pixels[3], pixels[4]);
    }
    pixels
}


fn scale_pixels(buffer: &[u32], orig_width: usize, orig_height: usize, new_width: usize, new_height: usize) -> Vec<u32> {
    let mut scaled = vec![0u32; new_width * new_height];
    let x_ratio = (orig_width << 16) / new_width;
    let y_ratio = (orig_height << 16) / new_height;

    for y in 0..new_height {
        let y2 = ((y * y_ratio) >> 16) * orig_width;
        for x in 0..new_width {
            let x2 = (x * x_ratio) >> 16;
            scaled[y * new_width + x] = buffer[y2 + x2];
        }
    }
    scaled
}



/// Represents a single HEVC NAL unit
pub struct NalUnit {
    pub nal_type: u8,
    pub data: Vec<u8>,
    pub is_keyframe: bool,
}

/// A parser for HEVC bitstreams to extract individual frames
pub struct HevcParser {
    buffer: Vec<u8>,
}

impl HevcParser {
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    /// Add more encoded data to the parser buffer
    pub fn add_data(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Find the next NAL unit start code in the buffer
    fn find_next_start_code(&self, start_pos: usize) -> Option<usize> {
        for i in start_pos..self.buffer.len() - 3 {
            // Look for 0x000001 or 0x00000001 (3 or 4 byte start codes)
            if (self.buffer[i] == 0 && self.buffer[i + 1] == 0 && self.buffer[i + 2] == 1) || 
               (i < self.buffer.len() - 4 && self.buffer[i] == 0 && self.buffer[i + 1] == 0 && 
                self.buffer[i + 2] == 0 && self.buffer[i + 3] == 1) {
                return Some(i);
            }
        }
        None
    }

    /// Extract the next complete NAL unit from the buffer
    pub fn next_nal_unit(&mut self) -> Option<NalUnit> {
        // Find the first start code
        let start_pos = self.find_next_start_code(0)?;
        
        // Determine start code length (3 or 4 bytes)
        let start_code_len = if start_pos + 3 < self.buffer.len() && self.buffer[start_pos + 2] == 0 && self.buffer[start_pos + 3] == 1 {
            4
        } else {
            3
        };
        
        // Find the next start code
        let next_start = self.find_next_start_code(start_pos + start_code_len);
        
        let (nal_end, has_next) = match next_start {
            Some(pos) => (pos, true),
            None => (self.buffer.len(), false)
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
}
/// extract the next complete encoded frame.

pub struct HevcEncoder {
    input_path: String,
    width: u32,
    height: u32,
    current_bitrate: String,
    frame_buffer: VecDeque<Vec<u8>>,  // Buffer for encoded frames
    last_keyframe: Option<Vec<u8>>,   // Store the most recent keyframe
    parser: HevcParser,
    
    // Current encoding process state
    current_child: Option<ffmpeg_sidecar::child::FfmpegChild>,
    current_packet_rx: Option<Receiver<Vec<u8>>>,
    current_stderr_handle: Option<std::thread::JoinHandle<()>>,
    
    // Position tracking
    current_position: f64,  // Current position in seconds
    next_chunk_position: f64, // Position to start the next chunk
    chunk_duration: f64,    // Duration of each chunk in seconds
    buffer_target_size: usize, // Target number of frames to keep in buffer
    buffer_min_threshold: usize, // Minimum threshold before starting next chunk
    
    // Performance tracking
    last_chunk_start_time: Option<TaiTime<0>>,
    encoding_in_progress: bool,
    stats: EncoderStats,
}

struct EncoderStats {
    chunks_encoded: usize,
    frames_produced: usize,
    buffer_underruns: usize,
    encoding_time: Duration,
    bitrate_changes: usize,
}

impl HevcEncoder {
    /// Creates a new HevcEncoder without starting the encoding process
    pub fn new(input_path: &str, width: u32, height: u32, initial_bitrate: &str, 
               chunk_duration: f64, buffer_target_size: usize) -> Result<Self> {
        // Calculate an appropriate threshold for starting the next chunk
        // Start encoding the next chunk when buffer has less than 25% of target
        let buffer_min_threshold = buffer_target_size / 4;
        
        Ok(Self {
            input_path: input_path.to_string(),
            width,
            height,
            current_bitrate: initial_bitrate.to_string(),
            frame_buffer: VecDeque::new(),
            last_keyframe: None,
            parser: HevcParser::new(),
            current_child: None,
            current_packet_rx: None,
            current_stderr_handle: None,
            current_position: 0.0,
            next_chunk_position: 0.0,
            chunk_duration,
            buffer_target_size,
            buffer_min_threshold,
            last_chunk_start_time: None,
            encoding_in_progress: false,
            stats: EncoderStats {
                chunks_encoded: 0,
                frames_produced: 0,
                buffer_underruns: 0,
                encoding_time: Duration::from_secs(0),
                bitrate_changes: 0,
            },
        })
    }
    
    /// Starts encoding a chunk of video from the current position
    pub fn start_chunk_encoding(&mut self, now: TaiTime<0>) -> Result<()> {
        // First, stop any current encoding process
        self.stop_encoding()?;
        
        let start_position = self.next_chunk_position;
        println!("Starting chunk encoding at position {:.2}s with bitrate {}", 
                 start_position, self.current_bitrate);
        
        // Track chunk start time for performance metrics
        self.last_chunk_start_time = Some(now);
        self.encoding_in_progress = true;
        
        // Create a new ffmpeg process for encoding the next chunk
        let mut child = FfmpegCommand::new()
            .hwaccel("cuda")
            .args(&["-ss", &format!("{:.3}", start_position)]) // Start from specified position
            .args(&["-t", &format!("{:.3}", self.chunk_duration)])    // Encode for chunk_duration seconds
            .input(&self.input_path)
            .args(&["-vf", &format!("scale={}:{}:force_original_aspect_ratio=disable,format=yuv420p", 
                                   self.width, self.height)])
            .args(&["-c:v", "hevc_nvenc"])
            .args(&["-preset", "p1"])      // Faster preset
            .args(&["-tune", "ull"])          // Ultra low latency tuning
            .args(&["-rc", "cbr"])
            .args(&["-b:v", &self.current_bitrate, "-maxrate", &self.current_bitrate])
            .args(&["-bufsize", &format!("{}", self.current_bitrate.replace("M", "000"))]) // Smaller buffer for more consistent bitrate
            .args(&["-rc-lookahead", "0"])    // No lookahead for lower latency
            .args(&["-g", &format!("{:.0}", IDR_FRAME_SIZE_GOP)])
            .args(&["-movflags", "+frag_keyframe+empty_moov"])
            .args(&["-flush_packets", "1"])
            .args(&["-bsf:v", "hevc_mp4toannexb"])
            .args(&["-an"])
            .args(&["-f", "hevc", "-"]) // Raw HEVC format
            .spawn()?;

        let stdout = child.take_stdout().unwrap();
        let stderr = child.take_stderr().unwrap();

        let (packet_tx, packet_rx) = unbounded();

        // Start stdout reader thread
        std::thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut buf = [0u8; 8192]; // Larger buffer for more efficient reading
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => break, // End of stream
                    Ok(n) => {
                        if packet_tx.send(buf[..n].to_vec()).is_err() {
                            break; // Channel closed
                        }
                    }
                    Err(e) => {
                        eprintln!("Encoder read error: {}", e);
                        break;
                    }
                }
            }
        });

        // Start stderr monitor thread - only capture errors
        let stderr_handle = std::thread::spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut buf = String::new();
            loop {
                buf.clear();
                match reader.read_to_string(&mut buf) {
                    Ok(0) => break,
                    Ok(_) => {
                        // Only print errors, not all stderr output
                        if buf.contains("Error") || buf.contains("error") {
                            eprint!("{}", buf);
                        }
                    }
                    Err(e) => {
                        eprintln!("Encoder stderr read error: {}", e);
                        break;
                    }
                }
            }
        });

        // Store the encoder state
        self.current_child = Some(child);
        self.current_packet_rx = Some(packet_rx);
        self.current_stderr_handle = Some(stderr_handle);
        
        // Update position for tracking
        self.next_chunk_position = start_position + self.chunk_duration;
        self.stats.chunks_encoded += 1;
        
        Ok(())
    }
    
    /// Stops the current encoding process
    pub fn stop_encoding(&mut self) -> Result<()> {
        if let Some(mut child) = self.current_child.take() {
            // Try to terminate the process gracefully
            if let Err(e) = child.kill() {
                eprintln!("Error killing ffmpeg process: {}", e);
                // Continue anyway
            }
            
            // Drop the channel receiver to close it
            self.current_packet_rx.take();
            
            // We won't wait for the stderr handle to complete
            self.current_stderr_handle.take();
            
            // Update encoding stats if we were tracking encoding time
            if let Some(start_time) = self.last_chunk_start_time.take() {
                self.stats.encoding_time += start_time.duration_since(TaiTime::EPOCH);
            }
            
            self.encoding_in_progress = false;
        }
        
        Ok(())
    }
    
    /// Changes the encoding bitrate for subsequent chunks
    pub fn set_bitrate(&mut self, new_bitrate: &str) {
        if self.current_bitrate != new_bitrate {
            println!("Changing bitrate from {} to {}", self.current_bitrate, new_bitrate);
            self.current_bitrate = new_bitrate.to_string();
            self.stats.bitrate_changes += 1;
            
            // We won't restart the encoding immediately - let the current chunk finish
            // The next chunk will use the new bitrate automatically
        }
    }
    
    /// Process available encoded packets
    pub fn process_incoming_packets(&mut self, now: TaiTime<0>) -> Result<()> {
        if let Some(packet_rx) = &self.current_packet_rx {
            let mut done = false;
            let mut packets_processed = 0;
            
            while !done {
                match packet_rx.try_recv() {
                    Ok(packet) => {
                        // Add packet data to the parser
                        self.parser.add_data(&packet);
                        packets_processed += 1;
                        
                        // Extract frames from the parser and buffer them
                        let frames = self.parser.get_frames();
                        for frame in frames {
                            // Check if this frame is a keyframe
                            let is_keyframe = Self::is_keyframe(&frame);
                            if is_keyframe {
                                self.last_keyframe = Some(frame.clone());
                            }
                            
                            self.frame_buffer.push_back(frame);
                            self.stats.frames_produced += 1;
                        }
                    },
                    Err(TryRecvError::Empty) => {
                        done = true;
                    },
                    Err(TryRecvError::Disconnected) => {
                        // Channel is closed, encoding chunk is done
                        if self.encoding_in_progress {
                            println!("Chunk encoding completed with {} packets processed", packets_processed);
                        }
                        
                        // Check if we need to start the next chunk based on buffer status
                        self.encoding_in_progress = false;
                        
                        // Update encoding stats
                        if let Some(start_time) = self.last_chunk_start_time.take() {
                            self.stats.encoding_time += start_time.duration_since(TaiTime::EPOCH);
                        }
                        
                        // Clear old encoder state
                        self.current_child.take();
                        self.current_packet_rx.take();
                        self.current_stderr_handle.take();
                        
                        return self.check_buffer_status(now);
                    },
                }
            }
        }
        
        // Check if we need to start a new chunk based on buffer status
        self.check_buffer_status(now)
    }
    
    /// Checks if we need to start encoding a new chunk based on buffer status
    fn check_buffer_status(&mut self, now:  TaiTime<0>) -> Result<()> {
        // Only start a new chunk if:
        // 1. No active encoding is in progress
        // 2. Buffer is below our minimum threshold 
        if !self.encoding_in_progress && self.frame_buffer.len() < self.buffer_min_threshold {
            println!("Buffer running low ({} frames, threshold: {}), starting new chunk", 
                    self.frame_buffer.len(), self.buffer_min_threshold);
            
            if self.frame_buffer.is_empty() {
                self.stats.buffer_underruns += 1;
            }
            
            self.start_chunk_encoding(now)?;
        }
        
        Ok(())
    }
    
    /// Resets the encoder to start from the beginning of the video
    pub fn reset_to_beginning(&mut self) -> Result<()> {
        self.stop_encoding()?;
        self.current_position = 0.0;
        self.next_chunk_position = 0.0;
        self.frame_buffer.clear();
        self.last_keyframe = None;
        
        Ok(())
    }
    
    /// Get the next frame from the buffer
    pub fn next_frame(&mut self, now: TaiTime<0>) -> Option<Vec<u8>> {
        let frame = self.frame_buffer.pop_front();
        
        // Each time we remove a frame, check if we need to refill the buffer
        // This check ensures we maintain a continuous supply of frames
        if let Err(e) = self.check_buffer_status(now) {
            eprintln!("Error checking buffer status: {}", e);
        }
        
        frame
    }
    
    /// Get the latest keyframe (useful for recovery after packet loss)
    pub fn get_latest_keyframe(&self) -> Option<Vec<u8>> {
        self.last_keyframe.clone()
    }
    
    /// Number of frames waiting in the buffer
    pub fn frames_available(&self) -> usize {
        self.frame_buffer.len()
    }
    
    /// Check if a frame contains a keyframe
    fn is_keyframe(frame: &[u8]) -> bool {
        // Same implementation as before
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
    
    /// Gets the current timestamp position in the video
    pub fn get_current_position(&self) -> f64 {
        self.current_position
    }
    
    /// Prints encoder statistics
    pub fn print_stats(&self) {
        println!("===== Encoder Statistics =====");
        println!("Chunks encoded: {}", self.stats.chunks_encoded);
        println!("Frames produced: {}", self.stats.frames_produced);
        println!("Buffer underruns: {}", self.stats.buffer_underruns);
        println!("Bitrate changes: {}", self.stats.bitrate_changes);
        println!("Total encoding time: {:.2}s", self.stats.encoding_time.as_secs_f64());
        println!("Current buffer size: {}", self.frame_buffer.len());
        println!("============================");
    }
}


  

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
    deadline: Option<TaiTime<0>>,
    num_shards_expected: usize,
    id_frame: u32,
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
    frame_first_shard_deadline: Option<TaiTime<0>>,
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
}
#[allow(unused)]
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
            encoder_wrapper: None, 
            chunk_frames: VecDeque::new(), 
            is_initializing_encoder: Arc::new(AtomicBool::new(false)), 
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
        let mut total_lost_deadline = 0;
        // Collect keys into a vector before modifying the map
        let keys: Vec<_> = self.lost_shards_deadline_map.keys().cloned().collect();

        let mut vec_keys = vec![];
        let mut vec_lost = vec![];

        // Now you can iterate over the keys and remove them from the map
        for frame_deadlined in keys {
    
            vec_keys.push(frame_deadlined);

            let lost_in_frame = self
                .lost_shards_deadline_map
                .remove(&frame_deadlined)
                .unwrap();
            // println!("LOST {} packets in frame {}", self.lost_shards_deadline_map.get(&frame_deadlined).unwrap(), frame_deadlined);
            debug_bgprint!(
                DebugColor::Red,
                "[Flush deadline] Packets lost in frame {}: {:?}",
                frame_deadlined,
                lost_in_frame
            );
            vec_lost.push(lost_in_frame);
            total_lost_deadline += lost_in_frame;
        }
        (vec_keys, vec_lost)
    }

    pub fn recv<T: XRDevice + asynchronix::model::Model>(
        &mut self,
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

            self.shard_recv_state.insert(RecvState {
                shard_length,
                stream_id,
                packet_index,
                shards_count,
                shard_index,
                packet_cursor: 0,
                overwritten_data_backup: None,
                should_discard: false,
                frame_first_shard_deadline: None,
            })
        };

        if shard_recv_state_mut.frame_first_shard_deadline.is_none() {
            shard_recv_state_mut.frame_first_shard_deadline = now.checked_add(DEADLINE_PACKETS_S);
        }

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

        debug_print!( DebugColor::Orange, "{:.9} [DBG StreamSocket RX] frame_id: {} deadline_current: {:?} in_progress_packets: {:?}, indices {:?}, shard: {:2.0} / {:2.0}" ,format_elapsed!(now) ,shard_recv_state_mut.packet_index, format_elapsed!(shard_recv_state_mut.frame_first_shard_deadline.unwrap()), components.in_progress_packets.len(), components.in_progress_packets.keys(), shard_recv_state_mut.shard_index, shard_recv_state_mut.shards_count - 1);

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
                    println!("First fallback");
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
                    println!(
                        "Warning: Creating new emergency buffer - consider increasing buffer pool"
                    );
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
                    deadline: shard_recv_state_mut.frame_first_shard_deadline,
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
            debug_print!(DebugColor::Orange, "FRAME IS COMPLETE!",);
            if shard_recv_state_mut.stream_id == VIDEO {
                if let Some(inner_map) = self.map_rx.get(&shard_recv_state_mut.packet_index) {
                    // println!("Retrieved from innermap, got {}",shard_recv_state_mut.packet_index);

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
                    lost_shards_deadline_map: HashMap::new(),
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
            lost_shards_deadline_map: HashMap::new(),
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

    chunk_frames: VecDeque<Vec<u8>>,

    // is_initializing_encoder: Arc<AtomicBool>,

    encoder_wrapper: Option<Arc<tokMutex<HevcEncoder>>>,
    
    // Keep the initialization flag:
    is_initializing_encoder: Arc<AtomicBool>,

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
   
    pub async fn get_buffer_emu(
        &mut self,
        header: &H,
        current_bitrate_mbps: f32,
        now: TaiTime<0>,
        ip: IpAddr,
    ) -> Result<Buffer<H>> {
        let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/bbb_1080p60fps.mp4";
        let mut buffer: Vec<u8> = Vec::new();
        
        if USE_FFMPEG {
            // Initialize the encoder if it doesn't exist yet
            if self.encoder_wrapper.is_none() {
                // Try to set the initialization flag atomically
                let was_initializing = self.is_initializing_encoder.compare_exchange(
                    false, true, AtOrdering::Acquire, AtOrdering::Relaxed
                ).is_ok();
                
                // Only proceed with initialization if we successfully set the flag
                if was_initializing {
                    // Format bitrate string (convert from Mbps to appropriately formatted string)
                    let bitrate_str = format!("{}M", current_bitrate_mbps);
                    println!("Initializing HEVC encoder with bitrate: {}", bitrate_str);
                    
                    // Create a new HevcEncoder with optimized parameters
                    let encoder = match HevcEncoder::new(
                        input_path, 
                        WIDTH_ENCODER as u32, 
                        HEIGHT_ENCODER as u32,
                        &bitrate_str,
                        5.0, // 5-second chunks
                        300  // Buffer target size
                    ) {
                        Ok(mut encoder) => {
                            // Start the initial encoding process
                            if let Err(e) = encoder.start_chunk_encoding(now) {
                                // Reset flag and return error if start fails
                                self.is_initializing_encoder.store(false, AtOrdering::Release);
                                return Err(anyhow::anyhow!("Failed to start encoding: {}", e));
                            }
                            
                            // *** IMPORTANT ADDITION: Wait for frames to be available ***
                            println!("Waiting for frames to be available...");
                            let wait_start = std::time::Instant::now();
                            let timeout = std::time::Duration::from_secs(30); // 30-second timeout for first frames
                            
                            // Process packets until we have frames or timeout
                            while encoder.frames_available() == 0 {
                                if wait_start.elapsed() > timeout {
                                    self.is_initializing_encoder.store(false, AtOrdering::Release);
                                    return Err(anyhow::anyhow!("Timed out waiting for encoder to produce frames"));
                                }
                                
                                if let Err(e) = encoder.process_incoming_packets(now) {
                                    eprintln!("Error processing packets during initialization: {}", e);
                                }
                                
                                // Short sleep to avoid tight loop
                                std::thread::sleep(Duration::from_millis(50));
                            }
                            
                            println!("Encoder has buffered {} frames", encoder.frames_available());
                            encoder
                        },
                        Err(e) => {
                            // Reset the flag if initialization fails
                            self.is_initializing_encoder.store(false, AtOrdering::Release);
                            return Err(anyhow::anyhow!("Failed to initialize encoder: {}", e));
                        }
                    };
                    
                    // Wrap encoder in Arc and Mutex for thread-safe access
                    self.encoder_wrapper = Some(Arc::new(tokMutex::new(encoder)));
                    
                    // Reset the flag once initialization is complete
                    self.is_initializing_encoder.store(false, AtOrdering::Release);
                    
                    println!("HEVC encoder initialization complete");
                } else {
                    // Wait for the other thread to complete initialization
                    let wait_start = std::time::Instant::now();
                    let timeout = std::time::Duration::from_secs(40); // 40-second timeout
                    
                    while self.is_initializing_encoder.load(AtOrdering::Relaxed) {
                        // Check for timeout
                        if wait_start.elapsed() > timeout {
                            eprintln!("Timed out waiting for encoder initialization by another thread");
                            // Instead of breaking, use fallback buffer
                            buffer = generate_fibonacci_video_payload(current_bitrate_mbps);
                            
                            // Prepare and return the buffer with header
                            let header_size = bincode::serialized_size(header)? as usize;
                            let hidden_offset = SHARD_PREFIX_SIZE + header_size;
                            
                            if buffer.len() < hidden_offset {
                                buffer.resize(hidden_offset, 0);
                            }
                            
                            let buffer_len = buffer.len();
                            self.next_packet_index += 1;
                            
                            return Ok(Buffer {
                                inner: buffer,
                                hidden_offset,
                                length: buffer_len,
                                _phantom: PhantomData,
                            });
                        }
                        std::thread::sleep(Duration::from_millis(100)); // Longer sleep to reduce CPU usage
                    }
                    
                    // After waiting, if the encoder is still not initialized, use fallback
                    if self.encoder_wrapper.is_none() {
                        eprintln!("Encoder wasn't initialized by other thread, using fallback");
                        buffer = generate_fibonacci_video_payload(current_bitrate_mbps);
                        
                        // Prepare and return the buffer with header
                        let header_size = bincode::serialized_size(header)? as usize;
                        let hidden_offset = SHARD_PREFIX_SIZE + header_size;
                        
                        if buffer.len() < hidden_offset {
                            buffer.resize(hidden_offset, 0);
                        }
                        
                        let buffer_len = buffer.len();
                        self.next_packet_index += 1;
                        
                        return Ok(Buffer {
                            inner: buffer,
                            hidden_offset,
                            length: buffer_len,
                            _phantom: PhantomData,
                        });
                    }
                }
            }
            
            // Check if bitrate needs updating
            if let Some(encoder_arc) = &self.encoder_wrapper {
                // Convert current bitrate to string format
                let bitrate_str = format!("{}M", current_bitrate_mbps);
                
                // Lock the encoder to check/update bitrate
                let mut encoder_guard = encoder_arc.lock().await;
                
                // Update bitrate if it's different from the current one
                if encoder_guard.current_bitrate != bitrate_str {
                    println!("Updating encoder bitrate from {} to {}", 
                             encoder_guard.current_bitrate, bitrate_str);
                    encoder_guard.set_bitrate(&bitrate_str);
                }
                
                // Process any incoming packets to keep the buffer filled
                if let Err(e) = encoder_guard.process_incoming_packets(now) {
                    eprintln!("Error processing encoder packets: {}", e);
                }
                
                // Try to get a frame from the buffer
                if let Some(frame) = encoder_guard.next_frame(now) {
                    buffer = frame;
                    // Debug info showing frame retrieved
                    println!("Frame retrieved, size: {} bytes, frames remaining: {}", 
                             buffer.len(), encoder_guard.frames_available());
                } else {
                    // If no frames are available, try to process more packets with a more aggressive approach
                    println!("No frames immediately available, processing more packets...");
                    
                    // Try multiple times to process packets and get a frame
                    for retry in 1..=5 {
                        if let Err(e) = encoder_guard.process_incoming_packets(now) {
                            eprintln!("Error processing encoder packets (retry {}): {}", retry, e);
                        }
                        
                        // Try to get a frame after processing
                        if let Some(frame) = encoder_guard.next_frame(now) {
                            buffer = frame;
                            println!("Frame retrieved on retry {}, size: {} bytes", retry, buffer.len());
                            break;
                        }
                        
                        // Short sleep between retries
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    
                    // If we still don't have a frame, try keyframe or fallback
                    if buffer.is_empty() {
                        // Last resort: try to get a keyframe if available
                        if let Some(keyframe) = encoder_guard.get_latest_keyframe() {
                            println!("No regular frames available, using keyframe");
                            buffer = keyframe;
                        } else {
                            // No frames available at all, use fallback instead of error
                            eprintln!("No frames available from encoder, using fallback");
                            // Drop the lock before generating fallback content
                            drop(encoder_guard);
                            buffer = generate_fibonacci_video_payload(current_bitrate_mbps);
                        }
                    }
                }
                
                // Make sure to drop the lock if we haven't already
                // drop(encoder_guard);
            }
        } else {
            // Fallback for non-FFMPEG mode
            buffer = generate_fibonacci_video_payload(current_bitrate_mbps);
        }
        
        // Ensure buffer has space for header
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

    pub fn get_buffer_tracking(&mut self, header: &H, now: TaiTime<0>) -> Result<Buffer<H>> {
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
    pub fn send_header_tracking(&mut self, header: &H, now: TaiTime<0>) -> Result<()>{
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

pub fn generate_fibonacci_video_payload(current_bitrate_mbps: f32) -> Vec<u8> {
    // Calculate the payload size based on bitrate
    let no_bytes_based_bitrate = (1416.97 * current_bitrate_mbps + -810.06) as usize;

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

#[rustfmt::skip]
pub fn generate_sample_ffmpeg(current_bitrate_mbps: f32, timestamp: f64, fps: f64) -> Vec<u8> {
    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/sample_short.mp4";
    let hours = (timestamp / 3600.0) as u32;
    let minutes = ((timestamp % 3600.0) / 60.0) as u32;
    let seconds = timestamp % 60.0;
    let formatted_timestamp = format!("{:02}:{:02}:{:06.3}", hours, minutes, seconds - 10.0);

    let current_bitrate_mbps = current_bitrate_mbps / 90.0; // fps

    print_pretty!(DebugColor::ForestGreen, "T_VIDEO={}", formatted_timestamp);

    // First pass: Analysis (multi-pass encoding)
    let first_pass_log = "/tmp/ffmpeg_first_pass.log";
    let mut first_pass = Command::new("ffmpeg")
        .args([
            "-hwaccel",
            "cuda",
            "-ss",
            &formatted_timestamp,
            "-i",
            input_path,
            "-pix_fmt",
            "yuv420p",
            "-vf",
            &format!("scale={}:{},format=yuv420p", WIDTH_ENCODER, HEIGHT_ENCODER),
            "-c:v",
            "hevc_nvenc",
            "-b:v",
            &format!("{:.0}K", current_bitrate_mbps as f64 * 1000.0),
            "-preset", "medium", // Higher quality preset
            "-rc", "vbr_hq", // Variable Bitrate High Quality mode
            "-cq", "19", // Constant Quality level (lower is higher quality)
            "-b_ref_mode", "2", // Enable B-frame reference mode
            "-bf", "3", // Number of B-frames (0-3)
            "-temporal-aq", "1", // Temporal Adaptive Quantization
            "-spatial-aq", "1", // Spatial Adaptive Quantization
            "-aq-strength", "8", // Adaptive Quantization strength
            "-frames:v", "1",
            "-an",              // no audio 
            "-pass", "1", 
            "-passlogfile", first_pass_log,
            "-f", "null",
            "/dev/null",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn first pass FFMPEG");

    let first_pass_status = first_pass.wait().expect("Failed to wait for first pass");

    if !first_pass_status.success() {
        eprintln!("First pass encoding failed");
        return Vec::new();
    }

    // Second pass: Actual encoding with analysis from first pass
    let mut ffmpeg = Command::new("ffmpeg")
        .args([
            "-hwaccel",
            "cuda",
            "-ss",
            &formatted_timestamp,
            "-i",
            input_path,
            "-pix_fmt",
            "yuv420p",
            "-vf",
            &format!("scale={}:{},format=yuv420p", WIDTH_ENCODER, HEIGHT_ENCODER),
            "-c:v",
            "hevc_nvenc",
            "-b:v",
            &format!("{:.0}K", current_bitrate_mbps as f64 * 1000.0),
            "-preset",
            "medium", // Higher quality preset
            "-rc",
            "vbr_hq", // Variable Bitrate High Quality mode
            "-cq",
            "19", // Constant Quality level (lower is higher quality)
            "-b_ref_mode",
            "2", // Enable B-frame reference mode
            "-bf",
            "3", // Number of B-frames (0-3)
            "-temporal-aq",
            "1", // Temporal Adaptive Quantization
            "-spatial-aq",
            "1", // Spatial Adaptive Quantization
            "-aq-strength",
            "8", // Adaptive Quantization strength
            "-frames:v",
            "1",
            "-an",
            "-pass",
            "2",
            "-passlogfile",
            first_pass_log,
            "-f",
            "mp4", // Output as mp4 container
            "-bsf:v",
            "hevc_mp4toannexb", // Crucial: Add this filter
            "-movflags",
            "+frag_keyframe+empty_moov",
            "-",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn second pass FFMPEG");

    let mut ffmpeg_stdout = ffmpeg.stdout.take().unwrap();
    let mut buf = Vec::new();
    ffmpeg_stdout
        .read_to_end(&mut buf); 
        // .expect("Failed to read encoded buffer")

    // Save for debugging
    std::fs::write("sample_frame_encoded.hevc", &buf.clone()).expect("Failed to write debug file");

    print_pretty!(
        DebugColor::Salmon,
        "Encoded frame size: {} bytes ({} KB)\nData = {:?}",
        buf.len(),
        buf.len() / 1024,
        &buf[..200]
    );

    buf
}

// pub fn generate_all_bitrates_all_frames(list_bitrates: Vec<f32>, fps: f64) {

//     let MAX_DURATION_MOVIE = 30.0;

//     for bitrate in list_bitrates{

//         let num_frames = MAX_DURATION_MOVIE * fps;
//         for frame in 0..num_frames as usize {

//         }
//         println!("Encoding movie with bitrate: {}", bitrate);

//         let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/sample_short.mp4";

//         let output_path = format!("/home/boris/Desktop/Rust_MG1/asynchronix/temp_video_bitrates/f{}_{}.mp4",  ,bitrate);

//     }

// }

// A structure to hold the process and its output buffering channel.
struct EncoderBuffer {
    process: Child,
    frame_rx: Receiver<Vec<u8>>,
}
lazy_static! {
    // Thread-safe FFmpeg process pool
    static ref FFMPEG_ENCODE_POOL: Arc<Mutex<HashMap<String, Child>>> = Arc::new(Mutex::new(HashMap::new()));
    // static ref FFMPEG_DECODE_POOL: Arc<Mutex<HashMap<String, Child>>> = Arc::new(Mutex::new(HashMap::new()));
}



#[rustfmt::skip]
pub fn generate_sample_ffmpeg_opti(current_bitrate_mbps: f32, timestamp: f64, fps: f64, ip: IpAddr) -> Vec<u8> {
    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/bbb_sunflower_2160p_60fps_stereo_abl.mp4";
    // given this sample video, choosing 100

    // Create a unique key for this specific encoding configuration
    let config_key = format!(
        "{}_{}_{}_{}_{}",
        input_path, current_bitrate_mbps, timestamp, WIDTH_ENCODER, HEIGHT_ENCODER,
    );

    let offset_video = 
        {
            let mut hasher = DefaultHasher::new();
            ip.hash(&mut hasher);
            let hash = hasher.finish(); 
            (hash%500) as f64
        }; // keep offset of video between 0-500

    // How many times to retry before giving up
    let max_retries = 3;
    let mut attempt = 0;

    loop {
        {
            // Lock the pool and either get an existing process or create a new one.
            let mut pool = FFMPEG_ENCODE_POOL.lock().unwrap();
            if !pool.contains_key(&config_key) {
                let timestamp_ = timestamp + offset_video;
                let hours = (timestamp_ / 3600.0) as u32;
                let minutes = ((timestamp_ % 3600.0) / 60.0) as u32;
                let seconds = timestamp_ % 60.0;
                let formatted_timestamp = format!("{:02}:{:02}:{:06.3}", hours, minutes, seconds);
                print_pretty!(DebugColor::ForestGreen, "T_VIDEO={}", formatted_timestamp);

                let bitrate_command = format!("{:.0}K", current_bitrate_mbps as f64 * 1000.0);

                print_pretty!(
                    DebugColor::DarkBlue,
                    "[DBG bitrate] frame: {}, per second: {}; command {}",
                    current_bitrate_mbps,
                    current_bitrate_mbps * INITIAL_FRAMERATE_FPS,
                    bitrate_command
                );

               
                let process = Command::new("ffmpeg")
                .args([
                        "-hwaccel", "cuda",
                        "-ss", &formatted_timestamp,
                        "-i", input_path,
                        "-pix_fmt", "yuv420p",
                        "-vf", &format!("scale={}:{},format=yuv420p", WIDTH_ENCODER, HEIGHT_ENCODER),
                        "-c:v", "hevc_nvenc",
                        "-preset", "fast",
                        "-rc", "cbr",
                        "-b_ref_mode", "2",
                        "-bf", "3",
                        "-temporal-aq", "1",
                        "-spatial-aq", "1",
                        "-aq-strength", "8",
                        "-frames:v", "1",
                        "-b:v", &bitrate_command,
                        "-an", // no audio
                        "-f", "mp4", 
                        "-bsf:v", "hevc_mp4toannexb", // to follow GoP 
                        "-movflags", "+frag_keyframe+empty_moov", // don't remember why
                        "-", // out through stdout
                    ])
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .expect("Failed to spawn FFmpeg encoder");

                pool.insert(config_key.clone(), process);
            }
        } // End of pool lock

        // Read the encoded buffer.
        let mut buf = Vec::new();
        {
            // Lock again to get a mutable reference to the process.
            let mut pool = FFMPEG_ENCODE_POOL.lock().unwrap();
            let ffmpeg = pool.get_mut(&config_key).expect("Process not found in pool");
            ffmpeg
                .stdout
                .as_mut()
                .unwrap()
                .read_to_end(&mut buf)
                .expect("Failed to read encoded buffer");
        }

        print_pretty!(
            DebugColor::Salmon,
            "Encoded frame size: {} bytes ({} KB)\nData = {:?}",
            buf.len(),
            buf.len() / 1024,
            &buf[..std::cmp::min(50, buf.len())]
        );

        if !buf.is_empty() {
            return buf;
        } else {
            attempt += 1;
            if attempt >= max_retries {
                panic!("Failed to generate a non-empty encoded frame after {} attempts", max_retries);
            }
            // Remove the problematic process so that a new one is spawned next time.
            let mut pool = FFMPEG_ENCODE_POOL.lock().unwrap();
            pool.remove(&config_key);
            print_pretty!(
                DebugColor::Red,
                "Encoded frame is empty, retrying (attempt {}/{})",
                attempt,
                max_retries
            );
        }
    }
}


pub fn generate_random_video_payload(current_bitrate_mbps: f32) -> Vec<u8> {
    // Initialize the random number generator
    let mut rng = rand::thread_rng();

    // Generate a random u8
    let _random_u8: u8 = rng.gen();

    // Calculate the payload size based on bitrate
    let no_bytes_based_bitrate = (1416.97 * current_bitrate_mbps + -810.06) as usize;

    // Create buffer with random values
    let buffer_inner = vec![7; no_bytes_based_bitrate]; // TODO: MAKE EACH RANDOM; NOW FOR DBG is 7!

    buffer_inner
}
