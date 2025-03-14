#![allow(warnings)]

use rand_distr::{Distribution, Normal};
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::str::FromStr;
use std::{
    fs::write,
    io,
    process::{ChildStdin, ChildStdout, Stdio},
};
// First, let's define the missing utility functions and structures

use std::sync::atomic::{AtomicBool};

use crossbeam::channel::{unbounded, bounded, TryRecvError};
use async_std;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use colored::Colorize;

use anyhow::Result;
use async_std::stream::StreamExt; // Add this import to fix the .next() error
use std::net::Ipv4Addr;
use tempfile::TempDir;
use tokio::sync::Mutex as tokMutex; 

use std::cell::RefCell;
use std::error::Error;
use std::fs;
use std::io::{BufReader, BufWriter, Read, Write};
use std::process::{Child, Command};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread_local;

// use crate::{print_pretty, print_prettyy}; 
use minifb::{Window, WindowOptions};
use std::{fs::File, thread, write};


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

use dashmap::DashMap;

use std::cmp::{self, max};
use std::collections::VecDeque;
use std::f64::consts::PI;
use std::future::Future;
use asynchronix::model::{Context, Model};
use asynchronix::ports::Output;
use std::collections::HashMap;
use std::sync::RwLock;

// use async_process::Child;
use lazy_static::lazy_static;


use std::collections::BTreeMap;

// use super::alvr_packets::NetworkStatisticsPacket;

// pub const UPDATE_BITRATE_INTERVAL: Duration = Duration::from_secs(1);
pub const MAX_HISTORY_SIZE: usize = 256;
pub const INITIAL_FRAMERATE_FPS: f32 = 90.0;


pub const CHUNK_DURATION_F64_s: f64 = 1.5;
pub const DEADLINE_PACKETS_S: Duration = Duration::from_millis(100);
pub const MAX_DEADLINE_IN_STATS: usize = 10;
pub const OFFSET_VIDEO: f64 = 300.0;


// pub const CHUNK_SIZE_FRAMES: usize = 300; 
pub const IDR_FRAME_SIZE_GOP: usize = 120; 

pub const MAX_PACKET_SIZE_RECV: usize = 2000 * 8;
pub const TRACKING: u16 = 0;
pub const HAPTICS: u16 = 1;
pub const AUDIO: u16 = 2;
pub const VIDEO: u16 = 3;
pub const STATISTICS: u16 = 4;

pub const CONTROL_STREAM: u16 = 5;

pub const _SERVER_DISCONNECTED_MESSAGE: &str = "The streamer has disconnected.";

pub const WIDTH_ENCODER: usize = 1920;
pub const HEIGHT_ENCODER: usize = 1080;


pub const FRAMERATE_WINDOWS: usize =  60;  

pub const SCALE_FACTOR_WINDOW: f64 = 0.55;
pub const VMAF_BATCH_SIZE: usize = 10;  // Process 10 frames at a time
pub const VMAF_BATCH_TIMEOUT_MS: u64 = 1000;  // Process batch after 1 second even if


pub const SHARD_PREFIX_SIZE: usize = mem::size_of::<u32>() // packet length - field itself (4 bytes)
    + mem::size_of::<u16>() // stream ID
    + mem::size_of::<u32>() // packet index
    + mem::size_of::<u32>() // shards count
    + mem::size_of::<u32>() // shards index
    + mem::size_of::<f32>(); // tx relative timestamp


const DEBUG_PRINT_ENABLED: bool = true; 

macro_rules! print_prettyy {
    ($color:expr, $fmt:expr, $($arg:tt)*) => {
        if DEBUG_PRINT_ENABLED == true {
            let msg = format!($fmt, $($arg)*);
            println!("{}", $color.to_background_fn()(msg));
        }
    };
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


// Define a specialized function for updating the main window directly
fn display_frame_pair_to_window(pair: &FramePair, server_ip: &IpAddr, display_frame_id: usize, window: &mut Window) -> bool {
    let decoded = match &pair.decoded {
        Some(frame) => frame,
        None => {
            eprintln!("ERROR: Decoded frame missing, cannot display", );
            return false;
        }
    };
    
    let reference = match &pair.reference {
        Some(frame) => frame,
        None => {
            eprintln!("ERROR: Reference frame missing, cannot display", );
            return false;
        }
    };

    // Log frame dimensions for diagnostic purposes
    println!("Processing frame #{} for display: decoded={} pixels, reference={} pixels", 
             display_frame_id, decoded.len(), reference.len());
    
    // Calculate dimensions with careful attention to scaling and alignment
    let scale_factor = SCALE_FACTOR_WINDOW;
    let scaled_width = (WIDTH_ENCODER as f64 * scale_factor) as usize;
    let scaled_height = (HEIGHT_ENCODER as f64 * scale_factor) as usize;
    let window_width = scaled_width * 2 + 10; // Two frames plus separator
    
    // Pre-allocate buffer with exact capacity to avoid reallocation
    let mut combined_buffer = vec![0u32; window_width * scaled_height];
    
    // Create scaled versions of each frame
    let scaled_current = resize_buffer(decoded, WIDTH_ENCODER, HEIGHT_ENCODER, scaled_width, scaled_height);
    let scaled_reference = resize_buffer(reference, WIDTH_ENCODER, HEIGHT_ENCODER, scaled_width, scaled_height);
    
    // Assemble composite frame with careful bounds checking
    for y in 0..scaled_height {
        // Left frame (low bitrate)
        for x in 0..scaled_width {
            let src_idx = y * scaled_width + x;
            let dst_idx = y * window_width + x;
            if src_idx < scaled_current.len() && dst_idx < combined_buffer.len() {
                combined_buffer[dst_idx] = scaled_current[src_idx];
            }
        }
        
        // Separator between frames
        for x in 0..10 {
            let idx = y * window_width + scaled_width + x;
            if idx < combined_buffer.len() {
                combined_buffer[idx] = 0x404040; // Darker gray for better visibility
            }
        }
        
        // Right frame (high bitrate reference)
        for x in 0..scaled_width {
            let src_idx = y * scaled_width + x;
            let dst_idx = y * window_width + scaled_width + 10 + x;
            if src_idx < scaled_reference.len() && dst_idx < combined_buffer.len() {
                combined_buffer[dst_idx] = scaled_reference[src_idx];
            }
        }
    }
    
    // Add text overlays for clear frame identification
    let text_color = 0x00FF00;  // bright green for high visibility
    let highlight_color = 0xFF0033;  // bright red for emphasis
    
    render_text(&mut combined_buffer, "LOW BITRATE (5 Mbps)", 10, 10, window_width, text_color, 2);
    render_text(&mut combined_buffer, "HIGH BITRATE (100 Mbps)", scaled_width + 20, 10, window_width, text_color, 2);
    
    // Display frame ID with proper centering
    let frame_info = format!("FRAME #{}", display_frame_id);
    let text_x = (window_width - frame_info.len() * 6 * 2) / 2;
    render_text(&mut combined_buffer, &frame_info, text_x, scaled_height - 20, window_width, highlight_color, 2);
    
    // Update window title with precise frame information
    window.set_title(&format!("HEVC Comparison - Frame #{} ({}x{})", 
                             display_frame_id, scaled_width*2+10, scaled_height));
    
    // Critical operation: Update the window buffer with our composite frame
    match window.update_with_buffer(&combined_buffer, window_width, scaled_height) {
        Ok(_) => {
            println!("✅ Successfully rendered frame #{} to window", display_frame_id);
            true
        },
        Err(e) => {
            eprintln!("❌ Buffer update failed for frame #{}: {}", display_frame_id, e);
            false
        }
    }
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
            DebugColor::SaddleBrown => |s| s.truecolor(139,69, 19),
            DebugColor::Tan => |s| s.truecolor(160,82, 45),

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
            DebugColor::SaddleBrown => |s| s.truecolor(139,69, 19),
            DebugColor::Tan => |s| s.truecolor(160,82, 45),


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

pub struct NalUnit {
    pub nal_type: u8,
    pub data: Vec<u8>,
    pub is_keyframe: bool,
}

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

        if !self.buffer.is_empty(){
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
        else{
            println!("PARSER BUFFER EMPTTTTTTTTTTTY" ); 
            None
        }
    }

    
    
    pub fn clear(&mut self) {
        println!("Clearing parser buffer: {} bytes", self.buffer.len());
        self.buffer.clear();
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
        
        // Store parameter sets based on NAL type
        match nal_type {
            32 => { // VPS
                let full_nal = self.create_full_nal(&self.buffer[start_pos..nal_end]);
                self.vps = Some(full_nal);
            },
            33 => { // SPS
                let full_nal = self.create_full_nal(&self.buffer[start_pos..nal_end]);
                self.sps = Some(full_nal);
            },
            34 => { // PPS
                let full_nal = self.create_full_nal(&self.buffer[start_pos..nal_end]);
                self.pps = Some(full_nal);
            },
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

    pub fn update_vps(&mut self, vps: &Vec<u8>){
        self.vps = Some(vps.clone()); 
    }
    pub fn update_sps(&mut self, sps: &Vec<u8>){
        self.sps = Some(sps.clone()); 
    }
    pub fn update_pps(&mut self, pps: &Vec<u8>){
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
    pub fn update_from_decoder(&self, decoder_id: &str, vps: Option<Vec<u8>>, 
                              sps: Option<Vec<u8>>, pps: Option<Vec<u8>>) -> bool {
        // Only accept updates from primary decoder or if we have no sets yet
        let is_primary = decoder_id == self.primary_decoder;
        let should_update = is_primary || 
                           (self.vps.read().unwrap().is_none() && 
                            self.sps.read().unwrap().is_none() && 
                            self.pps.read().unwrap().is_none());
                            
        if should_update {
            let mut updated = false;
            
            if let Some(vps_data) = vps {
                if vps_data.len() > 8 { // Reasonable minimum size
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
            self.last_keyframe_size.store(keyframe_size, Ordering::SeqCst);
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


pub struct ChunkedHevcEncoder {
    input: String,
    width: u32,
    height: u32,
    bitrate: String,
    chunk_duration: f64,   // Duration of each chunk in seconds.
    current_offset: f64,   // Current start timestamp.
    frame_tx: crossbeam::channel::Sender<Vec<u8>>,
    frame_rx: crossbeam::channel::Receiver<Vec<u8>>,

    frame_queue: VecDeque<Vec<u8>>,  // Add this new field for queuing frames
    parser: HevcParser, 
    encoder_str: String, 

}

impl ChunkedHevcEncoder {
    /// Create a new ChunkedHevcEncoder.
    pub fn new(input: &str, width: u32, height: u32, bitrate: &str, chunk_duration: f64, string: String, offset_video: f64) -> Self {
        // We use a bounded channel to store parsed frames.
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
            frame_queue: VecDeque::new(),  // Initialize the queue
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
    pub async fn start_chunking(&mut self, bitrate_mbps: f32)  {

        self.bitrate = format!("{:.2}M", bitrate_mbps);

        println!("{} CHUNKING with bitrate {}!", self.encoder_str, self.bitrate); 
        self.parser.buffer.clear();
        let mut command = FfmpegCommand::new();
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
            .args(&["-g", &format!("{:.0}", IDR_FRAME_SIZE_GOP)])  // using your GOP size constant
            .args(&["-movflags", "+frag_keyframe+empty_moov"])
            .args(&["-flush_packets", "1"])
            .args(&["-bsf:v", "hevc_mp4toannexb"])
            .args(&["-an"])
            .args(&["-f", "hevc", "-"]); // output raw HEVC

        // Spawn the ffmpeg process for this chunk.
        let mut child = command.spawn().unwrap();
        let stdout = child.take_stdout().unwrap();
        let mut reader = BufReader::new(stdout);

        // let mut parser = HevcParser::new();
        let mut buf = [0u8; 4096];
        print!("{} SPAWN CHUNK...", self.encoder_str ,); 
        // let loop_limit = 1000000;
        let mut i = 0;  
        // Read data from the process until it ends.
        loop {
            // println!("loop {}", i);
            // i += 1; 
            // if i > loop_limit {
            //     i = 0; 
            //     print!("BREAK\n"); 
            //     break; 
            // }
            match reader.read(&mut buf) {
                Ok(0) => break, // end of chunk
                Ok(n) => {
                    self.parser.add_data(&buf[..n]);
                    // Extract complete frames and send them on the channel.
                    let frames = self.parser.get_frames();
                    for frame in frames {
                        if let Err(e) = self.frame_tx.send(frame) {
                            eprintln!("{} Error sending frame: {}", e, self.encoder_str ,);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("{} Error reading ffmpeg chunk: {}", e, self.encoder_str ,);
                    break;
                }
            }
        }
        // let _ = child.wait();
        let _ = child.wait();

        // Update offset for the next chunk.
        self.current_offset += self.chunk_duration;
            // Add a safety check to clear parser buffer if it gets too large
        if self.parser.buffer.len() > 1_000_000_00 {  // 100MB limit
            println!("{} Parser buffer getting too large ({}), clearing", self.parser.buffer.len(), self.encoder_str ,);
            self.parser.buffer.clear();
        }

    }
    pub async fn next_frame(&mut self) -> Option<Vec<u8>> {

        let extracted_frames = self.parser.get_frames();
        if !extracted_frames.is_empty() {
            println!("{} Extracted {} frames from parser buffer, size {}", self.encoder_str , 
                     extracted_frames.len(), self.parser.buffer.len());
            
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




pub struct HevcDecoder {
    frame_rx: crossbeam::channel::Receiver<Vec<u8>>,
    packet_tx:crossbeam::channel::Sender<Vec<u8>>,
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

     // New fields for synchronization
    // shared_params: Option<Arc<SharedParameterSetManager>>,
    last_sync_generation: u64,
    force_keyframe_sync: bool,

    recovery_frames: usize, // Counter for frames to skip during recovery
    pending_clear: bool,    // Flag to indicate decoder state should be reset
    initialization_phase: bool, // Flag for the decoder's initialization phase

}


/// Standalone function for finding the next NAL start code in a buffer
pub fn find_next_start_code(buffer: &[u8], start_pos: usize) -> Option<usize> {
    for i in start_pos..buffer.len().saturating_sub(3) {
        // Look for 0x000001 or 0x00000001 (3 or 4 byte start codes)
        if (buffer[i] == 0 && buffer[i + 1] == 0 && buffer[i + 2] == 1) || 
           (i < buffer.len() - 4 && buffer[i] == 0 && buffer[i + 1] == 0 && 
            buffer[i + 2] == 0 && buffer[i + 3] == 1) {
            return Some(i);
        }
    }
    None
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
            .args(&["-tune", "zerolatency"])
            // .args(&["-preset", "ultrafast"])
            // .args(&["-vsync", "passthrough"])

            .args(&["-f", "rawvideo", "-"])
            .spawn().unwrap();

        let stdout = child.take_stdout().unwrap();
        let stdin = child.take_stdin().unwrap();
        let stderr = child.take_stderr().unwrap();
        

        let (frame_tx, frame_rx) = unbounded::<Vec<u8>>();
        let (packet_tx, packet_rx) = bounded::<Vec<u8>>(100);
        
        let mut initialization_complete = false;
        // if let Some(shared) = &shared_params {
        //     let (vps, sps, pps) = shared.get_parameter_sets();
        //     if vps.is_some() && sps.is_some() && pps.is_some() {
        //         initialization_complete = true;
        //         print_prettyy!(DebugColor::Navy, "INITIALIZEED HEVC DECODER!", ); 
        //     }
        // }

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
            max_buffered_frames: 2, 

            decoder_string: decoder_str.to_string(), 

            // shared_params,
            last_sync_generation: 0,
            force_keyframe_sync: false,

            recovery_frames: 0, // Counter for frames to skip during recovery
            pending_clear: false,    // Flag to indicate decoder state should be reset
            initialization_phase: false, // Flag for the decoder's initialization phase  
        }
    }

    // Basic NAL-based keyframe detection
    fn detect_keyframe_nal(&self, buffer: &[u8]) -> bool {
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

      // Enhanced keyframe detection with validation
    pub fn contains_keyframe(&self, buffer: &[u8]) -> bool {
    // Basic HEVC keyframe detection first
        let is_keyframe_nal = self.detect_keyframe_nal(buffer);
        
        if is_keyframe_nal {
            return true;
        }
        
        false
    }

    

    pub fn inject_parameter_sets(&mut self, vps: Option<Vec<u8>>, sps: Option<Vec<u8>>, pps: Option<Vec<u8>>) {
        // Prevent recursive parameter set injection
        static INJECTION_DEPTH: AtomicUsize = AtomicUsize::new(0);
        
        // Increment depth counter and get current value
        let depth = INJECTION_DEPTH.fetch_add(1, Ordering::SeqCst);
        
        // Guard against excessive recursion (more than 2 levels deep)
        if depth > 2 {
            println!("{} ⚠️ Preventing recursive parameter set injection (depth: {})", 
                     self.decoder_string, depth);
            INJECTION_DEPTH.fetch_sub(1, Ordering::SeqCst);
            return;
        }
        
        // Print injection information only for the first level
        if depth == 0 {
            if let Some(vps_data) = &vps {
                println!("{} Injecting VPS ({} bytes)", self.decoder_string, vps_data.len());
            }
            
            if let Some(sps_data) = &sps {
                println!("{} Injecting SPS ({} bytes)", self.decoder_string, sps_data.len());
            }
            
            if let Some(pps_data) = &pps {
                println!("{} Injecting PPS ({} bytes)", self.decoder_string, pps_data.len());
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
                    println!("{} ERROR: Failed to send VPS to decoder: {}", self.decoder_string, e);
                }
            }
        }
        
        if let Some(sps_data) = sps {
            was_processed = true;
            self.parser.update_sps(&sps_data);
            
            if depth == 0 {
                if let Err(e) = self.packet_tx.send(sps_data) {
                    println!("{} ERROR: Failed to send SPS to decoder: {}", self.decoder_string, e);
                }
            }
        }
        
        if let Some(pps_data) = pps {
            was_processed = true;
            self.parser.update_pps(&pps_data);
            
            if depth == 0 {
                if let Err(e) = self.packet_tx.send(pps_data) {
                    println!("{} ERROR: Failed to send PPS to decoder: {}", self.decoder_string, e);
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
    pub fn extract_complete_parameter_sets(&self, buffer: &[u8]) -> (Option<Vec<u8>>, Option<Vec<u8>>, Option<Vec<u8>>) {
        let mut temp_parser = HevcParser::new();
        temp_parser.add_data(buffer);
        
        let mut vps_packet = None;
        let mut sps_packet = None;
        let mut pps_packet = None;
        
        let mut start_pos = 0;
        
        // Extract complete NAL units with start codes
        while let Some(pos) = find_next_start_code(buffer, start_pos) {
            // Determine start code length (3 or 4 bytes)
            let start_code_len = if pos + 3 < buffer.len() && buffer[pos + 2] == 0 && buffer[pos + 3] == 1 {
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
            let next_pos = find_next_start_code(buffer, pos + start_code_len).unwrap_or(buffer.len());
            
            // Extract the complete NAL unit with start code
            match nal_type {
                32 => { // VPS
                    vps_packet = Some(buffer[pos..next_pos].to_vec());
                },
                33 => { // SPS
                    sps_packet = Some(buffer[pos..next_pos].to_vec());
                },
                34 => { // PPS
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
        
        if has_param_sets {
            has_parameter_update = true;
            
            print_prettyy!(DebugColor::Cyan, 
                "{} - Parameter sets found in packet: VPS: {}, SPS: {}, PPS: {}", 
                self.decoder_string, 
                vps.as_ref().map_or(0, |v| v.len()),
                sps.as_ref().map_or(0, |v| v.len()),
                pps.as_ref().map_or(0, |v| v.len()),);
            
        }
        
        // Record frame metrics
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
            
            // Reset recovery status on keyframe
            self.recovery_frames = 0;
            self.pending_clear = false;
            
            print_prettyy!(DebugColor::Magenta, 
                "{} 🔑 KEYFRAME detected (size: {} bytes)", 
                self.decoder_string, packet.len(),);
            
        }
        
        // Calculate smoothing factor for EWMA
        let alpha = 0.1; // Use a fixed alpha for simplicity
        self.ewma_frame_size = alpha * (frame_size as f64) + (1.0 - alpha) * self.ewma_frame_size;
        
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
                    print_prettyy!(DebugColor::Red, 
                        "{} ERROR: Failed to send packet to decoder: {}", 
                        self.decoder_string, e,);
                }
            } else if self.recovery_frames == 0 {
                // End of recovery period, start sending frames again
                if let Err(e) = self.packet_tx.send(packet) {
                    print_prettyy!(DebugColor::Red, 
                        "{} ERROR: Failed to send packet to decoder: {}", 
                        self.decoder_string, e,);
                }
                
                print_prettyy!(DebugColor::Green, 
                    "{} - Recovery complete, resuming normal operation", 
                    self.decoder_string,);
            }
            // Otherwise silently drop frames during recovery
        } else {
            // Forward packet to ffmpeg decoder in normal mode
            if let Err(e) = self.packet_tx.send(packet) {
                print_prettyy!(DebugColor::Red, 
                    "{} ERROR: Failed to send packet to decoder: {}", 
                    self.decoder_string, e,);
            }
        }
        
        // After parameter update, enter recovery mode if not already there
        if has_parameter_update && self.recovery_frames == 0 && !is_keyframe {
            self.recovery_frames = 30; // Skip ~30 frames or until next keyframe
            print_prettyy!(DebugColor::Yellow, 
                "{} - Parameter update detected, entering recovery mode for {} frames", 
                self.decoder_string, self.recovery_frames,);
        }
        
        // Check for decoder priming completion
        if !self.priming_complete && self.keyframes_seen >= 2 && self.frames_processed >= 60 {
            print_prettyy!(DebugColor::Blue, 
                "{} 🚀 Decoder priming complete! Processed {} frames including {} keyframes", 
                self.decoder_string, self.frames_processed, self.keyframes_seen,);
            self.priming_complete = true;
        }
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
    
    /// Extract and print VPS, SPS, and PPS from a packet
    pub fn extract_parameter_sets(&mut self, packet: &[u8]) {
        // Create a temporary parser just for this packet to avoid disturbing the main parser state
        let mut temp_parser = HevcParser::new();
        temp_parser.add_data(packet);
        
        // Process all possible NAL units in this packet
        while let Some(nal) = temp_parser.next_nal_unit() {
            match nal.nal_type {
                32 => println!("{} 📋 Found VPS NAL unit (size: {})", self.decoder_string, nal.data.len()),
                33 => println!("{} 📋 Found SPS NAL unit (size: {})", self.decoder_string, nal.data.len()),
                34 => println!("{} 📋 Found PPS NAL unit (size: {})", self.decoder_string, nal.data.len()),
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
            pps.map(|p| p.clone())
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
            let mut green_dominant_regions = 0;
            let mut total_regions = 0;
            
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
                    
                    total_regions += 1;
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
                    print_prettyy!(DebugColor::Red, 
                        "{} - Discarding corrupt frame detected during initialization", 
                        self.decoder_string,);
                        
                    // Force entry into recovery mode if we detect corruption
                    if self.recovery_frames == 0 {
                        self.recovery_frames = 5;
                    
                    }
                    
                    return None;
                }
            }
            
            // Frame is good
            Some((frame, inst))
        } else {
            if frames_added > 0 && !self.decoded_frames.is_empty() {
                // Strange case: we added frames but now buffer is empty?
                print_prettyy!(DebugColor::Yellow, 
                    "{} Strange: Added frames but buffer is now empty?", 
                    self.decoder_string,);
            }
            
            // No frames available
            None
        }
    }
        

}












// Thread-local storage for display windows
thread_local! {
    static DISPLAY_WINDOWS: RefCell<HashMap<IpAddr, minifb::Window>> = RefCell::new(HashMap::new());
}

// Structure to hold a pair of decoded and reference frames
#[derive(Default, Clone)]
struct FramePair {
    decoded: Option<Vec<u32>>,
    reference: Option<Vec<u32>>,
    decoded_raw: Option<Vec<u8>>,
    reference_raw: Option<Vec<u8>>,
    frame_id: usize,
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
// Resize a buffer of u32 pixels to a new resolution
fn resize_buffer(buffer: &[u32], old_width: usize, old_height: usize, 
                new_width: usize, new_height: usize) -> Vec<u32> {
    let mut result = vec![0u32; new_width * new_height];
    
    for y in 0..new_height {
        for x in 0..new_width {
            let src_x = (x * old_width) / new_width;
            let src_y = (y * old_height) / new_height;
            let src_idx = src_y * old_width + src_x;
            let dst_idx = y * new_width + x;
            
            if src_idx < buffer.len() && dst_idx < result.len() {
                result[dst_idx] = buffer[src_idx];
            }
        }
    }
    
    result
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


// Utility function to format elapsed time (for window titles)
macro_rules! format_elapsed {
    ($start:expr) => {{
        let elapsed = Instant::now().duration_since($start);
        let secs = elapsed.as_secs();
        let millis = elapsed.subsec_millis();
        format!("{}:{:02}.{:03}", secs / 60, secs % 60, millis)
    }};
}

// Process a single frame through the decoder
fn decode_frame(decoder: &mut HevcDecoder, encoded_buffer: Vec<u8>, frame_index: usize, ip: &IpAddr) -> (Vec<u8>, Vec<u32>, Option<Instant>) {
    let is_keyframe = decoder.contains_keyframe(&encoded_buffer);
    let frame_display = if is_keyframe { "KEYFRAME" } else { "frame" };
    
    print_prettyy!(DebugColor::Cyan, "[XRClient DECODE {}] Processing {} #{} (size: {} bytes)", 
                 ip, frame_display, frame_index, encoded_buffer.len(), );
    
    // Process the frame
    decoder.process_packet(encoded_buffer);
    
    // Process any decoded frames
    decoder.process_decoded_frames();

    // Try to get a decoded frame
    if let Some((frame, inst)) = decoder.next_decoded_frame() {
        // Convert to RGB
        let sample = frame.clone(); 
        if let Some(pixels) = convert_rgb_to_u32(&frame, WIDTH_ENCODER, HEIGHT_ENCODER) {
            print_prettyy!(DebugColor::Green, "[XRClient DECODE {}] ✅ Successfully decoded frame #{}", 
                         ip, frame_index, );
            return (sample, pixels, Some(inst));
        } else {
            print_prettyy!(DebugColor::Red, "[XRClient DECODE {}] ERROR: Failed to convert decoded frame to RGB", 
                         ip, );
            return (Vec::new(), Vec::new(), None);
        }
    } else {
        // Don't consider this an error during the priming phase
        if !decoder.priming_complete {
            print_prettyy!(DebugColor::Yellow, 
                         "[XRClient DECODER {}] Decoder still priming, frame buffered (processed: {}, keyframes: {})",
                         ip, decoder.frames_processed, decoder.keyframes_seen, );
        } else {
            print_prettyy!(DebugColor::Red, "[XRClient DECODE {}] No decoded frame available yet", ip, );
        }
        return (Vec::new(), Vec::new(), None);
    }
}

// Display a pair of frames side by side
fn display_frame_pair(pair: &FramePair, server_ip: &IpAddr, display_frame_id: usize) {
    let decoded = match &pair.decoded {
        Some(frame) => frame,
        None => {
            print_prettyy!(DebugColor::Red, "Decoded frame is missing, cannot display", );
            return;
        }
    };
    
    let reference = match &pair.reference {
        Some(frame) => frame,
        None => {
            print_prettyy!(DebugColor::Red, "Reference frame is missing, cannot display", );
            return;
        }
    };

    print_prettyy!(DebugColor::Cyan, "Displaying frame pair #{} (decoded: {} pixels, reference: {} pixels)", 
                 display_frame_id, decoded.len(), reference.len(), );
    
    let scale_factor = SCALE_FACTOR_WINDOW;
    let scaled_width = (WIDTH_ENCODER as f64 * scale_factor) as usize;
    let scaled_height = (HEIGHT_ENCODER as f64 * scale_factor) as usize;
    
    // Create a wider window to hold both frames with a separator
    let window_width = scaled_width * 2 + 10;
    let window_title = format!("Frame Compare - {}", server_ip);
    
    // Create combined buffer
    let mut combined_buffer = vec![0u32; window_width * scaled_height];
    
    // Scale and combine the frames
    let scaled_current = resize_buffer(decoded, WIDTH_ENCODER, HEIGHT_ENCODER, scaled_width, scaled_height);
    let scaled_reference = resize_buffer(reference, WIDTH_ENCODER, HEIGHT_ENCODER, scaled_width, scaled_height);
    
    // Copy the scaled current frame to the left side
    for y in 0..scaled_height {
        for x in 0..scaled_width {
            let src_idx = y * scaled_width + x;
            let dst_idx = y * window_width + x;
            if src_idx < scaled_current.len() && dst_idx < combined_buffer.len() {
                combined_buffer[dst_idx] = scaled_current[src_idx];
            }
        }
    }
    
    // Draw separator line
    for y in 0..scaled_height {
        for x in 0..10 {
            let idx = y * window_width + scaled_width + x;
            if idx < combined_buffer.len() {
                combined_buffer[idx] = 0x808080;
            }
        }
    }
    
    // Copy the scaled reference frame to the right side
    for y in 0..scaled_height {
        for x in 0..scaled_width {
            let src_idx = y * scaled_width + x;
            let dst_idx = y * window_width + scaled_width + 10 + x;
            if src_idx < scaled_reference.len() && dst_idx < combined_buffer.len() {
                combined_buffer[dst_idx] = scaled_reference[src_idx];
            }
        }
    }
    
    // Add text overlays
    let text_color = 0x00FF00; // green
    let highlight_color = 0xFF0033; // red
    
    // Mark decoded and reference sides
    render_text(&mut combined_buffer, "DECODED FRAME", 10, 10, window_width, text_color, 3);
    render_text(&mut combined_buffer, "REFERENCE FRAME", scaled_width + 15, 10, window_width, text_color, 3);
    
    // Frame info
    let frame_info = format!("FRAME #{}", display_frame_id);
    render_text(&mut combined_buffer, &frame_info, 
               (window_width - frame_info.len() * 6 * 3) / 2,
               scaled_height - 25, window_width, highlight_color, 3);
    
    print_prettyy!(DebugColor::Green, "Displaying synced frame #{} (window size: {}x{})", 
                 display_frame_id, window_width, scaled_height, );
    
    // Update the display window with improved window creation logic
    DISPLAY_WINDOWS.with(|windows_cell| {
        let mut windows = windows_cell.borrow_mut();
        
        // Create window if it doesn't exist yet
        if !windows.contains_key(server_ip) {
            print_prettyy!(DebugColor::Blue, "Creating new window for {}", server_ip, );
            let window = Window::new(
                &window_title,
                window_width,
                scaled_height,
                WindowOptions::default()
            );
            
            if let Ok(new_window) = window {
                windows.insert(server_ip.clone(), new_window);
                print_prettyy!(DebugColor::Green, "Successfully created window!", );
            } else {
                print_prettyy!(DebugColor::Red, "Failed to create window for {}", server_ip, );
                return; // Exit early if window creation failed
            }
        }
        
        // Update the window with the combined buffer
        if let Some(window) = windows.get_mut(server_ip) {
            window.set_title(&format!("{} | Frame Compare #{}", display_frame_id, server_ip));
            
            if let Err(e) = window.update_with_buffer(&combined_buffer, window_width, scaled_height) {
                print_prettyy!(DebugColor::Red, "Failed to update window buffer: {}", e, );
            } else {
                print_prettyy!(DebugColor::Green, "Successfully updated window with frame #{}", display_frame_id, );
            }
        } else {
            print_prettyy!(DebugColor::Red, "Window not found for {}", server_ip, );
        }
    });
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Define configuration parameters
    let max_bitrate_ladder_mbps = 100.0; 
    let current_bitrate_mbps = 5.0; 
    let max_frames = 300;
    
    let maxbitrate_cmd = format!("{:.1}M", max_bitrate_ladder_mbps);
    let bitrate_cmd = format!("{:.1}M", current_bitrate_mbps);
    let random_offset = rand::thread_rng().gen_range(50.0..OFFSET_VIDEO);
    
    // Define path to the input video
    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/cut_video.mp4";
    
    // Define network endpoints
    let server_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let client_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
    
    // Signal for stopping threads
    let running = Arc::new(AtomicBool::new(true));
    
    // Create channels for frame transmission
    let (regular_tx, regular_rx) = crossbeam::channel::bounded::<(Vec<u8>, usize)>(30);
    let (max_tx, max_rx) = crossbeam::channel::bounded::<(Vec<u8>, usize)>(30);
    
    // Create frame pair channel for window display
    // This is the key channel that bridges the async processing world with the UI world
    let (pair_tx, pair_rx) = crossbeam::channel::bounded::<FramePair>(10);
    
    println!("Starting HEVC encoding-decoding pipeline");
    let mut diagnostic_timer = Instant::now();
    let diagnostic_interval = Duration::from_secs(1);

    // Initialize encoders
    let mut encoder = ChunkedHevcEncoder::new(
        input_path,
        WIDTH_ENCODER as u32,
        HEIGHT_ENCODER as u32,
        &bitrate_cmd,
        CHUNK_DURATION_F64_s,
        format!("[ENCODER {}]", server_ip),
        random_offset,
    );
    
    let mut max_encoder = ChunkedHevcEncoder::new(
        input_path,
        WIDTH_ENCODER as u32,
        HEIGHT_ENCODER as u32,
        &maxbitrate_cmd,
        CHUNK_DURATION_F64_s,
        format!("[Bitrate MAX ENCODER {}]", server_ip),
        random_offset,
    );
    
    // Start async components in separate threads
    
    // Encoder thread
    let running_encoder = running.clone();
    let encoder_thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        
        rt.block_on(async {
            let mut frame_id = 0;
            
            // Start initial chunking
            println!("Starting initial encoding chunks");
            max_encoder.start_chunking(max_bitrate_ladder_mbps).await;
            encoder.start_chunking(current_bitrate_mbps).await;
            
            while running_encoder.load(Ordering::SeqCst) && frame_id < max_frames {
                // First try to get a frame from the regular encoder
                match encoder.next_frame().await {
                    Some(frame) => {
                        // Save frame to disk if needed (debug only)
                        // ...
                        
                        // Send frame to decoder
                        if let Err(e) = regular_tx.send((frame, frame_id)) {
                            eprintln!("Failed to send regular frame to channel: {}", e);
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(16)).await;

                    },
                    None => {
                        println!("No regular frame available, restarting encoder");
                        encoder.parser.buffer.clear();
                        encoder.frame_queue.clear();
                        encoder.start_chunking(current_bitrate_mbps).await;
                        
                        // Brief delay to let encoder produce frames
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }
                
                // Then try to get a frame from the max bitrate encoder
                match max_encoder.next_frame().await {
                    Some(frame) => {
                        // Save frame to disk if needed (debug only)
                        // ...
                        
                        // Send frame to decoder
                        if let Err(e) = max_tx.send((frame, frame_id)) {
                            eprintln!("Failed to send max frame to channel: {}", e);
                            break;
                        }
                    },
                    None => {
                        println!("No max frame available, restarting max encoder");
                        max_encoder.parser.buffer.clear();
                        max_encoder.frame_queue.clear();
                        max_encoder.start_chunking(max_bitrate_ladder_mbps).await;
                        
                        // Brief delay to let encoder produce frames
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }
                
                // Increment frame ID after successfully processing both frames
                frame_id += 1;
                
                // Brief delay to maintain reasonable frame rate
                tokio::time::sleep(Duration::from_millis(33)).await;
            }
            
            println!("Encoder thread completed after {} frames", frame_id);
        });
    });
    
    // Decoder thread
    let running_decoder = running.clone();
    let decoder_thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        
        rt.block_on(async {
            // Initialize decoders
            let mut decoder = HevcDecoder::new(
                FRAMERATE_WINDOWS as u32, 
                WIDTH_ENCODER as u32, 
                HEIGHT_ENCODER as u32, 
                &format!("[CLIENT_DECODER_REGULAR {}]", client_ip)
            );
            
            let mut max_decoder = HevcDecoder::new(
                FRAMERATE_WINDOWS as u32, 
                WIDTH_ENCODER as u32, 
                HEIGHT_ENCODER as u32, 
                &format!("[CLIENT_DECODER_MAX {}]", client_ip)
            );
            
            // Maps to track frame ordering
            let mut regular_frames = HashMap::new();
            let mut max_frames = HashMap::new();
            let mut next_display_id = 0;
            
            while running_decoder.load(Ordering::SeqCst) {
                // Process incoming frames without blocking the async runtime
                let process_frames = async {
                    // Process regular frames
                    match regular_rx.try_recv() {
                        Ok((frame, frame_id)) => {
                            println!("Received regular frame #{} (size: {})", frame_id, frame.len());
                            let (raw, pixels, timestamp) = decode_frame(&mut decoder, frame, frame_id, &client_ip);
                            
                            if !pixels.is_empty() && timestamp.is_some() {
                                // Store decoded frame
                                let mut pair = regular_frames.entry(frame_id)
                                    .or_insert_with(FramePair::default);
                                pair.decoded = Some(pixels);
                                pair.decoded_raw = Some(raw);
                                pair.frame_id = frame_id;
                                
                                println!("Stored decoded regular frame #{}", frame_id);
                            }
                        },
                        Err(crossbeam::channel::TryRecvError::Empty) => {},
                        Err(e) => {
                            eprintln!("Error receiving regular frame: {}", e);
                            return false;
                        }
                    }
                    
                    // Process max bitrate frames
                    match max_rx.try_recv() {
                        Ok((frame, frame_id)) => {
                            println!("Received max frame #{} (size: {})", frame_id, frame.len());
                            let (raw, pixels, timestamp) = decode_frame(&mut max_decoder, frame, frame_id, &client_ip);
                            
                            if !pixels.is_empty() && timestamp.is_some() {
                                // Store reference frame
                                let mut pair = max_frames.entry(frame_id)
                                    .or_insert_with(FramePair::default);
                                pair.reference = Some(pixels);
                                pair.reference_raw = Some(raw);
                                pair.frame_id = frame_id;
                                
                                println!("Stored reference max frame #{}", frame_id);
                            }
                        },
                        Err(crossbeam::channel::TryRecvError::Empty) => {},
                        Err(e) => {
                            eprintln!("Error receiving max frame: {}", e);
                            return false;
                        }
                    }
                    
                    true
                };
                
                // If processing fails, exit the loop
                if !process_frames.await {
                    break;
                }
                // Add this code block right before frame pairing logic
                if next_display_id == 0 && !regular_frames.is_empty() && !max_frames.is_empty() {
                    // First attempt at frame pairing - synchronize the IDs
                    next_display_id = synchronize_frame_ids(&regular_frames, &max_frames, next_display_id);
                    println!("Initial frame synchronization: next_display_id now set to {}", next_display_id);
                }
                // Check if we can create complete pairs
                while let Some(reg_pair) = regular_frames.remove(&next_display_id) {
                    if let Some(max_pair) = max_frames.remove(&next_display_id) {
                        // Combine the pairs
                        let complete_pair = FramePair {
                            decoded: reg_pair.decoded,
                            reference: max_pair.reference,
                            decoded_raw: reg_pair.decoded_raw,
                            reference_raw: max_pair.reference_raw,
                            frame_id: next_display_id,
                        };
                        
                        // Send to display thread
                        if let Err(e) = pair_tx.send(complete_pair) {
                            eprintln!("Failed to send frame pair to display: {}", e);
                        } else {
                            println!("Sent complete frame pair #{} to display", next_display_id);
                        }
                        
                        next_display_id += 1;
                    } else {
                        // Put back the regular frame and wait for the max frame
                        regular_frames.insert(next_display_id, reg_pair);
                        break;
                    }
                }
                
                // Cleanup old frames to prevent memory buildup
                let stale_threshold = next_display_id.saturating_sub(500);
                regular_frames.retain(|&k, _| k >= stale_threshold);
                max_frames.retain(|&k, _| k >= stale_threshold);

                if diagnostic_timer.elapsed() >= diagnostic_interval {
                    diagnostic_timer = Instant::now();
                    println!("DIAGNOSTIC: HashMap state before pairing:");
                    println!("  next_display_id: {}", next_display_id);
                    println!("  regular_frames: {} entries, keys: {:?}", 
                            regular_frames.len(), 
                            regular_frames.keys().take(5).collect::<Vec<_>>());
                    println!("  max_frames: {} entries, keys: {:?}", 
                            max_frames.len(), 
                            max_frames.keys().take(5).collect::<Vec<_>>());
                }
                
                // Brief delay to prevent CPU thrashing
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            
            println!("Decoder thread completed");
        });
    });
    
    // Display thread - This runs on the main thread without async to avoid Send issues
    let running_display = running.clone();
    
    // Create window dimensions based on scaled frame size
    let scale_factor = SCALE_FACTOR_WINDOW;
    let scaled_width = (WIDTH_ENCODER as f64 * scale_factor) as usize;
    let scaled_height = (HEIGHT_ENCODER as f64 * scale_factor) as usize;
    let window_width = scaled_width * 2 + 10; // Two frames + separator
    
    // Create window with explicit options
    println!("Creating display window ({} x {})", window_width, scaled_height);
    let mut window = Window::new(
        "HEVC Comparison",
        window_width,
        scaled_height,
        WindowOptions {
            resize: true,
            scale: minifb::Scale::X1,
            topmost: false,  // Don't make it topmost as it can be annoying
            ..WindowOptions::default()
        },
    ).unwrap_or_else(|e| {
        eprintln!("Failed to create window: {}", e);
        std::process::exit(1);
    });
    
    // Main display loop - note this is NOT async
    let mut frames_displayed = 0;
    let start_time = Instant::now();
    
    while running_display.load(Ordering::SeqCst) && window.is_open() {
        // Try to receive frame pair with timeout
        match pair_rx.recv_timeout(Duration::from_millis(16)) {
            Ok(pair) => {
      
                
                // Display the frame pair
                if display_frame_pair_to_window(&pair, &server_ip, pair.frame_id, &mut window) {
                    frames_displayed += 1;
                    println!("Displaying frame pair #{} (elapsed: {:?})", 
                    pair.frame_id, start_time.elapsed());
                }
                
            },
            Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                // No new frames, just update window to keep it responsive
                window.update();
            },
            Err(e) => {
                eprintln!("Error receiving frame pair: {}", e);
                break;
            }
        }
        
        // Check for window close or escape key
        if !window.is_open() || window.is_key_down(minifb::Key::Escape) {
            println!("Window closed or Escape pressed, shutting down");
            running_display.store(false, Ordering::SeqCst);
            running.store(false, Ordering::SeqCst);
            break;
        }
    }
    
    println!("Display loop completed - displayed {} frames", frames_displayed);
    
    // Signal all threads to stop and wait for them
    running.store(false, Ordering::SeqCst);
    encoder_thread.join().unwrap();
    decoder_thread.join().unwrap();
    
    println!("All processing completed, shutting down");
    Ok(())
}


// Implement this frame synchronization function in the decoder thread
fn synchronize_frame_ids(regular_frames: &HashMap<usize, FramePair>, 
        max_frames: &HashMap<usize, FramePair>, 
        current_id: usize) -> usize {
    // If current_id is valid, don't change it
    if regular_frames.contains_key(&current_id) && max_frames.contains_key(&current_id) {
    return current_id;
    }

    // Find all common frame IDs between the two maps
    let mut common_ids: Vec<usize> = regular_frames.keys()
    .filter(|&k| max_frames.contains_key(k))
    .cloned()
    .collect();

    // Sort to find the lowest common ID
    if !common_ids.is_empty() {
    common_ids.sort();
    println!("Synchronizing frame IDs: Adjusting next_display_id from {} to {}", 
    current_id, common_ids[0]);
    return common_ids[0];
    }

    // If no common IDs exist yet, return current_id
    current_id
}
