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
use tokio::sync::Semaphore;
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
// 1. Replace the separate encoder state with a single coordinator

pub struct SynchronizedDecoder {
    regular_decoder: HevcDecoder,
    max_decoder: HevcDecoder,
    frame_queue: VecDeque<FramePair>,
    throttle_semaphore: Arc<Semaphore>,
    next_frame_id: Arc<AtomicUsize>,
}
fn find_best_frame_match(
    regular_frames: &[(Vec<u8>, Vec<u32>)],
    max_frames: &[(Vec<u8>, Vec<u32>)]
) -> (usize, usize, f64) {
    let mut best_regular_idx = 0;
    let mut best_max_idx = 0;
    let mut best_similarity = 1.0; // Start with worst similarity (1.0 = completely different)
    
    // Compute similarity for all possible frame pairs
    for (reg_idx, (reg_raw, _)) in regular_frames.iter().enumerate() {
        for (max_idx, (max_raw, _)) in max_frames.iter().enumerate() {
            // Use an enhanced frame similarity metric
            let similarity = compute_enhanced_frame_similarity(reg_raw, max_raw, WIDTH_ENCODER, HEIGHT_ENCODER);
            
            // Update if we found a better match
            if similarity < best_similarity {
                best_similarity = similarity;
                best_regular_idx = reg_idx;
                best_max_idx = max_idx;
            }
        }
    }
    
    (best_regular_idx, best_max_idx, best_similarity)
}

impl SynchronizedDecoder {
    pub fn new(client_ip: IpAddr, throttle_semaphore: Arc<Semaphore>) -> Self {
        Self {
            regular_decoder: HevcDecoder::new(
                FRAMERATE_WINDOWS as u32,
                WIDTH_ENCODER as u32,
                HEIGHT_ENCODER as u32,
                &format!("[CLIENT_DECODER_REGULAR {}]", client_ip),
            ),
            max_decoder: HevcDecoder::new(
                FRAMERATE_WINDOWS as u32,
                WIDTH_ENCODER as u32,
                HEIGHT_ENCODER as u32,
                &format!("[CLIENT_DECODER_MAX {}]", client_ip),
            ),
            frame_queue: VecDeque::with_capacity(8),
            throttle_semaphore,
            next_frame_id: Arc::new(AtomicUsize::new(0)),
            }
        }

    fn synchronize_frame_buffers(&mut self) {
        if self.frame_queue.len() > 1 {
            return; // Already have synchronized pairs
        }
        
        // Get pending decoded frames from both decoders
        let regular_frames = self.regular_decoder.process_decoded_frames();
        let max_frames = self.max_decoder.process_decoded_frames();
        
        if regular_frames == 0 || max_frames == 0 {
            return; // Need frames from both decoders
        }
        
        // Collect all available decoded frames with their raw data
        let mut regular_decoded: Vec<(Vec<u8>, Vec<u32>)> = Vec::new();
        let mut max_decoded: Vec<(Vec<u8>, Vec<u32>)> = Vec::new();
        
        // Extract up to 5 frames from each decoder to analyze
        for _ in 0..5 {
            if let Some((raw, timestamp)) = self.regular_decoder.next_decoded_frame() {
                if let Some(pixels) = convert_rgb_to_u32(&raw, WIDTH_ENCODER, HEIGHT_ENCODER) {
                    regular_decoded.push((raw, pixels));
                }
            }
            
            if let Some((raw, timestamp)) = self.max_decoder.next_decoded_frame() {
                if let Some(pixels) = convert_rgb_to_u32(&raw, WIDTH_ENCODER, HEIGHT_ENCODER) {
                    max_decoded.push((raw, pixels));
                }
            }
        }
        
        // If we have frames from both decoders, find the best matching pair
        if !regular_decoded.is_empty() && !max_decoded.is_empty() {
            // This is the critical part - find the best matching frames using content similarity
            let (best_regular_idx, best_max_idx, similarity) = find_best_frame_match(
                &regular_decoded,
                &max_decoded
            );
            
            // Only create a pair if similarity is good enough (below threshold)
            if similarity < 0.15 { // 15% difference threshold for good matches
                let frame_id = self.next_frame_id.fetch_add(1, Ordering::SeqCst);
                
                // Create the perfectly synchronized frame pair
                let pair = FramePair {
                    decoded: Some(regular_decoded[best_regular_idx].1.clone()),
                    reference: Some(max_decoded[best_max_idx].1.clone()),
                    decoded_raw: Some(regular_decoded[best_regular_idx].0.clone()),
                    reference_raw: Some(max_decoded[best_max_idx].0.clone()),
                    frame_id,
                };
                
                self.frame_queue.push_back(pair);
                println!("✓ Created perfectly synchronized frame pair #{} (similarity: {:.2}%)",
                    frame_id, similarity * 100.0);
            }
            
            // Remove the matched frames and any earlier frames to maintain sync
            for i in 0..=best_regular_idx {
                if i < regular_decoded.len() {
                    self.regular_decoder.skip_frame();
                }
            }
            
            for i in 0..=best_max_idx {
                if i < max_decoded.len() {
                    self.max_decoder.skip_frame();
                }
            }
        }
    }
    
    // Modify process_frame_pair to use the new synchronization logic
    pub fn process_frame_pair(&mut self, regular_frame: Vec<u8>, max_frame: Vec<u8>, frame_id: usize) {
        // Process both packets, but don't try to force immediate pairing
        self.regular_decoder.process_packet(regular_frame);
        self.max_decoder.process_packet(max_frame);
        
        // Run the content-aware synchronization
        self.synchronize_frame_buffers();
    }
    
    // Try to extract decoded frames and pair them
    fn process_decoded_frames(&mut self, frame_id: usize) {
        // Process any available frames in both decoders
        self.regular_decoder.process_decoded_frames();
        self.max_decoder.process_decoded_frames();
        
        // Try to get a frame from each decoder
        let regular_result = self.regular_decoder.next_decoded_frame();
        let max_result = self.max_decoder.next_decoded_frame();
        
        // Only create a pair if we got frames from both decoders
        if let (Some((regular_raw, regular_timestamp)), Some((max_raw, max_timestamp))) = (&regular_result, &max_result) {
            // Convert raw RGB frames to u32 pixels for display
            if let (Some(regular_pixels), Some(max_pixels)) = (
                convert_rgb_to_u32(regular_raw, WIDTH_ENCODER, HEIGHT_ENCODER),
                convert_rgb_to_u32(max_raw, WIDTH_ENCODER, HEIGHT_ENCODER)
            ) {
                // Create and store the frame pair
                let pair = FramePair {
                    decoded: Some(regular_pixels),
                    reference: Some(max_pixels),
                    decoded_raw: Some(regular_raw.clone()),
                    reference_raw: Some(max_raw.clone()),
                    frame_id,
                };
                
                self.frame_queue.push_back(pair);
                println!("Created synchronized frame pair #{}", frame_id);
            }
        } else {
            println!("Couldn't get frames from both decoders for frame #{}", frame_id);
            
            // If one decoder produced a frame but not the other, we have a problem
            // This should be rare with lockstep processing, but let's log it
            if regular_result.is_some() && max_result.is_none() {
                println!("Warning: Only regular decoder produced a frame");
            } else if regular_result.is_none() && max_result.is_some() {
                println!("Warning: Only max decoder produced a frame");
            }
        }
    }
    
    // Get the next available frame pair
    pub fn next_frame_pair(&mut self) -> Option<FramePair> {
        if let Some(pair) = self.frame_queue.pop_front() {
            // Release one throttle permit when we consume a frame
            self.throttle_semaphore.add_permits(1);
            Some(pair)
        } else {
            None
        }
    }
}
pub struct EncodingCoordinator {
    frame_id: Arc<AtomicUsize>,
    regular_encoder: ChunkedHevcEncoder,
    max_encoder: ChunkedHevcEncoder,
    throttle_semaphore: Arc<Semaphore>,

    encoder_bitrate_mbps:    f32,
    maxencoder_bitrate_mbps: f32,


}

impl EncodingCoordinator {
    pub fn new(input_path: &str, regular_bitrate: f32, max_bitrate: f32) -> Self {
        // Both encoders use the exact same offset to ensure frame alignment
        let offset = OFFSET_VIDEO;
        
        Self {
            frame_id: Arc::new(AtomicUsize::new(0)),
            regular_encoder: ChunkedHevcEncoder::new(
                input_path,
                WIDTH_ENCODER as u32,
                HEIGHT_ENCODER as u32,
                &format!("{:.1}M", regular_bitrate),
                CHUNK_DURATION_F64_s,
                "[REGULAR_ENCODER]".to_string(),
                offset,
            ),
            max_encoder: ChunkedHevcEncoder::new(
                input_path,
                WIDTH_ENCODER as u32,
                HEIGHT_ENCODER as u32,
                &format!("{:.1}M", max_bitrate),
                CHUNK_DURATION_F64_s,
                "[MAX_ENCODER]".to_string(),
                offset,
            ),
            // Limit to 4 frames in-flight to prevent buffer explosion
            throttle_semaphore: Arc::new(Semaphore::new(4)),
            encoder_bitrate_mbps: regular_bitrate,
            maxencoder_bitrate_mbps: max_bitrate, 
        }
    }
    
    // Process frames in perfect lockstep
    pub async fn next_frame_pair(&mut self) -> Option<(Vec<u8>, Vec<u8>, usize)> {
        // Wait for throttle semaphore to have permits
        let _permit = self.throttle_semaphore.acquire().await.ok()?;
        
        // Get next frame from both encoders, retrying if necessary
        let regular_frame = loop {
            match self.regular_encoder.next_frame().await {
                Some(frame) => break frame,
                None => {
                    println!("Regular encoder: No frame available, restarting chunk");
                    self.regular_encoder.parser.clear();
                    self.regular_encoder.frame_queue.clear();
                    self.regular_encoder.start_chunking(self.encoder_bitrate_mbps).await;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        };
        
        let max_frame = loop {
            match self.max_encoder.next_frame().await {
                Some(frame) => break frame,
                None => {
                    println!("Max encoder: No frame available, restarting chunk");
                    self.max_encoder.parser.clear();
                    self.max_encoder.frame_queue.clear();
                    self.max_encoder.start_chunking(self.maxencoder_bitrate_mbps).await;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        };
        
        // Get and increment the frame ID atomically
        let frame_id = self.frame_id.fetch_add(1, Ordering::SeqCst);
        
        Some((regular_frame, max_frame, frame_id))
    }
    
    // Release throttle permit - called after frame has been displayed
    pub fn release_permit(&self) {
        self.throttle_semaphore.add_permits(1);
    }
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
fn display_frame_pair_enhanced(
    pair: &FramePair, 
    server_ip: &IpAddr, 
    display_frame_id: usize, 
    window: &mut Window, 
    sync_quality: Option<f64>
) -> bool {
    let decoded = match &pair.decoded {
        Some(frame) => frame,
        None => {
            eprintln!("ERROR: Decoded frame missing, cannot display");
            return false;
        }
    };
    
    let reference = match &pair.reference {
        Some(frame) => frame,
        None => {
            eprintln!("ERROR: Reference frame missing, cannot display");
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
                // Change separator color based on sync quality
                let separator_color = match sync_quality {
                    Some(q) if q < 0.05 => 0x00FF00, // Green for excellent sync
                    Some(q) if q < 0.15 => 0xFFFF00, // Yellow for good sync
                    Some(q) if q < 0.30 => 0xFF8000, // Orange for marginal sync
                    Some(_) => 0xFF0000,             // Red for poor sync
                    None => 0x404040,                // Gray if no sync info
                };
                combined_buffer[idx] = separator_color;
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
    
    // Add sync quality indicator if available
    if let Some(quality) = sync_quality {
        let sync_text = format!("SYNC QUALITY: {:.2}%", (1.0 - quality) * 100.0);
        let text_x = (window_width - sync_text.len() * 6 * 2) / 2;
        
        // Color based on quality
        let quality_color = if quality < 0.05 {
            0x00FF00 // Green for excellent
        } else if quality < 0.15 {
            0xFFFF00 // Yellow for good
        } else if quality < 0.30 {
            0xFF8000 // Orange for marginal
        } else {
            0xFF0000 // Red for poor
        };
        
        render_text(&mut combined_buffer, &sync_text, text_x, scaled_height - 40, window_width, quality_color, 2);
    }
    
    // Add difference visualization in bottom corner
    if let (Some(raw_decoded), Some(raw_reference)) = (&pair.decoded_raw, &pair.reference_raw) {
        // Create a small difference visualization
        let diff_size = 256;
        let diff_x = window_width - diff_size - 10;
        let diff_y = scaled_height - diff_size - 10;
        
        if raw_decoded.len() == raw_reference.len() && raw_decoded.len() >= WIDTH_ENCODER * HEIGHT_ENCODER * 3 {
            // Draw difference visualization
            render_enhanced_difference_visualization(
                &mut combined_buffer,
                raw_decoded,
                raw_reference,
                WIDTH_ENCODER,
                HEIGHT_ENCODER,
                diff_x,
                diff_y,
                diff_size,
                window_width,
            );
            
            // Label the visualization
            render_text(&mut combined_buffer, "DIFF", diff_x, diff_y - 15, window_width, 0xFFFFFF, 1);
        }
    }
    
    // Update window title with precise frame information and sync quality
    let title = if let Some(quality) = sync_quality {
        format!("HEVC Comparison - Frame #{} - Sync: {:.1}%", 
                display_frame_id, (1.0 - quality) * 100.0)
    } else {
        format!("HEVC Comparison - Frame #{}", display_frame_id)
    };
    window.set_title(&title);
    
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


// Advanced frame similarity computation with configurable thresholds
// Implements perceptual frame comparison techniques with multi-scale analysis
fn compute_enhanced_frame_similarity(frame1: &[u8], frame2: &[u8], width: usize, height: usize) -> f64 {
    // Return maximum difference if frames are incompatible
    if frame1.len() != frame2.len() || frame1.len() != width * height * 3 {
        return 1.0;
    }
    
    // Configuration parameters for multi-scale analysis
    const BLOCK_SIZES: [usize; 3] = [4, 16, 64]; // Multi-scale block sizes
    const WEIGHTS: [f64; 3] = [0.5, 0.3, 0.2];   // Relative importance of each scale
    const PERCEPTUAL_WEIGHTS: [f64; 3] = [0.3, 0.6, 0.1]; // R,G,B perceptual importance
    
    // Initialize accumulators for each scale
    let mut scale_diffs = [0.0; 3];
    let mut scale_samples = [0; 3];
    
    // Multi-scale analysis
    for (scale_idx, &block_size) in BLOCK_SIZES.iter().enumerate() {
        // Calculate sampling positions - sparse sampling for efficiency
        let step_x = (width / block_size).max(1);
        let step_y = (height / block_size).max(1);
        
        // Process each block
        for by in (0..height).step_by(step_y) {
            for bx in (0..width).step_by(step_x) {
                // Calculate block boundaries
                let block_end_x = (bx + block_size).min(width);
                let block_end_y = (by + block_size).min(height);
                
                // Initialize block statistics
                let mut block_diff_r = 0.0;
                let mut block_diff_g = 0.0;
                let mut block_diff_b = 0.0;
                let mut block_samples = 0;
                
                // Sample pixels within the block (sparse)
                for y in (by..block_end_y).step_by(2) {
                    for x in (bx..block_end_x).step_by(2) {
                        let idx = (y * width + x) * 3;
                        
                        if idx + 2 < frame1.len() && idx + 2 < frame2.len() {
                            // Calculate color channel differences
                            let r_diff = (frame1[idx] as i32 - frame2[idx] as i32).abs() as f64;
                            let g_diff = (frame1[idx+1] as i32 - frame2[idx+1] as i32).abs() as f64;
                            let b_diff = (frame1[idx+2] as i32 - frame2[idx+2] as i32).abs() as f64;
                            
                            // Accumulate weighted differences
                            block_diff_r += r_diff;
                            block_diff_g += g_diff;
                            block_diff_b += b_diff;
                            block_samples += 1;
                        }
                    }
                }
                
                // Only process blocks with valid samples
                if block_samples > 0 {
                    // Calculate perceptually weighted block difference
                    let avg_diff = (
                        block_diff_r * PERCEPTUAL_WEIGHTS[0] +
                        block_diff_g * PERCEPTUAL_WEIGHTS[1] +
                        block_diff_b * PERCEPTUAL_WEIGHTS[2]
                    ) / (block_samples as f64 * 255.0); // Normalize to [0-1]
                    
                    // Add to scale accumulator
                    scale_diffs[scale_idx] += avg_diff;
                    scale_samples[scale_idx] += 1;
                }
            }
        }
    }
    
    // Calculate weighted average across scales
    let mut final_diff = 0.0;
    let mut weight_sum = 0.0;
    
    for i in 0..BLOCK_SIZES.len() {
        if scale_samples[i] > 0 {
            let scale_avg = scale_diffs[i] / scale_samples[i] as f64;
            final_diff += scale_avg * WEIGHTS[i];
            weight_sum += WEIGHTS[i];
        }
    }
    
    // Normalize result
    if weight_sum > 0.0 {
        final_diff /= weight_sum;
    }
    
    // Apply non-linear transformation to enhance sensitivity
    // This emphasizes small differences, which is crucial for detecting
    // subtle temporal misalignments in nearly-identical frames
    let enhanced_diff = 1.0 - ((1.0 - final_diff).powf(0.5));
    
    // Scale final similarity measure to emphasize high similarity
    // This creates a more sensitive metric where 99% similar frames
    // are distinguished from 99.9% similar frames
    enhanced_diff
}

// Advanced heatmap color generation with perceptual enhancements
fn enhanced_diff_to_heatmap_color(diff: f64) -> u32 {
    // Apply logarithmic scaling to enhance small differences
    // Using base-10 log scale with adjustment factor
    let log_factor = 10.0;
    let enhanced_diff = if diff > 0.0 {
        let log_val = 1.0 + (-diff.ln() / log_factor).max(-10.0);
        (log_val / 11.0).min(1.0)
    } else {
        0.0
    };
    
    // Define color ranges for enhanced perceptual distinction
    // Uses a more sophisticated gradient with multiple color points
    let color = if enhanced_diff < 0.2 {
        // Dark blue to blue - lowest differences (most similar)
        let t = enhanced_diff / 0.2;
        let r = 0;
        let g = (t * 128.0) as u32;
        let b = 128 + (t * 127.0) as u32;
        (r << 16) | (g << 8) | b
    } else if enhanced_diff < 0.4 {
        // Blue to cyan - low differences
        let t = (enhanced_diff - 0.2) / 0.2;
        let r = 0;
        let g = 128 + (t * 127.0) as u32;
        let b = 255;
        (r << 16) | (g << 8) | b
    } else if enhanced_diff < 0.6 {
        // Cyan to green - moderate differences
        let t = (enhanced_diff - 0.4) / 0.2;
        let r = (t * 128.0) as u32;
        let g = 255;
        let b = 255 - (t * 255.0) as u32;
        (r << 16) | (g << 8) | b
    } else if enhanced_diff < 0.8 {
        // Green to yellow - significant differences
        let t = (enhanced_diff - 0.6) / 0.2;
        let r = 128 + (t * 127.0) as u32;
        let g = 255;
        let b = 0;
        (r << 16) | (g << 8) | b
    } else {
        // Yellow to red - extreme differences
        let t = (enhanced_diff - 0.8) / 0.2;
        let r = 255;
        let g = 255 - (t * 255.0) as u32;
        let b = 0;
        (r << 16) | (g << 8) | b
    }; 
    
    color
}

// Render an enhanced difference visualization with improved perceptual scaling
fn render_enhanced_difference_visualization(
    buffer: &mut [u32],
    frame1: &[u8],
    frame2: &[u8],
    width: usize,
    height: usize,
    x_pos: usize,
    y_pos: usize,
    size: usize,
    stride: usize,
) {
    // Calculate block size for visualization with appropriate bounds checking
    let block_width = width.checked_div(size).unwrap_or(1);
    let block_height = height.checked_div(size).unwrap_or(1);
    
    // Draw enhanced border with 3D effect
    for y in 0..size+2 {
        for x in 0..size+2 {
            // Calculate pixel coordinates with safety bounds checking
            let buffer_y = y_pos.saturating_add(y).saturating_sub(1);
            let buffer_x = x_pos.saturating_add(x).saturating_sub(1);
            let buffer_idx = buffer_y.saturating_mul(stride).saturating_add(buffer_x);
            
            if buffer_idx < buffer.len() {
                if x == 0 || y == 0 || x == size+1 || y == size+1 {
                    // Enhanced border with depth effect
                    let is_top_left = x == 0 || y == 0;
                    buffer[buffer_idx] = if is_top_left { 0xA0A0A0 } else { 0x606060 };
                }
            }
        }
    }
    
    // Track statistics for auto-scaling
    let mut min_diff: f64 = 1.0;
    let mut max_diff: f64 = 0.0;
    let mut diffs = vec![0.0; size * size];
    
    // First pass: Calculate differences and gather statistics
    for y in 0..size {
        for x in 0..size {
            // Calculate source region with bounds validation
            let src_x = x.saturating_mul(block_width);
            let src_y = y.saturating_mul(block_height);
            
            // Use advanced block comparison
            let mut total_diff = 0.0;
            let mut samples = 0;
            
            // Enhanced adaptive sampling based on block size
            let sample_step = block_width.max(block_height).max(4).min(16) / 4;
            
            for dy in (0..block_height.min(16)).step_by(sample_step.max(1)) {
                for dx in (0..block_width.min(16)).step_by(sample_step.max(1)) {
                    let pixel_x = src_x.saturating_add(dx);
                    let pixel_y = src_y.saturating_add(dy);
                    
                    // Calculate pixel index with bounds validation
                    let pixel_idx = pixel_y.saturating_mul(width).saturating_add(pixel_x).saturating_mul(3);
                    
                    // Ensure we don't go out of bounds
                    if pixel_idx + 2 < frame1.len() && pixel_idx + 2 < frame2.len() {
                        // Calculate perceptually weighted RGB differences
                        let r_diff = (frame1[pixel_idx] as i32 - frame2[pixel_idx] as i32).abs() as f64;
                        let g_diff = (frame1[pixel_idx+1] as i32 - frame2[pixel_idx+1] as i32).abs() as f64;
                        let b_diff = (frame1[pixel_idx+2] as i32 - frame2[pixel_idx+2] as i32).abs() as f64;
                        
                        // Perceptual weighting (human eye is more sensitive to green)
                        total_diff += r_diff * 0.2126 + g_diff * 0.7152 + b_diff * 0.0722;
                        samples += 1;
                    }
                }
            }
            
            // Calculate normalized difference with enhanced sensitivity
            let avg_diff = if samples > 0 {
                // Normalize and apply non-linear scaling to emphasize subtle differences
                let normalized = total_diff / (samples as f64 * 255.0);
                // Use a power function to enhance sensitivity to small differences
                1.0 - (1.0 - normalized).powf(0.4)
            } else {
                0.0
            };
            
            // Store diff for auto-scaling
            let idx = y * size + x;
            if idx < diffs.len() {
                diffs[idx] = avg_diff;
                min_diff = min_diff.min(avg_diff);
                max_diff = max_diff.max(avg_diff);
            }
        }
    }
    
    // Calculate dynamic range for auto-scaling
    let diff_range = max_diff - min_diff;
    
    // Second pass: Render with auto-scaled intensity
    for y in 0..size {
        for x in 0..size {
            let idx = y * size + x;
            if idx < diffs.len() {
                // Apply auto-scaling to maximize visualization contrast
                let norm_diff = if diff_range > 0.001 {
                    (diffs[idx] - min_diff) / diff_range
                } else {
                    // If range is too small, normalize around midpoint
                    let center = (min_diff + max_diff) / 2.0;
                    let scaled = (diffs[idx] - center) * 100.0 + 0.5;
                    scaled.max(0.0).min(1.0)
                };
                
                // Convert to heatmap color with enhanced perceptual mapping
                let heatmap_color = enhanced_diff_to_heatmap_color(norm_diff);
                
                // Plot the pixel with bounds validation
                let buffer_idx = (y_pos + y).saturating_mul(stride).saturating_add(x_pos + x);
                if buffer_idx < buffer.len() {
                    buffer[buffer_idx] = heatmap_color;
                }
            }
        }
    }
    
    // Add enhancement markers (grid lines) for better visualization interpretation
    for i in 1..4 {
        let line_pos = (size * i) / 4;
        
        // Horizontal grid line
        for x in 0..size {
            let buffer_idx = (y_pos + line_pos).saturating_mul(stride).saturating_add(x_pos + x);
            if buffer_idx < buffer.len() {
                // Semi-transparent grid line
                let existing = buffer[buffer_idx];
                let r = (existing >> 16) & 0xFF;
                let g = (existing >> 8) & 0xFF;
                let b = existing & 0xFF;
                
                // Blend with grid color (dark semi-transparent)
                let blend_factor = 0.8;
                let new_r = (r as f64 * blend_factor) as u32;
                let new_g = (g as f64 * blend_factor) as u32;
                let new_b = (b as f64 * blend_factor) as u32;
                
                buffer[buffer_idx] = (new_r << 16) | (new_g << 8) | new_b;
            }
        }
        
        // Vertical grid line
        for y in 0..size {
            let buffer_idx = (y_pos + y).saturating_mul(stride).saturating_add(x_pos + line_pos);
            if buffer_idx < buffer.len() {
                // Semi-transparent grid line
                let existing = buffer[buffer_idx];
                let r = (existing >> 16) & 0xFF;
                let g = (existing >> 8) & 0xFF;
                let b = existing & 0xFF;
                
                // Blend with grid color (dark semi-transparent)
                let blend_factor = 0.8;
                let new_r = (r as f64 * blend_factor) as u32;
                let new_g = (g as f64 * blend_factor) as u32;
                let new_b = (b as f64 * blend_factor) as u32;
                
                buffer[buffer_idx] = (new_r << 16) | (new_g << 8) | new_b;
            }
        }
    }
}



// Function to render a difference visualization between two frames
fn render_difference_visualization(
    buffer: &mut [u32],
    frame1: &[u8],
    frame2: &[u8],
    width: usize,
    height: usize,
    x_pos: usize,
    y_pos: usize,
    size: usize,
    stride: usize,
) {
    // Calculate block size for visualization (how many source pixels per vis pixel)
    let block_width = width / size;
    let block_height = height / size;
    
    // Draw a border
    for y in 0..size+2 {
        for x in 0..size+2 {
            if x == 0 || y == 0 || x == size+1 || y == size+1 {
                let buffer_idx = (y_pos + y - 1) * stride + (x_pos + x - 1);
                if buffer_idx < buffer.len() {
                    buffer[buffer_idx] = 0x808080; // Gray border
                }
            }
        }
    }
    
    // Calculate and visualize differences
    for y in 0..size {
        for x in 0..size {
            // Calculate source region
            let src_x = x * block_width;
            let src_y = y * block_height;
            
            // Average difference over the block
            let mut total_diff = 0.0;
            let mut samples = 0;
            
            // Sample a few pixels in the block
            for dy in 0..block_height.min(4) {
                for dx in 0..block_width.min(4) {
                    let pixel_x = src_x + dx;
                    let pixel_y = src_y + dy;
                    
                    // Calculate index in the RGB buffer
                    let pixel_idx = (pixel_y * width + pixel_x) * 3;
                    
                    // Ensure we don't go out of bounds
                    if pixel_idx + 2 < frame1.len() && pixel_idx + 2 < frame2.len() {
                        // Calculate RGB differences
                        let r_diff = (frame1[pixel_idx] as i32 - frame2[pixel_idx] as i32).abs() as f64;
                        let g_diff = (frame1[pixel_idx+1] as i32 - frame2[pixel_idx+1] as i32).abs() as f64;
                        let b_diff = (frame1[pixel_idx+2] as i32 - frame2[pixel_idx+2] as i32).abs() as f64;
                        
                        // Add to running total
                        total_diff += r_diff + g_diff + b_diff;
                        samples += 3;
                    }
                }
            }
            
            // Calculate average difference (normalized 0-1)
            let avg_diff = if samples > 0 {
                total_diff / (samples as f64 * 255.0)
            } else {
                0.0
            };
            
            // Convert difference to a heatmap color
            let heatmap_color = diff_to_heatmap_color(avg_diff);
            
            // Plot the pixel in our visualization
            let buffer_idx = (y_pos + y) * stride + (x_pos + x);
            if buffer_idx < buffer.len() {
                buffer[buffer_idx] = heatmap_color;
            }
        }
    }
}

// Convert a difference value (0-1) to a heatmap color
fn diff_to_heatmap_color(diff: f64) -> u32 {
    // Clamp the difference value to 0-1 range
    let clamped_diff = diff.min(1.0).max(0.0);
    
    // Apply a non-linear scaling to enhance visibility of small differences
    // Use a cube root transformation for more distinguishable colors at lower differences
    let enhanced_diff = clamped_diff.powf(1.0/3.0);
    
    // Map to a color from blue (cold, low diff) to red (hot, high diff)
    let r = (enhanced_diff * 255.0) as u32;
    let g = ((1.0 - enhanced_diff) * 255.0) as u32;
    let b = (255.0 - enhanced_diff * 255.0) as u32;
    
    // Combine into a single u32 color value
    (r << 16) | (g << 8) | b
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



// New structure to manage frame synchronization
pub struct FrameSynchronizer {
    // Pending frames waiting to be paired
    pending_regular: HashMap<usize, FramePair>,
    pending_reference: HashMap<usize, FramePair>,
    
    // Historical buffer of successfully paired frames for analysis
    paired_history: VecDeque<(usize, usize)>, // (regular_id, reference_id)
    
    // State tracking
    next_display_id: usize,
    last_sync_time: Instant,
    sync_interval: Duration,
    
    // Configuration
    max_drift_frames: usize,
    history_size: usize,
    sync_strategy: SyncStrategy,
    
    // Diagnostics
    frames_dropped: usize,
    sync_attempts: usize,
    sync_successes: usize,
}

// Synchronization strategies
#[derive(Clone, Debug)]
pub enum SyncStrategy {
    // Basic strategy - uses only frame IDs
    BasicId,
    // Fingerprint strategy - uses content fingerprinting
    ContentFingerprint,
    // Hybrid strategy - combines ID and fingerprint approaches
    Hybrid,
}

impl FrameSynchronizer {
    pub fn new(sync_strategy: SyncStrategy) -> Self {
        Self {
            pending_regular: HashMap::new(),
            pending_reference: HashMap::new(),
            paired_history: VecDeque::with_capacity(30),
            next_display_id: 0,
            last_sync_time: Instant::now(),
            sync_interval: Duration::from_secs(5),
            max_drift_frames: 10,
            history_size: 30,
            sync_strategy,
            frames_dropped: 0,
            sync_attempts: 0,
            sync_successes: 0,
        }
    }
    
    // Add a regular (low bitrate) frame to the synchronizer
    pub fn add_regular_frame(&mut self, frame_id: usize, pair: FramePair) {
        self.pending_regular.insert(frame_id, pair);
        self.try_cleanup_old_frames();
    }
    
    // Add a reference (high bitrate) frame to the synchronizer
    pub fn add_reference_frame(&mut self, frame_id: usize, pair: FramePair) {
        self.pending_reference.insert(frame_id, pair);
        self.try_cleanup_old_frames();
    }
    
    // Get the next synchronized frame pair, if available
    pub fn next_frame_pair(&mut self) -> Option<FramePair> {
        // Check if we need to re-synchronize
        self.check_sync_status();
        
        // Try to find a match using the current next_display_id
        if let Some(pair) = self.get_exact_pair(self.next_display_id) {
            // Successfully matched a pair at the current ID
            self.paired_history.push_back((self.next_display_id, self.next_display_id));
            if self.paired_history.len() > self.history_size {
                self.paired_history.pop_front();
            }
            
            self.next_display_id += 1;
            return Some(pair);
        }
        
        // If we couldn't find an exact match, try alternative strategies
        match self.sync_strategy {
            SyncStrategy::BasicId => self.try_basic_id_sync(),
            SyncStrategy::ContentFingerprint => self.try_fingerprint_sync(),
            SyncStrategy::Hybrid => self.try_hybrid_sync(),
        }
    }
    
    // Try to get an exact pair matching at the given ID
    fn get_exact_pair(&mut self, id: usize) -> Option<FramePair> {
        if self.pending_regular.contains_key(&id) && self.pending_reference.contains_key(&id) {
            // We have both frames with matching IDs
            let reg_pair = self.pending_regular.remove(&id)?;
            let ref_pair = self.pending_reference.remove(&id)?;
            
            // Combine them into a single pair
            Some(FramePair {
                decoded: reg_pair.decoded,
                reference: ref_pair.reference,
                decoded_raw: reg_pair.decoded_raw,
                reference_raw: ref_pair.reference_raw,
                frame_id: id,
            })
        } else {
            None
        }
    }
    
    // Basic ID-based synchronization strategy
    fn try_basic_id_sync(&mut self) -> Option<FramePair> {
        // Find all common frame IDs between both maps
        let common_ids: Vec<usize> = self.pending_regular.keys()
            .filter(|&k| self.pending_reference.contains_key(k))
            .cloned()
            .collect();
        
        if !common_ids.is_empty() {
            // Sort to find the lowest common ID
            let mut sorted_ids = common_ids.clone();
            sorted_ids.sort();
            
            // Update next_display_id to this common ID
            self.next_display_id = sorted_ids[0];
            self.sync_successes += 1;
            
            // Now try again with the new ID
            return self.get_exact_pair(self.next_display_id);
        }
        
        // No common IDs found
        None
    }
    
    // Content fingerprint-based synchronization
    fn try_fingerprint_sync(&mut self) -> Option<FramePair> {
        // Collect frames with raw data for fingerprinting
        let regular_frames: Vec<(usize, &FramePair)> = self.pending_regular.iter()
            .filter(|(_, pair)| pair.decoded_raw.is_some())
            .map(|(&id, pair)| (id, pair))
            .collect();
            
        let reference_frames: Vec<(usize, &FramePair)> = self.pending_reference.iter()
            .filter(|(_, pair)| pair.reference_raw.is_some())
            .map(|(&id, pair)| (id, pair))
            .collect();
        
        // Find best matching pair using frame fingerprints
        let mut best_match = None;
        let mut best_score = f64::MAX;
        
        for &(reg_id, reg_pair) in &regular_frames {
            for &(ref_id, ref_pair) in &reference_frames {
                if let (Some(reg_raw), Some(ref_raw)) = (&reg_pair.decoded_raw, &ref_pair.reference_raw) {
                    let score = self.compute_frame_difference(reg_raw, ref_raw);
                    
                    if score < best_score {
                        best_score = score;
                        best_match = Some((reg_id, ref_id));
                    }
                }
            }
        }
        
        // If we found a good match, create a pair
        if let Some((reg_id, ref_id)) = best_match {
            if best_score < 0.3 {  // Threshold for a good match
                let reg_pair = self.pending_regular.remove(&reg_id)?;
                let ref_pair = self.pending_reference.remove(&ref_id)?;
                
                // Update synchronization state
                self.paired_history.push_back((reg_id, ref_id));
                if self.paired_history.len() > self.history_size {
                    self.paired_history.pop_front();
                }
                
                // Set next display ID to be after the matched regular frame
                self.next_display_id = reg_id + 1;
                self.sync_successes += 1;
                
                // Combine into a single pair
                return Some(FramePair {
                    decoded: reg_pair.decoded,
                    reference: ref_pair.reference,
                    decoded_raw: reg_pair.decoded_raw,
                    reference_raw: ref_pair.reference_raw,
                    frame_id: reg_id, // Use regular frame ID for display
                });
            }
        }
        
        None
    }
    
    // Hybrid synchronization strategy
    fn try_hybrid_sync(&mut self) -> Option<FramePair> {
        // First try basic ID matching
        if let Some(pair) = self.try_basic_id_sync() {
            return Some(pair);
        }
        
        // If that fails, try content fingerprinting
        self.try_fingerprint_sync()
    }
    
    // Compute a similarity score between two frames
    fn compute_frame_difference(&self, frame1: &[u8], frame2: &[u8]) -> f64 {
        // Ensure frames are of comparable size
        if frame1.len() != frame2.len() {
            return f64::MAX;
        }
        
        // Sample the frames at regular intervals for efficiency
        let sample_count = 1000;
        let sample_interval = frame1.len() / sample_count;
        
        let mut total_diff = 0.0;
        let mut samples = 0;
        
        for i in (0..frame1.len()).step_by(sample_interval.max(1)) {
            if i + 2 < frame1.len() && i + 2 < frame2.len() {
                // Compare RGB values
                let diff_r = (frame1[i] as i32 - frame2[i] as i32).abs() as f64;
                let diff_g = (frame1[i+1] as i32 - frame2[i+1] as i32).abs() as f64;
                let diff_b = (frame1[i+2] as i32 - frame2[i+2] as i32).abs() as f64;
                
                total_diff += diff_r + diff_g + diff_b;
                samples += 3;
            }
        }
        
        // Normalize by sample count and color range
        if samples > 0 {
            total_diff / (samples as f64 * 255.0)
        } else {
            f64::MAX
        }
    }
    
    // Check if we need to re-sync and perform cleanup
    fn check_sync_status(&mut self) {
        let now = Instant::now();
        
        // Periodic re-sync check
        if now.duration_since(self.last_sync_time) > self.sync_interval {
            self.sync_attempts += 1;
            self.last_sync_time = now;
            
            // Analyze history to detect drift
            self.analyze_sync_history();
            
            // Log synchronization status
            println!(
                "🔄 Sync status: {} successful syncs out of {} attempts, {} frames dropped",
                self.sync_successes,
                self.sync_attempts,
                self.frames_dropped,
            );
            
            // Print pending frame counts
            println!(
                "   Pending frames: {} regular, {} reference",
                self.pending_regular.len(),
                self.pending_reference.len(),
            );
        }
    }
    
    // Analyze sync history to detect and correct drift
    fn analyze_sync_history(&mut self) {
        if self.paired_history.len() < 3 {
            return;
        }
        
        // Calculate average drift between regular and reference frame IDs
        let mut total_drift = 0;
        for &(reg_id, ref_id) in &self.paired_history {
            total_drift += reg_id as i64 - ref_id as i64;
        }
        let avg_drift = total_drift as f64 / self.paired_history.len() as f64;
        
        // If consistent drift is detected, adjust next_display_id
        if avg_drift.abs() > 0.5 {
            println!("🔍 Detected consistent frame drift of {:.2} frames", avg_drift);
            
            // Adjust strategy based on drift
            if avg_drift.abs() > self.max_drift_frames as f64 {
                println!("⚠️ Large drift detected, forcing resync");
                
                // Find the newest common ID to resync
                let common_ids: Vec<usize> = self.pending_regular.keys()
                    .filter(|&k| self.pending_reference.contains_key(k))
                    .cloned()
                    .collect();
                
                if !common_ids.is_empty() {
                    let mut sorted_ids = common_ids.clone();
                    sorted_ids.sort();
                    self.next_display_id = sorted_ids[0];
                    println!("🔄 Resynchronized to frame ID {}", self.next_display_id);
                }
            }
        }
    }
    
    // Cleanup old frames to prevent memory buildup
    fn try_cleanup_old_frames(&mut self) {
        let stale_threshold = self.next_display_id.saturating_sub(50);
        
        // Count items to be removed for logging
        let reg_before = self.pending_regular.len();
        let ref_before = self.pending_reference.len();
        
        // Remove stale frames
        self.pending_regular.retain(|&k, _| k >= stale_threshold);
        self.pending_reference.retain(|&k, _| k >= stale_threshold);
        
        // Count dropped frames
        let newly_dropped = (reg_before - self.pending_regular.len()) + 
                           (ref_before - self.pending_reference.len());
        self.frames_dropped += newly_dropped;
        
        if newly_dropped > 0 {
            println!("🧹 Cleaned up {} stale frames", newly_dropped);
        }
    }
    
    // Get diagnostic information
    pub fn get_diagnostics(&self) -> String {
        format!(
            "Frame Synchronizer Status:\n\
             - Strategy: {:?}\n\
             - Next display ID: {}\n\
             - Pending frames: {} regular, {} reference\n\
             - History buffer: {} paired frames\n\
             - Frames dropped: {}\n\
             - Sync attempts: {}, successes: {}",
            self.sync_strategy,
            self.next_display_id,
            self.pending_regular.len(),
            self.pending_reference.len(),
            self.paired_history.len(),
            self.frames_dropped,
            self.sync_attempts,
            self.sync_successes,
        )
    }
}

// Decoder thread with enhanced synchronization
fn run_decoder_thread(
    running: Arc<AtomicBool>,
    regular_rx: crossbeam::channel::Receiver<(Vec<u8>, usize)>,
    max_rx: crossbeam::channel::Receiver<(Vec<u8>, usize)>,
    pair_tx: crossbeam::channel::Sender<FramePair>,
    client_ip: IpAddr,
) {
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
            &format!("[CLIENT_DECODER_REGULAR {}]", client_ip),
        );
        
        let mut max_decoder = HevcDecoder::new(
            FRAMERATE_WINDOWS as u32, 
            WIDTH_ENCODER as u32, 
            HEIGHT_ENCODER as u32, 
            &format!("[CLIENT_DECODER_MAX {}]", client_ip),
        );
        
        // Initialize the frame synchronizer with hybrid strategy
        let mut synchronizer = FrameSynchronizer::new(SyncStrategy::Hybrid);
        
        // Diagnostic timer
        let mut diagnostic_timer = Instant::now();
        let diagnostic_interval = Duration::from_secs(5);
        
        // Main processing loop
        while running.load(Ordering::SeqCst) {
            // Process incoming frames
            let mut processed_frames = false;
            
            // Process regular frames
            match regular_rx.try_recv() {
                Ok((frame, frame_id)) => {
                    println!("Received regular frame #{} (size: {})", frame_id, frame.len());
                    let (raw, pixels, timestamp) = decode_frame(&mut decoder, frame, frame_id, &client_ip);
                    
                    if !pixels.is_empty() && timestamp.is_some() {
                        // Create frame pair and add to synchronizer
                        let pair = FramePair {
                            decoded: Some(pixels),
                            decoded_raw: Some(raw),
                            reference: None,
                            reference_raw: None,
                            frame_id,
                        };
                        
                        synchronizer.add_regular_frame(frame_id, pair);
                        processed_frames = true;
                        println!("Added decoded regular frame #{} to synchronizer", frame_id);
                    }
                },
                Err(crossbeam::channel::TryRecvError::Empty) => {},
                Err(e) => {
                    eprintln!("Error receiving regular frame: {}", e);
                    break;
                }
            }
            
            // Process max bitrate frames
            match max_rx.try_recv() {
                Ok((frame, frame_id)) => {
                    println!("Received max frame #{} (size: {})", frame_id, frame.len());
                    let (raw, pixels, timestamp) = decode_frame(&mut max_decoder, frame, frame_id, &client_ip);
                    
                    if !pixels.is_empty() && timestamp.is_some() {
                        // Create frame pair and add to synchronizer
                        let pair = FramePair {
                            decoded: None,
                            decoded_raw: None,
                            reference: Some(pixels),
                            reference_raw: Some(raw),
                            frame_id,
                        };
                        
                        synchronizer.add_reference_frame(frame_id, pair);
                        processed_frames = true;
                        println!("Added reference max frame #{} to synchronizer", frame_id);
                    }
                },
                Err(crossbeam::channel::TryRecvError::Empty) => {},
                Err(e) => {
                    eprintln!("Error receiving max frame: {}", e);
                    break;
                }
            }
            
            // Try to get synchronized frame pairs
            while let Some(complete_pair) = synchronizer.next_frame_pair() {
                // Send to display thread
                if let Err(e) = pair_tx.send(complete_pair.clone()) {
                    eprintln!("Failed to send frame pair to display: {}", e);
                    break;
                } else {
                    println!("Sent synchronized frame pair #{} to display", complete_pair.frame_id);
                    processed_frames = true;
                }
            }
            
            // Print diagnostics periodically
            if diagnostic_timer.elapsed() >= diagnostic_interval {
                diagnostic_timer = Instant::now();
                println!("DIAGNOSTIC: Frame synchronizer state:");
                println!("{}", synchronizer.get_diagnostics());
            }
            
            // Brief delay if nothing was processed to prevent CPU thrashing
            if !processed_frames {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        
        println!("Decoder thread completed");
    });
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



    pub fn skip_frame(&mut self) {
        self.decoded_frames.pop_front();
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
                    } else {
                        println!("{} ⚠️ Received malformed frame (size={}), expected {}", 
                            self.decoder_string, frame.len(), self.expected_frame_size);
                        
                        if frame.len() >= self.expected_frame_size * 9 / 10 && 
                           frame.len() <= self.expected_frame_size * 11 / 10 {
                            self.decoded_frames.push_back(frame);
                        }
                    }
                    
                    if self.decoded_frames.len() >= self.max_buffered_frames {
                        break;
                    }
                },
                Err(TryRecvError::Empty) => {
                    break;
                },
                Err(TryRecvError::Disconnected) => {
                    println!("{} 🛑 Decoder output channel disconnected!", self.decoder_string);
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
    let regular_bitrate_mbps = 5.0;
    let max_bitrate_mbps = 100.0;
    let max_frames = 2000;
    
    // Define path to the input video
    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/cut_video.mp4";
    
    // Define network endpoints
    let server_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let client_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
    
    // Signal for stopping threads
    let running = Arc::new(AtomicBool::new(true));
    
    // Create a throttle semaphore to prevent buffer explosion
    let throttle_semaphore = Arc::new(Semaphore::new(4));
    
    // Channels for frame communication
    let (mut tx, mut rx) = tokio::sync::mpsc::channel::<(Vec<u8>, Vec<u8>, usize)>(16);
    let (pair_tx, pair_rx) = crossbeam::channel::bounded::<FramePair>(8);
    
    println!("Starting synchronized HEVC encoding-decoding pipeline with precise frame alignment");
    
    // Start the encoding coordinator in its own thread
    let encoder_throttle = throttle_semaphore.clone();
    let running_encoder = running.clone();
    let encoder_thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        
        rt.block_on(async {
            // Initialize our encoding coordinator
            let mut coordinator = EncodingCoordinator::new(
                input_path, 
                regular_bitrate_mbps, 
                max_bitrate_mbps
            );
            
            // Start initial chunking
            println!("Starting initial encoding chunks");
            coordinator.max_encoder.start_chunking(max_bitrate_mbps).await;
            coordinator.regular_encoder.start_chunking(regular_bitrate_mbps).await;
            
            // Frame processing loop
            let mut frames_sent = 0;
            while running_encoder.load(Ordering::SeqCst) && frames_sent < max_frames {
                // Get the next frame pair from both encoders
                match coordinator.next_frame_pair().await {
                    Some((regular_frame, max_frame, frame_id)) => {
                        // Send both frames to the decoder
                        if let Err(e) = tx.send((regular_frame, max_frame, frame_id)).await {
                            eprintln!("Failed to send frame pair to decoder: {}", e);
                            break;
                        }
                        
                        frames_sent += 1;
                        println!("Sent frame pair #{} to decoder", frame_id);
                    },
                    None => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
                
                // Brief delay to maintain reasonable frame rate and allow the decoder to catch up
                tokio::time::sleep(Duration::from_millis(16)).await;
            }
            
            println!("Encoder thread completed after {} frames", frames_sent);
        });
    });
    
    // Start the decoder in its own thread with enhanced synchronization
    let decoder_throttle = throttle_semaphore.clone();
    let running_decoder = running.clone();
    let decoder_thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        
        rt.block_on(async {
            // Initialize our synchronized decoder with content-aware matching
            let mut decoder = SynchronizedDecoder::new(client_ip, decoder_throttle);
            
            while running_decoder.load(Ordering::SeqCst) {
                // Try to receive a frame pair from the encoder
                match rx.recv().await {
                    Some((regular_frame, max_frame, frame_id)) => {
                        // Process both frames with content-aware synchronization
                        decoder.process_frame_pair(regular_frame, max_frame, frame_id);
                        
                        // Check if we have any frame pairs ready for display
                        while let Some(pair) = decoder.next_frame_pair() {
                            // Send to display thread
                            if let Err(e) = pair_tx.send(pair) {
                                eprintln!("Failed to send frame pair to display: {}", e);
                                break;
                            }
                        }
                    },
                    None => {
                        // Run the synchronization logic even if no new frames arrived
                        decoder.synchronize_frame_buffers();
                        
                        // Check for already decoded pairs
                        while let Some(pair) = decoder.next_frame_pair() {
                            if let Err(e) = pair_tx.send(pair) {
                                eprintln!("Failed to send frame pair to display: {}", e);
                                break;
                            }
                        }
                        
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    },
                }
            }
            
            println!("Decoder thread completed");
        });
    });
    
    // Display logic with verification of synchronization
    let running_display = running.clone();
    
    // Create window dimensions
    let scale_factor = SCALE_FACTOR_WINDOW;
    let scaled_width = (WIDTH_ENCODER as f64 * scale_factor) as usize;
    let scaled_height = (HEIGHT_ENCODER as f64 * scale_factor) as usize;
    let window_width = scaled_width * 2 + 10; 
    
    // Create window with explicit options
    println!("Creating display window ({} x {})", window_width, scaled_height);
    let mut window = Window::new(
        "HEVC Comparison",
        window_width,
        scaled_height,
        WindowOptions {
            resize: true,
            scale: minifb::Scale::X1,
            topmost: false,
            ..WindowOptions::default()
        },
    ).unwrap_or_else(|e| {
        eprintln!("Failed to create window: {}", e);
        std::process::exit(1);
    });
    
    // Main display loop with synchronization verification
    let mut frames_displayed = 0;
    let mut last_frame_time = Instant::now();
    let mut last_sync_quality_check = Instant::now();
    let start_time = Instant::now();
    let mut sync_quality_history = VecDeque::with_capacity(30);
    
    while running_display.load(Ordering::SeqCst) && window.is_open() {
        // Target frame rate control
        let target_frame_time = Duration::from_millis(33); // ~30 FPS
        let elapsed = last_frame_time.elapsed();
        if elapsed < target_frame_time {
            std::thread::sleep(target_frame_time - elapsed);
        }
        last_frame_time = Instant::now();
        
        // Try to receive frame pair with timeout
        match pair_rx.recv_timeout(Duration::from_millis(16)) {
            Ok(pair) => {
                // Verify synchronization quality by comparing frame content
                let sync_quality = if let (Some(decoded_raw), Some(reference_raw)) = 
                                       (&pair.decoded_raw, &pair.reference_raw) {
                    let quality = compute_enhanced_frame_similarity(
                        decoded_raw, reference_raw, WIDTH_ENCODER, HEIGHT_ENCODER);
                    
                    // Add to history for trending analysis
                    sync_quality_history.push_back(quality);
                    if sync_quality_history.len() > 30 {
                        sync_quality_history.pop_front();
                    }
                    
                    Some(quality)
                } else {
                    None
                };
                
                // Display the frame pair with synchronization quality indicator
                if display_frame_pair_enhanced(&pair, &server_ip, pair.frame_id, &mut window, sync_quality) {
                    frames_displayed += 1;
                    
                    // Calculate and display FPS and sync quality
                    let fps = frames_displayed as f64 / start_time.elapsed().as_secs_f64();
                    let avg_quality = if !sync_quality_history.is_empty() {
                        sync_quality_history.iter().sum::<f64>() / sync_quality_history.len() as f64
                    } else {
                        0.0
                    };
                    
                    println!("Frame #{}: FPS={:.1}, Sync={:.2}% (Avg: {:.2}%)", 
                             pair.frame_id, fps, 
                             sync_quality.unwrap_or(0.0) * 100.0,
                             avg_quality * 100.0);
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
        
        // Periodically check sync quality trend
        if last_sync_quality_check.elapsed() > Duration::from_secs(5) {
            if !sync_quality_history.is_empty() {
                let avg_quality = sync_quality_history.iter().sum::<f64>() / 
                                 sync_quality_history.len() as f64;
                
                println!("Synchronization quality analysis:");
                println!("  Average similarity: {:.2}%", (1.0 - avg_quality) * 100.0);
                println!("  Frame count: {}", frames_displayed);
                
                // Check if sync is deteriorating
                if avg_quality > 0.2 { // More than 20% difference is concerning
                    println!("⚠️ Synchronization quality is suboptimal, may need adjustment");
                } else {
                    println!("✓ Synchronization quality is good");
                }
            }
            
            last_sync_quality_check = Instant::now();
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
    
    println!("All processing completed");
    Ok(())
}


fn old_main() -> Result<(), Box<dyn std::error::Error>> {
    // Define configuration parameters
    let regular_bitrate_mbps = 5.0;
    let max_bitrate_mbps = 100.0;
    let max_frames = 2000;
    
    // Define path to the input video
    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/cut_video.mp4";
    
    // Define network endpoints
    let server_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let client_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
    
    // Signal for stopping threads
    let running = Arc::new(AtomicBool::new(true));
    
    // Create a throttle semaphore to prevent buffer explosion
    let throttle_semaphore = Arc::new(Semaphore::new(4));
    
    // Channel for frame pairs to be displayed

    let (mut tx, mut rx) = tokio::sync::mpsc::channel::<(Vec<u8>, Vec<u8>, usize)>(16);

    let (pair_tx, pair_rx) = crossbeam::channel::bounded::<FramePair>(8);
    
    println!("Starting synchronized HEVC encoding-decoding pipeline");
    
    // Start the encoding coordinator in its own thread
    let encoder_throttle = throttle_semaphore.clone();
    let running_encoder = running.clone();
    let encoder_thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        
        rt.block_on(async {
            // Initialize our encoding coordinator
            let mut coordinator = EncodingCoordinator::new(
                input_path, 
                regular_bitrate_mbps, 
                max_bitrate_mbps
            );
            
            // Start initial chunking
            println!("Starting initial encoding chunks");
            coordinator.max_encoder.start_chunking(max_bitrate_mbps).await;
            coordinator.regular_encoder.start_chunking(regular_bitrate_mbps).await;
            
            // Frame processing loop
            let mut frames_sent = 0;
            while running_encoder.load(Ordering::SeqCst) && frames_sent < max_frames {
                // Get the next frame pair from both encoders
                match coordinator.next_frame_pair().await {
                    Some((regular_frame, max_frame, frame_id)) => {
                        // Send both frames to the decoder
                        if let Err(e) = tx.send((regular_frame, max_frame, frame_id)).await {
                            eprintln!("Failed to send frame pair to decoder: {}", e);
                            break;
                        }
                        
                        frames_sent += 1;
                        println!("Sent frame pair #{} to decoder", frame_id);
                    },
                    None => {
                        // This shouldn't happen with our retry loop, but just in case
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
                
                // Brief delay to maintain reasonable frame rate
                tokio::time::sleep(Duration::from_millis(16)).await;
            }
            
            println!("Encoder thread completed after {} frames", frames_sent);
        });
    });
    
    // Start the decoder in its own thread
    let decoder_throttle = throttle_semaphore.clone();
    let running_decoder = running.clone();
    let decoder_thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        
        rt.block_on(async {
            // Initialize our synchronized decoder
            let mut decoder = SynchronizedDecoder::new(client_ip, decoder_throttle);
            
            while running_decoder.load(Ordering::SeqCst) {
                // Try to receive a frame pair from the encoder
                match rx.recv().await {
                    Some((regular_frame, max_frame, frame_id)) => {
                        // Process both frames in lockstep
                        decoder.process_frame_pair(regular_frame, max_frame, frame_id);
                        
                        // Check if we have any frame pairs ready for display
                        while let Some(pair) = decoder.next_frame_pair() {
                            // Send to display thread
                            if let Err(e) = pair_tx.send(pair) {
                                eprintln!("Failed to send frame pair to display: {}", e);
                                break;
                            }
                        }
                    },
                    None => {
                        // No frames available, check for already decoded pairs
                        while let Some(pair) = decoder.next_frame_pair() {
                            // Send to display thread
                            if let Err(e) = pair_tx.send(pair) {
                                eprintln!("Failed to send frame pair to display: {}", e);
                                break;
                            }
                        }
                        
                        // Brief sleep to prevent CPU spinning
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    },
                }
            }
            
            println!("Decoder thread completed");
        });
    });
    
    // Display thread (main thread) - largely unchanged from your version
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
            topmost: false,
            ..WindowOptions::default()
        },
    ).unwrap_or_else(|e| {
        eprintln!("Failed to create window: {}", e);
        std::process::exit(1);
    });
    
    // Main display loop - note this is NOT async
    let mut frames_displayed = 0;
    let mut last_frame_time = Instant::now();
    let start_time = Instant::now();
    
    while running_display.load(Ordering::SeqCst) && window.is_open() {
        // Target frame rate control
        let target_frame_time = Duration::from_millis(33); // ~30 FPS
        let elapsed = last_frame_time.elapsed();
        if elapsed < target_frame_time {
            std::thread::sleep(target_frame_time - elapsed);
        }
        last_frame_time = Instant::now();
        
        // Try to receive frame pair with timeout
        match pair_rx.recv_timeout(Duration::from_millis(16)) {
            Ok(pair) => {
                // Display the frame pair with the simplified function
                if display_frame_pair_to_window(&pair, &server_ip, pair.frame_id, &mut window) {
                    frames_displayed += 1;
                    
                    // Calculate and display FPS
                    let fps = frames_displayed as f64 / start_time.elapsed().as_secs_f64();
                    println!("Displaying frame pair #{} (elapsed: {:?}, FPS: {:.2})", 
                            pair.frame_id, start_time.elapsed(), fps);
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



// Enhanced main function with frame synchronization workflow
fn fancy_main() -> Result<(), Box<dyn std::error::Error>> {
    // Define configuration parameters
    let max_bitrate_ladder_mbps = 100.0; 
    let current_bitrate_mbps = 5.0; 
    let max_frames = 2000;
    
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
    
    // Create frame pair channel for window display with sync quality metric
    let (pair_tx, pair_rx) = crossbeam::channel::bounded::<(FramePair, Option<f64>)>(10);
    
    println!("Starting HEVC encoding-decoding pipeline with enhanced synchronization");
    let start_time = Instant::now();

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
    
    // Encoder thread - largely unchanged
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
                        // Send frame to decoder
                        if let Err(e) = regular_tx.send((frame, frame_id)) {
                            eprintln!("Failed to send regular frame to channel: {}", e);
                            break;
                        }
                    },
                    None => {
                        println!("No regular frame available, restarting encoder chunk");
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
                        // Send frame to decoder
                        if let Err(e) = max_tx.send((frame, frame_id)) {
                            eprintln!("Failed to send max frame to channel: {}", e);
                            break;
                        }
                    },
                    None => {
                        println!("No max frame available, restarting max encoder chunk");
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
    
    // Enhanced decoder thread with frame synchronization
    let running_decoder = running.clone();
   // Corrected decoder thread implementation with sequential processing
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
                &format!("[CLIENT_DECODER_REGULAR {}]", client_ip),
            );
            
            let mut max_decoder = HevcDecoder::new(
                FRAMERATE_WINDOWS as u32, 
                WIDTH_ENCODER as u32, 
                HEIGHT_ENCODER as u32, 
                &format!("[CLIENT_DECODER_MAX {}]", client_ip),
            );
            
            // Initialize the frame synchronizer with hybrid strategy
            let mut synchronizer = FrameSynchronizer::new(SyncStrategy::Hybrid);
            
            // Diagnostic timer
            let mut diagnostic_timer = Instant::now();
            let diagnostic_interval = Duration::from_secs(5);
            
            // Main processing loop
            while running_decoder.load(Ordering::SeqCst) {
                // Process incoming frames from both streams
                let reg_processed = process_regular_stream(&mut decoder, &regular_rx, &mut synchronizer).await;
                let max_processed = process_max_stream(&mut max_decoder, &max_rx, &mut synchronizer).await;
                
                // Track whether we processed frames in this iteration
                let processed_frames = reg_processed || max_processed;
                
                // Try to extract synchronized frame pairs
                let mut pairs_sent = 0;
                while pairs_sent < 3 { // Process up to 3 pairs at once to avoid backing up
                    // Get next pair with similarity measurement
                    if let Some(pair) = synchronizer.next_frame_pair() {
                        // Calculate frame similarity for visualization
                        let sync_quality = if let (Some(reg_raw), Some(max_raw)) = (&pair.decoded_raw, &pair.reference_raw) {
                            Some(compute_frame_similarity(reg_raw, max_raw))
                        } else {
                            None
                        };
                        
                        // Send to display thread
                        if let Err(e) = pair_tx.send((pair.clone(), sync_quality)) {
                            eprintln!("Failed to send frame pair to display: {}", e);
                            break;
                        } else {
                            println!("Sent synchronized frame pair #{} to display (sync quality: {:?})", 
                                pair.frame_id, sync_quality);
                            pairs_sent += 1;
                        }
                    } else {
                        // No more pairs available
                        break;
                    }
                }
                
                // Print diagnostics periodically
                if diagnostic_timer.elapsed() >= diagnostic_interval {
                    diagnostic_timer = Instant::now();
                    println!("DIAGNOSTIC: Frame synchronizer state:");
                    println!("{}", synchronizer.get_diagnostics());
                    
                    // Also print decoder states
                    println!("Decoder states:");
                    println!("  Regular: {} frames processed, {} keyframes", 
                            decoder.frames_processed, decoder.keyframes_seen);
                    println!("  Max: {} frames processed, {} keyframes",
                            max_decoder.frames_processed, max_decoder.keyframes_seen);
                }
                
                // Brief delay if nothing was processed to prevent CPU thrashing
                if !processed_frames && pairs_sent == 0 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
            
            println!("Decoder thread completed");
        });
    });
    // Process frames from the regular stream
    async fn process_regular_stream(
        decoder: &mut HevcDecoder, 
        rx: &crossbeam::channel::Receiver<(Vec<u8>, usize)>,
        synchronizer: &mut FrameSynchronizer,
    ) -> bool {
        match rx.try_recv() {
            Ok((frame, frame_id)) => {
                println!("Received regular frame #{} (size: {})", frame_id, frame.len());
                let (raw, pixels, timestamp) = decode_frame(decoder, frame, frame_id, &std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 2)));
                
                if !pixels.is_empty() && timestamp.is_some() {
                    // Create frame pair and add to synchronizer
                    let pair = FramePair {
                        decoded: Some(pixels),
                        decoded_raw: Some(raw),
                        reference: None,
                        reference_raw: None,
                        frame_id,
                    };
                    
                    synchronizer.add_regular_frame(frame_id, pair);
                    println!("Added decoded regular frame #{} to synchronizer", frame_id);
                    return true;
                }
                false
            },
            Err(crossbeam::channel::TryRecvError::Empty) => false,
            Err(e) => {
                eprintln!("Error receiving regular frame: {}", e);
                false
            }
        }
    }
    
    // Process frames from the max bitrate stream
    async fn process_max_stream(
        decoder: &mut HevcDecoder, 
        rx: &crossbeam::channel::Receiver<(Vec<u8>, usize)>,
        synchronizer: &mut FrameSynchronizer,
    ) -> bool {
        match rx.try_recv() {
            Ok((frame, frame_id)) => {
                println!("Received max frame #{} (size: {})", frame_id, frame.len());
                let (raw, pixels, timestamp) = decode_frame(decoder, frame, frame_id, &std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 2)));
                
                if !pixels.is_empty() && timestamp.is_some() {
                    // Create frame pair and add to synchronizer
                    let pair = FramePair {
                        decoded: None,
                        decoded_raw: None,
                        reference: Some(pixels),
                        reference_raw: Some(raw),
                        frame_id,
                    };
                    
                    synchronizer.add_reference_frame(frame_id, pair);
                    println!("Added reference max frame #{} to synchronizer", frame_id);
                    return true;
                }
                false
            },
            Err(crossbeam::channel::TryRecvError::Empty) => false,
            Err(e) => {
                eprintln!("Error receiving max frame: {}", e);
                false
            }
        }
    }
    
    // Calculate similarity between two frames (0.0 = identical, 1.0 = completely different)
    fn compute_frame_similarity(frame1: &[u8], frame2: &[u8]) -> f64 {
        // Return max difference if frames are different sizes
        if frame1.len() != frame2.len() {
            return 1.0;
        }
        
        // Limit to certain number of sample points for performance
        let max_samples = 100_000;
        let sample_interval = (frame1.len() / 3).max(1) / max_samples.min(frame1.len() / 3);
        let sample_interval = sample_interval.max(1);
        
        let mut total_diff = 0.0;
        let mut samples = 0;
        
        // Sample RGB triplets
        for i in (0..frame1.len() - 2).step_by(sample_interval * 3) {
            let r_diff = (frame1[i] as i32 - frame2[i] as i32).abs() as f64;
            let g_diff = (frame1[i+1] as i32 - frame2[i+1] as i32).abs() as f64;
            let b_diff = (frame1[i+2] as i32 - frame2[i+2] as i32).abs() as f64;
            
            total_diff += r_diff + g_diff + b_diff;
            samples += 3;
        }
        
        // Normalize to 0.0-1.0 range
        if samples > 0 {
            total_diff / (samples as f64 * 255.0)
        } else {
            1.0
        }
    }
    
    // Display thread with enhanced visualization
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
            topmost: false,
            ..WindowOptions::default()
        },
    ).unwrap_or_else(|e| {
        eprintln!("Failed to create window: {}", e);
        std::process::exit(1);
    });
    
    // Main display loop - note this is NOT async
    let mut frames_displayed = 0;
    let mut last_frame_time = Instant::now();
    let start_time = Instant::now();
    
    while running_display.load(Ordering::SeqCst) && window.is_open() {
        // Target frame rate control
        let target_frame_time = Duration::from_millis(33); // ~30 FPS
        let elapsed = last_frame_time.elapsed();
        if elapsed < target_frame_time {
            std::thread::sleep(target_frame_time - elapsed);
        }
        last_frame_time = Instant::now();
        
        // Try to receive frame pair with timeout
        match pair_rx.recv_timeout(Duration::from_millis(16)) {
            Ok((pair, sync_quality)) => {
                // Display the frame pair with sync quality indicator
                if display_frame_pair_enhanced(&pair, &server_ip, pair.frame_id, &mut window, sync_quality) {
                    frames_displayed += 1;
                    
                    // Calculate and display FPS
                    let fps = frames_displayed as f64 / start_time.elapsed().as_secs_f64();
                    println!("Displaying frame pair #{} (elapsed: {:?}, FPS: {:.2})", 
                            pair.frame_id, start_time.elapsed(), fps);
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


// fn main() -> Result<(), Box<dyn std::error::Error>> {
//     // Define configuration parameters
//     let max_bitrate_ladder_mbps = 100.0; 
//     let current_bitrate_mbps = 5.0; 
//     let max_frames = 300;
    
//     let maxbitrate_cmd = format!("{:.1}M", max_bitrate_ladder_mbps);
//     let bitrate_cmd = format!("{:.1}M", current_bitrate_mbps);
//     let random_offset = rand::thread_rng().gen_range(50.0..OFFSET_VIDEO);
    
//     // Define path to the input video
//     let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/cut_video.mp4";
    
//     // Define network endpoints
//     let server_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
//     let client_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
    
//     // Signal for stopping threads
//     let running = Arc::new(AtomicBool::new(true));
    
//     // Create channels for frame transmission
//     let (regular_tx, regular_rx) = crossbeam::channel::bounded::<(Vec<u8>, usize)>(30);
//     let (max_tx, max_rx) = crossbeam::channel::bounded::<(Vec<u8>, usize)>(30);
    
//     // Create frame pair channel for window display
//     // This is the key channel that bridges the async processing world with the UI world
//     let (pair_tx, pair_rx) = crossbeam::channel::bounded::<FramePair>(10);
    
//     println!("Starting HEVC encoding-decoding pipeline");
//     let mut diagnostic_timer = Instant::now();
//     let diagnostic_interval = Duration::from_secs(1);

//     // Initialize encoders
//     let mut encoder = ChunkedHevcEncoder::new(
//         input_path,
//         WIDTH_ENCODER as u32,
//         HEIGHT_ENCODER as u32,
//         &bitrate_cmd,
//         CHUNK_DURATION_F64_s,
//         format!("[ENCODER {}]", server_ip),
//         random_offset,
//     );
    
//     let mut max_encoder = ChunkedHevcEncoder::new(
//         input_path,
//         WIDTH_ENCODER as u32,
//         HEIGHT_ENCODER as u32,
//         &maxbitrate_cmd,
//         CHUNK_DURATION_F64_s,
//         format!("[Bitrate MAX ENCODER {}]", server_ip),
//         random_offset,
//     );
    
//     // Start async components in separate threads
    
//     // Encoder thread
//     let running_encoder = running.clone();
//     let encoder_thread = std::thread::spawn(move || {
//         let rt = tokio::runtime::Builder::new_current_thread()
//             .enable_all()
//             .build()
//             .unwrap();
        
//         rt.block_on(async {
//             let mut frame_id = 0;
            
//             // Start initial chunking
//             println!("Starting initial encoding chunks");
//             max_encoder.start_chunking(max_bitrate_ladder_mbps).await;
//             encoder.start_chunking(current_bitrate_mbps).await;
            
//             while running_encoder.load(Ordering::SeqCst) && frame_id < max_frames {
//                 // First try to get a frame from the regular encoder
//                 match encoder.next_frame().await {
//                     Some(frame) => {
//                         // Save frame to disk if needed (debug only)
//                         // ...
                        
//                         // Send frame to decoder
//                         if let Err(e) = regular_tx.send((frame, frame_id)) {
//                             eprintln!("Failed to send regular frame to channel: {}", e);
//                             break;
//                         }
//                         tokio::time::sleep(Duration::from_millis(16)).await;

//                     },
//                     None => {
//                         println!("No regular frame available, restarting encoder");
//                         encoder.parser.buffer.clear();
//                         encoder.frame_queue.clear();
//                         encoder.start_chunking(current_bitrate_mbps).await;
                        
//                         // Brief delay to let encoder produce frames
//                         tokio::time::sleep(Duration::from_millis(100)).await;
//                         continue;
//                     }
//                 }
                
//                 // Then try to get a frame from the max bitrate encoder
//                 match max_encoder.next_frame().await {
//                     Some(frame) => {
//                         // Save frame to disk if needed (debug only)
//                         // ...
                        
//                         // Send frame to decoder
//                         if let Err(e) = max_tx.send((frame, frame_id)) {
//                             eprintln!("Failed to send max frame to channel: {}", e);
//                             break;
//                         }
//                     },
//                     None => {
//                         println!("No max frame available, restarting max encoder");
//                         max_encoder.parser.buffer.clear();
//                         max_encoder.frame_queue.clear();
//                         max_encoder.start_chunking(max_bitrate_ladder_mbps).await;
                        
//                         // Brief delay to let encoder produce frames
//                         tokio::time::sleep(Duration::from_millis(100)).await;
//                         continue;
//                     }
//                 }
                
//                 // Increment frame ID after successfully processing both frames
//                 frame_id += 1;
                
//                 // Brief delay to maintain reasonable frame rate
//                 tokio::time::sleep(Duration::from_millis(33)).await;
//             }
            
//             println!("Encoder thread completed after {} frames", frame_id);
//         });
//     });
    
//     // Decoder thread
//     let running_decoder = running.clone();
//     let decoder_thread = std::thread::spawn(move || {
//         run_decoder_thread(
//             running_decoder,
//             regular_rx,
//             max_rx,
//             pair_tx,
//             client_ip,
//         );
//     });
    
//     // Display thread - This runs on the main thread without async to avoid Send issues
//     let running_display = running.clone();
    
//     // Create window dimensions based on scaled frame size
//     let scale_factor = SCALE_FACTOR_WINDOW;
//     let scaled_width = (WIDTH_ENCODER as f64 * scale_factor) as usize;
//     let scaled_height = (HEIGHT_ENCODER as f64 * scale_factor) as usize;
//     let window_width = scaled_width * 2 + 10; // Two frames + separator
    
//     // Create window with explicit options
//     println!("Creating display window ({} x {})", window_width, scaled_height);
//     let mut window = Window::new(
//         "HEVC Comparison",
//         window_width,
//         scaled_height,
//         WindowOptions {
//             resize: true,
//             scale: minifb::Scale::X1,
//             topmost: false,  // Don't make it topmost as it can be annoying
//             ..WindowOptions::default()
//         },
//     ).unwrap_or_else(|e| {
//         eprintln!("Failed to create window: {}", e);
//         std::process::exit(1);
//     });
    
//     // Main display loop - note this is NOT async
//     let mut frames_displayed = 0;
//     let start_time = Instant::now();
    
//     while running_display.load(Ordering::SeqCst) && window.is_open() {
//         // Try to receive frame pair with timeout
//         match pair_rx.recv_timeout(Duration::from_millis(16)) {
//             Ok(pair) => {
      
                
//                 // Display the frame pair
//                 if display_frame_pair_to_window(&pair, &server_ip, pair.frame_id, &mut window) {
//                     frames_displayed += 1;
//                     println!("Displaying frame pair #{} (elapsed: {:?})", 
//                     pair.frame_id, start_time.elapsed());
//                 }
                
//             },
//             Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
//                 // No new frames, just update window to keep it responsive
//                 window.update();
//             },
//             Err(e) => {
//                 eprintln!("Error receiving frame pair: {}", e);
//                 break;
//             }
//         }
        
//         // Check for window close or escape key
//         if !window.is_open() || window.is_key_down(minifb::Key::Escape) {
//             println!("Window closed or Escape pressed, shutting down");
//             running_display.store(false, Ordering::SeqCst);
//             running.store(false, Ordering::SeqCst);
//             break;
//         }
//     }
    
//     println!("Display loop completed - displayed {} frames", frames_displayed);
    
//     // Signal all threads to stop and wait for them
//     running.store(false, Ordering::SeqCst);
//     encoder_thread.join().unwrap();
//     decoder_thread.join().unwrap();
    
//     println!("All processing completed, shutting down");
//     Ok(())
// }


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
