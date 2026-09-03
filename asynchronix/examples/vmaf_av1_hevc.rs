
use futures::stream::FuturesUnordered;
use futures_channel::mpsc::TryRecvError;
use futures_util::stream::StreamExt;
use std::{path::Path, time::Duration};
use tai_time::TaiTime;
use tokio::{sync::Semaphore};
mod lib;
use crate::lib::alvr_stream_socket::{ChunkedAv1Encoder, ChunkedHevcEncoder, ChunkedEncoder, ChunkedSoftwareHevcEncoder, VideoCodec};
// bring your types into scope (adjust these paths to your project)
use crate::lib::{DEBUG_PRINT_ENABLED, models_XR::{HEIGHT_ENCODER, SCALE_FACTOR_FFMPEG_WINDOW, WIDTH_ENCODER, HevcDecoder, Av1Decoder, VideoDecoder}, render_text,};
use std::fs;
use std::path::PathBuf;
use crate::lib::{DebugColor, };
// static METRIC_SLOTS: Lazy<Semaphore> = Lazy::new(|| Semaphore::const_new(30)); // ≤4 frames in flight
use anyhow::Result;
use async_std::task;

use crossbeam::channel::{bounded,Sender,};
use minifb::{ Window, WindowOptions};

use serde::Deserialize;

use std::collections::{HashMap, BTreeSet, VecDeque};

use std::fmt::{ Debug};
use std::fs::File;

use std::io::{Read,};
use once_cell::sync::Lazy;

use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::{
    collections::HashSet,
  
    // net::{TcpListener, UdpSocket},
};
use tempfile::TempDir;
use std::net::IpAddr;
use core::f64;
use regex::Regex;
use std::result::Result::Ok;
use std::vec;
use tokio::task::JoinSet;
use std::time:: {Instant};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow::array::Array;

const MAX_CONCURRENT_VMAF_SCENARIOS: usize = 1; 
pub const MAX_BITRATE_REFERENCE_MBPS: f32 = 100.0; 
pub const WINDOW_SCALE_MULTIPLIER: f64 = 0.1; 
pub const BOUNDED_CHANNEL_SIZE: usize = 50; // channel depth for VMAF crossbeam

const MAX_PARALLEL_VMAF: usize = 10; 

// Tunables
const MAX_DRIFT_GAP: u32 = 40; // Allow small out-of-order arrival before dropping
const FORCE_DROP_TIMEOUT: Duration = Duration::from_millis(12000); // Max wait for a lagging frame

const MAX_VMAF_BUFFER_SIZE: usize = 100;
const MAX_PENDING_TASKS: usize = 5;

const MAX_PENDING_PAIRS: usize = 50;


/// One global pool → one permit per concurrent VMAF job
static VMAF_SLOTS: Lazy<Arc<Semaphore>> =
    Lazy::new(|| Arc::new(Semaphore::const_new(MAX_PARALLEL_VMAF)));


const OCR_X: usize = 10;
const OCR_Y: usize = 10;
const OCR_W: usize = 58; // Measure this in a screenshot if needed!
const OCR_H: usize = 96;

// Add this struct to handle the recognition


#[derive(Clone)]
struct BufferedFrame {
    frame: FrameBuf,
    arrived_at: Instant,
}


pub struct FrameSyncManager {
    // Using BTreeSet for efficient Min/Max ID retrieval
    ref_ids: BTreeSet<u32>,
    dist_ids: BTreeSet<u32>,
    
    ref_buffer: HashMap<u32, BufferedFrame>,
    dist_buffer: HashMap<u32, BufferedFrame>,
    
    last_processed_id: i64,
    
    // Stats
    pub dropped_ref: usize,
    pub dropped_dist: usize,
    sync_started: bool,

}

impl FrameSyncManager {
    pub fn new() -> Self {
        Self {
            ref_ids: BTreeSet::new(),
            dist_ids: BTreeSet::new(),
            ref_buffer: HashMap::new(),
            dist_buffer: HashMap::new(),
            last_processed_id: -1,
            dropped_ref: 0,
            dropped_dist: 0,
            sync_started: false, 

        }
    }

    pub fn insert_ref_frame(&mut self, id: u32, frame: FrameBuf) {
        if self.ref_buffer.insert(id, BufferedFrame { frame, arrived_at: Instant::now() }).is_none() {
            self.ref_ids.insert(id);
        }
    }

    pub fn insert_dist_frame(&mut self, id: u32, frame: FrameBuf) {
        if self.dist_buffer.insert(id, BufferedFrame { frame, arrived_at: Instant::now() }).is_none() {
            self.dist_ids.insert(id);
        }
    }

    pub fn buffer_counts(&self) -> (usize, usize) {
        (self.ref_buffer.len(), self.dist_buffer.len())
    }

    /// The Core Logic: Returns a vector of matched pairs ready for VMAF processing.
    /// It automatically cleans up lagging frames.
    pub fn sync_and_retrieve(&mut self) -> Vec<(u32, FrameBuf, FrameBuf)> {
        let mut ready_pairs = Vec::new();

        if !self.sync_started {
            if self.dist_ids.is_empty() {
                // We have Refs, but no Dist yet. 
                // Return empty to wait (blocking drops) until the encoder starts.
                return ready_pairs; 
            } else {
                // First Dist frame arrived! Enable normal logic.
                self.sync_started = true;
            }
        }

        loop {
            // Peek at the oldest available frames in both buffers
            let min_ref = self.ref_ids.iter().next().cloned();
            let min_dist = self.dist_ids.iter().next().cloned();

            match (min_ref, min_dist) {
                (Some(r_id), Some(d_id)) => {
                    if r_id == d_id {
                        // 1. MATCH FOUND
                        ready_pairs.push(self.pop_pair(r_id));
                        self.last_processed_id = r_id as i64;
                    } 
                    else if r_id < d_id {
                        // 2. REF IS LAGGING (Ref=0, Dist=22)
                        // The Dist stream has moved past this Ref frame.
                        // Implication: The Dist frame corresponding to r_id was lost/dropped.
                        
                        let gap = d_id - r_id;
                        let age = self.ref_buffer.get(&r_id).unwrap().arrived_at.elapsed();

                        // Immediate catch-up if gap is large, or timeout if gap is small
                        if gap > MAX_DRIFT_GAP || age > FORCE_DROP_TIMEOUT {
                            // Drop the lagging REF frame to catch up
                            self.drop_ref(r_id, &format!("Lagging behind Dist (Gap: {}, Age: {:.2?})", gap, age));
                        } else {
                            // Gap is small, maybe Dist #r_id is just slightly delayed? Wait.
                            break; 
                        }
                    } 
                    else {
                        // 3. DIST IS LAGGING (Ref=100, Dist=95)
                        // This implies the encoder/network is slow delivering Dist #95.
                        // We must be more patient here, but eventually timeout.
                        
                        let age = self.dist_buffer.get(&d_id).unwrap().arrived_at.elapsed();
                        
                        if age > FORCE_DROP_TIMEOUT {
                            self.drop_dist(d_id, &format!("Timed out waiting for sync (Age: {:.2?})", age));
                        } else {
                            break; // Wait for Dist to arrive
                        }
                    }
                },
                (Some(r_id), None) => {
                    // Ref exists, waiting for ANY Dist. 
                    // Clean up Ref if it gets incredibly old (zombie frame prevention)
                    let age = self.ref_buffer.get(&r_id).unwrap().arrived_at.elapsed();
                    if age > Duration::from_secs(20) {
                        self.drop_ref(r_id, "Zombie REF frame (No DIST input)");
                    }
                    break; 
                },
                (None, Some(d_id)) => {
                    // Dist exists, waiting for Ref.
                    // This is rare (Ref is usually faster), but clean up if needed.
                    let age = self.dist_buffer.get(&d_id).unwrap().arrived_at.elapsed();
                    if age > Duration::from_secs(5) {
                        self.drop_dist(d_id, "Zombie DIST frame (No REF input)");
                    }
                    break;
                },
                (None, None) => break, // Both empty
            }
        }
        
        ready_pairs
    }

    // --- Helpers ---

    fn pop_pair(&mut self, id: u32) -> (u32, FrameBuf, FrameBuf) {
        self.ref_ids.remove(&id);
        self.dist_ids.remove(&id);
        let r_frame = self.ref_buffer.remove(&id).unwrap().frame;
        let d_frame = self.dist_buffer.remove(&id).unwrap().frame;
        (id, r_frame, d_frame)
    }

    fn drop_ref(&mut self, id: u32, reason: &str) {
        self.ref_ids.remove(&id);
        self.ref_buffer.remove(&id);
        self.dropped_ref += 1;
        // Optional: Log only occasionally to avoid spam
        if id % 10 == 0 {
             println!(">> [SYNC] Dropped REF #{} - {}", id, reason);
        }
    }

    fn drop_dist(&mut self, id: u32, reason: &str) {
        self.dist_ids.remove(&id);
        self.dist_buffer.remove(&id);
        self.dropped_dist += 1;
        println!(">> [SYNC] Dropped DIST #{} - {}", id, reason);
    }

    pub fn check_timeouts(&mut self) {
        // Define your hard limit (e.g., 2.0 or 5.0 seconds)
        // This must be longer than your expected network latency but shorter than "forever"
        const HARD_TIMEOUT: Duration = Duration::from_secs(30); 
         if !self.sync_started {
            return;
        }
        // --- 1. CLEAN REFERENCE BUFFER ---
        loop {
            // Peek at the oldest ID (BTreeSet is sorted, first is always smallest/oldest)
            let should_drop = if let Some(&id) = self.ref_ids.iter().next() {
                if let Some(frame) = self.ref_buffer.get(&id) {
                    if frame.arrived_at.elapsed() > HARD_TIMEOUT  {
                        Some(id)
                    } else {
                        None // Oldest frame is fresh enough, stop checking
                    }
                } else {
                    None // Should not happen, but safety first
                }
            } else {
                None // Buffer empty
            };

            // Apply the drop (we do this outside the borrow to please Rust)
            if let Some(id) = should_drop {
                self.drop_ref(id, "HARD TIMEOUT (Stale)");
            } else {
                break; // We are done
            }
        }

        // --- 2. CLEAN DISTORTED BUFFER ---
        loop {
            let should_drop = if let Some(&id) = self.dist_ids.iter().next() {
                if let Some(frame) = self.dist_buffer.get(&id) {
                    if frame.arrived_at.elapsed() > HARD_TIMEOUT {
                        Some(id)
                    } else {
                        None 
                    }
                } else {
                    None
                }
            } else {
                None
            };

            if let Some(id) = should_drop {
                self.drop_dist(id, "HARD TIMEOUT (Stale)");
            } else {
                break;
            }
        }
    }
}
struct DigitReader {
    templates: HashMap<u8, Vec<u8>>, 
    char_w: usize,
    char_h: usize,
    start_x: usize,
    start_y: usize,
    grid_w: usize,
    grid_h: usize,
    // NEW: Track recognition history for better temporal validation
    last_seen_id: Option<u32>,
    consecutive_failures: usize,
    // NEW: Multi-template support for robustness
    template_samples: HashMap<u8, Vec<Vec<u8>>>,
}

impl DigitReader {
    fn new(start_x: usize, start_y: usize, char_w: usize, char_h: usize) -> Self {
        Self {
            templates: HashMap::new(),
            char_w, char_h, start_x, start_y,
            grid_w: 12, 
            grid_h: 18,
            last_seen_id: None,
            consecutive_failures: 0,
            template_samples: HashMap::new(),
        }
    }

    /// Your existing process_patch is good, keep it
    fn process_patch(&self, rgb: &[u8], width: usize, digit_index: usize) -> Vec<u8> {
        let mut grid = Vec::with_capacity(self.grid_w * self.grid_h);
        let crop_x = self.start_x + (digit_index * self.char_w);
        let crop_y = self.start_y;

        let mut min_val = 255u8;
        let mut max_val = 0u8;

        for gy in 0..self.grid_h {
            for gx in 0..self.grid_w {
                let src_x_start = crop_x + (gx * self.char_w) / self.grid_w;
                let src_x_end = crop_x + ((gx + 1) * self.char_w) / self.grid_w;
                let src_y_start = crop_y + (gy * self.char_h) / self.grid_h;
                let src_y_end = crop_y + ((gy + 1) * self.char_h) / self.grid_h;

                let mut sum: u32 = 0;
                let mut count: u32 = 0;

                for y in src_y_start..src_y_end {
                    for x in src_x_start..src_x_end {
                        let px_idx = (y * width + x) * 3;
                        let gray = (rgb[px_idx] as u32 * 3 + rgb[px_idx+1] as u32 * 6 + rgb[px_idx+2] as u32) / 10;
                        sum += gray;
                        count += 1;
                    }
                }
                let avg = if count > 0 { (sum / count) as u8 } else { 0 };
                
                if avg < min_val { min_val = avg; }
                if avg > max_val { max_val = avg; }
                
                grid.push(avg);
            }
        }

        // Contrast stretch
        if max_val > min_val + 10 {
            let range = (max_val - min_val) as f32;
            for val in &mut grid {
                *val = (((*val - min_val) as f32 / range) * 255.0) as u8;
            }
        }

        grid
    }

    /// IMPROVED: Learn multiple samples and create averaged templates
    fn learn_digit(&mut self, rgb: &[u8], width: usize, number: u32) {
        let s = format!("{:05}", number); 
        for (i, char_digit) in s.chars().enumerate() {
            let digit = char_digit.to_digit(10).unwrap() as u8;
            let grid = self.process_patch(rgb, width, i);
            
            // Quality check
            let sum: u32 = grid.iter().map(|&x| x as u32).sum();
            if sum > (self.grid_w * self.grid_h * 50) as u32 {
                // Store sample
                self.template_samples.entry(digit)
                    .or_insert_with(Vec::new)
                    .push(grid.clone());
                
                // Keep only last 10 samples to avoid memory bloat
                let samples = self.template_samples.get_mut(&digit).unwrap();
                if samples.len() > 10 {
                    samples.remove(0);
                }
                
                // Update template to median of all samples (more robust than mean)
                self.templates.insert(digit, Self::median_template(samples));
            }
        }
    }

    /// Compute median template from multiple samples
    fn median_template(samples: &[Vec<u8>]) -> Vec<u8> {
        if samples.is_empty() { return Vec::new(); }
        
        let len = samples[0].len();
        let mut median = Vec::with_capacity(len);
        
        for i in 0..len {
            let mut values: Vec<u8> = samples.iter().map(|s| s[i]).collect();
            values.sort_unstable();
            median.push(values[values.len() / 2]);
        }
        
        median
    }

    /// IMPROVED: Better temporal validation and fallback strategies
    fn recognize(&mut self, rgb: &[u8], width: usize) -> Option<u32> {
        if self.templates.len() < 10 { return None; }

        // Try main recognition
        let result = self.recognize_internal(rgb, width);
        
        match result {
            Some(val) => {
                self.consecutive_failures = 0;
                
                // Validate against expected sequence
                if let Some(last) = self.last_seen_id {
                    if self.is_valid_sequence(last, val) {
                        self.last_seen_id = Some(val);
                        return Some(val);
                    } else {
                        // Sequence violation - but maybe it's real?
                        // Try with relaxed thresholds
                        if let Some(relaxed) = self.recognize_relaxed(rgb, width) {
                            if self.is_valid_sequence(last, relaxed) {
                                self.last_seen_id = Some(relaxed);
                                return Some(relaxed);
                            }
                        }
                        return None;
                    }
                } else {
                    // First frame
                    self.last_seen_id = Some(val);
                    return Some(val);
                }
            },
            None => {
                self.consecutive_failures += 1;
                
                // If we've failed many times in a row, reset expectations
                // This helps recover from persistent corruption
                if self.consecutive_failures > 20 {
                    println!(">> [OCR] Too many failures, resetting sequence tracker");
                    self.last_seen_id = None;
                    self.consecutive_failures = 0;
                }
                
                return None;
            }
        }
    }

    /// Main recognition with standard thresholds
    fn recognize_internal(&self, rgb: &[u8], width: usize) -> Option<u32> {
        self.recognize_with_thresholds(rgb, width, 12_000, 0.85, false)
    }

    /// Relaxed recognition for recovery
    fn recognize_relaxed(&self, rgb: &[u8], width: usize) -> Option<u32> {
        self.recognize_with_thresholds(rgb, width, 20_000, 0.92, true)
    }

    /// Core recognition with configurable thresholds
    fn recognize_with_thresholds(
        &self, 
        rgb: &[u8], 
        width: usize,
        base_sad_limit: u32,
        base_ratio_limit: f32,
        use_temporal_bias: bool,
    ) -> Option<u32> {
        let mut result_val: u32 = 0;
        let mut total_confidence = 0u32;
        
        let expected_digits: Vec<Option<u8>> = if use_temporal_bias {
            if let Some(last) = self.last_seen_id {
                format!("{:05}", last + 1).chars()
                    .map(|c| c.to_digit(10).map(|d| d as u8))
                    .collect()
            } else {
                vec![None; 5]
            }
        } else {
            vec![None; 5]
        };

        for i in 0..5 {
            let patch = self.process_patch(rgb, width, i);
            
            let mut scores: Vec<(u8, u32)> = self.templates.iter()
                .map(|(&digit, tmpl)| {
                    let score: u32 = patch.iter().zip(tmpl.iter())
                        .map(|(a, b)| (*a as i32 - *b as i32).abs() as u32)
                        .sum();
                    (digit, score)
                })
                .collect();
            
            scores.sort_by_key(|&(_, score)| score);
            
            if scores.len() < 2 { return None; }
            
            let (best_digit, best_score) = scores[0];
            let (_, second_score) = scores[1];

            // Dynamic thresholds based on expectations
            let mut sad_limit = base_sad_limit;
            let mut ratio_limit = base_ratio_limit;

            if use_temporal_bias {
                if let Some(exp) = expected_digits.get(i).cloned().flatten() {
                    if best_digit == exp {
                        sad_limit = (sad_limit as f32 * 1.5) as u32;
                        ratio_limit = 0.95;
                    }
                }
            }

            // Validation
            if best_score > sad_limit { 
                return None; 
            }

            let ratio = best_score as f32 / second_score as f32;
            if ratio > ratio_limit { 
                return None; 
            }

            result_val = result_val * 10 + best_digit as u32;
            total_confidence += best_score;
        }

        Some(result_val)
    }

    /// Improved sequence validation
    fn is_valid_sequence(&self, last: u32, current: u32) -> bool {
        // Allow backwards for out-of-order delivery
        if current <= last {
            // Accept if within reasonable window (30 frames back)
            return last - current <= 30;
        }
        
        // Forward jump
        let gap = current - last;
        
        // Normal increment (1-3 frames ahead due to processing delays)
        if gap <= 3 {
            return true;
        }
        
        // Small gap (4-60 frames) - likely frame drops, acceptable
        if gap <= 60 {
            return true;
        }
        
        // Large gap (60-150) - suspicious but might be real if many drops
        if gap <= 150 {
            if self.consecutive_failures < 5 {
                // If we haven't been failing, be skeptical
                return false;
            }
            // If we've been failing, maybe we missed a lot
            return true;
        }
        
        // Huge gap (>150) - almost certainly hallucination
        false
    }
}


#[derive(Clone)]
pub struct FrameBuf {     // structure for having synthetic frames replacing losses. Idea is to filter them out of analysis later, 
                      //  but this way we keep both decoders synced as best as we can.
    rgb: Vec<u8>,
    synthetic: bool, // true ⇢ this is a repeated / “fake” frame
}

#[derive(serde::Serialize, Deserialize)]
struct FrameMetrics {
    frame_number: u64,
    timestamp_ms: f64,
    vmaf: f64,
    psnr: f64,
    ssim: f64,
}

#[derive(Debug, Clone)]
struct FrameInfo {
    id: u32,
    lost: bool,
}

#[derive(Clone)]
struct MetricsLogger {
    writer: Arc<Mutex<csv::Writer<File>>>,
    name_folder: String,
    name_file_w_path: String,
    task_id: usize, 
}

impl MetricsLogger {

    /// Call once, after `join_all(vmaf_jobs).await`
    pub fn finalize(&self) -> Result<()> {
        // 1/ read everything (skip header)

        let mut rdr = csv::Reader::from_path(&*self.name_file_w_path)?;
        let mut rows: Vec<FrameMetrics> = rdr.deserialize().flatten().collect();

        // 2/ sort by frame_number
        rows.sort_by_key(|r| r.frame_number);
        {
            // drop the guard immediately so the file handle is released
            let mut guard = self.writer.lock().unwrap();
            guard.flush()?; // just to be safe
        }
        // 3/ overwrite the file
        let mut wtr = csv::Writer::from_path(&*self.name_file_w_path)?;
        // wtr.write_record(&["frame_number","timestamp_ms","vmaf","psnr","ssim"])?;
        for r in rows {
            wtr.serialize(r)?;
        }
        wtr.flush()?;
        Ok(())
    }

    pub fn new_for_trace( results_folder: &str, scenario: &str, trace_idx: usize, two_encoders: bool, task_id: usize, ) -> Result<Self> {
        
        let dir = format!("{}/{}", results_folder, scenario);
        std::fs::create_dir_all(&dir)?;
        let strrrrr = if two_encoders { "bitrate" } else { "loss" };

        let path = format!("{}/VMAF_metrics_{}_{}.csv", dir, strrrrr, trace_idx);
        let file = std::fs::File::create(&path.clone().to_string())?;

        Ok(Self {
            writer: Arc::new(std::sync::Mutex::new(csv::Writer::from_writer(file))),
            name_folder: scenario.to_string(),
            name_file_w_path: path,
            task_id, 
        })
    }
    /// *The heavy ffmpeg work happens in a dedicated thread;* the caller just awaits
    /// the semaphore, spawns, and returns immediately.
    pub async fn process_frame_buffers(
        &self,
        frame_number: u64,
        timestamp_ms: f64,
        ref_buf: Vec<u8>, // own the data
        lossy_buf: Vec<u8>,
        ip_client: IpAddr,
    ) -> anyhow::Result<()> {
        // 1️⃣ back‑pressure – wait until a slot is free
        let _permit = VMAF_SLOTS.acquire().await.unwrap();

        // 2️⃣ clone `self` (the logger) for the blocking thread
        let logger = self.clone();

        // 3️⃣ move everything into a blocking worker thread
        tokio::task::spawn_blocking(move || {
            // a) create a per‑frame temp dir
            let tmp = tempfile::TempDir::new().expect("create TempDir");

            let ref_path = tmp.path().join("ref.rgb");
            let lossy_path = tmp.path().join("lossy.rgb");

            std::fs::write(&ref_path, &ref_buf).expect("write ref");
            std::fs::write(&lossy_path, &lossy_buf).expect("write lossy");

            // b) run the heavy ffmpeg+libvmaf pipeline
            logger.process_frame_metrics(
                frame_number,
                timestamp_ms,
                ref_path.to_str().unwrap(),
                lossy_path.to_str().unwrap(),
                ip_client,
            );

            // c) temp dir and semaphore permit are dropped here
        })
        .await?; // propagate panic / JoinError

        Ok(())
    }

    // ───────────────────── helper for parsing metrics ───────
    fn extract_metric(&self, path: &std::path::Path, key: &str) -> Option<f64> {
        std::fs::read_to_string(path).ok().and_then(|content| {
            // Find the line containing the key
            content
                .lines()
                .find(|line| line.contains(key))
                .and_then(|line| {
                    // Extract the value after the key
                    let after_key = line.split(key).nth(1)?;
                    // Find the first number in the remaining text
                    after_key
                        .split_whitespace()
                        .next()?
                        .trim()
                        .parse::<f64>()
                        .ok()
                })
        })
    }

    pub fn process_frame_metrics(
        &self,
        frame_number: u64,
        timestamp_ms: f64,
        ref_path: &str,
        lossy_path: &str,
        ip_client: IpAddr,
    ) {
        // ─────────── setup a temp dir ───────────
        let tmp = TempDir::new().expect("create TempDir");
        let metrics_dir = tmp.path().join(&self.name_folder).join("Sink_for_video");
        std::fs::create_dir_all(&metrics_dir).unwrap();
        let vmaf_json = metrics_dir.join("vmaf.json");

        // ───────── single, combined FFmpeg call ─────────
        // Note: we enable the PSNR feature and the (float) SSIM feature
        let status = Command::new("ffmpeg")
            .args(&[
                "-threads", "1",
                "-filter_threads", "0",
                "-loglevel", "error",
                // "-hwaccel", "cuda", 
                // distorted raw RGB24
                "-f", "rawvideo",
                "-pixel_format", "rgb24",
                "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
                "-i", lossy_path,

                // reference raw RGB24
                "-f", "rawvideo",
                "-pixel_format", "rgb24",
                "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
                "-i", ref_path,

                // do all conversions + metrics in one filter_complex
                "-filter_complex",
                &format!(
                    "[0:v]format=yuv420p[dist];\
                    [1:v]format=yuv420p[ref];\
                    [dist][ref]libvmaf=model=version=vmaf_4k_v0.6.1:log_fmt=json:log_path={}:n_threads=2:\
                    feature=name=psnr|name=float_ssim",
                    vmaf_json.display()
                ),

                // only a single frame
                "-frames:v", "1",
                "-f", "null", "-",
            ])
            .status()
            .expect("spawn ffmpeg");

        if !status.success() {
            eprintln!("ffmpeg failed on frame #{frame_number}");
            return;
        }

        // ───────── parse the JSON ─────────
        let raw = std::fs::read_to_string(&vmaf_json).expect("read vmaf JSON");
        let j: serde_json::Value = serde_json::from_str(&raw).expect("parse vmaf JSON");

        // pooled_metrics now includes:
        //  • vmaf.mean
        //  • float_ssim.mean
        //  • psnr_y.mean (libvmaf reports PSNR per plane, not as a bare "psnr" key)
        // println!("METRICS: \n{j}");

        let vmaf_score = j["pooled_metrics"]["vmaf"]["mean"].as_f64().unwrap_or(0.0);

        let ssim_score = j["pooled_metrics"]["float_ssim"]["mean"]
            .as_f64()
            .unwrap_or(0.0);

        let psnr_score = j["pooled_metrics"]["psnr_y"]["mean"]
            .as_f64()
            .unwrap_or(0.0);

        // ─────────── log & emit ───────────
        print_green!(
            "Task {} - T:{:.3} [{}] | Frame {} : VMAF {:.2}, SSIM {:.4}, PSNR {:.2}",
            self.task_id,
            timestamp_ms,
            ip_client,
            frame_number,
            vmaf_score,
            ssim_score,
            psnr_score
        );

        let fm = FrameMetrics {
            frame_number,
            timestamp_ms,
            vmaf: vmaf_score,
            psnr: psnr_score,
            ssim: ssim_score,
        };
        self.log_metrics(&fm).unwrap();
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

fn rgb_to_u32(src: &[u8]) -> Vec<u32> {
    src.chunks_exact(3)
        .map(|px| ((px[0] as u32) << 16) | ((px[1] as u32) << 8) | px[2] as u32)
        .collect()
}

/* nearest-neighbour down-scale to (w_out,h_out) */
fn resize_nn(buf: &[u32], w_in: usize, h_in: usize, w_out: usize, h_out: usize) -> Vec<u32> {
    let mut out = vec![0u32; w_out * h_out];
    for y in 0..h_out {
        let src_y = y * h_in / h_out;
        for x in 0..w_out {
            let src_x = x * w_in / w_out;
            out[y * w_out + x] = buf[src_y * w_in + src_x];
        }
    }
    out
}

fn verify_visual_sync(
    ref_rgb: &[u8],
    dist_rgb: &[u8],
    width: usize,
    width_dist: usize, // In case resolutions differ slightly, though usually same
) -> bool {
    // 1. Define ROI (Region of Interest) containing the text
    // ample space for 3-4 digits at fontsize 96
    const ROI_W: usize = 350; 
    const ROI_H: usize = 120; 
    
    // Safety check for buffer sizes
    if ref_rgb.len() < ROI_W * 3 || dist_rgb.len() < ROI_W * 3 { return false; }

    let mut mismatch_pixels = 0;
    let mut text_pixels_count = 0;

    for y in 10..ROI_H { // Start at 10 to skip potential border noise
        for x in 10..ROI_W {
            // Index calculation for RGB buffers
            let idx_ref = (y * width + x) * 3;
            let idx_dist = (y * width_dist + x) * 3;

            // 2. Thresholding (Binarization)
            // We look for "Bright" pixels against the "Black" box.
            // Green Text (Ref): High G value. Red Text (Dist): High R value.
            // Threshold > 80 assumes the text is reasonably bright.
            
            let r_ref = ref_rgb[idx_ref];
            let g_ref = ref_rgb[idx_ref+1];
            let b_ref = ref_rgb[idx_ref+2];
            // Luminance approx or Max channel check
            let is_text_ref = r_ref.max(g_ref).max(b_ref) > 80;

            let r_dist = dist_rgb[idx_dist];
            let g_dist = dist_rgb[idx_dist+1];
            let b_dist = dist_rgb[idx_dist+2];
            let is_text_dist = r_dist.max(g_dist).max(b_dist) > 80;

            if is_text_ref { text_pixels_count += 1; }

            // 3. XOR Check (Do they disagree?)
            if is_text_ref != is_text_dist {
                mismatch_pixels += 1;
            }
        }
    }

    // 4. Decision Logic
    // If frames match, mismatch should be very low (just compression artifacts edges).
    // If frames differ (e.g. 189 vs 190), mismatch will be high.
    
    // Guard: If no text was found (e.g. black frame), assume sync or skip?
    if text_pixels_count < 50 { return true; } // Probably no text burned in yet, pass it.

    // Allow 15% error rate for compression artifacts on text edges
    let error_rate = mismatch_pixels as f32 / text_pixels_count as f32;
    
    // Debug print only on failure
    if error_rate > 0.15 {
        // println!(">> Sync Mismatch! Error Rate: {:.2}", error_rate);
        return false;
    }
    
    true
}
// /* Helper to draw a hollow rectangle for debug boxes */
// fn draw_debug_rect(buf: &mut [u32], buf_w: usize, x: usize, y: usize, w: usize, h: usize, color: u32) {
//     // Top & Bottom lines
//     for i in x..(x + w).min(buf_w) {
//         let top_idx = y * buf_w + i;
//         let bot_idx = (y + h) * buf_w + i;
//         if top_idx < buf.len() { buf[top_idx] = color; }
//         if bot_idx < buf.len() { buf[bot_idx] = color; }
//     }
//     // Left & Right lines
//     for j in y..(y + h) {
//         let left_idx = j * buf_w + x;
//         let right_idx = j * buf_w + (x + w);
//         if left_idx < buf.len() && x < buf_w { buf[left_idx] = color; }
//         if right_idx < buf.len() && (x + w) < buf_w { buf[right_idx] = color; }
//     }
// }>
/* draw the two half-frames *plus* the ID text */
fn draw_pair(
    window: &mut Window,
    rgb_l: &[u8],
    rgb_r: &[u8],
    scenario: &str,
    id: u32,
    t: f64,
) -> Result<()> {
    const W: usize = WIDTH_ENCODER;
    const H: usize = HEIGHT_ENCODER;
    // Increased scale for the window content if needed, or keep standard
    let sw = (W as f64 * SCALE_FACTOR_FFMPEG_WINDOW) as usize;
    let sh = (H as f64 * SCALE_FACTOR_FFMPEG_WINDOW) as usize;
    let ww = sw * 2 + 10;

    let left = resize_nn(&rgb_to_u32(rgb_l), W, H, sw, sh);
    let right = resize_nn(&rgb_to_u32(rgb_r), W, H, sw, sh);

    let mut buf = vec![0u32; ww * sh];
    for y in 0..sh {
        let dst = y * ww;
        // Copy Left Image (Modified)
        buf[dst..dst + sw].copy_from_slice(&left[y * sw..(y + 1) * sw]);
        // Copy Right Image (Reference)
        buf[dst + sw + 10..dst + sw + 10 + sw].copy_from_slice(&right[y * sw..(y + 1) * sw]);
    }

    // --- GUI OVERLAYS ---

    // 1. "MODIFIED" Label (Left Side, Top Left)
    // Scale 4 = Large Text
    render_text(&mut buf, "MODIFIED", 20, 50, ww, 0xFF5555, 3); 

    // 2. "REFERENCE" Label (Right Side, Top Right logic or Top Left of Right Panel)
    // Placing it at (sw + 30) puts it at the start of the right panel
    render_text(&mut buf, "REFERENCE", sw + 30, 50, ww, 0x55FF55, 3);

    // 3. Frame Counter (Bottom Center - Existing)
    let y_lbl = sh - 50; // Moved up slightly to accommodate larger text
    render_text(&mut buf, &format!("#{}", id), 10, y_lbl, ww, 0xFFAA00, 3);    render_text(
        &mut buf,
        &format!("#{}", id),
        sw + 20,
        y_lbl,
        ww,
        0xFFAA00,
        3,
    );

    let debug_x = (OCR_X as f64 * SCALE_FACTOR_FFMPEG_WINDOW) as usize;
    let debug_y = (OCR_Y as f64 * SCALE_FACTOR_FFMPEG_WINDOW) as usize;
    let debug_w =((OCR_W as f64 * 5.0) * SCALE_FACTOR_FFMPEG_WINDOW) as usize; // *5 for 5 digits
    let debug_h = (OCR_H as f64 * SCALE_FACTOR_FFMPEG_WINDOW) as usize;

    // Draw RED box on Left (Distorted)
    // draw_debug_rect(&mut buf, ww, debug_x, debug_y, debug_w, debug_h, 0xFF0000);

    // // Draw GREEN box on Right (Reference)
    // // Offset by (sw + 10) to move to the second panel
    // draw_debug_rect(&mut buf, ww, debug_x + sw + 10, debug_y, debug_w, debug_h, 0x00FF00);

    window.set_title(&format!("T: {:6.4} ID {} | Scenario: {scenario}", t, id,));
    window.update_with_buffer(&buf, ww, sh)?;
    Ok(())
}

fn make_encoder_task(
    tag: usize,
    bitrate_mbps: f32,
    framerate_fps: f32,
    trace: Arc<Vec<FrameInfo>>,
    tx: Sender<(usize, u32, Vec<u8>)>,
    video_path: String,
    offset_video: f64,
    simulate_loss: bool,
    idr_freq: u32,
    intra_refresh: bool,
    use_foveation: bool, 
    video_codec: VideoCodec, 
    pacer_rx: crossbeam::channel::Receiver<()>,) 

{
    task::spawn(async move {
        let width = WIDTH_ENCODER as u32;
        let height = HEIGHT_ENCODER as u32;
        let name = format!("ENC_{}", tag);
        
        // 1️⃣ Create your encoder based on the Codec Enum
        // We wrap the specific encoder in the ChunkedEncoder enum
        let mut enc = match video_codec {
            VideoCodec::AV1 => {
                ChunkedEncoder::Av1(ChunkedAv1Encoder::new(
                    &video_path,
                    width,
                    height,
                    &format!("{}M", bitrate_mbps),
                    1.0, // chunk duration (usually 1s)
                    name,
                    offset_video,
                    framerate_fps,
                    idr_freq as usize, // GOP size
                    intra_refresh,
                    use_foveation,
                    true, // vbv_perframe
                ))
            },
            VideoCodec::HEVC => {
                // Assuming you want the Hardware HEVC here. 
                // If you want software, change to ChunkedEncoder::HevcSoftware
                ChunkedEncoder::HevcSoftware(ChunkedSoftwareHevcEncoder::new(
                    &video_path,
                    width,
                    height,
                    &format!("{}M", bitrate_mbps),
                    1.0,
                    name,
                    offset_video,
                    framerate_fps,
                    idr_freq as usize,
                    intra_refresh,
                    use_foveation,
                    true, // vbv_perframe
                ))
            }
        }; 

        // 2️⃣ Iterate until we’ve produced every ID in the trace
        let mut produced = 0;
        
        // We need a time reference for the new start_chunking signature
        // Assuming asynchronix TaiTime is available
        let start_time: TaiTime<0> = TaiTime::now_from_utc(37); 


        while produced < trace.len() {


            let elapsed_dur = TaiTime::now_from_utc(37).duration_since(start_time);

            let now: TaiTime<0> = TaiTime::new(elapsed_dur.as_secs() as i64, elapsed_dur.subsec_nanos()).unwrap();
            // (re)fill the encoder’s internal queue
            // UPDATED: The new library signature takes (bitrate, TaiTime)
            // The logic for IDR/GOP is now internal to the encoder struct set in ::new()
            enc.start_chunking(bitrate_mbps, now, Vec::new()).await;

            // drain all frames this chunk produced (but never overrun our trace)
            while produced < trace.len() {
                // try to grab the next packet
                
                loop {
                        match pacer_rx.try_recv() {
                            Ok(_) => break, // Got a permit! Go ahead.
                            Err(crossbeam::channel::TryRecvError::Empty) => {
                                // No permit yet? Sleep briefly and try again.
                                async_std::task::sleep(Duration::from_millis(1)).await;
                            }
                            Err(_) => return, // Main channel closed, exit task.
                        }
                }
                
                if let Some(pkt) = enc.next_frame().await {
                    let info = &trace[produced];
                    produced += 1;

                    // let millis_sleep = (1000.0 / framerate_fps) as u64;
                    // async_std::task::sleep(Duration::from_millis(1)).await;
                    
                    // simulate loss only on the “low” path
                    if !simulate_loss || !info.lost {

                        if tx.len() >= BOUNDED_CHANNEL_SIZE - 1 {
                            println!("⚠️ Encoder Task {} BLOCKED on full channel for ID {}", tag, info.id);
                        }

                        if tx.send((tag, info.id, pkt)).is_err() {
                            println!("Encoder of tag {tag} hung up!");
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



    });
}

fn make_reference_reader_task(
    video_path: String,
    width: usize,
    height: usize,
    tx: Sender<(u32, Vec<u8>)>,
    start_offset: f64, // <--- NEW: Offset in seconds
    fps: f64,          // <--- NEW: Framerate to calculate start number
    pacer_rx: crossbeam::channel::Receiver<()>, 

) {
    std::thread::spawn(move || {

       
        // let filter_str = format!("drawtext=fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf: text='%{{eif\\:n\\:d\\:5}}': x=10: y=10: fontsize=96: fontcolor=white: box=1: boxcolor=black: boxborderw=30");
        let start_frame_idx = (start_offset * fps).round() as usize;
        let filter_str = format!(
            "drawtext=fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf: \
            text='%{{eif\\:n\\:d\\:5}}': start_number={}: x=10: y=10: fontsize=96: \
            fontcolor=white: box=1: boxcolor=black: boxborderw=30",
            start_frame_idx
        );


        let mut child = Command::new("ffmpeg")
            .args(&[
                "-ss", &format!("{:.6}", start_offset), 
                "-i", &video_path,
                "-vf", &filter_str,
                "-f", "rawvideo",
                "-pix_fmt", "rgb24",
                "-",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("Failed to start reference reader ffmpeg");

        let mut stdout = child.stdout.take().unwrap();
        let frame_size = width * height * 3;
        let mut frame_idx = 0;
        let mut buffer = vec![0u8; frame_size];

        loop {
            if pacer_rx.recv().is_err() {
                break; // Main thread hung up
            }
            // Read exactly one frame
            if stdout.read_exact(&mut buffer).is_err() {
                break; // End of stream or error
            }

            let adjusted_id = (start_frame_idx + frame_idx) as u32;


            // Send (ID, RGB_Data)
            // If receiver is dropped (Ctrl+C), this returns Err and we break
            if tx.send((adjusted_id, buffer.clone())).is_err() {
                break; 
            }
            frame_idx += 1;
        }

        //Kill ffmpeg when we are done!
        let _ = child.kill(); 
        let _ = child.wait(); // Clean up process entry
    });
}


/// Reads a frame trace (CSV or Parquet, dispatched by extension) and returns
/// the parsed (frame_id, timestamp) pairs in file order.
fn read_trace_ids_and_timestamps(trace_path: &Path) -> Result<(Vec<u32>, Vec<f64>)> {
    match trace_path.extension().and_then(|e| e.to_str()) {
        Some(ext) if ext.eq_ignore_ascii_case("parquet") => read_trace_parquet(trace_path),
        _ => read_trace_csv(trace_path),
    }
}

fn read_trace_csv(trace_path: &Path) -> Result<(Vec<u32>, Vec<f64>)> {
    let mut rdr = csv::ReaderBuilder::new().has_headers(true).from_path(trace_path)?;

    // Get header indices dynamically
    let headers = rdr.headers()?.clone();
    let idx_frame = headers.iter().position(|h| h.to_lowercase().contains("frame") || h.to_lowercase() == "id")
        .ok_or_else(|| anyhow::anyhow!("Could not find 'Frame' or 'id' column in CSV"))?;

    // Try to find timestamp column, fallback to index 0 or 1 if not found
    let idx_ts = headers.iter().position(|h| h.to_lowercase().contains("time") || h.to_lowercase().contains("ts"))
        .unwrap_or(0); // Fallback to 0 if unknown

    println!(">> CSV Columns mapped: FrameID at col {}, Timestamp at col {}", idx_frame, idx_ts);

    let mut raw_ids = Vec::new();
    let mut raw_ts = Vec::new();

    for (i, result) in rdr.records().enumerate() {
        let rec = result?;

        let id_str = rec.get(idx_frame).unwrap_or("").trim();
        let ts_str = rec.get(idx_ts).unwrap_or("").trim();

        if id_str.is_empty() { continue; }

        // Robust parsing: Handle "1.0" as 1 if necessary
        let parsed_id = id_str.parse::<f64>().map(|f| f as u32)
            .or_else(|_| id_str.parse::<u32>());

        match parsed_id {
            Ok(id) => {
                let ts = ts_str.parse::<f64>().unwrap_or(0.0);
                raw_ids.push(id);
                raw_ts.push(ts);
            },
            Err(e) => {
                if i < 5 {
                    eprintln!("Skipping row {}: Cannot parse ID '{}' as number. Error: {}", i, id_str, e);
                }
            }
        }
    }

    Ok((raw_ids, raw_ts))
}

/// Reads an `XR_stats_*.parquet` trace written by `alvr_statistics.rs`'s `ParquetSink`
/// (columns: `frame_index: UInt64`, `timestamp: Float64`, among others).
fn read_trace_parquet(trace_path: &Path) -> Result<(Vec<u32>, Vec<f64>)> {
    let file = File::open(trace_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();

    let idx_frame = schema.fields().iter().position(|f| {
        let n = f.name().to_lowercase();
        n.contains("frame") || n == "id"
    }).ok_or_else(|| anyhow::anyhow!("Could not find 'frame' or 'id' column in Parquet schema"))?;

    let idx_ts = schema.fields().iter().position(|f| {
        let n = f.name().to_lowercase();
        n.contains("time") || n.contains("ts")
    }).unwrap_or(0);

    println!(">> Parquet Columns mapped: FrameID at col {}, Timestamp at col {}", idx_frame, idx_ts);

    let reader = builder.build()?;

    let mut raw_ids = Vec::new();
    let mut raw_ts = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;

        let id_col = batch.column(idx_frame);
        let ts_col = batch.column(idx_ts);

        for row in 0..batch.num_rows() {
            if id_col.is_null(row) { continue; }

            let id = parquet_cell_as_f64(id_col, row).map(|f| f as u32);
            let Some(id) = id else { continue; };

            let ts = if ts_col.is_null(row) { 0.0 } else { parquet_cell_as_f64(ts_col, row).unwrap_or(0.0) };

            raw_ids.push(id);
            raw_ts.push(ts);
        }
    }

    Ok((raw_ids, raw_ts))
}

/// Best-effort numeric extraction from an Arrow column cell, covering the
/// integer/float types used across the XR/tracking/bitrate Parquet schemas.
fn parquet_cell_as_f64(col: &arrow::array::ArrayRef, row: usize) -> Option<f64> {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    match col.data_type() {
        DataType::Float64 => Some(col.as_any().downcast_ref::<Float64Array>()?.value(row)),
        DataType::Float32 => Some(col.as_any().downcast_ref::<Float32Array>()?.value(row) as f64),
        DataType::UInt64  => Some(col.as_any().downcast_ref::<UInt64Array>()?.value(row) as f64),
        DataType::UInt32  => Some(col.as_any().downcast_ref::<UInt32Array>()?.value(row) as f64),
        DataType::UInt16  => Some(col.as_any().downcast_ref::<UInt16Array>()?.value(row) as f64),
        DataType::UInt8   => Some(col.as_any().downcast_ref::<UInt8Array>()?.value(row) as f64),
        DataType::Int64   => Some(col.as_any().downcast_ref::<Int64Array>()?.value(row) as f64),
        DataType::Int32   => Some(col.as_any().downcast_ref::<Int32Array>()?.value(row) as f64),
        DataType::Utf8 => col.as_any().downcast_ref::<StringArray>()?.value(row).trim().parse::<f64>().ok(),
        _ => None,
    }
}

pub async fn process_trace_vs_original(
    trace_csv: PathBuf, 
    ip: IpAddr, 
    codec: VideoCodec, 
    fps_val: u32,
    video_name: String, 
    use_gui: bool, 
    parent_results_path: &str, 
    task_id: usize, 
    use_foveation: bool, 
) -> Result<()> {
    let mut last_status = Instant::now();


    // 1. Setup Scenario & Paths
    let scenario = trace_csv.parent().unwrap().file_name().unwrap().to_string_lossy().to_string();
    
    // Construct the path to the original pristine reference
    // Format: video_samples_vmaf/{video_name}_{fps}fps.mp4
    let ref_video_path = format!("video_samples_vmaf/{}_{}fps.mp4", video_name, fps_val);
    
    println!(">> Processing Scenario: {}", scenario);
    println!(">> Reference Video: {}", ref_video_path);

    if !Path::new(&ref_video_path).exists() {
        return Err(anyhow::anyhow!("Reference video not found at: {}", ref_video_path));
    }

    // 2. Setup Metrics Logger
    // Extract trace index for naming
    let file_name = trace_csv.file_name().unwrap().to_string_lossy();
    let caps = Regex::new(r"XR_stats_(\d+)\.(?:csv|parquet)$")?.captures(&file_name).expect("filename mismatch");
    let trace_idx: usize = caps[1].parse()?;
    
    let metric = MetricsLogger::new_for_trace( parent_results_path ,&scenario, trace_idx, false, task_id)?;

    // 3. Parse CSV for Bitrate & Simulation Data
    let bitrate_re = Regex::new(r"_Br(?P<br>\d+(\.\d+)?)(?:Mbps)?_")?;
    let bitrate_mbps: f32 = bitrate_re.captures(&scenario)
        .and_then(|caps| caps.name("br")).unwrap().as_str().parse()?;
        
    let intra_re = Regex::new(r"_IR(?P<ir>\d+)")?;
    let intra_refresh_enabled = intra_re.captures(&scenario)
        .and_then(|c| c.name("ir")).map(|m| m.as_str().parse::<u32>().unwrap() > 0).unwrap_or(false);


    let gop_re = Regex::new(r"_GoP(?P<gop>\d+)_")?; 
    let idr_freq: u32 = gop_re.captures(&scenario)
        .and_then(|caps| caps.name("gop")) // Use "gop" here
        .map(|m| m.as_str().parse())
        .expect("GoP value missing in scenario string")?;


    // Read Trace (CSV or Parquet)
    // Hardcoded defaults since XR_stats might not have them in row 1
    let offset_video = 20.0;

    let (raw_ids, raw_ts) = read_trace_ids_and_timestamps(&trace_csv)?;

    if raw_ids.is_empty() {
        return Err(anyhow::anyhow!("No valid frames found in trace after parsing. Check column mapping."));
    }
    
    // Create trace lookup
    let ts_map: HashMap<u32, f64> = raw_ids.iter().cloned().zip(raw_ts.iter().cloned()).collect();
    
    // Rebuild FrameInfo vector for the encoder task
    // Note: We use the actual parsed IDs to determine "loss" if necessary, 
    // or just assume the trace dictates what is sent.

    // println!("IDs are {:?}", raw_ids); 
    let max_id = *raw_ids.iter().max().unwrap_or(&0);
    let mut trace_vec = Vec::new();
    let id_set: HashSet<_> = raw_ids.iter().cloned().collect();
    
    for id in 0..=max_id {
        // FORCE Frame 0 to be delivered so the decoder gets the Sequence Headers
        let is_lost = if id == 0 { 
            false 
        } else { 
            !id_set.contains(&id) 
        };

        trace_vec.push(FrameInfo {
            id,
            lost: is_lost,
        });
    }



    let trace_arc = Arc::new(trace_vec);

    print_green!(">> Trace loaded: {} frames (Max ID: {}). Starting tasks...", id_set.len(), max_id);
    // 4. Start The Tasks


    let (tx_pacer_enc, rx_pacer_enc) = bounded::<()>(50);
    let (tx_pacer_ref, rx_pacer_ref) = bounded::<()>(50);


    // A) Distorted Encoder (Driven by CSV)
    let (tx_enc, rx_enc) = bounded::<(usize, u32, Vec<u8>)>(2000); // limited capacity to prevent OOM
    make_encoder_task(
        0,
        bitrate_mbps,
        fps_val as f32,
        trace_arc.clone(),
        tx_enc,
        ref_video_path.clone(), // Encoder reads the same source file
        offset_video,
        true, // Simulate loss based on trace
        idr_freq,
        intra_refresh_enabled,
        use_foveation, 
        codec,
        rx_pacer_enc, 
    );

    // B) Reference Reader (Direct from MP4)
    let (tx_ref, rx_ref) = bounded::<(u32, Vec<u8>)>(BOUNDED_CHANNEL_SIZE);
    make_reference_reader_task(
        ref_video_path.clone(),
        WIDTH_ENCODER,
        HEIGHT_ENCODER,
        tx_ref,
        offset_video, 
        fps_val as f64, 
        rx_pacer_ref
    );

    // C) Distorted Decoder (Decodes packets from A)
    let mut dec_enc = VideoDecoder::new(
        codec, fps_val as usize, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32, "ENC_DISTORTED"
    );

    // 5. Processing Loop
    let sw = (WIDTH_ENCODER as f64 * WINDOW_SCALE_MULTIPLIER) as usize;
    let sh = (HEIGHT_ENCODER as f64 * WINDOW_SCALE_MULTIPLIER) as usize;
    // let mut window = Window::new(
    //     &format!("VMAF: {} vs Orig", scenario),
    //     sw * 2 + 10, sh, WindowOptions::default(),
    // )?;

    let mut window: Option<SendWindow> = if use_gui {
        Some(SendWindow::new(
            &format!("VMAF: {} vs Orig", scenario),
            sw * 2 + 10, sh,
            WindowOptions {
                resize: true,
                scale_mode: minifb::ScaleMode::AspectRatioStretch,
                ..WindowOptions::default()
            },
        )?)
    } else {
        None
    };


    let mut sync_manager = FrameSyncManager::new();
    
    // Your existing initialization
    let mut vmaf_tasks = FuturesUnordered::new();
    // let vmaf_permit_sem = Arc::new(Semaphore::new(MAX_PENDING_TASKS));
    let sem = Arc::new(Semaphore::new(num_cpus::get().min(MAX_PENDING_TASKS)));
    let mut enc_done = false;
    let mut digit_reader = DigitReader::new(OCR_X, OCR_Y, OCR_W, OCR_H);
    

        // 1. PREP: Queue to hold matched frames waiting for a CPU slot
    let mut pending_pairs: VecDeque<(u32, FrameBuf, FrameBuf)> = VecDeque::new();
    let mut last_status = std::time::Instant::now();

   loop {
        tokio::select! {
            
            // =================================================================
            // BRANCH A: Handle Finished VMAF Tasks (High Priority)
            // =================================================================
            Some(res) = vmaf_tasks.next(), if !vmaf_tasks.is_empty() => {
                match res {
                    Ok(_) => { /* Success logging */ },
                    Err(e) => eprintln!("❌ VMAF Task Failed: {}", e),
                }
                // Loop restarts immediately to fill the slot via Branch C logic below
            }

            // =================================================================
            // BRANCH B: The "Ticker" (Runs constantly)
            // =================================================================
            _ = tokio::time::sleep(Duration::from_millis(1)) => {
                
                if pending_pairs.len() < MAX_PENDING_PAIRS {
                    // Try to send a permit to BOTH producers.
                    // 'try_send' is non-blocking: if the pacer channel is full (tasks haven't 
                    // picked up previous permits yet), this simply does nothing, which is correct.
                    // let _ = tx_pacer_enc.try_send(());
                    // let _ = tx_pacer_ref.try_send(());
                    if !tx_pacer_enc.is_full() && !tx_pacer_ref.is_full() {
                        let _ = tx_pacer_enc.try_send(());
                        let _ = tx_pacer_ref.try_send(());
                    }
                }



                // --- 1. ALWAYS READ PACKETS (Never Throttle Input) ---
                // CRITICAL FIX: We drain the channel completely. If we don't, 
                // the Encoder blocks, corrupts the stream (PPS error), and dies.
                if !enc_done {
                    loop {
                        match rx_enc.try_recv() {
                            Ok((_, _pkt_id, pkt)) => {
                                // Always process the packet so the decoder stays healthy
                                dec_enc.process_packet(pkt, _pkt_id);
                            },
                            Err(crossbeam::channel::TryRecvError::Disconnected) => {
                                enc_done = true;
                                break;
                            },
                            Err(crossbeam::channel::TryRecvError::Empty) => {
                                break; // Channel is completely drained for this tick
                            }
                        }
                    }
                }

                // --- 2. DECODE & MANAGE OVERFLOW ---
                // Now we extract frames. If the SyncManager buffer is full, 
                // we must make room by dropping OLD frames, not by blocking new ones.
                if sync_manager.dist_buffer.len() < MAX_VMAF_BUFFER_SIZE {
                    while let Some((rgb, _)) = dec_enc.next_decoded_frame() {
                        
                        if let Some(visual_id) = digit_reader.recognize(&rgb, WIDTH_ENCODER) {
                            
                            // Filter ancient garbage (standard logic)
                            if let Some(last) = digit_reader.last_seen_id {
                                if visual_id < last.saturating_sub(300) { continue; }
                            }

                            // Insert the valid frame
                            sync_manager.insert_dist_frame(visual_id, FrameBuf { rgb, synthetic: false });
                            
                            // Stop extracting from decoder if we hit the limit
                            if sync_manager.dist_buffer.len() >= MAX_VMAF_BUFFER_SIZE {
                                break;
                            }
                        }
                    }
                }

                // --- 3. READ REFERENCE (Safe to throttle this, it's a file) ---
                // We can stop reading reference frames if we have too many, because
                // the file reader thread won't crash if it blocks.
                if sync_manager.ref_buffer.len() < MAX_VMAF_BUFFER_SIZE {
                     while let Ok((id, rgb)) = rx_ref.try_recv() {
                         digit_reader.learn_digit(&rgb, WIDTH_ENCODER, id);
                         sync_manager.insert_ref_frame(id, FrameBuf { rgb, synthetic: false });
                         
                         // Stop reading if we filled up
                         if sync_manager.ref_buffer.len() >= MAX_VMAF_BUFFER_SIZE { break; }
                     }
                }

                // --- 4. SYNC & SPAWN (Standard Logic) ---
                
                // Check for hard timeouts (cleans up zombie frames)
                sync_manager.check_timeouts(); 

                // Get matched pairs
                let new_matches = sync_manager.sync_and_retrieve();
                for m in new_matches {
                    pending_pairs.push_back(m);
                }

                // Spawn tasks if we have CPU slots available
                while vmaf_tasks.len() < MAX_PENDING_TASKS {
                    if let Some((id, fb_ref, fb_dist)) = pending_pairs.pop_front() {
                        

                        let start_frame_idx = (offset_video * fps_val as f64).round() as u32;
                        let lookup_id = id- start_frame_idx; 

                        let ts = *ts_map.get(&lookup_id).unwrap_or(&0.0);
                        
                        // GUI Update
                        if let Some(ref mut w) = window {
                            let _ = draw_pair(w.inner(), &fb_dist.rgb, &fb_ref.rgb, &scenario, id, ts);
                            w.inner().update();
                        }

                        // Prepare Move variables
                        let logger = metric.clone();
                        let sem_clone = sem.clone();
                        let ip_clone = ip.clone();
                        let r_rgb = fb_ref.rgb;
                        let d_rgb = fb_dist.rgb;

                        // SPAWN
                        vmaf_tasks.push(tokio::spawn(async move {
                            let _p = sem_clone.acquire().await.unwrap(); 
                            logger.process_frame_buffers(id as u64, ts, r_rgb, d_rgb, ip_clone).await
                        }));
                    } else {
                        break; // No more pairs ready
                    }
                }
                
                // --- 5. LOGGING & EXIT ---
                
                if last_status.elapsed() > Duration::from_secs(1) {
                    let (r_len, d_len) = sync_manager.buffer_counts();
                    println!(">> [Task {}] VMAF: {}/{} | Pending: {} | Buf: R{} D{}", 
                        task_id, vmaf_tasks.len(), MAX_PENDING_TASKS, pending_pairs.len(), r_len, d_len);
                    last_status = std::time::Instant::now();
                }

                // Exit Condition
                if enc_done && 
                   sync_manager.dist_buffer.is_empty() && // Use buffer count, not just len var
                   pending_pairs.is_empty() && 
                   vmaf_tasks.is_empty() {
                    
                    // Final sanity check
                    if sync_manager.sync_and_retrieve().is_empty() {
                        println!(">> Trace processing complete for Task {}", task_id);
                        break; 
                    }
                }
            }
        }
    }
   
   // Await remaining tasks
    while let Some(res) = vmaf_tasks.next().await {
        if let Err(e) = res { eprintln!("Final Task Join Error: {}", e); }
    }

    metric.finalize()?;
    Ok(())
}


struct SendWindow(minifb::Window);
unsafe impl Send for SendWindow {}

impl SendWindow {
    fn new(name: &str, w: usize, h: usize, opts: WindowOptions) -> Result<Self> {
        Ok(Self(Window::new(name, w, h, opts)?))
    }
    
    // Helper to access inner window
    fn inner(&mut self) -> &mut minifb::Window {
        &mut self.0
    }
}


#[tokio::main]
pub async fn main() { // parallel run, num_workers == MAX_CONCURRENT_VMAF_SCENARIOS

    // let results_scenarios_folder = "/home/boris/Desktop/Rust_MG1/asynchronix/Results_d1.5m_allbitrate_allfps_1seed"; 
    let results_scenarios_folder = "/home/ferran/Desktop/SiEStA-VR/Results_quicktest_"; 

    let dummy_ip = "127.0.0.1".parse().unwrap();

    let use_foveation_const : bool = false; 

    // Regex compilation (done once)
    let re_codec = Arc::new(Regex::new(r"_Codec([^_]+)").unwrap());
    let re_fps =   Arc::new(Regex::new(r"_FPS(\d+)").unwrap());
    let re_video = Arc::new(Regex::new(r"_([^_]+)_Nclose").unwrap());

    // 1. Collect all valid jobs first
    // We do this synchronously to build a clean list of work items
    let mut tasks = Vec::new();
    let entries = fs::read_dir(results_scenarios_folder).expect("Read dir failed");

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() { continue; }

        let folder_name = path.file_name().unwrap().to_string_lossy().to_string();

        // Parse Metadata
        let codec_str = re_codec.captures(&folder_name).map(|c| c.get(1).unwrap().as_str().to_string());
        let fps = re_fps.captures(&folder_name).map(|c| c[1].parse::<u32>().unwrap_or(0));
        let video_pre = re_video.captures(&folder_name).map(|c| c.get(1).unwrap().as_str().to_string());

        let video_name: Option<String> = 
            if let Some(video) = video_pre{
                if video == "short"{            // small hack
                    Some("snow_short".to_string()) 
                }
                else{
                    Some(video.to_string())
                }
            }
            else{
                None
            }; 
        // Validate
        if codec_str.is_none() || fps.is_none() || video_name.is_none() || fps.unwrap() == 0 {
            eprintln!("Skipping {}, invalid metadata", folder_name);
            continue;
        }

        let codec_enum = match codec_str.as_deref() {
            Some("AV1") => VideoCodec::AV1,
            Some("HEVC") => VideoCodec::HEVC,
            _ => { eprintln!("Skipping {}, unknown codec", folder_name); continue; }
        };

        // Show the live preview window only when a display is actually available
        // (e.g. not on a headless SLURM node).
        let use_gui = std::env::var("DISPLAY").is_ok();

        // Find trace files (CSV or Parquet) within folder.
        // Prefer CSV over Parquet when both exist for the same trace, since
        // it's cheaper to read and was the original format.
        let trace_entries = fs::read_dir(&path).expect("Read subdir failed");
        let mut trace_candidate: Option<PathBuf> = None;
        for file in trace_entries.flatten() {
            let p = file.path();
            let is_csv = p.extension().map_or(false, |ext| ext == "csv");
            let is_parquet = p.extension().map_or(false, |ext| ext == "parquet");
            if !is_csv && !is_parquet { continue; }

            let fname = p.file_name().unwrap().to_string_lossy().into_owned();

            // Only process specific trace files
            if !fname.starts_with("XR_stats_0") { continue; }

            match &trace_candidate {
                None => trace_candidate = Some(p),
                Some(existing) if existing.extension().map_or(false, |e| e == "parquet") && is_csv => {
                    // Upgrade a previously found parquet candidate to CSV
                    trace_candidate = Some(p);
                }
                _ => {}
            }
        }

        if let Some(p) = trace_candidate {
            let parent_results = p.parent()
                .and_then(|p| p.parent())
                .and_then(|p| p.file_name())
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| "Unknown".to_string());

            // Push job struct to vector
            tasks.push((
                p, // trace_csv path
                dummy_ip,
                codec_enum,
                fps.unwrap(),
                video_name.clone().unwrap(),
                use_gui,
                parent_results
            ));
        }
    }

    println!(">> Found {} total scenarios to process.", tasks.len());

    // 2. Process in Parallel with Semaphore
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_VMAF_SCENARIOS));
    let mut set = JoinSet::new();

    for (task_id, (trace_csv, ip, codec, fps, v_name, gui, parent_res)) in tasks.into_iter().enumerate() {
        let sem = semaphore.clone();
        
        // Spawn the task
        set.spawn(async move {
            // Acquire permit - this task will wait here if MAX_CONCURRENT_SCENARIOS are already running
            let _permit = sem.acquire().await.unwrap();
            
            println!(">> Starting worker for: {:?}", trace_csv.file_name());

            // Run the heavy process
            let res = process_trace_vs_original(
                trace_csv.clone(),
                ip,
                codec,
                fps,
                v_name,
                gui,
                &parent_res,
                task_id,  
                use_foveation_const, 
            ).await;

            // Log result
            match res {
                Ok(_) => println!("✅ Finished: {:?}", trace_csv.file_name()),
                Err(e) => eprintln!("❌ Error in {:?}: {}", trace_csv.file_name(), e),
            }
            
            // Permit is dropped here, allowing the next task to start
        });
    }

    // 3. Wait for all to finish
    while let Some(res) = set.join_next().await {
        if let Err(e) = res {
            eprintln!("Worker thread panicked: {}", e);
        }
    }
    
    println!(">> All scenarios processed.");
}
