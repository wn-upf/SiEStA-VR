
use futures::stream::FuturesUnordered;
use futures_channel::mpsc::TryRecvError;
use futures_util::stream::StreamExt; 
use std::{path::Path, time::Duration};
use tai_time::TaiTime;
use tokio::{sync::Semaphore};
mod lib;
use crate::lib::alvr_stream_socket::{ChunkedAv1Encoder, ChunkedHevcEncoder, ChunkedEncoder, ChunkedSoftwareHevcEncoder, VideoCodec};
// bring your types into scope (adjust these paths to your project)
use crate::lib::{DEBUG_PRINT_ENABLED, models_XR::{HEIGHT_ENCODER, SCALE_FACTOR_WINDOW, WIDTH_ENCODER, HevcDecoder, Av1Decoder, VideoDecoder}, render_text,};
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

const MAX_CONCURRENT_VMAF_SCENARIOS: usize = 1; 
pub const MAX_BITRATE_REFERENCE_MBPS: f32 = 100.0; 
pub const WINDOW_SCALE_MULTIPLIER: f64 = 0.4; 
pub const BOUNDED_CHANNEL_SIZE: usize = 15; // channel depth for VMAF crossbeam

const MAX_PARALLEL_VMAF: usize = 20; 

/// One global pool → one permit per concurrent VMAF job
static VMAF_SLOTS: Lazy<Arc<Semaphore>> =
    Lazy::new(|| Arc::new(Semaphore::const_new(MAX_PARALLEL_VMAF)));


const OCR_X: usize = 10;
const OCR_Y: usize = 10;
const OCR_W: usize = 58; // Measure this in a screenshot if needed!
const OCR_H: usize = 90;

// Add this struct to handle the recognition

const INITIAL_FRAME_TIMEOUT: Duration = Duration::from_secs(3); // Reduced from 5s
const MAX_VMAF_BUFFER_SIZE: usize = 24;
const MAX_PENDING_TASKS: usize = 4;

// New: Adaptive timeout parameters
const MIN_FRAME_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_FRAME_TIMEOUT: Duration = Duration::from_secs(10);
const TIMEOUT_ADAPTATION_SAMPLES: usize = 20; // Sample last N frame intervals

// New: Flow control
const BUFFER_HIGH_WATER_MARK: usize = 20; // Start slowing down reads
const BUFFER_LOW_WATER_MARK: usize = 10;  // Resume normal operation

// NEW: Deadlock detection
const DEADLOCK_BUFFER_THRESHOLD: usize = 18; // Consider deadlock if both > this
const DEADLOCK_GAP_THRESHOLD: u32 = 30;      // and gap > this
const DEADLOCK_NO_MATCH_THRESHOLD: usize = 0; // and matching pairs = this

#[derive(Clone)]
struct BufferedFrame {
    frame: FrameBuf,
    arrived_at: Instant,
}

struct FrameArrivalTracker {
    last_arrivals: VecDeque<(u32, Instant)>,
    max_samples: usize,
    last_activity: Instant, // Track when we last saw ANY frame
}

impl FrameArrivalTracker {
    fn new(max_samples: usize) -> Self {
        Self {
            last_arrivals: VecDeque::with_capacity(max_samples),
            max_samples,
            last_activity: Instant::now(),
        }
    }

    fn record_arrival(&mut self, frame_id: u32) {
        let now = Instant::now();
        self.last_arrivals.push_back((frame_id, now));
        self.last_activity = now;
        
        if self.last_arrivals.len() > self.max_samples {
            self.last_arrivals.pop_front();
        }
    }

    fn calculate_adaptive_timeout(&self, default: Duration) -> Duration {
        if self.last_arrivals.len() < 3 {
            return default;
        }

        let mut intervals = Vec::new();
        for i in 1..self.last_arrivals.len() {
            let duration = self.last_arrivals[i].1
                .duration_since(self.last_arrivals[i-1].1);
            intervals.push(duration);
        }

        if intervals.is_empty() {
            return default;
        }

        intervals.sort();
        let p95_idx = (intervals.len() * 95) / 100;
        let p95_interval = intervals.get(p95_idx).unwrap_or(&default);
        let adaptive = p95_interval.saturating_mul(3);

        adaptive.clamp(MIN_FRAME_TIMEOUT, MAX_FRAME_TIMEOUT)
    }

    fn is_stalled(&self, threshold: Duration) -> bool {
        Instant::now().duration_since(self.last_activity) > threshold
    }

    fn time_since_last_frame(&self) -> Duration {
        Instant::now().duration_since(self.last_activity)
    }
}

#[derive(Debug)]
struct SyncHealth {
    ref_buffer_size: usize,
    dist_buffer_size: usize,
    frame_id_gap: u32,
    matching_frames: usize,
    ref_stalled: bool,
    dist_stalled: bool,
    current_ref_timeout: Duration,
    current_dist_timeout: Duration,
    in_deadlock: bool,
}

// Helper function for logging
fn age_for_logging(buffer: &HashMap<u32, BufferedFrame>, id: u32, now: Instant) -> f32 {
    buffer.get(&id)
        .map(|b| now.duration_since(b.arrived_at).as_secs_f32())
        .unwrap_or(0.0)
}
struct FrameSyncManager {
    ref_buffer: HashMap<u32, BufferedFrame>,
    dist_buffer: HashMap<u32, BufferedFrame>,
    ref_tracker: FrameArrivalTracker,
    dist_tracker: FrameArrivalTracker,
    last_processed_id: i64,
    
    expected_ref_id: u32,
    expected_dist_id: u32,
    
    dropped_ref_count: usize,
    dropped_dist_count: usize,
    
    // NEW: Deadlock tracking
    consecutive_deadlock_detections: usize,
    last_deadlock_recovery: Instant,
    
}

impl FrameSyncManager {
    fn new() -> Self {
        Self {
            ref_buffer: HashMap::new(),
            dist_buffer: HashMap::new(),
            ref_tracker: FrameArrivalTracker::new(TIMEOUT_ADAPTATION_SAMPLES),
            dist_tracker: FrameArrivalTracker::new(TIMEOUT_ADAPTATION_SAMPLES),
            last_processed_id: -1,
            expected_ref_id: 0,
            expected_dist_id: 0,
            dropped_ref_count: 0,
            dropped_dist_count: 0,
            consecutive_deadlock_detections: 0,
            last_deadlock_recovery: Instant::now(),
        }
    }

    fn get_ref_timeout(&self) -> Duration {
        self.ref_tracker.calculate_adaptive_timeout(INITIAL_FRAME_TIMEOUT)
    }

    fn get_dist_timeout(&self) -> Duration {
        self.dist_tracker.calculate_adaptive_timeout(INITIAL_FRAME_TIMEOUT)
    }

    fn insert_ref_frame(&mut self, id: u32, frame: FrameBuf) {
        self.ref_tracker.record_arrival(id);
        self.ref_buffer.insert(id, BufferedFrame {
            frame,
            arrived_at: Instant::now(),
        });
        
        if id >= self.expected_ref_id {
            self.expected_ref_id = id + 1;
        }
    }

    fn insert_dist_frame(&mut self, id: u32, frame: FrameBuf) {
        self.dist_tracker.record_arrival(id);
        self.dist_buffer.insert(id, BufferedFrame {
            frame,
            arrived_at: Instant::now(),
        });
        
        if id >= self.expected_dist_id {
            self.expected_dist_id = id + 1;
        }
    }

    /// NEW: Detect if we're in a deadlock state
    fn detect_deadlock(&self) -> bool {
        // Both buffers are nearly full
        let buffers_full = self.ref_buffer.len() >= DEADLOCK_BUFFER_THRESHOLD 
            && self.dist_buffer.len() >= DEADLOCK_BUFFER_THRESHOLD;
        
        if !buffers_full {
            return false;
        }

        // Large gap between expected IDs
        let id_gap = if self.expected_ref_id > self.expected_dist_id {
            self.expected_ref_id - self.expected_dist_id
        } else {
            self.expected_dist_id - self.expected_ref_id
        };
        
        let large_gap = id_gap > DEADLOCK_GAP_THRESHOLD;
        
        // No matching pairs available
        let matching_count = self.ref_buffer.keys()
            .filter(|id| self.dist_buffer.contains_key(id))
            .count();
        
        let no_matches = matching_count <= DEADLOCK_NO_MATCH_THRESHOLD;
        
        buffers_full && large_gap && no_matches
    }

    /// NEW: Emergency recovery from deadlock
    fn emergency_recovery(&mut self, task_id: usize) -> bool {
        if !self.detect_deadlock() {
            self.consecutive_deadlock_detections = 0;
            return false;
        }

        self.consecutive_deadlock_detections += 1;
        
        // Only trigger emergency recovery after consistent deadlock detection
        if self.consecutive_deadlock_detections < 3 {
            return false;
        }

        // Don't spam recovery
        if self.last_deadlock_recovery.elapsed() < Duration::from_secs(1) {
            return false;
        }

        println!("\n🚨 [TASK {}] DEADLOCK DETECTED - EMERGENCY RECOVERY", task_id);
        println!("   REF buffer: {} frames, expected ID: {}", 
            self.ref_buffer.len(), self.expected_ref_id);
        println!("   DIST buffer: {} frames, expected ID: {}", 
            self.dist_buffer.len(), self.expected_dist_id);
        
        // Determine which stream is lagging
        let ref_ahead = self.expected_ref_id > self.expected_dist_id;
        
        if ref_ahead {
            // Reference is ahead - drop oldest REF frames to make room for DIST to catch up
            let min_ref_id = *self.ref_buffer.keys().min().unwrap();
            let max_ref_id = *self.ref_buffer.keys().max().unwrap();
            let drop_threshold = min_ref_id + (max_ref_id - min_ref_id) / 2; // Drop oldest half
            
            let to_drop: Vec<u32> = self.ref_buffer.keys()
                .filter(|&&id| id <= drop_threshold)
                .cloned()
                .collect();
            
            println!("   Dropping {} oldest REF frames (≤ {})", to_drop.len(), drop_threshold);
            
            for id in to_drop {
                self.ref_buffer.remove(&id);
                self.dropped_ref_count += 1;
            }
        } else {
            // Distorted is ahead - drop oldest DIST frames
            let min_dist_id = *self.dist_buffer.keys().min().unwrap();
            let max_dist_id = *self.dist_buffer.keys().max().unwrap();
            let drop_threshold = min_dist_id + (max_dist_id - min_dist_id) / 2;
            
            let to_drop: Vec<u32> = self.dist_buffer.keys()
                .filter(|&&id| id <= drop_threshold)
                .cloned()
                .collect();
            
            println!("   Dropping {} oldest DIST frames (≤ {})", to_drop.len(), drop_threshold);
            
            for id in to_drop {
                self.dist_buffer.remove(&id);
                self.dropped_dist_count += 1;
            }
        }
        
        self.last_deadlock_recovery = Instant::now();
        self.consecutive_deadlock_detections = 0;
        
        println!("   Recovery complete. Buffers now: REF={}, DIST={}\n", 
            self.ref_buffer.len(), self.dist_buffer.len());
        
        true
    }

    /// FIXED: Cleanup with proper stall handling
    fn cleanup_expired_frames(&mut self, task_id: usize) -> (Vec<u32>, Vec<u32>) {
        let now = Instant::now();
        let ref_timeout = self.get_ref_timeout();
        let dist_timeout = self.get_dist_timeout();

        // Check if we're in a deadlock - emergency recovery takes priority
        if self.emergency_recovery(task_id) {
            return (Vec::new(), Vec::new()); // Already handled
        }

        let mut expired_ref = Vec::new();
        let mut expired_dist = Vec::new();

        // FIXED LOGIC: Only be lenient with timeouts if ONE stream is stalled
        // If BOTH are stalled, we're in trouble and need aggressive cleanup
        let ref_stalled = self.ref_tracker.is_stalled(Duration::from_secs(2));
        let dist_stalled = self.dist_tracker.is_stalled(Duration::from_secs(2));
        let both_stalled = ref_stalled && dist_stalled;

        // Check reference buffer
        for (&id, buffered) in &self.ref_buffer {
            let age = now.duration_since(buffered.arrived_at);
            
            // FIXED: If both stalled and old, drop it
            if both_stalled && age > Duration::from_secs(1) {
                expired_ref.push(id);
                continue;
            }
            
            // FIXED: Only be lenient if DIST is stalled but REF is not
            if dist_stalled && !ref_stalled {
                continue; // Don't drop ref frames if waiting for dist to resume
            }
            
            if age > ref_timeout {
                // Additional check for large gaps
                if id > self.expected_dist_id && (id - self.expected_dist_id) > 50 {
                    expired_ref.push(id);
                } else if age > ref_timeout.saturating_mul(2) {
                    expired_ref.push(id);
                }
            }
        }

        // Check distorted buffer (symmetric logic)
        for (&id, buffered) in &self.dist_buffer {
            let age = now.duration_since(buffered.arrived_at);
            
            if both_stalled && age > Duration::from_secs(1) {
                expired_dist.push(id);
                continue;
            }
            
            // FIXED: Only be lenient if REF is stalled but DIST is not
            if ref_stalled && !dist_stalled {
                continue;
            }
            
            if age > dist_timeout {
                if id > self.expected_ref_id && (id - self.expected_ref_id) > 50 {
                    expired_dist.push(id);
                } else if age > dist_timeout.saturating_mul(2) {
                    expired_dist.push(id);
                }
            }
        }

        // Remove expired frames
        for &id in &expired_ref {
            self.ref_buffer.remove(&id);
            self.dropped_ref_count += 1;
            if expired_ref.len() <= 10 { // Only print first 10
                println!(">>[TASK ID {}]⚠️ REF Timeout: Dropping Frame #{} (age: {:.2}s, timeout: {:.2}s)", 
                    task_id, id, 
                    age_for_logging(&self.ref_buffer, id, now),
                    ref_timeout.as_secs_f32()
                );
            }
        }
        if expired_ref.len() > 10 {
            println!(">>[TASK ID {}]⚠️ REF Timeout: Dropped {} frames total", task_id, expired_ref.len());
        }

        for &id in &expired_dist {
            self.dist_buffer.remove(&id);
            self.dropped_dist_count += 1;
            if expired_dist.len() <= 10 {
                println!(">> [TASK ID {}]⚠️ DIST Timeout: Dropping Frame #{} (age: {:.2}s, timeout: {:.2}s)", 
                    task_id, id,
                    age_for_logging(&self.dist_buffer, id, now),
                    dist_timeout.as_secs_f32()
                );
            }
        }
        if expired_dist.len() > 10 {
            println!(">> [TASK ID {}]⚠️ DIST Timeout: Dropped {} frames total", task_id, expired_dist.len());
        }

        (expired_ref, expired_dist)
    }

    fn should_throttle_reads(&self) -> (bool, bool) {
        let throttle_ref = self.ref_buffer.len() >= BUFFER_HIGH_WATER_MARK;
        let throttle_dist = self.dist_buffer.len() >= BUFFER_HIGH_WATER_MARK;
        (throttle_ref, throttle_dist)
    }

    fn find_ready_pairs(&self, max_pairs: usize) -> Vec<u32> {
        let mut ready_ids: Vec<u32> = self.ref_buffer.keys()
            .filter(|id| self.dist_buffer.contains_key(id))
            .cloned()
            .collect();
        
        ready_ids.sort();
        
        if self.last_processed_id >= 0 {
            let anchor = (self.last_processed_id + 1) as u32;
            ready_ids.sort_by_key(|id| {
                if *id >= anchor {
                    id - anchor
                } else {
                    u32::MAX - (anchor - id)
                }
            });
        }
        
        ready_ids.truncate(max_pairs);
        ready_ids
    }

    fn cleanup_stale_frames(&mut self, window_size: u32) {
        if self.last_processed_id > window_size as i64 {
            let threshold = (self.last_processed_id - window_size as i64) as u32;
            
            let removed_ref = self.ref_buffer.keys()
                .filter(|&&k| k < threshold)
                .count();
            let removed_dist = self.dist_buffer.keys()
                .filter(|&&k| k < threshold)
                .count();
            
            self.ref_buffer.retain(|&k, _| k >= threshold);
            self.dist_buffer.retain(|&k, _| k >= threshold);
            
            if removed_ref > 0 || removed_dist > 0 {
                println!(">> Cleaned up stale frames: {} ref, {} dist (threshold: {})", 
                    removed_ref, removed_dist, threshold);
            }
        }
    }

    fn get_sync_health(&self) -> SyncHealth {
        let ref_size = self.ref_buffer.len();
        let dist_size = self.dist_buffer.len();
        
        let id_gap = if self.expected_ref_id > self.expected_dist_id {
            self.expected_ref_id - self.expected_dist_id
        } else {
            self.expected_dist_id - self.expected_ref_id
        };

        let matching_count: usize = self.ref_buffer.keys()
            .filter(|id| self.dist_buffer.contains_key(id))
            .count();

        SyncHealth {
            ref_buffer_size: ref_size,
            dist_buffer_size: dist_size,
            frame_id_gap: id_gap,
            matching_frames: matching_count,
            ref_stalled: self.ref_tracker.is_stalled(Duration::from_secs(2)),
            dist_stalled: self.dist_tracker.is_stalled(Duration::from_secs(2)),
            current_ref_timeout: self.get_ref_timeout(),
            current_dist_timeout: self.get_dist_timeout(),
            in_deadlock: self.detect_deadlock(),
        }
    }

    fn print_stats(&self, task_id: usize) {
        let health = self.get_sync_health();
        println!("\n=== [TASK {}] Synchronization Statistics ===", task_id);
        println!("  Dropped frames: REF={}, DIST={}", self.dropped_ref_count, self.dropped_dist_count);
        println!("  Buffer sizes: REF={}, DIST={}", health.ref_buffer_size, health.dist_buffer_size);
        println!("  Frame ID gap: {}", health.frame_id_gap);
        println!("  Matching pairs ready: {}", health.matching_frames);
        println!("  Adaptive timeouts: REF={:.2}s, DIST={:.2}s", 
            health.current_ref_timeout.as_secs_f32(),
            health.current_dist_timeout.as_secs_f32());
        println!("  Stream status: REF stalled={}, DIST stalled={}", 
            health.ref_stalled, health.dist_stalled);
        if health.in_deadlock {
            println!("  ⚠️  DEADLOCK RISK DETECTED!");
        }
        println!("==========================================\n");
    }
}



struct DigitReader {
    // Templates for digits 0-9. Key is the digit (0-9), Value is the binary pixel mask
    templates: HashMap<u8, Vec<u8>>, 
    char_w: usize,
    char_h: usize,
    start_x: usize,
    start_y: usize,
}

impl DigitReader {
    fn new(start_x: usize, start_y: usize, char_w: usize, char_h: usize) -> Self {
        Self {
            templates: HashMap::new(),
            char_w, char_h, start_x, start_y
        }
    }

    /// Helper to extract a binary mask of a specific digit slot
    fn extract_patch(&self, rgb: &[u8], width: usize, digit_index: usize) -> Vec<u8> {
        let mut patch = Vec::with_capacity(self.char_w * self.char_h);
        // Assuming digits grow to the right: "123" -> '1' at x, '2' at x+w, ...
        let patch_x = self.start_x + (digit_index * self.char_w);
        
        for y in 0..self.char_h {
            for x in 0..self.char_w {
                let px_idx = ((self.start_y + y) * width + (patch_x + x)) * 3;
                let r = rgb[px_idx];
                let g = rgb[px_idx+1];
                let b = rgb[px_idx+2];
                // Simple thresholding: Text is White, Box is Black
                // Value 1 = Text, 0 = Background
                patch.push(if r > 128 || g > 128 || b > 128 { 1 } else { 0 });
            }
        }
        patch
    }

    /// Learn a digit from the clean Reference stream
    fn learn_digit(&mut self, rgb: &[u8], width: usize, number: u32) {
        let s = number.to_string();
        for (i, char_digit) in s.chars().enumerate() {
            let digit = char_digit.to_digit(10).unwrap() as u8;
            if !self.templates.contains_key(&digit) {
                let mask = self.extract_patch(rgb, width, i);
                // Sanity check: Don't learn empty black squares
                if mask.iter().filter(|&&v| v == 1).count() > 5 { 
                     self.templates.insert(digit, mask);
                     // println!(">> Learned Template for digit '{}'", digit);
                }
            }
        }
    }

    /// Recognize the number in a (possibly distorted) frame
    fn recognize(&self, rgb: &[u8], width: usize) -> Option<u32> {
        if self.templates.is_empty() { return None; }

        let mut result_str = String::new();
        // We scan up to 5 digit slots (supports up to frame 99999)
        for i in 0..5 {
            let patch = self.extract_patch(rgb, width, i);
            let active_pixels = patch.iter().filter(|&&v| v == 1).count();
            
            // Stop if we hit an empty space (end of number)
            if active_pixels < 5 { break; } 

            // Find best matching template (Lowest Hamming Distance / XOR)
            let mut best_digit = None;
            let mut min_diff = usize::MAX;

            for (&digit, tmpl) in &self.templates {
                // XOR diff count
                let diff = patch.iter().zip(tmpl.iter())
                    .filter(|(a, b)| a != b).count();
                
                if diff < min_diff {
                    min_diff = diff;
                    best_digit = Some(digit);
                }
            }

            // Heuristic: If the best match still has > 30% difference, it's garbage/noise
            let threshold = (self.char_w * self.char_h) / 3; 
            if min_diff < threshold {
                if let Some(d) = best_digit {
                    result_str.push_str(&d.to_string());
                }
            } else {
                 break; // Unrecognizable char, stop parsing
            }
        }

        if result_str.is_empty() { None } else { result_str.parse::<u32>().ok() }
    }
}



#[derive(Debug, Deserialize)]
struct FrameData {
    frame_index: u32,
    timestamp: f64,
    nominal_bitrate: f64,
}

#[derive(Clone)]
struct FrameBuf {     // structure for having synthetic frames replacing losses. Idea is to filter them out of analysis later, 
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
                    feature='name=psnr':feature='name=float_ssim'",
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
        // println!("METRICS: \n{j}");

        let vmaf_score = j["pooled_metrics"]["vmaf"]["mean"].as_f64().unwrap_or(0.0);

        let ssim_score = j["pooled_metrics"]["float_ssim"]["mean"]
            .as_f64()
            .unwrap_or(0.0);

        // ─────────── log & emit ───────────
        print_green!(
            "Task {} - T:{:.3} [{}] | Frame {} : VMAF {:.2}, SSIM {:.4}",
            self.task_id, 
            timestamp_ms,
            ip_client,
            frame_number,
            vmaf_score,
            ssim_score
        );

        let fm = FrameMetrics {
            frame_number,
            timestamp_ms,
            vmaf: vmaf_score,
            psnr: 0.0,
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
/* Helper to draw a hollow rectangle for debug boxes */
fn draw_debug_rect(buf: &mut [u32], buf_w: usize, x: usize, y: usize, w: usize, h: usize, color: u32) {
    // Top & Bottom lines
    for i in x..(x + w).min(buf_w) {
        let top_idx = y * buf_w + i;
        let bot_idx = (y + h) * buf_w + i;
        if top_idx < buf.len() { buf[top_idx] = color; }
        if bot_idx < buf.len() { buf[bot_idx] = color; }
    }
    // Left & Right lines
    for j in y..(y + h) {
        let left_idx = j * buf_w + x;
        let right_idx = j * buf_w + (x + w);
        if left_idx < buf.len() && x < buf_w { buf[left_idx] = color; }
        if right_idx < buf.len() && (x + w) < buf_w { buf[right_idx] = color; }
    }
}
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
    let sw = (W as f64 * SCALE_FACTOR_WINDOW) as usize;
    let sh = (H as f64 * SCALE_FACTOR_WINDOW) as usize;
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

    let debug_x = (OCR_X as f64 * SCALE_FACTOR_WINDOW) as usize;
    let debug_y = (OCR_Y as f64 * SCALE_FACTOR_WINDOW) as usize;
    let debug_w =((OCR_W as f64 * 5.0) * SCALE_FACTOR_WINDOW) as usize; // *5 for 5 digits
    let debug_h = (OCR_H as f64 * SCALE_FACTOR_WINDOW) as usize;

    // Draw RED box on Left (Distorted)
    draw_debug_rect(&mut buf, ww, debug_x, debug_y, debug_w, debug_h, 0xFF0000);

    // Draw GREEN box on Right (Reference)
    // Offset by (sw + 10) to move to the second panel
    draw_debug_rect(&mut buf, ww, debug_x + sw + 10, debug_y, debug_w, debug_h, 0x00FF00);

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
    video_codec: VideoCodec, 
) {
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
            enc.start_chunking(bitrate_mbps, now).await;

            // drain all frames this chunk produced (but never overrun our trace)
            while produced < trace.len() {
                // try to grab the next packet
                if let Some(pkt) = enc.next_frame().await {
                    let info = &trace[produced];
                    produced += 1;

                    let millis_sleep = (1000.0 / framerate_fps) as u64;
                    async_std::task::sleep(Duration::from_millis(millis_sleep)).await;

                    // simulate loss only on the “low” path
                    if !simulate_loss || !info.lost {
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
) {
    std::thread::spawn(move || {

        let filter_str = format!("drawtext=fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf: text='%{{n}}': x=10: y=10: fontsize=96: fontcolor=white: box=1: boxcolor=black@0.5");
        
        let mut child = Command::new("ffmpeg")
            .args(&[
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
            // Read exactly one frame
            if stdout.read_exact(&mut buffer).is_err() {
                break; // End of stream or error
            }
            // Send (ID, RGB_Data)
            // If receiver is dropped (Ctrl+C), this returns Err and we break
            if tx.send((frame_idx, buffer.clone())).is_err() {
                break; 
            }
            frame_idx += 1;
        }

        //Kill ffmpeg when we are done!
        let _ = child.kill(); 
        let _ = child.wait(); // Clean up process entry
    });
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
) -> Result<()> {

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
    let caps = Regex::new(r"XR_stats_(\d+)\.csv$")?.captures(&file_name).expect("filename mismatch");
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


    // Read CSV Trace
    let mut rdr = csv::ReaderBuilder::new().has_headers(true).from_path(&trace_csv)?;
    
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
    
    // Hardcoded defaults since XR_stats might not have them in row 1
    let offset_video = 0.0; 

    for (i, result) in rdr.records().enumerate() {
        let rec = result?;
        
        // Debug first row if it fails
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
                // This print will tell you EXACTLY what is failing
                if i < 5 { // Only print first few errors
                    eprintln!("Skipping row {}: Cannot parse ID '{}' as number. Error: {}", i, id_str, e);
                }
            }
        }
    }
    
    if raw_ids.is_empty() {
        return Err(anyhow::anyhow!("No valid frames found in CSV after parsing. Check column mapping."));
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

    // A) Distorted Encoder (Driven by CSV)
    let (tx_enc, rx_enc) = bounded::<(usize, u32, Vec<u8>)>(BOUNDED_CHANNEL_SIZE); // limited capacity to prevent OOM
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
        codec,
    );

    // B) Reference Reader (Direct from MP4)
    let (tx_ref, rx_ref) = bounded::<(u32, Vec<u8>)>(BOUNDED_CHANNEL_SIZE);
    make_reference_reader_task(
        ref_video_path.clone(),
        WIDTH_ENCODER,
        HEIGHT_ENCODER,
        tx_ref
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
            sw * 2 + 10, sh, WindowOptions::default(),
        )?)
    } else {
        None
    };


    const FRAME_MATCH_TIMEOUT: Duration = Duration::from_secs(10);
    let mut sync_manager = FrameSyncManager::new();
    
    // Your existing initialization
    let mut vmaf_tasks = FuturesUnordered::new();
    let sem = Arc::new(Semaphore::new(num_cpus::get().min(4)));
    let mut enc_done = false;
    let mut digit_reader = DigitReader::new(OCR_X, OCR_Y, OCR_W, OCR_H);
    
    let mut iteration_count = 0;
    
    loop {
        iteration_count += 1;
        
        // A. THROTTLE VMAF
        if vmaf_tasks.len() >= MAX_PENDING_TASKS {
            if let Some(res) = vmaf_tasks.next().await {
                if let Err(e) = res { eprintln!("Task Error: {}", e); }
            }
        }

        // B. ADAPTIVE TIMEOUT CHECK
        sync_manager.cleanup_expired_frames(task_id);

        // C. CHECK BUFFER PRESSURE
        let (throttle_ref, throttle_dist) = sync_manager.should_throttle_reads();
        
        // D. READ REFERENCE (with throttling)
        if !throttle_ref && sync_manager.ref_buffer.len() < MAX_VMAF_BUFFER_SIZE {
            match rx_ref.try_recv() {
                Ok((trusted_id, rgb)) => {
                    digit_reader.learn_digit(&rgb, WIDTH_ENCODER, trusted_id);
                    sync_manager.insert_ref_frame(trusted_id, FrameBuf { rgb, synthetic: false });
                },
                Err(_) => {}
            }
        }

        // E. READ DISTORTED (with throttling)
        if !throttle_dist && sync_manager.dist_buffer.len() < MAX_VMAF_BUFFER_SIZE {
            // Ingest packets
            if !enc_done {
                match rx_enc.try_recv() {
                    Ok((_, _pkt_id, pkt)) => dec_enc.process_packet(pkt, _pkt_id),
                    Err(crossbeam_channel::TryRecvError::Disconnected) => enc_done = true,
                    Err(_) => {}
                }
            }

            // Decode & recognize
            while let Some((rgb, _)) = dec_enc.next_decoded_frame() {
                if let Some(visual_id) = digit_reader.recognize(&rgb, WIDTH_ENCODER) {
                    sync_manager.insert_dist_frame(visual_id, FrameBuf { rgb, synthetic: false });
                } else {
                    // Could not recognize - frame might be corrupted
                }
            }
        }

        // F. PROCESS PAIRS (with improved matching)
        let ready_pairs = sync_manager.find_ready_pairs(MAX_PENDING_TASKS - vmaf_tasks.len());

        for id in ready_pairs {
            // Extract frames
            let fb_ref = sync_manager.ref_buffer.remove(&id).unwrap().frame;
            let fb_dist = sync_manager.dist_buffer.remove(&id).unwrap().frame;

            let ts = *ts_map.get(&id).unwrap_or(&0.0);
            sync_manager.last_processed_id = id as i64;

            // GUI update (if enabled)
            if let Some(ref mut w) = window {
                let _ = draw_pair(w.inner(), &fb_dist.rgb, &fb_ref.rgb, &scenario, id, ts);
                w.inner().update();
            }

            // Spawn VMAF task
            let logger = metric.clone();
            let sem_clone = sem.clone();
            let ip_clone = ip.clone();
            let r_rgb = fb_ref.rgb;
            let d_rgb = fb_dist.rgb;

            vmaf_tasks.push(tokio::spawn(async move {
                let _p = sem_clone.acquire().await.unwrap();
                let _ = logger.process_frame_buffers(id as u64, ts, r_rgb, d_rgb, ip_clone).await;
            }));
        }

        // G. CLEANUP STALE FRAMES (sliding window)
        sync_manager.cleanup_stale_frames(100);

        // H. PERIODIC STATS
        if iteration_count % 1000 == 0 {
            sync_manager.print_stats(task_id);
        }

        // I. EXIT CONDITION
        if enc_done && sync_manager.dist_buffer.is_empty() && vmaf_tasks.is_empty() {
            println!(">> Trace processing complete. Max ID processed: {}", sync_manager.last_processed_id);
            sync_manager.print_stats(task_id);
            break;
        }

        // Small sleep to prevent busy loop
        tokio::time::sleep(Duration::from_millis(1)).await;
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

    let results_scenarios_folder = "/home/boris/Desktop/Rust_MG1/asynchronix/Results_test2/"; 
    let dummy_ip = "127.0.0.1".parse().unwrap();

    // Regex compilation (done once)
    let re_codec = Arc::new(Regex::new(r"_Codec([^_]+)").unwrap());
    let re_fps =   Arc::new(Regex::new(r"_FPS(\d+)").unwrap());
    let re_video = Arc::new(Regex::new(r"_([^_]+)_FPS").unwrap());

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
        let video_name = re_video.captures(&folder_name).map(|c| c.get(1).unwrap().as_str().to_string());

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

        // Determine GUI usage (logic preserved)
        let user = std::env::var("USER").unwrap_or_default();
        let use_gui = match user.as_str() {
            "boris" => true,
            "fmaura" => false,
            _ => std::env::var("DISPLAY").is_ok(),
        };

        // Find CSV within folder
        let csv_entries = fs::read_dir(&path).expect("Read subdir failed");
        for file in csv_entries.flatten() {
            let p = file.path();
            if p.extension().map_or(false, |ext| ext == "csv") {
                let fname = p.file_name().unwrap().to_string_lossy().into_owned();
                
                // Only process specific trace files
                if fname.starts_with("XR_stats_0") {
                    
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


pub async fn main_serial() { // works but does one thread at a time. 
    let results_scenarios_folder = "/home/boris/Desktop/Rust_MG1/asynchronix/Results_test/"; 
    let dummy_ip = "127.0.0.1".parse().unwrap();

    let re_codec = Regex::new(r"_Codec([^_]+)").unwrap();
    let re_fps =   Regex::new(r"_FPS(\d+)").unwrap();
    let re_video = Regex::new(r"_([^_]+)_FPS").unwrap();

    let entries = fs::read_dir(results_scenarios_folder).expect("Read dir failed");

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() { continue; }

        let folder_name = path.file_name().unwrap().to_string_lossy().to_string();

        // 1. Extract Metadata
        let codec_str = re_codec.captures(&folder_name).map_or("Unknown", |c| c.get(1).unwrap().as_str());
        let fps = re_fps.captures(&folder_name).map_or(0, |c| c[1].parse::<u32>().unwrap_or(0));
        let video_name = re_video.captures(&folder_name).map_or("Unknown", |c| c.get(1).unwrap().as_str());

        let video_codec = match codec_str {
            "AV1" => VideoCodec::AV1,
            "HEVC" => VideoCodec::HEVC,
            _ => { eprintln!("Skipping {}, unknown codec", folder_name); continue; }
        };

        if fps == 0 || video_name == "Unknown" {
            eprintln!("Skipping {}, couldn't parse FPS or Video Name", folder_name);
            continue;
        }

        println!("Found Scenario: {} | Video: {} | FPS: {} | Codec: {:?}", folder_name, video_name, fps, video_codec);

        let user = std::env::var("USER").unwrap_or_default();
        let use_gui = match user.as_str() {
            "boris" => true,
            "fmaura" => false,
            _ => std::env::var("DISPLAY").is_ok(), // Fallback to display check for anyone else
        };
        
        // 2. Find the CSV file inside the folder
        let csv_entries = fs::read_dir(&path).expect("Read subdir failed");
        for file in csv_entries.flatten() {

            let p = file.path();
            let parent_results = p.parent()           // scenario_folder
                                .and_then(|p| p.parent()) // Results_test
                                .and_then(|p| p.file_name()) // Get just the folder name
                                .map(|n| n.to_string_lossy().into_owned())
                                .unwrap_or_else(|| "Unknown".to_string());

            if p.extension().map_or(false, |ext| ext == "csv") {
                let fname = p.file_name().unwrap().to_string_lossy().into_owned();
                // Ensure it matches your trace file naming convention
                if fname.starts_with("XR_stats_0") {
                    println!("   -> Processing Trace: {}", fname);
                    
                    // 3. Run the processing
                    if let Err(e) = process_trace_vs_original(
                        p, 
                        dummy_ip, 
                        video_codec, 
                        fps, 
                        video_name.to_string(),
                        use_gui, 
                        &parent_results, 
                        1, 

                    ).await {
                        eprintln!("ERROR processing {}: {}", fname, e);
                    }
                }
            }
        }
    }
}
