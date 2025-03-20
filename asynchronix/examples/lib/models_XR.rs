use crate::lib::alvr_control_socket::{
    framed_recv, framed_recv_vec, ControlSocketReceiver, ControlSocketSender,
};
use crate::lib::{
    alvr_stream_socket::{Buffer, StreamReceiver},
    HeuristicStats,
};
use rand::distributions::Uniform;
use rand::rngs::StdRng;
use rand::SeedableRng;
use rand::{thread_rng, Rng};
use rand_distr::{Distribution, Normal};
use std::{
    fs::write,
    io,
    process::{ChildStdin, ChildStdout, Stdio},
};

use crate::lib::alvr_packets::{DeviceMotion, Pose};
use crate::lib::HevcParser;
use anyhow::Result;
use async_std::stream::StreamExt; // Add this import to fix the .next() error
use std::cell::RefCell;
use std::error::Error;
use std::fs;
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::Ipv4Addr;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread_local;
use tempfile::TempDir;
use tokio::sync::Mutex as tokMutex;
use tokio::sync::Semaphore;

use minifb::{Window, WindowOptions};
use std::{fs::File, thread, write};

use crate::debug_print;
use crate::format_elapsed;
use crate::lib::{HeaderALVRStream, USE_FFMPEG, USE_VMAF};
use crate::print_pretty;
use crate::{debug_bgprint, print_prettyy, print_prettyyy};
use core::{f64, net};
use ffmpeg_sidecar::command::FfmpegCommand;
use glam::{Quat, Vec3};
use once_cell::sync::Lazy;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::net::IpAddr;
use std::thread::yield_now;
use std::time::SystemTime;
use std::time::{Duration, Instant};
use std::{mem, vec};

use minifb_fonts::font6x8;

use crate::lib::alvr_control_socket::ProtoControlSocket;
use crate::lib::alvr_packets::{ClientControlPacket, ClientStatistics, NetworkStatisticsPacket};
use crate::lib::alvr_stream_socket::{
    parse_shard_data, ConnectionError, DscpTos, Haptics, ReceiverData, SocketBufferSize,
    SocketProtocol, SocketReader, StreamSender, StreamSocketBuilder, Tracking, VideoPacketHeader,
};
use crate::lib::alvr_stream_socket::{
    AUDIO, HAPTICS, INITIAL_FRAMERATE_FPS, MAX_HISTORY_SIZE, STATISTICS, TRACKING, VIDEO,
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

use crate::lib::alvr_statistics::StatisticsManager;
use crate::lib::{exponential, AmpduPacket, Coords, DebugColor, MpduPacket, SlidingWindowAverage};
// use crate::lib::INITIAL_BITRATE_MBPS_SIM;
use super::alvr_packets::DeadlineShardlossStatPacket;
use super::alvr_stream_socket::{
    SocketWriter, StreamSocket, IDR_FRAME_SIZE_GOP, MAX_PACKET_SIZE_RECV,
};
use super::alvr_stream_socket::{CONTROL_STREAM, MAX_DEADLINE_IN_STATS};
use super::{SlidingWindowTimely, _INITIAL_BITRATE_MBPS_SIM};
// use async_process::Child;
use lazy_static::lazy_static;

use std::collections::BTreeMap;

pub const WIDTH_ENCODER: usize = 1920;
pub const HEIGHT_ENCODER: usize = 1080;

pub const FRAMERATE_WINDOWS: usize = 60;

pub const SCALE_FACTOR_WINDOW: f64 = 0.35;
pub const VMAF_BATCH_SIZE: usize = 10; // Process 10 frames at a time
pub const VMAF_BATCH_TIMEOUT_MS: u64 = 1000; // Process batch after 1 second even if

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
pub const TARGET_FRAMES_DECODER_QUEUE: usize = DECODER_BUFFERING_FRAMES / 2;

pub const VMAF_FRAME_GROUP_SIZE: usize = 10;
pub const TARGET_TIMESTAMP_TRACKING: Duration = Duration::from_millis(10);
pub const KEEP_FRAMES_DISK_INDEX: usize = 200;

// static _STATISTICS_MANAGER: OptLazy<StatisticsManager> = lazy_mut_none();

use crossbeam::channel::{bounded, unbounded, Receiver, Sender, TryRecvError};

lazy_static! {
    static ref REFERENCE_DECODERS: Arc<Mutex<HashMap<IpAddr, SynchronizedDecoder>>> =
        Arc::new(Mutex::new(HashMap::new()));
}

pub struct FramePair {
    decoded: Option<Vec<u32>>,
    reference: Option<Vec<u32>>,
    decoded_raw: Option<Vec<u8>>,
    reference_raw: Option<Vec<u8>>,
    frame_id: usize,
}

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

fn resize_buffer(
    buffer: &[u32],
    orig_width: usize,
    orig_height: usize,
    new_width: usize,
    new_height: usize,
) -> Vec<u32> {
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

// Advanced frame similarity computation with configurable thresholds
// Implements perceptual frame comparison techniques with multi-scale analysis
fn compute_enhanced_frame_similarity(
    frame1: &[u8],
    frame2: &[u8],
    width: usize,
    height: usize,
) -> f64 {
    // Return maximum difference if frames are incompatible
    if frame1.len() != frame2.len() || frame1.len() != width * height * 3 {
        return 1.0;
    }

    // Configuration parameters for multi-scale analysis
    const BLOCK_SIZES: [usize; 3] = [4, 16, 64]; // Multi-scale block sizes
    const WEIGHTS: [f64; 3] = [0.5, 0.3, 0.2]; // Relative importance of each scale
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
                            let g_diff =
                                (frame1[idx + 1] as i32 - frame2[idx + 1] as i32).abs() as f64;
                            let b_diff =
                                (frame1[idx + 2] as i32 - frame2[idx + 2] as i32).abs() as f64;

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
                    let avg_diff = (block_diff_r * PERCEPTUAL_WEIGHTS[0]
                        + block_diff_g * PERCEPTUAL_WEIGHTS[1]
                        + block_diff_b * PERCEPTUAL_WEIGHTS[2])
                        / (block_samples as f64 * 255.0); // Normalize to [0-1]

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

/// A synchronized frame with timestamp information
#[derive(Clone, Debug)]
pub struct TimestampedFrame {
    pub data: Vec<u8>,
    pub capture_timestamp: Instant, // When the frame was captured/encoded
    pub decode_timestamp: Instant,  // When the frame was decoded
    pub presentation_timestamp: u64, // Logical PTS (in frame count units)
    pub frame_index: usize,         // Frame sequence number
    pub is_keyframe: bool,          // Whether this is a keyframe
}

impl TimestampedFrame {
    pub fn new(data: Vec<u8>, frame_index: usize, is_keyframe: bool) -> Self {
        Self {
            data,
            capture_timestamp: Instant::now(),
            decode_timestamp: Instant::now(),
            presentation_timestamp: frame_index as u64,
            frame_index,
            is_keyframe,
        }
    }
}

/// A synchronized frame queue for managing temporal alignment
pub struct SyncFrameQueue {
    frames: BTreeMap<u64, TimestampedFrame>,
    max_buffer_size: usize,
    sync_threshold_ms: u64,
    decoder_id: String,
}

impl SyncFrameQueue {
    pub fn new(decoder_id: &str, max_buffer_size: usize, sync_threshold_ms: u64) -> Self {
        Self {
            frames: BTreeMap::new(),
            max_buffer_size,
            sync_threshold_ms,
            decoder_id: decoder_id.to_string(),
        }
    }

    pub fn add_frame(&mut self, frame: TimestampedFrame) {
        // Insert frame using its PTS as key for temporal ordering
        self.frames.insert(frame.presentation_timestamp, frame);

        // Enforce buffer size limit by removing oldest frames when needed
        if self.frames.len() > self.max_buffer_size {
            if let Some(oldest_key) = self.frames.keys().next().cloned() {
                self.frames.remove(&oldest_key);
                println!(
                    "{} 🔄 Removed oldest frame from buffer (PTS: {})",
                    self.decoder_id, oldest_key
                );
            }
        }
    }

    pub fn get_frame_at_pts(
        &mut self,
        target_pts: u64,
        tolerance_ms: u64,
    ) -> Option<TimestampedFrame> {
        // First try to get exact PTS match
        if let Some(frame) = self.frames.remove(&target_pts) {
            return Some(frame);
        }

        // If no exact match, find closest frame within tolerance
        let mut closest_pts = None;
        let mut min_distance = tolerance_ms;

        for &pts in self.frames.keys() {
            let distance = if pts > target_pts {
                pts - target_pts
            } else {
                target_pts - pts
            };

            if distance < min_distance {
                min_distance = distance;
                closest_pts = Some(pts);
            }
        }

        if let Some(pts) = closest_pts {
            println!(
                "{} ⌛ Found approximate PTS match: requested={}, actual={} (diff={}ms)",
                self.decoder_id, target_pts, pts, min_distance
            );
            return self.frames.remove(&pts);
        }

        None
    }

    pub fn get_ready_frames(&self) -> Vec<u64> {
        self.frames.keys().cloned().collect()
    }

    pub fn buffer_size(&self) -> usize {
        self.frames.len()
    }

    pub fn clear(&mut self) {
        self.frames.clear();
    }
}

/// Shared synchronization state between decoders
pub struct DecoderSyncManager {
    frame_queues: BTreeMap<String, SyncFrameQueue>,
    next_pts_to_display: u64,
    sync_threshold_ms: u64,
    last_sync_time: Instant,
    primary_decoder_id: String,
}

impl DecoderSyncManager {
    pub fn new(primary_decoder_id: &str, sync_threshold_ms: u64) -> Self {
        Self {
            frame_queues: BTreeMap::new(),
            next_pts_to_display: 0,
            sync_threshold_ms,
            last_sync_time: Instant::now(),
            primary_decoder_id: primary_decoder_id.to_string(),
        }
    }

    pub fn register_decoder(&mut self, decoder_id: &str, max_buffer_size: usize) {
        if !self.frame_queues.contains_key(decoder_id) {
            self.frame_queues.insert(
                decoder_id.to_string(),
                SyncFrameQueue::new(decoder_id, max_buffer_size, self.sync_threshold_ms),
            );
            println!("🔄 Registered decoder {} with sync manager", decoder_id);
        }
    }

    pub fn add_frame(&mut self, decoder_id: &str, frame: TimestampedFrame) {
        if let Some(queue) = self.frame_queues.get_mut(decoder_id) {
            queue.add_frame(frame);
        } else {
            println!(
                "⚠️ Attempted to add frame to unregistered decoder: {}",
                decoder_id
            );
        }
    }

    /// Check if all decoders have frames ready at the current sync point
    pub fn are_frames_ready(&self) -> bool {
        let target_pts = self.next_pts_to_display;

        // All registered decoders must have at least one frame
        for (id, queue) in &self.frame_queues {
            if queue.buffer_size() == 0 {
                return false;
            }

            // Check if at least one decoder has the exact PTS we're looking for
            if id == &self.primary_decoder_id {
                if !queue.frames.contains_key(&target_pts) {
                    return false;
                }
            }
        }

        true
    }

    /// Retrieve synchronized frames from all decoders at the current PTS
    pub fn get_synchronized_frames(&mut self) -> Option<BTreeMap<String, TimestampedFrame>> {
        if !self.are_frames_ready() {
            return None;
        }

        let target_pts = self.next_pts_to_display;
        let mut result = BTreeMap::new();

        // First ensure the primary decoder has this frame
        if let Some(queue) = self.frame_queues.get_mut(&self.primary_decoder_id) {
            if let Some(primary_frame) = queue.get_frame_at_pts(target_pts, self.sync_threshold_ms)
            {
                result.insert(self.primary_decoder_id.clone(), primary_frame);
            } else {
                // If primary doesn't have the target frame, we can't sync yet
                return None;
            }
        } else {
            // Primary decoder not registered
            return None;
        }

        // Then get matching frames from other decoders
        for (id, queue) in self.frame_queues.iter_mut() {
            if id != &self.primary_decoder_id {
                if let Some(frame) = queue.get_frame_at_pts(target_pts, self.sync_threshold_ms) {
                    result.insert(id.clone(), frame);
                }
            }
        }

        // Increment for next time if we succeeded
        if result.len() == self.frame_queues.len() {
            self.next_pts_to_display += 1;
            self.last_sync_time = Instant::now();

            return Some(result);
        }

        None
    }

    pub fn reset(&mut self) {
        for queue in self.frame_queues.values_mut() {
            queue.clear();
        }
        self.next_pts_to_display = 0;
        self.last_sync_time = Instant::now();
        println!("🔄 Decoder sync manager reset");
    }
}

fn find_best_frame_match(
    regular_frames: &[(Vec<u8>, Vec<u32>)],
    max_frames: &[(Vec<u8>, Vec<u32>)],
) -> (usize, usize, f64) {
    let mut best_regular_idx = 0;
    let mut best_max_idx = 0;
    let mut best_similarity = 1.0; // Start with worst similarity (1.0 = completely different)

    // Compute similarity for all possible frame pairs
    for (reg_idx, (reg_raw, _)) in regular_frames.iter().enumerate() {
        for (max_idx, (max_raw, _)) in max_frames.iter().enumerate() {
            // Use an enhanced frame similarity metric
            let similarity =
                compute_enhanced_frame_similarity(reg_raw, max_raw, WIDTH_ENCODER, HEIGHT_ENCODER);

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

pub struct SynchronizedDecoder {
    regular_decoder: HevcDecoder,
    max_decoder: HevcDecoder,
    frame_queue: VecDeque<FramePair>, // might contain some pairs from earlier logic
    output_queue: VecDeque<FramePair>, // all synchronized pairs are pushed here
    throttle_semaphore: Arc<Semaphore>,
    next_frame_id: Arc<AtomicUsize>,
    toggle_output: bool, // new field to alternate between queues
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
            output_queue: VecDeque::new(),
            throttle_semaphore,
            next_frame_id: Arc::new(AtomicUsize::new(0)),
            toggle_output: false, // start with false
        }
    }

    fn synchronize_frame_buffers(&mut self) {
        // Only proceed if the output queue is almost empty.
        if self.output_queue.len() > 1 {
            return;
        }

        // Process pending decoded frames from both decoders.
        let regular_count = self.regular_decoder.process_decoded_frames();
        let max_count = self.max_decoder.process_decoded_frames();

        if regular_count == 0 || max_count == 0 {
            return; // Need frames from both decoders.
        }

        // Collect up to 5 decoded frames from each decoder.
        let mut regular_decoded: Vec<(Vec<u8>, Vec<u32>)> = Vec::new();
        let mut max_decoded: Vec<(Vec<u8>, Vec<u32>)> = Vec::new();

        for _ in 0..20 {
            if let Some((raw, _timestamp)) = self.regular_decoder.next_decoded_frame() {
                if let Some(pixels) = convert_rgb_to_u32(&raw, WIDTH_ENCODER, HEIGHT_ENCODER) {
                    regular_decoded.push((raw, pixels));
                }
            }
            if let Some((raw, _timestamp)) = self.max_decoder.next_decoded_frame() {
                if let Some(pixels) = convert_rgb_to_u32(&raw, WIDTH_ENCODER, HEIGHT_ENCODER) {
                    max_decoded.push((raw, pixels));
                }
            }
        }

        // Only continue if we have frames from both decoders.
        if !regular_decoded.is_empty() && !max_decoded.is_empty() {
            // Find the best matching frames using your similarity metric.
            let (best_regular_idx, best_max_idx, similarity) =
                find_best_frame_match(&regular_decoded, &max_decoded);

            // If the similarity is below threshold, create a synchronized pair.
            if similarity < 0.015 {
                let frame_id = self.next_frame_id.fetch_add(1, Ordering::SeqCst);
                let sync_pair = FramePair {
                    decoded: Some(regular_decoded[best_regular_idx].1.clone()),
                    reference: Some(max_decoded[best_max_idx].1.clone()),
                    decoded_raw: Some(regular_decoded[best_regular_idx].0.clone()),
                    reference_raw: Some(max_decoded[best_max_idx].0.clone()),
                    frame_id,
                };
                self.output_queue.push_back(sync_pair);

            }

            // Buffer extra frames so that none are dropped.
            // If the regular side has extra frames (i.e. best_regular_idx > best_max_idx),
            // we create pairs by duplicating the matched max frame.
            if best_regular_idx > best_max_idx {
                let duplicate_max_pixels = max_decoded[best_max_idx].1.clone();
                let duplicate_max_raw = max_decoded[best_max_idx].0.clone();
                // Buffer the extra regular frames that occur before the best match.
                for i in 0..(best_regular_idx - best_max_idx) {
                    // Ensure the index exists.
                    if i < regular_decoded.len() {
                        let frame_id = self.next_frame_id.fetch_add(1, Ordering::SeqCst);
                        let pair = FramePair {
                            decoded: Some(regular_decoded[i].1.clone()),
                            reference: Some(duplicate_max_pixels.clone()),
                            decoded_raw: Some(regular_decoded[i].0.clone()),
                            reference_raw: Some(duplicate_max_raw.clone()),
                            frame_id,
                        };
                        self.output_queue.push_back(pair);
                        // println!(
                        //     "Buffered extra regular frame as pair #{} (duplicating max)",
                        //     frame_id
                        // );
                    }
                }
            }
            // Otherwise, if the max side has extra frames (i.e. best_max_idx > best_regular_idx),
            // we duplicate the matched regular frame.
            else if best_max_idx > best_regular_idx {
                let duplicate_regular_pixels = regular_decoded[best_regular_idx].1.clone();
                let duplicate_regular_raw = regular_decoded[best_regular_idx].0.clone();
                for i in 0..(best_max_idx - best_regular_idx) {
                    if i < max_decoded.len() {
                        let frame_id = self.next_frame_id.fetch_add(1, Ordering::SeqCst);
                        let pair = FramePair {
                            decoded: Some(duplicate_regular_pixels.clone()),
                            reference: Some(max_decoded[i].1.clone()),
                            decoded_raw: Some(duplicate_regular_raw.clone()),
                            reference_raw: Some(max_decoded[i].0.clone()),
                            frame_id,
                        };
                        self.output_queue.push_back(pair);
                        // println!(
                        //     "Buffered extra max frame as pair #{} (duplicating regular)",
                        //     frame_id
                        // );
                    }
                }
            }
        }
    }

    pub fn process_ref_only_sample(
        &mut self,
        max_frame: Vec<u8>,
        frame_id: usize,
    ) -> Option<(Vec<u8>, Vec<u32>)> {
        self.max_decoder.process_packet(max_frame);

        if let Some((raw, _timestamp)) = self.max_decoder.next_decoded_frame() {
            if let Some(pixels) = convert_rgb_to_u32(&raw, WIDTH_ENCODER, HEIGHT_ENCODER) {
                // max_decoded.push((raw, pixels));
                Some((raw, pixels))
            } else {
                None
            }
        } else {
            None
        }
    }

    // Modify process_frame_pair to use the new synchronization logic
    pub fn process_frame_pair(
        &mut self,
        regular_frame: Vec<u8>,
        max_frame: Vec<u8>,
        frame_id: usize,
    ) {
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
        if let (Some((regular_raw, regular_timestamp)), Some((max_raw, max_timestamp))) =
            (&regular_result, &max_result)
        {
            // Convert raw RGB frames to u32 pixels for display
            if let (Some(regular_pixels), Some(max_pixels)) = (
                convert_rgb_to_u32(regular_raw, WIDTH_ENCODER, HEIGHT_ENCODER),
                convert_rgb_to_u32(max_raw, WIDTH_ENCODER, HEIGHT_ENCODER),
            ) {
                // Create and store the frame pair
                let pair = FramePair {
                    decoded: Some(regular_pixels),
                    reference: Some(max_pixels),
                    decoded_raw: Some(regular_raw.clone()),
                    reference_raw: Some(max_raw.clone()),
                    frame_id,
                };

                // self.frame_queue.push_back(pair);
                self.output_queue.push_back(pair);
                println!("Created synchronized frame pair #{}", frame_id);
            }
        } else {
            println!(
                "Couldn't get frames from both decoders for frame #{}",
                frame_id
            );

            // If one decoder produced a frame but not the other, we have a problem
            // This should be rare with lockstep processing, but let's log it
            if regular_result.is_some() && max_result.is_none() {
                println!("Warning: Only regular decoder produced a frame");
            } else if regular_result.is_none() && max_result.is_some() {
                println!("Warning: Only max decoder produced a frame");
            }
        }
    }

    pub fn next_frame_pair(&mut self) -> Option<FramePair> {
        // If both queues have frames, alternate which one you return.
        if !self.frame_queue.is_empty() && !self.output_queue.is_empty() {
            let pair = if self.toggle_output {
                self.toggle_output = false;
                self.frame_queue.pop_front()
            } else {
                self.toggle_output = true;
                self.output_queue.pop_front()
            };
            if pair.is_some() {
                self.throttle_semaphore.add_permits(1);
            }
            return pair;
        }
        // Otherwise, if one of the queues is non-empty, return from that.
        if !self.frame_queue.is_empty() {
            let pair = self.frame_queue.pop_front();
            if pair.is_some() {
                self.throttle_semaphore.add_permits(1);
            }
            return pair;
        }
        if !self.output_queue.is_empty() {
            let pair = self.output_queue.pop_front();
            if pair.is_some() {
                self.throttle_semaphore.add_permits(1);
            }
            return pair;
        }
        None
    }
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

    decoder_string: String,

    // New fields for synchronization
    // shared_params: Option<Arc<SharedParameterSetManager>>,
    last_sync_generation: u64,
    force_keyframe_sync: bool,

    recovery_frames: usize,     // Counter for frames to skip during recovery
    pending_clear: bool,        // Flag to indicate decoder state should be reset
    initialization_phase: bool, // Flag for the decoder's initialization phase

    pending_frames: VecDeque<(Vec<u8>, Vec<u32>)>, // (raw, converted pixels)
}

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
            max_buffered_frames: 2,

            decoder_string: decoder_str.to_string(),

            // shared_params,
            last_sync_generation: 0,
            force_keyframe_sync: false,

            recovery_frames: 0,   // Counter for frames to skip during recovery
            pending_clear: false, // Flag to indicate decoder state should be reset
            initialization_phase: false, // Flag for the decoder's initialization phase
            pending_frames: VecDeque::new(),
        }
    }

    // Instead of skip_frame(), do:
    pub fn buffer_frame(&mut self, frame: (Vec<u8>, Vec<u32>)) {
        self.pending_frames.push_back(frame);
    }

    pub fn skip_frame(&mut self) {
        self.decoded_frames.pop_front();
        print_prettyy!(DebugColor::Red, "SKIPPED FRAME",);
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

        if has_param_sets {
            has_parameter_update = true;

            print_prettyy!(
                DebugColor::Cyan,
                "{} - Parameter sets found in packet: VPS: {}, SPS: {}, PPS: {}",
                self.decoder_string,
                vps.as_ref().map_or(0, |v| v.len()),
                sps.as_ref().map_or(0, |v| v.len()),
                pps.as_ref().map_or(0, |v| v.len()),
            );
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

        // After parameter update, enter recovery mode if not already there
        if has_parameter_update && self.recovery_frames == 0 && !is_keyframe {
            self.recovery_frames = 30; // Skip ~30 frames or until next keyframe
            print_prettyy!(
                DebugColor::Yellow,
                "{} - Parameter update detected, entering recovery mode for {} frames",
                self.decoder_string,
                self.recovery_frames,
            );
        }

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

// // New function to create a synchronized pair of decoders
// pub fn create_synchronized_decoders(
//     framerate: u32,
//     width: u32,
//     height: u32,
//     sync_threshold_ms: u64
// ) -> (HevcDecoder, HevcDecoder, Arc<Mutex<DecoderSyncManager>>) {
//     // Create sync manager
//     let sync_manager = Arc::new(Mutex::new(
//         DecoderSyncManager::new("primary_decoder", sync_threshold_ms)
//     ));

//     // Create decoders
//     let mut primary_decoder = HevcDecoder::new(framerate, width, height, "primary_decoder");
//     let mut secondary_decoder = HevcDecoder::new(framerate, width, height, "secondary_decoder");

//     // Connect decoders to sync manager
//     primary_decoder.connect_to_sync_manager(Arc::clone(&sync_manager), true);
//     secondary_decoder.connect_to_sync_manager(Arc::clone(&sync_manager), false);

//     (primary_decoder, secondary_decoder, sync_manager)
// }

#[derive(Clone)]
pub struct EncoderLatencyLimiter {
    pub max_saturation_multiplier: f32,
}
#[derive(Clone)]
pub struct DecoderLatencyLimiter {
    pub max_decoder_latency_ms: u64,
    pub latency_overstep_frames: usize,
    pub latency_overstep_multiplier: f32,
}
#[derive(Clone)]
pub enum BitrateMode {
    ConstantMbps(f32),
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
    },
}

#[allow(unused)]
#[derive(Clone)]
pub struct BitrateManager {
    last_frame_instant: TaiTime<0>,
    last_update_instant: TaiTime<0>,

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

    last_target_bitrate_bps: f32,
}

impl BitrateManager {
    pub fn report_encoded_frame_server(&mut self, now: TaiTime<0>) {
        print_prettyy!(
            DebugColor::Purple,
            "[bitrateManager] submitted encoded frame. avg_fps = {}",
            1.0 / self.frame_interval_average.get_average().as_secs_f32()
        );
        if self.last_frame_instant != TaiTime::EPOCH {
            let dur = now.duration_since(self.last_frame_instant);
            self.frame_interval_average.submit_sample(dur);
        }
        self.last_frame_instant = now;
    }

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

    pub fn one_pass_abr(&mut self, now: TaiTime<0>) -> f32 {
        let bitrate_bps = match self.bitrate_mode {
            BitrateMode::ConstantMbps(bitrate_mbps) => {
                self.last_target_bitrate_bps = bitrate_mbps as f32 * 1E6;
                self.last_target_bitrate_mbps = bitrate_mbps as f32;

                print_prettyy!(DebugColor::Navy, "CBR -> Bitrate = {} Mbps", bitrate_mbps);

                bitrate_mbps as f32 * 1e6
            }

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
                print_prettyy!(
                    DebugColor::Purple,
                    "{} ONE PASS OF NEST-VR!",
                    format_elapsed!(now)
                );

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
                print_prettyy!(
                    DebugColor::Purple,
                    "Server FPS = {}, nfr_thresh = {}, rtt_thresh = {}",
                    server_fps,
                    threshold_fps,
                    threshold_rtt
                );

                if heur_fps >= threshold_fps {
                    if rtt_avg_heur_s > threshold_rtt {
                        if random_prob >= threshold_u {
                            print_prettyy!(DebugColor::Purple, " BITRATE DECREASE",);

                            bitrate_bps -= steps_bps; // decrease bitrate by 1 step
                        }
                    } else {
                        if random_prob <= threshold_u {
                            print_prettyy!(DebugColor::Purple, " BITRATE INCREASE",);

                            bitrate_bps += steps_bps; // increase bitrate by 1 step
                        }
                    }
                } else {
                    bitrate_bps -= steps_bps; // decrease bitrate by 1 step
                    print_prettyy!(DebugColor::Purple, " BITRATE DECREASE 2",);
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
                    steps_mbps: steps_bps / 1e6,

                    network_heur_fps: heur_fps, // fps_rx
                    rtt_avg_heur_s: rtt_avg_heur_s,
                    random_prob: random_prob,

                    threshold_fps: threshold_fps,
                    threshold_rtt_s: threshold_rtt,
                    threshold_u: threshold_u,

                    capacity_estimated_mbps: estimated_capacity_bps / 1E6,

                    requested_bitrate_mbps: bitrate_bps / 1e6,
                };

                print_prettyy!(
                    DebugColor::Purple,
                    " ------NeSt-VR STATS-------: {:#?}",
                    heur_stats
                );

                self.last_target_bitrate_bps = bitrate_bps;
                self.last_target_bitrate_mbps = bitrate_bps / 1E6;
                bitrate_bps
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

    // pub fn report_timestamp_change_bitrate(&mut self, now: TaiTime<0>) {
    //     let dur = now.duration_since(TaiTime::EPOCH).as_secs_f64();
    //     // TODO: ACTUAL IMPLEMENTATION OF ABR, now just:

    //     // if dur < 5.0{
    //     //     self.last_target_bitrate_mbps = 10.0;
    //     // }
    //     // else if 5.0 <= dur && dur < 10.0 {
    //     //     self.last_target_bitrate_mbps = 0.01;
    //     // }
    //     if 10.0 <= dur && dur < 1000.0 {
    //         // self.last_target_bitrate_mbps = 10.0; // just CBR for now
    //     }
    //     // } else if 12.0 <= dur && dur < 25.0 {
    //     //     self.last_target_bitrate_mbps = 0.9;
    //     // } else if 25.0 <= dur && dur < 30.0 {
    //     //     self.last_target_bitrate_mbps = 10.0;
    //     // } else if 35.0 <= dur && dur < 45.0 {
    //     //     self.last_target_bitrate_mbps = 0.2;
    //     // } else if 45.0 <= dur && dur < 55.0 {
    //     //     self.last_target_bitrate_mbps = 10.0;
    //     // } else if 55.0 <= dur && dur < 65.0 {
    //     //     self.last_target_bitrate_mbps = 0.5;
    //     // } else if 65.0 <= dur && dur < 75.0 {
    //     //     self.last_target_bitrate_mbps = 10.0;
    //     // } else if 75.0 <= dur && dur < 85.0 {
    //     //     self.last_target_bitrate_mbps = 1.0;

    //     debug_bgprint!(
    //         DebugColor::Tan,
    //         "t = {}, [DBG bitrate set] {} Mbps",
    //         dur,
    //         self.last_target_bitrate_mbps,
    //     );
    // }
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
            last_frame_instant: TaiTime::EPOCH,
            last_update_instant: TaiTime::EPOCH,

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

            bitrate_mode: BitrateMode::ConstantMbps(initial_bitrate_mbps), // ONLY CBR FOR NOW!!!

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
    pub name_folder: String,
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
            name_folder: name_folder.to_string(),

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
                    if let Some((_, send_instant)) = map_clone.remove(&frame_id) {
                        rtt = now.duration_since(send_instant);
                        // println!("SEND INSTANT: {}, now: {}, rtt: {}", format_elapsed!(send_instant), format_elapsed!(now), rtt.as_secs_f32());
                    } else {
                        println!("frame {} RTT ZEROO!!!!!", network_stats.frame_index);
                        rtt = Duration::ZERO;
                    }

                    debug_bgprint!(DebugColor::Teal, "RTT = {:.9}", rtt.as_secs_f64());

                    let (peak_network_throughput_bps, frame_interarrival_s) =
                        self.STATISTICS_MANAGER.report_network_statistics(
                            network_stats,
                            rtt,
                            now,
                            self.bitrate_manager.last_target_bitrate_bps,
                        );

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
                        print_pretty!(
                            DebugColor::Red,
                            "[DBG_DEAD_RX server {}] Frame {} lost {} shards",
                            self.ip_self,
                            frame,
                            shard
                        );
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
                                if stream_id == VIDEO {
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

                // self.bitrate_manager.report_timestamp_change_bitrate(now);   // for programatically changing CBR bitrate

                if now.duration_since(self.bitrate_manager.last_update_instant)
                    >= Duration::from_secs(1)
                {
                    self.bitrate_manager.one_pass_abr(now);
                    self.bitrate_manager.last_update_instant = now;
                }
                let current_bitrate_mbps: f32 = self.bitrate_manager.last_target_bitrate_mbps;
                let max_bitrate_ladder_mbps: f32 = match self.bitrate_manager.bitrate_mode {
                    BitrateMode::NestVr {
                        max_bitrate_mbps, ..
                    } => max_bitrate_mbps, // Extract max_bitrate_mbps
                    _ => 100.0,
                };

                //
                // let current_bitrate_mbps: f32 = self.bitrate_manager.one_pass_abr(); // for ABR bitrates

                let mut buffer_emu = send_socket // generate the actual video frame data
                    .get_buffer_emu(
                        &header,
                        current_bitrate_mbps,
                        now,
                        self.ip_self,
                        self.frames_sent_counter,
                        &self.name_folder,
                        max_bitrate_ladder_mbps,
                    )
                    .await
                    .unwrap();

                if let Some(encoder_init) = send_socket.clone().ffmpeg_encoder {
                    self.video_app_sender.as_mut().unwrap().ffmpeg_encoder = Some(encoder_init);
                    // println!("ENCODER INITIALIZED");
                }
                if let Some(maxencoder_init) = send_socket.clone().ffmpeg_maxbitrate_encoder {
                    self.video_app_sender
                        .as_mut()
                        .unwrap()
                        .ffmpeg_maxbitrate_encoder = Some(maxencoder_init); // ACTUALLY CONSERVE THE COPY, CRITICAL!
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

                // let normal = Normal::new(0.0, 2.0).unwrap(); // Mean = 0, Std dev = 5
                // let epsilon = normal.sample(&mut rand::thread_rng()); // Random Gaussian value

                let time_until_next_frame = Duration::from_secs_f32(1.0 / (self.fps)); // no epsilon for now, deterministic FPS at server.

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
    name_folder: String,
}
impl MetricsLogger {
    fn new(ip: IpAddr, name_folder: &str) -> Result<Self> {
        let file = File::create(format!("Video_Sink/{}/{}/metrics.csv", name_folder, ip))?;
        let writer = csv::Writer::from_writer(file);
        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            name_folder: name_folder.to_string(),
        })
    }

    pub async fn process_frame_metrics(
        &self,
        frame_number: u64,
        timestamp_ms: f64,
        ref_path: &str,
        lossy_path: &str,
    ) -> Result<()> {


        // Create a temporary directory for processing
        let temp_dir = TempDir::new()?;

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
            .status()?;

        if !ref_status.success() {
            return Err(anyhow::anyhow!("Failed to convert reference frame to Y4M"));
        }

        // Convert lossy frame to Y4M
        let lossy_status = Command::new("ffmpeg")
            .args(&[
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
            .status()?;

        if !lossy_status.success() {
            return Err(anyhow::anyhow!("Failed to convert lossy frame to Y4M"));
        }

        // Create the Video_Sink directory within the temp directory
        let video_sink_dir = temp_dir.path().join(&self.name_folder).join("Video_Sink");
        std::fs::create_dir_all(&video_sink_dir)?;

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
        print_prettyy!(
            DebugColor::ForestGreen,
            "Frame {}: VMAF = {:.2}, PSNR = {:.2}, SSIM = {:.4}",
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
    pub decoder_queue: DroppingVecDeque<(usize, Vec<u8>)>,

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
    // pub decoder_arc: Option<Arc<tokMutex<HevcDecoder>>>,

    // pub ref_decoder_arc: Option<Arc<tokMutex<HevcDecoder>>>,
    synchronized_decoder: Option<Arc<Mutex<SynchronizedDecoder>>>,
    synchronized_throttle: Option<Arc<Semaphore>>,

    pub is_decoder_ready: bool,
    pub is_ref_decoder_ready: bool,
    // Add these new fields:
    initialization_buffer: Vec<Vec<u8>>, // Buffer to hold initial frames
    initialization_buffer_ref: Vec<Vec<u8>>,
    initialization_buffer_max: Vec<Vec<u8>>,

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
    display_queue: VecDeque<usize>, // Queue of frame IDs ready to display

    keyframe_sync_state: KeyframeSyncState,
    last_keyframe_id: usize,

    name_folder: String,
    frame_batch: Vec<(usize, Vec<u8>, Vec<u8>, f64)>, // (frame_id, sample, ref_sample, timestamp)
    last_batch_process_time: TaiTime<0>,

    shared_params: Option<Arc<SharedParameterSetManager>>,
    channel_tx_vmaf: Sender<(Vec<u8>, Vec<u8>, usize)>,
    channel_rx_vmaf: Receiver<(Vec<u8>, Vec<u8>, usize)>,
    // pub visualize_decoder_window: Option<Window>,
}
#[allow(unused)]
impl XRClient {
    pub fn new(server_ip: IpAddr, fps: f32, now: TaiTime<0>, name_folder: &str) -> Self {
        let (vmaf_tx, vmaf_rx) = bounded(5);
        let (group_tx, group_rx) = bounded(5); // Buffer up to 5 groups
        let synchronized_throttle = Arc::new(Semaphore::new(0));
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
            synchronized_decoder: None,
            synchronized_throttle: Some(synchronized_throttle),

            // Add these new fields:
            initialization_buffer: Vec::new(), // Buffer to hold initial frames
            initialization_buffer_ref: Vec::new(),
            initialization_buffer_max: Vec::new(),
            is_decoder_ready: false, // Flag to track if decoder is ready
            is_ref_decoder_ready: false,
            min_buffered_frames: 10, // Minimum frames to buffer before decoding
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
            display_queue: VecDeque::new(),
            keyframe_sync_state: KeyframeSyncState::default(),
            last_keyframe_id: 0,
            name_folder: name_folder.to_string(),

            frame_batch: Vec::new(),
            last_batch_process_time: TaiTime::EPOCH,
            shared_params: None,

            channel_tx_vmaf: vmaf_tx,
            channel_rx_vmaf: vmaf_rx,
            // visualize_decoder_window: None,
        }
    }

    pub fn initialize_synchronized_decoder(&mut self) {
        // Create or retrieve the semaphore
        let throttle_semaphore = match &self.synchronized_throttle {
            Some(semaphore) => Arc::clone(semaphore),
            None => {
                let sem = Arc::new(Semaphore::new(0));
                self.synchronized_throttle = Some(Arc::clone(&sem));
                sem
            }
        };

        // Initialize a new synchronized decoder
        let synchronized_decoder = SynchronizedDecoder::new(self.server_ip, throttle_semaphore);
        self.synchronized_decoder = Some(Arc::new(Mutex::new(synchronized_decoder)));

        print_pretty!(
            DebugColor::Green,
            "Initialized synchronized decoder for {} with throttle semaphore",
            self.server_ip
        );
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
                let mut rng = StdRng::from_entropy();

                let yaw: f32 = rng.gen_range((-PI as f32)..(PI as f32));
                let pitch: f32 = rng.gen_range((-PI as f32)..(PI as f32));

                let orientation = Quat::from_rotation_y(yaw) * Quat::from_rotation_x(pitch);
                let position_offset = (Vec3::new(rand::random(), rand::random(), rand::random())
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

                context
                    .scheduler
                    .schedule_event(loop_deadline, Self::generate_tracking_data, ())
                    .unwrap();
            }
        }
    }

    pub async fn send_tracking(&mut self, tracking: Tracking, now: TaiTime<0>) {
        if let Some(mut sender) = self.output_app_tracking_sender.clone() {
            let arc_inner_app_receiver = sender.app_network_interface.clone();

            let send_result = sender.send_header_tracking(&tracking, now);
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

                                let elapsed_tracking =
                                    now.duration_since(self.last_tracking_time).as_secs_f32();

                                if stream_id == TRACKING {
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
                                if stream_id == TRACKING {
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

    fn cleanup_old_frames_vmaf(&self, current_frame_id: usize, ip: IpAddr) -> Result<()> {
        // Only clean up frames that are at least 100 frames behind
        if current_frame_id <= KEEP_FRAMES_DISK_INDEX {
            return Ok(());
        }

        let oldest_frame_to_keep = current_frame_id - KEEP_FRAMES_DISK_INDEX;
        let base_dir = &format!("Video_Sink/{}", &self.name_folder);

        // Define paths to reference and lossy directories
        let ref_dir = format!("{}/{}/reference_rgb", base_dir, ip);
        let lossy_dir = format!("{}/{}/lossy_rgb", base_dir, ip);

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

        // Clean up both directories
        remove_old_frames(&ref_dir)?;
        remove_old_frames(&lossy_dir)?;

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
            return Ok(()); // Return early, don't try to process empty frames
        }

        // Ensure metrics logger is initialized
        if self.metrics_logger.is_none() {
            match MetricsLogger::new(ip, &self.name_folder) {
                Ok(logger) => {
                    println!("Initialized metrics logger for VMAF analysis");
                    self.metrics_logger = Some(logger);
                }
                Err(e) => {
                    eprintln!("Failed to initialize metrics logger: {}", e);
                    return Ok(());
                }
            }
        }

        // print_pretty!(DebugColor::ForestGreen, "Inside VMAF analysis? ", );

        // Create directories for temporary storage if they don't exist
        let base_dir = &format!("Video_Sink/{}", &self.name_folder);
        if let Err(e) = std::fs::create_dir_all(base_dir) {
            eprintln!("Failed to create directory {}: {}", base_dir, e);
            return Ok(());
        }

        // Save frames to temporary files
        let ref_path = format!(
            "{}/{}/reference_rgb/frame_{:04}.rgb",
            base_dir, ip, frame_id
        );
        let lossy_path = format!("{}/{}/lossy_rgb/frame_{:04}.rgb", base_dir, ip, frame_id);
        // print_pretty!(DebugColor::ForestGreen, "Inside VMAF analysis - Writing frames to disk", );

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
        // print_prettyy!(DebugColor::ForestGreen, "Inside VMAF analysis - Frame files written", );

        // convert to f64 as required by process_frame_metrics
        let timestamp_ms = now.duration_since(self.t_0).as_secs_f64();
        // print_pretty!(DebugColor::ForestGreen, "Inside VMAF analysis 2222 ? ", );

        // Process frame metrics
        if let Some(logger) = &self.metrics_logger {
            match logger
                .process_frame_metrics(
                    frame_id as u64,
                    timestamp_ms, // This is now f64 as expected
                    &ref_path,
                    &lossy_path,
                )
                .await
            {
                Ok(_) => {
                    // if frame_id % 10 == 0 {
                    //     println!("Processed VMAF analysis for frame {}", frame_id);
                    // }
                }
                Err(e) => {
                    eprintln!("Error in VMAF analysis for frame {}: {}", frame_id, e);
                }
            }
        }

        // Add to frame group for batch processing if enabled
        if self.enable_batch_processing {
            if self.current_frame_group.is_none() {
                self.current_frame_group = Some(FrameGroup {
                    frames: Vec::with_capacity(VMAF_FRAME_GROUP_SIZE),
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
                        let frames_to_send = std::mem::replace(
                            &mut group.frames,
                            Vec::with_capacity(VMAF_FRAME_GROUP_SIZE),
                        );
                        let group_to_send = FrameGroup {
                            frames: frames_to_send,
                        };

                        if let Err(e) = tx.send(group_to_send) {
                            eprintln!("Error sending frame group: {}", e);
                        }
                    }
                }
            }
        }
        // print_pretty!(DebugColor::ForestGreen, "Inside VMAF analysis 33333333333333 ? ", );

        // Update clean-up timer

        Ok(())
    }

    // Helper method to check if a frame contains a keyframe
    fn is_keyframe(&self, frame: &[u8]) -> bool {
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

    fn cleanup_hevc_rgb_files(&self, current_frame_id: usize, ip: IpAddr) -> Result<()> {
        // Only clean up frames that are at least 100 frames behind
        if current_frame_id <= KEEP_FRAMES_DISK_INDEX {
            return Ok(());
        }

        let oldest_frame_to_keep = current_frame_id - KEEP_FRAMES_DISK_INDEX;
        let base_dir = &format!(
            "/home/boris/Desktop/Rust_MG1/asynchronix/Video_Sink/{}",
            &self.name_folder
        );

        // Define path to hevc_ref directory
        let hevc_ref_dir = format!("{}/{}/hevc_ref", base_dir, ip);
        let max_ref_dir = format!("{}/{}/hevc_max", base_dir, ip);

        // Ensure the directory exists before trying to read it
        if !std::path::Path::new(&hevc_ref_dir).exists() {
            return Ok(()); // Nothing to clean if directory doesn't exist
        }
        if !std::path::Path::new(&max_ref_dir).exists() {
            return Ok(()); // Nothing to clean if directory doesn't exist
        }

        if let Ok(entries) = std::fs::read_dir(&hevc_ref_dir) {
            for entry in entries.filter_map(Result::ok) {
                let path = entry.path();

                // Only process .rgb files
                if let Some(extension) = path.extension() {
                    if extension == "rgb" {
                        if let Some(filename) = path.file_stem() {
                            if let Some(file_str) = filename.to_str() {
                                // Parse frame number from filename (e.g., "142.rgb" -> 142)
                                if let Ok(frame_num) = file_str.parse::<usize>() {
                                    if frame_num < oldest_frame_to_keep {
                                        if let Err(e) = std::fs::remove_file(&path) {
                                            eprintln!(
                                                "Failed to remove old RGB file {}: {}",
                                                path.display(),
                                                e
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    } else if extension == "hevc" {
                        if let Some(filename) = path.file_stem() {
                            if let Some(file_str) = filename.to_str() {
                                // Parse frame number from filename (e.g., "142.rgb" -> 142)
                                if let Ok(frame_num) = file_str.parse::<usize>() {
                                    if frame_num < oldest_frame_to_keep {
                                        if let Err(e) = std::fs::remove_file(&path) {
                                            eprintln!(
                                                "Failed to remove old RGB file {}: {}",
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
            }
        }

        // Read the directory and find .rgb files to remove
        if let Ok(entries) = std::fs::read_dir(&max_ref_dir) {
            for entry in entries.filter_map(Result::ok) {
                let path = entry.path();

                // Only process .rgb files
                if let Some(extension) = path.extension() {
                    if extension == "rgb" {
                        if let Some(filename) = path.file_stem() {
                            if let Some(file_str) = filename.to_str() {
                                // Parse frame number from filename (e.g., "142.rgb" -> 142)
                                if let Ok(frame_num) = file_str.parse::<usize>() {
                                    if frame_num < oldest_frame_to_keep {
                                        if let Err(e) = std::fs::remove_file(&path) {
                                            eprintln!(
                                                "Failed to remove old MAX RGB file {}: {}",
                                                path.display(),
                                                e
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    } else if extension == "hevc" {
                        if let Some(filename) = path.file_stem() {
                            if let Some(file_str) = filename.to_str() {
                                // Parse frame number from filename (e.g., "142.rgb" -> 142)
                                if let Ok(frame_num) = file_str.parse::<usize>() {
                                    if frame_num < oldest_frame_to_keep {
                                        if let Err(e) = std::fs::remove_file(&path) {
                                            eprintln!(
                                                "Failed to remove old MAX RGB file {}: {}",
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
            }
        }

        if current_frame_id % KEEP_FRAMES_DISK_INDEX == 0 {
            println!(
                "Cleaned up HEVC RGB files older than frame {}",
                oldest_frame_to_keep
            );
        }

        Ok(())
    }

    // pub async fn decode_hevc_to_rgb2(
    //     &mut self,
    //     encoded_buffer: Vec<u8>,
    //     frame_index: usize,
    //     client_ip: IpAddr,
    //     is_max_bitrate: bool
    // ) -> (Vec<u8>, Vec<u32>, Option<Instant>) {
    //     // Generate the appropriate decoder key based on client IP and stream type
    //     let decoder_key = self.get_decoder_key(client_ip, is_max_bitrate);

    //     // Create descriptive decoder identifier for diagnostic logging
    //     let decoder_id = if is_max_bitrate {
    //         format!("[MAXB_DECODER {}]", decoder_key)
    //     } else {
    //         format!("[REFB_DECODER {}]", decoder_key)
    //     };

    //     // Perform early buffer validation to prevent downstream processing errors
    //     let encoded_length = encoded_buffer.len();
    //     if encoded_buffer.is_empty() {
    //         print_pretty!(DebugColor::Yellow,
    //             "{} - WARNING: Empty encoded buffer received!", decoder_id,);
    //         return (Vec::new(), Vec::new(), None);
    //     }

    //     // Initialize shared parameter set manager if not already established
    //     if self.shared_params.is_none() {
    //         // Designate the main client decoder as the primary parameter set source
    //         let primary_decoder = format!("[CLIENT_DECODER {}]", client_ip);
    //         let shared = Arc::new(SharedParameterSetManager::new(&primary_decoder));
    //         self.shared_params = Some(Arc::clone(&shared));

    //         print_pretty!(DebugColor::Green,
    //             "Initialized shared parameter set manager with primary decoder: {}",
    //             primary_decoder,);

    //         // Immediately populate parameter sets from main decoder if available
    //         // This accelerates initialization for subsequent decoders
    //         if let Some(decoder_arc) = &self.decoder_arc {
    //             if let Ok(decoder_guard) = decoder_arc.try_lock() {
    //                 let (vps, sps, pps) = decoder_guard.get_parameter_sets();
    //                 if vps.is_some() || sps.is_some() || pps.is_some() {
    //                     shared.update_from_decoder(&primary_decoder, vps, sps, pps);

    //                     print_pretty!(DebugColor::Magenta,
    //                         "Populated shared parameter sets from main decoder",);
    //                 }
    //             }
    //         }
    //     }

    //     // Obtain a cloned reference to the shared parameter manager for thread safety
    //     let shared_params = self.shared_params.clone();

    //     // Actual frame decoding operation within a mutex-protected scope
    //     let result = {
    //         // Acquire exclusive access to the global decoder registry
    //         let mut decoders = REFERENCE_DECODERS.lock().unwrap();

    //         // Create new decoder instance if one doesn't exist for this decoder key
    //         if !decoders.contains_key(&decoder_key) {
    //             print_pretty!(DebugColor::Cyan,
    //                 "Initializing {} decoder for client {}",
    //                 if is_max_bitrate { "max bitrate" } else { "reference" },
    //                 decoder_key,);

    //             // Instantiate decoder with appropriate frame parameters
    //             let mut new_decoder = HevcDecoder::new(
    //                 FRAMERATE_WINDOWS as u32,
    //                 WIDTH_ENCODER as u32,
    //                 HEIGHT_ENCODER as u32,
    //                 &decoder_id,
    //             );

    //             // Apply parameter set synchronization if available
    //             if let Some(shared) = &shared_params {
    //                 let (vps, sps, pps) = shared.get_parameter_sets();

    //                 // Progressive parameter set application reduces initialization artifacts
    //                 // Apply VPS, SPS, and PPS in sequence with brief inter-application delays
    //                 if vps.is_some() {
    //                     print_pretty!(DebugColor::Magenta,
    //                         "{} - Initializing with VPS ({} bytes)",
    //                         decoder_id, vps.as_ref().unwrap().len(),);

    //                     new_decoder.inject_parameter_sets(vps.clone(), None, None);
    //                     std::thread::sleep(Duration::from_millis(2));
    //                 }

    //                 if sps.is_some() {
    //                     print_pretty!(DebugColor::Magenta,
    //                         "{} - Initializing with SPS ({} bytes)",
    //                         decoder_id, sps.as_ref().unwrap().len(),);

    //                     new_decoder.inject_parameter_sets(None, sps.clone(), None);
    //                     std::thread::sleep(Duration::from_millis(2));
    //                 }

    //                 if pps.is_some() {
    //                     print_pretty!(DebugColor::Magenta,
    //                         "{} - Initializing with PPS ({} bytes)",
    //                         decoder_id, pps.as_ref().unwrap().len(),);

    //                     new_decoder.inject_parameter_sets(None, None, pps.clone());
    //                 }

    //                 // Apply consolidated parameter set after individual injections
    //                 // This ensures full decoder configuration before processing frames
    //                 if vps.is_some() || sps.is_some() || pps.is_some() {
    //                     new_decoder.inject_parameter_sets(vps, sps, pps);

    //                     print_pretty!(DebugColor::Magenta,
    //                         "{} - Initialized with all parameter sets", decoder_id,);
    //                 } else {
    //                     print_pretty!(DebugColor::Yellow,
    //                         "{} - No shared parameter sets available yet", decoder_id,);
    //                 }
    //             }

    //             // Mark decoder as being in initialization phase for special handling
    //             new_decoder.initialization_phase = true;

    //             // Register the new decoder in the global registry
    //             decoders.insert(decoder_key.clone(), new_decoder);
    //         }

    //         // Keyframe detection and parameter set synchronization
    //         let is_keyframe = self.is_keyframe(&encoded_buffer);
    //         if is_keyframe {
    //             print_pretty!(DebugColor::Magenta,
    //                 "{} - Processing keyframe (size: {} bytes)", decoder_id, encoded_buffer.len(),);

    //             // Synchronize parameter sets on keyframes when shared manager is available
    //             if let Some(shared) = &shared_params {
    //                 if let Some(decoder) = decoders.get_mut(&decoder_key) {
    //                     // Determine if synchronization is required
    //                     // Force sync during initialization, early frames, or when new params are available
    //                     let generation = shared.get_generation();

    //                 }
    //             }
    //         }

    //         // Process the frame through the appropriate decoder
    //         if let Some(decoder) = decoders.get_mut(&decoder_key) {
    //             // Submit packet to decoder's processing pipeline
    //             decoder.process_packet(encoded_buffer);

    //             // Process any available decoded frames
    //             let frames_count = decoder.process_decoded_frames();

    //             // Attempt to retrieve a decoded frame
    //             if let Some((frame, inst)) = decoder.next_decoded_frame() {
    //                 // Convert raw RGB buffer to u32 pixels for display rendering
    //                 if let Some(pixels) = convert_rgb_to_u32(&frame, WIDTH_ENCODER, HEIGHT_ENCODER) {
    //                     (frame, pixels, Some(inst))
    //                 } else {
    //                     print_pretty!(DebugColor::Red,
    //                         "{} - Failed to convert frame to RGB", decoder_id,);
    //                     (Vec::new(), Vec::new(), None)
    //                 }
    //             } else {
    //                 // Special handling for decoders still in priming phase
    //                 if !decoder.priming_complete {
    //                     print_pretty!(DebugColor::Yellow,
    //                         "{} - Decoder still priming ({}/{} frames processed)",
    //                         decoder_id, decoder.frames_processed, decoder.keyframes_seen,);
    //                 } else {
    //                     print_pretty!(DebugColor::Yellow,
    //                         "{} - No decoded frame available yet", decoder_id,);
    //                 }
    //                 (Vec::new(), Vec::new(), None)
    //             }
    //         } else {
    //             // This condition should never occur due to our earlier creation logic
    //             print_pretty!(DebugColor::Red,
    //                 "{} - ERROR: Decoder initialization failed", decoder_id,);
    //             (Vec::new(), Vec::new(), None)
    //         }
    //     };

    //     // Return the decoded frame, rendered pixels, and timing information
    //     result
    // }

    pub fn vsync<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let now = context.scheduler.time();
            let mut T_vsync = Duration::from_secs_f64(1.0 / self.framerate as f64);

            // Initialize synchronized decoder if needed

            if self.synchronized_decoder.is_none() {
                self.initialize_synchronized_decoder();
            }

            // Clean up older processed frames from tracking buffer to prevent unbounded growth
            self.missing_frames_buffer.retain(|&id, &mut processed| {
                !processed || id > self.last_processed_frame_id - 100
            });

            // Process the next frame if available
            if let Some((id_f, video_frame)) = self.decoder_queue.pop() {
                let mut ip_client = self.server_ip.clone();
                let mut is_frame_lost = false;
                let mut difference = 0;

                // Normalize IP address if needed
                if let IpAddr::V4(mut ip4) = ip_client {
                    let mut octets = ip4.octets();
                    if octets[3] == 2 {
                        octets[3] = 1; // Change last byte from 2 to 1
                        ip_client = IpAddr::V4(std::net::Ipv4Addr::from(octets));
                    }
                }
                // Process any pending VMAF analysis tasks via channel
                if USE_VMAF {
                    if let Ok((refe, maxbe, id)) = self.channel_rx_vmaf.try_recv() {
                        self.vmaf_analysis(refe, maxbe, now, id, ip_client)
                            .await
                            .unwrap_or_else(|e| {
                                print_pretty!(DebugColor::Red, "VMAF analysis error: {}", e,);
                            });
                    }
                }

                // Path definitions for reference frames
                let max_rgb_write_path = format!(
                    "/home/boris/Desktop/Rust_MG1/asynchronix/Video_Sink/{}/{}/hevc_max/{}_max.rgb",
                    self.name_folder, ip_client, id_f
                );
                let ref_rgb_write_path = format!(
                    "/home/boris/Desktop/Rust_MG1/asynchronix/Video_Sink/{}/{}/hevc_ref/{}.rgb",
                    self.name_folder, ip_client, id_f
                );
                let maxb_file_path = format!("/home/boris/Desktop/Rust_MG1/asynchronix/Video_Sink/{}/{}/hevc_max/{}_max.hevc", 
                    self.name_folder, ip_client, id_f);
                let currentb_path = format!(
                    "/home/boris/Desktop/Rust_MG1/asynchronix/Video_Sink/{}/{}/hevc_ref/{}.hevc",
                    self.name_folder, ip_client, id_f
                );

                if id_f % 10 == 0 {
                    if USE_VMAF {
                        if let Err(e) = self.cleanup_old_frames_vmaf(id_f, ip_client) {
                            eprintln!("Error during frame cleanup: {}", e);
                        }
                    }

                    if USE_FFMPEG {
                        if let Err(e) = self.cleanup_hevc_rgb_files(id_f, ip_client) {
                            eprintln!("Error during hevc ref frame cleanup: {}", e);
                        }
                    }
                }

                // Missing frame detection and recovery logic
                if id_f > self.last_processed_frame_id + 1 {
                    is_frame_lost = true;
                    difference = id_f - self.last_processed_frame_id - 1;

                    let missing_start = self.last_processed_frame_id + 1;
                    let missing_end = id_f - 1;

                    print_pretty!(
                        DebugColor::Red,
                        "{} Detected missing frames between {} and {}",
                        ip_client,
                        missing_start,
                        missing_end,
                    );

                    // Track all missing frames in the buffer
                    for missing_id in (self.last_processed_frame_id + 1)..id_f {
                        if !self.missing_frames_buffer.contains_key(&missing_id) {
                            print_pretty!(
                                DebugColor::DarkOrange,
                                "{} Added missing frame {} to tracking system",
                                ip_client,
                                missing_id,
                            );
                            self.missing_frames_buffer.insert(missing_id, false);
                        }
                    }

                    // Process each missing frame, attempting recovery
                    let mut next_frame_id = self.last_processed_frame_id + 1;
                    let process_limit = id_f + 2;

                    while next_frame_id < process_limit {
                        if let Some(frame_state) = self.missing_frames_buffer.get(&next_frame_id) {
                            if *frame_state == true {
                                // Already processed, move to next
                                print_pretty!(
                                    DebugColor::Purple,
                                    "Frame {} already processed, skipping",
                                    next_frame_id,
                                );
                                next_frame_id += 1;
                                continue;
                            }
                        }

                        // Construct file paths for missing frame
                        let missing_max_path = format!("/home/boris/Desktop/Rust_MG1/asynchronix/Video_Sink/{}/{}/hevc_max/{}_max.hevc", 
                            self.name_folder, ip_client, next_frame_id);
                        let missing_reg_path = format!("/home/boris/Desktop/Rust_MG1/asynchronix/Video_Sink/{}/{}/hevc_ref/{}.hevc", 
                            self.name_folder, ip_client, next_frame_id);
                        let missing_max_rgb_path = format!("/home/boris/Desktop/Rust_MG1/asynchronix/Video_Sink/{}/{}/hevc_max/{}_max.rgb", 
                            self.name_folder, ip_client, next_frame_id);
                        let missing_ref_rgb_path = format!("/home/boris/Desktop/Rust_MG1/asynchronix/Video_Sink/{}/{}/hevc_ref/{}.rgb", 
                            self.name_folder, ip_client, next_frame_id);

                        // Try to recover max bitrate reference frame
                        if std::path::Path::new(&missing_max_path).exists() {
                            if let Ok(hevc_data) = fs::read(&missing_max_path) {
                                if !hevc_data.is_empty() {
                                    print_pretty!(
                                        DebugColor::Lime,
                                        "Decoding MAX BITRATE REFERENCE FRAME {}",
                                        next_frame_id,
                                    );

                                    // Process through synchronized decoder
                                    if let Some(sync_decoder) = &self.synchronized_decoder {
                                        let mut sync_decoder_guard = sync_decoder.lock().unwrap();

                                        // Create an empty dummy frame for the regular stream since it's missing
                                        let dummy_frame = Vec::new();

                                        // Process the frame pair with empty regular frame
                                        sync_decoder_guard.process_frame_pair(
                                            dummy_frame,
                                            hevc_data.clone(),
                                            next_frame_id,
                                        );

                                        print_pretty!(DebugColor::Green,
                                            "Successfully processed missing MAX frame #{} through synchronized decoder", 
                                            next_frame_id,);

                                        // Mark as processed
                                        self.missing_frames_buffer.insert(next_frame_id, true);
                                    } else {
                                    }
                                }
                            }
                        }

                        next_frame_id += 1;
                    }
                }

                // Update last processed frame ID
                self.last_processed_frame_id = id_f;
                let mut max_frame = Vec::new();
                let mut ref_frame = Vec::new();

                if USE_FFMPEG {
                    // Try reading max bitrate reference with retry logic
                    for attempt in 1..=5 {
                        match fs::read(&maxb_file_path) {
                            Ok(data) if !data.is_empty() => {
                                max_frame = data;
                                break;
                            }
                            Ok(_) => {
                                print_pretty!(
                                    DebugColor::Yellow,
                                    "MAX file for frame #{} exists but is empty (attempt {}/5)",
                                    id_f,
                                    attempt,
                                );
                                if id_f < 10 {
                                    continue;
                                } else {
                                    thread::sleep(Duration::from_millis(100));
                                }
                            }
                            Err(e) => {
                                if attempt < 5 {
                                    print_pretty!(DebugColor::Yellow,
                                        "Error reading MAX HEVC file for frame #{} (attempt {}/5): {}", 
                                        id_f, attempt, e,);
                                    if id_f < 10 {
                                        continue;
                                    }
                                    thread::sleep(Duration::from_millis(100));
                                } else {
                                    print_pretty!(DebugColor::Red,
                                        "Failed to read MAX HEVC file for frame #{} after 5 attempts", 
                                        id_f,);
                                }
                            }
                        }
                    }

                    // Try reading regular reference with retry logic
                    for attempt in 1..=5 {
                        match fs::read(&currentb_path) {
                            Ok(data) if !data.is_empty() => {
                                ref_frame = data;
                                break;
                            }
                            Ok(_) => {
                                print_pretty!(
                                    DebugColor::Yellow,
                                    "REF file for frame #{} exists but is empty (attempt {}/5)",
                                    id_f,
                                    attempt,
                                );
                                if id_f < 10 {
                                    continue;
                                } else {
                                    thread::sleep(Duration::from_millis(100));
                                }
                            }
                            Err(e) => {
                                if attempt < 5 {
                                    print_pretty!(DebugColor::Yellow,
                                        "Error reading REF HEVC file for frame #{} (attempt {}/5): {}", 
                                        id_f, attempt, e,);
                                    if id_f < 10 {
                                        continue;
                                    }
                                    thread::sleep(Duration::from_millis(100));
                                } else {
                                    print_pretty!(DebugColor::Red,
                                        "Failed to read REF HEVC file for frame #{} after 5 attempts", 
                                        id_f,);
                                }
                            }
                        }
                    }

                    // Detect keyframes
                    if self.is_keyframe(&video_frame) {
                        self.dec_saw_keyframe = true;
                        print_pretty!(
                            DebugColor::Magenta,
                            "*** KEYFRAME DETECTED IN REGULAR STREAM *** Size: {} bytes",
                            video_frame.len(),
                        );
                        self.dec_saw_keyframe_last_t = now;
                    }

                    if self.is_keyframe(&max_frame) {
                        print_pretty!(
                            DebugColor::Magenta,
                            "*** KEYFRAME DETECTED IN MAX BITRATE STREAM *** Size: {} bytes",
                            max_frame.len(),
                        );
                    }

                    if !self.is_decoder_ready {
                        // Buffer frames for initialization
                        if !video_frame.is_empty() {
                            self.initialization_buffer.push(video_frame.clone());
                        }
                        if !ref_frame.is_empty() {
                            self.initialization_buffer_ref.push(ref_frame.clone());
                        }
                        if !max_frame.is_empty() {
                            self.initialization_buffer_max.push(max_frame.clone());
                        }

                        let has_enough_frames =
                            self.initialization_buffer.len() >= self.min_buffered_frames;

                        if has_enough_frames
                            && (self.dec_saw_keyframe || self.is_keyframe(&video_frame))
                        {
                            if self.is_keyframe(&video_frame) {
                                self.dec_saw_keyframe = true;
                                self.dec_saw_keyframe_last_t = now;
                            }

                            print_pretty!(
                                DebugColor::Cyan,
                                "Decoder initialization complete! Buffered {} frames",
                                self.initialization_buffer.len(),
                            );

                            // Initialize synchronized decoder
                            if self.synchronized_decoder.is_none() {
                                self.initialize_synchronized_decoder();
                            }

                            // Process all buffered frames through synchronized decoder
                            if let Some(sync_decoder) = &self.synchronized_decoder {
                                let mut sync_decoder_guard = sync_decoder.lock().unwrap();

                                // Process all buffered frames as pairs
                                for i in 0..self.initialization_buffer.len() {
                                    let dec_frame = &self.initialization_buffer[i];
                                    let max_frame = if i < self.initialization_buffer_max.len() {
                                        &self.initialization_buffer_max[i]
                                    } else {
                                        continue;
                                    };

                                    // Process through synchronized decoder
                                    if !dec_frame.is_empty() && !max_frame.is_empty() {
                                        sync_decoder_guard.process_frame_pair(
                                            dec_frame.clone(),
                                            max_frame.clone(),
                                            i,
                                        );

                                        print_pretty!(
                                            DebugColor::Green,
                                            "Processed initialization frame pair #{}",
                                            i,
                                        );
                                    }
                                }
                            }

                            // Mark decoder as ready and clear buffers
                            self.is_decoder_ready = true;
                            self.is_ref_decoder_ready = true;
                            self.initialization_buffer.clear();
                            self.initialization_buffer_ref.clear();
                            self.initialization_buffer_max.clear();
                        } else {
                            print_pretty!(
                                DebugColor::Blue,
                                "Buffering frame {} of {} (keyframe seen: {})",
                                self.initialization_buffer.len(),
                                self.min_buffered_frames,
                                self.dec_saw_keyframe,
                            );
                        }
                    }
                }
                // Normal processing mode
                if !video_frame.is_empty() && (!max_frame.is_empty() || !USE_FFMPEG) {
                    if let Some(interarrival) =
                        now.checked_duration_since(self.last_decoded_frame_instant)
                    {
                        let miin: usize = usize::min(video_frame.len(), 50);
                        print_pretty!(
                            DebugColor::Violet,
                            "{} - [DBG VSYNC {}] Frame id {} processing. Size: {}, Queue len: {}, Interarrival: {:.4}s", 
                            format_elapsed!(now),
                            self.server_ip,
                            id_f,
                            video_frame.len(),
                            self.decoder_queue.len(),
                            interarrival.as_secs_f32(),
                        );
                        if USE_FFMPEG {
                            if let Some(sync_decoder) = &self.synchronized_decoder {
                                let mut sync_decoder_guard = sync_decoder.lock().unwrap();

                                // Process the frame pair through synchronized decoder
                                sync_decoder_guard.process_frame_pair(
                                    video_frame.clone(),
                                    max_frame.clone(),
                                    id_f,
                                );

                                print_pretty!(
                                    DebugColor::Green,
                                    "Processed frame pair #{} through synchronized decoder",
                                    id_f,
                                );

                                // Try to get a synchronized frame pair
                                if let Some(frame_pair) = sync_decoder_guard.next_frame_pair() {
                                    print_pretty!(
                                        DebugColor::Green,
                                        "Retrieved synchronized frame pair #{} from decoder",
                                        frame_pair.frame_id,
                                    );

                                    // Display synchronized frame pair
                                    thread_local! {
                                        static DISPLAY_WINDOWS: RefCell<HashMap<IpAddr, Window>> = RefCell::new(HashMap::new());
                                    }

                                    DISPLAY_WINDOWS.with(|windows_cell| {
                                        let mut windows = windows_cell.borrow_mut();
                                        // Create window if it doesn't exist
                                        if !windows.contains_key(&self.server_ip) {
                                            let window_title = format!("{} - Frame Compare {}", 
                                                                     format_elapsed!(now), self.server_ip);
                                            let window_width = (WIDTH_ENCODER as f64 * SCALE_FACTOR_WINDOW * 2.0 + 10.0) as usize;
                                            let window_height = (HEIGHT_ENCODER as f64 * SCALE_FACTOR_WINDOW) as usize;
                                            match Window::new(
                                                &window_title,
                                                window_width,
                                                window_height,
                                                WindowOptions::default()
                                            ) {
                                                Ok(window) => {
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
                                        // Update window with frame pair
                                        if let Some(window) = windows.get_mut(&self.server_ip) {
                                            if let (Some(decoded), Some(reference)) =
                                                (&frame_pair.decoded, &frame_pair.reference) {
                                                // Calculate content-based similarity
                                                let similarity = if let (Some(raw1), Some(raw2)) =
                                                    (&frame_pair.decoded_raw, &frame_pair.reference_raw) {
                                                    compute_enhanced_frame_similarity(
                                                        raw1, raw2, WIDTH_ENCODER, HEIGHT_ENCODER
                                                    )
                                                } else {
                                                    0.1 // Default value
                                                };
                                                // Use hardcoded bitrate values as requested
                                                let current_bitrate = 66.66;
                                                let max_bitrate = 100.0;
                                                // Display with enhanced visualization
                                                let display_result = display_frame_pair_enhanced(
                                                    &frame_pair,
                                                    &self.server_ip,
                                                    frame_pair.frame_id,
                                                    window,
                                                    Some(similarity),
                                                    max_bitrate,
                                                    current_bitrate,
                                                    now, 
                                                );
                                                if display_result {
                                                    print_pretty!(DebugColor::Green,
                                                        "Successfully displayed frame pair #{} (similarity: {:.2}%)", 
                                                        frame_pair.frame_id, (1.0 - similarity) * 100.0,);
                                                    // Offload VMAF analysis to channel for async processing
                                                    if USE_VMAF {
                                                        if let (Some(raw_decoded), Some(raw_maxb)) = (&frame_pair.decoded_raw, &frame_pair.reference_raw) {
                                                            let _ = self.channel_tx_vmaf.send((
                                                                raw_decoded.clone(),
                                                                raw_maxb.clone(),
                                                                frame_pair.frame_id
                                                            ));
                                                        }
                                                    }
                                                } else {
                                                    print_pretty!(DebugColor::Red,
                                                        "Failed to display frame pair #{}", 
                                                        frame_pair.frame_id,);
                                                }
                                            } else {
                                                print_pretty!(DebugColor::Yellow,
                                                    "Frame pair #{} missing decoded or reference frame", 
                                                    frame_pair.frame_id,);
                                            }
                                        }
                                    });
                                }
                            } else {
                                print_pretty!(
                                    DebugColor::Red,
                                    "Synchronized decoder not initialized!",
                                );
                            }
                        }
                    }
                }

                // Update timestamp and send output
                self.last_decoded_frame_instant = now;
                self.out_video_decoded
                    .send(video_frame[0..10.min(video_frame.len())].to_vec())
                    .await;
            } else {
                print_pretty!(
                    DebugColor::Yellow,
                    "Decoder queue is empty! Queue len: {}, T_VSYNC: {:.3} ms",
                    self.decoder_queue.len(),
                    T_vsync.as_secs_f32() * 1000.0,
                );
            }

            // Adjust vsync timing based on queue length
            if self.decoder_queue.len() < TARGET_FRAMES_DECODER_QUEUE {
                T_vsync = T_vsync.mul_f64(2.0);
                print_pretty!(
                    DebugColor::Violet,
                    "[DBG VSYNC] Doubling time ({:.4}s) due to queue length ({}) under target ({})",
                    T_vsync.as_secs_f32(),
                    self.decoder_queue.len(),
                    TARGET_FRAMES_DECODER_QUEUE,
                );
            }

            // Schedule next vsync
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
                            let _resulllt = StreamSocket::recv(
                                &mut ssocket,
                                self.server_ip,
                                sock.inner,
                                context,
                            );

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
    for y in 0..size + 2 {
        for x in 0..size + 2 {
            // Calculate pixel coordinates with safety bounds checking
            let buffer_y = y_pos.saturating_add(y).saturating_sub(1);
            let buffer_x = x_pos.saturating_add(x).saturating_sub(1);
            let buffer_idx = buffer_y.saturating_mul(stride).saturating_add(buffer_x);

            if buffer_idx < buffer.len() {
                if x == 0 || y == 0 || x == size + 1 || y == size + 1 {
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
                    let pixel_idx = pixel_y
                        .saturating_mul(width)
                        .saturating_add(pixel_x)
                        .saturating_mul(3);

                    // Ensure we don't go out of bounds
                    if pixel_idx + 2 < frame1.len() && pixel_idx + 2 < frame2.len() {
                        // Calculate perceptually weighted RGB differences
                        let r_diff =
                            (frame1[pixel_idx] as i32 - frame2[pixel_idx] as i32).abs() as f64;
                        let g_diff = (frame1[pixel_idx + 1] as i32 - frame2[pixel_idx + 1] as i32)
                            .abs() as f64;
                        let b_diff = (frame1[pixel_idx + 2] as i32 - frame2[pixel_idx + 2] as i32)
                            .abs() as f64;

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
            let buffer_idx = (y_pos + line_pos)
                .saturating_mul(stride)
                .saturating_add(x_pos + x);
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
            let buffer_idx = (y_pos + y)
                .saturating_mul(stride)
                .saturating_add(x_pos + line_pos);
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

// Ren

fn display_frame_pair_enhanced(
    pair: &FramePair,
    server_ip: &IpAddr,
    display_frame_id: usize,
    window: &mut Window,
    sync_quality: Option<f64>,
    maxb: f32,
    curb: f32,
    now: TaiTime<0>, 
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

    // Calculate dimensions with careful attention to scaling and alignment
    let scale_factor = SCALE_FACTOR_WINDOW;
    let scaled_width = (WIDTH_ENCODER as f64 * scale_factor) as usize;
    let scaled_height = (HEIGHT_ENCODER as f64 * scale_factor) as usize;
    let window_width = scaled_width * 2 + 10; // Two frames plus separator

    // Pre-allocate buffer with exact capacity to avoid reallocation
    let mut combined_buffer = vec![0u32; window_width * scaled_height];

    // Create scaled versions of each frame
    let scaled_current = resize_buffer(
        decoded,
        WIDTH_ENCODER,
        HEIGHT_ENCODER,
        scaled_width,
        scaled_height,
    );
    let scaled_reference = resize_buffer(
        reference,
        WIDTH_ENCODER,
        HEIGHT_ENCODER,
        scaled_width,
        scaled_height,
    );

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
    let text_color = 0x00FF00; // bright green for high visibility
    let highlight_color = 0xFF0033; // bright red for emphasis

    render_text(
        &mut combined_buffer,
        &format!("LOW BITRATE"),
        10,
        10,
        window_width,
        text_color,
        3,
    );
    render_text(
        &mut combined_buffer,
        &format!("MAX BITRATE ({} Mbps)", maxb),
        scaled_width + 20,
        10,
        window_width,
        text_color,
        3,
    );

    // Display frame ID with proper centering
    let frame_info = format!("FRAME #{}", display_frame_id);
    let text_x = (window_width - frame_info.len() * 6 * 2) / 2;
    render_text(
        &mut combined_buffer,
        &frame_info,
        text_x,
        scaled_height - 55,
        window_width,
        highlight_color,
        3,
    );

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

        render_text(
            &mut combined_buffer,
            &sync_text,
            text_x,
            scaled_height - 30,
            window_width,
            quality_color,
            3,
        );
    }

    // Add difference visualization in bottom corner
    if let (Some(raw_decoded), Some(raw_reference)) = (&pair.decoded_raw, &pair.reference_raw) {
        // Create a small difference visualization
        let diff_size = 256;
        let diff_x = window_width - diff_size - 10;
        let diff_y = scaled_height - diff_size - 10;

        if raw_decoded.len() == raw_reference.len()
            && raw_decoded.len() >= WIDTH_ENCODER * HEIGHT_ENCODER * 3
        {
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
            render_text(
                &mut combined_buffer,
                "DIFF",
                diff_x,
                diff_y - 16,
                window_width,
                0xFFFFFF,
                3,
            );
        }
    }

    // Update window title with precise frame information and sync quality
    let title = if let Some(quality) = sync_quality {
        format!(
            "HEVC Comparison - Frame #{} - Sync: {:.1}% - t: {}",
            display_frame_id,
            (1.0 - quality) * 100.0,
            format_elapsed!(now), 
        )
    } else {
        format!("HEVC Comparison - Frame #{}", display_frame_id)
    };
    window.set_title(&title);

    // Critical operation: Update the window buffer with our composite frame
    match window.update_with_buffer(&combined_buffer, window_width, scaled_height) {
        Ok(_) => {
            // println!(
            //     "✅ Successfully rendered frame #{} to window",
            //     display_frame_id
            // );
            true
        }
        Err(e) => {
            eprintln!(
                "❌ Buffer update failed for frame #{}: {}",
                display_frame_id, e
            );
            false
        }
    }
}

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
        let ref_path = temp_dir
            .path()
            .join(format!("ref_{}.rgb", frame.frame_number))
            .to_string_lossy()
            .to_string();
        let lossy_path = temp_dir
            .path()
            .join(format!("lossy_{}.rgb", frame.frame_number))
            .to_string_lossy()
            .to_string();

        std::fs::write(&ref_path, &frame.ref_rgb)?;
        std::fs::write(&lossy_path, &frame.lossy_rgb)?;

        // Process and log metrics for this frame
        match logger
            .process_frame_metrics(
                frame.frame_number,
                frame.timestamp_ms,
                &ref_path,
                &lossy_path,
            )
            .await
        {
            Ok(_) => {
                // Successfully processed
                println!("Processed frame {}", frame.frame_number);
            }
            Err(e) => {
                eprintln!(
                    "Error processing metrics for frame {}: {}",
                    frame.frame_number, e
                );
            }
        }
    }

    println!("Group processing complete");
    Ok(())
}
