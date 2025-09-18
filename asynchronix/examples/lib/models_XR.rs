use crate::lib::alvr_control_socket::{
    framed_recv_vec, ControlSocketReceiver, ControlSocketSender,
};
use crate::lib::{alvr_stream_socket::StreamReceiver, HeuristicStats};
// use nix::libc::LOCK_EX;
use rand::distributions::Uniform;
use rand::rngs::StdRng;
use rand::{Rng};
use rand::SeedableRng;
// use std::process::{ChildStdin, ChildStdout};
use crate::{debug_debug, print_magenta,
    //  print_brown
    };
use image::{ImageBuffer, Rgb};
use image_compare::rgb_hybrid_compare;
use rand::prelude::IteratorRandom;
use rand_distr::{Normal, Distribution};
use crate::lib::alvr_packets::{DeviceMotion, Pose};
use crate::lib::{AveragingStrategy, EdcaAc, HevcParser, WindowType};
use anyhow::Result;
use regex::Regex;
use std::cell::RefCell;
use std::fs::{OpenOptions};
use std::io::{BufReader, Read, Write};
use std::net::Ipv4Addr;
use std::process::Command;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread_local;
use tempfile::TempDir;
use tokio::sync::Semaphore;
use std::path::Path;
use minifb::{Window, WindowOptions};
use std::{fs::File};
use crate::lib::models_mm1k::NetworkPattern;

use crate::{format_elapsed, print_green};
use crate::lib::{HeaderALVRStream, USE_FFMPEG};
use crate::print_pretty;
#[allow(unused)]
use crate::{debug_bgprint, print_prettyy, print_red};
#[allow(unused)]
use crate::{debug_print, print_prettyyyy, print_yellow, print_pink};

use core::f64;
use ffmpeg_sidecar::command::FfmpegCommand;
use glam::{Quat, Vec3};
use once_cell::sync::Lazy;
use serde::Serialize;
use std::fmt::Debug;
use std::net::IpAddr;
use std::thread::yield_now;
use std::time::SystemTime;
use std::time::{Duration, Instant};
use std::{mem, vec};

use crate::lib::alvr_control_socket::ProtoControlSocket;
use crate::lib::alvr_packets::{ClientControlPacket, ClientStatistics, NetworkStatisticsPacket, EverestCommand,};
use crate::lib::alvr_stream_socket::{
    parse_shard_data, ConnectionError, DscpTos, Haptics, ReceiverData, SocketBufferSize,
    SocketProtocol, SocketReader, StreamSender, StreamSocketBuilder, Tracking, VideoPacketHeader, CHUNK_DURATION_F64_S,
};
use crate::lib::alvr_stream_socket::{
    AUDIO, HAPTICS, MAX_HISTORY_SIZE, STATISTICS, TRACKING, VIDEO,
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

pub const UPDATE_BITRATE_INTERVAL: Duration = Duration::from_secs(1);
pub const HANDSHAKE_ACTION_TIMEOUT: Duration = Duration::from_secs(2);
pub const MAX_UNREAD_PACKETS: usize = 5; // Applies per stream

pub const CAPACITY_RX_BUFFER: usize = 2000;
pub const STREAMING_RECV_TIMEOUT: Duration = Duration::from_millis(10);
pub const FRAMED_PREFIX_CONTROL_LENGTH: usize = mem::size_of::<u32>();

pub const DECODER_BUFFERING_FRAMES: usize = 3;
pub const BITRATE_UPDATE_INTERVAL: f64 = CHUNK_DURATION_F64_S; 

#[allow(unused)]                                                                                    
pub const TARGET_FRAMES_DECODER_QUEUE: usize = DECODER_BUFFERING_FRAMES / 2; // unused at the moment, 

pub const TARGET_TIMESTAMP_TRACKING: Duration = Duration::from_millis(10);
pub const KEEP_FRAMES_DISK_INDEX: usize = 200;

pub const ALPHA_THROUGHPUT: f32 = 0.1; 

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



// // Advanced frame similarity computation with configurable thresholds
// // Implements perceptual frame comparison techniques with multi-scale analysis
// fn compute_enhanced_frame_similarity(
//     frame1: &[u8],
//     frame2: &[u8],
//     width: usize,
//     height: usize,
// ) -> f64 {
//     // Return maximum difference if frames are incompatible
//     if frame1.len() != frame2.len() || frame1.len() != width * height * 3 {
//         return 1.0;
//     }

//     // Configuration parameters for multi-scale analysis
//     const BLOCK_SIZES: [usize; 3] = [4, 16, 64]; // Multi-scale block sizes
//     const WEIGHTS: [f64; 3] = [0.5, 0.3, 0.2]; // Relative importance of each scale
//     const PERCEPTUAL_WEIGHTS: [f64; 3] = [0.3, 0.6, 0.1]; // R,G,B perceptual importance

//     // Initialize accumulators for each scale
//     let mut scale_diffs = [0.0; 3];
//     let mut scale_samples = [0; 3];

//     // Multi-scale analysis
//     for (scale_idx, &block_size) in BLOCK_SIZES.iter().enumerate() {
//         // Overlapping block steps: step by half the block size.
//         let step_x = (block_size / 2).max(1);
//         let step_y = (block_size / 2).max(1);

//         // Process each overlapping block
//         for by in (0..height).step_by(step_y) {
//             for bx in (0..width).step_by(step_x) {
//                 let block_end_x = (bx + block_size).min(width);
//                 let block_end_y = (by + block_size).min(height);

//                 // Initialize block statistics.
//                 let mut block_diff_r = 0.0;
//                 let mut block_diff_g = 0.0;
//                 let mut block_diff_b = 0.0;
//                 let mut block_samples = 0;

//                 // Increased sampling density: sample every pixel (step of 1).
//                 for y in by..block_end_y {
//                     for x in bx..block_end_x {
//                         let idx = (y * width + x) * 3;
//                         if idx + 2 < frame1.len() && idx + 2 < frame2.len() {
//                             let r_diff = (frame1[idx] as i32 - frame2[idx] as i32).abs() as f64;
//                             let g_diff = (frame1[idx + 1] as i32 - frame2[idx + 1] as i32).abs() as f64;
//                             let b_diff = (frame1[idx + 2] as i32 - frame2[idx + 2] as i32).abs() as f64;

//                             block_diff_r += r_diff;
//                             block_diff_g += g_diff;
//                             block_diff_b += b_diff;
//                             block_samples += 1;
//                         }
//                     }
//                 }

//                 if block_samples > 0 {
//                     let avg_diff = (block_diff_r * PERCEPTUAL_WEIGHTS[0]
//                         + block_diff_g * PERCEPTUAL_WEIGHTS[1]
//                         + block_diff_b * PERCEPTUAL_WEIGHTS[2])
//                         / (block_samples as f64 * 255.0);
//                     scale_diffs[scale_idx] += avg_diff;
//                     scale_samples[scale_idx] += 1;
//                 }
//             }
//         }
//     }

//     // Calculate weighted average across scales
//     let mut final_diff = 0.0;
//     let mut weight_sum = 0.0;

//     for i in 0..BLOCK_SIZES.len() {
//         if scale_samples[i] > 0 {
//             let scale_avg = scale_diffs[i] / scale_samples[i] as f64;
//             final_diff += scale_avg * WEIGHTS[i];
//             weight_sum += WEIGHTS[i];
//         }
//     }

//     // Normalize result
//     if weight_sum > 0.0 {
//         final_diff /= weight_sum;
//     }

//     // Apply non-linear transformation to enhance sensitivity
//     // This emphasizes small differences, which is crucial for detecting
//     // subtle temporal misalignments in nearly-identical frames
//     let enhanced_diff = 1.0 - ((1.0 - final_diff).powf(0.5));

//     // Scale final similarity measure to emphasize high similarity
//     // This creates a more sensitive metric where 99% similar frames
//     // are distinguished from 99.9% similar frames
//     enhanced_diff
// }
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
            .hwaccel("cuda")
            .args(&["-f", "hevc", "-i", "-"])
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



#[derive(Clone, PartialEq, Debug)]
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

        }
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

    update_interval_s: Duration,

    rtt_average: SlidingWindowAverage<Duration>,
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
}


impl BitrateManager {
     pub fn reset(&mut self) {
        // Reset timestamps and counters
        self.last_frame_instant = TaiTime::EPOCH;
        self.last_update_instant = TaiTime::EPOCH;
        self.frame_index = 0;

        // Clear sliding window averages
        self.frame_interval_average.clear();
        self.encoder_latency_average.clear();
        self.network_latency_average.clear();
        self.rtt_average.clear();
        self.peak_throughput_average.clear();
        self.frame_interarrival_average.clear();
        self.bitrate_average_mbps.clear();

        // Reset bitrate state
        match &self.bitrate_mode {
            BitrateMode::ConstantMbps(init_mbps) => {
                self.last_target_bitrate_bps = *init_mbps * 1e6;
            }
            BitrateMode::EVeREst { bitrate_ladder_mbps } => {
                // Pick the lowest rung as a safe restart point
                self.last_target_bitrate_bps = bitrate_ladder_mbps[0] * 1e6;
            }
            BitrateMode::NestVr { min_bitrate_mbps, .. } => {
                self.last_target_bitrate_bps = min_bitrate_mbps * 1e6;
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
     
     pub fn new(max_history_size: usize, initial_framerate: f32, initial_bitrate_mbps: f32, abr_enabled: usize, nest_vr_profile: &NestVrProfile, 
        ) -> Self {
    
        let decrement: usize = match nest_vr_profile {
            NestVrProfile::Anxious => {10}, 
            NestVrProfile::Balanced => {1},
            NestVrProfile::Speedy => {2}, 
            NestVrProfile::Custom{..} => {1}, 
        };         
        
        let mut bitrate_ladder_std_bps = Vec::new(); 
        let bitrate_step_count = 9; 

        let max_mbps = 100.0; 
        let min_mbps = 10.0; 

        let (min_bps, max_bps) = ( min_mbps * 1e6, max_mbps * 1e6); 
        // let initial_bitrate_mbps = 50.0; 

        let bitrate_step_size_bps_nest = (max_bps - min_bps) / bitrate_step_count as f32;


        let bitrate_mode = match abr_enabled{
            1 =>  { 
                
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
                                            update_interval_nestvr_s: UPDATE_BITRATE_INTERVAL.as_secs_f32(), 
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
                    let values_original = [10.0, 20.0, 40.0, 60.0, 80.0, 120.0]; 
                    let mut bitrate_ladder_mbps = Vec::new(); 
                    for value in values_original.iter(){
                        bitrate_ladder_mbps.push(*value as f32); 
                        bitrate_ladder_std_bps.push(*value * 1e6)
                    }                 
                    
                    BitrateMode::EVeREst { bitrate_ladder_mbps }
                }
            _ => BitrateMode::ConstantMbps(initial_bitrate_mbps)
            };          
        crate::print_blue!("BITRATE MODE: {:?}", bitrate_mode); 

        Self {
            last_frame_instant: TaiTime::EPOCH,
            last_update_instant: TaiTime::EPOCH,

            frame_index: 0,

            frame_interval_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),
            encoder_latency_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),
            network_latency_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),

            bitrate_average_mbps: SlidingWindowAverage::new(initial_bitrate_mbps, max_history_size),
            // last_target_bitrate_mbps: initial_bitrate_mbps,
            update_interval_s: UPDATE_BITRATE_INTERVAL,

            rtt_average: SlidingWindowAverage::new(Duration::from_millis(5), max_history_size),
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
        }
    }
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

    pub fn report_network_statistics_abr(
        &mut self,
        network_rtt: Duration,
        peak_throughput_bps: f32,
        frame_interarrival_s: f32,
        network_stats: NetworkStatisticsPacket, 
        now: TaiTime<0>
    ) {
        self.rtt_average.submit_sample(network_rtt);

        self.peak_throughput_average
            .submit_sample(peak_throughput_bps);

        self.frame_interarrival_average
            .submit_sample(frame_interarrival_s);


        const T_USER_WIN : f32 = 5.0; // from original paper 



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
        
        if matches!(self.bitrate_mode , BitrateMode::EVeREst{ .. }) {
            print_pink!("Everest Stats:\nCapacity={:.4} mbps,\nThroughput={:.4} mbps,\nD_short={},\nD_long={},\n\n",self.everest_capacity_ewma / 1e6, self.everest_throughput_ewma / 1e6,  network_stats.everest_dshort, network_stats.everest_dlong,  ); 
        }
    }   

    pub fn one_pass_abr(&mut self, now: TaiTime<0>) -> f32 {

        const TIME_WARMUP_ABR: u64 = 11; 
        if now.duration_since(TaiTime::EPOCH) < Duration::from_secs(TIME_WARMUP_ABR){
            println!("No ABR (warmup) {} -> {}", format_elapsed!(now), TIME_WARMUP_ABR); 
            let bitrate_bps = self.last_target_bitrate_bps; 
            bitrate_bps 
        }
        else{
            let bitrate_bps = match &self.bitrate_mode {
                BitrateMode::ConstantMbps(bitrate_mbps) => {
                    self.last_target_bitrate_bps = *bitrate_mbps as f32 * 1E6;
                    // self.last_target_bitrate_mbps = *bitrate_mbps as f32;

                    print_prettyy!(DebugColor::Navy, "CBR -> Bitrate = {} Mbps", bitrate_mbps);

                    *bitrate_mbps as f32 * 1e6
                }

                BitrateMode::EVeREst { bitrate_ladder_mbps }
                    => {
                        let mut bitrate_bps = self.last_target_bitrate_bps; 
                        print_red!("bitrate first: {} Mbps", bitrate_bps / 1e6);  
                        
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
                        print_pink!("[Everest] N_users= {} / {} == {}, Capacity_margin={}\nBitrate={:.3}", self.everest_capacity_ewma, self.everest_throughput_ewma, n_users, capacity_margin_bps/1e6, bitrate_bps / 1e6); 
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
                    
                    print_pink!(
                        // DebugColor::Purple,
                        "{} ONE PASS OF NEST-VR! Bitrate: {} Mbps",
                        format_elapsed!(now), self.last_target_bitrate_bps / 1e6,  
                    );

                    let (max_bps, min_bps) = (max_bitrate_mbps * 1e6, min_bitrate_mbps * 1e6); 

                    let profile_config = nest_vr_profile; 
                    // Sample from uniform distribution
                    let mut rng = rand::thread_rng();
                    let uniform_dist = Uniform::new(0.0, 1.0);

                    let r_rtt = rng.sample(uniform_dist);
                    let r_inc = rng.sample(uniform_dist);

                    let frame_interval_s = f32::max(self.frame_interval_average.get_average().as_secs_f32(), 1e-9);

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
                    let rtt_avg_ms = self.rtt_average.get_average().as_secs_f32() * 1000.0;

                    let estimated_capacity_bps = f32::max(self.peak_throughput_average.get_average(), 1e-9);

                    let mut bitrate_bps: f32 = self.last_target_bitrate_bps;

                    // print_yellow!("nfr_avg = {}, rtt_avg = {} ms, r_inc = {}, r_rtt = {}, STEP SIZE = {} Mbps", nfr_avg, rtt_avg_ms, r_inc, r_rtt, self.bitrate_step_size_bps_nest / 1e6 ); 

                    if nfr_avg < profile_config.nfr_thresh {
                        // decrease
                        print_yellow!("decrease (nfr_thresh)",); 

                        bitrate_bps -=
                            profile_config.bitrate_dec_steps as f32 * self.bitrate_step_size_bps_nest;
                    } else {
                        if rtt_avg_ms > profile_config.rtt_thresh_ms {
                            if r_rtt <= profile_config.rtt_adj_prob {
                                // decrease
                                print_yellow!("decrease (rtt prob)",); 

                                bitrate_bps -= profile_config.bitrate_dec_steps as f32
                                    * self.bitrate_step_size_bps_nest;
                            }
                        } else {
                            if r_inc <= profile_config.bitrate_inc_prob {
                                // increase
                                print_yellow!("INCREASE (rtt prob)",); 

                                bitrate_bps += profile_config.bitrate_inc_steps as f32
                                    * self.bitrate_step_size_bps_nest;
                            }
                        }
                    }
                    print_pink!("bitrate after Nest: {} Mbps", f32::min( f32::max(bitrate_bps / 1e6, *min_bitrate_mbps), *max_bitrate_mbps )); 
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

                    print_pink!(
                        // DebugColor::Purple,
                        " ------NeSt-VR STATS-------: {:#?}",
                        heur_stats
                    );
                    self.last_target_bitrate_bps = bitrate_bps;
                    // self.last_target_bitrate_mbps = bitrate_bps / 1E6; 
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

#[allow(unused)]
pub struct XRServer {
    pub ip_self: IpAddr,
    pub ip_client: IpAddr,
    pub t_0: TaiTime<0>,
    pub bitrate_manager: BitrateManager,
    pub video_app_sender: Option<StreamSender<VideoPacketHeader>>,
    pub audio_app_sender: Option<StreamSender<()>>,  
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
        effects: &[NetworkPattern], 
        file_name_video: &str,
        gop_size: usize, 
        intra_refresh: bool, 
        abr_enabled: usize, 
        nest_vr_profile: &NestVrProfile, 

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


        let history_interval = BITRATE_UPDATE_INTERVAL;

        Self {
            ip_self,
            ip_client,
            t_0: t0_sim,

            bitrate_manager: BitrateManager::new(
                MAX_HISTORY_SIZE,
                frame_rate,
                initial_bitrate,
                abr_enabled, 
                nest_vr_profile, 
            ),

            video_app_sender: None,
            audio_app_sender: None, 

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
                        rtt = now.duration_since(send_instant);
                        // println!("SEND INSTANT: {}, now: {}, rtt: {}", format_elapsed!(send_instant), format_elapsed!(now), rtt.as_secs_f32());
                    } else {
                        println!("frame {} RTT ZEROO!!!!!", network_stats.frame_index);
                        rtt = Duration::ZERO;
                    }

                    debug_bgprint!(DebugColor::Teal, "RTT = {:.9}", rtt.as_secs_f64());

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
                                if stream_id == VIDEO || stream_id == AUDIO {
                                    
                                    if stream_id == VIDEO{
                                        packet.edca_ac = EdcaAc::Video; 
                                    }
                                    else if stream_id == AUDIO {
                                        packet.edca_ac = EdcaAc::Voice; 
                                    }
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



    pub fn generate_video_frame<'a>(
        &'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let now = context.scheduler.time();
            // let map_clone: Arc<RwLock<HashMap<u32, TaiTime<0>>>> = Arc::clone(&self.map_rtt);

            let map_clone: Arc<DashMap<u32, TaiTime<0>>> = Arc::clone(&self.map_rtt);
            
            if self.video_app_sender.is_none(){
                        println!("CATCH NO VIDEO APP SENDER"); 
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
   


                if !matches!(self.bitrate_manager.bitrate_mode , BitrateMode::EVeREst{ .. }) {
                    if (now.duration_since(self.bitrate_manager.last_update_instant) >= duration_abr){
                       
                        let last_bitrate_mbps = self.bitrate_manager.one_pass_abr(now) / 1e6;
                        self.bitrate_manager.last_update_instant = now;
                        

                        let perfect_info_message = PerfectInfoBitrateMessage{bitrate_ladder_bps: self.bitrate_manager.bitrate_ladder_bps.clone(),  bitrate_mbps: last_bitrate_mbps }; 

                        self.output_perfect_information_bitrate.send(perfect_info_message).await; // Client knows the bitrate ladder, needed for thresholds computing in HMD. 
                        
                        // self.bitrate_manager.last_target_bitrate_mbps = last_bitrate_mbps; 

                        // print_green!("[{}]  Current bitrate: {} Mbps", self.ip_self, self.bitrate_manager.last_target_bitrate_mbps); 
                    }
                } 
                else{ // EveRest classic is applied per-frame. 

                    let last_bitrate_mbps = self.bitrate_manager.one_pass_abr(now) / 1e6;
                    self.bitrate_manager.last_update_instant = now;
                    
                    let perfect_info_message = PerfectInfoBitrateMessage{bitrate_ladder_bps: self.bitrate_manager.bitrate_ladder_bps.clone(),  bitrate_mbps: last_bitrate_mbps }; 
                    self.output_perfect_information_bitrate.send(perfect_info_message).await;  // Client knows the bitrate ladder, needed for thresholds computing in HMD. 
                    
                    // self.bitrate_manager.last_target_bitrate_mbps = last_bitrate_mbps;   
                
                }
                if count % 30 == 0 {
                    print_green!("[{}]  Current bitrate: {} Mbps", self.ip_self, self.bitrate_manager.last_target_bitrate_bps / 1e6); 
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
    original_decoder: Option<Arc<Mutex<HevcDecoder>>>,

    pub is_decoder_ready: bool,
    pub is_ref_decoder_ready: bool,
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
        everest_enabled: bool, 
    ) -> Self {
        let (vmaf_tx, vmaf_rx) = bounded(10);
        let (group_tx, group_rx) = bounded(10); // Buffer up to 5 groups
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

            // Add these new fields:
            initialization_buffer: Vec::new(), // Buffer to hold initial frames
            is_decoder_ready: false, // Flag to track if decoder is ready
            is_ref_decoder_ready: false,
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
            // everest_capacity_vec: Vec::new() ,
            // everest_throughput_vec: Vec::new(), 
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

            // Schedule reboot after pause_time
            let delay = Duration::from_secs_f64(pause_time);
            
            print_magenta!("[session_end_schedule] delay: {}, now + delay: {} ", delay.as_secs_f64(), format_elapsed!(now + delay), ); 
            
            context.scheduler
                .schedule_event(now + delay, Self::session_reboot, ())
                .unwrap();
    }

    pub async fn session_reboot(&mut self, _: (), context: &Context<Self>) {
        let now = context.scheduler.time();
        println!("[XRClient {}] Rebooting session at {:.7}s", self.server_ip, format_elapsed!(now));

        let packet_size = 1400; // or pass from args/config
        self.t_0 = now;
        self.last_tracking_time = now;
        self.is_decoder_ready = false;
        self.is_ref_decoder_ready = false;

        // Re-establish streams
        self.configure_streams(packet_size, context).await;

        // Restart periodic tasks
        context.scheduler
            .schedule_event(Duration::from_millis(10), Self::video_receive_thread, ())
            .unwrap();

        context.scheduler
            .schedule_event(Duration::from_secs_f64(1.0 / self.framerate as f64), Self::vsync, () )
            .unwrap();

    }


    /// Truncated exponential sampler with mean `mean` before truncation and hard bounds [a,b].
    /// We adjust lambda to match the target mean approximately after truncation.
    fn truncated_exponential_seconds<R: Rng>(&mut self, rng: &mut R, mean: f64, a: f64, b: f64) -> f64 {
        // Guard rails
        let a = a.max(0.0);
        let b = b.max(a + 1e-6);
        // Simple fixed-point refinement for λ so E[X|a<=X<=b]≈mean (good enough here).
        let mut lambda = 1.0 / mean.max(1e-6);
        for _ in 0..6 {
            let ea = (-lambda * a).exp();
            let eb = (-lambda * b).exp();
            let z  = ea - eb;
            // E[X | a<=X<=b] for Exp(λ) truncated to [a,b]
            let ex_trunc = (1.0 / lambda) + (a * ea - b * eb) / z;
            lambda *= ex_trunc / mean;
        }
        // Inverse CDF for truncated exp
        let u: f64 = rng.gen();
        let ea = (-lambda * a).exp();
        let eb = (-lambda * b).exp();
        let x = - ( (u * (eb - ea) + ea).ln() ) / lambda;
        x.clamp(a, b)
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

                                // println!("[Client {} read ]: {} packet ", self.server_ip, str_id); 
                                if stream_id == TRACKING {
                                    debug_print!(
                                        DebugColor::ForestGreen,
                                        "{} UL TRACKING [{}]-> Δt_tracking:{:.4} |length: {}| Stream ID: {}|",
                                        format_elapsed!(now),
                                        self.server_ip, 
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

                                    packet.edca_ac = EdcaAc::Voice; // explanation: While small, these packets are most important to be timely for rendering. 
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
                                panic!("IS THIS HAPPENING"); 
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

        // match category{


        // }

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
            edca_ac: EdcaAc::BestEffort, // Non-crutial to be received timely, we don't want it to interfere with UL tracking. 
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
                            println!(
                                "FRAMES LOST {:?}, SHARDS LOST {:?}",
                                &frames_lost[..],
                                &shards_lost[..]
                            );
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
                    /////////////////////////////////////////////
                    
                    // pub const EVEREST_ENABLED : bool = false; 
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
                            
                            print_yellow!("b1 = {}, b2 = {} , 1/FPS = {}", bitrate_bps_comp, value_b2, 1.0/self.framerate); 
                            
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

                            crate::print_blue!("[CLIENT EVEREST]------------------------------\nIs D_short({}) >= D_upper({})? -> {}\nIs D_long({}) < D_lower({})? -> {}\nCMD={:?}",
                                     self.d_short_exp_avg, d_upper_everest, self.d_short_exp_avg >= d_upper_everest , self.d_long_exp_avg, d_lower_everest,  self.d_long_exp_avg < d_lower_everest, command_abr_everest ); 


                        }
                    }
                    //////////////////////////////////////////////
                    
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
                        edca_ac: EdcaAc::Video, // Explanation: Given we're computing the VF-RTT of video packets based on arrivals, let's assume this AC for UL to get the same 'treatment' by EDCA.  
                        
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

 

    fn cleanup_hevc_rgb_files(&mut self, current_frame_id: usize, ip: IpAddr) -> Result<()> {
        // Only clean up frames that are at least 100 frames behind
        if current_frame_id <= KEEP_FRAMES_DISK_INDEX {
            return Ok(());
        }

        let oldest_frame_to_keep = current_frame_id - KEEP_FRAMES_DISK_INDEX;
        let base_dir = &format!(
            "/home/boris/Desktop/Rust_MG1/asynchronix/Sink_for_video/{}",
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

        // Process hevc_ref directory
        if let Ok(entries) = std::fs::read_dir(&hevc_ref_dir) {
            for entry in entries.filter_map(Result::ok) {
                let path = entry.path();

                // Process both .rgb and .hevc files
                if let Some(extension) = path.extension() {
                    if extension == "rgb" || extension == "hevc" {
                        if let Some(filename) = path.file_stem() {
                            if let Some(file_str) = filename.to_str() {
                                // Parse frame number from filename
                                if let Ok(frame_num) = file_str.parse::<usize>() {
                                    if frame_num < oldest_frame_to_keep {
                                        // Log before deletion attempt for debugging
                                        // println!("Attempting to delete old file: {}", path.display());

                                        if let Err(e) = std::fs::remove_file(&path) {
                                            let file_type =
                                                if extension == "rgb" { "RGB" } else { "HEVC" };
                                            eprintln!(
                                                "Failed to remove old {} file {}: {}",
                                                file_type,
                                                path.display(),
                                                e
                                            );
                                        } else {
                                            // Optional: Log successful deletion
                                            let file_type =
                                                if extension == "rgb" { "RGB" } else { "HEVC" };
                                            // println!("Successfully deleted old {} file: {}", file_type, path.display());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Process hevc_max directory
        if let Ok(entries) = std::fs::read_dir(&max_ref_dir) {
            for entry in entries.filter_map(Result::ok) {
                let path = entry.path();

                // Process both .rgb and .hevc files
                if let Some(extension) = path.extension() {
                    if extension == "rgb" || extension == "hevc" {
                        if let Some(filename) = path.file_stem() {
                            if let Some(file_str) = filename.to_str() {
                                // Parse frame number from filename
                                if let Ok(frame_num) = file_str.parse::<usize>() {
                                    if frame_num < oldest_frame_to_keep {
                                        // Log before deletion attempt for debugging
                                        // println!("Attempting to delete old MAX file: {}", path.display());

                                        if let Err(e) = std::fs::remove_file(&path) {
                                            let file_type =
                                                if extension == "rgb" { "RGB" } else { "HEVC" };
                                            eprintln!(
                                                "Failed to remove old MAX {} file {}: {}",
                                                file_type,
                                                path.display(),
                                                e
                                            );
                                        } else {
                                            // Optional: Log successful deletion
                                            let file_type =
                                                if extension == "rgb" { "RGB" } else { "HEVC" };
                                            // println!("Successfully deleted old MAX {} file: {}", file_type, path.display());
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
                "Cleaned up HEVC and RGB files older than frame {}",
                oldest_frame_to_keep
            );
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

            if self.original_decoder.is_none(){
                self.original_decoder = Some(
                    Arc::new( Mutex::new( HevcDecoder::new(
                        self.framerate as u32,
                        WIDTH_ENCODER as u32,
                        HEIGHT_ENCODER as u32,
                        &format!("[SINGLE DECODER {}]", self.server_ip), 
                    )))); 
            }

            let third_octet = get_third_octet(self.server_ip).unwrap();                 
            let csv_path = format!(
                "/home/boris/Desktop/Rust_MG1/asynchronix/Results/{}/trace_offline_video{}.csv",
                self.name_folder,
                third_octet,
                // format_elapsed!(now), 
            );

            // --------------Initialize offline CSV tracker for frames ------------- 
            if self.offline_csv_trace.writer.is_none() {
                if !Path::new(&csv_path).exists() {
                    // panic!("CSV trace still missing after {}ms: {}", max_wait_ms, csv_path);
                    // println!("waiting until offline CSV created {}", csv_path ); 
                }
                else{
                    print_green!("Read from: {}", csv_path); 
                
                    self.offline_csv_trace.path = csv_path.clone().into();

                    // open for *append* so we keep the first row
                    let file = OpenOptions::new()
                        .write(true)
                        .append(true)
                        .open(&self.offline_csv_trace.path)
                        .expect("CSV trace created by encoder is missing!");
                    self.offline_csv_trace.writer = Some(
                        csv::WriterBuilder::new()
                            .has_headers(false)
                            .from_writer(file),
                    );

                }
                
            }

            // Clean up older processed frames from tracking buffer
            let current_last_processed = self.last_processed_frame_id;
            self.missing_frames_buffer.retain(|&id, &mut processed| {
                !processed || id.saturating_sub(current_last_processed) <= 100 // Avoid underflow
            });         

            // Process the next frame if available from the regular stream queue
            if let Some((id_f, video_frame)) = self.decoder_queue.pop() {

                let lost = if self.last_seen_id != 0 && id_f != self.last_seen_id + 1 { 1 } else { 0 };
                self.last_seen_id = id_f;

                let timestamp = now.duration_since(self.t_0).as_secs_f64();           // TaiTime -> f64 seconds

                
                if !Path::new(&csv_path).exists() {
                    // panic!("CSV trace still missing after {}ms: {}", max_wait_ms, csv_path);
                    println!("waiting until offline CSV created", ); 
                }
                else{

                    // emu effects part here? 

                    let csv_writer = self.offline_csv_trace.writer.as_mut().unwrap();
                
                    csv_writer.write_record(&[
                        "",                     // offset column (only first row uses it)
                        "",                     // source column (only first row uses it)
                        "",                     // IDR_freq
                        // "",                     // network emulation effects
                        &format!("{:.6}", timestamp),
                        &id_f.to_string(),
                        &lost.to_string(),
                        &format!("{:.3}", self.last_throughput_avg), 
                    ]).unwrap();         // propagate or log the error as you prefer
                    csv_writer.flush().unwrap();        // or buffer: up to you
    
                }
                let mut ip_client = self.server_ip; 
                if let IpAddr::V4(ip4) = ip_client {
                     let mut octets = ip4.octets();
                     if octets[3] == 2 {
                         octets[3] = 1; // Change last byte from 2 to 1
                         ip_client = IpAddr::V4(std::net::Ipv4Addr::from(octets));
                     }
                 }
               

                if id_f % 10 == 0 {

                    if USE_FFMPEG {
                        if let Err(e) = self.cleanup_hevc_rgb_files(id_f, ip_client) {
                            eprintln!("Error during hevc ref frame cleanup: {}", e);
                        }
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

                if !self.is_decoder_ready && USE_FFMPEG { 

                    if !video_frame.is_empty() { self.initialization_buffer.push(video_frame.clone()); }
                    
                     let has_enough_frames = self.initialization_buffer.len() >= self.min_buffered_frames;
                     if is_keyframe(&video_frame) { self.dec_saw_keyframe = true; self.dec_saw_keyframe_last_t = now; }

                     if has_enough_frames && self.dec_saw_keyframe {
                        print_pretty!(DebugColor::Cyan, "Decoder initialization criteria met! Buffered {} frames", self.initialization_buffer.len());

                        print_pretty!(DebugColor::Cyan, "Initialization processing complete.", );
                        self.is_decoder_ready = true;
                        // self.is_ref_decoder_ready = true; // Still needed?
                        self.initialization_buffer.clear();

                    } else { /* ... log buffering status ... */ }

                } // End of initialization logic


                if self.is_decoder_ready && USE_FFMPEG {
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
                print_red!(
                    // DebugColor::Yellow,
                    "[CLIENT {}] Decoder queue empty. T_VSYNC: {:.3} ms", self.server_ip,  T_vsync.as_secs_f32() * 1000.0
                );

            } // End if let Some((id_f, video_frame))

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
    render_text(&mut buffer, &format!("FRAME #{}", frame_id), margin, margin, scaled_w, 0x00FF00, 2);
    render_text(
        &mut buffer,
        &format!("Bitrate: {:.2} Mbps", bitrate_mbps),
        margin,
        margin + line_h,
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
        "{} -- Frame #{} @ {:.2}s - Test: {}",
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
    timestamp: TaiTime<0>,
}

#[allow(non_camel_case_types)]
#[allow(unused)]
// #[derive(Clone)]
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
    // by a VR gamer standing still but rapidly moving around. 
 pub fn move_coordinates_everest<'a>(&'a mut self,
        _: (),
        context: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move{

            let mut rng = rand::thread_rng();

            let delta_t = 0.01; //  is reasonable? 

            // Step length = speed * delta_t
            let step = 5.0 * delta_t;

            // Pick a random direction in 2D plane (azimuth only)
            let theta = rng.gen_range(0.0..2.0 * PI);
            let dx = step * theta.cos();
            let dy = step * theta.sin();

            // println!("[MOVE COORDS] Before: {:?}", self.sta_coordinates);

            // New candidate position
            let new_x = self.sta_coordinates.x + dx;
            let new_y = self.sta_coordinates.y + dy;

            // Boundaries: within ±0.5 m around initial position
            let min_x = self.orig_sta_coordinates.x - 0.5;
            let max_x = self.orig_sta_coordinates.x + 0.5;
            let min_y = self.orig_sta_coordinates.y - 0.5;
            let max_y = self.orig_sta_coordinates.y + 0.5;

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

            // z stays constant (HMD height)
            // println!("Coordinates After dt: {:?}", self.sta_coordinates);

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