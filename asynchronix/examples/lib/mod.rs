#![allow(non_snake_case)]

use std::collections::VecDeque;
use std::f64;

use csv::Writer;
use std::fs::OpenOptions;
use tai_time::TaiTime;

use crate::lib::alvr_packets::DeviceMotion;
use crate::lib::alvr_packets::Pose;
use colored::Colorize;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use std::io::{self, Write};
use std::path::Path;

const CW_MIN: i32 = 8;
const CHANNEL_WIDTH: usize = 80; //MHz

const LEGACY_PHY_DURATION: f64 = 20E-6; // microseconds
const PHY_DURATION: f64 = 100E-6;
const SLOT: f64 = 9E-6;
const SIFS: f64 = 16E-6;
const DIFS: f64 = 2.0 * SLOT + SIFS;

pub const DEFAULT_TMAX_AGG: f64 = 4.85E-3;
pub const MAX_AMPDU_SIZE: i32 = 64;
pub const P_TX: f64 = 20.0;

pub const _INITIAL_BITRATE_MBPS_SIM: f32 = 100.0;

// Define a constant to control debugging

pub mod alvr_packets;
pub mod alvr_statistics;
pub mod alvr_stream_socket;
pub mod models_XR;
pub mod models_mm1k;

pub mod alvr_control_socket;

// pub type OptLazy<T> = Lazy<Mutex<Option<T>>>;
// pub const fn lazy_mut_none<T>() -> OptLazy<T> {
//     Lazy::new(|| Mutex::new(None))
// }
// pub static DEBUG_PRINT_ENABLED: bool = false;

pub const DEBUG_PRINT_ENABLED: bool = false; // Change to false to disable

pub const USE_FFMPEG: bool = true;
pub const USE_VMAF: bool   = true;

#[macro_export]
macro_rules! debug_bgprint {
    ($color:expr, $fmt:expr, $($arg:tt)*) => {
        if DEBUG_PRINT_ENABLED == true {
            let msg = format!($fmt, $($arg)*);
            println!("{}", $color.to_background_fn()(msg));
        }
    };
}

#[macro_export]
macro_rules! print_pretty {
    ($color:expr, $fmt:expr, $($arg:tt)*) => {
        // if DEBUG_PRINT_ENABLED == true {
            let msg = format!($fmt, $($arg)*);
            println!("{}", $color.to_color_fn()(msg));
        // }
    }
}

#[macro_export]
macro_rules! print_prettyy {
    ($color:expr, $fmt:expr, $($arg:tt)*) => {
        if DEBUG_PRINT_ENABLED == true {
            let msg = format!($fmt, $($arg)*);
            println!("{}", $color.to_background_fn()(msg));
        }
    };
}
#[macro_export]
macro_rules! print_prettyyyy {
    ($color:expr, $fmt:expr, $($arg:tt)*) => {
        // if DEBUG_PRINT_ENABLED == true {
            let msg = format!($fmt, $($arg)*);
            println!("{}", $color.to_background_fn()(msg));
        // }
    };
}
#[macro_export]
macro_rules! print_prettyyy {
    ($color:expr, $fmt:expr, $($arg:tt)*) => {
        // if DEBUG_PRINT_ENABLED == true {
            // let msg = format!($fmt, $($arg)*);
            // println!("{}", $color.to_background_fn()(msg));
        }
    // };
}

#[macro_export]
macro_rules! debug_print {
    ($color:expr, $fmt:expr, $($arg:tt)*) => {
        if DEBUG_PRINT_ENABLED == true {
            let msg = format!($fmt, $($arg)*);
            println!("{}", $color.to_color_fn()(msg));
        }
    }
}

#[macro_export]
macro_rules! format_elapsed {
    ($elapsed:expr) => {{
        let total_seconds =
            $elapsed.as_secs() as f64 + ($elapsed.subsec_nanos() as f64 / 1_000_000_000.0);
        format!("{:.9}", total_seconds)
    }};
}
#[macro_export]
macro_rules! taitime_to_f64 {
    ($tai:expr) => {{
        let secs = $tai.as_secs() as f64;
        let nanos = $tai.subsec_nanos() as f64;
        secs + (nanos / 1_000_000_000.0)
    }};
}

#[macro_export]
macro_rules! print_red {
    ($fmt:expr, $($arg:tt)*) => {
        let msg = format!($fmt, $($arg)*);
        println!("{}", DebugColor::Red.to_background_fn()(msg));
    };
}

#[macro_export]
macro_rules! print_yellow {
    ($fmt:expr, $($arg:tt)*) => {
        // let msg = format!($fmt, $($arg)*);
        // println!("{}", DebugColor::Yellow.to_background_fn()(msg));
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
// int AccessPoint :: BinaryExponentialBackoff(int attempt)
// {
// 	int CW = Random(MIN(pow(2,attempt),pow(2,max_BEB_stages))*(CWmin+1));
// 	return CW;
// };
#[allow(unused)] // to use for non-deterministic backoff
pub fn time_of_BinaryExponentialBackoff(attempt: i32) -> i32 {
        let max_beb_stages = 6;
        let cw_min = 15;
        
        // Calculate the upper bound for the random range
        let factor = (2_i32).pow(attempt.min(max_beb_stages) as u32);
        let upper_bound = factor * (cw_min + 1);
        
        // Generate a random number in range [0, upper_bound)
        let mut rng = rand::thread_rng();
        rng.gen_range(0..upper_bound)
    }

#[derive(Clone)]
pub struct SlidingWindowWeighted<T> {
    history_buffer: VecDeque<T>,
    interval_buffer: VecDeque<f32>,
}

impl<T> SlidingWindowWeighted<T> {
    pub fn new(initial_value: T, initial_interval: f32) -> Self {
        Self {
            history_buffer: [initial_value].into_iter().collect(),
            interval_buffer: [initial_interval].into_iter().collect(),
        }
    }

    pub fn submit_sample(&mut self, sample: T, interval: f32) {
        self.history_buffer.push_back(sample);
        self.interval_buffer.push_back(interval);
    }

    fn cleanup_old_samples(&mut self) {
        self.history_buffer.clear();
        self.interval_buffer.clear();
    }

    pub fn get_interval_buffer_sum(&self) -> f32 {
        self.interval_buffer.iter().sum::<f32>()
    }
}

impl SlidingWindowWeighted<f32> {
    pub fn weighted_sum(&self) -> f32 {
        self.history_buffer
            .iter()
            .zip(self.interval_buffer.iter())
            .map(|(value, weight)| value * weight)
            .sum()
    }

    pub fn get_average(&mut self) -> f32 {
        let average = self.weighted_sum() / self.get_interval_buffer_sum();
        self.cleanup_old_samples();
        return average;
    }
}

#[allow(unused)]
pub struct SlidingWindowTimely<T> {
    history_buffer: VecDeque<T>,
    interval_buffer: VecDeque<f32>,
    max_window_duration: f32,
}
#[allow(unused)]
impl<T> SlidingWindowTimely<T> {
    pub fn new(initial_value: T, initial_interval: f32, max_window_duration: f32) -> Self {
        Self {
            history_buffer: [initial_value].into_iter().collect(),
            interval_buffer: [initial_interval].into_iter().collect(),
            max_window_duration,
        }
    }

    pub fn submit_sample(&mut self, sample: T, interval: f32) {
        self.history_buffer.push_back(sample);
        self.interval_buffer.push_back(interval);
        self.cleanup_old_samples();
    }

    fn cleanup_old_samples(&mut self) {
        let mut total_interval = 0.0;
        let mut index = self.interval_buffer.len();

        for &interval in self.interval_buffer.iter().rev() {
            total_interval += interval;
            if total_interval > self.max_window_duration {
                break;
            }
            index -= 1;
        }

        while index > 0 {
            if self.interval_buffer.len() > 1 {
                // keep at least one
                self.history_buffer.pop_front();
                self.interval_buffer.pop_front();
            }
            index -= 1;
        }
    }

    pub fn get_interval_buffer_sum(&self) -> f32 {
        self.interval_buffer.iter().sum::<f32>()
    }

    pub fn get_interval_buffer_mean(&self) -> f32 {
        self.get_interval_buffer_sum() / self.interval_buffer.len() as f32
    }
    pub fn get_length(&self) -> usize {
        self.interval_buffer.len()
    }
}
#[allow(unused)]
impl SlidingWindowTimely<f32> {
    pub fn get_average(&self) -> f32 {
        self.history_buffer.iter().sum::<f32>() / self.history_buffer.len() as f32
    }

    pub fn get_sum(&self) -> f32 {
        self.history_buffer.iter().sum::<f32>()
    }

    pub fn get_std(&self) -> f32 {
        if self.history_buffer.len() < 2 {
            return 0.;
        }
        let average = self.get_average();
        let variance = self
            .history_buffer
            .iter()
            .map(|&x| (x - average).powf(2.))
            .sum::<f32>()
            / (self.history_buffer.len() - 1) as f32; // sample variance
        variance.sqrt()
    }
}
#[allow(dead_code)]
pub struct NalUnit {
    pub nal_type: u8,
    pub data: Vec<u8>,
    pub is_keyframe: bool,
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

#[derive(Clone)]
pub struct SlidingWindowAverage<T> {
    history_buffer: VecDeque<T>,
    max_history_size: usize,
}
#[allow(unused)]
impl<T> SlidingWindowAverage<T> {
    pub fn new(initial_value: T, max_history_size: usize) -> Self {
        Self {
            history_buffer: [initial_value].into_iter().collect(),
            max_history_size,
        }
    }

    pub fn submit_sample(&mut self, sample: T) {
        if self.history_buffer.len() >= self.max_history_size {
            self.history_buffer.pop_front();
        }

        self.history_buffer.push_back(sample);
    }

    pub fn retain(&mut self, count: usize) {
        self.history_buffer
            .drain(0..self.history_buffer.len().saturating_sub(count));
    }

    pub fn history_buffer_len(&self) -> usize {
        self.history_buffer.len()
    }
}

impl SlidingWindowAverage<f32> {
    pub fn get_average(&self) -> f32 {
        self.history_buffer.iter().sum::<f32>() / self.history_buffer.len() as f32
    }

    pub fn get_std(&self) -> f32 {
        if self.history_buffer.len() < 2 {
            return 0.;
        }
        let average = self.get_average();
        let variance = self
            .history_buffer
            .iter()
            .map(|&x| (x - average).powf(2.))
            .sum::<f32>()
            / (self.history_buffer.len() - 1) as f32; // sample variance
        variance.sqrt()
    }
}
#[allow(unused)]
impl SlidingWindowAverage<Duration> {
    pub fn get_average(&self) -> Duration {
        self.history_buffer.iter().sum::<Duration>() / self.history_buffer.len() as u32
    }
}

#[macro_export]
macro_rules! format_timestamp {
    ($elapsed:expr) => {{
        let total_seconds =
            $elapsed.as_secs() as f64 + ($elapsed.subsec_nanos() as f64 / 1_000_000_000.0);
        format!("{:.9}", total_seconds)
    }};
}

// pub fn exponential(mean: f64) -> f64 {
//     let mut rng = thread_rng();
//     let exp = Exp::new(1.0 / mean).unwrap();
//     let value = exp.sample(&mut rng);
//     value
// }

pub fn exponential(mean: f64) -> f64 {
    let mut rng = rand::thread_rng();
    let u: f64 = rng.gen_range(0.0..=1.0); // Generate a random value in the range (0, 1]
    -mean * u.ln()
}

// Separate struct to hold the data that will be shared
#[derive(Clone)]
pub struct CsvData {
    v_timestamp: Vec<String>,
    v_packet_id: Vec<usize>,
    v_queue_size: Vec<usize>,
    v_queue_ts: Vec<f64>,
    v_queue_tq: Vec<f64>,
    v_packet_l: Vec<usize>,

    v_id_src: Vec<usize>,
    v_id_dest: Vec<usize>,
}

impl CsvData {
    pub fn new() -> Self {
        Self {
            v_timestamp: Vec::new(),
            v_packet_id: Vec::new(),
            v_queue_size: Vec::new(),
            v_queue_ts: Vec::new(),
            v_queue_tq: Vec::new(),
            v_packet_l: Vec::new(),
            v_id_src: Vec::new(),
            v_id_dest: Vec::new(),
        }
    }
}

    
#[derive(Clone)]
#[allow(dead_code)]
pub struct CsvType {
    csv_data: Arc<Mutex<CsvData>>,
    folder: String,
}
#[allow(dead_code)]
impl CsvType {
    pub fn new(folder_name: &str) -> Self {
        Self {
            // v_timestamp: Vec::new(),
            // v_packet_id: Vec::new(),
            // v_queue_size: Vec::new(),
            // v_queue_ts: Vec::new(),
            // v_queue_tq: Vec::new(),
            // v_packet_l: Vec::new(),
            csv_data: Arc::new(Mutex::new(CsvData::new())),
            folder: folder_name.to_string(),
        }
    }

    pub fn get_data_handle(&self) -> Arc<Mutex<CsvData>> {
        Arc::clone(&self.csv_data)
    }

    pub fn update_stats(
        &mut self,
        now: TaiTime<0>,
        id_packet: usize,
        queue_size: usize,
        Ts: f64,
        Tq: f64,
        length_packet: usize,
        id_src: usize,
        id_dest: usize,
    ) {
        let formatted_timestamp = format_timestamp!(now);
        // debug_print!(
        //     DebugColor::Purple,
        //     "{} [DBG STATS QUEUE]Pushing to csv_data - timestamp: {}, packet ID: {}, queue size: {}, queue Ts: {}, queue Tq: {}, packet length: {}, source ID: {}, destination ID: {}",
        //     format_elapsed!(now),
        //     formatted_timestamp,
        //     id_packet,
        //     queue_size,
        //     Ts,
        //     Tq,
        //     length_packet,
        //     id_src,
        //     id_dest
        // );
        if let Ok(mut data) = self.csv_data.lock() {
            data.v_timestamp.push(formatted_timestamp);
            data.v_packet_id.push(id_packet);
            data.v_queue_size.push(queue_size);
            data.v_queue_ts.push(Ts);
            data.v_queue_tq.push(Tq);
            data.v_packet_l.push(length_packet);
            data.v_id_src.push(id_src);
            data.v_id_dest.push(id_dest);
        }

        // Dump all the current data to CSV each time this is called.
        if let Err(e) = self.save_network_stats_to_csv() {
            eprintln!("Error writing CSV: {}", e);
        }
    }

    /// Dumps the entire content of the in-memory vectors to the CSV file.
    /// After a successful write, the vectors are cleared.
    pub fn save_network_stats_to_csv(&self) -> io::Result<()> {
        // Construct the file path
        let file_path = format!("Results/{}/QUEUE_stats.csv", self.folder);
        let path = Path::new(&file_path);

        // Open the file in append mode; create it if necessary.
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;

        // If the file is empty, write a header.
        if file.metadata()?.len() == 0 {
            writeln!(
                file,
                "timestamp,packet_ID,queue_size,L_packet,T_s,T_q,id_src,id_dest"
            )?;
        }

        // Lock the shared data and drain its contents.
        let mut data = self.csv_data.lock().unwrap();

        // Assuming all vectors have the same length.
        for i in 0..data.v_timestamp.len() {
            // Format one CSV record from the current index.
            let record = format!(
                "{},{},{},{},{},{},{},{}",
                data.v_timestamp[i],
                data.v_packet_id[i],
                data.v_queue_size[i],
                data.v_packet_l[i],
                data.v_queue_ts[i],
                data.v_queue_tq[i],
                data.v_id_src[i],
                data.v_id_dest[i]
            );
            writeln!(file, "{}", record)?;
        }
        file.flush()?;

        // Clear the vectors so the same data is not written again.
        data.v_timestamp.clear();
        data.v_packet_id.clear();
        data.v_queue_size.clear();
        data.v_queue_ts.clear();
        data.v_queue_tq.clear();
        data.v_packet_l.clear();
        data.v_id_src.clear();
        data.v_id_dest.clear();

        Ok(())
    }
}

#[allow(unused)]
#[derive(Clone)]
pub struct CumulativeStats {
    values: VecDeque<f64>,
    sum: f64,
    sum_of_squares: f64,
}
#[allow(unused)]
impl CumulativeStats {
    // Constructor
    pub fn new() -> Self {
        Self {
            values: VecDeque::new(),
            sum: 0.0,
            sum_of_squares: 0.0,
        }
    }

    // Add a new value
    pub fn add(&mut self, value: f64) {
        self.values.push_back(value);
        self.sum += value;
        self.sum_of_squares += value * value;
    }

    // Get average
    pub fn get_average(&self) -> f64 {
        if self.values.is_empty() {
            0.0
        } else {
            self.sum / self.values.len() as f64
        }
    }

    // Get standard deviation
    pub fn get_std_dev(&self) -> f64 {
        if self.values.len() < 2 {
            println!("LESS THAN TWO???");
            return 0.0;
        }

        let mean = self.get_average();
        let sum_sq_diff: f64 = self
            .values
            .iter()
            .map(|&value| {
                let diff = value - mean;
                diff * diff
            })
            .sum();

        // let std = (sum_sq_diff / (self.values.len() as f64 - 1.0)).sqrt();
        (sum_sq_diff / (self.values.len() as f64 - 1.0)).sqrt()
    }

    // Get coefficient of variation
    pub fn get_coefficient_variation(&self) -> f64 {
        let mean = self.get_average();
        if mean == 0.0 {
            println!("CV IS ZERO??");
            0.0
        } else {
            self.get_std_dev() / mean
        }
    }

    // Get second moment
    pub fn get_2nd_moment(&self) -> f64 {
        if self.values.is_empty() {
            0.0
        } else {
            self.sum_of_squares / self.values.len() as f64
        }
    }

    // Get last value
    pub fn get_last_value(&self) -> Option<f64> {
        if let Some(&last_value) = self.values.back() {
            Some(last_value)
        } else {
            eprintln!("[ERROR!] No values have been added yet.");
            None
        }
    }
}

#[allow(non_camel_case_types, unused)]
#[derive(Clone)]
pub struct perStaStats {
    pub sta_id: i32,
    pub _rx_packets_counter: i32,
    pub _q_time_sta_cum: CumulativeStats,
    pub _s_time_sta_cum: CumulativeStats,
    pub _csv_data: CsvData,
}
#[allow(non_camel_case_types, unused)]
impl perStaStats {
    pub fn new() -> Self {
        Self {
            sta_id: -1,
            _q_time_sta_cum: CumulativeStats::new(),
            _s_time_sta_cum: CumulativeStats::new(),
            _csv_data: CsvData::new(),
            _rx_packets_counter: 0,
        }
    }
    pub fn print_nicely(&self) {
        let title = format!("STA {}", self.sta_id + 1);

        // Define table rows with `let` bindings to extend the lifetime of the formatted strings
        let tq_label = format!("E[T_q]");
        let ts_label = format!("E[T_s]");

        let rows = vec![
            (
                "Packets received from STA:",
                format!("{:>10}", self._rx_packets_counter),
            ),
            (
                &tq_label,
                format!("{:>10.6}", self._q_time_sta_cum.get_average()),
            ),
            (
                &ts_label,
                format!("{:>10.6}", self._s_time_sta_cum.get_average()),
            ),
        ];

        // Print the table
        println!("+---------------------------------------------------+");
        println!("| {}                                              |", title);
        println!("+---------------------------------------------------+");
        for (label, value) in rows {
            println!("| {:<35} | {:>12} |", label, value);
        }
        println!("+------------------------------------------------+\n");
    }

    pub fn update_stats_per_sta(
        &mut self,
        now: TaiTime<0>,
        id_packet: usize,
        queue_size: usize,
        Ts: f64,
        Tq: f64,
        length_packet: usize,
        sta_src_id: usize,
        sta_dest_id: usize,
    ) {
        self._q_time_sta_cum.add(Tq);
        self._s_time_sta_cum.add(Ts);
        self._rx_packets_counter += 1;

        let formatted_timestamp = format_timestamp!(now);

        // debug_print!(
        //     DebugColor::Purple,
        //     "{} [DBG STATS STA] Pushing to csv_data - timestamp: {}, packet ID: {}, queue size: {}, queue Ts: {}, queue Tq: {}, packet length: {}, source ID: {}, destination ID: {}",
        //     format_elapsed!(now),
        //     formatted_timestamp,
        //     id_packet,
        //     queue_size,
        //     Ts,
        //     Tq,
        //     length_packet,
        //     sta_src_id,
        //     sta_dest_id
        // );

        self._csv_data.v_timestamp.push(formatted_timestamp);
        self._csv_data.v_packet_id.push(id_packet);
        self._csv_data.v_queue_size.push(queue_size);
        self._csv_data.v_queue_ts.push(Ts);
        self._csv_data.v_queue_tq.push(Tq);
        self._csv_data.v_packet_l.push(length_packet);
        self._csv_data.v_id_src.push(sta_src_id);
        self._csv_data.v_id_dest.push(sta_dest_id);
    }
}
#[allow(non_camel_case_types)]
#[derive(Clone)]
pub struct perStaLockStats {
    pub data: Arc<Mutex<perStaStats>>,
}
impl perStaLockStats {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(perStaStats::new())),
        }
    }
}

#[allow(unused)]
#[derive(Debug)]
pub struct LittleTheoremMM1K {
    // pub k: i32,      // Max capacity of system (u)
    pub lambda: f64, // Arrival rate
    pub mu: f64,     // Service rate
    pub rho: f64,    // Utilization factor
    pub n: f64,      // Average number of packets in the system
    pub n_q: f64,    // Average number of packets in queue
    pub p_0: f64,    // Probability of 0 packets in the system
    pub p_k: f64,    // Blocking probability
    pub t: f64,      // Average time in the system
    pub t_q: f64,    // avg. Waiting time in queue
    pub t_s: f64,    // avg. Service time
}

#[allow(unused)]
impl LittleTheoremMM1K {
    pub fn print_results(&self) {
        let title = "ANALYTICAL RESULTS (M/M/1/K)";
        let separator = "+------------------------------------------------+";

        // Print title and separator
        println!("{}", separator);
        println!("| {:<46} |", title);
        println!("{}", separator);

        // Print each field in the desired format
        println!(
            "| {:<27} | {:>15} |",
            "λ (average arrival rate)",
            format!("{:.6}", self.lambda)
        );
        println!(
            "| {:<27} | {:>15} |",
            "µ (service rate)",
            format!("{:.6}", self.mu)
        );
        println!(
            "| {:<27} | {:>15} |",
            "ρ (utilization factor)",
            format!("{:.6}", self.rho)
        );
        println!(
            "| {:<27} | {:>15} |",
            "P_0 (Prob. of 0 pkts)",
            format!("{:.6}", self.p_0)
        );
        println!(
            "| {:<27} | {:>15} |",
            "P_K (Blocking prob.)",
            format!("{:.6}", self.p_k)
        );
        println!(
            "| {:<27} | {:>15} |",
            "N (Avg. pkts in system)",
            format!("{:.6}", self.n)
        );
        println!(
            "| {:<27} | {:>15} |",
            "N_q (Avg. pkts in queue)",
            format!("{:.6}", self.n_q)
        );
        println!(
            "| {:<27} | {:>15} |",
            "-----------------------------", "-----------------"
        ); // Just for formatting
        println!(
            "| {:<27} | {:>15} |",
            "T (Avg. time in system)",
            format!("{:.6}", self.t)
        );
        println!(
            "| {:<27} | {:>15} |",
            "T_q (Avg. time in queue)",
            format!("{:.6}", self.t_q)
        );
        println!(
            "| {:<27} | {:>15} |",
            "T_s (Avg. service time)",
            format!("{:.6}", self.t_s)
        );
        println!("{}", separator);
    }
}

#[allow(unused)]
pub fn compute_mm1k_metrics(
    bandwidth_source: f64,
    l_packets: f64,
    bandwidth_departures: f64,
    k: usize,
) -> LittleTheoremMM1K {
    let lambda = bandwidth_source / l_packets as f64;
    let mu = bandwidth_departures / l_packets as f64;
    let rho = lambda / mu;
    let k_i: i32 = k as i32;

    if rho >= 1.0 {
        eprintln!("Unstable system: lambda must be less than mu.");
    }

    // Probability of 0 packets in the system
    let p_0 = (1.0 - rho) / (1.0 - rho.powi(k_i + 1));

    // Blocking probability
    let p_k = p_0 * rho.powi(k_i);

    // Average number of packets in the system
    let n = rho / (1.0 - rho) - ((k + 1) as f64 * rho.powi(k_i + 1)) / (1.0 - rho.powi(k_i + 1));
    let n_q = n - (1.0 - p_0);

    // Average time in the system
    let t = n / (lambda * (1.0 - p_k));
    let t_s = l_packets as f64 / bandwidth_departures;
    let t_q = t - t_s;

    LittleTheoremMM1K {
        // k: k_i,
        lambda,
        mu,
        rho,
        n,
        n_q,
        p_0,
        p_k,
        t,
        t_q,
        t_s,
    }
}

#[allow(unused)]
#[derive(Default, Debug, Clone)]
pub struct HeaderALVRStream {
    pub packet_length: u32,
    pub stream_id: u16,
    pub next_packet_index: u32,
    pub shards_count: u32,
    pub shard_index: u32,
    pub tx_instant: f32,
}

// Implementing Display for HeaderALVRStream
impl fmt::Display for HeaderALVRStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.next_packet_index == 0 {
            write!(f, "")
        } else {
            write!(
                f,
                "(ALVR F: {}, S: {}/{})",
                self.next_packet_index,
                self.shard_index,
                self.shards_count - 1
            )
        }
    }
}
#[derive(Debug, Clone)]
pub struct MpduPacket {
    pub packet_id: usize,
    pub length_packet: usize,
    pub queue_in_instant: TaiTime<0>,
    pub queue_out_instant: TaiTime<0>,
    pub T_q: Duration,
    pub T_s: Duration,
    // pub expected_T_s: Duration,
    pub sta_src_id: i32,
    pub sta_dest_id: i32,
    pub sta_src_coords: Coords,
    pub queue_length_when_out: usize,

    pub data_inner: Vec<u8>,
    pub header_alvr: HeaderALVRStream,

    pub has_consumed_emu_tokens: bool,
    pub emulated_added_delay_deadline: Option<TaiTime<0>>,
    // pub is_alvr_control_packet: bool,
}

#[allow(unused)]
impl MpduPacket {
    pub fn new() -> Self {
        Self {
            packet_id: 0,
            length_packet: 0,
            queue_in_instant: TaiTime::default(),
            queue_out_instant: TaiTime::default(),
            T_q: Duration::ZERO,
            T_s: Duration::ZERO,
            // expected_T_s: Duration::ZERO,
            sta_src_id: 0,
            sta_dest_id: 0,
            sta_src_coords: Coords::with_coords(0.0, 0.0, 0.0),
            queue_length_when_out: 0,
            data_inner: vec![],
            header_alvr: HeaderALVRStream::default(),
            has_consumed_emu_tokens: false,
            emulated_added_delay_deadline: None,
            // is_alvr_control_packet: false,
        }
    }

    pub fn print(&self, color: DebugColor) -> String {
        print_pretty!(
            color,
            "SRC: {} DEST: {}|  Packet ID: {}, ALVR F: {} S: {}/{} L: {}",
            self.sta_src_id,
            self.sta_dest_id, 
            self.packet_id,
            self.header_alvr.next_packet_index,
            self.header_alvr.shard_index,
            self.header_alvr.shards_count - 1,
            self.length_packet
            
        );
        let a = format!(
            "SRC: {} DEST: {}| Packet ID: {}, ALVR F: {} S: {}/{} L: {}",
            
            self.sta_src_id,
            self.sta_dest_id, 
            self.packet_id,
            self.header_alvr.next_packet_index,
            self.header_alvr.shard_index,
            self.header_alvr.shards_count - 1,
            self.length_packet
        );
        a
    }
}
#[derive(Debug, Clone)]
pub struct AmpduPacket {
    pub mpdu_packets: Vec<MpduPacket>, // Container for MPDU packets
    pub total_length: usize,           // Total length of aggregated packets
    pub sta_src_id: i32,
    pub sta_dest_id: i32, // ID for the destination STA
    pub size: i32,
    pub coordinates: Coords,
}

impl AmpduPacket {
    pub fn new() -> Self {
        AmpduPacket {
            mpdu_packets: Vec::new(), // Initialize an empty vector for MPDU packets
            total_length: 0,          // Initialize total length to 0
            sta_src_id: -1,
            sta_dest_id: -1, // Initialize STA_ID to -1 (assuming -1 indicates uninitialized)

            size: 0, // Initialize size to 0
            coordinates: Coords {
                x: 0.0,
                y: 0.0,
                z: 0.0,
            }, // Initialize coordinates to (0.0, 0.0, 0.0)
        }
    }
    // Method to print AMPDU_packet values
    pub fn print(&self) {
        println!(
            "\x1b[33m \t[AMPDU INFO]\tSize: {}, STA_src_ID: {}, STA_dest_ID: {}, Total Length: {}\x1b[0m",
            self.size, self.sta_src_id, self.sta_dest_id, self.total_length
        );
        for packet in &self.mpdu_packets {
            println!(
                "\x1b[33m\t - Packet ID: {:.0}, T_q: {:.3} ms , T_s: {:.3} ms",
                // |  ALVR: S{}/{} , F: {}  \x1b[0m",
                packet.packet_id,
                packet.T_q.as_secs_f64() * 1000.0,
                packet.T_s.as_secs_f64() * 1000.0,
                // packet.header_alvr.shard_index,
                // packet.header_alvr.shards_count - 1,
                // packet.header_alvr.next_packet_index,
            );
        }
    }

    // Method to reinitialize all values
    pub fn reset(&mut self) {
        self.mpdu_packets.clear(); // Clear the vector of MPDU packets
                                   // self.mpdu_packets.reserve(MAX_AMPDU_SIZE as usize);
        self.total_length = 0; // Reset total length
        self.size = 0; // Reset size
        self.sta_dest_id = -1; // Reset STA_ID (assuming -1 is an uninitialized value)
        self.coordinates = Coords {
            x: 0.0,
            y: 0.0,
            z: 0.0,
        }; // Reset coordinates to default (0.0, 0.0)
    }

    // fn with_capacity(capacity: usize) -> Self {
    //     Self {
    //         mpdu_packets: Vec::with_capacity(capacity),
    //         sta_id: 0,
    //         coordinates: Coords::new(),
    //         size: 0,
    //         total_length: 0,
    //     }
    // }

    // fn is_empty(&self) -> bool {
    //     self.mpdu_packets.is_empty()
    // }

    // fn add_packet(&mut self, packet: MpduPacket) {
    //     self.total_length += packet.length_packet;
    //     self.size += 1;
    //     self.mpdu_packets.push(packet);
    // }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Coords {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

impl Coords {
    pub fn new() -> Self {
        Self {
            x: 0.0,
            y: 0.0,
            z: 0.0,
        }
    }
    pub fn with_coords(x: f64, y: f64, z: f64) -> Self {
        Self { x, y, z }
    }
}
#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
pub struct ResultsFrameTXDelay {
    pub service_delay: f64,
    pub data_service_delay: f64,
    pub pathloss: f64,
    pub p_rx: f64, // Rust doesn't have a separate `long double`, so f64 is used
                   // pub o_rate: f64,
}

impl ResultsFrameTXDelay {
    pub fn new() -> Self {
        Self {
            service_delay: 0.0,
            data_service_delay: 0.0,
            pathloss: 0.0,
            p_rx: 0.0,
        }
    }

}

pub fn calculate_distance(x: f64, y: f64, z: f64, x_: f64, y_: f64, z_: f64) -> f64 {
    let dx = x_ - x;
    let dy = y_ - y;
    let dz = z_ - z;

    (dx * dx + dy * dy + dz * dz).sqrt()
}

pub fn path_loss(d: f64) -> f64 {
    let gamma = 2.06067_f64;
    54.12 + 10.0 * gamma * (d).log10() + 5.25 * 0.1467 * d
}


pub fn collision_delay(
    _total_bits_transmitted: f64, 
    _n_mpdus: i32,
    coords_src: Coords,
    coords_dest: Coords,
    p_tx: f64,) -> f32 {

    let mut effPt = p_tx;

    let SU_spatial_streams = 2.0;

    if SU_spatial_streams > 1.0 {
        effPt = effPt - 3.0 * SU_spatial_streams
    };

    let channel_width: usize = CHANNEL_WIDTH;

    // Effective Pt

    if channel_width > 20 {
        effPt = effPt - 3.0 * (channel_width as f64 / 20.0);
    }
    let distance = calculate_distance(
        coords_src.x,
        coords_src.y,
        coords_src.z,
        coords_dest.x,
        coords_dest.y,
        coords_dest.z,
    );

    let PL = path_loss(distance);
    let Pr = effPt - PL;

    // println!("AP to STA: I'm at {:?} and you're at {:?} |  Distance = {:.2}, PL = {:.2}, P_rx = {:.1}", coords_src, coords_dest, distance, PL, Pr);

    let (bits_symbol, coding_rate) = match Pr {
        _ if Pr < -82.0 => (1, 1.0 / 2.0),
        _ if Pr >= -82.0 && Pr < -79.0 => (1, 1.0 / 2.0),
        _ if Pr >= -79.0 && Pr < -77.0 => (2, 1.0 / 2.0),
        _ if Pr >= -77.0 && Pr < -74.0 => (2, 3.0 / 4.0),
        _ if Pr >= -74.0 && Pr < -70.0 => (4, 1.0 / 2.0),
        _ if Pr >= -70.0 && Pr < -66.0 => (4, 3.0 / 4.0),
        _ if Pr >= -66.0 && Pr < -65.0 => (6, 1.0 / 2.0),
        _ if Pr >= -65.0 && Pr < -64.0 => (6, 2.0 / 3.0),
        _ if Pr >= -64.0 && Pr < -59.0 => (6, 3.0 / 4.0),
        _ if Pr >= -59.0 && Pr < -57.0 => (8, 3.0 / 4.0),
        _ if Pr >= -57.0 && Pr < -55.0 => (6, 5.0 / 6.0),
        _ if Pr >= -55.0 && Pr < -53.0 => (10, 3.0 / 4.0),
        _ if Pr >= -53.0 && Pr < -49.0 => (10, 5.0 / 6.0),
        _ if Pr >= -49.0 && Pr < -46.0 => (12, 3.0 / 4.0),  // MCS 12, TODO: find a good reference for 802.11be SNR
        _ if Pr >= -46.0               => (12, 5.0 / 6.0),  // MCS 13
        _ => (1, 1.0 / 2.0), // Catch-all for Pr out of range
    };


    let Subcarriers = match channel_width {
        // https://www.arubanetworks.com/assets/wp/WP_802.11AX.pdf, page 12
        80 => 980,
        40 => 468,
        20 => 234,
        _ => 0, // Default case,  fallback
    };

    let _ORate: f64 = SU_spatial_streams * bits_symbol as f64 * coding_rate * Subcarriers as f64;

    let OBasicRate: f64 = 1.0 / 2.0 * 1.0 * 48.0;

    // let _L: f64 = total_bits_transmitted / n_mpdus as f64;

    let SF = 16.0;
    let TB = 18.0;
    // let _MD = 32.0;
    // let _MAC_H_size = 240.0;

    let T_RTS: f64 = LEGACY_PHY_DURATION + ((SF + 160.0 + TB) / OBasicRate).ceil() * 4E-6; // legacy symbol time is 4E-6
    let T_CTS: f64 = LEGACY_PHY_DURATION + ((SF + 112.0 + TB) / OBasicRate).ceil() * 4E-6;
    // let _T_DATA: f64 =
    //     PHY_DURATION + ((SF + n_mpdus as f64 * (_MD + _MAC_H_size + _L) + TB) / _ORate).ceil() * 16E-6;
    // let _T_ACK: f64 = LEGACY_PHY_DURATION + ((SF + 240.0 + TB) / OBasicRate).ceil() * 4E-6;



    let T_DETERMINISTIC_BACKOFF = (CW_MIN as f64 - 1.0) / 2.0 * SLOT; // add small time constant between consecutive TX to model backoff
    // let T_BACKOFF = time_of_BinaryExponentialBackoff(); // make random BO at least for the 1st time

    // let T = T_RTS + SIFS + T_CTS + SIFS + T_DATA + SIFS + T_ACK + DIFS + SLOT + T_BACKOFF;   


    let T_collision = T_RTS + SIFS + T_CTS + DIFS + SLOT + T_DETERMINISTIC_BACKOFF; 
    T_collision as f32

}


pub fn frametransmission_delay(
    total_bits_transmitted: f64,
    n_mpdus: i32,
    coords_src: Coords,
    coords_dest: Coords,
    p_tx: f64,
) -> ResultsFrameTXDelay {
    let mut effPt = p_tx;

    let SU_spatial_streams = 2.0;

    if SU_spatial_streams > 1.0 {
        effPt = effPt - 3.0 * SU_spatial_streams
    };

    let channel_width: usize = CHANNEL_WIDTH;

    // Effective Pt

    if channel_width > 20 {
        effPt = effPt - 3.0 * (channel_width as f64 / 20.0);
    }
    let distance = calculate_distance(
        coords_src.x,
        coords_src.y,
        coords_src.z,
        coords_dest.x,
        coords_dest.y,
        coords_dest.z,
    );

    let PL = path_loss(distance);
    let Pr = effPt - PL;

    // println!("AP to STA: I'm at {:?} and you're at {:?} |  Distance = {:.2}, PL = {:.2}, P_rx = {:.1}", coords_src, coords_dest, distance, PL, Pr);

    let (bits_symbol, coding_rate) = match Pr {
        _ if Pr < -82.0 => (1, 1.0 / 2.0),
        _ if Pr >= -82.0 && Pr < -79.0 => (1, 1.0 / 2.0),
        _ if Pr >= -79.0 && Pr < -77.0 => (2, 1.0 / 2.0),
        _ if Pr >= -77.0 && Pr < -74.0 => (2, 3.0 / 4.0),
        _ if Pr >= -74.0 && Pr < -70.0 => (4, 1.0 / 2.0),
        _ if Pr >= -70.0 && Pr < -66.0 => (4, 3.0 / 4.0),
        _ if Pr >= -66.0 && Pr < -65.0 => (6, 1.0 / 2.0),
        _ if Pr >= -65.0 && Pr < -64.0 => (6, 2.0 / 3.0),
        _ if Pr >= -64.0 && Pr < -59.0 => (6, 3.0 / 4.0),
        _ if Pr >= -59.0 && Pr < -57.0 => (8, 3.0 / 4.0),
        _ if Pr >= -57.0 && Pr < -55.0 => (6, 5.0 / 6.0),
        _ if Pr >= -55.0 && Pr < -53.0 => (10, 3.0 / 4.0),
        _ if Pr >= -53.0 && Pr < -49.0 => (10, 5.0 / 6.0),
        _ if Pr >= -49.0 && Pr < -46.0 => (12, 3.0 / 4.0),  // MCS 12, TODO: find a good reference for 802.11be SNR
        _ if Pr >= -46.0               => (12, 5.0 / 6.0),  // MCS 13
        _                              => (1, 1.0 / 2.0),   // Catch-all for Pr out of range
    };


    let Subcarriers = match channel_width {
        // https://www.arubanetworks.com/assets/wp/WP_802.11AX.pdf, page 12
        80 => 980,
        40 => 468,
        20 => 234,
        _ => 0, // Default case,  fallback
    };

    let ORate: f64 = SU_spatial_streams * bits_symbol as f64 * coding_rate * Subcarriers as f64;

    let OBasicRate: f64 = 1.0 / 2.0 * 1.0 * 48.0;

    let L: f64 = total_bits_transmitted / n_mpdus as f64;

    let SF = 16.0;
    let TB = 18.0;
    let MD = 32.0;
    let MAC_H_size = 240.0;

    let T_RTS: f64 = LEGACY_PHY_DURATION + ((SF + 160.0 + TB) / OBasicRate).ceil() * 4E-6; // legacy symbol time is 4E-6
    let T_CTS: f64 = LEGACY_PHY_DURATION + ((SF + 112.0 + TB) / OBasicRate).ceil() * 4E-6;
    let T_DATA: f64 =
        PHY_DURATION + ((SF + n_mpdus as f64 * (MD + MAC_H_size + L) + TB) / ORate).ceil() * 16E-6;
    let T_ACK: f64 = LEGACY_PHY_DURATION + ((SF + 240.0 + TB) / OBasicRate).ceil() * 4E-6;

    let T_DETERMINISTIC_BACKOFF = (CW_MIN as f64 - 1.0) / 2.0 * SLOT; // add small time constant between consecutive TX to model backoff
    // let T_BACKOFF = time_of_BinaryExponentialBackoff(); // make random BO at least for the 1st time

    let T = T_RTS + SIFS + T_CTS + SIFS + T_DATA + SIFS + T_ACK + DIFS + SLOT + T_DETERMINISTIC_BACKOFF;

    // println!("[DEBUUUG FT_DELAY] L_total = {:.2}, N_MPDUs = {}, T_s : {},  x: {:.1}, y: {:.1}\n", total_bits_transmitted, n_mpdus, T, coords_dest.x, coords_dest.y );

    ResultsFrameTXDelay {
        pathloss: PL,
        p_rx: Pr,
        // o_rate: ORate,
        service_delay: T,
        data_service_delay: T_DATA,
    }
}

#[allow(unused)] // as it's shared with other sims than XR. 
pub fn write_all_sta_csvs(
    sta_stats_vec: &HashMap<usize, perStaLockStats>,
    folder: &str,
    results_folder: &str,
) -> std::io::Result<()> {
    for (_index, sta_stats) in sta_stats_vec.iter() {
        // Lock the mutex to access the data
        if let Ok(stats) = sta_stats.data.lock() {
            // Create a filename with the station ID

            let filename: String = format!("{results_folder}/STA{}.csv", stats.sta_id);
            println!("FILENAME222: {filename}");
            // Open file with write permissions
            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&filename)?;

            let mut writer = Writer::from_writer(file);

            // Write header
            writer.write_record(&[
                "timestamp",
                "packet_ID",
                "queue_size",
                "L_packet",
                "T_s",
                "T_q",
                "id_src",
                "id_dest",
            ])?;

            // Write all stored data for this station
            for i in 0..stats._csv_data.v_timestamp.len() {
                writer.write_record(&[
                    &stats._csv_data.v_timestamp[i],
                    &stats._csv_data.v_packet_id[i].to_string(),
                    &stats._csv_data.v_queue_size[i].to_string(),
                    &stats._csv_data.v_packet_l[i].to_string(),
                    &stats._csv_data.v_queue_ts[i].to_string(),
                    &stats._csv_data.v_queue_tq[i].to_string(),
                    &stats._csv_data.v_id_src[i].to_string(),
                    &stats._csv_data.v_id_dest[i].to_string(),
                ])?;
            }

            writer.flush()?;

            // Optionally, print summary statistics for this station
            // println!("Station {} Statistics:", stats.sta_id);
            // println!(
            //     "  Average queue time: {:.6}",
            //     stats.q_time_sta_cum.get_average()
            // );
            // println!(
            //     "  Average service time: {:.6}",
            //     stats.s_time_sta_cum.get_average()
            // );
            println!("  CSV written to: {}", filename);
        }
    }
    Ok(())
}
// Bitrate statistics minus the empirical output value
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct NominalBitrateStats {
    pub scaled_calculated_bps: Option<f32>,
    pub decoder_latency_limiter_bps: Option<f32>,
    pub network_latency_limiter_bps: Option<f32>,
    pub encoder_latency_limiter_bps: Option<f32>,
    pub manual_max_bps: Option<f32>,
    pub manual_min_bps: Option<f32>,
    pub requested_bps: f32,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct GraphNetworkStatistics {
    pub frame_index: u32,

    pub frame_size_bytes: usize,

    pub client_fps: f32,
    pub server_fps: f32,

    pub frame_span_ms: f32,

    pub interarrival_jitter_ms: f32,

    pub ow_delay_ms: f32,
    pub filtered_ow_delay_ms: f32,

    pub rtt_ms: f32,

    pub frame_interarrival_ms: f32,
    pub frame_jitter_ms: f32,

    pub frames_skipped: u32,

    pub shards_lost: isize,
    pub shards_duplicated: u32,

    pub instant_network_throughput_bps: f32,
    pub peak_network_throughput_bps: f32,

    pub nominal_bitrate: NominalBitrateStats,

    pub interval_avg_plot_throughput: f32,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct GraphNetworkStatisticsCsv {
    pub timestamp: f64,
    pub frame_index: usize,

    pub frame_size_bytes: usize,

    pub client_fps: f32,
    pub server_fps: f32,

    pub frame_span_ms: f32,

    pub interarrival_jitter_ms: f32,

    pub ow_delay_ms: f32,
    pub filtered_ow_delay_ms: f32,

    pub rtt_ms: f32,

    pub frame_interarrival_ms: f32,
    pub frame_jitter_ms: f32,

    pub frames_skipped: u32,

    pub shards_lost: isize,
    pub shards_duplicated: u32,

    pub instant_network_throughput_bps: f32,
    pub peak_network_throughput_bps: f32,

    pub requested_bps: f32,

    pub interval_avg_plot_throughput: f32,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogSeverity {
    Error = 3,
    Warning = 2,
    Info = 1,
    Debug = 0,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LogEntry {
    pub severity: LogSeverity,
    pub content: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct StatisticsSummary {
    pub video_packets_total: usize,
    pub video_packets_per_sec: usize,

    pub video_mbytes_total: usize,
    pub video_mbits_per_sec: f32,

    pub video_throughput_mbits_per_sec: f32,

    pub total_pipeline_latency_average_ms: f32,
    pub game_delay_average_ms: f32,
    pub server_compositor_delay_average_ms: f32,
    pub encode_delay_average_ms: f32,
    pub network_delay_average_ms: f32,
    pub decode_delay_average_ms: f32,
    pub decoder_queue_delay_average_ms: f32,
    pub client_compositor_average_ms: f32,
    pub vsync_queue_delay_average_ms: f32,

    pub packets_dropped_total: usize,
    pub packets_dropped_per_sec: usize,

    pub packets_skipped_total: usize,
    pub packets_skipped_per_sec: usize,

    pub frame_jitter_ms: f32,

    pub client_fps: f32,
    pub server_fps: f32,

    pub battery_hmd: u32,
    pub hmd_plugged: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct GraphStatistics {
    pub frame_index: i32,
    pub is_idr: bool,

    pub frames_dropped: u32,

    pub total_pipeline_latency_s: f32,
    pub game_time_s: f32,
    pub server_compositor_s: f32,
    pub encoder_s: f32,
    pub network_s: f32,
    pub decoder_s: f32,
    pub decoder_queue_s: f32,
    pub client_compositor_s: f32,
    pub vsync_queue_s: f32,

    //pub client_fps: f32,
    //pub server_fps: f32,
    pub nominal_bitrate: NominalBitrateStats,
    pub actual_bitrate_bps: f32,
}

#[derive(Serialize, Deserialize, Clone, Debug, Copy, Default)]
pub struct HeuristicStats {
    pub frame_interval_s: f32,
    pub server_fps: f32,
    pub steps_mbps: f32,

    pub network_heur_fps: f32,
    pub rtt_avg_heur_s: f32,
    pub random_prob: f32,

    pub threshold_fps: f32,
    pub threshold_rtt_s: f32,
    pub threshold_u: f32,

    pub capacity_estimated_mbps: f32,

    pub requested_bitrate_mbps: f32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HapticsEvent {
    pub path: String,
    pub duration: Duration,
    pub frequency: f32,
    pub amplitude: f32,
}
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TrackingEvent {
    pub head_motion: Option<DeviceMotion>,
    pub controller_motions: [Option<DeviceMotion>; 2],
    pub hand_skeletons: [Option<[Pose; 26]>; 2],
    pub eye_gazes: [Option<Pose>; 2],
    pub fb_face_expression: Option<Vec<f32>>,
    pub htc_eye_expression: Option<Vec<f32>>,
    pub htc_lip_expression: Option<Vec<f32>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum EventType {
    Log(LogEntry),
    // Session(Box<SessionConfig>),
    StatisticsSummary(StatisticsSummary),
    GraphStatistics(GraphStatistics),
    GraphNetworkStatistics(GraphNetworkStatistics),
    HeuristicStats(HeuristicStats),
    Tracking(Box<TrackingEvent>),
    // Buttons(Vec<ButtonEvent>),
    Haptics(HapticsEvent),
    // AudioDevices(AudioDevicesList),
    // DriversList(Vec<PathBuf>),
    ServerRequestsSelfRestart,
}

// pub fn simpler_frametx_delay(bandwidth_dep:f64, mean_l: f64 )->ResultsFrameTXDelay {
//     ResultsFrameTXDelay{
//         pathloss: 0.,
//         p_rx: 0.0,
//         o_rate: bandwidth_dep,
//         service_delay: mean_l / bandwidth_dep,
//         data_service_delay: mean_l / bandwidth_dep,
//     }

// }  unused
