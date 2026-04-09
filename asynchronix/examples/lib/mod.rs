#![allow(non_snake_case)]
#![allow(unused)]
use std::collections::VecDeque;
use std::f64;

use csv::Writer;
use std::fs::OpenOptions;
use tai_time::TaiTime;

use crate::lib::alvr_packets::DeviceMotion;
use crate::lib::alvr_packets::Pose;
use crate::lib::models_XR::PerfectInfoBitrateMessage;
use crate::lib::models_mm1k::{STR_PLUS_MODE_MLO, AP_X, AP_Y};
use colored::Colorize;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use std::io::{self, Write};
use std::path::PathBuf;

pub const BATCH_SIZE_CSV_QUEUE: usize = 256 * 4;
pub const BATCH_SIZE_CSV_VIDEO: usize = 64; 


// const PE_DURATION: f64 = 16E-6;         // 802.11ax/be Packet Extension (4-20 us, Max 20us for QAM-4096 STAs)
const PE_DURATION: f64 = 0.0;         // not considered, matching WLAN toolbox

pub const LEGACY_PHY_DURATION:f64 = 20E-6; // microseconds
// pub const PHY_DURATION: f64 = 100E-6;
pub const EHT_PHY_DURATION: f64 = 76E-6;    // 802.11be Preamble    // L-STF      :   8.00 us
                                                                    // L-LTF      :   8.00 us
                                                                    // L-SIG      :   4.00 us
                                                                    // RL-SIG     :   4.00 us
                                                                    // U-SIG      :   8.00 us
                                                                    // EHT-SIG    :   8.00 us
                                                                    // EHT-STF    :   4.00 us
                                                                    // EHT-LTF    :  32.00 us
pub const SLOT: f64 = 9E-6;
pub const SIFS: f64 = 16E-6;

pub const SYMBOL_TIME_LEGACY: f64 = 4E-6; 
pub const SYMBOL_TIME_11AX: f64 = 16E-6; // 12.8 us symbol + 3.2 us guard interval. 
pub const DEFAULT_TMAX_AGG: f64 = 5.484E-3; 
pub const P_TX: f64 = 20.0;
#[allow(unused)]
pub const UPLINK_QUEUE_SIZE: usize = 1024;
pub const DOWNLINK_QUEUE_SIZE: usize = 1024;

pub const NUMBER_OF_RANDOM_EVENTS: usize = 20;

pub const _INITIAL_BITRATE_MBPS_SIM: f32 = 100.0;
#[allow(unused)]
pub const PREFIX_ID_DOWNLINK: i32 = 100;
#[allow(unused)]
pub const PREFIX_ID_UPLINK: i32 = 200;
#[allow(unused)]
pub const PREFIX_ID_BG: i32 = 300;

// Define a constant to control debugging

pub mod alvr_packets;
pub mod alvr_statistics;
pub mod alvr_stream_socket;
pub mod models_XR;
pub mod models_mm1k;

pub mod alvr_control_socket;
pub mod taitime_serde;

pub mod fovoptix;
pub mod gcc_nada_estimator;

// pub type OptLazy<T> = Lazy<Mutex<Option<T>>>;
// pub const fn lazy_mut_none<T>() -> OptLazy<T> {
//     Lazy::new(|| Mutex::new(None))
// }

pub const DEBUG_PRINT_ENABLED: bool = false; // Change to false to disable
pub const USE_FFMPEG_DEMO: bool = false;

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
macro_rules! db_debug_bgprint {
    ($color:expr, $fmt:expr, $($arg:tt)*) => {
        // if DEBUG_PRINT_ENABLED == true {
            let msg = format!($fmt, $($arg)*);
            println!("{}", $color.to_background_fn()(msg));
        }
    // };
}

#[macro_export]
macro_rules! print_pretty {
    ($color:expr, $fmt:expr, $($arg:tt)*) => {
        if DEBUG_PRINT_ENABLED == true {
            let msg = format!($fmt, $($arg)*);
            println!("{}", $color.to_color_fn()(msg));
        }
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
        if DEBUG_PRINT_ENABLED == true {
            let msg = format!($fmt, $($arg)*);
            println!("{}", $color.to_background_fn()(msg));
        }
    };
}

#[macro_export]
macro_rules! print_prettyyy {
    ($color:expr, $fmt:expr, $($arg:tt)*) => {
        if DEBUG_PRINT_ENABLED == true {
            let msg = format!($fmt, $($arg)*);
            println!("{}", $color.to_background_fn()(msg));
        };
    }; // };
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
macro_rules! debug_debug {
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
        let msg = format!($fmt, $($arg)*);
        println!("{}", DebugColor::Yellow.to_color_fn()(msg));
    };
}
#[macro_export]
macro_rules! print_green {
    ($fmt:expr, $($arg:tt)*) => {
        let msg = format!($fmt, $($arg)*);
        println!("{}", DebugColor::ForestGreen.to_background_fn()(msg));
    };
}
#[macro_export]
macro_rules! print_blue {
    ($fmt:expr, $($arg:tt)*) => {
        let msg = format!($fmt, $($arg)*);
        println!("{}", DebugColor::Blue.to_background_fn()(msg));
    };
}

#[macro_export]
macro_rules! print_dblue {
    ($fmt:expr, $($arg:tt)*) => {
        let msg = format!($fmt, $($arg)*);
        println!("{}", DebugColor::DarkBlue.to_background_fn()(msg));
    };
}
#[macro_export]
macro_rules! print_pink {
    ($fmt:expr, $($arg:tt)*) => {
        let msg = format!($fmt, $($arg)*);
        println!("{}", DebugColor::Rose.to_background_fn()(msg));
    };
}

#[macro_export]
macro_rules! print_magenta {
    ($fmt:expr, $($arg:tt)*) => {
        let msg = format!($fmt, $($arg)*);
        println!("{}", DebugColor::Magenta.to_background_fn()(msg));
    };
}

#[macro_export]
macro_rules! print_brown {
    ($fmt:expr, $($arg:tt)*) => {
        let msg = format!($fmt, $($arg)*);
        println!("{}", DebugColor::SaddleBrown.to_background_fn()(msg));
    };
}

use std::env;

pub fn get_prefix_path(directory: &str) -> String {
    let home = env::var("HOME").unwrap_or_default();

    if home.contains("fmaura") {
        // HPC user
        format!("{}/simulator_asynchronix/asynchronix/{}", home, directory)
    } else if home.contains("boris") {
        // Local Ubuntu user
        format!("{}/Desktop/Rust_MG1/asynchronix/{}", home, directory)
    } else {
        format!(
            "/gpfs/home/fmaura/simulator_asynchronix/asynchronix/{}",
            directory
        ) // still in HPC, absolute path
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
    pub fn clear(&mut self) {
        self.history_buffer.clear();
        self.interval_buffer.clear();
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

fn get_4_octet(ip: IpAddr) -> u8 {
    match ip {
        IpAddr::V4(v4) => v4.octets()[2],
        IpAddr::V6(_) => 0,
    }
} // get the 4th octet of IpAddr (for tagging CSVs)

#[allow(unused)]
#[allow(unused)]
fn draw_v_line(
    buffer: &mut [u32],
    x: usize,
    y_start: usize,
    y_end: usize,
    stride: usize,
    color: u32,
) {
    for y in y_start..y_end {
        if y * stride + x < buffer.len() {
            buffer[y * stride + x] = color;
        }
    }
}

#[allow(unused)]
fn draw_h_line(
    buffer: &mut [u32],
    y: usize,
    x_start: usize,
    x_end: usize,
    stride: usize,
    color: u32,
) {
    let start = y * stride + x_start;
    let end = y * stride + x_end;
    for idx in start..end {
        if idx < buffer.len() {
            buffer[idx] = color;
        }
    }
}
#[allow(unused)]
pub fn render_kv_cell(
    buffer: &mut [u32],
    label: &str,
    value: &str,
    x: usize,
    y: usize,
    stride: usize,
    color: u32,
    scale: usize,
    total_width_px: usize,
    separator_offset: Option<usize>, // New: Option to draw a vertical bar
) {
    const CHAR_BASE_W: usize = 6;
    const FONT_HEIGHT: usize = 7;
    let char_width = CHAR_BASE_W * scale;

    // 1. Render Label
    render_text(buffer, label, x, y, stride, color, scale);

    // 2. Render Separator Bar
    if let Some(offset) = separator_offset {
        let bar_x = x + offset;
        draw_v_line(buffer, bar_x, y, y + (FONT_HEIGHT * scale), stride, color);
    }

    // 3. Render Value (Right-aligned)
    let value_len_px = value.chars().count() * char_width;
    let value_x = if value_len_px < total_width_px {
        x + (total_width_px - value_len_px)
    } else {
        x + (label.chars().count() * char_width) + char_width
    };

    render_text(buffer, value, value_x, y, stride, color, scale);
}

#[macro_export]
macro_rules! render_hud_grid {
    (
        $buffer:expr, $stride:expr, $x:expr, $y:expr, $color:expr, $scale:expr, $width:expr, $spacing:expr,
        $draw_box:expr,           // New: Bool for border
        $separator_pos:expr,      // New: Option<usize> for bar position
        [ $( ($label:expr, $value:expr) ),* ]
    ) => {
        {
            let mut current_y = $y;
            let items_count = [ $( $label ),* ].len();
            let padding = 10;

            // Draw Box Around Area
            if $draw_box {
                let box_h = items_count * $spacing + padding;
                let x_start = $x.saturating_sub(padding);
                let x_end = $x + $width + padding;
                let y_start = $y.saturating_sub(padding);
                let y_end = $y + box_h;

                crate::lib::draw_h_line($buffer, y_start, x_start, x_end, $stride, $color); // Top
                crate::lib::draw_h_line($buffer, y_end, x_start, x_end, $stride, $color);   // Bottom
                crate::lib::draw_v_line($buffer, x_start, y_start, y_end, $stride, $color); // Left
                crate::lib::draw_v_line($buffer, x_end, y_start, y_end + 1, $stride, $color); // Right
            }

            $(
                crate::lib::render_kv_cell(
                    $buffer,
                    $label,
                    &format!("{}", $value),
                    $x,
                    current_y,
                    $stride,
                    $color,
                    $scale,
                    $width,
                    $separator_pos
                );
                current_y += $spacing;
            )*
        }
    };
}
// Renders ASCII text into the minifb window with coordinates.
pub fn render_text(
    buffer: &mut [u32],
    text: &str,
    x: usize,
    y: usize,
    stride: usize,
    color: u32,
    scale: usize,
) {
    const FONT_WIDTH: usize = 5;
    const FONT_HEIGHT: usize = 7;
    const CHAR_SPACING: usize = 1;

    let scaled_font_width = FONT_WIDTH * scale;
    let scaled_char_spacing = CHAR_SPACING * scale;

    // Extended font with lowercase letters
    let font = [
        // Space (0)
        [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        // ! (1)
        [0x04, 0x04, 0x04, 0x04, 0x00, 0x04, 0x00],
        // " (2)
        [0x0A, 0x0A, 0x00, 0x00, 0x00, 0x00, 0x00],
        // # (3)
        [0x0A, 0x0A, 0x1F, 0x0A, 0x1F, 0x0A, 0x0A],
        // $ (4)
        [0x04, 0x0F, 0x14, 0x0E, 0x05, 0x1E, 0x04],
        // % (5)
        [0x18, 0x19, 0x02, 0x04, 0x08, 0x13, 0x03],
        // & (6)
        [0x0C, 0x12, 0x14, 0x08, 0x15, 0x12, 0x0D],
        // ' (7)
        [0x0C, 0x04, 0x08, 0x00, 0x00, 0x00, 0x00],
        // ( (8)
        [0x02, 0x04, 0x08, 0x08, 0x08, 0x04, 0x02],
        // ) (9)
        [0x08, 0x04, 0x02, 0x02, 0x02, 0x04, 0x08],
        // * (10)
        [0x00, 0x04, 0x15, 0x0E, 0x15, 0x04, 0x00],
        // + (11)
        [0x00, 0x04, 0x04, 0x1F, 0x04, 0x04, 0x00],
        // , (12)
        [0x00, 0x00, 0x00, 0x00, 0x0C, 0x04, 0x08],
        // - (13)
        [0x00, 0x00, 0x00, 0x1F, 0x00, 0x00, 0x00],
        // . (14)
        [0x00, 0x00, 0x00, 0x00, 0x00, 0x0C, 0x0C],
        // / (15)
        [0x00, 0x01, 0x02, 0x04, 0x08, 0x10, 0x00],
        // 0-9 (16-25)
        [0x0E, 0x11, 0x13, 0x15, 0x19, 0x11, 0x0E],
        [0x04, 0x0C, 0x04, 0x04, 0x04, 0x04, 0x0E],
        [0x0E, 0x11, 0x01, 0x02, 0x04, 0x08, 0x1F],
        [0x1F, 0x02, 0x04, 0x02, 0x01, 0x11, 0x0E],
        [0x02, 0x06, 0x0A, 0x12, 0x1F, 0x02, 0x02],
        [0x1F, 0x10, 0x1E, 0x01, 0x01, 0x11, 0x0E],
        [0x06, 0x08, 0x10, 0x1E, 0x11, 0x11, 0x0E],
        [0x1F, 0x01, 0x02, 0x04, 0x08, 0x08, 0x08],
        [0x0E, 0x11, 0x11, 0x0E, 0x11, 0x11, 0x0E],
        [0x0E, 0x11, 0x11, 0x0F, 0x01, 0x02, 0x0C],
        // : ; < = > ? @ (26-32)
        [0x00, 0x0C, 0x0C, 0x00, 0x0C, 0x0C, 0x00],
        [0x00, 0x0C, 0x0C, 0x00, 0x0C, 0x04, 0x08],
        [0x02, 0x04, 0x08, 0x10, 0x08, 0x04, 0x02],
        [0x00, 0x00, 0x1F, 0x00, 0x1F, 0x00, 0x00],
        [0x08, 0x04, 0x02, 0x01, 0x02, 0x04, 0x08],
        [0x0E, 0x11, 0x01, 0x02, 0x04, 0x00, 0x04],
        [0x0E, 0x11, 0x01, 0x0D, 0x15, 0x15, 0x0E],
        // A-Z (33-58)
        [0x0E, 0x11, 0x11, 0x11, 0x1F, 0x11, 0x11],
        [0x1E, 0x11, 0x11, 0x1E, 0x11, 0x11, 0x1E],
        [0x0E, 0x11, 0x10, 0x10, 0x10, 0x11, 0x0E],
        [0x1C, 0x12, 0x11, 0x11, 0x11, 0x12, 0x1C],
        [0x1F, 0x10, 0x10, 0x1E, 0x10, 0x10, 0x1F],
        [0x1F, 0x10, 0x10, 0x1E, 0x10, 0x10, 0x10],
        [0x0E, 0x11, 0x10, 0x17, 0x11, 0x11, 0x0F],
        [0x11, 0x11, 0x11, 0x1F, 0x11, 0x11, 0x11],
        [0x0E, 0x04, 0x04, 0x04, 0x04, 0x04, 0x0E],
        [0x07, 0x02, 0x02, 0x02, 0x02, 0x12, 0x0C],
        [0x11, 0x12, 0x14, 0x18, 0x14, 0x12, 0x11],
        [0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x1F],
        [0x11, 0x1B, 0x15, 0x15, 0x11, 0x11, 0x11],
        [0x11, 0x11, 0x19, 0x15, 0x13, 0x11, 0x11],
        [0x0E, 0x11, 0x11, 0x11, 0x11, 0x11, 0x0E],
        [0x1E, 0x11, 0x11, 0x1E, 0x10, 0x10, 0x10],
        [0x0E, 0x11, 0x11, 0x11, 0x15, 0x12, 0x0D],
        [0x1E, 0x11, 0x11, 0x1E, 0x14, 0x12, 0x11],
        [0x0F, 0x10, 0x10, 0x0E, 0x01, 0x01, 0x1E],
        [0x1F, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04],
        [0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x0E],
        [0x11, 0x11, 0x11, 0x11, 0x11, 0x0A, 0x04],
        [0x11, 0x11, 0x11, 0x15, 0x15, 0x15, 0x0A],
        [0x11, 0x11, 0x0A, 0x04, 0x0A, 0x11, 0x11],
        [0x11, 0x11, 0x11, 0x0A, 0x04, 0x04, 0x04],
        [0x1F, 0x01, 0x02, 0x04, 0x08, 0x10, 0x1F],
        // [ \ ] ^ _ (59-63)
        [0x0E, 0x08, 0x08, 0x08, 0x08, 0x08, 0x0E],
        [0x00, 0x10, 0x08, 0x04, 0x02, 0x01, 0x00],
        [0x0E, 0x02, 0x02, 0x02, 0x02, 0x02, 0x0E],
        [0x04, 0x0A, 0x11, 0x00, 0x00, 0x00, 0x00],
        [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1F],
        // a-z (64-89) - lowercase letters
        [0x00, 0x00, 0x0E, 0x01, 0x0F, 0x11, 0x0F], // a
        [0x10, 0x10, 0x16, 0x19, 0x11, 0x11, 0x1E], // b
        [0x00, 0x00, 0x0E, 0x10, 0x10, 0x11, 0x0E], // c
        [0x01, 0x01, 0x0D, 0x13, 0x11, 0x11, 0x0F], // d
        [0x00, 0x00, 0x0E, 0x11, 0x1F, 0x10, 0x0E], // e
        [0x06, 0x09, 0x08, 0x1C, 0x08, 0x08, 0x08], // f
        [0x00, 0x0F, 0x11, 0x11, 0x0F, 0x01, 0x0E], // g
        [0x10, 0x10, 0x16, 0x19, 0x11, 0x11, 0x11], // h
        [0x04, 0x00, 0x0C, 0x04, 0x04, 0x04, 0x0E], // i
        [0x02, 0x00, 0x06, 0x02, 0x02, 0x12, 0x0C], // j
        [0x10, 0x10, 0x12, 0x14, 0x18, 0x14, 0x12], // k
        [0x0C, 0x04, 0x04, 0x04, 0x04, 0x04, 0x0E], // l
        [0x00, 0x00, 0x1A, 0x15, 0x15, 0x11, 0x11], // m
        [0x00, 0x00, 0x16, 0x19, 0x11, 0x11, 0x11], // n
        [0x00, 0x00, 0x0E, 0x11, 0x11, 0x11, 0x0E], // o
        [0x00, 0x00, 0x1E, 0x11, 0x1E, 0x10, 0x10], // p
        [0x00, 0x00, 0x0D, 0x13, 0x0F, 0x01, 0x01], // q
        [0x00, 0x00, 0x16, 0x19, 0x10, 0x10, 0x10], // r
        [0x00, 0x00, 0x0E, 0x10, 0x0E, 0x01, 0x1E], // s
        [0x08, 0x08, 0x1C, 0x08, 0x08, 0x09, 0x06], // t
        [0x00, 0x00, 0x11, 0x11, 0x11, 0x13, 0x0D], // u
        [0x00, 0x00, 0x11, 0x11, 0x11, 0x0A, 0x04], // v
        [0x00, 0x00, 0x11, 0x11, 0x15, 0x15, 0x0A], // w
        [0x00, 0x00, 0x11, 0x0A, 0x04, 0x0A, 0x11], // x
        [0x00, 0x00, 0x11, 0x11, 0x0F, 0x01, 0x0E], // y
        [0x00, 0x00, 0x1F, 0x02, 0x04, 0x08, 0x1F], // z
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
            '[' => 59,
            '\\' => 60,
            ']' => 61,
            '^' => 62,
            '_' => 63,
            'a'..='z' => (c as usize) - ('a' as usize) + 64, // Now maps to lowercase glyphs
            _ => 0,
        };

        // Draw the character with scaling
        for row in 0..FONT_HEIGHT {
            for scaled_row in 0..scale {
                let buffer_y = y + (row * scale) + scaled_row;

                for col in 0..FONT_WIDTH {
                    if (font[index][row] & (1 << (FONT_WIDTH - 1 - col))) != 0 {
                        for scaled_col in 0..scale {
                            let buffer_x = char_x + (col * scale) + scaled_col;

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

        char_x += scaled_font_width + scaled_char_spacing;
    }
}



#[derive(PartialEq)]
pub enum GraphType {
    Bar,
    Line,
}

// Bresenham's line algorithm for raw pixel buffers
fn draw_thick_line(
    buffer: &mut [u32],
    stride: usize,
    x0: usize,
    y0: usize,
    x1: usize,
    y1: usize,
    color: u32,
    thickness: isize,
) {
    let dx = (x1 as isize - x0 as isize).abs();
    let dy = -(y1 as isize - y0 as isize).abs();
    let mut err = dx + dy;
    let mut x = x0 as isize;
    let mut y = y0 as isize;
    let sx = if x0 < x1 { 1 } else { -1 };
    let sy = if y0 < y1 { 1 } else { -1 };

    loop {
        // Apply thickness by drawing adjacent pixels
        for ty in 0..thickness {
            for tx in 0..thickness {
                let px = x + tx;
                let py = y + ty;
                if px >= 0 && py >= 0 {
                    let pu = px as usize;
                    let pv = py as usize;
                    if pv < buffer.len() / stride && pu < stride {
                        buffer[pv * stride + pu] = color;
                    }
                }
            }
        }

        if x == x1 as isize && y == y1 as isize { break; }
        let e2 = 2 * err;
        if e2 >= dy { err += dy; x += sx; }
        if e2 <= dx { err += dx; y += sy; }
    }
}

fn render_loading_spinner(buffer: &mut [u32], width: usize, height: usize, time: f32) {
    let center_x = width as f32 / 2.0;
    let center_y = height as f32 / 2.0;
    let radius = 80.0;
    let dot_count: i32 = 12;
    let dot_radius = 8.0;

    // Clear the video area to a dark background
    for pixel in buffer.iter_mut().take(width * height) {
        *pixel = 0x050505;
    }

    for i in 0..dot_count {
        // Angle for this specific dot
        let angle = (i as f32 / dot_count as f32) * std::f32::consts::TAU;

        // Calculate position
        let x = (center_x + angle.cos() * radius) as usize;
        let y = (center_y + angle.sin() * radius) as usize;

        // Animation: intensity varies based on time and the dot's index
        // This creates the "chase" effect
        let intensity_factor = ((time * 5.0 - (i as f32 * 0.5)).sin() + 1.0) / 2.0;
        let brightness = (intensity_factor * 255.0) as u32;
        let color = (brightness << 16) | (brightness << 8) | brightness;

        // Draw a small 3x3 square for each "dot"
        let r_int = dot_radius as isize;
        for dy in -r_int..r_int {
            for dx in -r_int..r_int {
                // Distance check: x^2 + y^2 <= r^2
                if (dx * dx + dy * dy) as f32 <= dot_radius * dot_radius {
                    let px = (x as isize + dx) as usize;
                    let py = (y as isize + dy) as usize;

                    if px < width && py < height {
                        buffer[py * width + px] = color;
                    }
                }
            }
        }
    }
}

pub fn render_trajectory_graph(
    buffer: &mut [u32],
    stride: usize,
    trajectory: &VecDeque<glam::Vec3>, // <-- FIX 1: Accept Vec3 instead of (f32, f32)
    x_offset: usize,
    y_offset: usize,
    cell_size: usize,
    fps: f32,
    world_scale: f32,
) {
    if trajectory.is_empty() { return; }

    let cols = 24;
    let rows = 12;
    let graph_w = cols * cell_size;
    let graph_h = rows * cell_size;

    // 1. Draw Background Plate & 24x12 Grid
    for y in 0..graph_h {
        let py = y_offset + y;
        if py >= buffer.len() / stride { continue; }

        for x in 0..graph_w {
            let px = x_offset + x;
            if px >= stride { continue; }

            let idx = py * stride + px;
            
            // Draw Center Crosshairs brighter, standard grid lines darker
            let is_center_x = x == (cols / 2) * cell_size;
            let is_center_y = y == (rows / 2) * cell_size;
            let is_grid_x = x % cell_size == 0;
            let is_grid_y = y % cell_size == 0;

            if is_center_x || is_center_y {
                buffer[idx] = 0x888888; // Bright Grey axes
            } else if is_grid_x || is_grid_y {
                buffer[idx] = 0x333333; // Dark Grey grid
            } else {
                // Dim the background
                let current = buffer[idx];
                let r = ((current >> 16) & 0xFF) / 4;
                let g = ((current >> 8) & 0xFF) / 4;
                let b = (current & 0xFF) / 4;
                buffer[idx] = (r << 16) | (g << 8) | b;
            }
        }
    }

    // Border
    for x in 0..graph_w {
        if x_offset + x < stride {
            if y_offset < buffer.len() / stride { buffer[y_offset * stride + x_offset + x] = 0xAAAAAA; }
            if y_offset + graph_h < buffer.len() / stride { buffer[(y_offset + graph_h) * stride + x_offset + x] = 0xAAAAAA; }
        }
    }

    // Helper to map World Space (0,0 at center) to Pixel Space
    let get_pixel_coords = |world_x: f32, world_y: f32| -> (usize, usize) {
        // Shift the world coordinates so AP is at the center (0,0)
        let relative_x = world_x - AP_X as f32;
        let relative_y = world_y - AP_Y as f32;

        let px_offset = (relative_x / world_scale) * cell_size as f32;
        // Assuming your standard 2D top-down view maps Y-up to pixel-down (subtraction)
        let py_offset = (relative_y / world_scale) * cell_size as f32; 

        let center_px = (cols / 2) * cell_size;
        let center_py = (rows / 2) * cell_size;

        let final_x = (center_px as f32 + px_offset).clamp(0.0, graph_w as f32 - 1.0) as usize;
        let final_y = (center_py as f32 - py_offset).clamp(0.0, graph_h as f32 - 1.0) as usize; 

        (x_offset + final_x, y_offset + final_y)
    };

    // 2. Draw Trajectory Lines
    let mut prev_point: Option<(usize, usize)> = None;
    let window_size = fps.round() as usize;
    let total_points = trajectory.len();

    for (i, pos) in trajectory.iter().enumerate() {

        let (px, py) = get_pixel_coords(pos.x, pos.y); 

        if let Some((prev_x, prev_y)) = prev_point {
            let color = if i >= total_points.saturating_sub(window_size) {
                0xFF3333 // Bright Red for recent frames
            } else {
                0xDDDDDD // Off-White for older history
            };

            draw_thick_line(buffer, stride, prev_x, prev_y, px, py, color, 3);
        }
        prev_point = Some((px, py));
    }

    if let Some(current_pos) = trajectory.back() {
        let (px, py) = get_pixel_coords(current_pos.x, current_pos.y);
        let dot_radius = 4; 
        let dot_color = 0xFF0000; 

        // Draw the red dot
        for dy in 0..=(dot_radius * 2) {
            let cy = py + dy;
            let final_y = cy.saturating_sub(dot_radius);
            
            if final_y >= buffer.len() / stride { continue; }

            for dx in 0..=(dot_radius * 2) {
                let cx = px + dx;
                let final_x = cx.saturating_sub(dot_radius);
                
                if final_x < stride {
                    buffer[final_y * stride + final_x] = dot_color;
                }
            }
        }

        // NEW: Draw coordinates text next to the red dot
        let coord_str = format!("({:.1}, {:.1})", current_pos.x, current_pos.y);
        
        // Offset the text by 8 pixels right and 8 pixels up so it doesn't overlap the dot
        let text_x = (px + 8).min(stride.saturating_sub(1));
        let text_y = py.saturating_sub(8);
        
        render_text(buffer, &coord_str, text_x, text_y, stride, 0xFF0000, 1);
    }
    // DRAW THE AP // 
    let center_x_px = x_offset + (cols / 2) * cell_size;
    let center_y_px = y_offset + (rows / 2) * cell_size;
    let ap_dot_radius = 3;
    let ap_color = 0xFFFFFF; // White

    // Draw the white square/dot
    for dy in 0..=(ap_dot_radius * 2) {
        let cy = center_y_px + dy;
        let final_y = cy.saturating_sub(ap_dot_radius);
        
        if final_y >= buffer.len() / stride { continue; }

        for dx in 0..=(ap_dot_radius * 2) {
            let cx = center_x_px + dx;
            let final_x = cx.saturating_sub(ap_dot_radius);
            
            if final_x < stride {
                buffer[final_y * stride + final_x] = ap_color;
            }
        }
    }

    // Render "AP" text slightly offset from the dot so they don't overlap
    render_text(
        buffer, 
        "AP", 
        center_x_px + 6, 
        center_y_px.saturating_sub(12), 
        stride, 
        0xFFFFFF, 
        1 // Using scale 1 so it doesn't overpower the graph
    );


    // Render Title
    render_text(buffer, "Client trajectory (X,Y)", x_offset, y_offset.saturating_sub(25), stride, 0xFFFFFF, 2);


    // 4. Draw 5-meter markers on the axes
    // Calculate the maximum visible distance in meters from the center AP
    let max_x_meters = ((cols / 2) as f32 * world_scale).ceil() as i32;
    let max_y_meters = ((rows / 2) as f32 * world_scale).ceil() as i32;

    let marker_color = 0xAAAAAA; // Light grey for the text and ticks

    // Draw X-axis markers (Horizontal axis, Y = AP_Y)
    for m in (-max_x_meters..=max_x_meters).step_by(5) {
        if m == 0 { continue; } // Skip 0 since the AP label is already there
        
        let world_x = AP_X as f32 + m as f32;
        let (px, py) = get_pixel_coords(world_x, AP_Y as f32);
        
        // Draw a small vertical tick mark
        for dy in 0..=4 {
            let tick_y = (py + dy).saturating_sub(2);
            if tick_y < buffer.len() / stride && px < stride {
                buffer[tick_y * stride + px] = marker_color;
            }
        }

        // Render the text slightly below the tick mark
        let label = format!("{}m", m);
        render_text(buffer, &label, px.saturating_sub(4), py + 6, stride, marker_color, 1);
    }

    // Draw Y-axis markers (Vertical axis, X = AP_X)
    for m in (-max_y_meters..=max_y_meters).step_by(5) {
        if m == 0 { continue; } 
        
        let world_y = AP_Y as f32 + m as f32;
        let (px, py) = get_pixel_coords(AP_X as f32, world_y);
        
        // Draw a small horizontal tick mark
        for dx in 0..=4 {
            let tick_x = (px + dx).saturating_sub(2);
            if tick_x < stride && py < buffer.len() / stride {
                buffer[py * stride + tick_x] = marker_color;
            }
        }

        // Render the text slightly to the right of the tick mark
        let label = format!("{}m", m);
        render_text(buffer, &label, px + 6, py.saturating_sub(4), stride, marker_color, 1);
    }
}



pub fn render_stat_graph(
    buffer: &mut [u32],
    history: &VecDeque<f32>,
    x_offset: usize,
    y_offset: usize,
    stride: usize,
    graph_height: usize,
    y_range_override: Option<(f32, f32)>, 
    title: &str,
    unit: &str,
    graph_type: GraphType,
    color_orig: u32,
    info_update: Option<PerfectInfoBitrateMessage>, 
    fps: f32, 

) {
    if history.is_empty() { return; }

    let bar_width = 2;
    let spacing = 1;
    
    // --- FIX: Cap the graph width so it doesn't exceed the window (stride) ---
    // Leave a 20px padding on the right side of the screen
    let max_allowed_width = stride.saturating_sub(x_offset + 20); 
    let max_items = max_allowed_width / (bar_width + spacing);
    
    // Determine how many items we can actually draw
    let items_to_draw = std::cmp::min(history.len(), max_items);
    let graph_width = items_to_draw * (bar_width + spacing);
    // -------------------------------------------------------------------------

    let title_margin = 40;
    let label_margin = 75;

    // 1. Draw Background Plate
    let bg_y_start = y_offset.saturating_sub(title_margin + 10);
    let bg_y_end = y_offset + graph_height + 30; 
    let bg_x_start = x_offset.saturating_sub(label_margin + 20);
    // Ensure the background plate also respects the window bounds
    let bg_x_end = std::cmp::min(x_offset + graph_width + 50, stride); 

    for y in bg_y_start..bg_y_end {
        if y >= buffer.len() / stride { continue; }
        for x in bg_x_start..bg_x_end {
            if x >= stride { continue; }
            let idx = y * stride + x;
            let current = buffer[idx];
            let r = ((current >> 16) & 0xFF) / 2;
            let g = ((current >> 8) & 0xFF) / 2;
            let b = (current & 0xFF) / 2;
            buffer[idx] = (r << 16) | (g << 8) | b;
        }
    }

    // 2. Determine Min/Max Range for Y-Axis
    let (min_val, max_val) = y_range_override.unwrap_or_else(|| {
        let max = history.iter().copied().fold(0.0f32, f32::max);
        let min = history.iter().copied().fold(0.0f32, f32::min);
        let calculated_max = if max < 1.0 { 1.0 } else { max };
        let calculated_min = if min > 0.0 { 0.0 } else { min };
        (calculated_min, calculated_max)
    });
    
    // Prevent division by zero if min == max
    let range = if (max_val - min_val).abs() < f32::EPSILON { 1.0 } else { max_val - min_val };

    // Title & Unit
    render_text(buffer, title, x_offset, y_offset.saturating_sub(title_margin), stride, 0xFFFFFF, 2);
    render_text(buffer, unit, x_offset.saturating_sub(label_margin), y_offset.saturating_sub(title_margin), stride, 0xCCCCCC, 2);

    // 3. Draw Grid & Y-Axis Labels
    for i in 0..=4 {
        let visual_pct = i as f32 * 0.25;
        let marker_y = y_offset + graph_height - (visual_pct * graph_height as f32) as usize;
        let label_val = min_val + (range * visual_pct);

        if marker_y < buffer.len() / stride {
            for px in x_offset..(x_offset + graph_width) {
                if px < stride { buffer[marker_y * stride + px] = 0x555555; }
            }
        }
        
        let label_str = if title == "FLR"{
            format!("{:.2}", label_val)
        }
        else{
            format!("{:.1}", label_val)
        }; 

        // --- FIX: Changed the final argument from 2 to 1 to reduce Y-axis text scale ---
        render_text(buffer, &label_str, x_offset.saturating_sub(label_margin), marker_y.saturating_sub(8), stride, 0xCCCCCC, 1);
        // -------------------------------------------------------------------------------
    }

    // 4. Render Data
    let get_y = |val: f32| -> usize {
        let norm = ((val - min_val) / range).clamp(0.0, 1.0);
        y_offset + graph_height - (norm * graph_height as f32) as usize
    };

    let zero_y = get_y(0.0f32.clamp(min_val, max_val));

    let zero_y = get_y(0.0f32.clamp(min_val, max_val));

        match graph_type {
            GraphType::Bar => {
                // 4a. Draw the bars first
                for (i, &val) in history.iter().take(items_to_draw).enumerate() {
                    
                    let bar_color = if title == "FLR" && val <= 0.0001 {
                        0x222244 
                    } else {
                        color_orig
                    };

                    let py_val = get_y(val);
                    
                    let (py_top, py_bottom) = if py_val < zero_y {
                        (py_val, zero_y) 
                    } else {
                        (zero_y, py_val) 
                    };

                    let bar_h = py_bottom.saturating_sub(py_top);
                    
                    for bh in 0..=bar_h {
                        let py = py_bottom.saturating_sub(bh);
                        if py >= buffer.len() / stride { continue; }
                        for bw in 0..bar_width {
                            let px = x_offset + (i * (bar_width + spacing)) + bw;
                            if px < stride { 
                                buffer[py * stride + px] = bar_color; 
                            }
                        }
                    }
                }

                // --- NEW: 4b. Draw Dynamic Target Line (If info is provided) ---
                if let Some(info) = &info_update {
                    
                    let target_val: f32 = match title {
                        "Frame size" => {
                            // 1. Convert Mbps to total bits per second
                            let target_bps = info.bitrate_mbps * 1_000_000.0; 
                            
                            // 2. Divide by (8 * fps) to get target BYTES per frame.
                            // (Because your size_history array is currently in raw bytes)
                            target_bps / (8.0 * fps)
                        },
                        "FLR" => 0.05, // Put your actual FLR target here
                        _ => 0.0,      // Default fallback
                    };

                    let target_y_base = get_y(target_val);
                    let line_thickness = 3;
                    let col_tgt = 0xFF00FF; // Flashy Magenta

                    // Only draw if the target is vertically on-screen
                    if target_y_base < buffer.len() / stride {
                        
                        // Draw the horizontal line
                        for ty in 0..line_thickness {
                            let target_y = target_y_base.saturating_sub(ty);
                            if target_y >= buffer.len() / stride { continue; }

                            for px in x_offset..(x_offset + graph_width) {
                                if px < stride {
                                    // Overwrite pixels to put the line "on top" of the bars
                                    buffer[target_y * stride + px] = col_tgt;
                                }
                            }
                        }

                        // Draw the label to the right, safely bounded by stride
                        let text_x = std::cmp::min(x_offset + graph_width + 10, stride.saturating_sub(60));
                        render_text(
                            buffer,
                            // Adjust the text to show the correct units (Bytes)
                            &format!("TGT: {:.0} B", target_val), 
                            text_x,
                            target_y_base.saturating_sub(8),
                            stride,
                            col_tgt,
                            1,
                        );
                    }
                }
                // ---------------------------------------------------------------
            }
        GraphType::Line => {
            let mut prev_point: Option<(usize, usize)> = None;
            // --- FIX: Add `.take(items_to_draw)` to prevent rendering off-screen ---
            for (i, &val) in history.iter().take(items_to_draw).enumerate() {
                let px = x_offset + (i * (bar_width + spacing)) + (bar_width / 2);
                let py = get_y(val);

                if let Some((prev_x, prev_y)) = prev_point {
                    draw_thick_line(buffer, stride, prev_x, prev_y, px, py, color_orig, 2); 
                }
                prev_point = Some((px, py));
                
                if py < buffer.len() / stride && px < stride {
                    buffer[py * stride + px] = 0xFFFFFF; 
                }
            }
        }
    }

    // 5. Draw X-Axis Ticks (Multiples of FPS and Max)
    let fps_u = fps.round() as usize;
    let axis_y = y_offset + graph_height;
    
    if fps_u > 0 && items_to_draw > 0 {
        // We use a separate loop to ensure ticks are drawn ON TOP of any lines/bars
        for i in 0..items_to_draw {
            // Check if it's a multiple of the framerate OR the very last item drawn
            if i > 0 && (i % fps_u == 0 || i == items_to_draw - 1) {
                let px = x_offset + (i * (bar_width + spacing)) + (bar_width / 2);
                
                // Draw a 5-pixel tall downward tick line
                for ty in 0..5 {
                    let tick_y = axis_y + ty;
                    if tick_y < buffer.len() / stride && px < stride {
                        buffer[tick_y * stride + px] = 0xAAAAAA; // Light grey tick
                    }
                }
                
                // Render the frame number label
                let label = format!("{}", i);
                // Roughly center the text under the tick (assuming ~6px width per char at scale 1)
                let text_x = px.saturating_sub(label.len() * 3); 
                
                render_text(
                    buffer,
                    &label,
                    text_x,
                    axis_y + 8, // Place text below the tick
                    stride,
                    0xAAAAAA,
                    1, // Small text size
                );
            }
        }
    }


}
pub fn render_graph(
    buffer: &mut [u32],
    history: &VecDeque<f32>,
    x_offset: usize,
    y_offset: usize,
    stride: usize,
    perfect_info_msg: PerfectInfoBitrateMessage, 
    fps: f32,
    graph_height: usize,
    max_size_kb: f32,
    use_log: bool,

) {
    // 1. MADE WIDER: Increased width and spacing
    let bar_width = 7;
    let spacing = 2;
    let graph_width = history.len() * (bar_width + spacing);

    // --- CONFIGURATION ---
    // 2. INCREASED FONT SIZE: Changed from 2 to 3
    const DISPLAY_GRAPH_SCALE_TEXT: usize = 3;

    const COL_RED: u32 = 0xEE6666;
    const COL_YELLOW: u32 = 0xF0E68C;
    const COL_GREEN: u32 = 0x8FBC8F;
    const COL_GRID: u32 = 0x555555;
    const COL_TEXT: u32 = 0xCCCCCC;
    // const COL_TGT: u32 = 0x87CEFA;
    const COL_TGT: u32 = 0xFF00FF; // Flashy Magenta

    // Layout Offsets
    // Increased margins to handle larger text size
    let label_margin = 75;
    let title_margin = 55;

    // --- 0. DRAW BACKGROUND PLATE ---
    let bg_y_start = y_offset.saturating_sub(title_margin + 10);
    // Extended bottom margin significantly to fit the "0" and bottom padding
    let bg_y_end = y_offset + graph_height + 40;
    let bg_x_start = x_offset.saturating_sub(label_margin + 20);
    let bg_x_end = x_offset + graph_width + 220;

    for y in bg_y_start..bg_y_end {
        if y >= buffer.len() / stride {
            continue;
        }
        for x in bg_x_start..bg_x_end {
            if x >= stride {
                continue;
            }

            let pixel_idx = y * stride + x;
            let current_pixel = buffer[pixel_idx];

            // Dimming logic
            let r = ((current_pixel >> 16) & 0xFF) / 2;
            let g = ((current_pixel >> 8) & 0xFF) / 2;
            let b = (current_pixel & 0xFF) / 2;
            buffer[pixel_idx] = (r << 16) | (g << 8) | b;
        }
    }

    // --- LOG SCALE HELPERS ---
    let min_log_kb = 1.0f32;
    let log_min = min_log_kb.ln();
    let log_max = max_size_kb.max(min_log_kb + 0.1).ln();
    let log_range = log_max - log_min;

    let get_normalized_height = |kb_val: f32| -> f32 {
        if use_log {
            if kb_val < min_log_kb {
                0.0
            } else {
                ((kb_val.ln() - log_min) / log_range).min(1.0).max(0.0)
            }
        } else {
            (kb_val / max_size_kb).min(1.0).max(0.0)
        }
    };

    let target_bps = perfect_info_msg.bitrate_mbps * 1_000_000.0; 

    let current_target_kb = (target_bps / (8.0 * fps)) / 1024.0;

    // --- 1. TITLE & INFO ---
    render_text(
        buffer,
        &format!(" Frame size ({} window) in kBytes", history.len()),
        x_offset,
        y_offset.saturating_sub(title_margin),
        stride,
        0xFFFFFF,
        3, // Increased Title Size
    );

    render_text(
        buffer,
        "[kB]",
        x_offset.saturating_sub(label_margin),
        y_offset.saturating_sub(title_margin),
        stride,
        COL_TEXT,
        3, // Keep unit small
    );

    // --- 2. DRAW Y-AXIS MARKERS & GRID ---
    // Added 0 to the range to ensure the bottom line is drawn
    for i in 0..=4 {
        let visual_percentage = i as f32 * 0.25;
        let marker_y = y_offset + graph_height - (visual_percentage * graph_height as f32) as usize;

        let label_val = if i == 0 {
            0.0 // Force 0 for the bottom line
        } else if use_log {
            (visual_percentage * log_range + log_min).exp()
        } else {
            max_size_kb * visual_percentage
        };

        if marker_y < buffer.len() / stride {
            for px in x_offset..(x_offset + graph_width) {
                if px < stride {
                    buffer[marker_y * stride + px] = COL_GRID;
                }
            }
        }

        // Label
        // Adjusted y-offset (-10) to center larger text vertically on the grid line
        render_text(
            buffer,
            &format!("{:.0}", label_val),
            x_offset.saturating_sub(label_margin),
            marker_y.saturating_sub(10),
            stride,
            COL_TEXT,
            DISPLAY_GRAPH_SCALE_TEXT,
        );
    }

    // --- 3. DRAW THE DATA BARS ---
    for (i, &size_bytes) in history.iter().enumerate() {
        let size_kb = size_bytes / 1024.0;
        let norm_h = get_normalized_height(size_kb);
        let bar_height = (norm_h * graph_height as f32) as usize;

        let color = if size_kb > current_target_kb * 1.5 {
            0xFF5555
        } else if size_kb > current_target_kb {
            let intensity = ((size_kb / (current_target_kb * 1.5)) * 255.0) as u32;
            0xFF0000 | (intensity << 8)
        } else {
            let health = (size_kb / current_target_kb).min(1.0);
            let g = (150.0 + (105.0 * health)) as u32;
            (g << 8) | 100
        };

        // Render Bar
        for bh in 0..bar_height {
            let py = y_offset + graph_height - bh;
            if py < buffer.len() / stride {
                for bw in 0..bar_width {
                    let px = x_offset + (i * (bar_width + spacing)) + bw;
                    if px < stride {
                        buffer[py * stride + px] = color;
                    }
                }
            }
        }
    }

    // --- 4. DRAW DYNAMIC TARGET LINE ---
    let target_norm = get_normalized_height(current_target_kb);
    let target_y_base = y_offset + graph_height - (target_norm * graph_height as f32) as usize;
    let line_thickness = 3; // Make it 3 pixels thick

    if target_y_base < (buffer.len() / stride) {
        // Draw the thick line
        for ty in 0..line_thickness {
            let target_y = target_y_base.saturating_sub(ty);
            if target_y >= buffer.len() / stride {
                continue;
            }

            for px in x_offset..(x_offset + graph_width) {
                if px < stride {
                    // Overwrite whatever was there (puts it "on top")
                    buffer[target_y * stride + px] = COL_TGT;
                }
            }
        }

        // Render the label slightly offset from the thicker line
        render_text(
            buffer,
            &format!("TARGET: {:.1} kB", current_target_kb),
            x_offset + graph_width + 10,
            target_y_base.saturating_sub(12), // Adjusted for thickness
            stride,
            COL_TGT,
            2,
        );
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
    pub fn clear(&mut self) {
        self.history_buffer.clear();
        self.interval_buffer.clear();
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

#[derive(Debug, Clone)]
pub struct ObuUnit {
    pub obu_type: u8,
    pub data: Vec<u8>,
    pub is_sequence_header: bool,
}

pub struct Av1Parser {
    pub buffer: Vec<u8>,
    pub sequence_header: Option<Vec<u8>>,
}

impl Av1Parser {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            sequence_header: None,
        }
    }

    pub fn add_data(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    pub fn update_sequence_header(&mut self, data: &[u8]) {
        self.sequence_header = Some(data.to_vec());
    }

    pub fn get_sequence_header(&self) -> Option<&Vec<u8>> {
        self.sequence_header.as_ref()
    }

    fn parse_leb128(&self, offset: usize) -> Option<(usize, usize)> {
        let mut value: usize = 0;
        let mut bytes_read = 0;
        let mut shift = 0;

        loop {
            if offset + bytes_read >= self.buffer.len() {
                return None;
            }
            let byte = self.buffer[offset + bytes_read];
            value |= ((byte & 0x7F) as usize) << shift;
            bytes_read += 1;
            shift += 7;
            if (byte & 0x80) == 0 {
                break;
            }
            if bytes_read > 8 {
                return None;
            } // Safety
        }
        Some((value, bytes_read))
    }

    pub fn next_obu(&mut self) -> Option<ObuUnit> {
        if self.buffer.is_empty() {
            return None;
        }

        let header_byte = self.buffer[0];
        let obu_type = (header_byte >> 3) & 0xF;
        let extension_flag = (header_byte >> 2) & 1;
        let has_size_field = (header_byte >> 1) & 1;

        if has_size_field == 0 {
            return None;
        } // Simple safety check

        let mut offset = 1;
        if extension_flag == 1 {
            offset += 1;
            if self.buffer.len() < offset {
                return None;
            }
        }

        let (payload_size, leb_bytes) = self.parse_leb128(offset)?;
        offset += leb_bytes;

        let total_size = offset + payload_size;
        if self.buffer.len() < total_size {
            return None;
        }

        let obu_data = self.buffer[0..total_size].to_vec();
        self.buffer.drain(0..total_size);

        Some(ObuUnit {
            obu_type,
            data: obu_data,
            is_sequence_header: obu_type == 1,
        })
    }

    pub fn get_obu_units(&mut self) -> Vec<ObuUnit> {
        let mut units = Vec::new();
        while let Some(unit) = self.next_obu() {
            units.push(unit);
        }
        units
    }

    pub fn get_frames(&mut self) -> Vec<Vec<u8>> {
        self.get_obu_units().into_iter().map(|u| u.data).collect()
    }
}

pub struct HevcParser {
    pub buffer: Vec<u8>,
    // Store the most recent parameter sets
    pub vps: Option<Vec<u8>>,
    pub sps: Option<Vec<u8>>,
    pub pps: Option<Vec<u8>>,
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
    pub history_buffer: VecDeque<T>,
    pub max_history_size: usize,
}
#[allow(unused)]
impl<T> SlidingWindowAverage<T> {
    pub fn clear(&mut self) {
        self.history_buffer.clear();
    }

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

    // Method to return an iterator over the history_buffer
    pub fn get_history_iter(&self) -> std::collections::vec_deque::Iter<'_, T> {
        self.history_buffer.iter()
    }
}
impl SlidingWindowAverage<i64> {
    pub fn get_average(&self) -> f32 {
        // obtain the average value of integers.
        self.history_buffer.iter().sum::<i64>() as f32 / self.history_buffer.len() as f32
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

/// Draws an exponential sample using RNG seed.
pub fn exponential<R: Rng + ?Sized>(mean: f64, rng: &mut R) -> f64 {
    let u: f64 = rng.gen_range(0.0..1.0);
    -mean * u.ln()
}

// pub fn exponential(mean: f64) -> f64 {
//     let mut rng = rand::thread_rng();
//     let u: f64 = rng.gen_range(0.0..=1.0); // Generate a random value in the range (0, 1]
//     -mean * u.ln()
// }

// Separate struct to hold the data that will be shared
#[derive(Clone, Default)]
pub struct CsvData {
    v_timestamp: Vec<String>,
    v_packet_id: Vec<usize>,
    v_queue_size: Vec<usize>,
    v_queue_ts: Vec<f64>,
    v_queue_tq: Vec<f64>,
    v_packet_l: Vec<usize>,

    v_id_src: Vec<i32>,
    v_id_dest: Vec<i32>,
    v_ampdu_id: Vec<u32>,
    v_collision: Vec<usize>,
    v_T_collision: Vec<f64>,
    v_link_id: Vec<usize>,
    v_cw_value: Vec<usize>,
    v_retries: Vec<u8>,
    v_last_backoff_value: Vec<i32>,

    v_edca_ac: Vec<String>,
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
            v_ampdu_id: Vec::new(),
            v_collision: Vec::new(),
            v_T_collision: Vec::new(),
            v_link_id: Vec::new(),
            v_cw_value: Vec::new(),
            v_retries: Vec::new(),
            v_last_backoff_value: Vec::new(),
            v_edca_ac: Vec::new(),
            ..Default::default()
        }
    }
}
use std::io::BufWriter;

#[derive(Clone)]
pub struct CsvType {
    csv_data: Arc<Mutex<CsvData>>,
    writer: Arc<Mutex<BufWriter<std::fs::File>>>,
    batch_size: usize,
}

impl CsvType {
    /// Creates a new CsvType with a buffered writer and specified batch size.
    pub fn new(folder_name: &str, results_folder: &str) -> io::Result<Self> {
        let dir = format!("{}/{}",  results_folder , folder_name);
        std::fs::create_dir_all(&dir)?;
        let file_path = format!("{}/QUEUE_stats.csv", dir);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;
        let mut buf = BufWriter::new(file);
        // Write header if file is empty
        if buf.get_ref().metadata()?.len() == 0 {
            writeln!(buf, "timestamp,packet_ID,queue_size,L_packet,T_s,T_q,id_src,id_dest,AMPDU_ID,is_collision,T_collision,link_id,CW_value,backoff_retry_counter,last_BO_drawn,EDCA_AC")?;
            buf.flush()?;
        }
        Ok(Self {
            csv_data: Arc::new(Mutex::new(CsvData::new())),
            writer: Arc::new(Mutex::new(buf)),
            batch_size: BATCH_SIZE_CSV_QUEUE,
        })
    }

    /// Pushes a new record into the in-memory buffer and flushes when batch size is reached.
    pub fn update_stats(
        &self,
        now: TaiTime<0>,
        id_packet: usize,
        queue_size: usize,
        Ts: f64,
        Tq: f64,
        length_packet: usize,
        id_src: i32,
        id_dest: i32,
        ampdu_id: u32,
        is_collision: bool,
        T_collision: f64,
        link_id: usize,
        cw_val: usize,
        num_retries_backoff: u8,
        last_backoff: i32,
        edca_ac: String,
    ) {
        let ts_str = format_timestamp!(now);
        {
            let mut data = self.csv_data.lock().unwrap();
            data.v_timestamp.push(ts_str.clone());
            data.v_packet_id.push(id_packet);
            data.v_queue_size.push(queue_size);
            data.v_packet_l.push(length_packet);
            data.v_queue_ts.push(Ts);
            data.v_queue_tq.push(Tq);
            data.v_id_src.push(id_src);
            data.v_id_dest.push(id_dest);
            data.v_ampdu_id.push(ampdu_id);
            data.v_collision.push(is_collision as usize);
            data.v_T_collision.push(T_collision);
            data.v_link_id.push(link_id);
            data.v_cw_value.push(cw_val);
            data.v_retries.push(num_retries_backoff);
            data.v_last_backoff_value.push(last_backoff);
            data.v_edca_ac.push(edca_ac);
        }

        // Check if batch limit reached
        let flush_now = {
            let data = self.csv_data.lock().unwrap();
            data.v_timestamp.len() >= self.batch_size
        };

        if flush_now {
            if let Err(e) = self.flush_batch() {
                eprintln!("Error flushing CSV batch: {}", e);
            }
        }
    }

    /// Writes all buffered records to CSV and clears the buffer.
    fn flush_batch(&self) -> io::Result<()> {
        let mut data = self.csv_data.lock().unwrap();
        let mut writer = self.writer.lock().unwrap();

        for i in 0..data.v_timestamp.len() {
            writeln!(
                writer,
                "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                data.v_timestamp[i],
                data.v_packet_id[i],
                data.v_queue_size[i],
                data.v_packet_l[i],
                data.v_queue_ts[i],
                data.v_queue_tq[i],
                data.v_id_src[i],
                data.v_id_dest[i],
                data.v_ampdu_id[i],
                data.v_collision[i],
                data.v_T_collision[i],
                data.v_link_id[i],
                data.v_cw_value[i],
                data.v_retries[i],
                data.v_last_backoff_value[i],
                data.v_edca_ac[i],
            )?;
        }
        writer.flush()?;
        // Clear the in-memory buffer
        data.v_timestamp.clear();
        data.v_packet_id.clear();
        data.v_queue_size.clear();
        data.v_packet_l.clear();
        data.v_queue_ts.clear();
        data.v_queue_tq.clear();
        data.v_id_src.clear();
        data.v_id_dest.clear();
        data.v_ampdu_id.clear();
        data.v_collision.clear();
        data.v_T_collision.clear();
        data.v_link_id.clear();
        data.v_cw_value.clear();
        data.v_retries.clear();
        data.v_last_backoff_value.clear();
        data.v_edca_ac.clear();

        Ok(())
    }
}

// Optionally, implement Drop to flush any remaining data on drop
impl Drop for CsvType {
    fn drop(&mut self) {
        if let Err(e) = self.flush_batch() {
            eprintln!("Error flushing CSV on drop: {}", e);
        }
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
        sta_src_id: i32,
        sta_dest_id: i32,
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
                "(ALVR ID: {} |  s{:>3}/{:>3}, F: {:>4})",
                self.stream_id,
                self.shard_index,
                self.shards_count - 1,
                self.next_packet_index,
            )
        }
    }
}
#[derive(Debug, Clone)]
pub struct MpduPacket {
    pub packet_id: usize,
    pub length_packet_bits: usize,
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

    pub original_index: usize,
    pub edca_ac: EdcaAc,

    pub mac_key_cached: Option<MacKey>,
    pub assigned_link_id: Option<u8>,
    // pub is_alvr_control_packet: bool,
}
#[repr(u8)]
#[allow(unused)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EdcaAc {
    Voice = 0,
    Video = 1,
    BestEffort = 2,
    Background = 3,
}
impl Default for EdcaAc {
    fn default() -> Self {
        EdcaAc::BestEffort
    }
}

#[allow(unused)]
impl MpduPacket {
    pub fn new() -> Self {
        Self {
            packet_id: 0,
            length_packet_bits: 0,
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
            original_index: 0,
            edca_ac: EdcaAc::BestEffort,
            mac_key_cached: None,
            assigned_link_id: None,
            // is_alvr_control_packet: false,
        }
    }

    pub fn assign_link(&mut self, link_id: u8) {
        if STR_PLUS_MODE_MLO {
            self.assigned_link_id = None;
        } else {
            assert!(self.assigned_link_id.is_none(), "Link already assigned"); // make extra sure we don't assign links to packets more than once
            self.assigned_link_id = Some(link_id);
        }
    }

    pub fn print(&self, color: DebugColor) -> String {
        print_prettyyy!(
            color,
            "SRC: {} DEST: {}|  Packet ID: {}, ALVR F: {} S: {}/{} L: {}",
            self.sta_src_id,
            self.sta_dest_id,
            self.packet_id,
            self.header_alvr.next_packet_index,
            self.header_alvr.shard_index,
            self.header_alvr.shards_count - 1,
            self.length_packet_bits
        );
        let a = format!(
            "SRC: {} DEST: {}| Packet ID: {}, ALVR F: {} S: {}/{} L: {}",
            self.sta_src_id,
            self.sta_dest_id,
            self.packet_id,
            self.header_alvr.next_packet_index,
            self.header_alvr.shard_index,
            self.header_alvr.shards_count - 1,
            self.length_packet_bits
        );
        a
    }
}

type MacKey = (i32, EdcaAc, u8); // e.g. (AP/STA_ID, EDCA_AC, link_id)); last u8 for MLO link ID
type WindowKey = (i32, u8); // (STA_ID, link_id)

#[derive(Debug, Clone)]
pub struct AmpduPacket {
    pub mpdu_packets: Vec<MpduPacket>, // Container for MPDU packets
    pub total_length: usize,           // Total length of aggregated packets
    pub sta_src_id: i32,
    pub sta_dest_id: i32, // ID for the destination STA
    pub size: i32,
    pub coordinates: Coords,
    pub mac_key: MacKey,
    pub link_id: u8, // MLO field for intended link .

    pub mcs_assigned: u8,
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
            mac_key: MacKey::default(),
            link_id: 0,
            mcs_assigned: 0,
        }
    }
    // Method to print AMPDU_packet values
    pub fn print(&self) {
        println!(
            "\x1b[33m \t[AMPDU INFO]\tSize: {}, Total Length: {} Bits | SRC_ID: {}, DEST_ID: {} | MCS: {} \x1b[0m",
            self.size, self.total_length, self.sta_src_id, self.sta_dest_id, self.mcs_assigned,
        );
        //  println!("AMPDU on LINK-{}: {} packets, {} bytes",
        //     self.link_id, self.mpdu_packets.len(), self.total_length);
        for packet in &self.mpdu_packets {
            println!(
                "\x1b[33m\t - Packet ID:{:>4}, L ={:>6} bits ({:>5} Bytes inner) | T_q: {:.3} ms , T_s: {:.3} ms | StreamID: {} | shard {:>3}/{:>3} , F:{:>5}\x1b[0m",
                packet.packet_id,
                packet.length_packet_bits,
                packet.data_inner.len(), // data_inner length counts bytes
                packet.T_q.as_secs_f64() * 1000.0,
                packet.T_s.as_secs_f64() * 1000.0,
                packet.header_alvr.stream_id,
                packet.header_alvr.shard_index,
                packet.header_alvr.shards_count - 1,
                packet.header_alvr.next_packet_index,
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
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Coords {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

impl Coords {
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

// impl ResultsFrameTXDelay {
//     pub fn new() -> Self {
//         Self {
//             service_delay: 0.0,
//             data_service_delay: 0.0,
//             pathloss: 0.0,
//             p_rx: 0.0,
//         }
//     }
// }
#[inline]
pub fn calculate_distance(x: f64, y: f64, z: f64, x_: f64, y_: f64, z_: f64) -> f64 {
    let dx = x_ - x;
    let dy = y_ - y;
    let dz = z_ - z;

    (dx * dx + dy * dy + dz * dz).sqrt()
}
#[inline]
pub fn path_loss(d: f64) -> f64 {
    let gamma = 2.06067_f64;
    54.12 + 10.0 * gamma * (d).log10() + 5.25 * 0.1467 * d
}
#[inline]
pub fn collision_delay() -> f32 {
    let OBasicRate: f64 = 1.0 / 2.0 * 1.0 * 48.0;

    // let _L: f64 = total_bits_transmitted / n_mpdus as f64;

    let SF = 16.0;
    let TB = 18.0;

    let T_RTS: f64 = LEGACY_PHY_DURATION + ((SF + 160.0 + TB) / OBasicRate).ceil() * 4E-6; // legacy symbol time is 4E-6
    let T_CTS: f64 = LEGACY_PHY_DURATION + ((SF + 112.0 + TB) / OBasicRate).ceil() * 4E-6;

    let T_collision = T_RTS + SIFS + T_CTS;
    T_collision as f32
}

#[inline]
pub fn airtime_ampdu(
    total_bits_transmitted_app: f64,
    n_mpdus: i32,
    coords_src: Coords,
    coords_dest: Coords,
    _p_tx_orig: f64,
    channel_width: usize,
) -> (f64, u8) {
    
    let p_tx_cheated = match channel_width { // small hack, higher widths get higher P_tx
        20 => 20.0,
        40 => 20.0,
        80 => 20.0,
        160 => 23.0,
        320 => 30.05,
        _ => 20.0, // default at 20 dBm
    };

    let effPt: f64 = p_tx_cheated;


    let SU_spatial_streams = 1.0;


    let distance = calculate_distance(
        coords_src.x,
        coords_src.y,
        coords_src.z,
        coords_dest.x,
        coords_dest.y,
        coords_dest.z,
    );

    // print_pink!("coords_src: {:?}, coords_dest: {:?}, DISTANCE = {:.4} m", coords_src, coords_dest, distance);
    let PL = path_loss(distance);
    let mut Pr = effPt - PL;

    // 3. Calculate the noise adjustment for wider channels.
    let noise_adjustment_db = match channel_width {
        40 => 3.01,
        80 => 6.02,
        160 => 9.03,
        320 => 12.04,
        _ => 0.0, // For 20 MHz or any other default
    };

    // 4. Normalize the Pr to its 20 MHz equivalent.
    Pr = Pr - noise_adjustment_db;

    // println!("AP to STA: I'm at {:?} and you're at {:?} |  Distance = {:.2}, PL = {:.2}, P_rx = {:.1}", coords_src, coords_dest, distance, PL, Pr);

    let (bits_symbol, coding_rate, _mcs_val) = match Pr {
        _ if Pr < -82.0 => (1, 1.0 / 2.0, 0), // Could add additional PER in this case
        _ if Pr >= -82.0 && Pr < -79.0 => (1, 1.0 / 2.0, 0),
        _ if Pr >= -79.0 && Pr < -77.0 => (2, 1.0 / 2.0, 1),
        _ if Pr >= -77.0 && Pr < -74.0 => (2, 3.0 / 4.0, 2),
        _ if Pr >= -74.0 && Pr < -70.0 => (4, 1.0 / 2.0, 3),
        _ if Pr >= -70.0 && Pr < -66.0 => (4, 3.0 / 4.0, 4),
        _ if Pr >= -66.0 && Pr < -65.0 => (6, 1.0 / 2.0, 5),
        _ if Pr >= -65.0 && Pr < -64.0 => (6, 2.0 / 3.0, 6),
        _ if Pr >= -64.0 && Pr < -59.0 => (6, 3.0 / 4.0, 7),
        _ if Pr >= -59.0 && Pr < -57.0 => (8, 3.0 / 4.0, 8),
        _ if Pr >= -57.0 && Pr < -55.0 => (8, 5.0 / 6.0, 9),
        _ if Pr >= -55.0 && Pr < -53.0 => (10, 3.0 / 4.0, 10),
        _ if Pr >= -53.0 && Pr < -49.0 => (10, 5.0 / 6.0, 11),
        _ if Pr >= -49.0 && Pr < -46.0 => (12, 3.0 / 4.0, 12), // MCS 12, TODO: find a good reference for 802.11be SNR
        _ if Pr >= -46.0 => (12, 5.0 / 6.0, 13),               // MCS 13
        _ => (1, 1.0 / 2.0, 1),                                // Catch-all for Pr out of range
    };
    // println!("P_rx = {}", Pr);

    let Subcarriers = match channel_width {
        320 => 3920, // 320 MHz: data subcarriers (EHT / Wi-Fi7)
        160 => 1960, // 160 MHz: data subcarriers (HE/Wi-Fi6)
        80 => 980,   // https://www.arubanetworks.com/assets/wp/WP_802.11AX.pdf, page 12
        40 => 468,
        20 => 234,
        _ => 0, // Default case,  fallback
    };

    let ORate: f64 = SU_spatial_streams * bits_symbol as f64 * coding_rate * Subcarriers as f64;
    // let OBasicRate: f64 = 1.0 / 2.0 * 1.0 * 48.0; // 6 Mbps conservative rate
    let OBasicRate: f64 = 1.0 / 2.0 * 4.0 * 48.0; // evaluates to 96.0 bits/symbol, 4 bit symbol (16-QAM) * 1/2 CR * 48 subcarriers

    let app_payload_per_mpdu = total_bits_transmitted_app / n_mpdus as f64; 
    
    // 2. Network Stack Overhead: LLC/SNAP (8B) + IPv4 (20B) + UDP (8B) = 36 Bytes (288 bits)
    let L_avg = app_payload_per_mpdu + 288.0; // added protocol headers per-MPDU
    // let L: f64 = total_bits_transmitted / n_mpdus as f64; // TODO: Check if it's correct to have a size as f32 (in reality not, but as avg model? )

    let SF = 16.0;
    let TB = 18.0;
    let MD = 32.0;
    let MAC_H_size = 288.0; // FC, EHT control, Addresses, FCS, QoS control, etc. overhead in bits.  

    let T_RTS: f64 = LEGACY_PHY_DURATION + ((SF + 160.0 + TB) / OBasicRate).ceil() * SYMBOL_TIME_LEGACY; // legacy symbol time is 4E-6
    let T_CTS: f64 = LEGACY_PHY_DURATION + ((SF + 112.0 + TB) / OBasicRate).ceil() * SYMBOL_TIME_LEGACY;
    
    let mpdu_length_bits = L_avg + MAC_H_size; // Payload + MAC Header
    let padded_mpdu_size = (mpdu_length_bits / 32.0).ceil() * 32.0 ; // Round up to 32-bit boundary for padding
    
    let T_DATA: f64 = EHT_PHY_DURATION + ((SF + n_mpdus as f64 * (MD + padded_mpdu_size) + TB) / ORate).ceil() * SYMBOL_TIME_11AX + PE_DURATION; // 802.11ax symbol time 4 times greates for 16E-6 s

    // pub const EHT_PHY_DURATION: f64 = 76E-6;    // 802.11be Preamble    // L-STF      :   8.00 us
    //                                                                 // L-LTF      :   8.00 us
    //                                                                 // L-SIG      :   4.00 us
    //                                                                 // RL-SIG     :   4.00 us
    //                                                                 // U-SIG      :   8.00 us
    //                                                                 // EHT-SIG    :   8.00 us
    //                                                                 // EHT-STF    :   4.00 us
    //                                                                 // EHT-LTF    :  32.00 us


    let ba_base_bytes = 24.0; // Frame Control, Dur, RA, TA, BA Ctrl, Seq Ctrl, FCS
    let ba_bitmap_bytes = if n_mpdus <= 64 {
        8.0  // Standard Compressed (64 bits)
    } else {
        32.0 // HE Extended Compressed (256 bits)
    };

    let block_ack_bits = (ba_base_bytes + ba_bitmap_bytes) * 8.0; // 256 bits with 64-sized A-MPDUs
    
    let T_ACK: f64 = LEGACY_PHY_DURATION + ((SF + block_ack_bits + TB) / OBasicRate).ceil() * SYMBOL_TIME_LEGACY; 

    let phy_time = T_RTS + SIFS + T_CTS + SIFS + T_DATA + SIFS + T_ACK; // ⬅  removed DIFS + SLOT + BO, it happens in EDCA now.
    // let phy_time = T_DATA + SIFS + T_ACK; //  (without RTS/CTS, todo: set based on constant/input arg.)

    // let rts_cts_overhead_time: f64 = T_RTS + SIFS + T_CTS + SIFS;                            // ONLY FOR DEBUG
    // let _rts_cts_overhead_percent = (rts_cts_overhead_time / phy_time) * 100.0;              // ONLY FOR DEBUG
    // print_dblue!("[AMPDU airtime = {:.3} ms] Bits: {} Channel Width: {:?} MHz, O_rate: {:.2}, eff_Pt={}, Pr: {:.3}\n\t\t| distance = {:.3} |  PathLoss = {:.3} | RTS/CTS Overhead: {:.1} % |"
    //              ,phy_time * 1000.0, total_bits_transmitted_app,  channel_width, ORate, effPt, Pr, distance, PL, _rts_cts_overhead_percent,);
    (phy_time, _mcs_val as u8)
}

#[inline]
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

    // pub client_fps: f32,
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
    pub decoder_jitterbuffer_level: u8,
    pub num_rebuffering_events: u8,

    pub flr_deadline: usize,
    pub shardloss_deadline: usize,
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
use std::net::IpAddr;

pub fn get_third_octet(ip: IpAddr) -> Option<u8> {
    match ip {
        IpAddr::V4(ipv4) => {
            let octets = ipv4.octets(); // returns [u8; 4]
            Some(octets[2]) // third octet (0-based index)
        }
        _ => None, // IpAddr::V6(_) => None, // IPv6 doesn't have octets in the same sense
    }
}

#[derive(Default)]
pub struct OldCsvTrace {
    path: PathBuf,
    _writer: Option<csv::Writer<std::fs::File>>,
}
impl Clone for OldCsvTrace {
    fn clone(&self) -> Self {
        OldCsvTrace {
            path: self.path.clone(),
            _writer: None,
        }
    }
}

use std::fs::File;

use async_std::sync::Mutex as aMutex;

#[derive(Default)]
struct CsvTrace {
    path: PathBuf,
    writer: Option<Arc<aMutex<csv::Writer<BufWriter<File>>>>>,
}

impl Clone for CsvTrace {
    fn clone(&self) -> Self {
        CsvTrace {
            path: self.path.clone(),
            writer: None, // cloned instance will re-init its own writer
        }
    }
}

impl CsvTrace {
    /// Open in append mode with buffering
    fn init_writer(&mut self) -> anyhow::Result<()> {
        if self.path.as_os_str().is_empty() {
            anyhow::bail!("CsvTrace path not set");
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let buf = BufWriter::with_capacity(256 * 1024, file); // 256KB buffer
        let wtr = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(buf);

        self.writer = Some(Arc::new(aMutex::new(wtr)));
        Ok(())
    }

    /// Append a record safely from multiple async tasks
    async fn write_record<I, T>(&self, record: I) -> anyhow::Result<()>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<[u8]>,
    {
        if let Some(wtr) = &self.writer {
            let mut w = wtr.lock().await;
            w.write_record(record)?;
        }
        Ok(())
    }

    /// Flush manually if needed (e.g. at sim end)
    async fn flush(&self) -> anyhow::Result<()> {
        if let Some(wtr) = &self.writer {
            let mut w = wtr.lock().await;
            w.flush()?;
        }
        Ok(())
    }
}

#[allow(unused)]
#[derive(Clone, PartialEq, Debug)]
pub enum WindowType {
    BySeconds {
        sliding_window_secs: Option<f32>,
    },
    // #[schema(strings(display_name = "Sample-based"))]
    BySamples {
        // #[schema(strings(display_name = "Window size"))]
        // #[schema(flag = "real-time")]
        // #[schema(gui(slider(min = 32, max = 256, step = 1)), suffix = " samples")]
        sliding_window_samp: usize,
    },
}
#[allow(unused)]
#[derive(Clone, PartialEq, Debug)]
pub enum AveragingStrategy {
    SimpleWindowAverage {
        // #[schema(flag = "real-time")]
        // #[schema(strings(display_name = "Statistics sliding window type"))]
        window_type: WindowType,
    },
    // #[schema(strings(display_name = "Exponential Weighted Moving Average"))]
    ExponentialMovingAverage {
        // #[schema(flag = "real-time")]
        // #[schema(strings(
        //     help = "EWMA_t = alpha*r_t+(1-alpha)*EWMA_{t-1}, where `alpha` denotes the EWMA weight and `r` is the value in the current period."
        // ))]
        // #[schema(gui(slider(min = 0.1, max = 1.0, step = 0.01)))]
        ewma_weight: f32,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, Copy, Default)]
pub struct HeuristicStats {
    pub bitrate_step_count: usize,

    pub bitrate_dec_steps: usize,
    pub bitrate_inc_steps: usize,

    pub bitrate_step_size_mbps: f32,

    pub r_rtt: f32,
    pub r_inc: f32,

    pub rtt_adj_prob: f32,
    pub bitrate_inc_prob: f32,

    pub fps_tx_avg: f32,
    pub fps_rx_avg: f32,

    pub nfr_avg: f32,
    pub rtt_avg_ms: f32,

    pub nfr_thresh: f32,
    pub rtt_thresh_ms: f32,

    pub requested_bitrate_mbps: f32,
    pub estimated_capacity_mbps: f32,
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
