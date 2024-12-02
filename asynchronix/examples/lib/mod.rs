#![allow(non_snake_case)]

use std::collections::VecDeque;
use std::f64;

use csv::Writer;
use std::fs::OpenOptions;
use tai_time::TaiTime;

use rand::Rng;
use std::time::{Duration, Instant};

use std::sync::Arc;
use std::sync::Mutex;
use serde::{Deserialize, Serialize};  
use colored::Colorize;
use crate::lib::alvr_stream_socket::{DeviceMotion, Pose};
use once_cell::sync::Lazy;

const CW_MIN: i32 = 15;
const CHANNEL_WIDTH: usize = 80; //MHz

const LEGACY_PHY_DURATION: f64 = 20E-6; // microseconds
const PHY_DURATION: f64 = 100E-6;
const SLOT: f64 = 9E-6;
const SIFS: f64 = 16E-6;
const DIFS: f64 = 31E-6;

pub const DEFAULT_TMAX_AGG: f64 = 4.85E-3;
pub const MAX_AMPDU_SIZE: i32 = 64;
pub const P_TX: f64 = 20.0;

pub const INITIAL_BITRATE_MBPS_SIM: f32 = 10.0;

// Define a constant to control debugging
pub const DEBUG_PRINT_ENABLED: bool = false; // Change to false to disable

pub mod alvr_packets;
pub mod alvr_statistics;
pub mod alvr_stream_socket;
pub mod models_XR;
pub mod models_mm1k;

pub mod alvr_control_socket;

pub type OptLazy<T> = Lazy<Mutex<Option<T>>>;
pub const fn lazy_mut_none<T>() -> OptLazy<T> {
    Lazy::new(|| Mutex::new(None))
}

#[macro_export]
macro_rules! debug_print {
    ($color:expr, $fmt:expr, $($arg:tt)*) => {
        // Check if debugging is enabled
        if DEBUG_PRINT_ENABLED {
            let msg = format!($fmt, $($arg)*);
            println!("{}", $color.to_color_fn()(msg));
        }
    };
}
#[macro_export]
macro_rules! debug_bgprint {
    ($color:expr, $fmt:expr, $($arg:tt)*) => {
        // Check if debugging is enabled
            let msg = format!($fmt, $($arg)*);
            println!("{}", $color.to_background_fn()(msg));

    };
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
    DarkOrange
}

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


pub struct SlidingWindowTimely<T> {
    history_buffer: VecDeque<T>,
    interval_buffer: VecDeque<f32>,
    max_window_duration: f32,
}

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

#[derive(Clone)]
pub struct SlidingWindowAverage<T> {
    history_buffer: VecDeque<T>,
    max_history_size: usize,
}

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

#[derive(Clone)]
pub struct CsvType {
    csv_data: Arc<Mutex<CsvData>>,
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
        }
    }

    pub fn write_to_csv(&self, folder: &str) -> std::io::Result<()> {
        
        let path = format!("Results/{folder}/QUEUE_stats.csv"); 
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?; 

        let mut writer = Writer::from_writer(file);

        // Write header
        writer.write_record(&[
            "timestamp",
            "packet_ID",
            "queue_size",
            "L_packet",
            "T_s",
            "T_q",
        ])?;

        // Write all stored data at once
        for i in 0..self.v_timestamp.len() {
            writer.write_record(&[
                &self.v_timestamp[i],
                &self.v_packet_id[i].to_string(),
                &self.v_queue_size[i].to_string(),
                &self.v_packet_l[i].to_string(),
                &self.v_queue_ts[i].to_string(),
                &self.v_queue_tq[i].to_string(),
            ])?;
        }

        writer.flush()?;
        Ok(())
    }
}

impl CsvType {
    pub fn new() -> Self {
        Self {
            // v_timestamp: Vec::new(),
            // v_packet_id: Vec::new(),
            // v_queue_size: Vec::new(),
            // v_queue_ts: Vec::new(),
            // v_queue_tq: Vec::new(),
            // v_packet_l: Vec::new(),
            csv_data: Arc::new(Mutex::new(CsvData::new())),
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
    ) {
        let formatted_timestamp = format_timestamp!(now);

        if let Ok(mut data) = self.csv_data.lock() {
            data.v_timestamp.push(formatted_timestamp);
            data.v_packet_id.push(id_packet);
            data.v_queue_size.push(queue_size);
            data.v_queue_ts.push(Ts);
            data.v_queue_tq.push(Tq);
            data.v_packet_l.push(length_packet);
        }
    }
}

#[derive(Clone)]
pub struct CumulativeStats {
    values: VecDeque<f64>,
    sum: f64,
    sum_of_squares: f64,
}

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
            println!("ZEROOOOOOOOOOOOOOOo");
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

#[allow(non_camel_case_types)]
#[derive(Clone)]
pub struct perStaStats {
    pub sta_id: i32,
    pub rx_packets_counter: i32,
    pub q_time_sta_cum: CumulativeStats,
    pub s_time_sta_cum: CumulativeStats,
    pub csv_data: CsvData,
}
impl perStaStats {
    pub fn new() -> Self {
        Self {
            sta_id: -1,
            q_time_sta_cum: CumulativeStats::new(),
            s_time_sta_cum: CumulativeStats::new(),
            csv_data: CsvData::new(),
            rx_packets_counter: 0,
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
                format!("{:>10}", self.rx_packets_counter),
            ),
            (
                &tq_label,
                format!("{:>10.6}", self.q_time_sta_cum.get_average()),
            ),
            (
                &ts_label,
                format!("{:>10.6}", self.s_time_sta_cum.get_average()),
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
    ) {
        self.q_time_sta_cum.add(Tq);
        self.s_time_sta_cum.add(Ts);
        self.rx_packets_counter += 1;

        let formatted_timestamp = format_timestamp!(now);

        self.csv_data.v_timestamp.push(formatted_timestamp);
        self.csv_data.v_packet_id.push(id_packet);
        self.csv_data.v_queue_size.push(queue_size);
        self.csv_data.v_queue_ts.push(Ts);
        self.csv_data.v_queue_tq.push(Tq);
        self.csv_data.v_packet_l.push(length_packet);
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

#[derive(Default, Debug, Clone)]
pub struct HeaderALVRStream {
    pub packet_length: u32,
    pub stream_id: u16,
    pub next_packet_index: u32,
    pub shards_count: u32,
    pub shard_index: u32,
    pub tx_instant: f32,
}

#[derive(Debug, Clone)]
pub struct MpduPacket {
    pub packet_id: usize,
    pub length_packet: usize,
    pub queue_in_instant: TaiTime<0>,
    pub queue_out_instant: TaiTime<0>,
    pub sink_in_instant: Instant,
    pub T_q: Duration,
    pub T_s: Duration,
    pub expected_T_s: Duration,

    pub sta_src_id: i32,
    pub sta_dest_id: i32,
    pub sta_src_coords: Coords,
    pub queue_length_when_out: usize,

    pub data_inner: Vec<u8>,
    pub header_alvr: HeaderALVRStream,
    // pub is_alvr_control_packet: bool, 
}

impl MpduPacket {
    pub fn new() -> Self {
        Self {
            packet_id: 0,
            length_packet: 0,
            queue_in_instant: TaiTime::default(),
            queue_out_instant: TaiTime::default(),
            sink_in_instant: Instant::now(),
            T_q: Duration::ZERO,
            T_s: Duration::ZERO,
            expected_T_s: Duration::ZERO,

            sta_src_id: 0,
            sta_dest_id: 0,
            sta_src_coords: Coords::new(),
            queue_length_when_out: 0,
            data_inner: vec![],
            header_alvr: HeaderALVRStream::default(),
            // is_alvr_control_packet: false, 
        }
    }

    pub fn print(&self) {
        println!("Packet ID: {}, L: {}", self.packet_id, self.length_packet);
    }
}
#[derive(Debug, Clone)]
pub struct AmpduPacket {
    pub mpdu_packets: Vec<MpduPacket>, // Container for MPDU packets
    pub total_length: usize,           // Total length of aggregated packets
    pub sta_dest_id: i32,                   // ID for the source STA
    pub size: i32,
    pub coordinates: Coords,
}

impl AmpduPacket {
    pub fn new() -> Self {
        AmpduPacket {
            mpdu_packets: Vec::new(), // Initialize an empty vector for MPDU packets
            total_length: 0,          // Initialize total length to 0
            sta_dest_id: -1, // Initialize STA_ID to -1 (assuming -1 indicates uninitialized)
            size: 0,    // Initialize size to 0
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
            "\x1b[33m \t[AMPDU INFO]\tSize: {}, STA_dest_ID: {}, Total Length: {}\x1b[0m",
            self.size, self.sta_dest_id, self.total_length
        );
        for packet in &self.mpdu_packets {
            println!(
                "\x1b[33m\t - Packet ID: {:.0},T_q: {:.8} , T_s: {:.8}, {:?} \x1b[0m",
                packet.packet_id,
                packet.T_q.as_secs_f64(),
                packet.T_s.as_secs_f64(), 
                packet.header_alvr

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
}

#[derive(Debug, Default)]
pub struct ResultsFrameTXDelay {
    pub service_delay: f64,
    pub data_service_delay: f64,
    pub pathloss: f64,
    pub p_rx: f64, // Rust doesn't have a separate `long double`, so f64 is used
                   // pub o_rate: f64,
}

impl ResultsFrameTXDelay {
    // pub fn clear(&mut self) {
    //     self.service_delay = 0.0;
    //     self.data_service_delay = 0.0;
    //     self.pathloss = 0.0;
    //     self.p_rx = 0.0;
    //     self.o_rate = 0.0;
    // }
    pub fn new() -> Self{
        Self { service_delay: (0.0), data_service_delay: (0.0), pathloss: (0.0), p_rx: (0.0) }
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
pub fn frametransmission_delay(
    total_bits_transmitted: f64,
    n_mpdus: i32,
    coords_src: Coords,
    coords_dest: Coords,
    p_tx: f64,
) -> ResultsFrameTXDelay {
    let channel_width: usize = CHANNEL_WIDTH;

    // Effective Pt
    let mut effPt = p_tx;
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
        _ if Pr >= -53.0 => (10, 5.0 / 6.0),
        _ => (1, 1.0 / 2.0), // Catch-all for Pr out of range
    };

    let Subcarriers = match channel_width {
        // https://www.arubanetworks.com/assets/wp/WP_802.11AX.pdf, page 12
        80 => 980,
        40 => 468,
        20 => 234,
        _ => 0, // Default case,  fallback
    };

    let SU_spatial_streams = 2.0;
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

    let T =
        T_RTS + SIFS + T_CTS + SIFS + T_DATA + SIFS + T_ACK + DIFS + SLOT + T_DETERMINISTIC_BACKOFF;

    // println!("[DEBUUUG FT_DELAY] L_total = {:.2}, N_MPDUs = {}, T_s : {},  x: {:.1}, y: {:.1}\n", total_bits_transmitted, n_mpdus, T, coords_dest.x, coords_dest.y );

    ResultsFrameTXDelay {
        pathloss: PL,
        p_rx: Pr,
        // o_rate: ORate,
        service_delay: T,
        data_service_delay: T_DATA,
    }
}

pub fn write_all_sta_csvs(sta_stats_vec: &Vec<perStaLockStats>, folder: &str) -> std::io::Result<()> {
    for (_index, sta_stats) in sta_stats_vec.iter().enumerate() {
        // Lock the mutex to access the data
        if let Ok(stats) = sta_stats.data.lock() {
            // Create a filename with the station ID
            let filename: String = format!("Results/{folder}/STA{}.csv", stats.sta_id);
            println!("FILENAMEEE: {filename}"); 
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
            ])?;

            // Write all stored data for this station
            for i in 0..stats.csv_data.v_timestamp.len() {
                writer.write_record(&[
                    &stats.csv_data.v_timestamp[i],
                    &stats.csv_data.v_packet_id[i].to_string(),
                    &stats.csv_data.v_queue_size[i].to_string(),
                    &stats.csv_data.v_packet_l[i].to_string(),
                    &stats.csv_data.v_queue_ts[i].to_string(),
                    &stats.csv_data.v_queue_tq[i].to_string(),
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
pub struct GraphNetworkStatistics_csv {
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

    pub requested_bps: f32,

    pub interval_avg_plot_throughput: f32,
}


#[derive(
    Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord,
)]
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
    pub steps_bps: f32,

    pub network_heur_fps: f32,
    pub rtt_avg_heur_s: f32,
    pub random_prob: f32,

    pub threshold_fps: f32,
    pub threshold_rtt_s: f32,
    pub threshold_u: f32,

    pub requested_bitrate_bps: f32,
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
