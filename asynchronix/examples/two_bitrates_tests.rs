

// pub const INTRAREFRESH_ENABLED: bool = true; 

pub const FRAMERATE_WINDOWS: usize = 60; 
pub const INITIAL_FRAMERATE_FPS: f32 = 90.0; 
// pub const IDR_FRAME_SIZE_GOP: usize = 30; 

const SCALE: f64 = 0.3;
pub const WIDTH_ENCODER: usize = 3840;
pub const HEIGHT_ENCODER: usize = 2160;


const MAX_PARALLEL_VMAF: usize = 20;
const WORKERS: usize = 1;


pub const RESYNC_BUFFER: usize = 20; 
const SIM_HISTORY: usize = 20; 
const DESYNC_STD_DEV :f64 = 20.0; 
const DESYNC_HIST_WINDOW: Duration = Duration::from_millis(1500); 

pub const MAX_BITRATE_REFERENCE: f32 = 100.0; 

#[path = "lib/mod.rs"]      // relative path to the module root you want
mod lib; 
use lib::render_text; 

macro_rules! print_prettyy {
    ($color:expr, $fmt:expr, $($arg:tt)*) => {
        // if true {
            let msg = format!($fmt, $($arg)*);
            println!("{}", $color.to_background_fn()(msg));
        // }
    };
}



pub const RGB_SIMILARITY_THRESHOLD: f64 = 0.5;
// pub const MAX_REGULAR_FRAMES_FOR_COMPARE: usize = 10;

// Similarity thresholds for sync state transitions
const GOOD_SIMILARITY_THRESHOLD: f64 = 0.15; // 80% similar to establish sync
const ACCEPTABLE_SIMILARITY_THRESHOLD: f64 = 0.35; // 60% similar to maintain sync
/// Threshold for considering a frame match "good" (lower value = more similar)
/// Value of 0.2 means frames are approximately 80% similar

/// Number of consecutive good matches required to establish synchronization
pub const CONSECUTIVE_MATCHES_TO_LOCK: u32 = 1;

/// Number of consecutive poor matches before considering sync lost
pub const CONSECUTIVE_MISMATCHES_TO_RECOVER: u32 = 30;

pub const BUFFERING_START_FRAMES_UNTIL_PLAYBACK: usize = 30; 
const MAX_BUFFERING_TIME: Duration = Duration::from_secs(30); // Maximum time to wait for buffer

const RECOVERY_GRACE_PERIOD: usize = 5;      // Frames to wait before trying to find new similarity matches
const RECOVERY_MATCH_THRESHOLD: f64 = 0.35;   // More lenient similarity threshold during recovery
const RECOVERY_MAX_ATTEMPTS: usize = 1;       // How many consecutive frames to check before accepting new offset


use std::collections::{BTreeSet, BTreeMap};
use futures::stream::StreamExt;

use tokio::sync::Semaphore;

use std::io::BufRead;
use walkdir::WalkDir;
use tokio::join;
use tokio::spawn;

/// One global pool → one permit per concurrent VMAF job
static VMAF_SLOTS: Lazy<Arc<Semaphore>> = Lazy::new(|| {
    Arc::new(Semaphore::const_new(MAX_PARALLEL_VMAF))
});
use futures::stream::FuturesUnordered;


use futures::future::join_all;

// static METRIC_SLOTS: Lazy<Semaphore> = Lazy::new(|| Semaphore::const_new(30)); // ≤4 frames in flight
use asynchronix::model::Context;
use crossbeam::channel::{bounded, unbounded, Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::collections::HashMap;
use std::io::{Read, Write};
#[allow(unused_imports)]
#[allow(dead_code)]
use std::process::{Child, Command, Stdio};
use colored::Colorize;
use std::sync::{Arc, Mutex};
use ffmpeg_sidecar::command::FfmpegCommand;
use rand::seq::IteratorRandom;
use std::io::BufReader;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use anyhow::Result;
use async_std::task;
use minifb::{Key, Scale, Window, WindowOptions};
use rand::Rng;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::{ VecDeque};
use std::error::Error;
use std::fs::File;
use std::hash::Hash;
use std::io::{ BufWriter};
use std::process::{ChildStdin, ChildStdout};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use std::cell::RefCell;
use std::fmt::{self, Debug};
use std::{
    collections::{HashSet,},
    io,
    marker::PhantomData,
    mem,
    // net::{TcpListener, UdpSocket},
}; 



use std::net::IpAddr;
// use tokio::sync::Semaphore;
use std::result::Result::Ok;
use tai_time::TaiTime;
use std::{
    fs::{ OpenOptions},
    path::PathBuf,
};
use csv::Writer;
use rand::distributions::Uniform;
use rand::rngs::StdRng;
use rand::SeedableRng;
use image::{ImageBuffer, Rgb};
use image_compare::rgb_hybrid_compare;
use regex::Regex;
use std::net::Ipv4Addr;
use std::thread_local;
use std::path::Path;
use core::f64;
use glam::{Quat, Vec3};
use once_cell::sync::Lazy;
use std::time::SystemTime;
use std::{ vec};
use dashmap::DashMap;
use asynchronix::model::{ Model};
use asynchronix::ports::Output;
use std::cmp::{self, max};
use std::f64::consts::PI;
use std::future::Future;
use std::sync::RwLock;

use lazy_static::lazy_static;

#[derive(Debug, Clone)]
struct FrameData {
    path: String,
    timestamp_ms: f64,
    frame_number: u64,
}

struct FrameGroup {
    _frames: Vec<FrameData>,
}

#[derive(serde::Serialize, Deserialize)]
struct FrameMetrics {
    frame_number: u64,
    timestamp_ms: f64,
    vmaf: f64,
    psnr: f64,
    ssim: f64,
}


#[macro_export]
macro_rules! print_greennn {
    ($fmt:expr, $($arg:tt)*) => {
        let msg = format!($fmt, $($arg)*);
        println!("{}", DebugColor::ForestGreen.to_background_fn()(msg));
    };
}
#[macro_export]
macro_rules! print_reds {
    ($fmt:expr, $($arg:tt)*) => {
        let msg = format!($fmt, $($arg)*);
        println!("{}", DebugColor::Red.to_background_fn()(msg));
    };
}
#[macro_export]
macro_rules! print_purples {
    ($fmt:expr, $($arg:tt)*) => {
        let msg = format!($fmt, $($arg)*);
        println!("{}", DebugColor::Purple.to_background_fn()(msg));
    };
}



#[derive(Clone)]
struct MetricsLogger {
    writer: Arc<Mutex<csv::Writer<File>>>,
    name_folder: String,
    name_file_w_path: String, 
}

impl MetricsLogger {
    fn new(ip: IpAddr, name_folder: &str) -> Result<Self> {
        let mut value = 99;
        if let IpAddr::V4(ip4) = ip {
            let octets = ip4.octets();
            value = octets[2]
        }
        // create it (and any missing parents) if it doesn't exist
        let dir =format!("Results/{}", name_folder); 
        std::fs::create_dir_all(&dir)?;
        let filename = format!( "Results/{}/VMAF_metrics_{}.csv", name_folder, value); 
        let file = File::create(filename.clone())?;
        let writer = csv::Writer::from_writer(file);
        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            name_folder: name_folder.to_string(),
            name_file_w_path: filename, 
        })
    }
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
            guard.flush()?;          // just to be safe
        }
        // 3/ overwrite the file
        let mut wtr = csv::Writer::from_path(&*self.name_file_w_path)?;
        // wtr.write_record(&["frame_number","timestamp_ms","vmaf","psnr","ssim"])?;
        for r in rows { wtr.serialize(r)?; }
        wtr.flush()?;
        Ok(())
    }

    pub fn new_for_trace(
        scenario: &str,
        trace_idx: usize,
        two_encoders: bool, 
    ) -> Result<Self> {
        let dir = format!("Results/{}", scenario);
        std::fs::create_dir_all(&dir)?;

        let strrrrr = if two_encoders{
                "bitrate"
            }
            else{
                "loss"
            }; 


        // let filename = format!( "Results/{}/VMAF_metrics_{}_{}.csv", name_folder, strrrrr, value); 


        let path = format!("{}/VMAF_metrics_{}_{}.csv", dir, strrrrr,  trace_idx);
        let file = std::fs::File::create(&path.clone().to_string())?;
        Ok(Self {
            writer: Arc::new(std::sync::Mutex::new(csv::Writer::from_writer(file))),
            name_folder: scenario.to_string(),
            name_file_w_path: path, 
        })
    }
    /// *The heavy ffmpeg work happens in a dedicated thread;* the caller just awaits
    /// the semaphore, spawns, and returns immediately.
    pub async fn process_frame_buffers(
        &self,
        frame_number: u64,
        timestamp_ms: f64,
        ref_buf: Vec<u8>,        // own the data
        lossy_buf: Vec<u8>,
        ip_client: IpAddr,
    ) -> anyhow::Result<()> {
        // 1️⃣ back‑pressure – wait until a slot is free
        // let _permit = VMAF_SLOTS.acquire().await.unwrap();

        // 2️⃣ clone `self` (the logger) for the blocking thread
        let logger = self.clone();

        // 3️⃣ move everything into a blocking worker thread
        tokio::task::spawn_blocking(move || {
            // a) create a per‑frame temp dir
            let tmp = tempfile::TempDir::new().expect("create TempDir");

            let ref_path   = tmp.path().join("ref.rgb");
            let lossy_path = tmp.path().join("lossy.rgb");

            std::fs::write(&ref_path,   &ref_buf).expect("write ref");
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
        .await?;       // propagate panic / JoinError

        Ok(())
    }

       
      // ───────────────────── helper for parsing metrics ───────
    fn extract_metric(&self, path: &std::path::Path, key: &str) -> Option<f64> {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|content| {
                // Find the line containing the key
                content.lines()
                    .find(|line| line.contains(key))
                    .and_then(|line| {
                        // Extract the value after the key
                        let after_key = line.split(key).nth(1)?;
                        // Find the first number in the remaining text
                        after_key.split_whitespace()
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
        let metrics_dir = tmp.path()
            .join(&self.name_folder)
            .join("Sink_for_video");
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
        let raw = std::fs::read_to_string(&vmaf_json)
            .expect("read vmaf JSON");
        let j: serde_json::Value =
            serde_json::from_str(&raw).expect("parse vmaf JSON");

        // pooled_metrics now includes:
        //  • vmaf.mean
        //  • float_ssim.mean
        // println!("METRICS: \n{j}"); 

        let vmaf_score = j["pooled_metrics"]["vmaf"]["mean"]
            .as_f64().unwrap_or(0.0);
        
        let ssim_score = j["pooled_metrics"]["float_ssim"]["mean"]
            .as_f64().unwrap_or(0.0);

        // ─────────── log & emit ───────────
        print_greennn!(
            "T:{:.3} [{}] | Frame {} : VMAF {:.2}, SSIM {:.4}",
            timestamp_ms, ip_client, frame_number,
            vmaf_score,  ssim_score
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

use std::time::Instant; 

#[derive(Clone)]
pub struct NalUnit {
    pub nal_type: u8,
    pub data: Vec<u8>,
    pub is_keyframe: bool,
}
pub struct SynStateOld {
    id_offset:       i32,                      // LOW id + offset  → REF id
    recent_low:      VecDeque<(u32, Vec<u8>)>, // (id, rgb)
    recent_ref:      VecDeque<(u32, Vec<u8>)>,
    sim_history:     VecDeque<f64>,            // store VMAF-ish similarity
    desync_since:    Option<Instant>,          // first time stddev > 20
}

impl SynStateOld {
    pub fn new() -> Self {
        Self {
            id_offset: 0,
            recent_low: VecDeque::with_capacity(RESYNC_BUFFER),
            recent_ref: VecDeque::with_capacity(RESYNC_BUFFER),
            sim_history: VecDeque::with_capacity(SIM_HISTORY),
            desync_since: None,
        }
    }

    /// Call **once per _valid_ pair** (you already have `sim` in [0, 1]).
    /// `sim` is converted to a VMAF-style scale (0–100) internally.
    pub fn on_pair(&mut self, id_low: u32, id_ref: u32, sim: f64) {
        // ---- 1.  update similarity history --------------------------------
        let vmaf_like = sim * 100.0;                   // 0–1  →  0–100
        if self.sim_history.len() == SIM_HISTORY { self.sim_history.pop_front(); }
        self.sim_history.push_back(vmaf_like);

        // ---- 2.  test stddev ----------------------------------------------
        if self.sim_history.len() == SIM_HISTORY {
            let mean = self.sim_history.iter().copied().sum::<f64>() / SIM_HISTORY as f64;
            let var  = self.sim_history.iter().map(|v| (v-mean)*(v-mean)).sum::<f64>() / SIM_HISTORY as f64;
            let stddev = var.sqrt();

            print_prettyy!(DebugColor::DarkBlue, "STD DEV WINDOW = {:.3}", stddev);  

            if stddev >= DESYNC_STD_DEV {
                // either start or continue the desync timer
                self.desync_since.get_or_insert_with(Instant::now);
            } else {
                // noise back to normal – reset timer
                self.desync_since = None;
            }
        }

        // ---- 3.  if we have been desynced for ≥ 2 s → try realign ---------
        if let Some(since) = self.desync_since {
            if since.elapsed() >= DESYNC_HIST_WINDOW {
                print_reds!("Trying to realign! D: {}", since.elapsed().as_secs_f32()); 
                if let Some(delta) = self.find_new_offset() {
                    println!("[RESYNC] detected offset {delta:+} (low+δ → ref)");
                    self.id_offset = delta;
                    // flush history so we don’t instantly trigger again
                    self.sim_history.clear();
                }
                self.desync_since = None;             // restart the detector
            }
        }
    }

    /// Keep the ring-buffers fresh (call for *every* decoded frame).
    pub fn on_frame_in_low(&mut self, id: u32, rgb: Vec<u8>) {
        Self::push_with_cap(&mut self.recent_low, (id, rgb));
    }
    pub fn on_frame_in_ref(&mut self, id: u32, rgb: Vec<u8>) {
        Self::push_with_cap(&mut self.recent_ref, (id, rgb));
    }

    /// Current mapping:  *deliver LOW id + `offset()` when looking inside REF*
    #[inline] pub fn offset(&self) -> i32 { self.id_offset }

    //──────────────────────── helpers ──────────────────────────────────────
    fn push_with_cap<T>(dq: &mut VecDeque<T>, v: T) {
        if dq.len() == RESYNC_BUFFER { dq.pop_front(); }
        dq.push_back(v);
    }

    /// brute-force search in the 20×20 buffers – 400 similarities max
    fn find_new_offset(&self) -> Option<i32> {
        let mut best = (0_i32, 0.0_f64);            // (δ, score)

        for (id_l, rgb_l) in &self.recent_low {
            for (id_r, rgb_r) in &self.recent_ref {
                let s = similarity_rgb_hybrid(rgb_l, rgb_r, WIDTH_ENCODER, HEIGHT_ENCODER);
                
                if s > best.1 {
                    best = ((*id_r as i32) - (*id_l as i32), s);
                }
            }
        }
        if best.1 > 0.8 { Some(best.0) } else { None }   // need “good enough” match
    }


    fn find_new_offset_window(&self) -> Option<i32> {

        /// How many consecutive frames we demand before trusting a δ
        const WIN_LEN: usize = 8;
        /// Maximum window-average similarity we still call “good”
        const AVG_OK: f64 = 0.85;

        // nothing to do if one side is empty
        if self.recent_low.is_empty() || self.recent_ref.is_empty() {
            return None;
        }

        // quick O(1) lookup for REF frames
        let ref_map: HashMap<u32, &Vec<u8>> =
            self.recent_ref.iter().map(|(id, rgb)| (*id, rgb)).collect();

        // we will see the same δ many times – keep only first encounter
        let mut seen: HashSet<i32> = HashSet::new();
        let mut best: Option<(i32, f64)> = None;                 // (δ, best_avg)

        for (id_l0, _rgb_l0) in &self.recent_low {
            for (id_r0, _rgb_r0) in &self.recent_ref {
                let delta = *id_r0 as i32 - *id_l0 as i32;
                if !seen.insert(delta) {
                    continue; // already evaluated this δ
                }

                // ---- score this δ -------------------------------------------------
                let mut win: VecDeque<f64> = VecDeque::with_capacity(WIN_LEN);
                let mut best_avg_for_delta = f64::MIN;

                let mut prev_id_l = None::<u32>;

                for (id_l, rgb_l) in &self.recent_low {
                    let id_r = (*id_l as i32 + delta) as u32;

                    // do we have the matching REF frame?
                    if let Some(rgb_r) = ref_map.get(&id_r) {
                        // make sure frames are consecutive (robust against drops)
                        if let Some(p) = prev_id_l {
                            if *id_l != p + 1 {
                                win.clear();          // gap → break the run
                            }
                        }
                        prev_id_l = Some(*id_l);

                        // push similarity into the sliding window
                        let s = similarity_rgb_hybrid(
                            rgb_l,
                            rgb_r,
                            WIDTH_ENCODER,
                            HEIGHT_ENCODER,
                        );
                        if win.len() == WIN_LEN {
                            win.pop_front();
                        }
                        win.push_back(s);

                        if win.len() == WIN_LEN {
                            let avg = win.iter().copied().sum::<f64>() / WIN_LEN as f64;
                            best_avg_for_delta = best_avg_for_delta.max(avg);
                        }
                    } else {
                        win.clear();                  // missing pair → break the run
                        prev_id_l = None;
                    }
                }

                // keep this δ only if it ever achieved WIN_LEN consecutive good pairs
                if best_avg_for_delta > AVG_OK {
                    match best {
                        None => best = Some((delta, best_avg_for_delta)),
                        Some((_, b)) if best_avg_for_delta < b => {
                            best = Some((delta, best_avg_for_delta))
                        }
                        _ => {}
                    }
                }
            }
        }

        best.map(|(d, _)| d)
    }
}


struct RingBuffer {
    buf: VecDeque<f32>,
    capacity: usize,
}

impl RingBuffer {
    fn new(capacity: usize) -> Self {
        RingBuffer {
            buf: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn push(&mut self, value: f32) {
        if self.buf.len() == self.capacity {
            self.buf.pop_front(); // remove oldest
        }
        self.buf.push_back(value);
    }

    fn std_dev(&self) -> f32 {
        let n = self.buf.len();
        if n == 0 {
            return 0.0;
        }

        let mean = self.buf.iter().copied().sum::<f32>() / n as f32;
        let var = self.buf
            .iter()
            .map(|x| {
                let diff = x - mean;
                diff * diff
            })
            .sum::<f32>()
            / n as f32;

        var.sqrt()
    }


    fn as_vec(&self) -> Vec<f32> {
        self.buf.iter().copied().collect()
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

#[derive(Debug, Clone)]
struct FrameInfo {
    id:       u32,
    lost:     bool,
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
    id_queue: VecDeque<u32>,

    pending_param_sets: Option<(Option<Vec<u8>>, Option<Vec<u8>>, Option<Vec<u8>>)>,
}
#[allow(non_camel_case_types, unused)]
impl HevcDecoder {
    pub fn new(framerate: u32, width: u32, height: u32, decoder_str: &str) -> Self {
        let frame_size = (width as usize) * (height as usize) * 3;

        let decoder_string = decoder_str.to_string();

        let mut child = FfmpegCommand::new()
            .hwaccel("cuda")

            .args(&["-skip_frame", "none",              // NO SKIPPING UPON FRAME LOSS!
                    "-skip_loop_filter", "none",
                    "-skip_idct", "none"])
            .args(&["-err_detect", "aggressive",])
            .args(&["-fflags", "+discardcorrupt"])

            .args(&["-f", "hevc", "-i", "-"])
            .args(&["-vsync", "0"])
            // .args(&["-vf", &format!("fps={}", framerate)])
            
            .args(&["-pix_fmt", "rgb24"])
            // .args(&["-tune", "zerolatency"])
            // .args(&["-bf", "0"])    // disable use of B-frames
            // .args(&["-preset", "ultrafast"])
            // .args(&["-fps_mode", "passthrough"])
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
            max_buffered_frames: 30,
            min_buffered_frames: 5, 

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
            processing_semaphore: Arc::new(Semaphore::new(10)),  
            decoded_frame_counter: 0, 
            id_queue: VecDeque::new(), 
            pending_param_sets: None, 
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
        // if depth == 0 {
        //     if let Some(vps_data) = &vps {
        //         println!(
        //             "{} Injecting VPS ({} bytes)",
        //             self.decoder_string,
        //             vps_data.len()
        //         );
        //     }

        //     if let Some(sps_data) = &sps {
        //         println!(
        //             "{} Injecting SPS ({} bytes)",
        //             self.decoder_string,
        //             sps_data.len()
        //         );
        //     }

        //     if let Some(pps_data) = &pps {
        //         println!(
        //             "{} Injecting PPS ({} bytes)",
        //             self.decoder_string,
        //             pps_data.len()
        //         );
        //     }
        // }

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
            
            // self.decoded_frames.clear();
            // self.id_queue.clear();
            // self.frame_buffer.clear();
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

    pub async fn process_packet(&mut self, packet: Vec<u8>, id: u32) {
        // Track if this packet contains a parameter set
        let mut has_parameter_update = false;

        // Extract parameter sets from this packet
        let (vps, sps, pps) = self.extract_complete_parameter_sets(&packet);
        let has_param_sets = vps.is_some() || sps.is_some() || pps.is_some();

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

            // if let Some((vps, sps, pps)) = self.pending_param_sets.take() {
            //     self.inject_parameter_sets(vps, sps, pps);
            //     print_prettyy!(DebugColor::Teal, "{} 🔁 Injected pending parameter sets at keyframe", self.decoder_string);
            // }

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

        let _permit = self.processing_semaphore.acquire().await;
        
        // Add data to the parser
        self.parser.add_data(&packet);

        // Extract frames from the parser and buffer them
        let frames = self.parser.get_frames();
        let mut pushed = false; 

        
        for frame in frames {
            self.frame_buffer.push_back(frame);
                self.id_queue.push_back(id);
                pushed = true; 
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
        // self.id_queue.push_back(id);

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
        let max_processing_time = Duration::from_millis(100);

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
                    if self.decoded_frames.len() >= self.min_buffered_frames {
                        // we’re primed now, stop draining further
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

    pub fn next_decoded_frame(&mut self) -> Option<(Vec<u8>, u32, Instant)> {
        let ts = Instant::now();
        let _ = self.process_decoded_frames();

        if let Some(frame) = self.decoded_frames.pop_front() {
            let id = self.id_queue
                .pop_front()
                .expect("decoder out‐of‐sync: id_queue empty");
            Some((frame, id, ts))
        } else {
            None
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

// use super::alvr_packets::NetworkStatisticsPacket;

pub struct ChunkedHevcEncoder {
    input: String,
    width: u32,
    height: u32,
    bitrate: String,
    chunk_duration: f64,
    current_offset: f64,
    frame_tx: Sender<Vec<u8>>,
    frame_rx: Receiver<Vec<u8>>,

    frame_queue: VecDeque<Vec<u8>>,
    parser: HevcParser,
    encoder_str: String,
    intra_refresh: bool, 


}
#[allow(unused)]
impl ChunkedHevcEncoder {
    pub fn new(
        input: &str,
        width: u32,
        height: u32,
        bitrate: &str,
        chunk_duration: f64,
        string: String,
        offset_video: f64,
        intra_refresh: bool, 
    ) -> Self {
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
            frame_queue: VecDeque::new(),
            parser: HevcParser::new(),
            encoder_str: string.clone(),
            intra_refresh, 
        }
    }

    pub fn clear_parser(&mut self) {
        self.parser.buffer.clear();
    }

    /// Continuously spawn ffmpeg processes to produce video chunks.
    /// Each process is configured to start at the current_offset and run for chunk_duration seconds.
    /// As data is read from ffmpeg’s stdout, it is fed to a HevcParser which extracts complete frames.
    /// Each complete frame is sent via the async channel.

    pub async fn start_chunking(&mut self, bitrate_mbps: f32, idr: u32, ) {
        let bitrate_adjusted_fps = bitrate_mbps * FRAMERATE_WINDOWS as f32 / INITIAL_FRAMERATE_FPS;
        // Since the encoded video samples are 60fps, we thus adjust bitrate to match with the actual second units.

        self.bitrate = format!("{:.2}M", bitrate_adjusted_fps);

        println!(
            "{} CHUNKING with bitrate {} Mbps",
            self.encoder_str, bitrate_mbps
        );
        self.parser.buffer.clear();
        let mut command = FfmpegCommand::new();
        
        if self.intra_refresh {
        
            command
                .hwaccel("cuda")
                .args(&["-ss", &self.current_offset.to_string()])
                .args(&["-t", &self.chunk_duration.to_string()])
                .args(&["-re"]) // read at real-time speed
                .input(&self.input)
                .args(&[
                    "-vf",
                    &format!(
                        "scale={}:{}:force_original_aspect_ratio=disable,format=yuv420p",
                        self.width, self.height
                    ),
                ])
                .args(&["-c:v", "hevc_nvenc"])
                .args(&["-preset", "ultrafast"])
                .args(&["-rc", "cbr"])
                .args(&["-bf", "0"])    // disable use of B-frames
                .args(&["-b:v", &self.bitrate, "-maxrate", &self.bitrate])
                .args(&["-rc-lookahead", "0"])
                .args(&["-g", "0"]) // Disable GOP, intra-refresh instead
                .args(&["-intra-refresh", "1"]) // Enable intra-refresh coding
                .args(&["-movflags", "+frag_keyframe+empty_moov"])
                .args(&["-flush_packets", "1"])
                .args(&["-bsf:v", "hevc_mp4toannexb"])
                .args(&["-an"])
                .args(&["-f", "hevc", "-"]); // output raw HEVC
       
        } else {

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
                .args(&["-preset", "ultrafast"])
                // .args(&["-tune", "zerolatency"])

                .args(&["-rc", "cbr"])
                .args(&["-bf", "0"])    // disable use of B-frames
             
                .args(&["-fps_mode", "passthrough"])
                .args(&["-b:v", &self.bitrate, "-maxrate", &self.bitrate])
                .args(&["-rc-lookahead", "0"])
                .args(&["-g", &format!("{:.0}", idr)])
                .args(&["-movflags", "+frag_keyframe+empty_moov"])
                .args(&["-flush_packets", "1"])
                .args(&["-bsf:v", "hevc_mp4toannexb"])
                .args(&["-an"])
                .args(&["-f", "hevc", "-"]); // output raw HEVC
        }

        // Spawn the ffmpeg process for this chunk.
        let mut child = command.spawn().unwrap();
        let stdout = child.take_stdout().unwrap();
        let mut reader = BufReader::new(stdout);

        // if let Some(stderr) = child.take_stderr() {
        //     let mut err_reader = std::io::BufReader::new(stderr);
        //     std::thread::spawn(move || {
        //         for line in err_reader.lines() {
        //             match line {
        //                 Ok(l) => println!("ffmpeg stderr: {}", l),
        //                 Err(e) => {
        //                     eprintln!("Error reading ffmpeg stderr: {}", e);
        //                     break;
        //                 }
        //             }
        //         }
        //     });
        // }

        // let mut parser = HevcParser::new();
        let mut buf = [0u8; 4096];

        loop {
            match reader.read(&mut buf) {
                Ok(0) => break, // end of chunk
                Ok(n) => {
                    self.parser.add_data(&buf[..n]);
                    // Extract complete frames and send them on the channel.
                    let frames = self.parser.get_frames();
                    for frame in frames {
                        if let Err(e) = self.frame_tx.send(frame) {
                            eprintln!("{} Error sending frame: {}", e, self.encoder_str,);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("{} Error reading ffmpeg chunk: {}", e, self.encoder_str,);
                    break;
                }
            }
        }
        // let _ = child.wait();
        let _ = child.wait();

        // Update offset for the next chunk.
        self.current_offset += self.chunk_duration;
        // Add a safety check to clear parser buffer if it gets too large
        if self.parser.buffer.len() > 100_000_000_00 {
            // 100MB limit
            println!(
                "{} Parser buffer getting too large ({}), clearing",
                self.parser.buffer.len(),
                self.encoder_str,
            );
            self.parser.buffer.clear();
        }
    }
    pub async fn next_frame(&mut self) -> Option<Vec<u8>> {
        // First try parser's frames - keeping original behavior
        let extracted_frames = self.parser.get_frames();
        if !extracted_frames.is_empty() {
            println!(
                "{} Extracted {} frames from parser buffer, size {}",
                self.encoder_str,
                extracted_frames.len(),
                self.parser.buffer.len()
            );

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
        if let Ok(frame) = self.frame_rx.recv_timeout(Duration::from_millis(10)) {
            return Some(frame);
        }

        None
    }
}

use csv::StringRecord;


fn make_encoder_task(
    tag: usize,
    bitrate_mbps: f32,
    trace: Arc<Vec<FrameInfo>>,
    tx: Sender<(usize, u32, Vec<u8>)>,
    video_path: String,
    offset_video: f64,
    simulate_loss: bool,
    idr_freq: u32, 
    intra_refresh: bool, 

) {
    task::spawn(async move {
        // 1️⃣ Create your encoder
        let mut enc = ChunkedHevcEncoder::new(
            &video_path,
            WIDTH_ENCODER as u32,
            HEIGHT_ENCODER as u32,
            &format!("{bitrate_mbps}M"),
            1.0,
            format!("ENC{}M", bitrate_mbps),
            offset_video,
            intra_refresh, 
        );

        // 2️⃣ Iterate until we’ve produced every ID in the trace
        let mut produced = 0;
        while produced < trace.len() {
            // (re)fill the encoder’s internal queue
            enc.start_chunking(bitrate_mbps, idr_freq).await;
            // async_std::task::sleep(Duration::from_millis(6000)).await;
            // drain all frames this chunk produced (but never overrun our trace)
            while produced < trace.len() {
                // try to grab the next packet
                if let Some(pkt) = enc.next_frame().await {
                    let info = &trace[produced];
                    produced += 1;

                    // async_std::task::sleep(Duration::from_millis(600)).await;

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
        // when this async block returns, `enc` is dropped and ffmpeg subprocesses exit
    });
}
/* ---------------------------  MAIN  ---------------------------------- */
/* convert “RGBRGB…” byte slice to Vec<u32> suitable for minifb */
fn rgb_to_u32(src: &[u8]) -> Vec<u32> {
    src.chunks_exact(3)
       .map(|px| ((px[0] as u32) << 16) | ((px[1] as u32) << 8) | px[2] as u32)
       .collect()
}

/* nearest-neighbour down-scale to (w_out,h_out) */
fn resize_nn(buf: &[u32], w_in: usize, h_in: usize,
             w_out: usize, h_out: usize) -> Vec<u32>
{
    let mut out = vec![0u32; w_out * h_out];
    for y in 0..h_out {
        let src_y = y * h_in / h_out;
        for x in 0..w_out {
            let src_x = x * w_in / w_out;
            out[y*w_out + x] = buf[src_y*w_in + src_x];
        }
    }
    out
}


/* similarity in [0.0,1.0]  (1.0 ⇒ perfect match) */
fn similarity_rgb_hybrid(a: &[u8], b: &[u8], w: usize, h: usize) -> f64 {
    let img1 = ImageBuffer::<Rgb<u8>, _>::from_raw(w as u32, h as u32, a.to_vec()).unwrap();
    let img2 = ImageBuffer::<Rgb<u8>, _>::from_raw(w as u32, h as u32, b.to_vec()).unwrap();
    1.0 - rgb_hybrid_compare(&img1, &img2).unwrap().score
}
type Packet = (usize, u32, Vec<u8>);

fn make_low_task(
    trace: Arc<Vec<FrameInfo>>,
    mut rx_raw: Receiver<Vec<u8>>,
    tx: Sender<Packet>,
) {
    task::spawn(async move {
        for row in trace.iter() {
            // pull one packet for this CSV row
            let pkt = loop { if let Ok(p) = rx_raw.try_recv() { break p } };
            if !row.lost {
                tx.send((0, row.id, pkt)).ok();      // only good frames go out
            }
        }
    });
}
fn make_ref_task(
    trace: Arc<Vec<FrameInfo>>,
    mut rx_raw: Receiver<Vec<u8>>,
    tx: Sender<Packet>,
) {
    task::spawn(async move {
        for row in trace.iter() {
            let pkt = loop { if let Ok(p) = rx_raw.try_recv() { break p } };
            tx.send((1, row.id, pkt)).ok();          // never skipped
        }
    });
}


/* draw the two half-frames *plus* the ID text */
fn draw_pair(
    window:  &mut Window,
    rgb_l:   &[u8],
    rgb_r:   &[u8],
    scenario: &str, 
    id:      u32,
    t:       f64, 
) -> Result<()> {
    const W: usize = WIDTH_ENCODER;
    const H: usize = HEIGHT_ENCODER;
    // const SCALE: f64 = 0.28;
    let sw = (W as f64 * SCALE) as usize;
    let sh = (H as f64 * SCALE) as usize;
    let ww = sw * 2 + 10;

    let left  = resize_nn(&rgb_to_u32(rgb_l),  W, H, sw, sh);
    let right = resize_nn(&rgb_to_u32(rgb_r),  W, H, sw, sh);

    let mut buf = vec![0u32; ww * sh];
    for y in 0..sh {
        let dst = y * ww;
        buf[dst..dst+sw].copy_from_slice(&left[y*sw..(y+1)*sw]);
        buf[dst+sw+10..dst+sw+10+sw].copy_from_slice(&right[y*sw..(y+1)*sw]);
    }

    let y_lbl = sh - 40;
    render_text(&mut buf, &format!("#{}", id), 10,           y_lbl, ww, 0xFFAA00, 2);
    render_text(&mut buf, &format!("#{}", id), sw + 20,      y_lbl, ww, 0xFFAA00, 2);

    window.set_title(&format!("T: {:6.4} ID {} | Scenario: {scenario}", t, id,));
    window.update_with_buffer(&buf, ww, sh)?;
    Ok(())
}

#[derive(Clone)]
struct FrameBuf { // structure for having synthetic frames replacing losses. Idea is to filter them out of analysis later, but this way we keep both decoders synced as best as we can. 
    rgb: Vec<u8>,
    synthetic: bool,   // true ⇢ this is a repeated / “fake” frame
}

// 1) Update FramePair to remember both input IDs
pub struct FramePair {
    pub decoded:       Option<Vec<u32>>, // low-bitrate pixels
    pub reference:     Option<Vec<u32>>, // high-bitrate pixels
    pub decoded_raw:   Option<Vec<u8>>,  // low-bitrate raw RGB24
    pub reference_raw: Option<Vec<u8>>,  // high-bitrate raw RGB24
    pub frame_id:      usize,            // internal pair counter
    pub regular_id:    usize,            // original “low” frame id
    pub max_id:        usize,            // original “high” frame id
}



pub struct SynchronizedDecoder {
    regular_decoder: HevcDecoder,
    max_decoder: HevcDecoder,
    output_queue: VecDeque<FramePair_old>,
    throttle_semaphore: Arc<Semaphore>,
    next_frame_id: Arc<AtomicUsize>,
    
    // Sync state
    state: SyncState,
    offset: Option<i64>,
    
    // Counters for state transitions
    match_counter: usize,
    mismatch_counter: usize,
    
    // Buffering
    buffering_active: bool,
    buffering_start_time: Option<Instant>,
    
    client_ip: IpAddr,

    recovery_frame_counter: usize,             // Counts frames processed in recovery mode
    recovery_last_offset: Option<i64>,         // Stores the offset when entering recovery mode
    recovery_consecutive_matches: usize,       // Counts consecutive good matches with a new offset
    recovery_candidate_offset: Option<i64>,    // Potential new offset during recovery

    recovery_base_offset: i64, 
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SyncState {
    Seeking,    // Looking for initial synchronization
    Locked,     // Stable synchronization with known offset
    Recovering, // Lost sync, trying to find it again
}

// Configuration constants
const MAX_OUTPUT_QUEUE_LEN: usize = 60;
const MATCHES_NEEDED_TO_LOCK: usize = 1;
const MISMATCHES_TO_RECOVERY: usize = 5;
const MAX_PEEK_COUNT: usize = 50;
const BUFFERING_THRESHOLD: usize = 50;


#[derive(Debug)]
pub struct FramePair_old {
    decoded: Option<Vec<u32>>,
    reference: Option<Vec<u32>>,
    decoded_raw: Option<Vec<u8>>,
    reference_raw: Option<Vec<u8>>,
    frame_id: usize,
}


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
            output_queue: VecDeque::with_capacity(MAX_OUTPUT_QUEUE_LEN),
            throttle_semaphore,
            next_frame_id: Arc::new(AtomicUsize::new(0)),
            state: SyncState::Seeking,
            offset: None,
            match_counter: 0,
            mismatch_counter: 0,
            buffering_active: true,
            buffering_start_time: Some(Instant::now()),
            client_ip,

            recovery_candidate_offset: None,
            recovery_frame_counter: 0,
            recovery_last_offset: None, 
            recovery_consecutive_matches: 0 , 

            recovery_base_offset: 0, 
        }
    }
    pub async fn process_raw_packet(
        &mut self,
        tag: usize,
        packet: Vec<u8>,
        id: u32,
    ) {
        if tag == 0 {
            self.regular_decoder.process_packet(packet, id).await;
        } else {
            self.max_decoder.process_packet(packet, id).await;
        }
        self.synchronize_frame_buffers();
    }
    
    pub async fn process_packets(&mut self, regular_frame: Option<Vec<u8>>, max_frame: Option<Vec<u8>>, id_reg: u32, id_max: u32) {
        // Process incoming packets
        if let Some(reg_data) = regular_frame.clone() {
            self.regular_decoder.process_packet(reg_data, id_reg).await;
        }
        if let Some(max_data) = max_frame.clone() {
            self.max_decoder.process_packet(max_data, id_max).await;
        }
        if !regular_frame.is_none() && !max_frame.is_none(){
            self.synchronize_frame_buffers();

        }
        // Try to synchronize frames
    }

    pub fn notify_regular_loss(&mut self, lost_count: usize) {
        if let Some(off) = self.offset {
            self.offset = Some(off - lost_count as i64);
            // Also clear any pending mismatch escalation
            self.mismatch_counter = 0;
            // self.recovery_candidate_offset = None;
            // self.recovery_consecutive_matches = 0;
        }
    }



    
    fn check_buffering_status(&mut self) -> bool {
        if !self.buffering_active {
            return false;
        }
        
        let buffer_size = self.output_queue.len();
        let elapsed = self.buffering_start_time
            .map(|t| t.elapsed())
            .unwrap_or(Duration::ZERO);
            
        if buffer_size >= BUFFERING_THRESHOLD || elapsed >= MAX_BUFFERING_TIME {
            println!("✅ Buffer ready! Size: {}/{}, Time elapsed: {:.2}s", 
                buffer_size, BUFFERING_THRESHOLD, elapsed.as_secs_f32());
            self.buffering_active = false;
            return false;
        }
        
        return true;
    }
    
    fn can_synchronize(&self) -> bool {
        // Need sufficient frames in both decoders and space in output queue
        return self.regular_decoder.available_frames() >= 20 &&
               self.max_decoder.available_frames() >= 1 &&
               self.output_queue.len() < MAX_OUTPUT_QUEUE_LEN;
    }
    
    fn synchronize_frame_buffers(&mut self) {
        // Process any decoded frames
        self.regular_decoder.process_decoded_frames();
        self.max_decoder.process_decoded_frames();
        
        // Main synchronization loop
        while self.can_synchronize() {
            // Get the next max frame
            if let Some((max_raw, max_pixels, max_id)) = self.consume_max_frame() {
                // Peek at regular frames to find matches
                let regular_candidates = if self.state == SyncState::Locked
                {
                    self.peek_regular_frames(2)
                } else{
                    self.peek_regular_frames(MAX_PEEK_COUNT)
                };

                print_prettyy!( DebugColor::Yellow, "Peeked {} regular frames, {} available ", regular_candidates.len(), self.regular_decoder.decoded_frames.len()); 
                if regular_candidates.is_empty() {
                    continue; // No regular frames available
                }
                
                // Find best match based on current state
                let match_result = self.find_best_match(&max_raw, max_id, &regular_candidates);
                
                // Handle the match result
                self.process_match(match_result, max_id, max_raw, max_pixels);
            } else {
                break; // No more max frames
            }
        }
    }
    
    fn find_best_match(
        &self,
        max_raw: &[u8],
        max_id: usize,
        candidates: &[(Vec<u8>, Vec<u32>, usize)]
    ) -> Option<(usize, f64, usize)> {
        match self.state {
            SyncState::Seeking | SyncState::Recovering => {
                // Find the best similarity match across all candidates
                let mut best_idx = 0;
                let mut best_sim = f64::MAX;
                let mut best_reg_id = 0;
                
                for (idx, (reg_raw, _, reg_id)) in candidates.iter().enumerate() {
                    let sim = compute_enhanced_frame_similarity(reg_raw, max_raw, WIDTH_ENCODER, HEIGHT_ENCODER);
                    if sim < best_sim {
                        best_sim = sim;
                        best_idx = idx;
                        best_reg_id = *reg_id;
                    }
                }
                // println!("[{:?}] best_sim {:.3}", self.state, best_sim);
                
                if best_sim <= ACCEPTABLE_SIMILARITY_THRESHOLD {
                    print_greennn!("[{:?}] {} has best_sim  ({:.3}), better than threshold {}", self.state, best_idx,best_sim, ACCEPTABLE_SIMILARITY_THRESHOLD);

                    return Some((best_idx, best_sim, best_reg_id));
                }
                else{
                    print_purples!("No good similarity", ); 
                }
            },
            SyncState::Locked => {
                // In locked state, use the expected offset
                if let Some(offset) = self.offset {
                    println!("LOCKED: Offset -> {}", offset); 

                    let expected_reg_id = (max_id as i64 + offset) as usize;
                    
                    // First try to find exact match
                    for (idx, (reg_raw, _, reg_id)) in candidates.iter().enumerate() {
                        if *reg_id == expected_reg_id {
                            let sim = compute_enhanced_frame_similarity(reg_raw, max_raw, WIDTH_ENCODER, HEIGHT_ENCODER);
                            if sim <= ACCEPTABLE_SIMILARITY_THRESHOLD {
                                return Some((idx, sim, *reg_id));
                            }
                        }
                    }
                    
                    // If exact match not found, find best similarity
                    let mut best_idx = 0;
                    let mut best_sim = f64::MAX;
                    let mut best_reg_id = 0;
                    
                    for (idx, (reg_raw, _, reg_id)) in candidates.iter().enumerate() {
                        let sim = compute_enhanced_frame_similarity(reg_raw, max_raw, WIDTH_ENCODER, HEIGHT_ENCODER);
                        if sim < best_sim {
                            best_sim = sim;
                            best_idx = idx;
                            best_reg_id = *reg_id;
                        }
                    }
                    
                    if best_sim <= ACCEPTABLE_SIMILARITY_THRESHOLD {
                        return Some((best_idx, best_sim, best_reg_id));
                    }
                }
            }
        }
        
        None // No good match found
    }
    
    fn process_match(
        &mut self, 
        match_result: Option<(usize, f64, usize)>,
        max_id: usize,
        max_raw: Vec<u8>,
        max_pixels: Vec<u32>
    ) {
        let mut matched_regular = None;
        
        if self.state == SyncState::Recovering {
            self.recovery_frame_counter += 1;
            
            // During initial recovery period, just use the previous offset and don't check similarity
            if self.recovery_frame_counter < RECOVERY_GRACE_PERIOD {
                if let Some(old_offset) = self.recovery_last_offset {
                    // Calculate where the regular frame should be based on the old offset
                    let expected_reg_id = (max_id as i64 + old_offset) as usize;
                    
                    // Find and consume the regular frame closest to the expected ID
                    let mut closest_idx = 0;
                    let mut closest_distance = usize::MAX;
                    
                    let candidates = self.peek_regular_frames(MAX_PEEK_COUNT);
                    for (idx, (_, _, reg_id)) in candidates.iter().enumerate() {
                        let distance = reg_id.abs_diff(expected_reg_id);
                        if distance < closest_distance {
                            closest_distance = distance;
                            closest_idx = idx;
                        }
                    }
                    
                    // Consume frames up to and including the closest match
                    if !candidates.is_empty() {
                        let consumed_frames = self.consume_regular_frames(closest_idx + 1);
                        matched_regular = consumed_frames.last().cloned();
                        
                        println!("🔄 Recovery: Using old offset {}. Expected: {}, Actual: {:?}, Distance: {}",
                                 old_offset, expected_reg_id, 
                                 matched_regular.as_ref().map(|(_, _, id)| id), 
                                 closest_distance);
                    } else {
                        // No regular frames available, just output the max frame
                        println!("🔄 Recovery: No regular frames available for Max #{}", max_id);
                    }
                }
            } else {
                // After grace period, start looking for good similarity matches again
                if let Some((best_idx, best_sim, best_reg_id)) = match_result {
                    let new_offset = best_reg_id as i64 - max_id as i64;
                    
                    // Check if this is a genuinely good match
                    if best_sim <= RECOVERY_MATCH_THRESHOLD {
                        if self.recovery_candidate_offset == Some(new_offset) {
                            // This offset matches our previous candidate
                            self.recovery_consecutive_matches += 1;
                            
                            // If we've seen enough consecutive matches with this offset
                            if self.recovery_consecutive_matches >= RECOVERY_MAX_ATTEMPTS {
                                // We have confidence in this new offset
                                if self.recovery_last_offset == Some(new_offset) {
                                    // It's the same as our original offset - return to locked state
                                    println!("✅ Recovery complete: Returning to locked state with original offset {}", new_offset);
                                    self.state = SyncState::Locked;
                                    self.offset = Some(new_offset);
                                } else {
                                    // It's a new offset - we need to update
                                    println!("✅ Recovery complete: Re-synchronized with new offset {} (was {})",
                                             new_offset, self.recovery_last_offset.unwrap_or(0));
                                    self.state = SyncState::Locked;
                                    self.offset = Some(new_offset);
                                }
                                self.recovery_consecutive_matches = 0;
                                self.recovery_candidate_offset = None;
                            }
                        } else {
                            // New candidate offset
                            println!("🔄 Recovery: Found potential new offset {} (sim: {:.4})", new_offset, best_sim);
                            // self.recovery_candidate_offset = Some(new_offset);
                            // self.recovery_last_offset = Some(new_offset); 
                            // self.recovery_consecutive_matches = 1;
                            self.state = SyncState::Locked; 
                            self.offset = Some(new_offset); 
                        }
                        
                        // Consume the matched regular frame
                        let consumed_frames = self.consume_regular_frames(best_idx + 1);
                        matched_regular = consumed_frames.last().cloned();
                    } else {
                        // Not a good match - keep using the old offset
                        if let Some(old_offset) = self.recovery_last_offset {
                            // Use time-based matching like in the grace period
                            let expected_reg_id = (max_id as i64 + old_offset) as usize;
                            
                            // Find closest regular frame (similar to grace period logic)
                            let candidates = self.peek_regular_frames(MAX_PEEK_COUNT);
                            if !candidates.is_empty() {
                                let mut closest_idx = 0;
                                let mut closest_distance = usize::MAX;
                                
                                for (idx, (_, _, reg_id)) in candidates.iter().enumerate() {
                                    let distance = reg_id.abs_diff(expected_reg_id);
                                    if distance < closest_distance {
                                        closest_distance = distance;
                                        closest_idx = idx;
                                    }
                                }
                                
                                // Consume frames up to and including the closest match
                                let consumed_frames = self.consume_regular_frames(closest_idx + 1);
                                matched_regular = consumed_frames.last().cloned();
                                
                                println!("🔄 Recovery: Using recovery offset {}. Expected: {}, Actual: {:?} (poor sim: {:.4})",
                                         old_offset, expected_reg_id, 
                                         matched_regular.as_ref().map(|(_, _, id)| id), 
                                         best_sim);
                            }
                        } else {
                            // Fallback - consume just the oldest frame
                            let consumed_frames = self.consume_regular_frames(1);
                            matched_regular = consumed_frames.last().cloned();
                        }
                    }
                } else {
                    // No match found - use old offset if available
                    if let Some(old_offset) = self.recovery_last_offset {
                        // Use time-based matching as fallback
                        let expected_reg_id = (max_id as i64 + old_offset) as usize;
                        println!("🔄 Recovery: No match found. Using old offset {} for Max #{} (expecting Reg #{})",
                                 old_offset, max_id, expected_reg_id);
                        
                        // Try to find a frame close to the expected ID
                        let candidates = self.peek_regular_frames(MAX_PEEK_COUNT);
                        if !candidates.is_empty() {
                            let mut closest_idx = 0;
                            let mut closest_distance = usize::MAX;
                            
                            for (idx, (_, _, reg_id)) in candidates.iter().enumerate() {
                                let distance = reg_id.abs_diff(expected_reg_id);
                                if distance < closest_distance {
                                    closest_distance = distance;
                                    closest_idx = idx;
                                }
                            }
                            
                            // Consume frames up to and including the closest match
                            let consumed_frames = self.consume_regular_frames(closest_idx + 1);
                            matched_regular = consumed_frames.last().cloned();
                        }
                    }
                }
            }
        } else {
            // Process non-recovery states as before
            if let Some((best_idx, best_sim, best_reg_id)) = match_result {
                // We found a good match
                if best_sim <= GOOD_SIMILARITY_THRESHOLD {
                    // Very good match - update state
                    self.mismatch_counter = 0;
                    
                    match self.state {
                        SyncState::Seeking | SyncState::Recovering => {
                            self.match_counter += 1;
                            
                            // Calculate offset between streams
                            let new_offset = best_reg_id as i64 - max_id as i64;
                            
                            if self.match_counter >= MATCHES_NEEDED_TO_LOCK && !self.buffering_active {
                                // We've found enough consecutive good matches to lock
                                println!("🔒 Synchronization Locked! Offset: {}", new_offset);
                                self.state = SyncState::Locked;
                                self.offset = Some(new_offset);
                                self.match_counter = 0;
                            } else {
                                // Not enough matches yet, but update the potential offset
                                self.offset = Some(new_offset);
                            }
                        },
                        SyncState::Locked => {
                            // Already locked, just confirm current offset
                        }
                        // SyncState::Recovering => {} // Handled in the above logic
                    }
                    
                    // Consume regular frames up to and including the matched one
                    let consumed_frames = self.consume_regular_frames(best_idx + 1);
                    matched_regular = consumed_frames.last().cloned();
                } else {
                    // Match isn't good enough
                    self.handle_mismatch(max_id);

                    
                    // Consume just one regular frame to advance
                    let consumed_frames = self.consume_regular_frames(1);
                    matched_regular = consumed_frames.last().cloned();
                }
            } else {
                // No match found
                self.handle_mismatch(max_id); 
                // Don't consume any regular frames
            }
        }
        
        // Output the pair
        self.output_pair(matched_regular, Some((max_raw, max_pixels, max_id)));
    }
    
    fn handle_mismatch(&mut self, max_id: usize) {
        self.match_counter = 0;
        
        if self.state == SyncState::Locked {
            self.mismatch_counter += 1;
            if self.mismatch_counter >=2{
                print_purples!("Locked-> Mismatch {} / {}" , self.mismatch_counter, MISMATCHES_TO_RECOVERY); 

            }
            
            if self.mismatch_counter >= MISMATCHES_TO_RECOVERY {
                println!("🔄 Sync lost ({}). Entering recovery mode with preserved offset: {:?}", 
                         self.mismatch_counter, self.offset);
                
                // Store the current offset when entering recovery mode
                self.recovery_last_offset = self.offset;
                self.state = SyncState::Recovering;
                self.mismatch_counter = 0;
                self.recovery_frame_counter = 0;
                self.recovery_consecutive_matches = 0;
                self.recovery_candidate_offset = None;

                self.recovery_base_offset = self.offset.unwrap(); 

            }
        }
    }
    
    fn consume_max_frame(&mut self) -> Option<(Vec<u8>, Vec<u32>, usize)> {
        let current_frame_id = self.max_decoder.internal_frame_counter;
        
        self.max_decoder.decoded_frames.pop_front().and_then(|frame_raw| {
            self.max_decoder.internal_frame_counter += 1;
            
            convert_rgb_to_u32(&frame_raw, WIDTH_ENCODER, HEIGHT_ENCODER)
                .map(|pixels| (frame_raw, pixels, current_frame_id))
        })
    }
    
    fn peek_regular_frames(&self, max_count: usize) -> Vec<(Vec<u8>, Vec<u32>, usize)> {
        let mut frames = Vec::with_capacity(max_count);
        let base_id = self.regular_decoder.frames_processed;
        
        for (idx, frame_raw) in self.regular_decoder.decoded_frames.iter().take(max_count).enumerate() {
            if let Some(pixels) = convert_rgb_to_u32(frame_raw, WIDTH_ENCODER, HEIGHT_ENCODER) {
                frames.push((frame_raw.clone(), pixels, base_id + idx));
            }
        }
        
        frames
    }
    
    fn consume_regular_frames(&mut self, count: usize) -> Vec<(Vec<u8>, Vec<u32>, usize)> {
        let mut frames = Vec::with_capacity(count);
        
        for _ in 0..count {
            let current_frame_id = self.regular_decoder.frames_processed;
            
            if let Some(frame_raw) = self.regular_decoder.decoded_frames.pop_front() {
                self.regular_decoder.frames_processed += 1;
                
                if let Some(pixels) = convert_rgb_to_u32(&frame_raw, WIDTH_ENCODER, HEIGHT_ENCODER) {
                    frames.push((frame_raw, pixels, current_frame_id));
                }
            } else {
                break;
            }
        }
        
        frames
    }
    
    fn output_pair(
        &mut self,
        regular: Option<(Vec<u8>, Vec<u32>, usize)>,
        reference: Option<(Vec<u8>, Vec<u32>, usize)>
    ) {
        let frame_id = self.next_frame_id.fetch_add(1, Ordering::SeqCst);
        
        let (decoded, decoded_raw, _) = match regular {
            Some((raw, pixels, id)) => (Some(pixels), Some(raw), Some(id)),
            None => (None, None, None),
        };
        
        let (reference_pixels, reference_raw, _) = match reference {
            Some((raw, pixels, id)) => (Some(pixels), Some(raw), Some(id)),
            None => (None, None, None),
        };
        
        let sync_pair = FramePair_old {
            decoded,
            reference: reference_pixels,
            decoded_raw,
            reference_raw,
            frame_id,
        };
        
        // println!("Debug sync_pair: {:?}", sync_pair); 

        self.output_queue.push_back(sync_pair);
    }
    
    pub fn next_frame_pair(&mut self) -> Option<FramePair_old> {
        // Check if we're still buffering
        if self.check_buffering_status() {
            return None;
        }
        
        // Make sure we have enough buffer before returning frames
        if self.output_queue.len() <= BUFFERING_THRESHOLD / 3 {
            self.synchronize_frame_buffers();
        }
        
        // Only return a frame if we have sufficient buffer
        if self.output_queue.len() > BUFFERING_THRESHOLD / 3 {
            if let Some(pair) = self.output_queue.pop_front() {
                self.throttle_semaphore.add_permits(1);
                Some(pair)
            } else {
                None
            }
        } else {
            None
        }
    }
    
    // Public API methods
    pub fn get_sync_state(&self) -> SyncState {
        self.state.clone()
    }
    
    pub fn get_stable_offset(&self) -> Option<i64> {
        self.offset
    }
    
    pub fn change_stable_offset(&mut self, new: i64) {
        self.offset = Some(new);
    }
}

pub async fn process_trace_single_encoder_new(
    trace_csv: PathBuf,
    ip: IpAddr,
) -> Result<()> {
    // extract scenario name & trace index
    let file_name = trace_csv.file_name().unwrap().to_string_lossy();
    let caps = Regex::new(r"trace_offline_video(\d+)\.csv$")?
        .captures(&file_name)
        .expect("filename didn’t match");
    let trace_idx: usize = caps[1].parse()?;

    let scenario = trace_csv.parent()
        .and_then(|p| p.file_name())
        .unwrap()
        .to_string_lossy();
    print_prettyy!(DebugColor::Blue, "Starting SIM: {} | Scenario: {}", file_name, scenario);

    // extract bitrate
    let bitrate_re = Regex::new(r"_Br(?P<br>\d+(\.\d+)?)_")?;
    let bitrate: f32 = bitrate_re
        .captures(&scenario)
        .and_then(|caps| caps.name("br"))
        .ok_or_else(|| anyhow::anyhow!("Bitrate not found in scenario name"))?
        .as_str()
        .parse()?;
    println!("Extracted bitrate: {}", bitrate);

    let intra_re = Regex::new(r"_IR(?P<ir>[01])")?;
    let intra_refresh: bool = intra_re
        .captures(&scenario)
        .and_then(|caps| caps.name("ir"))
        .ok_or_else(|| anyhow::anyhow!("IR flag not found in scenario name"))?
        .as_str()
        .ne("0");


    println!("Extracted intra_refresh flag: {}", intra_refresh);

    // setup metrics logger
    let metric = MetricsLogger::new_for_trace(&scenario, trace_idx, false, )?;

    // parse CSV trace
    let trace_path = trace_csv.to_str().unwrap();
    let mut rdr = csv::ReaderBuilder::new().has_headers(true).from_path(trace_path)?;

        let start = Instant::now(); 
    // 1. ─ parse CSV ───────────────────────────────────────
    let mut raw_ids     = Vec::new();
    let mut raw_ts: Vec<f64> = Vec::new(); 
    let mut path_video  = None::<String>;
    let mut offset_video= None::<f64>;
    let mut idr_frequency = None::<u32>;
    let mut throughput_data = Vec::<f64>::new();





    // let trace_path = "/…/trace_offline_video0.csv";
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(trace_path)?;

    for rec in rdr.records() {
        let rec = rec?;

        // first non‐empty row → OFFSET_VIDEO (col 0), PATH_VIDEO (col 1), IDR_FREQUENCY (col 2), intra_refresh (col 3); 

        println!("rec = {:?}", rec); 

        if path_video.is_none() {
            if let (Some(o), Some(p), Some(i)) =
                (rec.get(0), rec.get(1), rec.get(2))
            {
                if !o.trim().is_empty()
                 && !p.trim().is_empty()
                 && !i.trim().is_empty()
                {
                    offset_video    = Some(o.trim().parse()?);
                    path_video      = Some(p.trim().to_string());
                    idr_frequency   = Some(i.trim().parse()?);
                    // intra_refresh = Some(j.trim().parse()?);  

                    continue;
                }
            }
        }

        // subsequent rows → timestamp(col 3), ID_frame(col 4), Lost(col 5), Throughput(col 6)
        if let (Some(_ts), Some(id_s), Some(lost_s), Some(tp_s)) =
            (rec.get(3), rec.get(4), rec.get(5), rec.get(6))
        {
            // skip any empty/data‐garbage rows
            if id_s.trim().is_empty() { continue; }

            let id:    u32   = id_s.trim().parse()?;
            let lost:  bool  = lost_s.trim().parse::<u32>()? != 0;
            let tp:    f64   = tp_s.trim().parse()?;
            let ts:     f64 = _ts.trim().parse()?; 

            raw_ids.push(id);
            raw_ts.push(ts); 
            throughput_data.push(tp);

            // if you want to track lost in the CSV pass-through you could also
            // store it alongside id here, or later when you build FrameInfo[]
        }
    }


        // sanity‐check & unwrap
    let video = path_video
        .clone()
        .ok_or_else(|| anyhow::anyhow!("missing video path"))?;
    let offset = offset_video
        .ok_or_else(|| anyhow::anyhow!("missing offset"))?;
    let idr    = idr_frequency
        .ok_or_else(|| anyhow::anyhow!("missing IDR_FREQUENCY"))?;

    let ts_map: HashMap<u32,f64> = raw_ids.iter().cloned().zip(raw_ts.iter().cloned()).collect();


    println!(
        "Video={}\n, offset={} s\n, IDR_FREQ={}fps\n, read {} frames\n, {} throughput samples\n",
        video,
        offset,
        idr,
        raw_ids.len(),
        throughput_data.len()
    );


    let video = path_video.clone().ok_or_else(|| anyhow::anyhow!("missing video path"))?;
    let offset = offset_video.ok_or_else(|| anyhow::anyhow!("missing offset"))?;
    let idr = idr_frequency.ok_or_else(|| anyhow::anyhow!("missing IDR_FREQUENCY"))?;

    // rebuild full trace & lost set
    raw_ids.sort_unstable(); raw_ids.dedup();
    let min_id = *raw_ids.first().unwrap();
    let max_id = *raw_ids.last().unwrap();
    let id_set: HashSet<_> = raw_ids.iter().cloned().collect();
    let mut trace = Vec::with_capacity((max_id - min_id + 1) as usize);
    for id in min_id..=max_id {
        trace.push(FrameInfo { id, lost: !id_set.contains(&id) });
    }
    let lost_ids: BTreeSet<u32> = trace.iter().filter(|f| f.lost).map(|f| f.id).collect();
    let trace = Arc::new(trace);

    // spawn encoder task
    let (tx_low, rx_low) = unbounded::<(usize,u32,Vec<u8>)>();
    make_encoder_task(
        0, bitrate, Arc::clone(&trace), tx_low.clone(), video.clone(), offset, false, idr, intra_refresh, 
    );
    drop(tx_low);

    let mut dec_low = HevcDecoder::new(60, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32, "LOW");
    let mut dec_ref = HevcDecoder::new(60, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32, "REF");

    // create window
    const SCALE: f64 = 0.28;
    let sw = (WIDTH_ENCODER as f64 * SCALE) as usize;
    let sh = (HEIGHT_ENCODER as f64 * SCALE) as usize;
    let mut window = Window::new(
        "Frame-sync offline",
        sw * 2 + 10,
        sh,
        WindowOptions::default(),
    )?;

    // sync-state machine
    enum SyncState { InSync, OutOfSync }
    let mut sync_state = SyncState::InSync;
    let mut lost_run = 0;
    let loss_threshold = 5;
    let mut pending_resync = false;

    let expected = trace.len();
    let mut seen_pairs = 0;
    // let mut vmaf_jobs = Vec::new();
    let sem = Arc::new(Semaphore::new(num_cpus::get())); 
    let mut vmaf_tasks: FuturesUnordered<tokio::task::JoinHandle<()>> =
        FuturesUnordered::new();



    let mut low_buf = HashMap::new();
    let mut ref_buf = HashMap::new();
    let mut last_real_low: Option<FrameBuf> = None;
    let mut ready_ids = BTreeSet::new();
    let mut low_done = false;

    while window.is_open() {
        // receive low packets
        match rx_low.recv_timeout(Duration::from_millis(2000)) {
            Ok((_tag, id, pkt)) => {
                // reference side
                dec_ref.process_packet(pkt.clone(), id).await;
                // update sync state on loss
                if lost_ids.contains(&id) {
                    lost_run += 1;
                    sync_state = SyncState::OutOfSync;
                    if lost_run > loss_threshold { pending_resync = true; }
                    // synthetic filler
                    if let Some(last) = last_real_low.take() {
                        let fb = FrameBuf { rgb: last.rgb.clone(), synthetic: true };
                        low_buf.insert(id, fb.clone());
                        ready_ids.insert(id);
                    }
                } else {
                    // real low decode
                    lost_run = 0;
                    dec_low.process_packet(pkt, id).await;
                    while let Some((rgb, fid, _)) = dec_low.next_decoded_frame() {
                        // check for IDR resync
                        if fid % idr == 0 && pending_resync {
                            sync_state = SyncState::InSync;
                            pending_resync = false;
                            print_prettyy!(DebugColor::Green, "Resync at IDR frame {}", fid);
                        }
                        let fb = FrameBuf { rgb: rgb.clone(), synthetic: false };
                        low_buf.insert(fid, fb.clone());
                        last_real_low = Some(fb);
                        if ref_buf.contains_key(&fid) {
                            ready_ids.insert(fid);
                        }
                    }
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => { low_done = true; }
        }

        // drain ref decoder
        while let Some((rgb_r, id, _)) = dec_ref.next_decoded_frame() {
            ref_buf.insert(id, FrameBuf { rgb: rgb_r, synthetic: false });
            if low_buf.contains_key(&id) {
                ready_ids.insert(id);
            }
        }

        // pair frames and optionally run VMAF
        let mut to_remove = Vec::new();
        for &id in &ready_ids {
            if let (Some(fb_l), Some(fb_r)) = (low_buf.remove(&id), ref_buf.remove(&id)) {
                // draw always
                draw_pair(&mut window, &fb_l.rgb, &fb_r.rgb, &scenario, id, ts_map[&id])?;
                seen_pairs += 1;
                // only metric when in-sync and real frame
                if matches!(sync_state, SyncState::InSync) && !fb_l.synthetic {
                    let permit = VMAF_SLOTS.clone().acquire_owned().await.unwrap();
                    let logger = metric.clone();
                    let rgb_ref = fb_r.rgb.clone();
                    let rgb_low: Vec<u8> = fb_l.rgb.clone();
                    let clone_ts_map = ts_map.clone(); 
                    // Run VMAF between the two encoder outputs (fb_enc0 vs fb_enc1)
                    let sem_clone   = sem.clone();
                    let logger      = metric.clone();
                    let ts_map      = ts_map.clone();
                    let ip_clone    = ip.clone();
                    let rgb_enc1    = fb_l.rgb.clone();
                    let rgb_enc0    = fb_r.rgb.clone();
                    let handle = tokio::spawn(async move {
                        let _permit = sem_clone.acquire().await.unwrap();
                        if let Err(e) = logger
                            .process_frame_buffers(id as u64, ts_map[&id], rgb_enc1, rgb_enc0, ip_clone)
                            .await
                        {
                            eprintln!("VMAF job failed on #{}: {}", id, e);
                        }
                    });
                    vmaf_tasks.push(handle);
                } else {
                    println!("⏭ Skipping VMAF for out-of-sync or synthetic id {}", id);
                }
                to_remove.push(id);
            }
        }
        for id in to_remove { ready_ids.remove(&id); }

        if seen_pairs >= expected || low_done { break; }
        window.update();
    }

    while let Some(res) = vmaf_tasks.next().await {
        if let Err(join_err) = res {
            eprintln!("VMAF task panicked: {}", join_err);
        }
    }
    
    metric.finalize()?;
    Ok(())
}


pub async fn process_trace_two_encoders_no_loss(
    trace_csv: PathBuf,
    ip: IpAddr,
) -> Result<()> {
    // extract scenario name & trace index
    let file_name = trace_csv.file_name().unwrap().to_string_lossy();
    let caps = Regex::new(r"trace_offline_video(\d+)\.csv$")?
        .captures(&file_name)
        .expect("filename didn’t match");
    let trace_idx: usize = caps[1].parse()?;

    let scenario = trace_csv.parent()
        .and_then(|p| p.file_name())
        .unwrap()
        .to_string_lossy();
    print_prettyy!(DebugColor::Blue, "Starting SIM: {} | Scenario: {}", file_name, scenario);

    // extract bitrate
    let bitrate_re = Regex::new(r"_Br(?P<br>\d+(\.\d+)?)_")?;
    let bitrate: f32 = bitrate_re
        .captures(&scenario)
        .and_then(|caps| caps.name("br"))
        .ok_or_else(|| anyhow::anyhow!("Bitrate not found in scenario name"))?
        .as_str()
        .parse()?;
    println!("Extracted bitrate: {}", bitrate);

    let intra_re = Regex::new(r"_IR(?P<ir>\d+)")?;
    let intra_refresh_enabled = intra_re
        .captures(&scenario)
        .and_then(|caps| caps.name("ir"))
        .ok_or_else(|| anyhow::anyhow!("IR flag missing"))?
        .as_str()
        .parse::<u32>()? > 0;

    // setup metrics logger (no-loss mode)
    let metric = MetricsLogger::new_for_trace(&scenario, trace_idx, true)?;

    // parse CSV trace with updated format (includes intra-refresh flag)
    let trace_path = trace_csv.to_str().unwrap();
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(trace_path)?;

    // 1. ─ parse CSV ───────────────────────────────────────
    let mut raw_ids = Vec::new();
    let mut raw_ts: Vec<f64> = Vec::new();
    let mut path_video = None::<String>;
    let mut offset_video = None::<f64>;
    let mut idr_frequency = None::<u32>;
    let mut _throughput_data = Vec::<f64>::new();

    for rec in rdr.records() {
        let rec = rec?;

        // first non-empty row → OFFSET_VIDEO (col 0), PATH_VIDEO (col 1), IDR_FREQUENCY (col 2), INTRA_REFRESH (col 3)
        if path_video.is_none() {
            if let (Some(o), Some(p), Some(i)) =
                (rec.get(0), rec.get(1), rec.get(2))
            {
                if !o.trim().is_empty()
                    && !p.trim().is_empty()
                    && !i.trim().is_empty()
                {
                    offset_video = Some(o.trim().parse()?);
                    path_video = Some(p.trim().to_string());
                    idr_frequency = Some(i.trim().parse()?);
                    // intra_refresh = Some(j.trim().parse()?);
                    continue;
                }
            }
        }

        // subsequent rows → timestamp(col 4), ID_frame(col 5), Lost(col 6), Throughput(col 7)
        if let (Some(_ts), Some(id_s), Some(_lost), Some(tp_s)) =
            (rec.get(3), rec.get(4), rec.get(5), rec.get(6))
        {
            if id_s.trim().is_empty() { continue; }

            let id: u32 = id_s.trim().parse()?;
            let ts: f64 = _ts.trim().parse()?;
            let tp: f64 = tp_s.trim().parse()?;

            raw_ids.push(id);
            raw_ts.push(ts);
            _throughput_data.push(tp);
        }
    }

    // sanity-check & unwrap
    let video = path_video
        .clone()
        .ok_or_else(|| anyhow::anyhow!("missing video path"))?;
    let offset = offset_video
        .ok_or_else(|| anyhow::anyhow!("missing offset"))?;
    let idr = idr_frequency
        .ok_or_else(|| anyhow::anyhow!("missing IDR_FREQUENCY"))?;


    let ts_map = Arc::new(
        raw_ids.iter().cloned()
            .zip(raw_ts.iter().cloned())
            .collect::<HashMap<_, _>>()
    );

    println!(
        "Video={}\n, offset={} s\n, IDR_FREQ={}fps\n, read {} frames\n, {} throughput samples\n",
        video,
        offset,
        idr,
        raw_ids.len(),
        _throughput_data.len()
    );

    // rebuild full trace (no loss assumption)
    raw_ids.sort_unstable(); raw_ids.dedup();
    let min_id = *raw_ids.first().unwrap();
    let max_id = *raw_ids.last().unwrap();
    let id_set: HashSet<_> = raw_ids.iter().cloned().collect();
    let mut trace = Vec::with_capacity((max_id - min_id + 1) as usize);
    for id in min_id..=max_id {
        trace.push(FrameInfo { id, lost: !id_set.contains(&id) });
    }
    let trace = Arc::new(trace);

    // spawn two encoder tasks with intra-refresh option
    let (tx_encoder_0, rx_encoder_0) = unbounded::<(usize, u32, Vec<u8>)>();
    let (tx_encoder_1, rx_encoder_1) = unbounded::<(usize, u32, Vec<u8>)>();

    make_encoder_task(
        0,
        bitrate,
        Arc::clone(&trace),
        tx_encoder_0.clone(),
        video.clone(),
        offset,
        false,
        idr,
        intra_refresh_enabled,
    );
    make_encoder_task(
        1,
        MAX_BITRATE_REFERENCE,
        Arc::clone(&trace),
        tx_encoder_1.clone(),
        video,
        offset,
        false,
        0, // doesn't get used for IR, IR 100 Mbps reference for VMAF. 
        true,
    );

    drop(tx_encoder_0);
    drop(tx_encoder_1);

    // Initialize two distinct decoders, one for each encoder's output
    let mut dec_encoder_0 = HevcDecoder::new(60, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32, "ENC_0");
    let mut dec_encoder_1 = HevcDecoder::new(60, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32, "ENC_1");

    // create window
    let sw = (WIDTH_ENCODER as f64 * SCALE) as usize;
    let sh = (HEIGHT_ENCODER as f64 * SCALE) as usize;
    let mut window = Window::new(
        "Frame-sync offline: Encoder 0 vs Encoder 1", // Updated window title
        sw * 2 + 10,
        sh,
        WindowOptions::default(),
    )?;

    let expected = trace.len();
    let mut seen_pairs = 0;
    // let mut vmaf_jobs = Vec::new();

    let sem = Arc::new(Semaphore::new(num_cpus::get())); 
    let mut vmaf_tasks: FuturesUnordered<tokio::task::JoinHandle<()>> =
        FuturesUnordered::new();


    let mut encoder_0_buf = HashMap::new(); // Buffer for decoded frames from encoder 0
    let mut encoder_1_buf = HashMap::new(); // Buffer for decoded frames from encoder 1
    let mut ready_ids = BTreeSet::new();
    let mut encoder_0_done = false;
    let mut encoder_1_done = false;

    while window.is_open() {
        // Receive packets from Encoder 0
        match rx_encoder_0.recv_timeout(Duration::from_millis(10)) {
            Ok((_tag, id, pkt)) => {
                dec_encoder_0.process_packet(pkt, id).await;
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => { encoder_0_done = true; }
        }

        // Receive packets from Encoder 1
        match rx_encoder_1.recv_timeout(Duration::from_millis(10)) {
            Ok((_tag, id, pkt)) => {
                dec_encoder_1.process_packet(pkt, id).await;
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => { encoder_1_done = true; }
        }

        // Drain Encoder 0 decoder
        while let Some((rgb, fid, _)) = dec_encoder_0.next_decoded_frame() {
            let fb = FrameBuf { rgb: rgb.clone(), synthetic: false };
            encoder_0_buf.insert(fid, fb);
            if encoder_1_buf.contains_key(&fid) {
                ready_ids.insert(fid);
            }
        }

        // Drain Encoder 1 decoder
        while let Some((rgb, fid, _)) = dec_encoder_1.next_decoded_frame() {
            let fb = FrameBuf { rgb: rgb.clone(), synthetic: false };
            encoder_1_buf.insert(fid, fb);
            if encoder_0_buf.contains_key(&fid) {
                ready_ids.insert(fid);
            }
        }

        // pair frames and optionally run VMAF
        let mut to_remove = Vec::new();
        for &id in &ready_ids {
            if let (Some(fb_enc0), Some(fb_enc1)) = (encoder_0_buf.remove(&id), encoder_1_buf.remove(&id)) {
                // draw always, comparing the two encoder outputs
                draw_pair(&mut window, &fb_enc0.rgb, &fb_enc1.rgb, &scenario, id, ts_map[&id])?;
                seen_pairs += 1;

                // Run VMAF between the two encoder outputs (fb_enc0 vs fb_enc1)
                let sem_clone   = sem.clone();
                let logger      = metric.clone();
                let ts_map      = ts_map.clone();
                let ip_clone    = ip.clone();
                let rgb_enc1    = fb_enc1.rgb.clone();
                let rgb_enc0    = fb_enc0.rgb.clone();
                let handle = tokio::spawn(async move {
                       let _permit = sem_clone.acquire().await.unwrap();
                       if let Err(e) = logger
                           .process_frame_buffers(id as u64, ts_map[&id], rgb_enc1, rgb_enc0, ip_clone)
                           .await
                       {
                           eprintln!("VMAF job failed on #{}: {}", id, e);
                       }
                   });
                   vmaf_tasks.push(handle);

                to_remove.push(id);
            }
        }
        for id in to_remove { ready_ids.remove(&id); }

        if seen_pairs >= expected || (encoder_0_done && encoder_1_done) { break; }
        window.update();
    }

    while let Some(res) = vmaf_tasks.next().await {
        if let Err(join_err) = res {
            eprintln!("VMAF task panicked: {}", join_err);
        }
    }
    
    
    
    // join_all(vmaf_jobs).await;
    metric.finalize()?;
    Ok(())
}


fn main() -> Result<()> {
    // 1) Your IP
    let ip: IpAddr = "192.168.0.1".parse().unwrap();

    // 2) Trace‐filename regex
    let trace_re = Regex::new(r"^trace_offline_video\d+\.csv$")?;

    // 3) Group CSVs by their parent folder
    let mut scenarios: Vec<(PathBuf, Vec<PathBuf>)> = {
        let mut map: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        for entry in WalkDir::new("Results/")
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
        {
            let path = entry.path().to_path_buf();
            if let Some(fname) = path.file_name().and_then(|s| s.to_str()) {
                if trace_re.is_match(fname) {
                    map.entry(path.parent().unwrap().to_path_buf())
                       .or_default()
                       .push(path);
                }
            }
        }
        let mut v: Vec<_> = map.into_iter().collect();
        v.sort_by(|(a, _), (b, _)| a.cmp(b));
        v
    };

    // 4) Split into N groups by index mod N
    let mut groups: Vec<Vec<(PathBuf, Vec<PathBuf>)>> = vec![Vec::new(); WORKERS];
    for (i, scenario) in scenarios.drain(..).enumerate() {
        groups[i % WORKERS].push(scenario);
    }

    // 5) Worker factory
    let make_worker = |group: Vec<(PathBuf, Vec<PathBuf>)>, ip: IpAddr| {
        thread::spawn(move || -> Result<()> {
            // each thread gets its own current-thread Tokio runtime
            // let rt = tokio::runtime::Builder::new_current_thread()
            //     .enable_all()
            //     .build()?;

            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(num_cpus::get())   // e.g. 8 on your machine
                .enable_all()
                .build()?;

            rt.block_on(async {
                for (_folder, traces) in group {
                    for trace_csv in traces {
                        process_trace_two_encoders_no_loss(trace_csv.clone(), ip.clone()).await?;
                        // process_trace_single_encoder_new(trace_csv.clone()  , ip.clone()).await?; 
                    }
                }
                Ok(())
            })
        })
    };

    // 6) Spawn all WORKERS threads
    let mut handles = Vec::with_capacity(WORKERS);
    for group in groups {
        handles.push(make_worker(group, ip.clone()));
    }

    // 7) Join all of them
    for (idx, h) in handles.into_iter().enumerate() {
        h.join()
         .unwrap_or_else(|_| panic!("worker {} panicked", idx))?; 
    }

    Ok(())
}
