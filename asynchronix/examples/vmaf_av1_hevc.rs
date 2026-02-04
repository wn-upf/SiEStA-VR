
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

use crossbeam::channel::{bounded,  RecvTimeoutError, Sender,};
use minifb::{ Window, WindowOptions};

use serde::Deserialize;

use std::collections::{HashMap, BTreeSet};

use std::fmt::{ Debug};
use std::fs::File;

use std::io::{Read,};

use std::process::{Child, Command, Stdio};
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


const MAX_CONCURRENT_VMAF_SCENARIOS: usize = 2; 


pub const MAX_BITRATE_REFERENCE_MBPS: f32 = 100.0; 
pub const WINDOW_SCALE_MULTIPLIER: f64 = 0.4; 
pub const BOUNDED_CHANNEL_SIZE: usize = 15; // channel depth for VMAF crossbeam


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

    pub fn new_for_trace( results_folder: &str, scenario: &str, trace_idx: usize, two_encoders: bool) -> Result<Self> {
        
        let dir = format!("{}/{}", results_folder, scenario);
        std::fs::create_dir_all(&dir)?;
        let strrrrr = if two_encoders { "bitrate" } else { "loss" };

        let path = format!("{}/VMAF_metrics_{}_{}.csv", dir, strrrrr, trace_idx);
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
        ref_buf: Vec<u8>, // own the data
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
            "T:{:.3} [{}] | Frame {} : VMAF {:.2}, SSIM {:.4}",
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
    // const SCALE: f64 = 0.28;
    let sw = (W as f64 * SCALE_FACTOR_WINDOW) as usize;
    let sh = (H as f64 * SCALE_FACTOR_WINDOW) as usize;
    let ww = sw * 2 + 10;

    let left = resize_nn(&rgb_to_u32(rgb_l), W, H, sw, sh);
    let right = resize_nn(&rgb_to_u32(rgb_r), W, H, sw, sh);

    let mut buf = vec![0u32; ww * sh];
    for y in 0..sh {
        let dst = y * ww;
        buf[dst..dst + sw].copy_from_slice(&left[y * sw..(y + 1) * sw]);
        buf[dst + sw + 10..dst + sw + 10 + sw].copy_from_slice(&right[y * sw..(y + 1) * sw]);
    }

    let y_lbl = sh - 40;
    render_text(&mut buf, &format!("#{}", id), 10, y_lbl, ww, 0xFFAA00, 2);
    render_text(
        &mut buf,
        &format!("#{}", id),
        sw + 20,
        y_lbl,
        ww,
        0xFFAA00,
        2,
    );

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
        let mut child = Command::new("ffmpeg")
            .args(&[
                "-i", &video_path,
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
    
    let metric = MetricsLogger::new_for_trace( parent_results_path ,&scenario, trace_idx, false)?;

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


    let mut buf_distorted = HashMap::new();
    let mut buf_ref = HashMap::new();
    let mut ready_ids = BTreeSet::new();
    
    let mut vmaf_tasks = FuturesUnordered::new();
    // let sem = Arc::new(Semaphore::new(num_cpus::get()));
    let sem = Arc::new(Semaphore::new(num_cpus::get().min(4)));


    let mut enc_done = false;

    const MAX_BUFFER_SIZE: usize = 5;  // Max frames to hold in RAM per stream
    const MAX_PENDING_TASKS: usize = 4; // Max VMAF calculations running/queued


    // while window.is_open() {
    loop{
        // A. THROTTLE: Check if we have too many pending VMAF tasks
        // If we do, we wait for one to finish before reading more video data.
        if vmaf_tasks.len() >= MAX_PENDING_TASKS {
            if let Some(res) = vmaf_tasks.next().await {
                 if let Err(e) = res { eprintln!("Task Join Error: {}", e); }
            }
        }

        // B. READ DISTORTED (Only if buffer has space)
        // We check buffer space. If we are 'enc_done', we still enter here to drain the decoder!
        if buf_distorted.len() < MAX_BUFFER_SIZE {
            
            // 1. Pull raw packets from the network/channel
            // Only attempt this if the encoder is still alive
            if !enc_done {
                match rx_enc.try_recv() {
                    Ok((_, id, pkt)) => {
                        // Push packet into decoder
                        dec_enc.process_packet(pkt, id);
                    },
                    Err(crossbeam_channel::TryRecvError::Disconnected) => {
                        println!(">> Encoder Disconnected. Finalizing stream...");
                        enc_done = true; // Stop trying to read from this channel
                    },
                    Err(crossbeam_channel::TryRecvError::Empty) => {
                        // Channel is alive but empty, just continue
                    }
                }
            }

            // 2. Always drain decoded frames from the decoder
            // We must do this even if 'enc_done' is true, because the decoder 
            // might still be processing the last packet we just sent it.
            while let Some((rgb, id)) = dec_enc.next_decoded_frame() {
                buf_distorted.insert(id, FrameBuf { rgb, synthetic: false });
                
                // Check if this new frame matches a reference frame we already have
                if buf_ref.contains_key(&id) { 
                    ready_ids.insert(id); 
                }
            }
        }

        // C. READ REFERENCE (Only if buffer has space)
        // This is crucial: if buf_ref is full, we STOP reading rx_ref.
        // This causes rx_ref channel to fill, which causes tx_ref.send() to block in the thread, pausing ffmpeg.
        if buf_ref.len() < MAX_BUFFER_SIZE {
             match rx_ref.try_recv() {
                Ok((id, rgb)) => {
                    buf_ref.insert(id, FrameBuf { rgb, synthetic: false });
                    if buf_distorted.contains_key(&id) { ready_ids.insert(id); }
                },
                Err(_) => {} // Empty or disconnected
            }
        }

        // D. PROCESS PAIRS
        let mut processed_ids = Vec::new();
        // Only process what we can fit into the task queue
        while vmaf_tasks.len() < MAX_PENDING_TASKS {
            // Get the next ready ID
            let next_id = match ready_ids.iter().next() {
                Some(&id) => id,
                None => break,
            };

            if let (Some(fb_dist), Some(fb_ref)) = (buf_distorted.remove(&next_id), buf_ref.remove(&next_id)) {
                let ts = *ts_map.get(&next_id).unwrap_or(&0.0);
                
                // Update Window (Cheap)
                // Use unwrap_or to prevent crash on drawing error
                // let _ = draw_pair(&mut window, &fb_dist.rgb, &fb_ref.rgb, &scenario, next_id, ts);

                if let Some(ref mut w) = window {
                    let _ = draw_pair(w.inner(), &fb_dist.rgb, &fb_ref.rgb, &scenario, next_id, ts);
                    w.inner().update();
                }


                // Spawn VMAF (Expensive - takes ownership of RAM)
                let logger = metric.clone();
                let sem = sem.clone();
                let ip = ip.clone();
                let r_rgb = fb_ref.rgb;
                let d_rgb = fb_dist.rgb;

                vmaf_tasks.push(tokio::spawn(async move {
                    // Acquire semaphore inside task to limit CPU usage
                    let _p = sem.acquire().await.unwrap();
                    if let Err(e) = logger.process_frame_buffers(next_id as u64, ts, r_rgb, d_rgb, ip).await {
                        eprintln!("VMAF Error frame {}: {}", next_id, e);
                    }
                }));

                processed_ids.push(next_id);
            }
            ready_ids.remove(&next_id);
        }

        if let Some(ref w) = window {
            if !w.0.is_open() { break; } // Access inner .0
        }

        // Cleanup check
        if enc_done && buf_distorted.is_empty() && vmaf_tasks.is_empty() {
             println!("Trace processing complete."); 
             break;
        }
                
        // Small sleep to prevent tight loop burning 100% CPU on empty checks
        // Use standard sleep, not async sleep if not needed, but here we are in async fn
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    // Await all VMAF jobs
    while let Some(res) = vmaf_tasks.next().await {
        if let Err(e) = res { eprintln!("Task Join Error: {}", e); }
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

    for (trace_csv, ip, codec, fps, v_name, gui, parent_res) in tasks {
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
                &parent_res
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

                    ).await {
                        eprintln!("ERROR processing {}: {}", fname, e);
                    }
                }
            }
        }
    }
}
