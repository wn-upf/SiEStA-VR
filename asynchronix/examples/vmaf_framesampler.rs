use anyhow::Result;
use async_std::sync::{Arc, Mutex};
use async_std::task;
use crossbeam::channel::{bounded, unbounded, Receiver, Sender, TryRecvError};
use ffmpeg_sidecar::command::FfmpegCommand;
use minifb::{Key, Scale, Window, WindowOptions};
use rand::Rng;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::fs::File;
use std::hash::Hash;
use std::io::{BufReader, BufWriter, Read, Write};
use std::process::Command;
use std::process::{ChildStdin, ChildStdout};
use std::thread;
use std::time::Duration;
use std::time::Instant;

use tempfile::TempDir;

// Define the expected (encoder) dimensions.
// pub const WIDTH_ENCODER: usize = 3840;
// pub const HEIGHT_ENCODER: usize = 2160;
pub const WIDTH_ENCODER: usize = 1920;
pub const HEIGHT_ENCODER: usize = 1080;

pub const INITIAL_BITRATE: &str = "10M";
pub const WINDOW_SCALE_FACTOR: f64 = 0.7;

pub const IDR_FRAME_SIZE_GOP: usize = 120;

pub const PACKET_LOSS_PROBABILITY: f64 = 0.04;

pub const CHUNK_SIZE_ENCODER_S: f64 = 3.0;
pub const FRAME_CUTOFF_LIMIT: usize = 1200;

pub const FRAME_GROUP_SIZE: usize = 5;

pub const OFFSET_VIDEO: f64 = 250.0;

pub const REENCODE: bool = true;

pub const USE_VMAF_EXAMPLE: bool = true;

/// Results of video quality metrics analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
struct QualityMetrics {
    frame_number: u64,
    timestamp_ms: u64,
    vmaf_score: f64,
    psnr_y: f64,
    psnr_avg: f64,
    ssim_score: f64,
}

// Add this function to clean up old frames
fn cleanup_old_data(
    synced_pairs: &mut Vec<SyncedFramePair>,
    current_time: Instant,
    epoch: Instant,
    retention_duration: Duration,
) -> Result<(), Box<dyn Error>> {
    // Calculate cutoff timestamp (current time - 20 seconds)
    let cutoff_time_ms = current_time.duration_since(epoch).as_millis() as u64
        - retention_duration.as_millis() as u64;

    println!(
        "Cleaning up frames older than {} seconds (before timestamp {}ms)",
        retention_duration.as_secs(),
        cutoff_time_ms
    );

    // Track how many files we clean up
    let mut cleaned_count = 0;

    // Remove old frame pairs and delete their files
    synced_pairs.retain(|pair| {
        let keep = pair.timestamp_ms >= cutoff_time_ms;

        if !keep {
            // Delete the RGB files from disk
            if let Err(e) = std::fs::remove_file(&pair.ref_path) {
                eprintln!("Failed to delete {}: {}", pair.ref_path, e);
            }
            if let Err(e) = std::fs::remove_file(&pair.lossy_path) {
                eprintln!("Failed to delete {}: {}", pair.lossy_path, e);
            }
            cleaned_count += 1;
        }

        keep
    });

    // Also clean up temporary Y4M files
    for dir_name in &["Video_Sink/reference_rgb", "Video_Sink/lossy_rgb"] {
        if let Ok(entries) = std::fs::read_dir(dir_name) {
            for entry in entries {
                if let Ok(entry) = entry {
                    if let Ok(metadata) = entry.metadata() {
                        if let Ok(modified) = metadata.modified() {
                            if modified < std::time::SystemTime::now() - retention_duration {
                                if let Err(e) = std::fs::remove_file(entry.path()) {
                                    eprintln!("Failed to delete temporary file: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if cleaned_count > 0 {
        println!("Cleaned up {} old frame pairs", cleaned_count);
    }

    Ok(())
}
/// Buffer for frame analysis that processes frames in batches

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetricsResult {
    frame_number: u64,
    timestamp_ms: u64,
    vmaf: f64,
    psnr: f64,
    ssim: f64,
}
//
// Update the process_group function to use the enhanced method
async fn process_group_vmaf(group: FrameGroup, logger: &MetricsLogger) -> Result<()> {
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

#[derive(Debug, Clone)]
struct FrameData {
    ref_rgb: Vec<u8>,
    lossy_rgb: Vec<u8>,
    timestamp_ms: u64,
    frame_number: u64,
}
struct FrameGroup {
    frames: Vec<FrameData>,
}

struct MetricsLogger {
    writer: Arc<Mutex<csv::Writer<File>>>,
}
impl MetricsLogger {
    fn new() -> Result<Self> {
        let file = File::create("Video_Sink/metrics.csv")?;
        let writer = csv::Writer::from_writer(file);
        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    pub async fn process_frame_metrics(
        &self,
        frame_number: u64,
        timestamp_ms: u64,
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
        let video_sink_dir = temp_dir.path().join("Video_Sink");
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
        println!(
            "Frame {}: VMAF = {:.2}, PSNR = {:.2}, SSIM = {:.4}",
            frame_number, vmaf_score, psnr_avg, ssim_score
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

        // Print progress information
        if frame_number % 10 == 0 {
            println!(
                "Frame {}: VMAF = {:.2}, PSNR = {:.2}, SSIM = {:.4}",
                frame_number, vmaf_score, psnr_avg, ssim_score
            );
        }

        Ok(())
    }

    async fn log_metrics(&self, metrics: &FrameMetrics) -> Result<()> {
        let mut writer = self.writer.lock().await;
        writer.serialize(metrics)?;
        writer.flush()?;
        Ok(())
    }
}

#[derive(serde::Serialize)]
struct FrameMetrics {
    frame_number: u64,
    timestamp_ms: u64,
    vmaf: f64,
    psnr: f64,
    ssim: f64,
}

#[derive(Debug, Clone)]
struct FrameMetadata {
    frame_number: u64,
    timestamp_ms: u64,
    is_keyframe: bool,
}

// New struct to store synchronized frame pairs
#[derive(Debug, Clone)]
struct SyncedFramePair {
    frame_number: u64,
    ref_path: String,
    lossy_path: String,
    timestamp_ms: u64,
}

// New encoder type that chunks the video into fixed-duration segments.
/// Each chunk is produced by invoking ffmpeg with "-ss" (start time)
/// and "-t" (duration) options. Parsed complete frames are sent over an async channel.
pub struct ChunkedHevcEncoder {
    input: String,
    width: u32,
    height: u32,
    bitrate: String,
    chunk_duration: f64, // Duration of each chunk in seconds.
    current_offset: f64, // Current start timestamp.
    frame_tx: Sender<Vec<u8>>,
    frame_rx: Receiver<Vec<u8>>,
}

impl ChunkedHevcEncoder {
    /// Create a new ChunkedHevcEncoder.
    pub fn new(input: &str, width: u32, height: u32, bitrate: &str, chunk_duration: f64) -> Self {
        // We use a bounded channel to store parsed frames.

        let random_offset = rand::random::<f64>() * OFFSET_VIDEO;

        println!("Initializing chunkedhevcencoder");
        let (frame_tx, frame_rx) = bounded(100);
        Self {
            input: input.to_string(),
            width,
            height,
            bitrate: bitrate.to_string(),
            chunk_duration,
            current_offset: OFFSET_VIDEO,
            frame_tx,
            frame_rx,
        }
    }

    /// Continuously spawn ffmpeg processes to produce video chunks.
    /// Each process is configured to start at the current_offset and run for chunk_duration seconds.
    /// As data is read from ffmpeg's stdout, it is fed to a HevcParser which extracts complete frames.
    /// Each complete frame is sent via the async channel.
    pub async fn start_chunking(&mut self) -> Result<()> {
        loop {
            println!("CHUNKING!");

            // Build an ffmpeg command for the current chunk:
            // –ss <current_offset> –t <chunk_duration> plus the rest of your encoding options.
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
                .args(&[
                    "-b:v",
                    &self.bitrate,
                    "-maxrate",
                    &self.bitrate,
                    "-minrate",
                    &self.bitrate,
                ])
                .args(&["-rc-lookahead", "0"])
                .args(&["-g", &format!("{:.0}", IDR_FRAME_SIZE_GOP)]) // using your GOP size constant
                .args(&["-movflags", "+frag_keyframe+empty_moov"])
                .args(&["-flush_packets", "1"])
                .args(&["-bsf:v", "hevc_mp4toannexb"])
                .args(&["-an"])
                .args(&["-f", "hevc", "-"]); // output raw HEVC

            // Spawn the ffmpeg process for this chunk.
            let mut child = command.spawn()?;
            let stdout = child.take_stdout().unwrap();
            let mut reader = BufReader::new(stdout);

            let mut parser = HevcParser::new();
            let mut buf = [0u8; 4096];

            // Read data from the process until it ends.
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => break, // end of chunk
                    Ok(n) => {
                        parser.add_data(&buf[..n]);
                        // Extract complete frames and send them on the channel.
                        let frames = parser.get_frames();
                        for frame in frames {
                            if let Err(e) = self.frame_tx.send(frame) {
                                eprintln!("Error sending frame: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error reading ffmpeg chunk: {}", e);
                        break;
                    }
                }
            }
            let _ = child.wait();

            // Update offset for the next chunk.
            self.current_offset += self.chunk_duration;
            // (Optional: Reset current_offset to zero if you want to loop over the input.)

            // Optionally yield control to allow other async tasks to run.
            task::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Async getter that awaits and returns the next available frame.
    pub async fn next_frame(&self) -> Option<Vec<u8>> {
        self.frame_rx.recv().ok()
    }
}

/// Represents a single HEVC NAL unit
pub struct NalUnit {
    pub nal_type: u8,
    pub data: Vec<u8>,
    pub is_keyframe: bool,
}

/// A parser for HEVC bitstreams to extract individual frames
pub struct HevcParser {
    buffer: Vec<u8>,
}

impl HevcParser {
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    /// Add more encoded data to the parser buffer
    pub fn add_data(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Find the next NAL unit start code in the buffer
    fn find_next_start_code(&self, start_pos: usize) -> Option<usize> {
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
    }

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
}

/// Converts raw RGB byte data (3 bytes per pixel) into a Vec<u32> pixel buffer
/// where each pixel is represented as 0xRRGGBB.
fn convert_rgb_to_u32(rgb_data: &[u8], width: usize, height: usize) -> Vec<u32> {
    let expected_len = width * height * 3;
    if rgb_data.len() != expected_len {
        eprintln!(
            "Unexpected RGB data length. Expected {}, got {}",
            expected_len,
            rgb_data.len()
        );
        return Vec::new();
    }

    let mut pixels = Vec::with_capacity(width * height);
    for chunk in rgb_data.chunks_exact(3) {
        let pixel = ((chunk[0] as u32) << 16) | ((chunk[1] as u32) << 8) | (chunk[2] as u32);
        pixels.push(pixel);
    }

    if !pixels.is_empty() {
        // println!("First 5 pixels: {:x} {:x} {:x} {:x} {:x}",
        //     pixels[0], pixels[1], pixels[2], pixels[3], pixels[4]);
    }
    pixels
}

fn scale_pixels(
    buffer: &[u32],
    orig_width: usize,
    orig_height: usize,
    new_width: usize,
    new_height: usize,
) -> Vec<u32> {
    let mut scaled = vec![0u32; new_width * new_height];
    let x_ratio = (orig_width << 16) / new_width;
    let y_ratio = (orig_height << 16) / new_height;

    for y in 0..new_height {
        let y2 = ((y * y_ratio) >> 16) * orig_width;
        for x in 0..new_width {
            let x2 = (x * x_ratio) >> 16;
            scaled[y * new_width + x] = buffer[y2 + x2];
        }
    }
    scaled
}

pub struct HevcDecoder {
    frame_rx: Receiver<Vec<u8>>,
    packet_tx: Sender<Vec<u8>>,
    _stdin_handle: std::thread::JoinHandle<()>,
    _stderr_handle: std::thread::JoinHandle<()>,
    width: u32,
    height: u32,
    parser: HevcParser,
    frame_buffer: VecDeque<Vec<u8>>, // Buffer for parsed HEVC frames
    decoded_frames: VecDeque<Vec<u8>>, // Buffer for decoded RGB frames

    ewma_frame_size: f64, // Store the EWMA value
    last_update: Instant, // Track last update time

    epoch: Instant,
}

impl HevcDecoder {
    pub fn new(framerate: u32, width: u32, height: u32, epoch: Instant) -> Result<Self> {
        let frame_size = (width as usize) * (height as usize) * 3;
        let mut child = FfmpegCommand::new()
            .hwaccel("cuda")
            // Change: Use raw HEVC format for input
            .args(&["-f", "hevc", "-i", "-"])
            .args(&["-vf", &format!("fps={}", framerate)])
            .args(&["-pix_fmt", "rgb24"])
            .args(&["-f", "rawvideo", "-"])
            .spawn()?;

        let stdout = child.take_stdout().unwrap();
        let stdin = child.take_stdin().unwrap();
        let stderr = child.take_stderr().unwrap();

        let (frame_tx, frame_rx) = unbounded::<Vec<u8>>();
        let (packet_tx, packet_rx) = bounded::<Vec<u8>>(100);

        // Start stdout reader thread
        std::thread::spawn({
            let frame_size = frame_size;
            move || {
                let mut reader = BufReader::new(stdout);
                let mut buffer = Vec::with_capacity(frame_size * 2);
                let mut chunk = vec![0u8; 4096];
                loop {
                    match reader.read(&mut chunk) {
                        Ok(0) => break,
                        Ok(n) => {
                            buffer.extend_from_slice(&chunk[..n]);
                            while buffer.len() >= frame_size {
                                let frame = buffer.drain(..frame_size).collect();
                                // frame_tx.send(frame).unwrap();

                                if let Ok(_) = frame_tx.send(frame) {
                                    // frame
                                } else {
                                    println!("SEND ERROR!");
                                    // break;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Decoder read error: {}", e);
                            break;
                        }
                    }
                }
            }
        });

        let stdin_handle = std::thread::spawn(move || {
            let mut writer = stdin;
            for packet in packet_rx {
                if let Err(e) = writer.write_all(&packet) {
                    eprintln!("Decoder write error: {}", e);
                    break;
                }

                // It's important to flush after each frame to ensure real-time processing
                if let Err(e) = writer.flush() {
                    eprintln!("Decoder flush error: {}", e);
                    break;
                }
            }
        });

        // Start stderr monitor thread
        let stderr_handle = std::thread::spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut buf = String::new();
            loop {
                buf.clear();
                match reader.read_to_string(&mut buf) {
                    Ok(0) => break,
                    Ok(_) => eprint!("{}", buf),
                    Err(e) => {
                        eprintln!("Decoder stderr read error: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(Self {
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
            last_update: Instant::now(),
            epoch: epoch,
        })
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
    // Process incoming encoded packets
    pub fn process_packet(&mut self, packet: Vec<u8>) -> Result<()> {
        // Add packet data to the parser

        let frame_size = packet.len() as f64;
        let now = Instant::now();
        let delta_t = now.duration_since(self.last_update).as_secs_f64();
        self.last_update = now;

        // Calculate smoothing factor α
        let alpha = 1.0 - (-delta_t / 1.0).exp();

        // Update EWMA
        self.ewma_frame_size = alpha * frame_size + (1.0 - alpha) * self.ewma_frame_size;

        println!(
            "{:.3} - Parsing w size: {}, EWMA size: {:.2}",
            Instant::now().duration_since(self.epoch).as_secs_f64(),
            frame_size,
            self.ewma_frame_size
        );
        self.parser.add_data(&packet);

        // Extract frames from the parser and buffer them
        let frames = self.parser.get_frames();
        for frame in frames {
            self.frame_buffer.push_back(frame);
        }

        // Forward the packet to ffmpeg for decoding
        self.packet_tx.send(packet)?;

        Ok(())
    }

    // Your existing method to get raw frames from ffmpeg
    fn try_next_decoded_frame(&self) -> Result<Option<Vec<u8>>> {
        match self.frame_rx.try_recv() {
            Ok(frame) => Ok(Some(frame)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => {
                Err(anyhow::anyhow!("Decoder frame channel disconnected"))
            }
        }
    }

    // Process any available decoded frames from ffmpeg
    pub fn process_decoded_frames(&mut self) -> Result<()> {
        // Drain any available decoded frames into our buffer
        while let Ok(Some(frame)) = self.try_next_decoded_frame() {
            // println!("Frame got on decoder, size: {}", frame.len());
            self.decoded_frames.push_back(frame);
        }

        Ok(())
    }

    // Get the next available encoded frame
    pub fn next_encoded_frame(&mut self) -> Option<Vec<u8>> {
        self.frame_buffer.pop_front()
    }

    // Get the next available decoded RGB frame
    pub fn next_decoded_frame(&mut self) -> Option<Vec<u8>> {
        self.decoded_frames.pop_front()
    }
}

fn convert_rgb_to_yuv420p(rgb: &[u8], width: usize, height: usize) -> Vec<u8> {
    let mut yuv = vec![0u8; (width * height * 3) / 2]; // YUV420p has 1.5 bytes per pixel

    // Step 1: Compute Y plane
    for j in 0..height {
        for i in 0..width {
            let idx = (j * width + i) * 3;
            let r = rgb[idx] as f32;
            let g = rgb[idx + 1] as f32;
            let b = rgb[idx + 2] as f32;
            // ITU-R BT.601 conversion for example
            let y = (0.299 * r + 0.587 * g + 0.114 * b).round() as u8;
            yuv[j * width + i] = y;
        }
    }

    // Step 2: Downsample U and V channels (4:2:0)
    // Here, we average over each 2x2 block.
    let chroma_width = width / 2;
    let chroma_height = height / 2;
    let mut u_plane = vec![0u8; chroma_width * chroma_height];
    let mut v_plane = vec![0u8; chroma_width * chroma_height];

    for j in 0..chroma_height {
        for i in 0..chroma_width {
            let mut sum_u = 0.0;
            let mut sum_v = 0.0;
            for y in 0..2 {
                for x in 0..2 {
                    let idx = ((j * 2 + y) * width + (i * 2 + x)) * 3;
                    let r = rgb[idx] as f32;
                    let g = rgb[idx + 1] as f32;
                    let b = rgb[idx + 2] as f32;
                    // Using BT.601 formulas for U and V:
                    let u = (-0.168736 * r - 0.331264 * g + 0.5 * b + 128.0).round();
                    let v = (0.5 * r - 0.418688 * g - 0.081312 * b + 128.0).round();
                    sum_u += u;
                    sum_v += v;
                }
            }
            let chroma_idx = j * chroma_width + i;
            u_plane[chroma_idx] = (sum_u / 4.0).round() as u8;
            v_plane[chroma_idx] = (sum_v / 4.0).round() as u8;
        }
    }

    // Copy the U and V planes after the Y plane.
    yuv[width * height..width * height + u_plane.len()].copy_from_slice(&u_plane);
    yuv[width * height + u_plane.len()..].copy_from_slice(&v_plane);

    yuv
}

#[async_std::main]
async fn main() -> Result<()> {
    // ffmpeg_sidecar::download::auto_download()?;

    let mut num_updates_ref = 0;
    let EPOCH = Instant::now();
    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/cut_video.mp4";
    println!("Starting video codec simulation...");

    // Create directories for RGB frames
    std::fs::create_dir_all("Video_Sink/reference_rgb")?;
    std::fs::create_dir_all("Video_Sink/lossy_rgb")?;

    let (group_tx, group_rx) = bounded(5); // Buffer up to 5 groups
    let metrics_logger = MetricsLogger::new()?;

    if USE_VMAF_EXAMPLE {
        async_std::task::spawn(async move {
            while let Ok(group) = group_rx.recv() {
                println!("GOT GROUP!!");
                process_group_vmaf(group, &metrics_logger)
                    .await
                    .unwrap_or_else(|e| {
                        eprintln!("Error processing group: {}", e);
                    });
            }
        });
    } else {
        for i in 1..1000 {
            println!("NO VMAF FOUNDDDDDD");
        }
    }
    // Start processing thread

    let retention_duration = Duration::from_secs(20); // 20-second retention window
    let mut last_cleanup_time = Instant::now();
    let cleanup_interval = Duration::from_secs(5); // Check for cleanup every 5 seconds

    // Create the chunked encoder.
    let mut chunked_encoder = ChunkedHevcEncoder::new(
        input_path,
        WIDTH_ENCODER as u32,
        HEIGHT_ENCODER as u32,
        INITIAL_BITRATE,
        CHUNK_SIZE_ENCODER_S, // Chunk duration in seconds
    );
    // Clone the async receiver so we can poll for frames in the main loop.
    let frame_rx = chunked_encoder.frame_rx.clone();

    // For storing synchronized frame pairs
    let mut synced_pairs: Vec<SyncedFramePair> = Vec::new();

    if REENCODE == true {
        // Spawn the chunking task in the background.
        async_std::task::spawn(async move {
            if let Err(e) = chunked_encoder.start_chunking().await {
                eprintln!("Chunking task error: {}", e);
            }
        });

        // Create the decoder as before.
        let mut ref_decoder =
            HevcDecoder::new(60, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32, EPOCH)?;
        let mut lossy_decoder =
            HevcDecoder::new(60, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32, EPOCH)?;

        let mut frame_counter: u64 = 0;

        let frame_size_rgb = WIDTH_ENCODER * HEIGHT_ENCODER * 3;
        let black_frame = vec![0u8; frame_size_rgb];
        let mut last_lossy_frame: Option<Vec<u8>> = None;

        // CSV index for frame pairs
        let mut sync_index = BufWriter::new(File::create("Video_Sink/synced_frames.csv")?);
        writeln!(sync_index, "frame_number,timestamp_ms,ref_path,lossy_path")?;

        println!("Encoder and decoder initialized");

        // Window setup.
        let scale_factor = WINDOW_SCALE_FACTOR;
        let scaled_width = (WIDTH_ENCODER as f64 * scale_factor) as usize;
        let scaled_height = (HEIGHT_ENCODER as f64 * scale_factor) as usize;

        // let mut vmaf_metrics = VmafMetrics::new(scaled_width as u32, (scaled_height / 4) as u32);
        // // Create a third window for the VMAF visualization
        // // let mut vmaf_window = Window::new(
        // //     "VMAF Metrics",
        // //     scaled_width,
        // //     scaled_height / 4,
        // //     WindowOptions::default(),
        // // )?;
        let mut reference_window = Window::new(
            "Reference Video",
            scaled_width,
            scaled_height,
            WindowOptions::default(),
        )?;

        let mut lossy_window = Window::new(
            "
            Lossy Video",
            scaled_width,
            scaled_height,
            WindowOptions::default(),
        )?;

        println!("Window opened");

        // FPS control.
        let frame_duration = Duration::from_secs_f64(1.0 / 60.0);
        let mut next_frame_time = Instant::now();
        let mut fps_timer = Instant::now();
        let mut frames_displayed: usize = 0;
        let mut frames_dropped: usize = 0;

        // Transmission simulation.
        let transmission_interval = Duration::from_millis(16); // ~60fps
        let mut next_transmission_time = Instant::now();

        // Pipeline recovery.
        let mut last_frame_time = Instant::now();
        let frame_timeout = Duration::from_millis(2000);
        let mut drop_probability = PACKET_LOSS_PROBABILITY;
        let mut rng = rand::thread_rng();

        // let mut frame_buffer: HashMap<u64, (Option<Vec<u8>>, Option<Vec<u8>>)> = HashMap::new();
        let mut current_group = FrameGroup {
            frames: Vec::with_capacity(FRAME_GROUP_SIZE),
        };

        while reference_window.is_open()
            && lossy_window.is_open()
            && !reference_window.is_key_down(Key::Escape)
            && !lossy_window.is_key_down(Key::Escape)
        {
            // Check if it's time to clean up old data
            // if Instant::now().duration_since(last_cleanup_time) >= cleanup_interval {
            //     if let Err(e) = cleanup_old_data(&mut synced_pairs, Instant::now(), EPOCH, retention_duration) {
            //         eprintln!("Error during cleanup: {}", e);
            //     }
            //     last_cleanup_time = Instant::now();
            // }

            // 1. Pipeline recovery: if no frame received in a while, try to recover using a keyframe.
            if Instant::now().duration_since(last_frame_time) > frame_timeout {
                println!("Pipeline stalled, attempting recovery...");
                // Await a frame (which should ideally be a keyframe) for recovery.
                if let Ok(keyframe) = frame_rx.recv() {
                    println!("Sending recovery keyframe");

                    if let Err(e) = ref_decoder.process_packet(keyframe.clone()) {
                        eprintln!("Error sending recovery keyframe: {}", e);
                    }

                    if let Err(e) = lossy_decoder.process_packet(keyframe.clone()) {
                        eprintln!("Error sending recovery keyframe to lossy decoder: {}", e);
                    }
                } else {
                    println!("No keyframe available for recovery");
                }
                last_frame_time = Instant::now();
            }

            // 2. Frame transmission simulation: non-blocking try_recv from our async channel.
            let now = Instant::now();
            if now >= next_transmission_time {
                if let Ok(frame) = frame_rx.try_recv() {
                    //Retrieve reference frame
                    if let Err(e) = ref_decoder.process_packet(frame.clone()) {
                        eprintln!("Reference decoder error: {}", e);
                    }
                    // Simulate frame loss probability.
                    if rng.gen::<f64>() >= drop_probability {
                        if let Err(e) = lossy_decoder.process_packet(frame) {
                            eprintln!("Error sending frame to decoder: {}", e);
                        }
                    } else {
                        println!(
                            "{:.3} Simulated frame loss!",
                            now.duration_since(lossy_decoder.epoch).as_secs_f32()
                        );
                        frames_dropped += 1;
                    }
                    next_transmission_time += transmission_interval;
                }
            }

            // 3. Process any decoded frames.
            if let Err(e) = ref_decoder.process_decoded_frames() {
                eprintln!("Error processing decoded frames: {}", e);
            }
            if let Err(e) = lossy_decoder.process_decoded_frames() {
                eprintln!("Error lossy processing decoded frames: {}", e);
            }

            // Process encoded frames for frame numbers
            if let Some(_) = ref_decoder.next_encoded_frame() {
                frame_counter += 1;
            }

            // 4. Display frames and save synchronized pairs
            let display_time = Instant::now();

            if display_time >= next_frame_time {
                if let Some(ref_frame) = ref_decoder.next_decoded_frame() {
                    // Update the last successful frame time
                    last_frame_time = Instant::now();
                    let frame_number = frame_counter; // Get the frame number
                                                      // Convert and display the reference frame
                    let pixels = convert_rgb_to_u32(&ref_frame, WIDTH_ENCODER, HEIGHT_ENCODER);
                    let scaled = scale_pixels(
                        &pixels,
                        WIDTH_ENCODER,
                        HEIGHT_ENCODER,
                        scaled_width,
                        scaled_height,
                    );
                    if let Err(e) =
                        reference_window.update_with_buffer(&scaled, scaled_width, scaled_height)
                    {
                        eprintln!("Error updating window buffer: {}", e);
                    } else {
                        frames_displayed += 1;
                    }

                    // Check if we also have a lossy frame
                    if let Some(mut lossy_frame) = lossy_decoder.next_decoded_frame() {
                        // We have both frames - save them as a synchronized pair
                        let timestamp = Instant::now().duration_since(EPOCH).as_millis() as u64;

                        let ref_path =
                            format!("Video_Sink/reference_rgb/frame_{:04}.rgb", frame_counter);
                        std::fs::write(&ref_path, &ref_frame)?;

                        let lossy_path =
                            format!("Video_Sink/lossy_rgb/frame_{:04}.rgb", frame_counter);
                        std::fs::write(&lossy_path, &lossy_frame)?;

                        // Record the synchronized pair with these consistent paths
                        synced_pairs.push(SyncedFramePair {
                            frame_number: frame_counter,
                            ref_path: ref_path.clone(),
                            lossy_path: lossy_path.clone(),
                            timestamp_ms: timestamp,
                        });
                        // Write the synchronized pair to the CSV file immediately
                        writeln!(
                            sync_index,
                            "{},{},{},{}",
                            frame_counter, timestamp, ref_path, lossy_path
                        )?;

                        let frame_data = FrameData {
                            ref_rgb: ref_frame,
                            lossy_rgb: lossy_frame.clone(),
                            timestamp_ms: Instant::now().duration_since(EPOCH).as_millis() as u64,
                            frame_number: frame_counter,
                        };

                        current_group.frames.push(frame_data);

                        // Send group when full
                        if current_group.frames.len() >= FRAME_GROUP_SIZE {
                            if let Err(e) = group_tx.send(current_group) {
                                eprintln!("Error sending group: {}", e);
                            }
                            current_group = FrameGroup {
                                frames: Vec::with_capacity(FRAME_GROUP_SIZE),
                            };
                        }

                        // Display the lossy frame
                        let pixels =
                            convert_rgb_to_u32(&lossy_frame, WIDTH_ENCODER, HEIGHT_ENCODER);
                        let scaled = scale_pixels(
                            &pixels,
                            WIDTH_ENCODER,
                            HEIGHT_ENCODER,
                            scaled_width,
                            scaled_height,
                        );
                        last_lossy_frame = Some(lossy_frame.clone());
                        lossy_window.update_with_buffer(&scaled, scaled_width, scaled_height)?;
                    } else if let Some(prev_lossy_frame) = last_lossy_frame.clone() {
                        // Use the previous lossy frame if no new one is available
                        let pixels =
                            convert_rgb_to_u32(&prev_lossy_frame, WIDTH_ENCODER, HEIGHT_ENCODER);
                        let scaled = scale_pixels(
                            &pixels,
                            WIDTH_ENCODER,
                            HEIGHT_ENCODER,
                            scaled_width,
                            scaled_height,
                        );
                        lossy_window.update_with_buffer(&scaled, scaled_width, scaled_height)?;
                    }

                    next_frame_time += frame_duration;
                }
            } else {
                // Yield to other tasks.
                async_std::task::sleep(Duration::from_millis(1)).await;
            }

            // FPS counter.
            if Instant::now().duration_since(fps_timer) >= Duration::from_secs(2) {
                println!(
                    "FPS: {}, Synced Pairs: {}",
                    frames_displayed / 2,
                    synced_pairs.len()
                );
                frames_displayed = 0;
                fps_timer = Instant::now();
            }

            reference_window.update();
            lossy_window.update();
            sync_index.flush()?;

            num_updates_ref += 1;
            if num_updates_ref >= FRAME_CUTOFF_LIMIT {
                println!("REACHED {} FRAME LIMIT!", FRAME_CUTOFF_LIMIT);

                // Flush the sync index

                // Wait a moment to ensure all files are written
                // std::thread::sleep(Duration::from_secs(2));
                break;
            }
            // vmaf_window.update();
        }

        // Process remaining frames
        if !current_group.frames.is_empty() {
            group_tx.send(current_group)?;
        }
    }

    // Calculate VMAF after processing is complete

    // if std::path::Path::new("Video_Sink/synced_frames.csv").exists() {
    //     println!("Launching sync visualization...");
    //     match visualize_sync() {
    //         Ok(_) => println!("Sync visualization completed successfully"),
    //         Err(e) => eprintln!("Sync visualization failed: {}", e),
    //     }
    // } else {
    //     println!("No synced frames data found. Run the encoder/decoder first.");
    // }

    println!("Exiting gracefully...");
    Ok(())
}
