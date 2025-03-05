use anyhow::Result;
use std::hash::Hash;
use std::io::{BufReader, Read, Write, BufWriter};
use std::process::{ChildStdin, ChildStdout};
use ffmpeg_sidecar::command::FfmpegCommand;
use minifb::{Key, Scale, Window, WindowOptions};
use rand::Rng;
use crossbeam::channel::{bounded, unbounded, Receiver, Sender, TryRecvError};
use std::time::Duration;
use std::time::Instant;
use std::collections::{HashMap, VecDeque};
use async_std::task;
use plotters::prelude::*;
use std::fs::File;
use std::path::Path;
use std::process::Command; 
use std::thread;
use std::error::Error;
use plotters::prelude::*;
use serde::Deserialize;
use serde_json::Value;



// Define the expected (encoder) dimensions.
// pub const WIDTH_ENCODER: usize = 3840;
// pub const HEIGHT_ENCODER: usize = 2160;
pub const WIDTH_ENCODER: usize = 1920;
pub const HEIGHT_ENCODER: usize = 1080;

pub const INITIAL_BITRATE : &str= "10M"; 
pub const WINDOW_SCALE_FACTOR: f64 = 0.7; 

pub const IDR_FRAME_SIZE_GOP: usize = 120;

pub const PACKET_LOSS_PROBABILITY: f64 = 0.02; 

pub const CHUNK_SIZE_ENCODER_S: f64 = 3.0; 
pub const FRAME_CUTOFF_LIMIT: usize = 700; 


pub const SUBSET_FRAMES_VMAF: usize = FRAME_CUTOFF_LIMIT; 



pub const OFFSET_VIDEO: f64 = 250.0;

pub const REENCODE: bool = true; 

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

use plotters::prelude::*;

// Maximum number of frames to keep in history for plotting
const VMAF_HISTORY_SIZE: usize = 180; // 3 seconds at 60fps

// Structure to hold VMAF metrics for real-time display
struct VmafMetrics {
    // History of VMAF scores for plotting
    scores: VecDeque<f64>,
    // Current frame's VMAF score
    current_score: f64,
    // Running statistics
    min_score: f64,
    max_score: f64,
    avg_score: f64,
    total_frames: usize,
    // Path to save plot image
    plot_path: String,
    // Dimensions for the plot
    width: u32,
    height: u32,
}

impl VmafMetrics {
    pub fn new(width: u32, height: u32) -> Self {
        Self {
            scores: VecDeque::with_capacity(VMAF_HISTORY_SIZE),
            current_score: 0.0,
            min_score: 100.0,
            max_score: 0.0,
            avg_score: 0.0,
            total_frames: 0,
            plot_path: "Video_Sink/vmaf_plot.png".to_string(),
            width,
            height,
        }
    }

    // Add a new VMAF score to the history
    pub fn add_score(&mut self, score: f64) {
        self.current_score = score;
        
        // Update statistics
        self.min_score = self.min_score.min(score);
        self.max_score = self.max_score.max(score);
        self.total_frames += 1;
        
        // Update running average
        let total_score = self.avg_score * (self.total_frames - 1) as f64 + score;
        self.avg_score = total_score / self.total_frames as f64;
        
        // Add to history, keeping the last VMAF_HISTORY_SIZE scores
        self.scores.push_back(score);
        if self.scores.len() > VMAF_HISTORY_SIZE {
            self.scores.pop_front();
        }
    }
    
    // Generate a plot of VMAF scores
    pub fn generate_plot(&self) -> Result<(), Box<dyn std::error::Error>> {
        let root = BitMapBackend::new(&self.plot_path, (self.width, self.height))
            .into_drawing_area();
        
        root.fill(&WHITE)?;
        
        let min_y = (self.min_score.max(0.0) - 5.0).max(0.0);
        let max_y = (self.max_score + 5.0).min(100.0);
        
        let mut chart = ChartBuilder::on(&root)
            .caption("VMAF Score Over Time", ("sans-serif", 20).into_font())
            .margin(5)
            .x_label_area_size(30)
            .y_label_area_size(30)
            .build_cartesian_2d(0..self.scores.len(), min_y..max_y)?;
        
        chart.configure_mesh()
            .x_labels(5)
            .y_labels(5)
            .y_desc("VMAF Score")
            .x_desc("Frame")
            .axis_desc_style(("sans-serif", 15))
            .draw()?;
        
        // Plot VMAF scores as a line
        chart.draw_series(LineSeries::new(
            self.scores.iter().enumerate().map(|(i, &score)| (i, score)),
            &BLUE,
        ))?
        .label("VMAF Score")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &BLUE));
        
        // Add a reference line for "good quality" threshold at VMAF = 80
        chart.draw_series(LineSeries::new(
            vec![(0, 80.0), (self.scores.len(), 80.0)],
            &RED.mix(0.5),
        ))?
        .label("Good Quality")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &RED.mix(0.5)));
        
        // Add stats to the chart
        chart.draw_series(std::iter::once(Text::new(
            format!("Current: {:.1} | Avg: {:.1} | Min: {:.1} | Max: {:.1}",
                self.current_score, self.avg_score, self.min_score, self.max_score),
            (self.scores.len() / 2, max_y - 5.0),
            ("sans-serif", 15).into_font(),
        )))?;
        
        chart.configure_series_labels()
            .background_style(&WHITE.mix(0.8))
            .border_style(&BLACK)
            .draw()?;
        
        root.present()?;
        
        Ok(())
    }
    
    // Get the path to the plot image
    pub fn get_plot_path(&self) -> &str {
        &self.plot_path
    }
}




// New encoder type that chunks the video into fixed-duration segments.
/// Each chunk is produced by invoking ffmpeg with "-ss" (start time)
/// and "-t" (duration) options. Parsed complete frames are sent over an async channel.
pub struct ChunkedHevcEncoder {
    input: String,
    width: u32,
    height: u32,
    bitrate: String,
    chunk_duration: f64,   // Duration of each chunk in seconds.
    current_offset: f64,   // Current start timestamp.
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
            current_offset: random_offset, 
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
                .args(&["-b:v", &self.bitrate, "-maxrate", &self.bitrate, "-minrate", &self.bitrate])
                .args(&["-rc-lookahead", "0"])
                .args(&["-g", &format!("{:.0}", IDR_FRAME_SIZE_GOP)])  // using your GOP size constant
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
            if (self.buffer[i] == 0 && self.buffer[i + 1] == 0 && self.buffer[i + 2] == 1) || 
               (i < self.buffer.len() - 4 && self.buffer[i] == 0 && self.buffer[i + 1] == 0 && 
                self.buffer[i + 2] == 0 && self.buffer[i + 3] == 1) {
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
        let start_code_len = if start_pos + 3 < self.buffer.len() && self.buffer[start_pos + 2] == 0 && self.buffer[start_pos + 3] == 1 {
            4
        } else {
            3
        };
        
        // Find the next start code
        let next_start = self.find_next_start_code(start_pos + start_code_len);
        
        let (nal_end, has_next) = match next_start {
            Some(pos) => (pos, true),
            None => (self.buffer.len(), false)
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
        let pixel = ((chunk[0] as u32) << 16) | 
                   ((chunk[1] as u32) << 8) | 
                   (chunk[2] as u32);
        pixels.push(pixel);
    }

    if !pixels.is_empty() {
        // println!("First 5 pixels: {:x} {:x} {:x} {:x} {:x}", 
        //     pixels[0], pixels[1], pixels[2], pixels[3], pixels[4]);
    }
    pixels
}


fn scale_pixels(buffer: &[u32], orig_width: usize, orig_height: usize, new_width: usize, new_height: usize) -> Vec<u32> {
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
    frame_buffer: VecDeque<Vec<u8>>,  // Buffer for parsed HEVC frames
    decoded_frames: VecDeque<Vec<u8>>, // Buffer for decoded RGB frames

    ewma_frame_size: f64,  // Store the EWMA value
    last_update: Instant,   // Track last update time


    epoch: Instant, 
}

impl HevcDecoder {
    pub fn new(framerate: u32, width: u32, height: u32, epoch: Instant, ) -> Result<Self> {
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
            if (frame[i] == 0 && frame[i + 1] == 0 && frame[i + 2] == 1) || 
               (frame[i] == 0 && frame[i + 1] == 0 && frame[i + 2] == 0 && frame[i + 3] == 1) {
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
            frame_size, self.ewma_frame_size
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
            Err(TryRecvError::Disconnected) => Err(anyhow::anyhow!("Decoder frame channel disconnected")),
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
            let y = (0.299*r + 0.587*g + 0.114*b).round() as u8;
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
                    let u = (-0.168736*r - 0.331264*g + 0.5*b + 128.0).round();
                    let v = (0.5*r - 0.418688*g - 0.081312*b + 128.0).round();
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
    
    // Create the chunked encoder.
    let mut chunked_encoder = ChunkedHevcEncoder::new(
        input_path,
        WIDTH_ENCODER as u32,
        HEIGHT_ENCODER as u32,
        INITIAL_BITRATE,
        CHUNK_SIZE_ENCODER_S,  // Chunk duration in seconds
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
        let mut ref_decoder = HevcDecoder::new(60, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32, EPOCH)?;
        let mut lossy_decoder = HevcDecoder::new(60, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32, EPOCH)?;

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

        let mut lossy_window = Window::new("
            Lossy Video", scaled_width,
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


        let mut frame_buffer: HashMap<u64, (Option<Vec<u8>>, Option<Vec<u8>>)> = HashMap::new();

        while reference_window.is_open() && lossy_window.is_open()
            && !reference_window.is_key_down(Key::Escape) 
            && !lossy_window.is_key_down(Key::Escape) {
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
                        println!("{:.3} Simulated frame loss!", now.duration_since(lossy_decoder.epoch).as_secs_f32());
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
                    let scaled = scale_pixels(&pixels, WIDTH_ENCODER, HEIGHT_ENCODER, scaled_width, scaled_height);
                    if let Err(e) = reference_window.update_with_buffer(&scaled, scaled_width, scaled_height) {
                        eprintln!("Error updating window buffer: {}", e);
                    } else {
                        frames_displayed += 1;
                    }
                    
                    // Check if we also have a lossy frame
                    if let Some( mut lossy_frame) = lossy_decoder.next_decoded_frame() {
                        // We have both frames - save them as a synchronized pair
                        let timestamp = Instant::now().duration_since(EPOCH).as_millis() as u64;
                        
                        let ref_path = format!("Video_Sink/reference_rgb/frame_{:04}.rgb", frame_counter);
                        std::fs::write(&ref_path, &ref_frame)?;
                        
                        let lossy_path = format!("Video_Sink/lossy_rgb/frame_{:04}.rgb", frame_counter);
                        std::fs::write(&lossy_path, &lossy_frame)?;
                        
                        // Record the synchronized pair with these consistent paths
                        synced_pairs.push(SyncedFramePair {
                            frame_number: frame_counter,
                            ref_path: ref_path.clone(),
                            lossy_path: lossy_path.clone(),
                            timestamp_ms: timestamp,
                        });
                        // Write the synchronized pair to the CSV file immediately
                        writeln!(sync_index, "{},{},{},{}", 
                        frame_counter, 
                        timestamp, 
                        ref_path, 
                        lossy_path)?;
                                    
                        // Display the lossy frame
                        let pixels = convert_rgb_to_u32(&lossy_frame, WIDTH_ENCODER, HEIGHT_ENCODER);
                        let scaled = scale_pixels(&pixels, WIDTH_ENCODER, HEIGHT_ENCODER, scaled_width, scaled_height);
                        last_lossy_frame = Some(lossy_frame.clone());
                        lossy_window.update_with_buffer(&scaled, scaled_width, scaled_height)?;
                    } else if let Some(prev_lossy_frame) = last_lossy_frame.clone() {
                        // Use the previous lossy frame if no new one is available
                        let pixels = convert_rgb_to_u32(&prev_lossy_frame, WIDTH_ENCODER, HEIGHT_ENCODER);
                        let scaled = scale_pixels(&pixels, WIDTH_ENCODER, HEIGHT_ENCODER, scaled_width, scaled_height);
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
                println!("FPS: {}, Synced Pairs: {}", frames_displayed / 2, synced_pairs.len());
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
    }

    // Calculate VMAF after processing is complete
    calculate_vmaf_direct().unwrap();

    

    println!("Exiting gracefully...");
    Ok(())
}

fn print_vmaf_score(score: f64, frame_number: u64) {
    let quality_label = if score >= 90.0 {
        "Excellent"
    } else if score >= 80.0 {
        "Good"
    } else if score >= 70.0 {
        "Fair" 
    } else if score >= 50.0 {
        "Poor"
    } else {
        "Bad"
    };
    
    println!("Frame {}: VMAF = {:.2} ({}) {}", 
        frame_number, 
        score, 
        quality_label,
        // Simple ASCII bar chart
        "#".repeat((score / 10.0) as usize)
    );
}

// pub fn calculate_vmaf() -> Result<()> {
//     println!("Starting VMAF calculation on synchronized RGB frames...");
    
//     // Read the synchronized frame pairs from the CSV file
//     let synced_frames_csv = match std::fs::read_to_string("Video_Sink/synced_frames.csv") {
//         Ok(content) => content,
//         Err(e) => {
//             eprintln!("Error reading synced_frames.csv: {}", e);
//             return Err(anyhow::anyhow!("Failed to read synchronized frames CSV"));
//         }
//     };
    
//     let mut synced_pairs: Vec<SyncedFramePair> = Vec::new();
//     let mut valid_pairs: Vec<SyncedFramePair> = Vec::new();
    
//     for line in synced_frames_csv.lines().skip(1) {  // Skip the header
//         let parts: Vec<&str> = line.split(',').collect();
//         if parts.len() >= 4 {
//             let frame_number: u64 = parts[0].parse()?;
//             let timestamp_ms: u64 = parts[1].parse()?;
//             let ref_path = parts[2].to_string();
//             let lossy_path = parts[3].to_string();
            
//             // Create a pair
//             synced_pairs.push(SyncedFramePair {
//                 frame_number,
//                 timestamp_ms,
//                 ref_path,
//                 lossy_path,
//             });
//         }
//     }
    
//     println!("Found {} synchronized frame pairs", synced_pairs.len());
    
//     if synced_pairs.is_empty() {
//         return Err(anyhow::anyhow!("No synchronized frames found"));
//     }
    
//     // Create temp directories to convert RGB to YUV (needed for VMAF)
//     std::fs::create_dir_all("Video_Sink/temp")?;
    
//     // Verify each file exists and has non-zero size before including it
//     for pair in &synced_pairs {
//         let ref_metadata = match std::fs::metadata(&pair.ref_path) {
//             Ok(metadata) => metadata,
//             Err(e) => {
//                 eprintln!("Warning: Cannot access reference frame {}: {}", pair.ref_path, e);
//                 continue;
//             }
//         };
        
//         let lossy_metadata = match std::fs::metadata(&pair.lossy_path) {
//             Ok(metadata) => metadata,
//             Err(e) => {
//                 eprintln!("Warning: Cannot access lossy frame {}: {}", pair.lossy_path, e);
//                 continue;
//             }
//         };
        
//         // Check file sizes to ensure they're not empty
//         if ref_metadata.len() == 0 {
//             eprintln!("Warning: Reference frame {} is empty, skipping", pair.ref_path);
//             continue;
//         }
        
//         if lossy_metadata.len() == 0 {
//             eprintln!("Warning: Lossy frame {} is empty, skipping", pair.lossy_path);
//             continue;
//         }
        
//         // If we get here, both files exist and have content
//         valid_pairs.push(pair.clone());
//     }
    
//     println!("Found {} valid frame pairs after validation", valid_pairs.len());
    
//     if valid_pairs.is_empty() {
//         return Err(anyhow::anyhow!("No valid frame pairs found after validation"));
//     }
    
//     // Instead of using concat with ffmpeg, let's directly concatenate the files
//     // This gives us more control and avoids ffmpeg concat issues
//     println!("Creating concatenated reference file...");
//     {
//         let mut ref_concat = std::fs::File::create("Video_Sink/temp/reference_concat.rgb")?;
//         for pair in &valid_pairs {
//             match std::fs::read(&pair.ref_path) {
//                 Ok(data) => {
//                     if let Err(e) = ref_concat.write_all(&data) {
//                         eprintln!("Error writing to reference concat: {}", e);
//                     }
//                 },
//                 Err(e) => {
//                     eprintln!("Error reading reference frame {}: {}", pair.ref_path, e);
//                 }
//             }
//         }
//         ref_concat.flush()?;
//     }
    
//     println!("Creating concatenated lossy file...");
//     {
//         let mut lossy_concat = std::fs::File::create("Video_Sink/temp/lossy_concat.rgb")?;
//         for pair in &valid_pairs {
//             match std::fs::read(&pair.lossy_path) {
//                 Ok(data) => {
//                     if let Err(e) = lossy_concat.write_all(&data) {
//                         eprintln!("Error writing to lossy concat: {}", e);
//                     }
//                 },
//                 Err(e) => {
//                     eprintln!("Error reading lossy frame {}: {}", pair.lossy_path, e);
//                 }
//             }
//         }
//         lossy_concat.flush()?;
//     }
    
//     // Verify that our concatenated files have content
//     let ref_concat_metadata = std::fs::metadata("Video_Sink/temp/reference_concat.rgb")?;
//     let lossy_concat_metadata = std::fs::metadata("Video_Sink/temp/lossy_concat.rgb")?;
    
//     if ref_concat_metadata.len() == 0 || lossy_concat_metadata.len() == 0 {
//         return Err(anyhow::anyhow!("Concatenated files are empty"));
//     }
    
//     println!("Concatenated reference file size: {} bytes", ref_concat_metadata.len());
//     println!("Concatenated lossy file size: {} bytes", lossy_concat_metadata.len());
    
//     // Convert raw RGB streams to YUV format (needed for VMAF)
//     let ref_yuv_status = Command::new("ffmpeg")
//         .args(&[
//             "-f", "rawvideo",
//             "-pixel_format", "rgb24",
//             "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
//             "-framerate", "60",
//             "-i", "Video_Sink/temp/reference_concat.rgb",
//             "-pix_fmt", "yuv420p",
//             "Video_Sink/temp/reference.yuv",
//             "-y"
//         ])
//         .status()?;
        
//     if !ref_yuv_status.success() {
//         eprintln!("Failed to convert reference to YUV");
//         // Continue anyway to see if the lossy conversion works
//     }
    
//     let lossy_yuv_status = Command::new("ffmpeg")
//         .args(&[
//             "-f", "rawvideo",
//             "-pixel_format", "rgb24",
//             "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
//             "-framerate", "60",
//             "-i", "Video_Sink/temp/lossy_concat.rgb",
//             "-pix_fmt", "yuv420p",
//             "Video_Sink/temp/lossy.yuv",
//             "-y"
//         ])
//         .status()?;
        
//     if !lossy_yuv_status.success() {
//         eprintln!("Failed to convert lossy to YUV");
//         // Continue anyway to see if we can still calculate metrics
//     }
    
//     // Check if YUV files were created successfully
//     if !std::path::Path::new("Video_Sink/temp/reference.yuv").exists() || 
//        !std::path::Path::new("Video_Sink/temp/lossy.yuv").exists() {
//         return Err(anyhow::anyhow!("YUV conversion failed, output files not created"));
//     }
    
//     // Run VMAF calculation without model_path parameter
//     let vmaf_output = Command::new("ffmpeg")
//         .args(&[
//             "-hwaccel", "cuda",
//             "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
//             "-framerate", "60",
//             "-pixel_format", "yuv420p",
//             "-i", "Video_Sink/temp/reference.yuv",
//             "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
//             "-framerate", "60",
//             "-pixel_format", "yuv420p",
//             "-i", "Video_Sink/temp/lossy.yuv",
//             // Use simpler VMAF configuration without model_path
//             "-lavfi", "libvmaf=log_fmt=json:log_path=Video_Sink/vmaf.json",
//             "-f", "null", "-"
//         ])
//         .output()?;
        
//     println!("VMAF calculation completed");
//     println!("VMAF calculation output:");
//     println!("STDOUT: {}", String::from_utf8_lossy(&vmaf_output.stdout));
//     println!("STDERR: {}", String::from_utf8_lossy(&vmaf_output.stderr));
    
//     // Run PSNR calculation
//     let psnr_output = Command::new("ffmpeg")
//         .args(&[
//             "-hwaccel", "cuda",
//             "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
//             "-framerate", "60",
//             "-pixel_format", "yuv420p",
//             "-i", "Video_Sink/temp/reference.yuv",
//             "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
//             "-framerate", "60",
//             "-pixel_format", "yuv420p",
//             "-i", "Video_Sink/temp/lossy.yuv",
//             "-lavfi", "psnr=stats_file=Video_Sink/psnr.log",
//             "-f", "null", "-"
//         ])
//         .output()?;
        
//     println!("PSNR calculation completed");
    
//     // Run SSIM calculation
//     let ssim_output = Command::new("ffmpeg")
//         .args(&[
//             "-hwaccel", "cuda",
//             "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
//             "-framerate", "60",
//             "-pixel_format", "yuv420p",
//             "-i", "Video_Sink/temp/reference.yuv",
//             "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
//             "-framerate", "60",
//             "-pixel_format", "yuv420p",
//             "-i", "Video_Sink/temp/lossy.yuv",
//             "-lavfi", "ssim=stats_file=Video_Sink/ssim.log",
//             "-f", "null", "-"
//         ])
//         .output()?;
        
//     println!("SSIM calculation completed");
    
//     // Parse and display results
//     if let Ok(vmaf_json) = std::fs::read_to_string("Video_Sink/vmaf.json") {
//         println!("\nVMAF Results Summary:");
//         if let Some(vmaf_score_idx) = vmaf_json.find("\"vmaf\":") {
//             let score_end = vmaf_json[vmaf_score_idx+7..].find(",").unwrap_or(10);
//             println!("VMAF Score: {}", &vmaf_json[vmaf_score_idx+7..vmaf_score_idx+7+score_end]);
//         }
//     }
    
//     if let Ok(psnr_log) = std::fs::read_to_string("Video_Sink/psnr.log") {
//         let lines: Vec<&str> = psnr_log.lines().collect();
//         if !lines.is_empty() {
//             println!("\nPSNR Results:");
//             println!("{}", lines.last().unwrap_or(&"No PSNR data"));
//         }
//     }
    
//     if let Ok(ssim_log) = std::fs::read_to_string("Video_Sink/ssim.log") {
//         let lines: Vec<&str> = ssim_log.lines().collect();
//         if !lines.is_empty() {
//             println!("\nSSIM Results:");
//             println!("{}", lines.last().unwrap_or(&"No SSIM data"));
//         }
//     }
    
//     println!("\nDetailed results are available in:");
//     println!("- Video_Sink/vmaf.json");
//     println!("- Video_Sink/psnr.log");
//     println!("- Video_Sink/ssim.log");
    
//     Ok(())
// }
pub fn calculate_vmaf_direct() -> Result<(), Box<dyn Error>> {
    println!("Starting direct VMAF calculation from RGB frames...");
    
    // Get current working directory for absolute paths
    let current_dir = std::env::current_dir()?;
    println!("Current working directory: {}", current_dir.display());
    
    // Read the synchronized frame pairs from the CSV file
    let synced_frames_csv = match std::fs::read_to_string("Video_Sink/synced_frames.csv") {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Error reading synced_frames.csv: {}", e);
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Failed to read synchronized frames CSV: {}", e)
            )));
        }
    };
    
    // Parse the CSV to get frame pairs
    let mut valid_pairs = Vec::new();
    
    for line in synced_frames_csv.lines().skip(1) {  // Skip the header
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() >= 4 {
            let frame_number: u64 = parts[0].parse()?;
            let ref_path = parts[2].to_string();
            let lossy_path = parts[3].to_string();
            
            // Check if both files exist and are not empty
            if std::path::Path::new(&ref_path).exists() && std::path::Path::new(&lossy_path).exists() {
                if std::fs::metadata(&ref_path)?.len() > 0 && std::fs::metadata(&lossy_path)?.len() > 0 {
                    valid_pairs.push((frame_number, ref_path, lossy_path));
                } else {
                    eprintln!("Skipping frame {} as files exist but are empty", frame_number);
                }
            } else {
                eprintln!("Skipping frame {} as files don't exist", frame_number);
            }
        }
    }
    
    println!("Found {} valid frame pairs for analysis", valid_pairs.len());
    if valid_pairs.is_empty() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "No valid frame pairs found for VMAF calculation"
        )));
    }
    
    // Create directories for temporary files
    std::fs::create_dir_all("Video_Sink/temp_ref")?;
    std::fs::create_dir_all("Video_Sink/temp_lossy")?;
    
    // Create file lists for ffmpeg with absolute paths
    let ref_list_path = current_dir.join("Video_Sink/temp_ref/list.txt").to_string_lossy().to_string();
    let lossy_list_path = current_dir.join("Video_Sink/temp_lossy/list.txt").to_string_lossy().to_string();
    
    let mut ref_list = std::fs::File::create(&ref_list_path)?;
    let mut lossy_list = std::fs::File::create(&lossy_list_path)?;
    
    println!("Reference list file: {}", ref_list_path);
    println!("Lossy list file: {}", lossy_list_path);
    
    // Prepare Y4M files for each frame (more compatible format for VMAF)
    let mut successful_frames = 0;
    
    for (idx, (frame_num, ref_path, lossy_path)) in valid_pairs.iter().enumerate() {
        // Only process a subset of frames if there are too many (to speed up the process)
        if valid_pairs.len() > SUBSET_FRAMES_VMAF && idx % (valid_pairs.len() / SUBSET_FRAMES_VMAF) != 0 {
            continue;
        }
        
        // Convert each RGB frame to Y4M - use absolute paths without the "../../" prefix
        let ref_y4m = current_dir.join(format!("Video_Sink/temp_ref/frame_{:04}.y4m", frame_num))
            .to_string_lossy().to_string();
        let lossy_y4m = current_dir.join(format!("Video_Sink/temp_lossy/frame_{:04}.y4m", frame_num))
            .to_string_lossy().to_string();
        
        println!("Converting frame {}:", frame_num);
        println!("  Ref: {} -> {}", ref_path, ref_y4m);
        println!("  Lossy: {} -> {}", lossy_path, lossy_y4m);
        
        // Convert reference frame to Y4M with more verbose output
        let ref_output = Command::new("ffmpeg")
            .args(&[
                "-y",                       // Overwrite output files
                "-f", "rawvideo",           // Input is raw video
                "-pixel_format", "rgb24",   // RGB 24-bit format
                "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
                "-i", ref_path,             // Input file
                "-pix_fmt", "yuv420p",      // Convert to YUV
                &ref_y4m                    // Output file (Y4M format)
            ])
            .output()?;
        
        let ref_status = ref_output.status;
        
        if !ref_status.success() {
            eprintln!("Error converting reference frame: {}", String::from_utf8_lossy(&ref_output.stderr));
            continue;
        }
        
        // Convert lossy frame to Y4M
        let lossy_output = Command::new("ffmpeg")
            .args(&[
                "-y",
                "-f", "rawvideo",
                "-pixel_format", "rgb24",
                "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
                "-i", lossy_path,
                "-pix_fmt", "yuv420p",
                &lossy_y4m
            ])
            .output()?;
        
        let lossy_status = lossy_output.status;
        
        if !lossy_status.success() {
            eprintln!("Error converting lossy frame: {}", String::from_utf8_lossy(&lossy_output.stderr));
            continue;
        }
        
        // If both conversions succeeded, add to the lists - use proper file syntax for ffmpeg concat demuxer
        if std::path::Path::new(&ref_y4m).exists() && std::path::Path::new(&lossy_y4m).exists() {
            if std::fs::metadata(&ref_y4m)?.len() > 0 && std::fs::metadata(&lossy_y4m)?.len() > 0 {
                // Use the correct syntax for ffmpeg concat demuxer
                writeln!(ref_list, "file '{}'", ref_y4m.replace("'", "\\'"))?;
                writeln!(lossy_list, "file '{}'", lossy_y4m.replace("'", "\\'"))?;
                successful_frames += 1;
            }
        }
    }
    
    ref_list.flush()?;
    lossy_list.flush()?;
    
    println!("Successfully converted {} frames to Y4M format", successful_frames);
    if successful_frames == 0 {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "No frames were successfully converted to Y4M format"
        )));
    }
    
    // Now run ffmpeg to concatenate and calculate VMAF in one step with verbose output
    println!("Running VMAF calculation...");
    
    let vmaf_output = Command::new("ffmpeg")
        .args(&[
            "-v", "info",                // Verbose output for better debugging
            "-f", "concat",
            "-safe", "0",
            "-i", &ref_list_path,        // Reference video list
            "-f", "concat",
            "-safe", "0",
            "-i", &lossy_list_path,      // Lossy video list
            // "-lavfi", "libvmaf=log_fmt=json:log_path=Video_Sink/vmaf_direct.json",
            "-filter_complex", "[0:v][1:v]libvmaf=log_fmt=json:log_path=Video_Sink/vmaf.json",
            "-filter_complex", "[0:v][1:v]psnr=stats_file=Video_Sink/psnr.log",
            "-filter_complex", "[0:v][1:v]ssim=stats_file=Video_Sink/ssim.log",


            "-f", "null", "-"
        ])
        .output()?;
    
    // Always print FFmpeg output for debugging
    println!("FFmpeg STDOUT: {}", String::from_utf8_lossy(&vmaf_output.stdout));
    println!("FFmpeg STDERR: {}", String::from_utf8_lossy(&vmaf_output.stderr));
    
    // Check if the command was successful
    if !vmaf_output.status.success() {
        println!("VMAF calculation command failed with exit code: {:?}", vmaf_output.status.code());
        
        // Try alternative approach with direct frame comparison if concat method fails
        println!("Trying alternative approach with direct frame comparison...");
        return calculate_vmaf_simple(&valid_pairs[0]);
    }
    
    // Check if the VMAF calculation was successful
    if !std::path::Path::new("Video_Sink/vmaf.json").exists() {
        println!("VMAF calculation failed - output file not created");
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "VMAF calculation failed - output file not created"
        )));
    }
    
    // Read the VMAF results
    let vmaf_json = std::fs::read_to_string("Video_Sink/vmaf.json")?;
    
    // Parse the JSON to extract the VMAF score
    let vmaf_score = if let Some(score_idx) = vmaf_json.find("\"vmaf\":") {
        let end_idx = vmaf_json[score_idx+7..].find(",").unwrap_or(10);
        let score_str = &vmaf_json[score_idx+7..score_idx+7+end_idx];
        score_str.trim().parse::<f64>().unwrap_or(0.0)
    } else {
        0.0 // Default if parsing fails
    };
    
    // Simple visualization in the terminal
    println!("\n=== VMAF Analysis Results ===");
    println!("Overall VMAF Score: {:.2}", vmaf_score);
    
    // Quality category based on VMAF score
    let quality = match vmaf_score {
        score if score >= 90.0 => "Excellent",
        score if score >= 80.0 => "Good",
        score if score >= 70.0 => "Fair",
        score if score >= 60.0 => "Poor",
        _ => "Bad"
    };
    
    println!("Quality Category: {}", quality);
    println!("Visual representation: {}", "#".repeat((vmaf_score / 5.0) as usize));
    println!("\nDetailed results saved to: Video_Sink/vmaf.json");
    
    // // Generate a simple plot
    // if let Err(e) = plot_vmaf_simple(&vmaf_json) {
    //     println!("Warning: Failed to generate VMAF plot: {}", e);
    // }
    
    Ok(())
}

// Fallback method that uses a single frame pair for VMAF calculation
fn calculate_vmaf_simple(frame_pair: &(u64, String, String)) -> Result<(), Box<dyn Error>> {
    println!("Starting simple VMAF calculation with a single frame pair...");
    
    let (frame_num, ref_path, lossy_path) = frame_pair;
    
    // Create directory for simple test
    std::fs::create_dir_all("Video_Sink/temp_simple")?;
    
    // Convert reference frame directly to YUV
    let ref_yuv = format!("Video_Sink/temp_simple/reference_{}.yuv", frame_num);
    let ref_output = Command::new("ffmpeg")
        .args(&[
            "-y", 
            "-f", "rawvideo",
            "-pixel_format", "rgb24",
            "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
            "-i", ref_path,
            "-pix_fmt", "yuv420p",
            &ref_yuv
        ])
        .output()?;
    
    if !ref_output.status.success() {
        println!("Error converting reference frame: {}", String::from_utf8_lossy(&ref_output.stderr));
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to convert reference frame to YUV"
        )));
    }
    
    // Convert lossy frame directly to YUV
    let lossy_yuv = format!("Video_Sink/temp_simple/lossy_{}.yuv", frame_num);
    let lossy_output = Command::new("ffmpeg")
        .args(&[
            "-y", 
            "-f", "rawvideo",
            "-pixel_format", "rgb24",
            "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
            "-i", lossy_path,
            "-pix_fmt", "yuv420p",
            &lossy_yuv
        ])
        .output()?;
    
    if !lossy_output.status.success() {
        println!("Error converting lossy frame: {}", String::from_utf8_lossy(&lossy_output.stderr));
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to convert lossy frame to YUV"
        )));
    }
    
    // Now run simple VMAF calculation on just these two files
    println!("Running VMAF calculation on single frame...");
    
    let vmaf_output = Command::new("ffmpeg")
        .args(&[
            "-v", "info",
            "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
            "-pixel_format", "yuv420p",
            "-i", &ref_yuv,
            "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
            "-pixel_format", "yuv420p",
            "-i", &lossy_yuv,
            "-lavfi", "libvmaf=log_fmt=json:log_path=Video_Sink/vmaf_simple.json",
            "-f", "null", "-"
        ])
        .output()?;
    
    println!("FFmpeg STDOUT: {}", String::from_utf8_lossy(&vmaf_output.stdout));
    println!("FFmpeg STDERR: {}", String::from_utf8_lossy(&vmaf_output.stderr));
    
    if !vmaf_output.status.success() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Simple VMAF calculation failed"
        )));
    }
    
    if !std::path::Path::new("Video_Sink/vmaf_simple.json").exists() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "VMAF calculation failed - output file not created"
        )));
    }
    
    println!("Simple VMAF calculation completed successfully!");
    
    // Read and display results
    let vmaf_json = std::fs::read_to_string("Video_Sink/vmaf_simple.json")?;
    
    // Parse the JSON to extract the VMAF score
    let vmaf_score = if let Some(score_idx) = vmaf_json.find("\"vmaf\":") {
        let end_idx = vmaf_json[score_idx+7..].find(",").unwrap_or(10);
        let score_str = &vmaf_json[score_idx+7..score_idx+7+end_idx];
        score_str.trim().parse::<f64>().unwrap_or(0.0)
    } else {
        0.0 // Default if parsing fails
    };
    
    println!("\n=== Simple VMAF Analysis Results ===");
    println!("Frame {} VMAF Score: {:.2}", frame_num, vmaf_score);
    println!("Quality Category: {}", match vmaf_score {
        score if score >= 90.0 => "Excellent",
        score if score >= 80.0 => "Good",
        score if score >= 70.0 => "Fair",
        score if score >= 60.0 => "Poor",
        _ => "Bad"
    });
    
    Ok(())
}

// fn plot_vmaf_simple(vmaf_json: &str) -> Result<(), Box<dyn Error>> {
//     println!("Generating VMAF plot...");
    
//     // Parse the frames data from the JSON with a more flexible regex
//     let mut frames = Vec::new();
//     let frame_regex = regex::Regex::new(r#"frameNum"\s*:\s*(\d+).*?"vmaf"\s*:\s*([0-9.]+)"#).unwrap();
    
//     for cap in frame_regex.captures_iter(vmaf_json) {
//         if cap.len() >= 3 {
//             let frame_num = cap[1].parse::<usize>().unwrap_or(0);
//             let vmaf = cap[2].parse::<f64>().unwrap_or(0.0);
//             frames.push((frame_num, vmaf));
//         }
//     }
    
//     if frames.is_empty() {
//         println!("No frame data found for plotting. Showing first 200 chars of JSON:");
//         println!("{}", &vmaf_json.chars().take(200).collect::<String>());
//         return Ok(());
//     }
    
//     println!("Found {} frames with VMAF data", frames.len());
    
//     // Sort frames by frame number
//     frames.sort_by_key(|&(num, _)| num);
    
//     // Create the output image file
//     let output_file = "Video_Sink/vmaf_plot_simple.png";
//     let root = BitMapBackend::new(output_file, (800, 600)).into_drawing_area();
//     root.fill(&WHITE)?;
    
//     // Determine value ranges
//     let max_frame = frames.iter().map(|&(num, _)| num).max().unwrap_or(1);
//     let min_vmaf = frames.iter().map(|&(_, vmaf)| vmaf).fold(f64::INFINITY, f64::min).max(0.0);
//     let max_vmaf = frames.iter().map(|&(_, vmaf)| vmaf).fold(f64::NEG_INFINITY, f64::max).min(100.0);
    
//     let mut chart = ChartBuilder::on(&root)
//         .caption("VMAF Scores by Frame", ("sans-serif", 30))
//         .margin(10)
//         .x_label_area_size(30)
//         .y_label_area_size(30)
//         .build_cartesian_2d(
//             0..max_frame, 
//             (min_vmaf - 5.0).max(0.0)..(max_vmaf + 5.0).min(100.0)
//         )?;
    
//     chart.configure_mesh()
//         .x_desc("Frame Number")
//         .y_desc("VMAF Score")
//         .draw()?;
    
//     // Draw the VMAF scores
//     chart.draw_series(LineSeries::new(
//         frames.iter().map(|&(num, vmaf)| (num, vmaf)),
//         &RED,
//     ))?;
    
//     // Draw reference lines for quality thresholds
//     chart.draw_series(LineSeries::new(
//         vec![(0, 80.0), (max_frame, 80.0)],
//         &BLUE.mix(0.5),
//     ))?
//     .label("Good Quality (80)")
//     .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &BLUE.mix(0.5)));
    
//     chart.draw_series(LineSeries::new(
//         vec![(0, 90.0), (max_frame, 90.0)],
//         &GREEN.mix(0.5),
//     ))?
//     .label("Excellent Quality (90)")
//     .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &GREEN.mix(0.5)));
    
//     // Calculate and display average VMAF
//     let avg_vmaf = frames.iter().map(|&(_, vmaf)| vmaf).sum::<f64>() / frames.len() as f64;
//     chart.draw_series(std::iter::once(Text::new(
//         format!("Average VMAF: {:.2}", avg_vmaf),
//         (max_frame / 2, min_vmaf + 10.0),
//         ("sans-serif", 20).into_font(),
//     )))?;
    
//     chart.configure_series_labels()
//         .background_style(&WHITE.mix(0.8))
//         .border_style(&BLACK)
//         .draw()?;
    
//     // Finalize the drawing area
//     root.present()?;
    
//     println!("VMAF plot saved to: {}", output_file);
    
//     Ok(())
// }























///////////////////////////////////////////////////////////////

pub fn calculate_vmaf() -> Result<()> {
    println!("Starting VMAF calculation on synchronized RGB frames...");
    
    // Read the synchronized frame pairs from the CSV file
    let synced_frames_csv = std::fs::read_to_string("Video_Sink/synced_frames.csv")?;
    let mut synced_pairs: Vec<SyncedFramePair> = Vec::new();
    
    for line in synced_frames_csv.lines().skip(1) {  // Skip the header
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() >= 4 {
            synced_pairs.push(SyncedFramePair {
                frame_number: parts[0].parse()?,
                timestamp_ms: parts[1].parse()?,
                ref_path: parts[2].to_string(),
                lossy_path: parts[3].to_string(),
            });
        }
    }
    
    println!("Found {} synchronized frame pairs", synced_pairs.len());
    
    if synced_pairs.is_empty() {
        return Err(anyhow::anyhow!("No synchronized frames found"));
    }
    
    // Create temp directories to convert RGB to YUV (needed for VMAF)
    std::fs::create_dir_all("Video_Sink/temp")?;
    
    // Create raw RGB streams from synchronized frames
    let _ref_concat = File::create("Video_Sink/temp/reference_concat.rgb")?;
    let _lossy_concat = File::create("Video_Sink/temp/lossy_concat.rgb")?;
    
    // Write frame lists for both streams - use absolute paths to avoid path issues
    let mut ref_list = File::create("Video_Sink/temp/ref_frames.txt")?;
    let mut lossy_list = File::create("Video_Sink/temp/lossy_frames.txt")?;
    
    // Get current directory to create absolute paths
    let current_dir = std::env::current_dir()?;
    
    for pair in &synced_pairs {
        // Convert to absolute paths
        let ref_absolute_path = current_dir.join(&pair.ref_path);
        let lossy_absolute_path = current_dir.join(&pair.lossy_path);
        
        // Write the absolute paths to the list files
        writeln!(ref_list, "file '{}'", ref_absolute_path.display())?;
        writeln!(lossy_list, "file '{}'", lossy_absolute_path.display())?;
    }
    
    ref_list.flush()?;
    lossy_list.flush()?;
    
    // Concatenate RGB frames into raw streams using ffmpeg
    let ref_concat_status = Command::new("ffmpeg")
        .args(&[
            "-f", "concat",
            "-safe", "0",
            "-i", "Video_Sink/temp/ref_frames.txt",
            "-c", "copy",
            "-f", "rawvideo",
            "Video_Sink/temp/reference_concat.rgb",
            "-y"
        ])
        .status()?;
        
    if !ref_concat_status.success() {
        eprintln!("Failed to concatenate reference frames");
    }
    
    let lossy_concat_status = Command::new("ffmpeg")
        .args(&[
            "-f", "concat",
            "-safe", "0",
            "-i", "Video_Sink/temp/lossy_frames.txt",
            "-c", "copy",
            "-f", "rawvideo",
            "Video_Sink/temp/lossy_concat.rgb",
            "-y"
        ])
        .status()?;
        
    if !lossy_concat_status.success() {
        eprintln!("Failed to concatenate lossy frames");
    }
    
    // Convert raw RGB streams to YUV format (needed for VMAF)
    let ref_yuv_status = Command::new("ffmpeg")
        .args(&[
            "-f", "rawvideo",
            "-pixel_format", "rgb24",
            "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
            "-framerate", "60",
            "-i", "Video_Sink/temp/reference_concat.rgb",
            "-pix_fmt", "yuv420p",
            "Video_Sink/temp/reference.yuv",
            "-y"
        ])
        .status()?;
        
    if !ref_yuv_status.success() {
        eprintln!("Failed to convert reference to YUV");
    }
    
    let lossy_yuv_status = Command::new("ffmpeg")
        .args(&[
            "-f", "rawvideo",
            "-pixel_format", "rgb24",
            "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
            "-framerate", "60",
            "-i", "Video_Sink/temp/lossy_concat.rgb",
            "-pix_fmt", "yuv420p",
            "Video_Sink/temp/lossy.yuv",
            "-y"
        ])
        .status()?;
        
    if !lossy_yuv_status.success() {
        eprintln!("Failed to convert lossy to YUV");
    }
    
    // Run VMAF calculation without model_path parameter
    let vmaf_output = Command::new("ffmpeg")
        .args(&[
            "-hwaccel", "cuda",
            "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
            "-framerate", "60",
            "-pixel_format", "yuv420p",
            "-i", "Video_Sink/temp/reference.yuv",
            "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
            "-framerate", "60",
            "-pixel_format", "yuv420p",
            "-i", "Video_Sink/temp/lossy.yuv",
            // Remove model_path parameter which is causing errors
            "-lavfi", "[0:v][1:v]libvmaf=log_fmt=json:log_path=Video_Sink/vmaf.json",
            "-f", "null", "-"
        ])
        .output()?;
        
    println!("VMAF calculation completed");
    println!("VMAF calculation output:");
    println!("STDOUT: {}", String::from_utf8_lossy(&vmaf_output.stdout));
    println!("STDERR: {}", String::from_utf8_lossy(&vmaf_output.stderr));
    
    // Run PSNR calculation
    let psnr_output = Command::new("ffmpeg")
        .args(&[
            "-hwaccel", "cuda",
            "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
            "-framerate", "60",
            "-pixel_format", "yuv420p",
            "-i", "Video_Sink/temp/reference.yuv",
            "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
            "-framerate", "60",
            "-pixel_format", "yuv420p",
            "-i", "Video_Sink/temp/lossy.yuv",
            "-lavfi", "psnr=stats_file=Video_Sink/psnr.log",
            "-f", "null", "-"
        ])
        .output()?;
        
    println!("PSNR calculation completed");
    
    // Run SSIM calculation
    let ssim_output = Command::new("ffmpeg")
        .args(&[
            "-hwaccel", "cuda",
            "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
            "-framerate", "60",
            "-pixel_format", "yuv420p",
            "-i", "Video_Sink/temp/reference.yuv",
            "-video_size", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
            "-framerate", "60",
            "-pixel_format", "yuv420p",
            "-i", "Video_Sink/temp/lossy.yuv",
            "-lavfi", "ssim=stats_file=Video_Sink/ssim.log",
            "-f", "null", "-"
        ])
        .output()?;
        
    println!("SSIM calculation completed");
    


    // Parse and display results
    if let Ok(vmaf_json) = std::fs::read_to_string("Video_Sink/vmaf.json") {
        println!("\nVMAF Results Summary:");
        if let Some(vmaf_score_idx) = vmaf_json.find("\"vmaf\":") {
            let score_end = vmaf_json[vmaf_score_idx+7..].find(",").unwrap_or(10);
            println!("VMAF Score: {}", &vmaf_json[vmaf_score_idx+7..vmaf_score_idx+7+score_end]);
        }
    }
    
    if let Ok(psnr_log) = std::fs::read_to_string("Video_Sink/psnr.log") {
        let lines: Vec<&str> = psnr_log.lines().collect();
        if !lines.is_empty() {
            println!("\nPSNR Results:");
            println!("{}", lines.last().unwrap_or(&"No PSNR data"));
        }
    }
    
    if let Ok(ssim_log) = std::fs::read_to_string("Video_Sink/ssim.log") {
        let lines: Vec<&str> = ssim_log.lines().collect();
        if !lines.is_empty() {
            println!("\nSSIM Results:");
            println!("{}", lines.last().unwrap_or(&"No SSIM data"));
        }
    }
    
    println!("\nDetailed results are available in:");
    println!("- Video_Sink/vmaf.json");
    println!("- Video_Sink/psnr.log");
    println!("- Video_Sink/ssim.log");
    

    // Spawn two threads to run the plotting and video playback concurrently.
{
    // let plot_thread = thread::spawn(|| {
    if let Err(e) = plot_vmaf_results() {
        eprintln!("Error plotting VMAF results: {:?}", e);
    }
    // });
    
    // let video_thread = thread::spawn(|| {
    if let Err(e) = play_lossy_video() {
        eprintln!("Error playing lossy video: {:?}", e);
    }
    // });
    
    // Wait for both threads to finish.
    // let _ = plot_thread.join();
    // let _ = video_thread.join();
}







    Ok(())
}




fn parse_index(index_path: &str) -> Result<HashMap<u64, String>> {
    let content = std::fs::read_to_string(index_path)?;
    let mut frames = HashMap::new();
    
    for line in content.lines().skip(1) {
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() >= 4 {
            let frame_number = parts[0].parse::<u64>()?;
            // Don't include Video_Sink again since it's already in the path
            let filename = parts[3].trim().to_string();
            frames.insert(frame_number, filename);
        }
    }
    
    Ok(frames)
}


// Define the structures matching the expected VMAF JSON output.
// This example assumes the JSON looks like:
// {
//    "frames": [
//       { "frame": 0, "vmaf": 98.7 },
//       { "frame": 1, "vmaf": 97.5 },
//       ...
//    ]
// }
#[derive(Deserialize, Debug)]
struct Metrics {
    vmaf: f64,
}

#[derive(Deserialize, Debug)]
struct VmafFrame {
    frameNum: usize,
    metrics: Metrics,
}

#[derive(Deserialize, Debug)]
struct VmafResult {
    frames: Vec<VmafFrame>,
}

/// Plots the VMAF scores (converted to timestamps assuming 60 fps) and saves the chart as a PNG.
/// Plots the VMAF scores (converted to timestamps assuming 60 fps) and saves the chart as a PNG.
/// Includes retries and waits for the file to be available.
fn plot_vmaf_results() -> Result<(), Box<dyn Error>> {
    println!("Preparing to plot VMAF results...");
    
    // Wait for VMAF file to be fully written
    // Try multiple times with increasing delays
    let vmaf_path = "Video_Sink/vmaf.json";
    let max_attempts = 5;
    let mut attempt = 0;
    let mut vmaf_json = String::new();
    
    while attempt < max_attempts {
        attempt += 1;
        println!("Checking for VMAF results file (attempt {}/{})", attempt, max_attempts);
        
        // Check if file exists
        if !std::path::Path::new(vmaf_path).exists() {
            println!("VMAF file not found yet, waiting...");
            std::thread::sleep(std::time::Duration::from_secs(3 * attempt)); // Increasing wait time
            continue;
        }
        
        // Try to read the file
        match std::fs::read_to_string(vmaf_path) {
            Ok(content) => {
                if content.trim().is_empty() {
                    println!("VMAF file exists but is empty, waiting...");
                    std::thread::sleep(std::time::Duration::from_secs(2 * attempt));
                    continue;
                }
                
                // Check if file has valid JSON with frames data
                match serde_json::from_str::<serde_json::Value>(&content) {
                    Ok(json) => {
                        if json.get("frames").is_none() || json["frames"].as_array().map_or(true, |arr| arr.is_empty()) {
                            println!("VMAF file doesn't contain frame data yet, waiting...");
                            std::thread::sleep(std::time::Duration::from_secs(2 * attempt));
                            continue;
                        }
                        
                        // Valid JSON with frames data
                        vmaf_json = content;
                        break;
                    },
                    Err(e) => {
                        println!("VMAF file contains invalid JSON ({}), waiting...", e);
                        std::thread::sleep(std::time::Duration::from_secs(2 * attempt));
                        continue;
                    }
                }
            },
            Err(e) => {
                println!("Error reading VMAF file: {}, waiting...", e);
                std::thread::sleep(std::time::Duration::from_secs(2 * attempt));
                continue;
            }
        }
    }
    
    if vmaf_json.is_empty() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Failed to read valid VMAF data after {} attempts", max_attempts)
        )));
    }
    
    println!("Successfully loaded VMAF data, parsing results...");
    
    // Try to parse the VMAF JSON
    let vmaf_result: Result<VmafResult, _> = serde_json::from_str(&vmaf_json);
    
    let vmaf_result = match vmaf_result {
        Ok(result) => result,
        Err(e) => {
            // If parsing failed, try to extract data using regex as a fallback
            println!("Failed to parse VMAF JSON with serde: {}. Trying alternative parsing...", e);
            
            // Create a simple structure to hold our data
            struct SimpleVmafData {
                frames: Vec<(usize, f64)>, // (frame number, vmaf score)
            }
            
            let mut simple_data = SimpleVmafData { frames: Vec::new() };
            
            // Try to extract frame numbers and VMAF scores using regex
            let frame_regex = regex::Regex::new(r#""frameNum"\s*:\s*(\d+)"#).unwrap();
            let vmaf_regex = regex::Regex::new(r#""vmaf"\s*:\s*([0-9.]+)"#).unwrap();
            
            let mut frame_captures = frame_regex.captures_iter(&vmaf_json).map(|cap| {
                cap[1].parse::<usize>().unwrap_or(0)
            }).collect::<Vec<_>>();
            
            let mut vmaf_captures = vmaf_regex.captures_iter(&vmaf_json).map(|cap| {
                cap[1].parse::<f64>().unwrap_or(0.0)
            }).collect::<Vec<_>>();
            
            // Ensure we have the same number of frames and scores
            let min_len = std::cmp::min(frame_captures.len(), vmaf_captures.len());
            if min_len == 0 {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Could not extract frame data from VMAF results"
                )));
            }
            
            // Pair frame numbers with their scores
            for i in 0..min_len {
                simple_data.frames.push((frame_captures[i], vmaf_captures[i]));
            }
            
            // Convert to our expected structure
            VmafResult {
                frames: simple_data.frames.into_iter().map(|(frame_num, vmaf)| {
                    VmafFrame {
                        frameNum: frame_num,
                        metrics: Metrics { vmaf }
                    }
                }).collect()
            }
        }
    };
    
    // Check if we have frame data to plot
    if vmaf_result.frames.is_empty() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "No frame data found in VMAF results"
        )));
    }
    
    println!("Plotting {} frames of VMAF data", vmaf_result.frames.len());
    
    // Convert frame numbers to timestamps (seconds) and collect (time, score) pairs
    let data: Vec<(f64, f64)> = vmaf_result.frames.iter()
        .map(|f| (f.frameNum as f64 / 60.0, f.metrics.vmaf))
        .collect();
    
    // Determine x-axis range and y-axis range
    let (min_time, max_time) = if let (Some(first), Some(last)) = (data.first(), data.last()) {
        (first.0, last.0)
    } else {
        (0.0, 1.0)
    };
    
    let min_vmaf = data.iter().map(|&(_, v)| v).fold(f64::INFINITY, f64::min).max(0.0);
    let max_vmaf = data.iter().map(|&(_, v)| v).fold(f64::NEG_INFINITY, f64::max).min(100.0);
    
    // Create output directory if it doesn't exist
    if let Some(dir) = std::path::Path::new("Video_Sink/vmaf_plot.png").parent() {
        std::fs::create_dir_all(dir)?;
    }
    
    // Create the output image file
    let output_file = "Video_Sink/vmaf_plot.png";
    
    // BitMapBackend::new returns the backend directly, not a Result
    let root_area = BitMapBackend::new(output_file, (800, 600)).into_drawing_area();
    
    // Fill the background
    root_area.fill(&WHITE)?;
    
    let mut chart = ChartBuilder::on(&root_area)
        .caption("VMAF Score Over Time", ("sans-serif", 40))
        .margin(20)
        .x_label_area_size(40)
        .y_label_area_size(40)
        .build_cartesian_2d(
            min_time..max_time, 
            (min_vmaf - 5.0).max(0.0)..(max_vmaf + 5.0).min(100.0)
        )?;
    
    chart.configure_mesh()
        .x_desc("Time (seconds)")
        .y_desc("VMAF Score")
        .draw()?;
    
    chart.draw_series(LineSeries::new(
        data.clone(),
        &RED,
    ))?;
    
    // Add a reference line for "good quality" threshold at VMAF = 80
    chart.draw_series(LineSeries::new(
        vec![(min_time, 80.0), (max_time, 80.0)],
        &BLUE.mix(0.5),
    ))?
    .label("Good Quality")
    .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &BLUE.mix(0.5)));
    
    // Add a reference line for "excellent quality" threshold at VMAF = 90
    chart.draw_series(LineSeries::new(
        vec![(min_time, 90.0), (max_time, 90.0)],
        &GREEN.mix(0.5),
    ))?
    .label("Excellent Quality")
    .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &GREEN.mix(0.5)));
    
    // Calculate average VMAF
    let avg_vmaf = data.iter().map(|&(_, v)| v).sum::<f64>() / data.len() as f64;
    
    // Add summary statistics
    chart.draw_series(std::iter::once(Text::new(
        format!("Average VMAF: {:.2}", avg_vmaf),
        (min_time + (max_time - min_time) * 0.5, min_vmaf + 5.0),
        ("sans-serif", 20).into_font(),
    )))?;
    
    chart.configure_series_labels()
        .background_style(&WHITE.mix(0.8))
        .border_style(&BLACK)
        .draw()?;
    
    // Finalize the drawing area
    root_area.present()?;
    
    println!("VMAF plot generated at: {}", output_file);
    
    // Don't try to display the image directly since that depends on external libraries
    println!("Plot saved to {}. Please open it with an image viewer.", output_file);
    
    Ok(())
}
/// Uses the `show-image` crate to open the plot image in a window.
fn display_image(image_path: &str) -> Result<(), Box<dyn Error>> {
    use show_image::{create_window, ImageView, ImageInfo};
    use image::io::Reader as ImageReader;
    
    // Load the image from file.
    let img = ImageReader::open(image_path)?.decode()?;
    let img = img.to_rgba8();
    let (width, height) = img.dimensions();
    
    // Create an ImageInfo for an RGBA image.
    let image_info = ImageInfo::rgba8(width, height);
    // Obtain the raw pixel data.
    let raw_data = img.into_raw();
    let image_view = ImageView::new(image_info, &raw_data);
    
    // Create and show the window.
    let window = create_window("VMAF Plot", Default::default())?;
    window.set_image("plot", image_view)?;
    
    // Wait until the window is closed.
    window.wait_until_destroyed()?;
    
    Ok(())
}

/// Plays the lossy video by spawning an external process that runs `ffplay` with the proper arguments.
fn play_lossy_video() -> Result<(), Box<dyn Error>> {
    // Ensure WIDTH_ENCODER and HEIGHT_ENCODER are defined in your project.
    let width = WIDTH_ENCODER;
    let height = HEIGHT_ENCODER;
    let video_file = "Video_Sink/temp/lossy.yuv";  // raw YUV file
    
    // Spawn ffplay with arguments so that it interprets the raw YUV correctly.
    let status = Command::new("ffplay")
        .args(&[
            "-f", "rawvideo",
            "-pixel_format", "yuv420p",
            "-video_size", &format!("{}x{}", width, height),
            "-framerate", "60",
            video_file,
            "-autoexit",          // close when done
            "-loglevel", "quiet",  // suppress console output
        ])
        .status()?;
    
    if !status.success() {
        eprintln!("Failed to play the lossy video.");
    }
    
    Ok(())
}
