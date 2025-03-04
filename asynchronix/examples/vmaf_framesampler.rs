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

use std::fs::File;
use std::path::Path;
use std::process::Command; 

// Define the expected (encoder) dimensions.
// pub const WIDTH_ENCODER: usize = 3840;
// pub const HEIGHT_ENCODER: usize = 2160;
pub const WIDTH_ENCODER: usize = 1920;
pub const HEIGHT_ENCODER: usize = 1080;

pub const INITIAL_BITRATE : &str= "30M"; 
pub const WINDOW_SCALE_FACTOR: f64 = 0.7; 

pub const IDR_FRAME_SIZE_GOP: usize = 300;

pub const PACKET_LOSS_PROBABILITY: f64 = 0.01; 

pub const CHUNK_SIZE_ENCODER_S: f64 = 10.0; 
pub const FRAME_CUTOFF_LIMIT: usize = 1000; 

pub const OFFSET_VIDEO: f64 = 0.0;

pub const REENCODE: bool = false; 

#[derive(Debug, Clone)]
struct FrameMetadata {
    frame_number: u64,
    timestamp_ms: u64,
    is_keyframe: bool,
}

struct VmafTracker {
    csv_writer: BufWriter<File>,
    frame_counter: u64,
    last_vmaf_score: Option<f64>,
}

impl VmafTracker {
    fn new() -> Result<Self, io::Error> {
        // Create directory if it doesn't exist
        fs::create_dir_all("Video_Sink/vmaf")?;
        
        // Create CSV file for VMAF scores
        let vmaf_file = File::create("Video_Sink/vmaf/vmaf_scores.csv")?;
        let mut csv_writer = BufWriter::new(vmaf_file);
        
        // Write CSV header
        writeln!(csv_writer, "frame_number,timestamp_ms,vmaf_score")?;
        
        Ok(VmafTracker {
            csv_writer,
            frame_counter: 0,
            last_vmaf_score: None,
        })
    }
    
    fn save_frame_pair(&mut self, ref_frame: &[u8], lossy_frame: &[u8], width: usize, height: usize, timestamp_ms: u64) -> Result<(), io::Error> {
        self.frame_counter += 1;
        
        // Create directories for YUV frames if they don't exist
        fs::create_dir_all("Video_Sink/vmaf/reference_yuv")?;
        fs::create_dir_all("Video_Sink/vmaf/lossy_yuv")?;
        
        // Convert RGB to YUV420p and save reference frame
        let ref_yuv_path = format!("Video_Sink/vmaf/reference_yuv/frame_{:04}.yuv", self.frame_counter);
        rgb_to_yuv420p(ref_frame, width, height, &ref_yuv_path)?;
        
        // Convert RGB to YUV420p and save lossy frame
        let lossy_yuv_path = format!("Video_Sink/vmaf/lossy_yuv/frame_{:04}.yuv", self.frame_counter);
        rgb_to_yuv420p(lossy_frame, width, height, &lossy_yuv_path)?;
        
        // Calculate VMAF for this frame pair
        let vmaf_score = calculate_frame_vmaf(&ref_yuv_path, &lossy_yuv_path, width, height)?;
        self.last_vmaf_score = Some(vmaf_score);
        
        // Write to CSV
        writeln!(self.csv_writer, "{},{},{:.4}", self.frame_counter, timestamp_ms, vmaf_score)?;
        self.csv_writer.flush()?;
        
        println!("Frame #{}: VMAF Score = {:.2}", self.frame_counter, vmaf_score);
        
        Ok(())
    }
    
    fn get_last_vmaf_score(&self) -> Option<f64> {
        self.last_vmaf_score
    }
}

// Convert RGB to YUV420p and save to file
fn rgb_to_yuv420p(rgb_data: &[u8], width: usize, height: usize, output_path: &str) -> Result<(), io::Error> {
    // Create a temp RGB file
    let temp_rgb_path = format!("{}.rgb", output_path);
    let mut rgb_file = File::create(&temp_rgb_path)?;
    rgb_file.write_all(rgb_data)?;
    
    // Use ffmpeg to convert RGB to YUV420p
    let status = Command::new("ffmpeg")
        .args(&[
            "-f", "rawvideo",
            "-pixel_format", "rgb24",
            "-video_size", &format!("{}x{}", width, height),
            "-i", &temp_rgb_path,
            "-f", "rawvideo",
            "-pix_fmt", "yuv420p",
            "-y", output_path
        ])
        .status()?;
    
    // Remove temp RGB file
    fs::remove_file(temp_rgb_path)?;
    
    if !status.success() {
        return Err(io::Error::new(io::ErrorKind::Other, "Failed to convert RGB to YUV420p"));
    }
    
    Ok(())
}

// Calculate VMAF for a single frame pair
fn calculate_frame_vmaf(ref_yuv_path: &str, lossy_yuv_path: &str, width: usize, height: usize) -> Result<f64, io::Error> {
    // Create a temporary file to store VMAF output
    let vmaf_output = "Video_Sink/vmaf/temp_vmaf_output.json";
    
    // Run ffmpeg with libvmaf to calculate VMAF
    let status = Command::new("ffmpeg")
        .args(&[
            "-f", "rawvideo",
            "-pixel_format", "yuv420p", 
            "-video_size", &format!("{}x{}", width, height),
            "-i", ref_yuv_path,
            "-f", "rawvideo",
            "-pixel_format", "yuv420p",
            "-video_size", &format!("{}x{}", width, height),
            "-i", lossy_yuv_path,
            "-lavfi", "libvmaf=log_fmt=json:log_path=Video_Sink/vmaf/temp_vmaf_output.json:n_threads=4",
            "-f", "null", "-"
        ])
        .status()?;
    
    if !status.success() {
        return Err(io::Error::new(io::ErrorKind::Other, "Failed to calculate VMAF"));
    }
    
    // Parse the VMAF output
    let vmaf_json = fs::read_to_string(vmaf_output)?;
    
    // Very simple JSON parsing to extract VMAF score
    // In a real implementation, you would use a proper JSON parser
    if let Some(idx) = vmaf_json.find("\"vmaf\":") {
        let score_start = idx + 7; // length of "\"vmaf\":"
        let score_end = vmaf_json[score_start..].find(',').unwrap_or(vmaf_json[score_start..].len());
        let vmaf_score = vmaf_json[score_start..score_start+score_end].trim().parse::<f64>().unwrap_or(0.0);
        return Ok(vmaf_score);
    }
    
    Err(io::Error::new(io::ErrorKind::Other, "Failed to parse VMAF score"))
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
    /// As data is read from ffmpeg’s stdout, it is fed to a HevcParser which extracts complete frames.
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
                                frame_tx.send(frame).unwrap();
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

#[async_std::main]
async fn main() -> Result<()> {
    // ffmpeg_sidecar::download::auto_download()?;


    let mut num_updates_ref = 0; 
    let EPOCH = Instant::now(); 
    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/cut_video.mp4";
    println!("Starting video codec simulation...");

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

    if REENCODE == true{
        // Spawn the chunking task in the background.
        async_std::task::spawn(async move {
            if let Err(e) = chunked_encoder.start_chunking().await {
                eprintln!("Chunking task error: {}", e);
            }
        });

        // Create the decoder as before.
        let mut ref_decoder = HevcDecoder::new(60, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32, EPOCH)?;
        let mut lossy_decoder = HevcDecoder::new(60, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32, EPOCH)?;

        // let mut ref_file = BufWriter::new(File::create("Video_Sink/reference_video.rgb")?);
        // let mut lossy_file = BufWriter::new(File::create("Video_Sink/lossy_video.rgb")?);


        // let mut reference_frames_map = HashMap::new();
        // let mut lossy_frames_map = HashMap::new(); 
        let mut frame_counter: u64 = 0;

        std::fs::create_dir_all("Video_Sink/reference_hevc")?;
        std::fs::create_dir_all("Video_Sink/lossy_hevc")?;

        // Index file writers
        let mut ref_index = BufWriter::new(File::create("Video_Sink/reference_index.csv")?);
        let mut lossy_index = BufWriter::new(File::create("Video_Sink/lossy_index.csv")?);

        // Write CSV headers
        writeln!(ref_index, "frame_number,timestamp_ms,is_keyframe,filename")?;
        writeln!(lossy_index, "frame_number,timestamp_ms,is_keyframe,filename")?;





        let frame_size_rgb = WIDTH_ENCODER * HEIGHT_ENCODER * 3;
        let black_frame = vec![0u8; frame_size_rgb];
        let mut last_lossy_frame: Option<Vec<u8>> = None;


        println!("Encoder and decoder initialized");

        // Window setup.
        let scale_factor = WINDOW_SCALE_FACTOR;
        let scaled_width = (WIDTH_ENCODER as f64 * scale_factor) as usize;
        let scaled_height = (HEIGHT_ENCODER as f64 * scale_factor) as usize;
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
                        println!("Simulated frame loss!");
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

            if let Some(frame) = ref_decoder.next_encoded_frame() {

                frame_counter += 1; 
                
                let timestamp = Instant::now().duration_since(EPOCH).as_millis() as u64;
                let is_keyframe = HevcDecoder::is_keyframe(&frame);
                let metadata = FrameMetadata {
                    frame_number: frame_counter,
                    timestamp_ms: timestamp,
                    is_keyframe,
                };

                let filename = format!("Video_Sink/reference_hevc/frame_{:04}.hevc", frame_counter);
                let mut file = File::create(&filename)?;
                file.write_all(&frame)?;

                writeln!(
                    ref_index, 
                    "{},{},{},{}", 
                    metadata.frame_number, 
                    metadata.timestamp_ms, 
                    metadata.is_keyframe, 
                    filename
                )?;
                // reference_frames_map.insert(frame_counter, metadata);

            
                if let Some(lossy_frame) = lossy_decoder.next_encoded_frame() {   // ref frames always should appear, lossy frames not guaranteed so it can be inside scope of "let Some(ref_frame)"
                    let timestamp = Instant::now().duration_since(EPOCH).as_millis() as u64;
                    let is_keyframe = HevcDecoder::is_keyframe(&lossy_frame);
                    let metadata = FrameMetadata {
                        frame_number: frame_counter,
                        timestamp_ms: timestamp,
                        is_keyframe,
                    };
                    
                    // Save encoded HEVC frame
                    let filename = format!("Video_Sink/lossy_hevc/frame_{:04}.hevc", frame_counter);
                    let mut file = File::create(&filename)?;
                    file.write_all(&lossy_frame)?;
                    
                    // Update index
                    writeln!(
                        lossy_index, 
                        "{},{},{},{}", 
                        metadata.frame_number, 
                        metadata.timestamp_ms, 
                        metadata.is_keyframe, 
                        filename
                    )?;
                    
                    // lossy_frames_map.insert(frame_counter, metadata);
                }
            
            
            
            }
        
            // 4. Display frames.
            let display_time = Instant::now();
            if display_time >= next_frame_time {
                if let Some(frame) = ref_decoder.next_decoded_frame() {

                    // println!("Frame got on decoder, size: {}", frame.len());

                    // Update the last successful frame time.
                    // Convert and display the frame.
                    let pixels = convert_rgb_to_u32(&frame, WIDTH_ENCODER, HEIGHT_ENCODER);
                    let scaled = scale_pixels(&pixels, WIDTH_ENCODER, HEIGHT_ENCODER, scaled_width, scaled_height);
                    if let Err(e) = reference_window.update_with_buffer(&scaled, scaled_width, scaled_height) {
                        eprintln!("Error updating window buffer: {}", e);
                    } else {
                        frames_displayed += 1;
                    }
                    last_frame_time = Instant::now();


                    if let Some(lossy_frame) = lossy_decoder.next_decoded_frame() {
                        let pixels = convert_rgb_to_u32(&lossy_frame, WIDTH_ENCODER, HEIGHT_ENCODER);
                        let scaled = scale_pixels(&pixels, WIDTH_ENCODER, HEIGHT_ENCODER, scaled_width, scaled_height);
                        last_lossy_frame = Some(lossy_frame.clone());
                        lossy_window.update_with_buffer(&scaled, scaled_width, scaled_height)?;

                        // lossy_file.write_all(&lossy_frame)?;
                    }
                    else if let Some(prev_lossy_frame) = last_lossy_frame.clone() {
                        // lossy_file.write_all(&prev_lossy_frame)?;
                    }
                    else{
                        // lossy_file.write_all(&black_frame.clone())?;
                        }


                    next_frame_time += frame_duration;
                }
            } else {
                // Yield to other tasks.
                async_std::task::sleep(Duration::from_millis(1)).await;
            }

            // FPS counter.
            if Instant::now().duration_since(fps_timer) >= Duration::from_secs(2) {
                println!("FPS: {}", frames_displayed / 2);
                frames_displayed = 0;
                fps_timer = Instant::now();
            }

            reference_window.update();
            lossy_window.update(); 

            num_updates_ref += 1; 
            if num_updates_ref >= FRAME_CUTOFF_LIMIT {
                println!("REACHED {} FRAME LIMIT!", FRAME_CUTOFF_LIMIT); 
                std::thread::sleep(Duration::from_secs(5));
                break;
            }
        }
    }
   



    calculate_vmaf()?;

    println!("Exiting gracefully...");
    Ok(())
}



pub fn calculate_vmaf() -> Result<()> {
    // Step 1: Create temporary YUV files from our frame collections
    
    // First, read indexes to determine available frames
    let ref_frames = parse_index("Video_Sink/reference_index.csv")?;
    let lossy_frames = parse_index("Video_Sink/lossy_index.csv")?;
    



    // Find common frames between reference and lossy videos
    let mut common_frames: Vec<u64> = ref_frames.keys()
        .filter(|k| lossy_frames.contains_key(k))
        .cloned()
        .collect();
    common_frames.sort(); 

    println!("Found {} synchronized frames for VMAF comparison", common_frames.len());
    // println!("\n\n**^***Common frames are the following: {:?}", common_frames);

    // std::thread::sleep(Duration::from_secs(4)); 



    // Create synchronized frame lists
    let mut ref_list = File::create("Video_Sink/ref_frames.txt")?;
    let mut lossy_list = File::create("Video_Sink/lossy_frames.txt")?;
    


    for frame_num in &common_frames{
        let ref_path = ref_frames[frame_num].replace("Video_Sink/", "");
        let lossy_path = lossy_frames[frame_num].replace("Video_Sink/", "");
        writeln!(ref_list, "file '{}'", ref_path)?;
        writeln!(lossy_list, "file '{}'", lossy_path)?;
    }
    
    ref_list.flush()?;
    lossy_list.flush()?;
    
   // Create a raw reference video from your frames
   Command::new("ffmpeg")
   .args(&[
       "-f", "hevc",
       "-i", &ref_frames[&common_frames[0]], // Use first frame to get metadata
       "-safe", "0",
       "-c", "copy",
       "-bsf:v", "hevc_mp4toannexb", // Ensure proper bitstream formatting
       "-f", "mp4",
       "Video_Sink/reference_temp.mp4",
       "-y", 

   ])
   .status()?;

    // Create a raw lossy video from your frames
    Command::new("ffmpeg")
    .args(&[
        "-f", "hevc",
        "-i", &lossy_frames[&common_frames[0]], // Use first frame to get metadata  
        "-safe", "0",
        "-c", "copy",
        "-bsf:v", "hevc_mp4toannexb", // Ensure proper bitstream formatting
        "-f", "mp4",
        "Video_Sink/lossy_temp.mp4", 
        "-y", 
    ])
    .status()?;


    println!("TODO: Run VMAF calculation on the synchronized videos!!");
    std::thread::sleep(Duration::from_millis(6000)); 
    // // Run VMAF directly on the MP4 files (which have complete headers)
    // let output = Command::new("ffmpeg")
    // .args(&[
    //     "-i", "Video_Sink/reference_temp.mp4",
    //     "-i", "Video_Sink/lossy_temp.mp4",
    //     "-filter_complex", "[0:v][1:v]libvmaf=log_fmt=json:log_path=vmaf.json",
    //     "-filter_complex", "[0:v][1:v]psnr=stats_file=psnr.log" ,
    //     "-filter_complex", "[0:v][1:v]ssim=stats_file=ssim.log" ,
    //     "-f", "null -"
    // ])
    // .output()?;

    // println!("VMAF calculation completed.");
    // println!("STDOUT: {}", String::from_utf8_lossy(&output.stdout));
    // println!("STDERR: {}", String::from_utf8_lossy(&output.stderr));

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