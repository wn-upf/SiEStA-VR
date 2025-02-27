




use anyhow::Result;
use std::io::{BufReader, Read, Write};
use std::process::{ChildStdin, ChildStdout};
use ffmpeg_sidecar::command::FfmpegCommand;
use minifb::{Key, Scale, Window, WindowOptions};
use rand::Rng;
use crossbeam::channel::{bounded, unbounded, Receiver, Sender, TryRecvError};
use std::time::Duration;
use std::time::Instant;
use std::collections::VecDeque;
use async_std::task;
// Define the expected (encoder) dimensions.
// pub const WIDTH_ENCODER: usize = 3840;
// pub const HEIGHT_ENCODER: usize = 2160;

pub const WIDTH_ENCODER: usize = 1920;
pub const HEIGHT_ENCODER: usize = 1080;


pub const INITIAL_BITRATE : &str= "2M"; 
pub const WINDOW_SCALE_FACTOR: f64 = 0.5; 

pub const IDR_FRAME_SIZE_GOP: usize = 60;

pub const PACKET_LOSS_PROBABILITY: f64 = 0.000; 



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



pub struct HevcEncoder {
    input_path: String,
    width: u32,
    height: u32,
    current_bitrate: String,
    frame_buffer: VecDeque<Vec<u8>>,  // Buffer for encoded frames
    last_keyframe: Option<Vec<u8>>,    // Store the most recent keyframe
    parser: HevcParser,
    
    // Current encoding process state
    current_child: Option<ffmpeg_sidecar::child::FfmpegChild>,
    current_packet_rx: Option<Receiver<Vec<u8>>>,
    current_stderr_handle: Option<std::thread::JoinHandle<()>>,
    
    // Position tracking
    current_position: f64,  // Current position in seconds
    chunk_duration: f64,    // Duration of each chunk in seconds
    buffer_target_size: usize, // Target number of frames to keep in buffer
}

impl HevcEncoder {
    /// Creates a new HevcEncoder without starting the encoding process
    pub fn new(input_path: &str, width: u32, height: u32, initial_bitrate: &str, 
               chunk_duration: f64, buffer_target_size: usize) -> Result<Self> {
        Ok(Self {
            input_path: input_path.to_string(),
            width,
            height,
            current_bitrate: initial_bitrate.to_string(),
            frame_buffer: VecDeque::new(),
            last_keyframe: None,
            parser: HevcParser::new(),
            current_child: None,
            current_packet_rx: None,
            current_stderr_handle: None,
            current_position: 0.0,
            chunk_duration,
            buffer_target_size,
        })
    }
    
    /// Starts encoding a chunk of video from the current position
    pub fn start_chunk_encoding(&mut self) -> Result<()> {
        // First, stop any current encoding process
        self.stop_encoding()?;
        println!("Starting chunk encoding at position {:.2}s with bitrate {}", 
        self.current_position, self.current_bitrate);

        // Create a new ffmpeg process for encoding the next chunk
        let mut child = FfmpegCommand::new()
            .hwaccel("cuvid")
            .args(&["-ss", &format!("{:.3}", self.current_position)]) // Start from current position
            .args(&["-t", &format!("{:.3}", self.chunk_duration)])    // Encode for chunk_duration seconds
            .input(&self.input_path)
            .args(&["-vf", &format!("scale={}:{}:force_original_aspect_ratio=disable,format=yuv420p", 
                                    self.width, self.height)])
            .args(&["-c:v", "hevc_nvenc"])
            .args(&["-preset", "fast"])
            .args(&["-rc", "cbr"])
            .args(&["-b:v", &self.current_bitrate, "-maxrate", &self.current_bitrate])
            .args(&["-rc-lookahead", "0"])
            .args(&["-g", &format!("{:.0}", IDR_FRAME_SIZE_GOP)])
            .args(&["-movflags", "+frag_keyframe+empty_moov"])
            .args(&["-flush_packets", "1"])
            .args(&["-bsf:v", "hevc_mp4toannexb"])
            .args(&["-an"])
            .args(&["-f", "hevc", "-"]) // Raw HEVC format
            .spawn()?;

        let stdout = child.take_stdout().unwrap();
        let stderr = child.take_stderr().unwrap();

        let (packet_tx, packet_rx) = unbounded();

        // Start stdout reader thread
        std::thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut buf = [0u8; 4096];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => break, // End of stream
                    Ok(n) => {
                        if packet_tx.send(buf[..n].to_vec()).is_err() {
                            break; // Channel closed
                        }
                    }
                    Err(e) => {
                        eprintln!("Encoder read error: {}", e);
                        break;
                    }
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
                    Ok(_) => {
                        // Only print errors, not all stderr output
                        if buf.contains("Error") || buf.contains("error") {
                            eprint!("{}", buf);
                        }
                    }
                    Err(e) => {
                        eprintln!("Encoder stderr read error: {}", e);
                        break;
                    }
                }
            }
        });

        // Store the encoder state
        self.current_child = Some(child);
        self.current_packet_rx = Some(packet_rx);
        self.current_stderr_handle = Some(stderr_handle);
        
        // Update current position for next chunk
        self.current_position += self.chunk_duration;
        
        Ok(())
    }
    
    /// Stops the current encoding process
    pub fn stop_encoding(&mut self) -> Result<()> {
        if let Some(mut child) = self.current_child.take() {
            // Try to terminate the process gracefully
            if let Err(e) = child.kill() {
                eprintln!("Error killing ffmpeg process: {}", e);
                // Continue anyway
            }
            
            // Drop the channel receiver to close it
            self.current_packet_rx.take();
            
            // We won't wait for the stderr handle to complete
            self.current_stderr_handle.take();
        }
        
        Ok(())
    }
    
    /// Changes the encoding bitrate for subsequent chunks
    pub fn set_bitrate(&mut self, new_bitrate: &str) {
        if self.current_bitrate != new_bitrate {
            self.current_bitrate = new_bitrate.to_string();
            
            // If we have an active encoding process, restart it to apply the new bitrate
            if self.current_child.is_some() {
                println!("Restarting encoder to apply new bitrate: {}", new_bitrate);
                // Save current position to restart from the same point
                let current_pos = self.current_position - self.chunk_duration;
                self.current_position = current_pos;
                
                // Attempt to restart encoding
                if let Err(e) = self.start_chunk_encoding() {
                    eprintln!("Failed to restart encoding with new bitrate: {}", e);
                }
            }
        }        // The new bitrate will be used the next time we start encoding
    }
    
    /// Process available encoded packets, similar to the original method
    pub fn process_incoming_packets(&mut self) -> Result<()> {
        if let Some(packet_rx) = &self.current_packet_rx {
            let mut done = false;
            
            while !done {
                match packet_rx.try_recv() {
                    Ok(packet) => {
                        // Add packet data to the parser
                        self.parser.add_data(&packet);
                        
                        // Extract frames from the parser and buffer them
                        let frames = self.parser.get_frames();
                        for frame in frames {
                            // Check if this frame is a keyframe
                            let is_keyframe = Self::is_keyframe(&frame);
                            if is_keyframe {
                                self.last_keyframe = Some(frame.clone());
                            }
                            
                            self.frame_buffer.push_back(frame);
                        }
                    },
                    Err(TryRecvError::Empty) => {
                        done = true;
                    },
                    Err(TryRecvError::Disconnected) => {
                        // Channel is closed, encoding chunk is done
                        // We should start a new one if buffer is getting low
                        return self.check_buffer_status();
                    },
                }
            }
        }
        
        // Check if we should start a new chunk based on buffer status
        self.check_buffer_status()
    }
    
    /// Checks if we need to start encoding a new chunk based on buffer status
    fn check_buffer_status(&mut self) -> Result<()> {
        // If no active encoding and buffer is below target, start a new chunk
        if self.current_child.is_none() && self.frame_buffer.len() < self.buffer_target_size {
            println!("Buffer running low ({} frames), starting new chunk encoding", 
                     self.frame_buffer.len());
            self.start_chunk_encoding()?;
        }
        
        Ok(())
    }
    /// Resets the encoder to start from the beginning of the video
    pub fn reset_to_beginning(&mut self) -> Result<()> {
        self.stop_encoding()?;
        self.current_position = 0.0;
        self.frame_buffer.clear();
        self.last_keyframe = None;
        
        Ok(())
    }
    
    /// Get the next frame from the buffer
    pub fn next_frame(&mut self) -> Option<Vec<u8>> {
        let frame = self.frame_buffer.pop_front();
        
        // Check if we need to refill the buffer
        if let Err(e) = self.check_buffer_status() {
            eprintln!("Error checking buffer status: {}", e);
        }
        
        frame
    }
    
    /// Get the latest keyframe (useful for recovery after packet loss)
    pub fn get_latest_keyframe(&self) -> Option<Vec<u8>> {
        self.last_keyframe.clone()
    }
    
    /// Number of frames waiting in the buffer
    pub fn frames_available(&self) -> usize {
        self.frame_buffer.len()
    }
    
    /// Check if a frame contains a keyframe
    fn is_keyframe(frame: &[u8]) -> bool {
        // Same implementation as before
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
    
    /// Gets the current timestamp position in the video
    pub fn get_current_position(&self) -> f64 {
        self.current_position
    }
    
    // /// Asynchronously waits until the encoder has been initialized
    // pub async fn wait_for_initialization(&mut self) -> Result<()> {
    //     // Poll until at least one frame is in the buffer.
    //     while self.frames_available() == 0 {
    //         // Process any incoming packets.
    //         self.process_incoming_packets()?;
    //         // Sleep briefly to yield to other async tasks.
    //         std::thread::sleep(Duration::from_millis(10));
    //     }
    //     Ok(())
    // }
    
    // /// Asynchronously retrieves the next frame from the encoder.
    // pub async fn get_buffer_emu(&mut self) -> Option<Vec<u8>> {
    //     loop {
    //         if let Some(frame) = self.next_frame() {
    //             return Some(frame);
    //         }
    //         if let Err(e) = self.process_incoming_packets() {
    //             eprintln!("Error processing incoming packets: {}", e);
    //         }
    //         // Yield briefly to allow other async tasks to run.
    //         std::thread::sleep(Duration::from_millis(1));
    //     }
    // }
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
}

impl HevcDecoder {
    pub fn new(framerate: u32, width: u32, height: u32) -> Result<Self> {
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
        })
    }

    // Process incoming encoded packets
    pub fn process_packet(&mut self, packet: Vec<u8>) -> Result<()> {
        // Add packet data to the parser
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
fn main() -> Result<()> {
    ffmpeg_sidecar::download::auto_download()?;
    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/cut_video.mp4";
    println!("Starting video codec simulation..."); 

    // Initialize encoder with chunked processing
    let mut encoder = HevcEncoder::new(
        input_path, 
        WIDTH_ENCODER as u32, 
        HEIGHT_ENCODER as u32, 
        "2M",  // Initial bitrate
        5.0,   // 2-second chunks
        300     // Target to keep 60 frames in buffer (1 second at 60fps)
    )?;
    
    // Start initial encoding
    encoder.start_chunk_encoding()?;
    println!("Started initial encoding at bitrate {}", "2M");
    
    // Wait for initialization - synchronous version
    while encoder.frames_available() == 0 {
        encoder.process_incoming_packets()?;
        std::thread::sleep(Duration::from_millis(10));
    }
    
    let mut decoder = HevcDecoder::new(60, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32)?;
    println!("Encoder and decoder initialized"); 
    
    // Packet loss simulation parameters
    let mut drop_probability = PACKET_LOSS_PROBABILITY; 
    let mut rng = rand::thread_rng();

    // Window setup
    let scale_factor = WINDOW_SCALE_FACTOR;
    let scaled_width = (WIDTH_ENCODER as f64 * scale_factor) as usize;
    let scaled_height = (HEIGHT_ENCODER as f64 * scale_factor) as usize;

    let mut window = Window::new(
        "Video Stream",
        scaled_width,
        scaled_height,
        WindowOptions::default(),
    )?;
    println!("Window opened"); 
    
    // FPS control
    let frame_duration = std::time::Duration::from_secs_f64(1.0 / 60.0);
    let mut next_frame_time = std::time::Instant::now();
    let mut fps_timer = Instant::now(); 
    let mut frames_displayed: usize = 0;
    let mut frames_dropped: usize = 0;
    
    // Frame transmission simulation - we'll use this to control transmission rate
    let transmission_interval = Duration::from_millis(16); // ~60fps
    let mut next_transmission_time = Instant::now();
    
    // Pipeline recovery
    let mut consecutive_failures = 0;
    let max_failures = 5; // Max consecutive failures before recovery
    let mut last_frame_time = Instant::now();
    let frame_timeout = Duration::from_millis(500); // If no frames for 500ms, try recovery

    // Bitrate change schedule (time in seconds, bitrate)
    let bitrate_schedule = vec![
        (10.0, "4M"),   // After 10 seconds, switch to 4 Mbps
        (20.0, "1M"),   // After 20 seconds, switch to 1 Mbps
        (30.0, "8M"),   // After 30 seconds, switch to 8 Mbps
        (40.0, "2M"),   // After 40 seconds, back to 2 Mbps
    ];
    let start_time = Instant::now();
    let mut next_bitrate_idx = 0;

    println!("Starting main loop with initial bitrate 2M");
    
    while window.is_open() && !window.is_key_down(Key::Escape) {
        // Check bitrate schedule
        if next_bitrate_idx < bitrate_schedule.len() {
            let (change_time, new_bitrate) = bitrate_schedule[next_bitrate_idx];
            let elapsed = start_time.elapsed().as_secs_f64();
            
            if elapsed >= change_time {
                println!("Time: {:.1}s - Changing bitrate to {}", elapsed, new_bitrate);
                encoder.set_bitrate(new_bitrate);
                next_bitrate_idx += 1;
            }
        }
        
        // Check if we need to attempt recovery
        if Instant::now().duration_since(last_frame_time) > frame_timeout {
            println!("Pipeline stalled, attempting recovery...");
            
            // Send a keyframe if available to reset decoder state
            if let Some(keyframe) = encoder.get_latest_keyframe() {
                println!("Sending recovery keyframe");
                if let Err(e) = decoder.process_packet(keyframe) {
                    eprintln!("Error sending recovery keyframe: {}", e);
                }
            }
            
            last_frame_time = Instant::now();
        }
        
        // 1. Process incoming encoded data to fill encoder's frame buffer
        if let Err(e) = encoder.process_incoming_packets() {
            eprintln!("Error processing encoder packets: {}", e);
            consecutive_failures += 1;
            
            if consecutive_failures > max_failures {
                eprintln!("Too many failures, attempting recovery");
                // Could restart encoder/decoder here if needed
                consecutive_failures = 0;
            }
            
            continue; // Skip to next iteration
        }
        
        // Reset failure counter if successful
        consecutive_failures = 0;
        
        // 2. Simulate frame-by-frame transmission at a controlled rate
        let now = Instant::now();
        if now >= next_transmission_time && encoder.frames_available() > 0 {
            // Time to transmit a new frame
            if let Some(frame) = encoder.next_frame() {
                // Simulate packet loss
                if rng.gen::<f64>() >= drop_probability {
                    // Send the frame to decoder
                    if let Err(e) = decoder.process_packet(frame) {
                        eprintln!("Error sending frame to decoder: {}", e);
                    }
                } else {
                    println!("Simulated frame loss!");
                    frames_dropped += 1;
                }
                
                // Schedule next transmission
                next_transmission_time += transmission_interval;
            }
        }
        
        // 3. Process any decoded frames
        if let Err(e) = decoder.process_decoded_frames() {
            eprintln!("Error processing decoded frames: {}", e);
        }

        // 4. Display frames at controlled framerate
        let display_time = std::time::Instant::now();
        if display_time >= next_frame_time {
            if let Some(frame) = decoder.next_decoded_frame() {
                // Update the last successful frame time
                last_frame_time = Instant::now();
                
                // Convert and display the frame
                let pixels = convert_rgb_to_u32(&frame, WIDTH_ENCODER, HEIGHT_ENCODER);
                let scaled = scale_pixels(&pixels, WIDTH_ENCODER, HEIGHT_ENCODER, scaled_width, scaled_height);
                
                if let Err(e) = window.update_with_buffer(&scaled, scaled_width, scaled_height) {
                    eprintln!("Error updating window buffer: {}", e);
                } else {
                    frames_displayed += 1;
                }
                
                // Schedule next frame display time
                next_frame_time += frame_duration;
            }
        } else {
            // Sleep to save CPU if it's not time for next frame
            std::thread::sleep(std::cmp::min(
                next_frame_time.saturating_duration_since(display_time),
                Duration::from_millis(1)
            ));
        }
        
        // FPS counter and buffer status
        if Instant::now().duration_since(fps_timer) >= Duration::from_secs(2) {
            let elapsed = start_time.elapsed().as_secs_f64();
            let current_bitrate = if next_bitrate_idx > 0 {
                bitrate_schedule[next_bitrate_idx - 1].1
            } else {
                "2M"
            };
            
            println!("Time: {:.1}s | FPS: {} | Bitrate: {} | Buffer size: {} | Position: {:.2}s", 
                elapsed, frames_displayed / 2, current_bitrate, 
                encoder.frames_available(), encoder.get_current_position());
            
            frames_displayed = 0;
            fps_timer = Instant::now();
        }
        
        // Process window events
        window.update();
        
        // Add keyboard controls for interactive testing
        if window.is_key_pressed(Key::Key1, minifb::KeyRepeat::No) {
            encoder.set_bitrate("1M");
            println!("Manually set bitrate to 1M");
        } else if window.is_key_pressed(Key::Key2, minifb::KeyRepeat::No) {
            encoder.set_bitrate("2M");
            println!("Manually set bitrate to 2M");
        } else if window.is_key_pressed(Key::Key4, minifb::KeyRepeat::No) {
            encoder.set_bitrate("4M");
            println!("Manually set bitrate to 4M");
        } else if window.is_key_pressed(Key::Key8, minifb::KeyRepeat::No) {
            encoder.set_bitrate("8M");
            println!("Manually set bitrate to 8M");
        }
    }
    
    // Clean shutdown
    println!("Stopping encoder...");
    encoder.stop_encoding()?;
    println!("Exiting gracefully...");
    Ok(())
}