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

// Define the expected (encoder) dimensions.
pub const WIDTH_ENCODER: usize = 3840;
pub const HEIGHT_ENCODER: usize = 2160;

pub const INITIAL_BITRATE : &str= "2M"; 
pub const WINDOW_SCALE_FACTOR: f64 = 0.5; 

pub const IDR_FRAME_SIZE_GOP: usize = 300;

pub const PACKET_LOSS_PROBABILITY: f64 = 0.01; 



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
    packet_rx: Receiver<Vec<u8>>,
    _child: ffmpeg_sidecar::child::FfmpegChild,
    _stderr_handle: std::thread::JoinHandle<()>,
    parser: HevcParser,
    frame_buffer: VecDeque<Vec<u8>>,  // Buffer for encoded frames
    last_keyframe: Option<Vec<u8>>,    // Store the most recent keyframe
}
impl HevcEncoder {
    pub fn new(input: &str, width: u32, height: u32, bitrate: &str) -> Result<Self> {
        let mut child = FfmpegCommand::new()
            .hwaccel("cuvid")
            .args(&["-re"]) // Read input at real-time speed
            .args(&["-stream_loop", "-1"]) // Loop input indefinitely
            .input(input)
            .args(&["-vf", &format!("scale={}:{}:force_original_aspect_ratio=disable,format=yuv420p", width, height)])
            .args(&["-c:v", "hevc_nvenc"])
            .args(&["-preset", "fast"])
            .args(&["-rc", "cbr"])
            .args(&["-b:v", bitrate, "-maxrate", bitrate])
            .args(&["-rc-lookahead", "0"])
            .args(&["-g", &format!("{:.0}", IDR_FRAME_SIZE_GOP) ])
            .args(&["-movflags", "+frag_keyframe+empty_moov"])
            .args(&["-flush_packets", "1"])
            .args(&["-bsf:v", "hevc_mp4toannexb"])
            .args(&["-an"])
            // .args(&["-f", "mp4", "-"])
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
                    Ok(0) => break,
                    Ok(n) => {
                        packet_tx.send(buf[..n].to_vec()).unwrap();
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
                    Ok(_) => eprint!("{}", buf),
                    Err(e) => {
                        eprintln!("Encoder stderr read error: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(Self {
            packet_rx,
            _child: child,
            _stderr_handle: stderr_handle,
            parser: HevcParser::new(),
            frame_buffer: VecDeque::new(),
            last_keyframe: None,
        })
    }

    // pub fn try_next_packet(&mut self) -> Result<Option<Vec<u8>>> {
    //     match self.packet_rx.try_recv() {
    //         Ok(packet) => {
    //             // Add packet data to the parser
    //             self.parser.add_data(&packet);
                
    //             // Extract frames from the parser and buffer them
    //             let frames = self.parser.get_frames();
    //             for frame in frames {
    //                 // Check if this frame is a keyframe
    //                 let is_keyframe = Self::is_keyframe(&frame);
    //                 if is_keyframe {
    //                     self.last_keyframe = Some(frame.clone());
    //                 }
                    
    //                 self.frame_buffer.push_back(frame);
    //             }
                
    //             Ok(Some(packet))
    //         },
    //         Err(TryRecvError::Empty) => Ok(None),
    //         Err(TryRecvError::Disconnected) => Err(anyhow::anyhow!("Encoder channel disconnected")),
    //     }
    // }

    // pub fn process_incoming_packets(&mut self) -> Result<()> {
    //     while let Ok(Some(_)) = self.try_next_packet() {
    //         // Just process the packets to fill our frame buffer
    //     }
    //     Ok(())
    // }
    pub fn try_next_packet(&mut self) -> Result<Option<Vec<u8>>> {
        // println!("HevcEncoder::try_next_packet - trying to receive packet"); // ADDED LOG
        match self.packet_rx.try_recv() {
            Ok(packet) => {
                // println!("HevcEncoder::try_next_packet - received packet of size: {}", packet.len()); // ADDED LOG
                // Add packet data to the parser
                self.parser.add_data(&packet);

                // Extract frames from the parser and buffer them
                let frames = self.parser.get_frames();
                // println!("HevcEncoder::try_next_packet - extracted {} frames from packet", frames.len()); // ADDED LOG
                for frame in frames {
                    // Check if this frame is a keyframe
                    let is_keyframe = Self::is_keyframe(&frame);
                    if is_keyframe {
                        self.last_keyframe = Some(frame.clone());
                    }

                    self.frame_buffer.push_back(frame);
                }

                Ok(Some(packet))
            },
            Err(TryRecvError::Empty) => {
                // println!("HevcEncoder::try_next_packet - channel empty"); // ADDED LOG
                Ok(None)
            },
            Err(TryRecvError::Disconnected) => {
                eprintln!("HevcEncoder::try_next_packet - channel disconnected"); // Existing error log
                Err(anyhow::anyhow!("Encoder channel disconnected"))
            }
        }
    }

    pub fn process_incoming_packets(&mut self) -> Result<()> {
        // println!("HevcEncoder::process_incoming_packets - start processing"); // ADDED LOG
        let mut packets_processed = 0;
        while let Ok(Some(_)) = self.try_next_packet() {
            packets_processed += 1; // Count processed packets
            // Just process the packets to fill our frame buffer
        }
        // println!("HevcEncoder::process_incoming_packets - processed {} packets", packets_processed); // ADDED LOG
        Ok(())
    }
    
    
    // Get the next frame from the buffer
    pub fn next_frame(&mut self) -> Option<Vec<u8>> {
        self.frame_buffer.pop_front()
    }


    
    // Check if a frame contains a keyframe
    fn is_keyframe(frame: &[u8]) -> bool {
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
    // Get the latest keyframe (useful for recovery after packet loss)
    pub fn get_latest_keyframe(&self) -> Option<Vec<u8>> {
        self.last_keyframe.clone()
    }
    
    // Number of frames waiting in the buffer
    pub fn frames_available(&self) -> usize {
        self.frame_buffer.len()
    }

    /// Asynchronously waits until the encoder has been initialized (i.e. there is at least one frame available).
    pub async fn wait_for_initialization(&mut self) -> Result<()> {
        // Poll until at least one frame is in the buffer.
        while self.frames_available() == 0 {
            // Process any incoming packets.
            self.process_incoming_packets()?;
            // Sleep briefly to yield to other async tasks.
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }

    /// Asynchronously retrieves the next frame from the encoder.
    /// This function is async-friendly and yields the next available frame.
    pub async fn get_buffer_emu(&mut self) -> Option<Vec<u8>> {
        loop {
            if let Some(frame) = self.next_frame() {
                return Some(frame);
            }
            if let Err(e) = self.process_incoming_packets() {
                eprintln!("Error processing incoming packets: {}", e);
            }
            // Yield briefly to allow other async tasks to run.
            std::thread::sleep(Duration::from_millis(1));
        }
    }
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

    let mut encoder = HevcEncoder::new(input_path, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32, &INITIAL_BITRATE)?;
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

    while window.is_open() && !window.is_key_down(Key::Escape) {
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
        
        // FPS counter
        if Instant::now().duration_since(fps_timer) >= Duration::from_secs(2) {
            println!("FPS: {}", frames_displayed / 2); 
            frames_displayed = 0;
            fps_timer = Instant::now();
        }
        
        // Process window events
        window.update();
        
        // // Toggle packet loss with spacebar
        // if window.is_key_pressed(Key::Space, minifb::KeyRepeat::No) {
        //     if drop_probability == 0.0 {
        //         drop_probability = 0.05;
        //         println!("Packet loss simulation enabled (5%)");
        //     } else {
        //         drop_probability = 0.0;
        //         println!("Packet loss simulation disabled");
        //     }
        // }
    }
    
    println!("Exiting gracefully...");
    Ok(())
}