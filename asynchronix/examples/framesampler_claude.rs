use anyhow::Result;
use std::io::{BufReader, Read, Write};
use std::process::{Command, Stdio};
use ffmpeg_sidecar::command::FfmpegCommand;
use minifb::{Key, Scale, Window, WindowOptions};
use rand::Rng;
use crossbeam::channel::{bounded, unbounded, Receiver, Sender, TrySendError, TryRecvError};
use std::time::Duration;
use std::time::Instant;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::thread;

// Define the expected (encoder) dimensions.
pub const WIDTH_ENCODER: usize = 1920;
pub const HEIGHT_ENCODER: usize = 1080;

pub const INITIAL_BITRATE: &str = "2M"; 
pub const WINDOW_SCALE_FACTOR: f64 = 0.5; 

pub const IDR_FRAME_SIZE_GOP: usize = 60;

pub const PACKET_LOSS_PROBABILITY: f64 = 0.000; 

/// Represents a single HEVC NAL unit with metadata
#[derive(Clone, Debug)]
pub struct NalUnit {
    pub nal_type: u8,
    pub data: Vec<u8>,
    pub is_keyframe: bool,
    pub timestamp: u64,  // Timestamp in milliseconds
    pub sequence: u32,   // Sequence number for packet ordering
}

/// A parser for HEVC bitstreams to extract individual NAL units
pub struct HevcParser {
    buffer: Vec<u8>,
    sequence_counter: u32,
    timestamp_base: Instant,
    vps_sps_pps: Option<Vec<u8>>, // Store HEVC parameter sets (VPS, SPS, PPS) for recovery
}

impl HevcParser {
    pub fn new() -> Self {
        Self { 
            buffer: Vec::new(),
            sequence_counter: 0,
            timestamp_base: Instant::now(),
            vps_sps_pps: None,
        }
    }

    /// Add more encoded data to the parser buffer
    pub fn add_data(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Find the next NAL unit start code in the buffer
    fn find_next_start_code(&self, start_pos: usize) -> Option<usize> {
        for i in start_pos..self.buffer.len().saturating_sub(3) {
            // Look for 0x000001 or 0x00000001 (3 or 4 byte start codes)
            if (self.buffer[i] == 0 && self.buffer[i + 1] == 0 && self.buffer[i + 2] == 1) || 
               (i < self.buffer.len() - 4 && self.buffer[i] == 0 && self.buffer[i + 1] == 0 && 
                self.buffer[i + 2] == 0 && self.buffer[i + 3] == 1) {
                return Some(i);
            }
        }
        None
    }
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
        
        // Create the complete NAL unit including the start code
        let mut nal_data = Vec::new();
        // Add the start code (always use 4-byte version for consistency)
        nal_data.extend_from_slice(&[0, 0, 0, 1]);
        // Add the NAL unit data (including header)
        nal_data.extend_from_slice(&self.buffer[nal_header_pos..nal_end]);
        
        // Determine if this is a keyframe (I-frame)
        // In HEVC, NAL types 16-21 represent IRAP (Intra Random Access Point) pictures
        let is_keyframe = (16..=21).contains(&nal_type);
        
        // Parameter sets (VPS=32, SPS=33, PPS=34)
        let is_param_set = (32..=34).contains(&nal_type);
        
        if is_param_set {
            if let Some(ref mut param_sets) = self.vps_sps_pps {
                if !param_sets.contains(&nal_data) {
                    param_sets.extend_from_slice(&nal_data);
                    println!("Updated parameter sets, now {} bytes", param_sets.len());
                }
            }
        }
        
        // Remove the processed NAL unit from the buffer
        self.buffer.drain(0..nal_end);
        
        // Create timestamp (milliseconds since parser creation)
        let timestamp = self.timestamp_base.elapsed().as_millis() as u64;
        
        // Increment sequence counter
        self.sequence_counter += 1;
        
        Some(NalUnit {
            nal_type,
            data: nal_data,
            is_keyframe,
            timestamp,
            sequence: self.sequence_counter,
        })
    }

    /// Extract all complete NAL units from buffer
    pub fn get_nal_units(&mut self) -> Vec<NalUnit> {
        let mut nal_units = Vec::new();
        
        while let Some(nal) = self.next_nal_unit() {
            println!("Found NAL unit type {}, keyframe: {}", nal.nal_type, nal.is_keyframe);
            nal_units.push(nal);
        }
        
        nal_units
    }

    pub fn get_frames(&mut self) -> Vec<Vec<u8>> {
        // First extract all NAL units
        let nal_units = self.get_nal_units();
        
        if nal_units.is_empty() {
            return Vec::new();
        }
        
        // Print detailed info about NAL units
        println!("Processing {} NAL units for frame assembly", nal_units.len());
        for (i, nal) in nal_units.iter().enumerate() {
            println!("NAL #{}: type={}, keyframe={}, size={} bytes", 
                     i, nal.nal_type, nal.is_keyframe, nal.data.len());
        }
        
        let mut frames = Vec::new();
        let mut current_frame = Vec::new();
        if let Some(params) = &self.vps_sps_pps {
            current_frame.extend_from_slice(params);
        }
        
        let mut last_nal_was_vcl = false;
        
        for nal in &nal_units {
            current_frame.extend_from_slice(&[0, 0, 0, 1]);
            current_frame.extend_from_slice(&nal.data);
            
            // Start new frame after each keyframe
            if nal.is_keyframe && !current_frame.is_empty() {
                frames.push(current_frame);
                current_frame = Vec::new();
                if let Some(params) = &self.vps_sps_pps {
                    current_frame.extend_from_slice(params);
                }
            }
        }
        
        // Add the last frame if not empty
        if !current_frame.is_empty() {
            frames.push(current_frame);
            println!("Added final frame of size {} bytes", frames.last().unwrap().len());
        }
        
        println!("Successfully assembled {} complete frames", frames.len());
        frames
    }

    /// Get stored parameter sets (VPS, SPS, PPS)
    pub fn get_parameter_sets(&self) -> Option<Vec<u8>> {
        self.vps_sps_pps.clone()
    }
}

/// Control commands for the encoder
enum EncoderControl {
    ChangeBitrate(String),
    ForceKeyframe,
    Terminate,
}

/// Enhanced chunk-based HEVC encoder
pub struct ChunkedHevcEncoder {
    // Video source configuration
    input_path: String,
    width: usize,
    height: usize,
    
    // Encoding state
    current_bitrate: String,
    frame_buffer: VecDeque<Vec<u8>>,
    nal_buffer: VecDeque<NalUnit>,
    
    // Chunking parameters
    current_position: f64,
    next_chunk_position: f64,
    chunk_duration: f64,
    chunk_overlap: f64,
    
    // FFmpeg process management
    current_child: Option<ffmpeg_sidecar::child::FfmpegChild>,
    current_packet_rx: Option<Receiver<Vec<u8>>>,
    current_stderr_handle: Option<std::thread::JoinHandle<()>>,
    
    // HEVC state tracking for seamless chunks
    parser: HevcParser,
    last_keyframe: Option<Vec<u8>>,
    parameter_sets: Option<Vec<u8>>,
    
    // Performance and recovery
    use_hardware_accel: bool,
    buffer_target_size: usize,
    consecutive_empty_chunks: usize,
    last_recovery_time: Instant,
    recovery_attempts: usize,
    frames_encoded: u64,

    // Pacing control
    target_frames_in_buffer: usize,
    min_frames_threshold: usize,
    max_frames_threshold: usize,
    next_chunk_delay: Duration,
    last_chunk_start_time: Instant,
    is_chunk_pending: bool,

}

impl ChunkedHevcEncoder {
    pub fn new(input_path: &str, width: usize, height: usize, initial_bitrate: &str,
              chunk_duration: f64, buffer_target_size: usize) -> Result<Self> {
        // Use reasonable chunk duration with overlap for smooth transitions
        let actual_chunk_duration: f64 = chunk_duration.min(2.0);  // Cap at 5 seconds
        // let chunk_overlap = (0.5 as f64).min(actual_chunk_duration / 4.0);  // Reasonable overlap
        let chunk_overlap = 1.0; 
        let target_frames_in_buffer = buffer_target_size;
        let min_frames_threshold = buffer_target_size / 3;  // Start encoding when buffer drops below 1/3
        let max_frames_threshold = buffer_target_size * 3 / 2;  // Slow down if too many frames
        

        let encoder = Self {
            input_path: input_path.to_string(),
            width,
            height,
            current_bitrate: initial_bitrate.to_string(),
            frame_buffer: VecDeque::with_capacity(buffer_target_size * 2),
            nal_buffer: VecDeque::new(),
            current_position: 0.0,
            next_chunk_position: 0.0,
            chunk_duration: actual_chunk_duration,
            chunk_overlap,
            current_child: None,
            current_packet_rx: None,
            current_stderr_handle: None,
            parser: HevcParser::new(),
            last_keyframe: None,
            parameter_sets: None,
            use_hardware_accel: true,
            buffer_target_size,
            consecutive_empty_chunks: 0,
            last_recovery_time: Instant::now(),
            recovery_attempts: 0,
            frames_encoded: 0,
            
            target_frames_in_buffer,
            min_frames_threshold,
            max_frames_threshold,
            next_chunk_delay: Duration::from_millis(0),
            last_chunk_start_time: Instant::now(),
            is_chunk_pending: false,
        };
        
        Ok(encoder)
    }
    // Modify the check_buffer_status method for better pacing
    
    // Add a method to adaptively adjust chunk generation based on consumption rate
    pub fn adjust_pacing(&mut self, frames_consumed_per_second: f32) {
        // Calculate ideal chunks per second based on consumption
        // Assume each chunk produces roughly 60 frames (1 second at 60fps)
        let target_frames_per_chunk = 60.0;
        let ideal_chunks_per_second = frames_consumed_per_second / target_frames_per_chunk;
        
        // Don't allow less than 0.1 chunks per second (10 second max delay)
        let ideal_chunks_per_second = ideal_chunks_per_second.max(0.1);
        
        // Calculate ideal delay between chunks
        let ideal_delay_seconds = 1.0 / ideal_chunks_per_second;
        let ideal_delay = Duration::from_secs_f32(ideal_delay_seconds);
        
        // Update next delay (with some smoothing to avoid rapid changes)
        let current_delay = self.next_chunk_delay;
        self.next_chunk_delay = Duration::from_millis(
            (current_delay.as_millis() as f32 * 0.7 + ideal_delay.as_millis() as f32 * 0.3) as u64
        );
        
        println!("Pacing: consumption rate {:.1} fps, adjusted chunk delay to {:?}", 
                frames_consumed_per_second, self.next_chunk_delay);
    }
    fn check_buffer_status(&mut self) -> Result<()> {
        let current_time = Instant::now();
        let buffer_size = self.frame_buffer.len();
        
        // Calculate how much time has passed since the last chunk started
        let time_since_last_chunk = current_time.duration_since(self.last_chunk_start_time);
        
        // Check if we should delay starting a new chunk
        if self.is_chunk_pending && time_since_last_chunk < self.next_chunk_delay {
            // Not enough time has passed, wait longer
            return Ok(());
        }
        
        // If we have no active encoding process
        if self.current_child.is_none() {
            // Different thresholds based on buffer state
            if buffer_size < self.min_frames_threshold {
                // Buffer is low, start a new chunk immediately
                println!("Buffer low ({}/{} frames), starting new chunk", 
                        buffer_size, self.target_frames_in_buffer);
                
                // Reset chunk timing info
                self.last_chunk_start_time = current_time;
                self.is_chunk_pending = false;
                self.next_chunk_delay = Duration::from_millis(0);
                
                // Start encoding
                self.start_chunk_encoding()?;
            } 
            else if buffer_size < self.target_frames_in_buffer && !self.is_chunk_pending {
                // Buffer is below target but not critical - schedule a chunk soon
                let delay = Duration::from_millis(500);  // Small delay
                println!("Buffer below target ({}/{} frames), scheduling chunk in {:?}", 
                        buffer_size, self.target_frames_in_buffer, delay);
                
                self.next_chunk_delay = delay;
                self.last_chunk_start_time = current_time;
                self.is_chunk_pending = true;
            }
            else if buffer_size >= self.max_frames_threshold {
                // Buffer is very full, delay starting a new chunk
                println!("Buffer full ({}/{} frames), delaying next chunk", 
                        buffer_size, self.target_frames_in_buffer);
                
                self.is_chunk_pending = false;
            }
        }
        
        Ok(())
    }

    // Add this to the ChunkedHevcEncoder
    pub fn create_debug_frames(&mut self) {
        if let Some(keyframe) = &self.last_keyframe {
            // Create a synthetic frame
            let mut frame = Vec::new();
            
            // Add parameter sets if available
            if let Some(params) = &self.parameter_sets {
                frame.extend_from_slice(params);
            }
            
            // Add keyframe
            frame.extend_from_slice(keyframe);
            
            // Add to frame buffer
            self.frame_buffer.push_back(frame.clone());
            println!("Created synthetic frame, size: {}", frame.len());
        }
    }
    // Add this method to create a synthetic frame when no frames are properly assembled
    pub fn create_synthetic_frame(&mut self) -> bool {
        // Only do this if we have both parameter sets and a keyframe
        if let (Some(params), Some(keyframe)) = (&self.parameter_sets, &self.last_keyframe) {
            let mut frame = Vec::new();
            
            // Include parameter sets
            frame.extend_from_slice(params);
            
            // Include keyframe
            // Note: The keyframe should already have its start code (0001)
            // If it doesn't, you would need to add it here
            frame.extend_from_slice(keyframe);
            
            println!("Created synthetic frame: {} bytes total (params: {}, keyframe: {})",
                    frame.len(), params.len(), keyframe.len());
                    
            // Add to frame buffer
            self.frame_buffer.push_back(frame);
            self.frames_encoded += 1;
            
            return true;
        }
        
        println!("Cannot create synthetic frame: missing params or keyframe");
        false
    }

    /// Start the encoder with optimized parameters for frame production
    pub fn start(&mut self) -> Result<()> {
        println!("Starting HEVC encoder with optimized parameters");
        
        let start_position = self.next_chunk_position;
        self.next_chunk_position = start_position + self.chunk_duration - self.chunk_overlap;
        
        // Always use software encoding for better compatibility
        self.use_hardware_accel = false;
        
        // Simple, reliable command for encoding
        let mut child = FfmpegCommand::new()
            .args(&["-ss", &format!("{:.3}", start_position)])
            .args(&["-t", &format!("{:.3}", self.chunk_duration)])
            .input(&self.input_path)
            // Force keyframe at start for reliable parser initialization
            .args(&["-force_key_frames", "expr:gte(t,0)"])
            .args(&["-x265-params", "keyint=30:min-keyint=15:scenecut=0"])
            .args(&["-vsync", "0"])  // Important for frame timing
            .args(&["-c:v", "libx265"])
            .args(&["-preset", "ultrafast"])
            .args(&["-tune", "zerolatency"])
            .args(&["-b:v", &self.current_bitrate])
            .args(&["-an"])
            .args(&["-f", "hevc", "-"])
            .spawn()?;
        
        println!("Starting FFmpeg process at position {:.2}s", start_position);
        // let mut child = command.spawn()?;
        
        // Setup channels and threads
        let stdout = child.take_stdout().unwrap();
        let stderr = child.take_stderr().unwrap();
        
        let (packet_tx, packet_rx) = unbounded();
        
        // Stdout reader thread with better buffering
        std::thread::spawn(move || {
            let mut reader = BufReader::with_capacity(65536, stdout);
            let mut buf = [0u8; 65536];
            let mut total_bytes = 0;
            
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => {
                        println!("Encoder EOF reached");
                        break;
                    },
                    Ok(n) => {
                        total_bytes += n;
                        if total_bytes % 102400 < n {
                            println!("Received {}KB from encoder", total_bytes / 1024);
                        }
                        
                        if packet_tx.send(buf[..n].to_vec()).is_err() {
                            println!("Encoder channel closed");
                            break;
                        }
                    },
                    Err(e) => {
                        eprintln!("Encoder read error: {}", e);
                        break;
                    }
                }
            }
            println!("Encoder stdout reader finished, read {}KB", total_bytes / 1024);
        });
        
        // Stderr monitor thread
        let stderr_handle = std::thread::spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut buf = String::new();
            
            loop {
                buf.clear();
                match reader.read_to_string(&mut buf) {
                    Ok(0) => break,
                    Ok(_) => {
                        if !buf.trim().is_empty() {
                            print!("FFmpeg: {}", buf);
                        }
                    },
                    Err(e) => {
                        eprintln!("Encoder stderr read error: {}", e);
                        break;
                    }
                }
            }
        });
        
        // Store state
        self.current_child = Some(child);
        self.current_packet_rx = Some(packet_rx);
        self.current_stderr_handle = Some(stderr_handle);
        
        Ok(())
    }

        // This function could be used as a fallback when no parameter sets are detected
    pub fn create_minimal_parameter_sets(&mut self) -> Vec<u8> {
        // Create minimal VPS
        let vps: Vec<u8> = vec![
            0x00, 0x00, 0x00, 0x01, 0x40, 0x01, 0x0c, 0x01,
            0xff, 0xff, 0x01, 0x60, 0x00, 0x00, 0x03, 0x00,
            0xb0, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00,
            0x78, 0x02
        ];
        
        // Create minimal SPS - this is simplified, might need customization
        let sps: Vec<u8> = vec![
            0x00, 0x00, 0x00, 0x01, 0x42, 0x01, 0x01, 0x01,
            0x60, 0x00, 0x00, 0x03, 0x00, 0x80, 0x00, 0x00,
            0x03, 0x00, 0x00, 0x03, 0x00, 0x78, 0xa0, 0x03,
            0xc0, 0x80, 0x10, 0xe5, 0x96, 0x66, 0x69, 0x26,
            0x64, 0x00, 0x20
        ];
        
        // Create minimal PPS
        let pps: Vec<u8> = vec![
            0x00, 0x00, 0x00, 0x01, 0x44, 0x01, 0xc0, 0xf3,
            0xc0, 0x02
        ];
        
        let mut params = Vec::new();
        params.extend_from_slice(&vps);
        params.extend_from_slice(&sps);
        params.extend_from_slice(&pps);
        
        println!("Created minimal parameter sets, {} bytes", params.len());
        
        // Store for future use
        self.parameter_sets = Some(params.clone());
        
        params
    }

    
    /// Changes the encoding bitrate for subsequent chunks
    pub fn set_bitrate(&mut self, new_bitrate: &str) -> Result<()> {
        if self.current_bitrate != new_bitrate {
            println!("Changing bitrate from {} to {}", self.current_bitrate, new_bitrate);
            self.current_bitrate = new_bitrate.to_string();
            // The new bitrate will be applied when the next chunk starts
        }
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
            self.current_stderr_handle.take();
        }
        
        Ok(())
    }
    
   // Process available encoded packets and manage chunk transitions
   pub fn process_incoming_packets(&mut self) -> Result<()> {
        // 1. Collect packets from the current encoding process
        let mut disconnected = false;
        let mut packets_to_process = Vec::new();
        let mut packet_count = 0;
        let mut total_bytes = 0;
        
        // First, collect packets without holding the mutable borrow of self
        if let Some(packet_rx) = &self.current_packet_rx {
            // Try to receive multiple packets at once for efficiency
            for _ in 0..50 {  // Process more packets per call
                match packet_rx.try_recv() {
                    Ok(packet) => {
                        packet_count += 1;
                        total_bytes += packet.len();
                        packets_to_process.push(packet);
                    },
                    Err(TryRecvError::Empty) => {
                        break;
                    },
                    Err(TryRecvError::Disconnected) => {
                        disconnected = true;
                        break;
                    },
                }
            }
        }
        
        // 2. Now process all the collected packets
        for packet in packets_to_process {
            // Add packet data to the parser
            self.parser.add_data(&packet);
        }
        
        // Log packet reception
        if packet_count > 0 {
            println!("Received {} packets, {} bytes total", packet_count, total_bytes);
        }
        
        // Process parser data after all packets have been added
        self.process_parser_data();
        
        // 3. Handle disconnection if needed
        if disconnected {
            println!("Encoder channel disconnected, chunk complete");
            
            // Track buffer state before final processing
            let frames_before = self.frame_buffer.len();
            
            // Do one final extraction pass
            self.process_parser_data();
            
            // Clean up the current process
            self.current_child.take();
            self.current_packet_rx.take();
            self.current_stderr_handle.take();
            
            // Update position
            self.current_position = self.next_chunk_position;
            
            // Check if the chunk produced any frames
            let frames_after = self.frame_buffer.len();
            println!("Chunk complete: {} frames before, {} frames after", 
                    frames_before, frames_after);
                    
            if frames_after == frames_before {
                // No new frames - might indicate a problem
                self.consecutive_empty_chunks += 1;
                println!("Warning: Chunk produced no frames ({} consecutive)",
                        self.consecutive_empty_chunks);
                
                if self.consecutive_empty_chunks >= 2 {  // Reduced threshold
                    // After multiple empty chunks, try recovery sooner
                    println!("Multiple empty chunks, attempting recovery");
                    self.attempt_recovery()?;
                }
            } else {
                // Reset empty chunk counter
                self.consecutive_empty_chunks = 0;
                println!("Successfully added {} frames to buffer", frames_after - frames_before);
            }
        } else if self.current_child.is_none() {
            // No active encoding process
            println!("No active encoding process");
        }
        
        // 4. Check buffer status to determine if we need to start a new chunk
        self.check_buffer_status()?;
        
        Ok(())
    }

    
    /// Process parser data to extract frames and NAL units
    fn process_parser_data(&mut self) {
        // Extract NAL units
        let nal_units = self.parser.get_nal_units();
        if !nal_units.is_empty() {
            println!("Extracted {} NAL units from parser", nal_units.len());
        }
        
        for nal in nal_units {
            // Store keyframes for recovery
            if nal.is_keyframe {
                println!("Found keyframe NAL type: {}", nal.nal_type);
                let mut keyframe = Vec::new();
                keyframe.extend_from_slice(&[0, 0, 0, 1]);
                keyframe.extend_from_slice(&nal.data);
                self.last_keyframe = Some(keyframe.clone());
            }
            
            // Add to NAL buffer
            self.nal_buffer.push_back(nal);
        }
        
        // Extract complete frames
        let frames = self.parser.get_frames();
        if !frames.is_empty() {
            println!("Extracted {} complete frames", frames.len());
            
            // Process directly without buffer if we're starving
            if self.frame_buffer.is_empty() && !frames.is_empty() {
                println!("Buffer was empty, adding frames directly");
            }
            
            for frame in frames {
                self.frame_buffer.push_back(frame);
                self.frames_encoded += 1;
            }
        }
        
        // Update parameter sets from parser
        if let Some(params) = self.parser.get_parameter_sets() {
            self.parameter_sets = Some(params.clone());
            println!("Updated parameter sets, size: {}", params.len());
        }
    }
    
    // /// Check if we need to start a new encoding chunk
    // fn check_buffer_status(&mut self) -> Result<()> {
    //     // If we have no active encoding and our buffer is getting low
    //     if self.current_child.is_none() {
    //         // Calculate buffer threshold
    //         let buffer_threshold = self.buffer_target_size / 2;
            
    //         if self.frame_buffer.len() < buffer_threshold {
    //             // Buffer is low, start a new chunk
    //             println!("Buffer running low ({} frames), starting new chunk", 
    //                     self.frame_buffer.len());
    //             self.start_chunk_encoding()?;
    //         }
    //     }
        
    //     Ok(())
    // }
    
    /// Start encoding a new chunk with proper parameter handling for seamless transitions
    pub fn start_chunk_encoding(&mut self) -> Result<()> {
        // Stop any current encoding
        self.stop_encoding()?;
        
        // Calculate start position for this chunk
        let start_position = self.next_chunk_position;
        if let Some(params) = &self.parameter_sets {
            self.parser.vps_sps_pps = Some(params.clone());
        }

        // Update position for the next chunk
        self.next_chunk_position = start_position + self.chunk_duration - self.chunk_overlap;
        
        println!("Starting chunk encoding at position {:.2}s with bitrate {}", 
                start_position, self.current_bitrate);
        
        // Try to start the encoding with retries
        let mut retry_count = 0;
        let max_retries = 3;
        
        while retry_count < max_retries {
            match self.create_encoding_process(start_position) {
                Ok((child, packet_rx, stderr_handle)) => {
                    // Store the encoder state
                    self.current_child = Some(child);
                    self.current_packet_rx = Some(packet_rx);
                    self.current_stderr_handle = Some(stderr_handle);
                    return Ok(());
                },
                Err(e) => {
                    retry_count += 1;
                    eprintln!("Failed to start encoding process (attempt {}/{}): {}", 
                            retry_count, max_retries, e);
                    
                    // Try a different strategy if we keep failing
                    if retry_count == 2 {
                        self.use_hardware_accel = !self.use_hardware_accel;
                        println!("Toggling hardware acceleration to {}", self.use_hardware_accel);
                    }
                    
                    if retry_count >= max_retries {
                        // If we've exhausted retries but have buffer, continue
                        if !self.frame_buffer.is_empty() {
                            // Advance position to try a different part
                            self.next_chunk_position += 1.0;
                            println!("Failed to start encoding, will try again at {:.2}s",
                                    self.next_chunk_position);
                            return Ok(());
                        }
                        
                        // Otherwise, return the error
                        return Err(e);
                    }
                    
                    // Wait before retrying
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
        
        // Should not reach here due to return in the while loop
        unreachable!()
    }
    
    /// Creates a new encoding process with optimized parameters
    fn create_encoding_process(&self, start_position: f64) 
            -> Result<(ffmpeg_sidecar::child::FfmpegChild, 
                        Receiver<Vec<u8>>, 
                        std::thread::JoinHandle<()>)> {
                            
        // Calculate GOP size based on chunk duration and framerate
        // Smaller GOP sizes work better for chunking to ensure proper keyframes
        let fps = 60.0;  // Assuming 60fps
        let gop_size = (fps * 1.0 as f32).round() as usize;  // GOP size of 1 second
        let gop_value = gop_size.to_string();
        
        let mut command = FfmpegCommand::new();
        
        // Add hardware acceleration if enabled
        if self.use_hardware_accel {
            command.hwaccel("auto");
        }
        
        // Build common parameters
        command
            .args(&["-ss", &format!("{:.3}", start_position)])  // Start position
            .args(&["-t", &format!("{:.3}", self.chunk_duration)])  // Duration with small margin
            .args(&["-force_key_frames", "expr:gte(t,0)"])
            .input(&self.input_path)
            // .args(&["-threads", "4"])  // Use 4 threads for encoding
            .args(&["-vf", &format!("scale={}:{}:force_original_aspect_ratio=disable,format=yuv420p", 
                                     self.width, self.height)]);
        
        // Configure encoder based on hardware support
        if self.use_hardware_accel {
            command
                .args(&["-c:v", "hevc_nvenc"])
                .args(&["-preset", "p1"])
                .args(&["-tune", "ull"]);
        } else {
            command
                .args(&["-c:v", "libx265"])
                .args(&["-preset", "ultrafast"]);
        }
        
        // Common encoding parameters optimized for chunking
        command
            .args(&["-rc", "cbr"])  // Constant bitrate for consistency
            .args(&["-b:v", &self.current_bitrate])
            .args(&["-maxrate", &self.current_bitrate])
            .args(&["-bufsize", &self.current_bitrate])
            // Critical for chunking: 
            // Force keyframe at the start of each chunk and at regular intervals
            .args(&["-g", &gop_value])  // GOP size
            .args(&["-keyint_min", &gop_value])  // Min keyframe interval
            // .args(&["-forced-idr", "1"])  // Force IDR frames
            // .args(&["-forced_idr", "1"])  // Alternative spelling for different ffmpeg versions
            // Always start chunk with a keyframe
            .args(&["-force_key_frames", "expr:gte(t,0)"])
            // Output settings
            .args(&["-movflags", "+frag_keyframe+empty_moov"])
            .args(&["-flush_packets", "1"])
            .args(&["-bsf:v", "hevc_mp4toannexb"])  // Required for raw HEVC
            .args(&["-an"])  // No audio
            .args(&["-f", "hevc", "-"]);  // Output raw HEVC
        
        // Start the process
        let mut child = command.spawn()?;
        
        // Get handles to the process I/O
        let stdout = child.take_stdout().unwrap();
        let stderr = child.take_stderr().unwrap();
        
        // Set up communication channels
        let (packet_tx, packet_rx) = unbounded();
        
        // Start stdout reader thread
        std::thread::spawn(move || {
            let mut reader = BufReader::with_capacity(65536, stdout);
            let mut buf = [0u8; 65536];  // Larger buffer for efficiency
            
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => break,  // End of stream
                    Ok(n) => {
                        // Send the data on the channel
                        if packet_tx.send(buf[..n].to_vec()).is_err() {
                            break;  // Channel closed
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
                        // Only print significant messages
                        if buf.contains("Error") || buf.contains("error") || 
                           buf.contains("warning") || buf.contains("Warning") {
                            eprint!("FFmpeg: {}", buf);
                        }
                    }
                    Err(e) => {
                        eprintln!("Encoder stderr read error: {}", e);
                        break;
                    }
                }
            }
        });
        
        Ok((child, packet_rx, stderr_handle))
    }
    
    /// Get the next frame from the buffer
    pub fn next_frame(&mut self) -> Option<Vec<u8>> {
        let frame = self.frame_buffer.pop_front();
        
        // Automatically check if we need to refill the buffer
        if let Err(e) = self.check_buffer_status() {
            eprintln!("Error checking buffer status: {}", e);
        }
        
        frame
    }
    
    /// Get the latest keyframe (useful for recovery after packet loss)
    pub fn get_latest_keyframe(&self) -> Option<Vec<u8>> {
        // If we have a last keyframe and parameter sets
        if let (Some(keyframe), Some(params)) = (&self.last_keyframe, &self.parameter_sets) {
            // For recovery, we want to send parameter sets followed by keyframe
            let mut recovery_frame = params.clone();
            recovery_frame.extend_from_slice(keyframe);
            Some(recovery_frame)
        } else {
            // Fall back to just the keyframe if we don't have parameter sets
            self.last_keyframe.clone()
        }
    }
    
    /// Number of frames available in the buffer
    pub fn frames_available(&self) -> usize {
        self.frame_buffer.len()
    }
    
    /// Attempt error recovery with smart strategies
    pub fn attempt_recovery(&mut self) -> Result<()> {
        // Only try recovery if enough time has passed since last attempt
        let now = Instant::now();
        if now.duration_since(self.last_recovery_time) < Duration::from_secs(5) {
            return Ok(());
        }
        
        println!("Attempting encoder recovery (attempt {})", self.recovery_attempts + 1);
        self.last_recovery_time = now;
        self.recovery_attempts += 1;
        
        // Different strategies based on attempt number
        match self.recovery_attempts % 4 {
            0 => {
                // Try restarting at the same position
                println!("Recovery: Restarting at current position {:.2}s", 
                        self.next_chunk_position);
            },
            1 => {
                // Toggle hardware acceleration
                self.use_hardware_accel = !self.use_hardware_accel;
                println!("Recovery: Toggling hardware acceleration to {}", 
                        self.use_hardware_accel);
            },
            2 => {
                // Move to a different position
                self.next_chunk_position += 2.0;
                println!("Recovery: Skipping ahead to {:.2}s", self.next_chunk_position);
            },
            _ => {
                // Reset to beginning
                self.next_chunk_position = 0.0;
                println!("Recovery: Restarting from beginning");
            }
        }
        
        // Restart encoding with new settings
        self.stop_encoding()?;
        self.start_chunk_encoding()?;
        
        Ok(())
    }
    
    /// Check if we need to perform stall recovery
    pub fn check_stall(&mut self, last_frame_time: Instant, stall_timeout: Duration) -> Result<bool> {
        if last_frame_time.elapsed() > stall_timeout && 
           self.last_recovery_time.elapsed() > Duration::from_secs(3) {
            // We're stalled - attempt recovery
            println!("Stall detected (no frames for {}ms)",
                    last_frame_time.elapsed().as_millis());
                    
            self.attempt_recovery()?;
            return Ok(true);
        }
        
        Ok(false)
    }
    
    /// Get the current position in the video
    pub fn get_current_position(&self) -> f64 {
        self.current_position
    }
    
    /// Get encoder statistics
    pub fn get_stats(&self) -> (u64, usize, String) {
        (self.frames_encoded, self.frame_buffer.len(), self.current_bitrate.clone())
    }
}

/// Improved HEVC decoder with better error resilience
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
    
    // Error tracking and recovery
    consecutive_errors: usize,
    last_successful_decode: Instant,
}

impl HevcDecoder {
    pub fn new(framerate: u32, width: u32, height: u32) -> Result<Self> {
        let frame_size = (width as usize) * (height as usize) * 3;
        
        // Create FFmpeg command with improved error resilience
        let mut child = FfmpegCommand::new()
            .hwaccel("cuda")  // Auto select hardware acceleration
            .args(&["-analyzeduration", "1000000"]) // Increase analysis time
            .args(&["-probesize", "1000000"])      // Increase probe size
            // Critical for chunked decoding - improved error resilience
            .args(&["-err_detect", "ignore_err"])
            .args(&["-fflags", "+discardcorrupt+genpts"])
            .args(&["-f", "hevc", "-i", "-"])      // Raw HEVC input
            .args(&["-vf", &format!("fps={}", framerate)])
            .args(&["-pix_fmt", "rgb24"])
            .args(&["-f", "rawvideo", "-"])
            .spawn()?;

        let stdout = child.take_stdout().unwrap();
        let stdin = child.take_stdin().unwrap();
        let stderr = child.take_stderr().unwrap();

        let (frame_tx, frame_rx) = unbounded::<Vec<u8>>();
        let (packet_tx, packet_rx) = bounded::<Vec<u8>>(100);
        
        // Start stdout reader thread with improved debug output
        std::thread::spawn({
            let frame_size = frame_size;
            move || {
                println!("Starting decoder stdout reader thread, frame size: {}", frame_size);
                let mut reader = BufReader::with_capacity(frame_size * 2, stdout);
                let mut buffer = Vec::with_capacity(frame_size * 3);
                let mut chunk = vec![0u8; 4096];  // Smaller chunks for more frequent updates
                let mut total_bytes = 0;
                let mut frames_read = 0;
                
                loop {
                    match reader.read(&mut chunk) {
                        Ok(0) => {
                            println!("Decoder stdout reached end of stream");
                            break;
                        }
                        Ok(n) => {
                            buffer.extend_from_slice(&chunk[..n]);
                            total_bytes += n;
                            
                            // Process complete frames
                            while buffer.len() >= frame_size {
                                let frame = buffer.drain(..frame_size).collect();
                                frames_read += 1;
                                
                                // Log progress
                                if frames_read % 10 == 0 {
                                    println!("Decoder: read {} frames, {}KB total", 
                                             frames_read, total_bytes / 1024);
                                }
                                
                                if frame_tx.send(frame).is_err() {
                                    println!("Decoder frame channel closed");
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Decoder read error: {}", e);
                            break;
                        }
                    }
                }
                
                println!("Decoder stdout reader finished: {} frames, {}KB total", 
                         frames_read, total_bytes / 1024);
            }
        });

        // Start stdin writer thread with retry logic
        let stdin_handle = std::thread::spawn(move || {
            let mut writer = stdin;
            
            for packet in packet_rx {
                // Try to write with retries
                let mut retry_count = 0;
                let max_retries = 3;
                
                while retry_count < max_retries {
                    match writer.write_all(&packet) {
                        Ok(_) => {
                            // Successfully wrote packet, flush to ensure real-time processing
                            if writer.flush().is_ok() {
                                break;  // Success
                            }
                            // Flush failed, retry
                            retry_count += 1;
                        },
                        Err(e) => {
                            eprintln!("Decoder write error (attempt {}/{}): {}", 
                                    retry_count + 1, max_retries, e);
                            retry_count += 1;
                            
                            if retry_count >= max_retries {
                                eprintln!("Failed to write packet after {} attempts", max_retries);
                                break;
                            }
                            
                            // Wait before retry
                            std::thread::sleep(Duration::from_millis(10));
                        }
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
                        // Only print significant error messages
                        if buf.contains("Error") || buf.contains("error") {
                            eprint!("Decoder error: {}", buf);
                        }
                    }
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
            decoded_frames: VecDeque::with_capacity(120),  // 2 seconds of frames at 60fps
            consecutive_errors: 0,
            last_successful_decode: Instant::now(),
        })
    }

    /// Process incoming encoded packet with robust error handling
    pub fn process_packet(&mut self, packet: Vec<u8>) -> Result<()> {
        // Add packet data to the parser
        self.parser.add_data(&packet);
        
        // Extract frames from the parser and buffer them
        let frames = self.parser.get_frames();
        for frame in frames {
            self.frame_buffer.push_back(frame);
        }
        
        // Attempt to send the packet with retries and backoff
        let mut retry_count = 0;
        let max_retries = 5;
        let mut delay = 1;
        
        while retry_count < max_retries {
            match self.packet_tx.try_send(packet.clone()) {
                Ok(_) => {
                    // Success
                    if self.consecutive_errors > 0 {
                        println!("Decoder communication restored after {} errors", 
                                self.consecutive_errors);
                        self.consecutive_errors = 0;
                    }
                    return Ok(());
                },
                Err(TrySendError::Full(_)) => {
                    // Channel is full, wait with exponential backoff
                    std::thread::sleep(Duration::from_millis(delay));
                    retry_count += 1;
                    delay = (delay * 2).min(50);  // Cap at 50ms
                },
                Err(TrySendError::Disconnected(_)) => {
                    return Err(anyhow::anyhow!("Decoder input channel disconnected"));
                }
            }
        }
        
        // If we reach here, we failed after all retries
        self.consecutive_errors += 1;
        
        if self.consecutive_errors >= 10 {
            // Many consecutive errors - might need more drastic recovery
            return Err(anyhow::anyhow!("Decoder appears to be stalled after multiple errors"));
        }
        
        // Just log warning for occasional errors
        eprintln!("Warning: Decoder input channel full, packet dropped (error {}/10)",
                self.consecutive_errors);
        
        Ok(())
    }
    
    /// Try to get the next decoded frame
    fn try_next_decoded_frame(&mut self) -> Result<Option<Vec<u8>>> {
        match self.frame_rx.try_recv() {
            Ok(frame) => {
                self.last_successful_decode = Instant::now();
                Ok(Some(frame))
            },
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => {
                Err(anyhow::anyhow!("Decoder frame channel disconnected"))
            },
        }
    }
    
    /// Process any available decoded frames
    pub fn process_decoded_frames(&mut self) -> Result<()> {
        // Limit number of frames processed in one call
        let mut count = 0;
        let max_frames_per_call = 10;
        
        // Process frames
        while count < max_frames_per_call {
            match self.try_next_decoded_frame() {
                Ok(Some(frame)) => {
                    self.decoded_frames.push_back(frame);
                    count += 1;
                },
                Ok(None) => break,  // No more frames available
                Err(e) => return Err(e),
            }
        }
        
        // Limit decoded frame buffer size
        let max_decoded_buffer = 120;  // 2 seconds at 60fps
        while self.decoded_frames.len() > max_decoded_buffer {
            self.decoded_frames.pop_front();
        }
        
        Ok(())
    }

    /// Get the next available decoded RGB frame
    pub fn next_decoded_frame(&mut self) -> Option<Vec<u8>> {
        self.decoded_frames.pop_front()
    }
    
    /// Check if the decoder is stalled
    pub fn is_stalled(&self, timeout: Duration) -> bool {
        self.last_successful_decode.elapsed() > timeout
    }
    
    /// Number of frames available in the decoded buffer
    pub fn frames_available(&self) -> usize {
        self.decoded_frames.len()
    }
}

/// Converts raw RGB byte data to u32 pixel buffer
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
    
    pixels
}

/// Scale pixel buffer to a new size
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

/// Main function with simplified approach
fn main() -> Result<()> {
    // Initialize FFmpeg
    ffmpeg_sidecar::download::auto_download()?;
    
    // Configuration - make sure this path is valid
    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/bbb_1080p60fps.mp4";
    println!("Starting chunked video codec simulation..."); 

    // Initialize chunked encoder with shorter chunks for faster feedback
    let mut encoder = ChunkedHevcEncoder::new(
        input_path, 
        WIDTH_ENCODER, 
        HEIGHT_ENCODER, 
        INITIAL_BITRATE,
        2.0,      // Smaller 2-second chunks 
        120       // Target buffer size (2 seconds at 60fps)
    )?;
    
    // First, check if the input file exists
    if !std::path::Path::new(input_path).exists() {
        return Err(anyhow::anyhow!("Input file not found: {}", input_path));
    }
    
    // Disable hardware acceleration initially
    encoder.use_hardware_accel = false;
    println!("Starting with software encoding (libx265)");
    
    // Start initial encoding
    encoder.start()?;
    println!("Started initial encoding at bitrate {}", INITIAL_BITRATE);
    
    // Process for a while giving the encoder time to produce frames
    let init_start = Instant::now();
    let progress_interval = Duration::from_secs(1);
    let mut last_progress = init_start;
    
    println!("Processing encoder output...");
    let max_init_wait = Duration::from_secs(30);  // Longer wait time
    while encoder.frames_available() == 0 && init_start.elapsed() < max_init_wait {
        if let Err(e) = encoder.process_incoming_packets() {
            eprintln!("Error processing packets: {}", e);
        }
        
        // Try every few seconds to create a synthetic frame
        if init_start.elapsed().as_secs() % 3 == 0 {
            if encoder.create_synthetic_frame() {
                println!("Created emergency synthetic frame");
                break;
            }
        }
        
        // Show progress
        if init_start.elapsed().as_secs() % 2 == 0 {
            println!("Still waiting for frames... ({} seconds)", 
                    init_start.elapsed().as_secs());
        }
        
        // Sleep to avoid CPU spin
        std::thread::sleep(Duration::from_millis(100));
    }

    // Check if we got frames
    if encoder.frames_available() > 0 {
        println!("Encoder initialized with {} frames", encoder.frames_available());
    } else {
        // Try one more recovery attempt with hardware accel toggled
        println!("No frames produced yet, trying hardware toggle");
        encoder.use_hardware_accel = !encoder.use_hardware_accel;
        encoder.attempt_recovery()?;
        
        // Give it a little more time
        let retry_start = Instant::now();
        while encoder.frames_available() == 0 && retry_start.elapsed() < Duration::from_secs(10) {
            encoder.create_debug_frames();
            std::thread::sleep(Duration::from_millis(100));
        }
        
        if encoder.frames_available() == 0 {
            return Err(anyhow::anyhow!("Failed to initialize encoder after multiple attempts"));
        }
        
        println!("Encoder finally initialized with {} frames", encoder.frames_available());
    }
    
    // Initialize decoder
    let mut decoder = HevcDecoder::new(60, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32)?;
    println!("Decoder initialized"); 
    
    // Packet loss simulation
    let mut drop_probability = PACKET_LOSS_PROBABILITY; 
    let mut rng = rand::thread_rng();

    // Window setup
    let scale_factor = WINDOW_SCALE_FACTOR;
    let scaled_width = (WIDTH_ENCODER as f64 * scale_factor) as usize;
    let scaled_height = (HEIGHT_ENCODER as f64 * scale_factor) as usize;

    let mut window = Window::new(
        "Chunked Video Stream",
        scaled_width,
        scaled_height,
        WindowOptions::default(),
    )?;
    println!("Window opened"); 
    
    // Timing control
    let frame_duration = Duration::from_secs_f64(1.0 / 60.0);
    let mut next_frame_time = Instant::now();
    let mut fps_timer = Instant::now(); 
    let mut frames_displayed = 0;
    let mut frames_dropped = 0;
    
    // Frame transmission timing
    let transmission_interval = Duration::from_millis(16); // ~60fps
    let mut next_transmission_time = Instant::now();
    
    // Recovery timing
    let mut last_frame_time = Instant::now();
    let frame_timeout = Duration::from_millis(500);
    
    // Bitrate change schedule (time in seconds, bitrate)
    let bitrate_schedule = vec![
        (10.0, "4M"),   // After 10 seconds, switch to 4 Mbps
        (20.0, "1M"),   // After 20 seconds, switch to 1 Mbps
        (30.0, "8M"),   // After 30 seconds, switch to 8 Mbps
        (40.0, "2M"),   // After 40 seconds, back to 2 Mbps
    ];
    let start_time = Instant::now();
    let mut next_bitrate_idx = 0;

    println!("Starting main loop with initial bitrate {}", INITIAL_BITRATE);
    
    while window.is_open() && !window.is_key_down(Key::Escape) {
        // 1. First priority: Process encoder packets to keep buffer filled
        if let Err(e) = encoder.process_incoming_packets() {
            eprintln!("Error processing encoder packets: {}", e);
        }
        
        // 2. Check bitrate schedule
        if next_bitrate_idx < bitrate_schedule.len() {
            let (change_time, new_bitrate) = bitrate_schedule[next_bitrate_idx];
            let elapsed = start_time.elapsed().as_secs_f64();
            
            if elapsed >= change_time {
                println!("Time: {:.1}s - Changing bitrate to {}", elapsed, new_bitrate);
                if let Err(e) = encoder.set_bitrate(new_bitrate) {
                    eprintln!("Error changing bitrate: {}", e);
                }
                next_bitrate_idx += 1;
            }
        }
        
        // 3. Check for stalls and attempt recovery if needed
        if let Ok(stalled) = encoder.check_stall(last_frame_time, frame_timeout) {
            if stalled {
                // After encoder recovery, also check decoder
                if decoder.is_stalled(frame_timeout) {
                    println!("Decoder appears stalled, sending recovery keyframe");
                    
                    // Send a keyframe to reset decoder state
                    if let Some(keyframe) = encoder.get_latest_keyframe() {
                        if let Err(e) = decoder.process_packet(keyframe) {
                            eprintln!("Error sending recovery keyframe: {}", e);
                        }
                    }
                }
            }
        }
        
        // 4. Simulate frame transmission at controlled rate
        let now = Instant::now();
        if now >= next_transmission_time && encoder.frames_available() > 0 {
            // Time to transmit a new frame
            if let Some(frame) = encoder.next_frame() {
                // Simulate packet loss if enabled
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
        
        // 5. Process any decoded frames
        if let Err(e) = decoder.process_decoded_frames() {
            eprintln!("Error processing decoded frames: {}", e);
        }

        // 6. Display frames at controlled framerate
        let display_time = Instant::now();
        // In main(), add tracking for frame consumption rate
        let mut frame_consumption_counter = 0;
        let mut last_pacing_adjustment = Instant::now();


        if display_time >= next_frame_time {
            if let Some(frame) = decoder.next_decoded_frame() {
                // Update successful frame timestamp
                last_frame_time = Instant::now();
    
                // Convert and display the frame
                let pixels = convert_rgb_to_u32(&frame, WIDTH_ENCODER, HEIGHT_ENCODER);
                let scaled = scale_pixels(&pixels, WIDTH_ENCODER, HEIGHT_ENCODER, scaled_width, scaled_height);
                
                if let Err(e) = window.update_with_buffer(&scaled, scaled_width, scaled_height) {
                    eprintln!("Error updating window buffer: {}", e);
                } else {
                    frames_displayed += 1;
                    frame_consumption_counter += 1;
                }
                
                // Schedule next frame time
                next_frame_time += frame_duration;
            }
        } else {
            // Sleep to save CPU if it's not time for next frame
            std::thread::sleep(std::cmp::min(
                next_frame_time.saturating_duration_since(display_time),
                Duration::from_millis(1)
            ));
        }


        // Periodically adjust pacing based on consumption rate
        if last_pacing_adjustment.elapsed() >= Duration::from_secs(2) {
            let consumption_period = last_pacing_adjustment.elapsed().as_secs_f32();
            let frames_per_second = frame_consumption_counter as f32 / consumption_period;
            
            // Adjust encoder pacing based on consumption rate
            encoder.adjust_pacing(frames_per_second);
            
            // Reset consumption counter
            frame_consumption_counter = 0;
            last_pacing_adjustment = Instant::now();
        }
        
        // 7. FPS counter and stats display
        if fps_timer.elapsed() >= Duration::from_secs(2) {
            // Get current stats
            let (total_frames, buffer_size, current_bitrate) = encoder.get_stats();
            let elapsed = start_time.elapsed().as_secs_f64();
            
            println!("Time: {:.1}s | FPS: {} | Buffer: {} | Bitrate: {} | Position: {:.2}s",
                     elapsed, frames_displayed / 2, buffer_size, current_bitrate,
                     encoder.get_current_position());
            
            frames_displayed = 0;
            frames_dropped = 0;
            fps_timer = Instant::now();
        }
        
        // 8. Process window events and keyboard input
        window.update();
        
        // 9. Keyboard controls
        if window.is_key_pressed(Key::Key1, minifb::KeyRepeat::No) {
            encoder.set_bitrate("1M")?;
            println!("Manually set bitrate to 1M");
        } else if window.is_key_pressed(Key::Key2, minifb::KeyRepeat::No) {
            encoder.set_bitrate("2M")?;
            println!("Manually set bitrate to 2M");
        } else if window.is_key_pressed(Key::Key4, minifb::KeyRepeat::No) {
            encoder.set_bitrate("4M")?;
            println!("Manually set bitrate to 4M");
        } else if window.is_key_pressed(Key::Key8, minifb::KeyRepeat::No) {
            encoder.set_bitrate("8M")?;
            println!("Manually set bitrate to 8M");
        } else if window.is_key_pressed(Key::R, minifb::KeyRepeat::No) {
            // Manual recovery
            println!("Manually triggering recovery");
            encoder.attempt_recovery()?;
        }
    }
    
    // Clean shutdown
    println!("Stopping encoder...");
    encoder.stop_encoding()?;
    println!("Exiting gracefully");
    Ok(())
}