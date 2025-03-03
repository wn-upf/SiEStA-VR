use anyhow::Result;
use std::io::BufReader;
use std::collections::VecDeque;
use std::time::Duration;
use crossbeam::channel::{unbounded, Receiver, TryRecvError};
use ffmpeg_sidecar::command::FfmpegCommand;
use std::io::Read;
// Define the expected encoder dimensions
pub const WIDTH_ENCODER: usize = 3840;
pub const HEIGHT_ENCODER: usize = 2160;
pub const INITIAL_BITRATE: &str = "2M";
pub const IDR_FRAME_SIZE_GOP: usize = 30000;

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

/// HEVC Encoder adapter for integration with asynchronix/nexosim simulation framework
pub struct EncoderAdapter {
    packet_rx: Receiver<Vec<u8>>,
    _child: ffmpeg_sidecar::child::FfmpegChild,
    _stderr_handle: std::thread::JoinHandle<()>,
    parser: HevcParser,
    frame_buffer: VecDeque<Vec<u8>>,  // Buffer for encoded frames
    last_keyframe: Option<Vec<u8>>,    // Store the most recent keyframe
    initialized: bool,                 // Flag to track initialization status
}

impl EncoderAdapter {
    /// Create a new encoder adapter with the specified input file, dimensions, and bitrate
    pub fn new(input: &str, width: u32, height: u32, bitrate: &str) -> Result<Self> {
        // Ensure ffmpeg is available
        ffmpeg_sidecar::download::auto_download()?;
        
        // Create FFmpeg child process for encoding
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
            initialized: false,
        })
    }

    /// Process incoming packets from the encoder
    fn process_incoming_packets(&mut self) -> Result<()> {
        let mut packets_processed = 0;
        while let Ok(Some(_)) = self.try_next_packet() {
            packets_processed += 1;
        }
        
        if packets_processed > 0 && !self.initialized {
            self.initialized = true;
        }
        
        Ok(())
    }
    
    /// Try to get the next packet from the encoder
    fn try_next_packet(&mut self) -> Result<Option<Vec<u8>>> {
        match self.packet_rx.try_recv() {
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

                Ok(Some(packet))
            },
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => {
                eprintln!("Encoder channel disconnected");
                Err(anyhow::anyhow!("Encoder channel disconnected"))
            }
        }
    }
    
    /// Determine if a frame is a keyframe by examining NAL headers
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
    
    /// Number of frames waiting in the buffer
    pub fn frames_available(&self) -> usize {
        self.frame_buffer.len()
    }

    /// Get the latest keyframe (useful for recovery after packet loss)
    pub fn get_latest_keyframe(&self) -> Option<Vec<u8>> {
        self.last_keyframe.clone()
    }

    /// Initialize the encoder and wait for the first frame to be available
    pub async fn initialize(&mut self) -> Result<()> {
        // Process packets until we have at least one frame available
        while self.frames_available() == 0 {
            self.process_incoming_packets()?;
            
            // Yield to other tasks
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        
        self.initialized = true;
        Ok(())
    }

    /// Get the next frame - compatible with asynchronix/nexosim simulation calls
    pub async fn next_frame(&mut self) -> Result<Vec<u8>> {
        // Ensure the encoder is initialized
        if !self.initialized {
            self.initialize().await?;
        }
        
        // Process any new packets first
        self.process_incoming_packets()?;
        
        // Return a frame from the buffer if available
        if let Some(frame) = self.frame_buffer.pop_front() {
            return Ok(frame);
        }
        
        // If no frames are available, process more packets and wait
        loop {
            self.process_incoming_packets()?;
            
            if let Some(frame) = self.frame_buffer.pop_front() {
                return Ok(frame);
            }
            
            // Yield to other tasks
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

/// Implementation of the Simulator trait for integration with asynchronix/nexosim
impl SimulatorAdapter for EncoderAdapter {
    type Output = Vec<u8>;
    
    async fn step(&mut self) -> Result<Self::Output> {
        // This method will be called by the simulation framework to get the next frame
        self.next_frame().await
    }
}

/// Trait for integration with asynchronix/nexosim simulation framework
pub trait SimulatorAdapter {
    type Output;
    
    async fn step(&mut self) -> Result<Self::Output>;
}

/// Example function showing how to use the encoder adapter with a simulator
pub async fn example_usage() -> Result<()> {
    // Create a new encoder adapter
    let mut encoder = EncoderAdapter::new(
        "/path/to/input.mp4", 
        WIDTH_ENCODER as u32, 
        HEIGHT_ENCODER as u32, 
        INITIAL_BITRATE
    )?;
    
    // Initialize the encoder
    encoder.initialize().await?;
    
    // Extract frames as part of a simulation loop
    for _ in 0..10 {
        // This would be called by the simulator
        let frame = encoder.step().await?;
        println!("Got frame of size: {} bytes", frame.len());
        
        // Process the frame as needed...
        
        // Simulate some time passing in the simulation
        std::thread::sleep(Duration::from_millis(16));
    }
    
    Ok(())
}