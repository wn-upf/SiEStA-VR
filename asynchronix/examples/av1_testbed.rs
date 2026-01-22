use crossbeam::channel::{bounded, unbounded, Receiver, Sender, TryRecvError};
use minifb::{Key, Window, WindowOptions};
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tai_time::TaiTime;

use std::collections::BTreeMap;
use tokio::sync::mpsc;


// --- 1. Helpers & Mocks for your custom types ---

// Mocking your print_prettyy macro
macro_rules! print_pretty {
    ($color:expr, $($arg:tt)*) => {
        println!($($arg)*); // Simplified to standard print for this script
    };
}

macro_rules! format_elapsed2{
    ($elapsed:expr) => {{
        let total_seconds =
            $elapsed.as_secs() as f64 + ($elapsed.subsec_nanos() as f64 / 1_000_000_000.0);
        format!("{:.9}", total_seconds)
    }};
}

#[derive(Debug)]
pub enum DebugColor {
    Red, Green, Blue, Magenta, Teal,
}

// --- CHUNKED AV1 ENCODER ---

pub struct ChunkedAv1Encoder {
    input: String,
    width: u32,
    height: u32,
    bitrate: String,
    chunk_duration: f64,
    current_offset: f64,
    frame_tx: Sender<Vec<u8>>,
    frame_rx: Receiver<Vec<u8>>,
    frame_queue: VecDeque<Vec<u8>>,
    parser: Av1Parser, // Uses the new Av1Parser
    encoder_str: String,
    gop_size: usize,
    intra_refresh: bool,
    framerate: f32, // Added to allow FPS control
    start_instant: Instant, 
}

impl ChunkedAv1Encoder {
    pub fn new(
        input: &str,
        width: u32,
        height: u32,
        bitrate: &str,
        chunk_duration: f64,
        string: String,
        offset_video: f64,
        framerate: f32,
        gop_size: usize,
        intra_refresh: bool,
    ) -> Self {
        println!("Initializing ChunkedAv1Encoder");
        let (frame_tx, frame_rx) = bounded(1000);

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
            parser: Av1Parser::new(),
            encoder_str: string.clone(),
            gop_size,
            intra_refresh,
            framerate,
            start_instant: Instant::now(), 
        }
    }

    pub async fn start_chunking(&mut self, bitrate_mbps: f32, now: Instant ) {
        let bitrate_adjusted_fps = bitrate_mbps; 
        self.bitrate = format!("{:.2}M", bitrate_adjusted_fps);
        
        println!(
            "{} - {} AV1 CHUNKING with bitrate {} Mbps", // Changed to {:?} just in case TaiTime doesn't implement Display
            now.duration_since(self.start_instant).as_secs_f32(),
            self.encoder_str,
            bitrate_mbps,
        );

        // Clear parser buffer to avoid stale data
        // self.parser.buffer.clear(); 

        let mut command = FfmpegCommand::new();
        
        // Base arguments
        let mut child = command
            // .hwaccel("cuda")
            .args(&["-ss", &self.current_offset.to_string()])
            .args(&["-t", &self.chunk_duration.to_string()])
            .args(&["-threads", "8"])
            .args(&["-hide_banner", "-nostats", "-loglevel", "error"])
            .input(&self.input)
            .args(&["-vf", &format!( "scale={}:{}:force_original_aspect_ratio=disable,format=yuv420p"         // Video Filter
                                    ,self.width, self.height)])
            .args(&["-c:v", "libsvtav1"]) 
            .args(&["-preset", "9"]) // 9 is highest for 4k, so choosing it for XR RTC. 
            // .args(&["-svtav1-params", "rc=2:lookahead=0:pred-struct=1"]) // rc=1 (VBR), lookahead=0 , pred-struct=2 (Low Delay P, no future encoded frames for XR)
            .args(&["-svtav1-params", "rc=2:lookahead=0:pred-struct=1:lp=2:tile-columns=2:tile-rows=1:fast-decode=1"]) // Tiling for fastness, lp: level of parallelism,
            .args(&["-b:v", &self.bitrate ])
            .args(&["-bufsize", &self.bitrate])
            .args(&["-g", &format!("{}", self.gop_size)])
            // .args(&["-intra-refresh", &format!("{}", self.intra_refresh as i32)])
            .args(&["-f", "obu", "-"])
            .spawn().unwrap();

        let stdout = child.stdout.take().unwrap(); 
        let mut reader = BufReader::new(stdout);

        // Access the public field .stderr directly and .take() the option
        if let Some(stderr) = child.stderr.take() {
            let mut err_reader = BufReader::new(stderr);
            std::thread::spawn(move || {
                for line in err_reader.lines() {
                    if let Ok(l) = line {
                        println!("ffmpeg stderr (AV1): {}", l);
                    }
                }
            });
        }
        // --- FIX ENDS HERE ---

        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    self.parser.add_data(&buf[..n]);
                    let frames = self.parser.get_frames();
                    for frame in frames {
                        if let Err(e) = self.frame_tx.send(frame) {
                            eprintln!("{} Error sending AV1 frame: {}", e, self.encoder_str);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("{} Error reading AV1 chunk: {}", e, self.encoder_str);
                    break;
                }
            }
        }
        let _ = child.wait();
        self.current_offset += self.chunk_duration;
    }

    pub async fn next_frame(&mut self) -> Option<Vec<u8>> {
        // 1. Priority: Check if we have frames in the local queue
        if let Some(frame) = self.frame_queue.pop_front() {
            return Some(frame);
        }

        // 2. Check the crossbeam channel
        // We use try_recv() to avoid blocking the async executor.
        // Since start_chunking fills this channel, frames should be ready.
        if let Ok(frame) = self.frame_rx.try_recv() {
            return Some(frame);
        }

        None
    }

}


// Wrapper for Command to match your syntax
pub struct FfmpegCommand {
    cmd: Command,
}
impl FfmpegCommand {
    pub fn new() -> Self {
        Self { cmd: Command::new("ffmpeg") }
    }
    pub fn args(mut self, args: &[&str]) -> Self {
        self.cmd.args(args);
        self
    }
    pub fn hwaccel(mut self, _hw: &str) -> Self {
        self.cmd.args(&["-hwaccel", "cuda"]); // hardcoded for this example
        self
    }
    pub fn input(mut self, input: &str) -> Self {
        self.cmd.arg("-i").arg(input);
        self
    }
    pub fn spawn(mut self) -> std::io::Result<std::process::Child> {
        self.cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
    }
}

// --- 2. AV1 Parser Logic ---

#[derive(Debug, Clone)]
pub struct ObuUnit {
    pub obu_type: u8,
    pub data: Vec<u8>,
    pub is_sequence_header: bool,
}

pub struct Av1Parser {
    buffer: Vec<u8>,
    sequence_header: Option<Vec<u8>>,
}

impl Av1Parser {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            sequence_header: None,
        }
    }

    pub fn add_data(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    pub fn update_sequence_header(&mut self, data: &[u8]) {
        self.sequence_header = Some(data.to_vec());
    }

    pub fn get_sequence_header(&self) -> Option<&Vec<u8>> {
        self.sequence_header.as_ref()
    }

    fn parse_leb128(&self, offset: usize) -> Option<(usize, usize)> {
        let mut value: usize = 0;
        let mut bytes_read = 0;
        let mut shift = 0;

        loop {
            if offset + bytes_read >= self.buffer.len() { return None; }
            let byte = self.buffer[offset + bytes_read];
            value |= ((byte & 0x7F) as usize) << shift;
            bytes_read += 1;
            shift += 7;
            if (byte & 0x80) == 0 { break; }
            if bytes_read > 8 { return None; } // Safety
        }
        Some((value, bytes_read))
    }

    pub fn next_obu(&mut self) -> Option<ObuUnit> {
        if self.buffer.is_empty() { return None; }

        // OBU Header parsing
        let header_byte = self.buffer[0];
        let obu_type = (header_byte >> 3) & 0xF;
        let extension_flag = (header_byte >> 2) & 1;
        let has_size_field = (header_byte >> 1) & 1;

        if has_size_field == 0 {
            // Without size fields, we can't parse a stream easily.
            // ffmpeg -f obu usually includes them.
            self.buffer.clear();
            return None;
        }

        let mut offset = 1;
        if extension_flag == 1 {
            offset += 1;
            if self.buffer.len() < offset { return None; }
        }

        let (payload_size, leb_bytes) = self.parse_leb128(offset)?;
        offset += leb_bytes;

        let total_size = offset + payload_size;
        if self.buffer.len() < total_size { return None; }

        let obu_data = self.buffer[0..total_size].to_vec();
        self.buffer.drain(0..total_size);

        // OBU Type 1 is Sequence Header
        let is_sequence_header = obu_type == 1;
        if is_sequence_header {
            self.sequence_header = Some(obu_data.clone());
        }

        Some(ObuUnit {
            obu_type,
            data: obu_data,
            is_sequence_header,
        })
    }

    pub fn get_frames(&mut self) -> Vec<Vec<u8>> {
        let mut frames = Vec::new();
        while let Some(obu) = self.next_obu() {
            frames.push(obu.data);
        }
        frames
    }
}

// --- 3. The AV1 Decoder (Adapted from HevcDecoder) ---

pub struct Av1Decoder {
    frame_rx: Receiver<Vec<u8>>,
    packet_tx: Sender<Vec<u8>>,
    _stdin_handle: thread::JoinHandle<()>,
    _stderr_handle: thread::JoinHandle<()>,
    pub width: u32,
    pub height: u32,
    parser: Av1Parser,
    frame_buffer: VecDeque<Vec<u8>>,
    decoded_frames: VecDeque<Vec<u8>>,
    
    // Metrics
    pub frames_processed: usize,
    pub keyframes_seen: usize,
    pub expected_frame_size: usize,
    pub total_bytes_processed: f64,
    
    // Control
    decoder_string: String,
    priming_complete: bool,
    processing_semaphore: Arc<Semaphore>,
    
    // Sync
    id_queue: VecDeque<u32>,
    decoded_frame_counter: usize,
}

impl Av1Decoder {
    pub fn new(width: u32, height: u32, decoder_str: &str) -> Self {
        let frame_size = (width as usize) * (height as usize) * 3; // RGB24
        let decoder_string = decoder_str.to_string();

        //  - We construct the FFmpeg pipe here
        let mut child = FfmpegCommand::new()
            .args(&["-threads", "4"])
            // .hwaccel("cuda") // Enable if you have RTX 30/40 series
            .args(&["-hide_banner", "-loglevel", "error"])
            .args(&["-f", "obu"]) // Input format is raw OBU
            .args(&["-i", "-"])   // Read from stdin
            .args(&["-vsync", "0"])
            .args(&["-pix_fmt", "rgb24"]) // Output format
            .args(&["-f", "rawvideo", "-"]) // Write to stdout
            .spawn()
            .expect("Failed to spawn ffmpeg decoder");

        let stdout = child.stdout.take().unwrap();
        let stdin =  child.stdin.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        let (frame_tx, frame_rx) = unbounded::<Vec<u8>>();
        let (packet_tx, packet_rx) = bounded::<Vec<u8>>(100);

        // 1. STDOUT Reader (Decoded Frames)
        let decoder_str_clone = decoder_string.clone();
        thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut buffer = Vec::with_capacity(frame_size * 2);
            let mut chunk = vec![0u8; 8192];

            loop {
                match reader.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => {
                        buffer.extend_from_slice(&chunk[..n]);
                        while buffer.len() >= frame_size {
                            let frame = buffer.drain(..frame_size).collect::<Vec<u8>>();
                            if let Err(_) = frame_tx.send(frame) { return; }
                        }
                    }
                    Err(e) => {
                        eprintln!("{} Read Error: {}", decoder_str_clone, e);
                        break;
                    }
                }
            }
        });

        // 2. STDIN Writer (Encoded Packets)
        let decoder_str_clone2 = decoder_string.clone();
        let stdin_handle = thread::spawn(move || {
            let mut writer = stdin;
            for packet in packet_rx {
                if let Err(e) = writer.write_all(&packet) {
                    eprintln!("{} Write Error: {}", decoder_str_clone2, e);
                    break;
                }
                let _ = writer.flush();
            }
        });

        // 3. STDERR Handler
        let decoder_str_clone3 = decoder_string.clone();
        let stderr_handle = thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines() {
                if let Ok(l) = line {
                    println!("{} [FFMPEG]: {}", decoder_str_clone3, l);
                }
            }
        });

        Self {
            frame_rx,
            packet_tx,
            _stdin_handle: stdin_handle,
            _stderr_handle: stderr_handle,
            width,
            height,
            parser: Av1Parser::new(),
            frame_buffer: VecDeque::new(),
            decoded_frames: VecDeque::new(),
            frames_processed: 0,
            keyframes_seen: 0,
            expected_frame_size: frame_size,
            total_bytes_processed: 0.0,
            decoder_string: decoder_string,
            priming_complete: false,
            processing_semaphore: Arc::new(Semaphore::new(10)),
            id_queue: VecDeque::new(),
            decoded_frame_counter: 0,
        }
    }

    pub async fn process_packet(&mut self, packet: Vec<u8>, id: u32) {
        let _permit = self.processing_semaphore.acquire().await.unwrap();
        
        // Add to parser
        self.parser.add_data(&packet);
        
        // In AV1, we don't need to manually extract "Frames" as strictly as HEVC NALs
        // for the decoder pipe, but we do it to maintain your logic structure.
        let obus = self.parser.get_frames();
        
        for obu_data in obus {
            // Detect Sequence Header (Keyframe-ish)
            if obu_data.len() > 1 {
                let obu_type = (obu_data[0] >> 3) & 0xF;
                if obu_type == 1 {
                    self.keyframes_seen += 1;
                    print_pretty!(DebugColor::Magenta, "{} 🔑 Seq Header Detected", self.decoder_string);
                }
            }

            self.frames_processed += 1;
            
            // Send to FFmpeg
            if let Err(e) = self.packet_tx.send(obu_data) {
                 eprintln!("Failed to send to ffmpeg: {}", e);
            }
            
            // Keep ID sync
            self.id_queue.push_back(id);
        }
    }

    pub fn next_decoded_frame(&mut self) -> Option<(Vec<u8>, u32)> {
        // Poll the receiver channel
        loop {
            match self.frame_rx.try_recv() {
                Ok(frame) => {
                    if frame.len() == self.expected_frame_size {
                        self.decoded_frames.push_back(frame);
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return None,
            }
        }

        if let Some(frame) = self.decoded_frames.pop_front() {
            // In a real scenario, you handle ID queue sync carefully. 
            // For this visualization, we just pop.
            let id = self.id_queue.pop_front().unwrap_or(0);
            return Some((frame, id));
        }
        None
    }
}



#[tokio::main]
async fn main() {
    let width = 3840;
    let height = 2160;
    
    // 1. Scaling Configuration
    let scale_factor = 0.4; 
    let scaled_w = (width as f64 * scale_factor) as usize;
    let scaled_h = (height as f64 * scale_factor) as usize;

    let input_file = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/swordsmith_90fps.mp4";

    println!("Initializing {}x{} display (scaled from 4K)...", scaled_w, scaled_h);

    // 2. Setup Decoder and Channels
    let mut decoder = Av1Decoder::new(width, height, "AV1_DEC_01");
    let (tx_source, rx_source) = unbounded::<Vec<u8>>();
    
    // Removed the incorrect 'let now: f64 = ...' definition here

    // 3. Setup Source Thread (FFmpeg)
    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        
        rt.block_on(async {
            let chunk_len = 2.5; 
            let max_parallel_chunks = 1; // How many FFmpeg instances to run at once
            let (result_tx, mut result_rx) = mpsc::channel(100);
            
            let mut next_offset_to_encode = 0.0;
            let mut next_offset_to_send = 0.0;
            let mut reorder_buffer: BTreeMap<u64, Vec<Vec<u8>>> = BTreeMap::new();
            let mut active_workers = 0;

            let start_time = Instant::now();

            loop {
                // A. SPAWN WORKERS: Fill the pipeline until we hit max_parallel_chunks
                while active_workers < max_parallel_chunks {
                    let offset = next_offset_to_encode;
                    let tx = result_tx.clone();
                    let input = input_file.to_string();
                    
                    // Determine bitrate for this specific chunk
                    let elapsed = start_time.elapsed().as_secs();
                    let bitrate = if (elapsed / 5) % 2 == 0 { 100.0 } else { 1.0 };

                    tokio::spawn(async move {
                        // Create a fresh encoder for this chunk
                        let mut encoder = ChunkedAv1Encoder::new(
                            &input, 3840, 2160, "10M", chunk_len,
                            "AV1_Parallel_Worker".to_string(),
                            offset, 90.0, 60, false
                        );

                        // --- CRITICAL OPTIMIZATION ---
                        // Since we are running 3 encoders, we restrict each to use 
                        // only a portion of the CPU to prevent thrashing.
                        // Preset 12 is essential for real-time 4K software encoding.
                        encoder.start_chunking(bitrate, Instant::now()).await;

                        let mut frames = Vec::new();
                        while let Some(f) = encoder.next_frame().await {
                            frames.push(f);
                        }
                        
                        // Send frames back with the offset as a key (converted to u64 for BTreeMap)
                        let _ = tx.send(((offset * 1000.0) as u64, frames)).await;
                    });

                    next_offset_to_encode += chunk_len;
                    active_workers += 1;

                    // Loop video logic
                    if next_offset_to_encode > 30.0 {
                        next_offset_to_encode = 0.0;
                    }
                }

                // B. COLLECT RESULTS: Wait for any worker to finish
                if let Some((offset_key, frames)) = result_rx.recv().await {
                    reorder_buffer.insert(offset_key, frames);
                    active_workers -= 1;
                }

                // C. DISPATCH: Send chunks to the main thread in the correct order
                let current_key = (next_offset_to_send * 1000.0) as u64;
                while let Some(frames) = reorder_buffer.remove(&current_key) {
                    for frame in frames {
                        if let Err(_) = tx_source.send(frame) { return; }
                    }
                    next_offset_to_send += chunk_len;
                    if next_offset_to_send > 30.0 { next_offset_to_send = 0.0; }
                }
            }
        });
    });

    // 4. Visualization Window 
    let mut window = Window::new(
        "AV1 Realtime Decode - Scaled View",
        scaled_w,
        scaled_h,
        WindowOptions::default(),
    ).unwrap_or_else(|e| {
        panic!("{}", e);
    });

    let mut scaled_buffer = vec![0u32; scaled_w * scaled_h];
    let mut frame_count = 0;
    let mut last_log = Instant::now();

    let target_fps = 24.0; // Absolute cinema (the encoder is too slow for real time 90FPS@4K processing demo) 
    let target_frame_time = Duration::from_secs_f64(1.0 / target_fps);
    let mut next_frame_time = Instant::now();


   while window.is_open() && !window.is_key_down(Key::Escape) {
        
        // A. ALWAYS Ingest packets as fast as they arrive
        // We do this continuously so the UDP/channel buffer doesn't overflow
        while let Ok(packet) = rx_source.try_recv() {
            decoder.process_packet(packet, 0).await;
        }

        // B. PACE the Rendering (VSync Logic)
        let now = Instant::now();
        if now >= next_frame_time {
            
            // Try to get ONE decoded frame
            if let Some((rgb_data, _id)) = decoder.next_decoded_frame() {
                frame_count += 1;
                
                // Optimized Scaling (Same as before)
                for y in 0..scaled_h {
                    for x in 0..scaled_w {
                        let sx = (x * width as usize) / scaled_w;
                        let sy = (y * height as usize) / scaled_h;
                        let src_idx = (sy * width as usize + sx) * 3;
                        
                        if src_idx + 2 < rgb_data.len() {
                            let r = rgb_data[src_idx] as u32;
                            let g = rgb_data[src_idx + 1] as u32;
                            let b = rgb_data[src_idx + 2] as u32;
                            scaled_buffer[y * scaled_w + x] = (r << 16) | (g << 8) | b;
                        }
                    }
                }
                
                // Update window
                window.update_with_buffer(&scaled_buffer, scaled_w, scaled_h).unwrap();
                
                // Schedule next frame time. 
                // If we are lagging, reset to 'now' to catch up, otherwise add target duration.
                if now > next_frame_time + target_frame_time {
                    next_frame_time = now + target_frame_time;
                } else {
                    next_frame_time += target_frame_time;
                }
            } else {
                // Buffer underflow (no frame ready yet), just keep window open
                window.update();
            }
        } else {
            // If we have time to spare, sleep a tiny bit to save CPU
            // (But keep it short to keep polling network packets)
            thread::sleep(Duration::from_millis(1));
            window.update(); // Keep window responsive
        }

        // FPS Logging
        if last_log.elapsed().as_secs() >= 1 {
            println!("FPS: {} | Keyframes: {}", frame_count, decoder.keyframes_seen);
            frame_count = 0;
            last_log = Instant::now();
        }
    }
}


#[allow(unused)]
// Renders ASCII text into the minifb window with coordinates.
pub fn render_text(
    buffer: &mut [u32],
    text: &str,
    x: usize,
    y: usize,
    stride: usize,
    color: u32,
    scale: usize,
) {
    // Simple 5x7 pixel font (common for basic bitmap fonts)
    // Each character is represented as an array of 7 bytes, where each byte represents a row
    // and the bits in each byte represent the pixels in that row
    const FONT_WIDTH: usize = 5;
    const FONT_HEIGHT: usize = 7;
    const CHAR_SPACING: usize = 1;

    // Apply scaling
    let scaled_font_width = FONT_WIDTH * scale;
    let scaled_char_spacing = CHAR_SPACING * scale;

    // Define a simple bitmap font (only uppercase letters and some basic characters)
    // Each character is 5x7 pixels
    let font = [
        // Space
        [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        // !
        [0x04, 0x04, 0x04, 0x04, 0x00, 0x04, 0x00],
        // "
        [0x0A, 0x0A, 0x00, 0x00, 0x00, 0x00, 0x00],
        // #
        [0x0A, 0x0A, 0x1F, 0x0A, 0x1F, 0x0A, 0x0A],
        // $
        [0x04, 0x0F, 0x14, 0x0E, 0x05, 0x1E, 0x04],
        // %
        [0x18, 0x19, 0x02, 0x04, 0x08, 0x13, 0x03],
        // &
        [0x0C, 0x12, 0x14, 0x08, 0x15, 0x12, 0x0D],
        // '
        [0x0C, 0x04, 0x08, 0x00, 0x00, 0x00, 0x00],
        // (
        [0x02, 0x04, 0x08, 0x08, 0x08, 0x04, 0x02],
        // )
        [0x08, 0x04, 0x02, 0x02, 0x02, 0x04, 0x08],
        // *
        [0x00, 0x04, 0x15, 0x0E, 0x15, 0x04, 0x00],
        // +
        [0x00, 0x04, 0x04, 0x1F, 0x04, 0x04, 0x00],
        // ,
        [0x00, 0x00, 0x00, 0x00, 0x0C, 0x04, 0x08],
        // -
        [0x00, 0x00, 0x00, 0x1F, 0x00, 0x00, 0x00],
        // .
        [0x00, 0x00, 0x00, 0x00, 0x00, 0x0C, 0x0C],
        // /
        [0x00, 0x01, 0x02, 0x04, 0x08, 0x10, 0x00],
        // 0
        [0x0E, 0x11, 0x13, 0x15, 0x19, 0x11, 0x0E],
        // 1
        [0x04, 0x0C, 0x04, 0x04, 0x04, 0x04, 0x0E],
        // 2
        [0x0E, 0x11, 0x01, 0x02, 0x04, 0x08, 0x1F],
        // 3
        [0x1F, 0x02, 0x04, 0x02, 0x01, 0x11, 0x0E],
        // 4
        [0x02, 0x06, 0x0A, 0x12, 0x1F, 0x02, 0x02],
        // 5
        [0x1F, 0x10, 0x1E, 0x01, 0x01, 0x11, 0x0E],
        // 6
        [0x06, 0x08, 0x10, 0x1E, 0x11, 0x11, 0x0E],
        // 7
        [0x1F, 0x01, 0x02, 0x04, 0x08, 0x08, 0x08],
        // 8
        [0x0E, 0x11, 0x11, 0x0E, 0x11, 0x11, 0x0E],
        // 9
        [0x0E, 0x11, 0x11, 0x0F, 0x01, 0x02, 0x0C],
        // :
        [0x00, 0x0C, 0x0C, 0x00, 0x0C, 0x0C, 0x00],
        // ;
        [0x00, 0x0C, 0x0C, 0x00, 0x0C, 0x04, 0x08],
        // <
        [0x02, 0x04, 0x08, 0x10, 0x08, 0x04, 0x02],
        // =
        [0x00, 0x00, 0x1F, 0x00, 0x1F, 0x00, 0x00],
        // >
        [0x08, 0x04, 0x02, 0x01, 0x02, 0x04, 0x08],
        // ?
        [0x0E, 0x11, 0x01, 0x02, 0x04, 0x00, 0x04],
        // @
        [0x0E, 0x11, 0x01, 0x0D, 0x15, 0x15, 0x0E],
        // A
        [0x0E, 0x11, 0x11, 0x11, 0x1F, 0x11, 0x11],
        // B
        [0x1E, 0x11, 0x11, 0x1E, 0x11, 0x11, 0x1E],
        // C
        [0x0E, 0x11, 0x10, 0x10, 0x10, 0x11, 0x0E],
        // D
        [0x1C, 0x12, 0x11, 0x11, 0x11, 0x12, 0x1C],
        // E
        [0x1F, 0x10, 0x10, 0x1E, 0x10, 0x10, 0x1F],
        // F
        [0x1F, 0x10, 0x10, 0x1E, 0x10, 0x10, 0x10],
        // G
        [0x0E, 0x11, 0x10, 0x17, 0x11, 0x11, 0x0F],
        // H
        [0x11, 0x11, 0x11, 0x1F, 0x11, 0x11, 0x11],
        // I
        [0x0E, 0x04, 0x04, 0x04, 0x04, 0x04, 0x0E],
        // J
        [0x07, 0x02, 0x02, 0x02, 0x02, 0x12, 0x0C],
        // K
        [0x11, 0x12, 0x14, 0x18, 0x14, 0x12, 0x11],
        // L
        [0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x1F],
        // M
        [0x11, 0x1B, 0x15, 0x15, 0x11, 0x11, 0x11],
        // N
        [0x11, 0x11, 0x19, 0x15, 0x13, 0x11, 0x11],
        // O
        [0x0E, 0x11, 0x11, 0x11, 0x11, 0x11, 0x0E],
        // P
        [0x1E, 0x11, 0x11, 0x1E, 0x10, 0x10, 0x10],
        // Q
        [0x0E, 0x11, 0x11, 0x11, 0x15, 0x12, 0x0D],
        // R
        [0x1E, 0x11, 0x11, 0x1E, 0x14, 0x12, 0x11],
        // S
        [0x0F, 0x10, 0x10, 0x0E, 0x01, 0x01, 0x1E],
        // T
        [0x1F, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04],
        // U
        [0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x0E],
        // V
        [0x11, 0x11, 0x11, 0x11, 0x11, 0x0A, 0x04],
        // W
        [0x11, 0x11, 0x11, 0x15, 0x15, 0x15, 0x0A],
        // X
        [0x11, 0x11, 0x0A, 0x04, 0x0A, 0x11, 0x11],
        // Y
        [0x11, 0x11, 0x11, 0x0A, 0x04, 0x04, 0x04],
        // Z
        [0x1F, 0x01, 0x02, 0x04, 0x08, 0x10, 0x1F],
        // [
        [0x0E, 0x08, 0x08, 0x08, 0x08, 0x08, 0x0E],
        // \
        [0x00, 0x10, 0x08, 0x04, 0x02, 0x01, 0x00],
        // ]
        [0x0E, 0x02, 0x02, 0x02, 0x02, 0x02, 0x0E],
        // ^
        [0x04, 0x0A, 0x11, 0x00, 0x00, 0x00, 0x00],
        // _
        [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1F],
    ];

    let mut char_x = x;

    for c in text.chars() {
        let index = match c {
            ' ' => 0,
            '!' => 1,
            '"' => 2,
            '#' => 3,
            '$' => 4,
            '%' => 5,
            '&' => 6,
            '\'' => 7,
            '(' => 8,
            ')' => 9,
            '*' => 10,
            '+' => 11,
            ',' => 12,
            '-' => 13,
            '.' => 14,
            '/' => 15,
            '0'..='9' => (c as usize) - ('0' as usize) + 16,
            ':' => 26,
            ';' => 27,
            '<' => 28,
            '=' => 29,
            '>' => 30,
            '?' => 31,
            '@' => 32,
            'A'..='Z' => (c as usize) - ('A' as usize) + 33,
            'a'..='z' => (c as usize) - ('a' as usize) + 33, // Map lowercase to uppercase
            '[' => 59,
            '\\' => 60,
            ']' => 61,
            '^' => 62,
            '_' => 63,
            _ => 0, // Default to space for unknown characters
        };

        // Draw the character with scaling
        for row in 0..FONT_HEIGHT {
            for scaled_row in 0..scale {
                let buffer_y = y + (row * scale) + scaled_row;

                for col in 0..FONT_WIDTH {
                    // Check if the current pixel is set in the font bitmap
                    if (font[index][row] & (1 << (FONT_WIDTH - 1 - col))) != 0 {
                        for scaled_col in 0..scale {
                            let buffer_x = char_x + (col * scale) + scaled_col;

                            // Calculate buffer index and check bounds
                            if buffer_y < buffer.len() / stride && buffer_x < stride {
                                let buffer_index = buffer_y * stride + buffer_x;
                                if buffer_index < buffer.len() {
                                    buffer[buffer_index] = color;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Move to the next character position
        char_x += scaled_font_width + scaled_char_spacing;
    }
}
