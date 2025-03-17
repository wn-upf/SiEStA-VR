use anyhow::Result;
use crossbeam::channel::{bounded, unbounded, Receiver, Sender, TryRecvError};
use ffmpeg_sidecar::command::FfmpegCommand;
use minifb::{Key, Scale, Window, WindowOptions};
use rand::Rng;
use std::collections::VecDeque;
use std::fs;
use std::future::Future;
use std::io::{BufReader, Read, Write};
use std::path::Path;
use std::path::PathBuf;
use std::process::{ChildStdin, ChildStdout};
use std::task::{Context, Poll};
use std::time::Duration;
use std::time::Instant;
// Define the expected (encoder) dimensions.
pub const WIDTH_ENCODER: usize = 1920;
pub const HEIGHT_ENCODER: usize = 1080;

pub const INITIAL_BITRATE: &str = "2M";

pub const IDR_FRAME_SIZE_GOP: usize = 90;

pub const FRAMERATE_CODEC: usize = 60;

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
        //      pixels[0], pixels[1], pixels[2], pixels[3], pixels[4]);
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

pub struct HevcEncoder {
    input: String,
    width: u32,
    height: u32,
    bitrate: String,
    chunk_size_frames: usize,
    // Buffer to hold chunks; each chunk is a vector of bytes read from ffmpeg
    current_chunk: Vec<Vec<u8>>,
    // Track the start time (in seconds) for the next chunk
    timestamp_tracker: f32,

    idr_frame_size: usize,
    fps: usize,
    time_scale_chunks: f32,
}

impl HevcEncoder {
    /// Create a new encoder. Instead of launching a persistent ffmpeg process,
    /// we simply store the configuration and initialize the buffer and timestamp.
    pub fn new(
        input: &str,
        width: u32,
        height: u32,
        bitrate: &str,
        chunk_size_frames: usize,
        idr_frame_size: usize,
        fps: usize,
    ) -> Result<Self> {
        Ok(Self {
            input: input.to_owned(),
            width,
            height,
            bitrate: bitrate.to_owned(),
            chunk_size_frames,
            current_chunk: Vec::new(),
            timestamp_tracker: 0.0,
            idr_frame_size,
            fps: fps,
            time_scale_chunks: chunk_size_frames as f32 / fps as f32,
        })
    }

    /// Spawns a new ffmpeg process to extract a 1-second chunk starting at `timestamp_tracker`.
    /// It uses the `-ss` and `-t` options so that ffmpeg runs for only 1 second and then exits.
    async fn spawn_chunk(&mut self) -> Result<()> {
        // Format the start time (in seconds). For more precision you could use decimals.
        let start_time = format!("{}", self.timestamp_tracker);
        println!("Spawning chunk at timestamp: {}", start_time); // ADDED LOGGING

        let time_scale_chunks = format!("{:.3}", self.time_scale_chunks);

        let mut child = FfmpegCommand::new()
            .hwaccel("cuda")
            // Seek to the current timestamp and run for 1 second
            .args(&["-ss", &start_time])
            .args(&["-t", &time_scale_chunks])
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
            .args(&["-b:v", &self.bitrate, "-maxrate", &self.bitrate])
            .args(&["-rc-lookahead", "0"])
            .args(&["-g", &format!("{:.0}", self.idr_frame_size)])
            .args(&["-movflags", "+frag_keyframe+empty_moov"])
            .args(&["-flush_packets", "1"])
            .args(&["-bsf:v", "hevc_mp4toannexb"])
            .args(&["-an"])
            // Choose a container format that works well for streaming (e.g., mpegts)
            .args(&["-f", "mpegts", "-"])
            .spawn()?;

        let mut stdout = child
            .take_stdout()
            .expect("Failed to capture ffmpeg stdout in spawn_chunk");
        let mut chunk_data = Vec::new();
        let mut buf = [0u8; 4096];
        println!("Reading stdout from ffmpeg..."); // ADDED LOGGING
        while let Ok(n) = stdout.read(&mut buf) {
            if n == 0 {
                break;
            }
            chunk_data.extend_from_slice(&buf[..n]);
        }
        // println!("Finished reading stdout, waiting for child process..."); // ADDED LOGGING
        child.wait()?;
        // println!("Child process finished."); // ADDED LOGGING

        // Append the chunk data (which is now a complete 1-second segment) to our buffer.
        self.current_chunk.push(chunk_data);
        // Update timestamp_tracker so the next spawn will process the subsequent second.
        self.timestamp_tracker += self.time_scale_chunks;
        println!("Timestamp updated to: {}", self.timestamp_tracker); // ADDED LOGGING
        Ok(())
    }
    async fn spawn_chunk_files(&mut self) -> Result<()> {
        // Format the start time (in seconds)
        let start_time = format!("{}", self.timestamp_tracker);
        println!("Spawning chunk at timestamp: {}", start_time);

        // Define the output file pattern (files will be written into simu_decode_samples/)
        let output_pattern = "simu_decode_samples/frame_%04d.hevc";

        let mut child = FfmpegCommand::new()
            .hwaccel("cuda")
            // Seek to the current timestamp and process for a fixed duration (self.time_scale_chunks)
            .args(&["-ss", &start_time])
            .args(&["-t", &format!("{:.3}", self.time_scale_chunks)])
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
            .args(&["-b:v", &self.bitrate, "-maxrate", &self.bitrate])
            .args(&["-rc-lookahead", "0"])
            // Use your specified GOP structure (self.idr_frame_size), so not forcing every frame to be an IDR
            .args(&["-g", &format!("{:.0}", self.idr_frame_size)])
            .args(&["-movflags", "+frag_keyframe+empty_moov"])
            .args(&["-flush_packets", "1"])
            .args(&["-bsf:v", "hevc_mp4toannexb"])
            .args(&["-an"])
            // Use the segment muxer to split the output by frame.
            // -segment_frames 1 tells ffmpeg to start a new file after every encoded frame.
            .args(&["-f", "segment", "-segment_frames", "1", output_pattern])
            .spawn()?;

        child.wait()?;

        // Update the timestamp_tracker for the next chunk.
        self.timestamp_tracker += self.time_scale_chunks;
        println!("Timestamp updated to: {}", self.timestamp_tracker);
        Ok(())
    }

    pub fn load_frames_from_folder(folder: &str) -> Result<VecDeque<Vec<u8>>> {
        let mut frames = VecDeque::new();

        // Read directory entries and filter by files with ".hevc" extension.
        let mut entries: Vec<PathBuf> = fs::read_dir(folder)?
            .filter_map(|entry| {
                entry.ok().and_then(|e| {
                    let path = e.path();
                    // Check for .hevc extension (case-insensitive)
                    if path
                        .extension()
                        .and_then(|s| s.to_str())
                        .map(|ext| ext.eq_ignore_ascii_case("hevc"))
                        .unwrap_or(false)
                    {
                        Some(path)
                    } else {
                        None
                    }
                })
            })
            .collect();

        // Sort entries lexicographically so the filenames are in order.
        entries.sort();

        // Read each file into a Vec<u8> and push into the deque.
        for path in entries {
            let data = fs::read(&path)?;
            frames.push_back(data);
        }

        for entry in fs::read_dir(folder)? {
            // clear the folder after
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                fs::remove_dir_all(&path)?; // Recursively remove directories
            } else {
                fs::remove_file(&path)?; // Remove files
            }
        }

        Ok(frames)
    }

    /// Returns the next chunk of packets.
    /// If the internal buffer has fewer than `chunk_size_frames` packets, spawn a new ffmpeg process to add one.
    pub async fn try_next_chunk(&mut self) -> Result<Option<Vec<Vec<u8>>>> {
        if self.current_chunk.len() < self.chunk_size_frames {
            println!(
                "Current chunk size is {}, less than {}, spawning new chunk.",
                self.current_chunk.len(),
                self.chunk_size_frames
            ); // ADDED LOGGING
            self.spawn_chunk().await.unwrap();
        } else {
            println!("Current chunk size is sufficient, taking chunk."); // ADDED LOGGING
        }
        // Take the entire current chunk and return it.
        let chunk = std::mem::take(&mut self.current_chunk);
        Ok(Some(chunk))
    }

    pub async fn try_next_chunk_files(&mut self) -> Result<Option<VecDeque<Vec<u8>>>> {
        self.spawn_chunk_files().await.unwrap();

        let chunk = HevcEncoder::load_frames_from_folder("simu_decode_samples/").unwrap();

        // Take the entire current chunk and return it.
        // let chunk = std::mem::take(&mut self.current_chunk);
        Ok(Some(chunk))
    }
}

pub struct HevcDecoder {
    frame_rx: Receiver<Vec<u8>>,
    packet_tx: Sender<Vec<u8>>,
    _stdin_handle: std::thread::JoinHandle<()>,
    _stderr_handle: std::thread::JoinHandle<()>,
    width: u32,
    height: u32,
}

impl HevcDecoder {
    pub fn new(framerate: u32, width: u32, height: u32) -> Result<Self> {
        let frame_size = (width as usize) * (height as usize) * 3;
        let mut child = FfmpegCommand::new()
            .hwaccel("cuda")
            .args(&["-f", "mpegts", "-i", "-"]) // Changed input format to mpegts to match encoder output
            // .args(&["-c:v", "hevc"]) // No need to specify codec again, it should be auto-detected
            .args(&["-vf", &format!("fps={}", framerate)])
            .args(&["-pix_fmt", "rgb24"])
            .args(&["-f", "rawvideo", "-"])
            .spawn()?;

        let stdout = child.take_stdout().unwrap();
        let stdin = child.take_stdin().unwrap();
        let stderr = child.take_stderr().unwrap();

        // Changed: Explicitly specify Vec<u8> type for the channel
        let (frame_tx, frame_rx) = unbounded::<Vec<u8>>();
        let (packet_tx, packet_rx) = bounded::<Vec<u8>>(100); // Added type parameter
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
                    // packet is Vec<u8> here
                    eprintln!("Decoder write error: {}", e);
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
        })
    }

    pub fn try_next_frame(&self) -> Result<Option<Vec<u8>>> {
        // println!("TRY READ FRAME"); // ADDED LOGGING
        match self.frame_rx.try_recv() {
            Ok(frame) => {
                // println!("Frame received!"); // ADDED LOGGING
                Ok(Some(frame))
            }
            Err(TryRecvError::Empty) => {
                println!("No frame available yet."); // ADDED LOGGING
                Ok(None)
            }
            Err(TryRecvError::Disconnected) => {
                Err(anyhow::anyhow!("Decoder frame channel disconnected"))
            }
        }
    }
}

// A simple executor for running a single async task
struct SimpleExecutor;

impl SimpleExecutor {
    fn block_on<F: Future>(future: F) -> F::Output {
        // Create a pinned future
        let mut future = Box::pin(future);

        // Create a dummy task context
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);

        // Poll the future until it's ready
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => {
                    // Do nothing, just keep polling
                }
            }
        }
    }
}

fn main() -> Result<()> {
    let input_path =
        "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/bbb_1080p60fps.mp4";
    println!("HIHI!!!");

    let chunk_size_frames = 300; // 5 second buffer @60 FPS

    let mut encoder = HevcEncoder::new(
        input_path,
        WIDTH_ENCODER as u32,
        HEIGHT_ENCODER as u32,
        &INITIAL_BITRATE,
        chunk_size_frames,
        IDR_FRAME_SIZE_GOP,
        FRAMERATE_CODEC,
    )?;

    let decoder = HevcDecoder::new(60, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32)?;
    println!("SET UP ENCODE DECODE");
    let drop_probability = 0.000;
    let mut rng = rand::thread_rng();

    let scale_factor = 0.8;
    let scaled_width = (WIDTH_ENCODER as f64 * scale_factor) as usize;
    let scaled_height = (HEIGHT_ENCODER as f64 * scale_factor) as usize;

    let mut window = Window::new(
        "Video Stream",
        scaled_width,
        scaled_height,
        WindowOptions::default(),
    )?;
    println!("WINDOW OPEN");
    // Target 60 FPS (16.67ms per frame)
    let frame_duration = std::time::Duration::from_secs_f64(1.0 / FRAMERATE_CODEC as f64);
    let mut next_frame_time = std::time::Instant::now();
    let mut t = Instant::now();
    let mut number_fps: usize = 0; // var to keep track somehow of fps

    while window.is_open() && !window.is_key_down(Key::Escape) {
        // Process encoder packets in chunks
        if let Ok(Some(chunk)) = SimpleExecutor::block_on(encoder.try_next_chunk_files()) {
            // println!("chunk of size: {} received!", chunk.len());

            for packet in chunk {
                // println!("Sending packet of size: {}", packet.len());
                decoder.packet_tx.send(packet)?;
            }
        }

        // Process decoder frames
        if Instant::now().duration_since(t) >= Duration::from_secs(2) {
            t = Instant::now();
            println!("****************FPS COUNTER AVG 2secs: {}", number_fps / 2);
            number_fps = 0;
        }

        while let Ok(Some(frame)) = decoder.try_next_frame() {
            let frame_start_time = Instant::now(); // Record frame start time
            let pixels = convert_rgb_to_u32(&frame, WIDTH_ENCODER, HEIGHT_ENCODER);
            let scaled = scale_pixels(
                &pixels,
                WIDTH_ENCODER,
                HEIGHT_ENCODER,
                scaled_width,
                scaled_height,
            );
            window.update_with_buffer(&scaled, scaled_width, scaled_height)?;
            number_fps += 1;

            let elapsed_time = frame_start_time.elapsed(); // Calcu
            if elapsed_time < frame_duration {
                std::thread::sleep(frame_duration - elapsed_time); // Sleep to maintain frame rate
            }
        }
        // else{
        //     // println!("FAIIIIIIIIIIIIIIIL"); // No need to print fail when no frame is available, it's normal
        // }

        // // Maintain frame rate
        // let now = std::time::Instant::now();
        // if now < next_frame_time {
        //     std::thread::sleep(next_frame_time - now);
        // }
        next_frame_time += frame_duration;
    }

    Ok(())
}
