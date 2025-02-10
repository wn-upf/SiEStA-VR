use bincode::de;
use ffmpeg_sidecar::{
    command::FfmpegCommand,
    event::{FfmpegEvent, LogLevel},
};
use rayon::prelude::*;
use std::fs::File;
use std::io::BufWriter;
use std::{
    error::Error,
    fs,
    io::{Read, Write},
    process::Command,
    thread,
};

use std::net::IpAddr;
use std::hash::DefaultHasher;
use std::io::BufRead;
// use sdl2::pixels::Color;
// use sdl2::render::Canvas;
// use sdl2::video::Window;
// use sdl2::Sdl;
use minifb::Scale;
use minifb::{Key, Window, WindowOptions};
use std::fs::create_dir_all;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use std::collections::HashMap;


use std::sync::{Arc, Mutex}; 
use std::hash::{Hash, Hasher};



use asynchronix::model::Context;
use crossbeam::channel::{unbounded, Receiver, RecvTimeoutError, Sender, TryRecvError};
// use futures_util::stream::empty;

use std::process::{Child};  
use std::os::unix::io::AsRawFd;
use nix::fcntl;
use nix::fcntl::{fcntl, FcntlArg, OFlag};
// use tokio::io::{AsyncReadExt, BufReader};

use std::io::{BufReader};

use lazy_static::lazy_static;

use std::process::{ChildStdout, ChildStdin};
use std::io::{ Seek, SeekFrom};


mod lib; 
use crate::lib::{DEBUG_PRINT_ENABLED, USE_FFMPEG, DebugColor};
// use crate::print_pretty; 
// use crate::{debug_bgprint, format_elapsed};
pub const DEADLINE_PACKETS_S: Duration = Duration::from_millis(100);
pub const MAX_DEADLINE_IN_STATS: usize = 10;
pub const OFFSET_VIDEO: f64 = 350.0;
use rand::Rng;
use std::cell::RefCell;
use std::fmt::{self, Debug};
use std::{
    cmp::Ordering,
    collections::{HashSet, VecDeque},
    io,
    marker::PhantomData,
    mem,
    // net::{TcpListener, UdpSocket},
};

// use crate::debug_print;
use crate::lib::models_XR::{
    XRDevice,
    XRServer, // ,XRClient
    HEIGHT_ENCODER,
    WIDTH_ENCODER,
};

use crate::lib::models_XR::SHARD_PREFIX_SIZE;
// use crate::lib::DebugColor;
use anyhow::{anyhow, Result};
use glam::{Quat, Vec3};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
// use std::io::{Read, Write};
use std::net::{Ipv4Addr};

use std::result::Result::Ok;
use tai_time::TaiTime;





lazy_static! {
    // Thread-safe FFmpeg process pool
    static ref FFMPEG_ENCODE_POOL_OLD: Arc<Mutex<HashMap<String, Child>>> = Arc::new(Mutex::new(HashMap::new()));
    // static ref FFMPEG_DECODE_POOL: Arc<Mutex<HashMap<String, Child>>> = Arc::new(Mutex::new(HashMap::new()));
}



pub const INITIAL_FRAMERATE_FPS: f32 = 90.0; 
pub const OFFSET_VIDEO_TIMESTAMP: f64 = 40.0;


// pub const WIDTH_ENCODER: usize = 1280;
// pub const HEIGHT_ENCODER: usize = 720;

static mut FRAME_INDEX_multibitrate: u32 = 0;

pub fn generate_multi_bitrate_frames(
    timestamp: f64,
    fps: f64,
    bitrate_ladder: &[f32], // Array of bitrates in Mbps
    output_dir: &str,
) -> Result<Vec<String>, Box<dyn Error>> {
    let input_path =
        "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/bbb_1080p60fps.mp4";
    let base_output_dir = Path::new(output_dir);
    create_dir_all(base_output_dir)?;

    // Precompute timestamp format once
    let timestamp_ = timestamp + OFFSET_VIDEO_TIMESTAMP;
    let hours = (timestamp_ / 3600.0) as u32;
    let minutes = ((timestamp_ % 3600.0) / 60.0) as u32;
    let seconds = timestamp_ % 60.0;
    let formatted_timestamp = format!("{:02}:{:02}:{:06.3}", hours, minutes, seconds);

    // Use Rayon for parallel processing
    let encoded_files: Vec<_> = bitrate_ladder
        .par_iter()
        .filter_map(|&current_bitrate_mbps| {
            let output_filename: String;
            unsafe {
                output_filename = format!(
                    "frame_{}_{:.1}mbps.mp4",
                    FRAME_INDEX_multibitrate, current_bitrate_mbps,
                );
            }

            let output_path = base_output_dir.join(&output_filename);

            let bitrate_command = format!("{:.0}K", current_bitrate_mbps as f64 * 1000.0);
            println!(
                "Encoding frame at timestamp {} with bitrate {}",
                formatted_timestamp, bitrate_command
            );

            let process = Command::new("ffmpeg")
                .args([
                    "-hwaccel",
                    "cuda",
                    "-ss",
                    &formatted_timestamp,
                    "-i",
                    input_path,
                    "-pix_fmt",
                    "yuv420p",
                    "-vf",
                    &format!("scale={}:{},format=yuv420p", WIDTH_ENCODER, HEIGHT_ENCODER),
                    "-c:v",
                    "hevc_nvenc",
                    "-preset",
                    "fast",
                    "-rc",
                    "vbr_hq",
                    "-b_ref_mode",
                    "2",
                    "-bf",
                    "3",
                    "-temporal-aq",
                    "1",
                    "-spatial-aq",
                    "1",
                    "-aq-strength",
                    "8",
                    "-frames:v",
                    "1",
                    "-b:v",
                    &bitrate_command,
                    "-an",
                    "-y", // Overwrite output files
                    output_path.to_str().unwrap(),
                ])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn();

            match process {
                Ok(mut proc) => match proc.wait_with_output() {
                    Ok(output) if output.status.success() => {
                        if output_path.exists() {
                            Some(output_filename)
                        } else {
                            None
                        }
                    }
                    Ok(output) => {
                        eprintln!("FFmpeg error: {}", String::from_utf8_lossy(&output.stderr));
                        None
                    }
                    Err(e) => {
                        eprintln!("Failed to wait for process: {}", e);
                        None
                    }
                },
                Err(e) => {
                    eprintln!("Failed to spawn ffmpeg: {}", e);
                    None
                }
            }
        })
        .collect();

    Ok(encoded_files)
}

pub fn encode_frame_sequence(
    start_timestamp: f64,
    frame_count: u32,
    fps: f64,
    output_dir: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let bitrate_ladder = vec![0.5, 1.0, 2.0, 4.0, 8.0]; // Bitrates in Mbps

    for frame_idx in 0..frame_count {
        let timestamp = start_timestamp + (frame_idx as f64 / fps);

        match generate_multi_bitrate_frames(timestamp, fps, &bitrate_ladder, output_dir) {
            Ok(files) => {
                println!(
                    "Successfully encoded frame at {} with {} variations:",
                    timestamp,
                    files.len()
                );
                for file in files {
                    println!("  - {}", file);
                }
            }
            Err(e) => eprintln!("Error encoding frame at {}: {}", timestamp, e),
        }
        unsafe {
            FRAME_INDEX_multibitrate += 1;
        }
    }

    Ok(())
}


// pub fn generate_all_bitrates_all_frames(
//     list_bitrates: Vec<f32>,
//     fps: f64,
// ) -> Result<(), std::io::Error> {
//     let max_duration_movie = 30.0;
//     let input_path =
//         "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/bbb_1080p60fps.mp4";
//     let base_output_dir = "/home/boris/Desktop/Rust_MG1/asynchronix/temp_video_bitrates";
//     let frame_size = WIDTH_ENCODER * HEIGHT_ENCODER * 3 / 2; // Assuming YUV420p

//     // Create output directory if it doesn't exist
//     fs::create_dir_all(base_output_dir)?;

//     for bitrate in list_bitrates {
//         println!("Encoding movie with bitrate: {} Mbps", bitrate);

//         let bitrate_command = format!("{:.0}K", bitrate as f64 * 1000.0);
//         let output_dir = format!("{}/bitrate_{}", base_output_dir, bitrate);
//         fs::create_dir_all(&output_dir)?;

//         let mut ffmpeg = Command::new("ffmpeg")
//             .args([
//                 "-hwaccel",
//                 "cuda",
//                 "-i",
//                 input_path,
//                 "-t",
//                 &max_duration_movie.to_string(),
//                 "-pix_fmt",
//                 "yuv420p",
//                 "-vf",
//                 &format!("scale={}:{},format=yuv420p", WIDTH_ENCODER, HEIGHT_ENCODER),
//                 "-c:v",
//                 "hevc_nvenc",
//                 "-b:v",
//                 &bitrate_command,
//                 "-an",
//                 "-f",
//                 "rawvideo",
//                 "-",
//             ])
//             .stdout(Stdio::piped())
//             .spawn()?;

//         let stdout = ffmpeg.stdout.as_mut().unwrap();
//         let mut buffer = vec![0u8; frame_size];
//         let mut frame_index = 0;

//         loop {
//             match stdout.read_exact(&mut buffer) {
//                 Ok(()) => {
//                     // Successfully read a frame
//                     let frame_path = format!("{}/frame_{:05}.raw", output_dir, frame_index);
//                     let mut file = BufWriter::new(File::create(frame_path)?);
//                     file.write_all(&buffer)?;

//                     frame_index += 1;
//                 }
//                 Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
//                     // End of file reached
//                     break;
//                 }
//                 Err(e) => {
//                     // Other error
//                     return Err(e);
//                 }
//             }
//         }

//         let status = ffmpeg.wait()?;
//         if !status.success() {
//             return Err(std::io::Error::new(
//                 std::io::ErrorKind::Other,
//                 format!("FFmpeg failed for bitrate {}", bitrate),
//             ));
//         }

//         println!("Completed encoding for bitrate: {} Mbps", bitrate);
//     }

//     Ok(())
// }



pub fn generate_sample_ffmpeg(current_bitrate_mbps: f32, timestamp: f64) -> Vec<u8> {
    let input_path =
        "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/bbb_1080p60fps.mp4";

    // let stri = format!("00:{:2.9}", timestamp as f64); // Ensure seconds as integer
    // let str2 = format!(r"select=gte(n\,{})", timestamp);

    let hours = (timestamp / 3600.0) as u32;
    let minutes = ((timestamp % 3600.0) / 60.0) as u32;
    let seconds = timestamp % 60.0;
    let formatted_timestamp = format!(
        "{:02}:{:02}:{:06.3}",
        hours,
        minutes,
        seconds + OFFSET_VIDEO_TIMESTAMP
    );
    // println!("STRI: {}", stri);
    let mut ffmpeg = match FfmpegCommand::new()
        .args([
            "-hwaccel",
            "cuda",
            "-ss",
            &formatted_timestamp,
            "-i",
            input_path,
            "-pix_fmt",
            "yuv420p",
            "-vf",
            &format!(
                "scale={}:{},format=yuv420p",
                WIDTH_ENCODER, HEIGHT_ENCODER
            ),
            "-c:v",
            "hevc_nvenc",
            "-b:v",
            &format!("{:.0}K", current_bitrate_mbps as f64 * 1000.0),
            "-frames:v",
            "1",
            "-f",
            "rawvideo",
            "-an",
            "-",
        ])
        .spawn()
    {
        Ok(cmd) => cmd,
        Err(e) => {
            eprintln!("Failed to spawn FFmpeg: {}", e);
            return Vec::new();
        }
    };

    let mut ffmpeg_stdout = ffmpeg.take_stdout().unwrap();
    let mut buf = Vec::with_capacity(WIDTH_ENCODER * HEIGHT_ENCODER * 3); // Adjust to expected frame size (RGB24)

    let n = ffmpeg_stdout.read_to_end(&mut buf).unwrap(); // Read until EOF
    println!("Frame data length: {}", n); // Debugging the actual frame data length

    // std::fs::write("iter_encoded_frame.mp4", &buf).unwrap();

    // Return only the data read from FFmpeg
    buf
}

fn decode_hevc_to_rgb24(encoded_data: Vec<u8>) -> Vec<u32> {
    // std::fs::write("iter_decoded_frame.mp4", &encoded_data).unwrap();

    println!("Decoded buffer size: {}", encoded_data.len());

    let mut ffmpeg = Command::new("ffmpeg")
        .args([
            // "-loglevel",
            // "debug",
            "-hwaccel",
            "cuda",
            "-c:v",
            "hevc_cuvid",
            "-f",
            "rawvideo", // Explicitly specify input format
            "-pix_fmt",
            "yuv420p", // Match input pixel format
            "-s",
            &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER), // Specify input dimensions
            "-i",
            "pipe:0",
            "-f",
            "rawvideo", // Specify output format explicitly
            "-pix_fmt",
            "rgb24", // Force RGB output
            "-vf",
            &format!("scale={}:{}", WIDTH_ENCODER, HEIGHT_ENCODER),
            "-", // Output to stdout
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to spawn FFmpeg");
    // Write input data to FFmpeg stdin
    {
        let mut stdin = ffmpeg.stdin.take().unwrap();
        stdin.write_all(&encoded_data).unwrap();
    }

    // Capture FFmpeg stdout
    let mut buf: Vec<u8> = Vec::new();
    let read_result = ffmpeg.stdout.take().unwrap().read_to_end(&mut buf);

    match read_result {
        Ok(n) => {
            println!("Decoded frame data length: {} Kb", n / 1000);

            if buf.is_empty() {
                eprintln!("Warning: Decoded frame is empty");
                return Vec::new();
            }

            // Convert to u32 buffer for minifb
            convert_rgb_to_u32(&buf, WIDTH_ENCODER, HEIGHT_ENCODER)
        }
        Err(e) => {
            eprintln!("Error reading FFmpeg output: {}", e);
            Vec::new()
        }
    }
}

fn convert_rgb_to_u32(rgb_data: &[u8], width: usize, height: usize) -> Vec<u32> {
    if rgb_data.len() != width * height * 3 {
        eprintln!(
            "Unexpected RGB data length. Expected {}, got {}",
            width * height * 3,
            rgb_data.len()
        );
        return Vec::new();
    }

    rgb_data
        .chunks_exact(3)
        .map(|chunk| {
            let r = chunk[0] as u32;
            let g = chunk[1] as u32;
            let b = chunk[2] as u32;
            (r << 16) | (g << 8) | b
        })
        .collect()
}





lazy_static! {
    static ref FFMPEG_ENCODE_POOL: Arc<Mutex<HashMap<String, EncoderProcess>>> = 
        Arc::new(Mutex::new(HashMap::new()));
}

struct EncoderProcess {
    child: Child,
    stdin: BufWriter<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    buffer: Vec<u8>,
    alive: bool,
}





#[rustfmt::skip]
pub fn generate_sample_ffmpeg_opti_alvr(current_bitrate_mbps: f32, timestamp: f64, fps: f64, ip: IpAddr) -> Result<Vec<u8>, std::io::Error> {
    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/bbb_sunflower_2160p_60fps_stereo_abl.mp4";
    // given this sample video, choosing 100

    // Create a unique key for this specific encoding configuration
    let config_key = format!(
        "{}_{}_{}_{}_{}",
        input_path, current_bitrate_mbps, timestamp, WIDTH_ENCODER, HEIGHT_ENCODER,
    );

    let offset_video = 
        {
            let mut hasher = DefaultHasher::new();
            ip.hash(&mut hasher);
            let hash = hasher.finish(); 
            (hash%500) as f64
        }; // keep offset of video between 0-500

    // How many times to retry before giving up
    let max_retries = 3;
    let mut attempt = 0;

    loop {
        {
            // Lock the pool and either get an existing process or create a new one.
            let mut pool = FFMPEG_ENCODE_POOL_OLD.lock().unwrap();
            if !pool.contains_key(&config_key) {
                let timestamp_ = timestamp + offset_video;
                let hours = (timestamp_ / 3600.0) as u32;
                let minutes = ((timestamp_ % 3600.0) / 60.0) as u32;
                let seconds = timestamp_ % 60.0;
                let formatted_timestamp = format!("{:02}:{:02}:{:06.3}", hours, minutes, seconds);
                print_pretty!(DebugColor::ForestGreen, "T_VIDEO={}", formatted_timestamp);

                let bitrate_command = format!("{:.0}K", current_bitrate_mbps as f64 * 1000.0);

                print_pretty!(
                    DebugColor::DarkBlue,
                    "[DBG bitrate] frame: {}, per second: {}; command {}",
                    current_bitrate_mbps,
                    current_bitrate_mbps * INITIAL_FRAMERATE_FPS,
                    bitrate_command
                );

               
                let process = Command::new("ffmpeg")
                .args([
                        "-hwaccel",
                        "cuda",
                        "-ss",
                        &formatted_timestamp,
                        "-i",
                        input_path,
                        "-pix_fmt",
                        "yuv420p",
                        "-vf",
                        &format!("scale={}:{},format=yuv420p", WIDTH_ENCODER, HEIGHT_ENCODER),
                        "-c:v",
                        "hevc_nvenc",
                        "-preset",
                        "fast",
                        "-rc",
                        "cbr",
                        "-b_ref_mode",
                        "2",
                        "-bf",
                        "3",
                        "-temporal-aq",
                        "1",
                        "-spatial-aq",
                        "1",
                        "-aq-strength",
                        "8",
                        "-frames:v",
                        "1",
                        "-b:v",
                        &bitrate_command,
                        "-an",
                        "-f",
                        "mp4",
                        "-bsf:v",
                        "hevc_mp4toannexb",
                        "-movflags",
                        "+frag_keyframe+empty_moov",
                        "-",
                    ])
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .expect("Failed to spawn FFmpeg encoder");

                pool.insert(config_key.clone(), process);
            }
        } // End of pool lock

        // Read the encoded buffer.
        let mut buf = Vec::new();
        {
            // Lock again to get a mutable reference to the process.
            let mut pool = FFMPEG_ENCODE_POOL_OLD.lock().unwrap();
            let ffmpeg = pool.get_mut(&config_key).expect("Process not found in pool");
            ffmpeg
                .stdout
                .as_mut()
                .unwrap()
                .read_to_end(&mut buf)
                .expect("Failed to read encoded buffer");
        }

        print_pretty!(
            DebugColor::Salmon,
            "Encoded frame size: {} bytes ({} KB)\nData = {:?}",
            buf.len(),
            buf.len() / 1024,
            &buf[..std::cmp::min(50, buf.len())]
        );

        if !buf.is_empty() {
            return Ok(buf);
        } else {
            attempt += 1;
            if attempt >= max_retries {
                panic!("Failed to generate a non-empty encoded frame after {} attempts", max_retries);
            }
            // Remove the problematic process so that a new one is spawned next time.
            let mut pool = FFMPEG_ENCODE_POOL_OLD.lock().unwrap();
            pool.remove(&config_key);
            print_pretty!(
                DebugColor::Red,
                "Encoded frame is empty, retrying (attempt {}/{})",
                attempt,
                max_retries
            );
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Empty encoded frame",
            ));
        }
    }
}









// pub fn generate_sample_ffmpeg_opti(
//     current_bitrate_mbps: f32,
//     raw_frame: &[u8],
// ) -> Result<Vec<u8>, Box<dyn Error>> {
//     let config_key = format!(
//         "raw_{}_{}_{}",
//         current_bitrate_mbps, WIDTH_ENCODER, HEIGHT_ENCODER,
//     );

//     let mut pool = FFMPEG_ENCODE_POOL.lock().unwrap();
    
//     // Clean up dead processes first
//     pool.retain(|_, encoder| encoder.alive);

//     let encoder = pool.entry(config_key.clone()).or_insert_with(|| {
//         let bitrate = (current_bitrate_mbps * 1000.0) as u32;
        
//         let mut child = Command::new("ffmpeg")
//             .args(&[
//         "-f", "rawvideo",
//         "-pix_fmt", "yuv420p",
//         "-s", &format!("{}x{}", WIDTH_ENCODER, HEIGHT_ENCODER),
//         "-i", "pipe:0",
//         "-c:v", "hevc_nvenc",
//         "-preset", "fast",
//         "-rc", "cbr",
//         "-b:v", &format!("{}K", bitrate),
//         "-frames:v", "1",
//         "-an",
//         "-g", "1",
//         "-f", "hevc",
//         "-bsf:v", "hevc_mp4toannexb",  // Ensure proper NAL unit formatting
//         "-vf", "format=yuv420p",        // Force pixel format
//         "-strict", "experimental",
//         "pipe:1"
//     ])
//             .stdin(Stdio::piped())
//             .stdout(Stdio::piped())
//             .stderr(Stdio::piped()) // Capture stderr for debugging
//             .spawn()
//             .expect("Failed to start FFmpeg");

//         // Start error logging thread
//         let stderr = child.stderr.take().unwrap();
//         std::thread::spawn(move || {
//             let mut reader = BufReader::new(stderr);
//             let mut buf = String::new();
//             while let Ok(n) = reader.read_line(&mut buf) {
//                 if n == 0 { break; }
//                 eprintln!("[FFmpeg] {}", buf.trim());
//                 buf.clear();
//             }
//         });

//         EncoderProcess {
//             stdin: BufWriter::new(child.stdin.take().unwrap()),
//             stdout: BufReader::new(child.stdout.take().unwrap()),
//             child,
//             buffer: Vec::with_capacity(1024 * 1024),
//             alive: true,
//         }
//     });

//     // Check if process died unexpectedly
//     if let Ok(Some(status)) = encoder.child.try_wait() {
//         encoder.alive = false;
//         return Err(format!("Encoder died with status {:?}", status).into());
//     }

//     // Handle write failure
//     encoder.stdin.write_all(raw_frame).map_err(|e| {
//         encoder.alive = false;
//         e
//     })?;
    
//     encoder.stdin.flush().map_err(|e| {
//         encoder.alive = false;
//         e
//     })?;

//     // Read loop with error handling
//     let mut encoded = Vec::new();
//     loop {
//         let mut tmp = [0u8; 4096];
//         match encoder.stdout.read(&mut tmp) {
//             Ok(0) => break,
//             Ok(n) => {
//                 encoder.buffer.extend_from_slice(&tmp[..n]);
//                 if let Some(nal) = find_nal_unit(&encoder.buffer) {
//                     encoded.extend_from_slice(nal);
//                     encoder.buffer.drain(..nal.len());
//                     break;
//                 }
//             }
//             Err(e) => {
//                 encoder.alive = false;
//                 return Err(e.into());
//             }
//         }
//     }

//     Ok(encoded)
// }



// fn find_nal_unit(buffer: &[u8]) -> Option<&[u8]> {
//     // Start from beginning of buffer to find first start code
//     let mut start = 0;
    
//     // Look for 0x00000001 start code
//     while start + 4 <= buffer.len() {
//         if buffer[start..start + 4] == [0, 0, 0, 1] {
//             break;
//         }
//         start += 1;
//     }
    
//     if start + 4 > buffer.len() {
//         return None; // No start code found
//     }
    
//     // Find next start code or end of buffer
//     let mut end = start + 4;
//     while end + 4 <= buffer.len() {
//         if buffer[end..end + 4] == [0, 0, 0, 1] {
//             return Some(&buffer[start..end]);
//         }
//         end += 1;
//     }
    
//     // If we reach here, take until end of buffer
//     Some(&buffer[start..])
// }



// const YUV420_FRAME_SIZE: usize = (WIDTH_ENCODER * HEIGHT_ENCODER * 3) / 2;  // 1.5 bytes per pixel

// struct YuvFrameLoader {
//     file: BufReader<File>,
//     current_frame: usize,
//     total_frames: usize,
// }

// impl YuvFrameLoader {
//     fn new(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
//         let file = File::open(path)?;
//         let metadata = file.metadata()?;
//         let total_frames = (metadata.len() as usize) / YUV420_FRAME_SIZE;
        
//         Ok(Self {
//             file: BufReader::new(file),
//             current_frame: 0,
//             total_frames,
//         })
//     }

//     fn next_frame(&mut self) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
//         if self.current_frame >= self.total_frames {
//             return Ok(None);  // End of file
//         }

//         let mut buffer = vec![0u8; YUV420_FRAME_SIZE];
//         self.file.read_exact(&mut buffer)?;
//         self.current_frame += 1;
        
//         Ok(Some(buffer))
//     }

//     fn reset(&mut self) -> Result<(), Box<dyn std::error::Error>> {
//         self.file.seek(SeekFrom::Start(0))?;
//         self.current_frame = 0;
//         Ok(())
//     }
// }
///////////////// MAIN FUNCTION - TEST AREA ////////////////////////

fn main() {
    let mut current_bitrate_mbps = 20.0;
    let mut timestamp = 2.0;


    let scale_factor = 0.4;
    let scaled_width = (WIDTH_ENCODER as f64 * scale_factor) as usize;
    let scaled_height = (HEIGHT_ENCODER as f64 * scale_factor) as usize;
    
    // Create a persistent window
    let mut window = Window::new(
        "FFmpeg Video Stream",
        scaled_width,
        scaled_height,
        WindowOptions {
            scale: Scale::X2,
            ..WindowOptions::default()
        },
    )
    .expect("Unable to create window");

    // Limit the update rate to ~30 FPS (adjust as needed)
    // window.limit_update_rate(Some(Duration::from_millis(33)));

    println!("pregenerating video");

    let bitrates = vec![1.0, 2.0, 4.0, 8.0]; // Bitrates in Mbps
    let fps = 30.0;

    let start_timestamp = 2.0;
    let frame_count = 100;
    let base_output_dir = "/home/boris/Desktop/Rust_MG1/asynchronix/temp_video_bitrates";


    // let mut yuv_loader = YuvFrameLoader::new("raw_frames.yuv").unwrap();

    // println!("Loaded YUV file with {} frames", yuv_loader.total_frames);

    // Main loop
    let mut decoded_index = 0;
    while window.is_open() && !window.is_key_down(Key::Escape) {
        timestamp += 1.0 / 30.0; // For 30 FPS
        println!("TIMESTAMP: {}", timestamp);
        if timestamp >= 2.0 && timestamp < 3.0 {
            current_bitrate_mbps = 0.9;
        } else if timestamp >= 2.5 && timestamp < 3.5 {
            current_bitrate_mbps = 0.1;
        }
        else{
            current_bitrate_mbps = 0.7;
        }

        // Process a new frame
        // let frame_result = generate_sample_ffmpeg_opti_alvr(current_bitrate_mbps, timestamp, fps, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))); 
        let frame_result: std::result::Result<Vec<u8>, std::io::Error> = Ok(generate_sample_ffmpeg(current_bitrate_mbps, timestamp) ); 

        match frame_result {
            Ok(frame) => {

                if !frame.is_empty() {

                    // Decode and convert to display format
                    let rgb_frame = decode_hevc_to_rgb24(frame);
                    
                    if rgb_frame.len() == WIDTH_ENCODER * HEIGHT_ENCODER * 3 {
                        window
                            .update_with_buffer(&rgb_frame, WIDTH_ENCODER, HEIGHT_ENCODER)
                            .expect("Failed to update window");
                    } else {
                        eprintln!("Invalid RGB frame size: {}", rgb_frame.len());
                    }


                    // Update the window with the new frame
                    window
                        .update_with_buffer(&rgb_frame, WIDTH_ENCODER, HEIGHT_ENCODER)
                        .unwrap();
                    } else {
                        eprintln!("Failed to decode frame at timestamp: {}", timestamp);
                    }
            }

            Err(e) => {
                eprintln!("Encoding failed: {}. Retrying...", e);
                // The dead encoder will be cleaned up on next iteration
                continue;
            }
        }


        // Increment timestamp for the next frame

        // Allow the program to pause or quit if desired (optional feature)
        if window.is_key_down(Key::Q) {
            println!("Quitting...");
            break;
        }
        
    }
}





