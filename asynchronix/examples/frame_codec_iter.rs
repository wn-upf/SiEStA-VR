use std::{io::{Read, Write}, process::Command, thread};
use ffmpeg_sidecar::{
    command::FfmpegCommand,
    event::{FfmpegEvent, LogLevel},
};
// use sdl2::pixels::Color;
// use sdl2::render::Canvas;
// use sdl2::video::Window;
// use sdl2::Sdl;
use std::process::Stdio; 
use minifb::Scale;
use std::time::Duration; 
use minifb::{Window, WindowOptions, Key};

pub const OFFSET_VIDEO_TIMESTAMP:f64 = 40.0; 

pub const WIDTH_ENCODER_: usize = 720; 
pub const HEIGHT_DECODER_:usize = 480; 

fn main() {
    let current_bitrate_mbps = 20.0;
    let mut timestamp = 0.0;

    // Create a persistent window
    let mut window = Window::new(
        "FFmpeg Video Stream",
        WIDTH_ENCODER_,
        HEIGHT_DECODER_,
        WindowOptions {
            scale: Scale::X2,
            ..WindowOptions::default()
        },
    )
    .expect("Unable to create window");

    // Limit the update rate to ~30 FPS (adjust as needed)
    // window.limit_update_rate(Some(Duration::from_millis(33)));

    // Main loop
    while window.is_open() && !window.is_key_down(Key::Escape) {
        // Process a new frame
        let frame = generate_sample_ffmpeg(current_bitrate_mbps, timestamp);

        if !frame.is_empty() {
            // Decode and convert to display format
            let rgb_frame = decode_hevc_to_rgb24(frame);

            // Update the window with the new frame
            window
                .update_with_buffer(&rgb_frame, WIDTH_ENCODER_, HEIGHT_DECODER_)
                .unwrap();
        } else {
            eprintln!("Failed to decode frame at timestamp: {}", timestamp);
        }

        // Increment timestamp for the next frame
        timestamp += 1.0 / 30.0; // For 30 FPS

        // Allow the program to pause or quit if desired (optional feature)
        if window.is_key_down(Key::Q) {
            println!("Quitting...");
            break;
        }
    }
}

pub fn generate_sample_ffmpeg(current_bitrate_mbps: f32, timestamp: f64) -> Vec<u8> {
    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/bbb_1080p60fps.mp4"; 

    // let stri = format!("00:{:2.9}", timestamp as f64); // Ensure seconds as integer
    // let str2 = format!(r"select=gte(n\,{})", timestamp); 

    let hours = (timestamp / 3600.0) as u32;
    let minutes = ((timestamp % 3600.0) / 60.0) as u32;
    let seconds = timestamp % 60.0;
    let formatted_timestamp = format!("{:02}:{:02}:{:06.3}", hours, minutes, seconds + OFFSET_VIDEO_TIMESTAMP);
    // println!("STRI: {}", stri); 
    let mut ffmpeg = match FfmpegCommand::new()
            .args([
                "-hwaccel", "cuda", 
                "-ss", &formatted_timestamp,  
                "-i", input_path,   
                "-pix_fmt", "yuv420p", 
                "-vf", &format!("scale={}:{},format=yuv420p", WIDTH_ENCODER_, HEIGHT_DECODER_),
                "-c:v", "hevc_nvenc",             
                "-b:v", &format!("{:.0}K", current_bitrate_mbps as f64 / 30.0 * 1000.0), 
                "-frames:v", "1",    
                "-f", "rawvideo",    
                "-an",               
                "-"],                
            )
            .spawn() {
                Ok(cmd) => cmd,
                Err(e) => {
                    eprintln!("Failed to spawn FFmpeg: {}", e);
                    return Vec::new();
                }
            };

    let mut ffmpeg_stdout = ffmpeg.take_stdout().unwrap();
    let mut buf = Vec::with_capacity(WIDTH_ENCODER_ * HEIGHT_DECODER_ * 3); // Adjust to expected frame size (RGB24)
    
    let n = ffmpeg_stdout.read_to_end(&mut buf).unwrap(); // Read until EOF
    println!("Frame data length: {}", n); // Debugging the actual frame data length

    // Return only the data read from FFmpeg
    buf
}

fn decode_hevc_to_rgb24(encoded_data: Vec<u8>) -> Vec<u32> {
   
    let mut ffmpeg = Command::new("ffmpeg")
        .args([
            "-hwaccel", "cuda",
            "-c:v", "hevc_cuvid",
            "-f", "rawvideo",  // Explicitly specify input format
            "-pix_fmt", "yuv420p",  // Match input pixel format
            "-s", &format!("{}x{}", WIDTH_ENCODER_, HEIGHT_DECODER_),  // Specify input dimensions
            "-i", "pipe:0",
            "-f", "rawvideo",  // Specify output format explicitly
            "-pix_fmt", "rgb24",  // Force RGB output
            "-vf", &format!("scale={}:{}", WIDTH_ENCODER_, HEIGHT_DECODER_),
            "-"  // Output to stdout
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
    let mut buf = Vec::new();
    let read_result = ffmpeg.stdout.take().unwrap().read_to_end(&mut buf);
    
    match read_result {
        Ok(n) => {
            println!("Decoded frame data length: {}", n);
            
            if buf.is_empty() {
                eprintln!("Warning: Decoded frame is empty");
                return Vec::new();
            }

            // Convert to u32 buffer for minifb
            convert_rgb_to_u32(&buf, WIDTH_ENCODER_, HEIGHT_DECODER_)
        },
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