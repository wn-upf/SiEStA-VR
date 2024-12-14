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

pub const WIDTH_ENCODER_: usize = 720; 
pub const HEIGHT_DECODER_:usize = 480; 


fn convert_rgb_to_u32(rgb: &[u8], width: usize, height: usize) -> Vec<u32> {
    let mut buffer = Vec::with_capacity(width * height);

    for y in 0..height {
        for x in 0..width {
            let r = rgb[(y * width + x) * 3] as u32;
            let g = rgb[(y * width + x) * 3 + 1] as u32;
            let b = rgb[(y * width + x) * 3 + 2] as u32;
            let a = 255u32; // Assume full alpha for RGB to RGBA conversion
            buffer.push((a << 24) | (r << 16) | (g << 8) | b); // ARGB format
        }
    }

    buffer
}

fn main() {
    let current_bitrate_mbps = 10.0;  // Example bitrate for frame extraction
    let mut timestamp = 0.0; 
    // Initialize the window
    let mut window = Window::new("FFmpeg Video Stream", 640, 360, WindowOptions {
        scale: minifb::Scale::X2,  // Optional: Adjust window scaling
        ..WindowOptions::default()
    }).expect("Unable to create window");
    // Set up window to continuously show the frames
    while window.is_open() && !window.is_key_down(minifb::Key::Escape) {
        // Process a new frame
        let frame = generate_sample_ffmpeg(current_bitrate_mbps, timestamp);

        if !frame.is_empty() {
            // Ensure the frame size matches the expected size
            // let expected_size = 1920 * 1080 * 3;  // RGB24 frame size for 1920x1080
            // if frame.len() != expected_size {
            //     eprintln!("Error: Frame data size does not match expected size.");
            //     continue; // Skip this frame if it is incorrect
            // }

            decode_hevc_to_rgb24(frame); 
            // let rgb_frame = convert_rgb_to_u32(&frame, 1920, 1080); 
            // // Update the window's buffer with the new frame
            // window.update_with_buffer(&rgb_frame, 1920, 1080).unwrap();
            timestamp += 1.0 /30.0;  // Increment timestamp for the next frame
        }

        // Sleep to control frame rate, adjust the duration as needed
    }
}
pub fn generate_sample_ffmpeg(current_bitrate_mbps: f32, timestamp: f64) -> Vec<u8> {
    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/bbb_1080p60fps.mp4"; 

    // let stri = format!("00:{:2.9}", timestamp as f64); // Ensure seconds as integer
    // let str2 = format!(r"select=gte(n\,{})", timestamp); 

    let hours = (timestamp / 3600.0) as u32;
    let minutes = ((timestamp % 3600.0) / 60.0) as u32;
    let seconds = timestamp % 60.0;
    let formatted_timestamp = format!("{:02}:{:02}:{:06.3}", hours, minutes, seconds);
    // println!("STRI: {}", stri); 
    let mut ffmpeg = match FfmpegCommand::new()
        .args([
            "-hwaccel", "cuda", // Use CUDA acceleration for encoding
            "-ss", &formatted_timestamp,       // Seek to the timestamp
            "-i", input_path,   // Input file
            "-c:v", "hevc_nvenc",             // Use NVIDIA H.265 encoder
            "-b:v", &format!("{}M", current_bitrate_mbps as f64 / 30.0), // Set bitrate
            "-frames:v", "1",    // Process a single frame
            "-f", "rawvideo",    // Output raw video
            "-pix_fmt", "rgb24", 
            "-vf", &format!("scale={}:{}", WIDTH_ENCODER_, HEIGHT_DECODER_),
            "-an",               // Disable audio
            "-"],                // Output      // Output to stdout
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


fn decode_hevc_to_rgb24(encoded_data: Vec<u8>) {
    // Spawn the ffmpeg process
    
    let mut ffmpeg = Command::new("ffmpeg")
        .args([
            "-hwaccel", "cuda",         // Use CUDA acceleration for decoding
            "-c:v", "hevc_cuvid",       // Use NVIDIA HEVC decoder
            "-i", "pipe:0",             // Read input from stdin (pipe)
            "-f", "rawvideo",           // Output raw video
            "-pix_fmt", "rgb24",        // Set pixel format to RGB24
            "-vf", &format!("scale={}:{}", WIDTH_ENCODER_, HEIGHT_DECODER_),   // Scale the video to 1920x1080
            "-an",                      // Disable audio
            "pipe:1",                   // Write output to stdout (pipe)
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("FAILED TO SPAWN FFMPEG"); 

       // Spawn stdin thread to write encoded data
       let stdin_thread = {
        let encoded_data_clone = encoded_data.clone();
        thread::spawn(move || {
            if let Some(mut stdin) = ffmpeg.stdin.take() {
                stdin.write_all(&encoded_data_clone).unwrap();
            }
        })
    };

    // Capture stdout
    let mut buf = Vec::new();
    ffmpeg.stdout.take().unwrap().read_to_end(&mut buf).unwrap();
    stdin_thread.join().unwrap();
   
    let frame_u32 = convert_rgb_to_u32(&buf, WIDTH_ENCODER_, HEIGHT_DECODER_);
    display_raw_rgb24_frame(buf, WIDTH_ENCODER_, HEIGHT_DECODER_);

}



pub fn display_raw_rgb24_frame(frame_data: Vec<u8>, width: usize, height: usize) {
    // Convert RGB24 byte vector to u32 vector for minifb

    let buffer: Vec<u32> = frame_data
        .chunks_exact(3)
        .map(|chunk| {
            let r = chunk[0] as u32;
            let g = chunk[1] as u32;
            let b = chunk[2] as u32;
            (r << 16) | (g << 8) | b
        })
        .collect();

    // Create window
    let mut window = Window::new(
        "Decoded Frame",
        width,
        height,
        WindowOptions {
            resize: false,
            scale: minifb::Scale::X1,
            ..WindowOptions::default()
        }
    )
    .expect("Unable to create window");

    // Set window limits
    window.limit_update_rate(Some(std::time::Duration::from_millis(16)));

    // Display loop
    while window.is_open() && !window.is_key_down(Key::Escape) {
        // Update the window with the frame data
        window
            .update_with_buffer(&buffer, width, height)
            .expect("Failed to update window");
    }
}
