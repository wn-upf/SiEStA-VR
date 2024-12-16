use std::sync::mpsc::{self, Sender, Receiver};
use std::{io::{Read, Write}, thread, time::Duration};
use std::process::{Command, Stdio};
use minifb::{Key, Window, WindowOptions, Scale};
use ffmpeg_sidecar::command::FfmpegCommand;

pub const OFFSET_VIDEO_TIMESTAMP: f64 = 10.0;
pub const WIDTH_ENCODER_: usize = 720;
pub const HEIGHT_DECODER_: usize = 480;

fn main() {
    let current_bitrate_mbps = 5.0;
    let mut timestamp = 0.0;

    // Create channels for communication
    let (encoder_tx, encoder_rx): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = mpsc::channel();
    let (decoder_tx, decoder_rx): (Sender<Vec<u32>>, Receiver<Vec<u32>>) = mpsc::channel();

    // Spawn encoder thread
    let encoder_tx_clone = encoder_tx.clone();
    thread::spawn(move || {
        while let Ok(frame) = generate_sample_ffmpeg(current_bitrate_mbps, timestamp) {
            if encoder_tx_clone.send(frame).is_err() {
                break;
            }
            timestamp += 1.0 / 30.0; // For 30 FPS
        }
    });

    // Spawn decoder thread
    thread::spawn(move || {
        while let Ok(encoded_frame) = encoder_rx.recv() {
            let decoded_frame = decode_hevc_to_rgb24(encoded_frame);
            if decoder_tx.send(decoded_frame).is_err() {
                break;
            }
        }
    });

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

    // Main loop
    while window.is_open() && !window.is_key_down(Key::Escape) {
        if let Ok(rgb_frame) = decoder_rx.try_recv() {
            // Update the window with the new frame
            window
                .update_with_buffer(&rgb_frame, WIDTH_ENCODER_, HEIGHT_DECODER_)
                .unwrap();
        }

        // Allow the program to pause or quit if desired
        if window.is_key_down(Key::Q) {
            println!("Quitting...");
            break;
        }

        // Limit the update rate (optional)
        thread::sleep(Duration::from_millis(33)); // ~30 FPS
    }
}

pub fn generate_sample_ffmpeg(current_bitrate_mbps: f32, timestamp: f64) -> Result<Vec<u8>, String> {
    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/bbb_1080p60fps.mp4";

    let hours = (timestamp / 3600.0) as u32;
    let minutes = ((timestamp % 3600.0) / 60.0) as u32;
    let seconds = timestamp % 60.0;
    let formatted_timestamp = format!(
        "{:02}:{:02}:{:06.3}",
        hours,
        minutes,
        seconds + OFFSET_VIDEO_TIMESTAMP
    );

    let mut ffmpeg = match FfmpegCommand::new()
        .args([
            "-hwaccel", "cuda",
            "-ss", &formatted_timestamp,
            "-i", input_path,
            "-pix_fmt", "yuv420p",
            "-vf",
            &format!("scale={}:{},format=yuv420p", WIDTH_ENCODER_, HEIGHT_DECODER_),
            "-c:v", "hevc_nvenc",
            "-b:v",
            &format!("{:.0}K", current_bitrate_mbps as f64 / 30.0 * 1000.0),
            "-frames:v", "",
            "-f", "rawvideo",
            "-an",
            "-",
        ])
        .spawn()
    {
        Ok(cmd) => cmd,
        Err(e) => return Err(format!("Failed to spawn FFmpeg: {}", e)),
    };

    let mut buf = Vec::with_capacity(WIDTH_ENCODER_ * HEIGHT_DECODER_ * 3); // Adjust to expected frame size (RGB24)
    ffmpeg
        .take_stdout()
        .take()
        .unwrap()
        .read_to_end(&mut buf)
        .map_err(|e| format!("Error reading FFmpeg output: {}", e))?;
    Ok(buf)
}

fn decode_hevc_to_rgb24(encoded_data: Vec<u8>) -> Vec<u32> {
    let mut ffmpeg = Command::new("ffmpeg")
        .args([
            "-hwaccel", "cuda",
            "-c:v", "hevc_cuvid",
            "-f", "rawvideo",
            "-pix_fmt", "yuv420p",
            "-s",
            &format!("{}x{}", WIDTH_ENCODER_, HEIGHT_DECODER_),
            "-i", "pipe:0",
            "-f", "rawvideo",
            "-pix_fmt", "rgb24",
            "-vf",
            &format!("scale={}:{}", WIDTH_ENCODER_, HEIGHT_DECODER_),
            "-",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to spawn FFmpeg");

    {
        let mut stdin = ffmpeg.stdin.take().unwrap();
        stdin.write_all(&encoded_data).unwrap();
    }

    let mut buf = Vec::new();
    ffmpeg
        .stdout
        .take()
        .unwrap()
        .read_to_end(&mut buf)
        .unwrap();

    convert_rgb_to_u32(&buf, WIDTH_ENCODER_, HEIGHT_DECODER_)
}

fn convert_rgb_to_u32(rgb_data: &[u8], width: usize, height: usize) -> Vec<u32> {
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
