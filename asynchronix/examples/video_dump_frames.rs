extern crate ffmpeg_next as ffmpeg;

use std::error::Error;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};

pub const IDR_REFRESH: usize = 20; // Refresh IDR every K frames

fn main() -> Result<(), Box<dyn Error>> {
    // Initialize FFmpeg library
    ffmpeg::init().unwrap();

    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/bbb_1080p60fps.mp4"; 
    let output_path = "/home/boris/Desktop/Rust_MG1/asynchronix/Video_Sink/output_video.mp4";
    let temp_frame_dir = "/home/boris/Desktop/Rust_MG1/asynchronix/Video_Sink/frames/";

    let max_timestamp = 60;
    // Check if input file exists
    if !PathBuf::from(input_path).exists() {
        return Err("Input file does not exist.".into());
    }

    // Create a temporary directory to store individual frames
    fs::create_dir_all(temp_frame_dir)?;

    // Step 1: Extract frames from the video
    let extract_status = Command::new("ffmpeg")
        .arg("-i")
        .arg(input_path)
        .arg("-vf")
        .arg("scale=1280:720") // Scaling the video to 1280x720 resolution
        .arg("-vsync")
        .arg("0") // Ensure frames are extracted at their exact timestamp
        .arg("-t")
        .arg(max_timestamp.to_string())
        .arg(format!("{}/frame_%04d.png", temp_frame_dir)) // Output file path for frames
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    // Wait for the frame extraction process to finish
    let output = extract_status.wait_with_output()?;
    if !output.status.success() {
        eprintln!(
            "FFmpeg failed during frame extraction: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        return Err("FFmpeg frame extraction failed.".into());
    }

    // Step 2: Encode each frame individually
    // FFmpeg concatenates frames with the right encoding settings into the final video
    let concat_status = Command::new("ffmpeg")
        .arg("-framerate")
        .arg("60") // Set desired framerate (e.g., 30 fps)
        .arg("-i")
        .arg(format!("{}/frame_%04d.png", temp_frame_dir)) // Input frames pattern
        .arg("-c:v")
        .arg("libx265") // Video codec (e.g., H.265)
        .arg("-b:v")
        .arg("10K") // Set the video bitrate (e.g., 100 Mbps)
        .arg("-preset")
        .arg("fast") // Encoding preset (fast, medium, slow)
        .arg("-y") // Overwrite output file without asking
        .arg(output_path) // Output video file
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    // Wait for the concatenation process to finish
    let concat_output = concat_status.wait_with_output()?;
    if !concat_output.status.success() {
        eprintln!(
            "FFmpeg failed during concatenation: {}",
            String::from_utf8_lossy(&concat_output.stderr)
        );
        return Err("FFmpeg concatenation failed.".into());
    }

    // Step 4: Clean up temporary frames
    fs::remove_dir_all(temp_frame_dir)?;

    // Success
    println!("Video encoding complete: {}", output_path);
    Ok(())
}
