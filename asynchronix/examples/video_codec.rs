use std::path::Path;
use std::process::Command;
use anyhow::{Result, Context};
use ffmpeg_next::{
    format::input,
    media::Type,
    codec,
    format::Pixel,  // Use Pixel instead of Format
};

use image::{RgbImage, ImageBuffer, Rgb};

struct VideoTranscoder;

impl VideoTranscoder {
    fn transcode_to_hevc(input_path: &str, output_path: &str) -> Result<()> {
        // Use FFmpeg CLI for transcoding
        let output = Command::new("ffmpeg")
            .args(&[
                "-i", input_path,          // Input file
                "-c:v", "hevc",            // Video codec: HEVC
                "-preset", "medium",        // Encoding preset
                "-crf", "23",              // Constant Rate Factor (quality)
                "-c:a", "copy",            // Copy audio without re-encoding
                output_path
            ])
            .output()
            .context("Failed to execute FFmpeg")?;

        // Check for FFmpeg errors
        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "FFmpeg error: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        println!(
            "Transcoding completed. stdout: {}, stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        Ok(())
    }

    // Optional: get video metadata
    fn get_video_metadata(input_path: &str) -> Result<()> {
        // Open the input file
        let input_context = input(&Path::new(input_path))
            .context("Failed to open input file")?;

        // Find video stream
        let video_stream = input_context.streams()
            .best(Type::Video)
            .context("No video stream found")?;

        println!("Video Stream Information:");
        // println!("Codec: {}", video_stream.codec());
        println!("Duration: {:?}", input_context.duration());
        println!("Frames: {:?}", video_stream.frames());

        Ok(())
    }
}

fn main() -> Result<()> {
    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/asynchronix/examples/BigBuckBunny.mp4";
    let output_path = "output.hevc.mp4";

    // Optionally get metadata first
    VideoTranscoder::get_video_metadata(input_path)?;

    // Transcode to HEVC
    VideoTranscoder::transcode_to_hevc(input_path, output_path)?;

    Ok(())
}